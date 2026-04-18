"""
飞书长连接方式接收消息

多会话并行架构：
- MainSession：每个 chat_id 一个，永不关闭，维护主对话历史
- ParallelSession：按需创建，最多 1 个，处理完自动关闭
- 等待队列：最多 3 条，FIFO 顺序
- 所有会话的消息记录都汇入 MainSession 的历史日志
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
load_dotenv()

import json
import logging
import threading
import time
import uuid
from queue import Queue, Empty
from dataclasses import dataclass, field
from typing import Optional

import lark_oapi as lark
from lark_oapi.adapter.flask import *
from lark_oapi.api.im.v1 import *

from src.claude_code import chat_sync, PersistentClient
from src.feishu_utils.feishu_utils import send_message, reply_message, update_card_message, send_card_message, download_file_message
from src.data_base_utils import get_session, save_session, get_workspace, save_workspace

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")

DEFAULT_DOWNLOAD_DIR = str(Path(__file__).parent.parent / "data" / "downloads")

MAX_PARALLEL_SESSIONS = 2  # 最多 2 个并行会话
MAX_WAITING_QUEUE = 3       # 等待队列最多 3 条

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# ---------- 连接状态跟踪 ----------
_known_chat_ids: set[str] = set()
_chat_ids_lock = threading.Lock()

_token_cache: dict = {"token": None, "time": 0}
_ws_connected = False
_ws_lock = threading.Lock()
_first_connect_notified = False
_first_connect_lock = threading.Lock()

# ---------- token 获取 ----------
def get_token():
    now = time.time()
    if _token_cache["token"] and now - _token_cache["time"] < 5400:
        return _token_cache["token"]
    from src.feishu_utils.feishu_utils import get_tenant_access_token
    token = get_tenant_access_token()
    _token_cache["token"] = token
    _token_cache["time"] = now
    return token

# ---------- 飞书通知 ----------
def notify_all_chats(text: str):
    with _chat_ids_lock:
        chat_ids = list(_known_chat_ids)
    if not chat_ids:
        return
    token = get_token()
    if not token:
        return
    for chat_id in chat_ids:
        try:
            send_message(chat_id, text, token)
        except Exception as e:
            logger.error(f"通知失败 [{chat_id[:8]}...]: {e}")

# ---------- 日志处理器 ----------
_IGNORE_EVENT_TYPES = frozenset([
    "im.message.message_read_v1",
    "im.chat.access_event.bot_p2p_chat_entered_v1",
])

class LarkLogFilter(logging.Filter):
    def filter(self, record):
        try:
            msg = record.getMessage()
            for evt_type in _IGNORE_EVENT_TYPES:
                if evt_type in msg and "processor not found" in msg:
                    return False
            return True
        except Exception:
            return True

class FeishuLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = record.getMessage()
            global _ws_connected
            if "connected to wss://" in msg:
                with _ws_lock:
                    was_connected = _ws_connected
                    _ws_connected = True
                if not was_connected:
                    global _first_connect_notified
                    with _first_connect_lock:
                        if not _first_connect_notified:
                            _first_connect_notified = True
                            def _notify():
                                time.sleep(2)
                                notify_all_chats("🟢 Claude Code 已上线，可以开始使用")
                            threading.Thread(target=_notify, daemon=True).start()
            elif "receive message loop exit" in msg or "disconnect" in msg.lower():
                with _ws_lock:
                    _ws_connected = False
        except Exception:
            pass

_lark_logger = logging.getLogger("Lark")
_lark_logger.setLevel(logging.INFO)
_lark_logger.addFilter(LarkLogFilter())
_lark_logger.addHandler(FeishuLogHandler())
_lark_logger.propagate = False

# ---------- 多会话架构 ----------

@dataclass
class MainSession:
    """主会话：每个 chat_id 一个，永不关闭"""
    chat_id: str
    session_id: str
    busy: bool = False
    queue: Queue = field(default_factory=Queue)
    thread: Optional[threading.Thread] = None
    log_file: str = ""
    workspace: str = ""
    persistent_client: Optional[PersistentClient] = field(default=None, repr=False)

    def __post_init__(self):
        _logs_dir = str(Path(__file__).parent.parent / "logs")
        os.makedirs(_logs_dir, exist_ok=True)
        self.log_file = os.path.join(_logs_dir, f"claude_session_{self.chat_id}.log")

    def log_message(self, role: str, text: str, session_tag: str = "main"):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(f"[{ts}][{session_tag}] {role}: {text[:200]}\n")

    def get_persistent_client(self) -> PersistentClient:
        """获取或创建持久客户端"""
        if self.persistent_client is None:
            self.persistent_client = PersistentClient(
                session_id=self.session_id if self.session_id else None,
                cwd=self.workspace if self.workspace else None,
                idle_timeout=1920,  # 32分钟无消息自动断开
            )
        return self.persistent_client


@dataclass
class ParallelSession:
    """并行会话：按需创建，处理完自动关闭"""
    session_id: str
    parent_chat_id: str
    busy: bool = False
    queue: Queue = field(default_factory=Queue)
    thread: Optional[threading.Thread] = None


# 全局状态
_main_sessions: dict[str, MainSession] = {}
_parallel_sessions: dict[str, ParallelSession] = {}
_waiting_queue: dict[str, Queue] = {}
_global_lock = threading.Lock()


def _cancel_all_tasks(chat_id: str) -> int:
    """
    取消指定 chat_id 的所有正在处理的任务（主会话 + 并行会话）。
    返回取消的任务数。
    """
    count = 0
    with _global_lock:
        if chat_id in _main_sessions:
            main = _main_sessions[chat_id]
            if main.busy:
                if main.persistent_client:
                    main.persistent_client.disconnect()
                    main.persistent_client = None
                main.busy = False
                count += 1
        for ps in list(_parallel_sessions.values()):
            if ps.parent_chat_id == chat_id and ps.busy:
                ps.busy = False
                count += 1
    return count


def _dispatch_to_session(chat_id: str, text: str, message_id: str, chat_type: str, target: str):
    """向指定会话分发消息"""
    if target == "main":
        main = _main_sessions[chat_id]
        main.queue.put((text, message_id, chat_type))
        thread_alive = main.thread is not None and main.thread.is_alive()
        logger.info(f"[{chat_id[:8]}...] dispatch main: thread_alive={thread_alive}, queue_size={main.queue.qsize()}")
        if not thread_alive:
            main.busy = True
            main.thread = threading.Thread(target=_process_main_session, args=(main,), daemon=True)
            main.thread.start()
            logger.info(f"[{chat_id[:8]}...] 主会话线程已创建")
    elif target == "parallel":
        with _global_lock:
            parallel = list(_parallel_sessions.values())[0]
        parallel.queue.put((text, message_id, chat_type))
        if parallel.thread is None or not parallel.thread.is_alive():
            parallel.busy = True
            parallel.thread = threading.Thread(target=_process_parallel_session, args=(parallel,), daemon=True)
            parallel.thread.start()


def _process_main_session(main: MainSession):
    """处理主会话队列"""
    logger.info(f"[{main.chat_id[:8]}...] 主会话线程启动")
    while True:
        try:
            message, message_id, chat_type = main.queue.get(timeout=1)
        except Empty:
            # 队列空，检查等待队列
            got_from_waiting = False
            try:
                with _global_lock:
                    if main.chat_id in _waiting_queue and not _waiting_queue[main.chat_id].empty():
                        message, message_id, chat_type = _waiting_queue[main.chat_id].get()
                        got_from_waiting = True
            except Exception:
                pass

            if not got_from_waiting:
                # 队列空且没有等待消息 → 关闭检查
                main.busy = False
                try:
                    with _global_lock:
                        _check_close_parallel_nolock(main.chat_id)
                except Exception:
                    pass
                logger.info(f"[{main.chat_id[:8]}...] 主会话线程退出（空闲）")
                return

        main.busy = True
        main.log_message("user", message, session_tag="main")

        try:
            status_res = send_card_message(main.chat_id, "🤔 思考中...", get_token(), workspace=main.workspace)
            status_msg_id = status_res.get("data", {}).get("message_id", "")

            # 使用持久客户端
            pc = main.get_persistent_client()

            elapsed = [0]
            def on_heartbeat():
                elapsed[0] += 30
                if status_msg_id:
                    update_card_message(
                        status_msg_id,
                        f"⏳ 处理中…（已运行 {elapsed[0]} 秒）",
                        get_token(),
                        workspace=main.workspace,
                    )

            reply, new_session_id = pc.chat_sync(message, on_heartbeat=on_heartbeat)
            if new_session_id != main.session_id:
                main.session_id = new_session_id
                save_session(main.chat_id, new_session_id)
                logger.info(f"主会话 {main.chat_id[:8]}... session 更新: {new_session_id[:8]}...")

            if status_msg_id:
                update_card_message(status_msg_id, reply, get_token(), workspace=main.workspace)
            else:
                if chat_type == "group":
                    reply_message(message_id, reply)
                else:
                    send_message(main.chat_id, reply, get_token())

            main.log_message("assistant", reply, session_tag="main")
            logger.info(f"[主会话] 回复: {reply[:80]}...")

        except TimeoutError:
            logger.error(f"[主会话] 处理超时 [{main.chat_id[:8]}...]")
            main.persistent_client = None
            try:
                if status_msg_id:
                    update_card_message(
                        status_msg_id,
                        "❌ 处理超时（30 分钟），请简化任务后重试。",
                        get_token(),
                        workspace=main.workspace,
                    )
                else:
                    send_message(
                        main.chat_id,
                        "❌ 处理超时（30 分钟），请简化任务后重试。",
                        get_token(),
                    )
            except Exception:
                pass
            main.busy = False

        except Exception as e:
            logger.error(f"[主会话] 处理失败: {e}")
            try:
                send_message(main.chat_id, f"❌ 处理失败: {e}", get_token())
            except Exception:
                pass
            main.busy = False


def _process_parallel_session(parallel: ParallelSession):
    """处理并行会话队列"""
    session_tag = f"parallel_{parallel.session_id[:8]}"

    while True:
        try:
            message, message_id, chat_type = parallel.queue.get(timeout=1)
        except Empty:
            # 队列空，检查等待队列
            got_from_waiting = False
            try:
                with _global_lock:
                    if parallel.parent_chat_id in _waiting_queue and not _waiting_queue[parallel.parent_chat_id].empty():
                        message, message_id, chat_type = _waiting_queue[parallel.parent_chat_id].get()
                        got_from_waiting = True
            except Exception:
                pass

            if not got_from_waiting:
                # 队列空且没有等待消息 → 关闭并行会话
                parallel.busy = False
                try:
                    with _global_lock:
                        _close_parallel_nolock(parallel.session_id)
                except Exception:
                    pass
                logger.info(f"[{session_tag}] 并行会话线程退出（空闲）")
                return

        parallel.busy = True

        # 记录到主会话日志
        with _global_lock:
            if parallel.parent_chat_id in _main_sessions:
                ts = time.strftime("%Y-%m-%d %H:%M:%S")
                with open(_main_sessions[parallel.parent_chat_id].log_file, "a", encoding="utf-8") as f:
                    f.write(f"[{ts}][{session_tag}] user: {message[:200]}\n")

        try:
            # 并行会话使用父主会话的工作空间
            parent_workspace = ""
            with _global_lock:
                if parallel.parent_chat_id in _main_sessions:
                    parent_workspace = _main_sessions[parallel.parent_chat_id].workspace

            status_res = send_card_message(parallel.parent_chat_id, "🤔 思考中...", get_token(), workspace=parent_workspace)
            status_msg_id = status_res.get("data", {}).get("message_id", "")

            reply, new_session_id = chat_sync(message, session_id=parallel.session_id, cwd=parent_workspace)
            if new_session_id != parallel.session_id:
                parallel.session_id = new_session_id

            if status_msg_id:
                update_card_message(status_msg_id, reply, get_token(), workspace=parent_workspace)
            else:
                if chat_type == "group":
                    reply_message(message_id, reply)
                else:
                    send_message(parallel.parent_chat_id, reply, get_token())

            # 记录回复到主会话日志
            with _global_lock:
                if parallel.parent_chat_id in _main_sessions:
                    ts = time.strftime("%Y-%m-%d %H:%M:%S")
                    with open(_main_sessions[parallel.parent_chat_id].log_file, "a", encoding="utf-8") as f:
                        f.write(f"[{ts}][{session_tag}] assistant: {reply[:200]}\n")

            logger.info(f"[{session_tag}] 回复: {reply[:80]}...")

        except Exception as e:
            logger.error(f"[{session_tag}] 处理失败: {e}")
            try:
                send_message(parallel.parent_chat_id, f"❌ 处理失败: {e}", get_token())
            except Exception:
                pass
            parallel.busy = False


def _check_close_parallel(chat_id: str):
    """检查是否可以关闭并行会话（外部调用，会获取锁）"""
    with _global_lock:
        _check_close_parallel_nolock(chat_id)


def _check_close_parallel_nolock(chat_id: str):
    """检查是否可以关闭并行会话（已在锁内调用，不再获取锁）"""
    if chat_id in _waiting_queue and not _waiting_queue[chat_id].empty():
        # 有等待消息，唤醒主会话处理
        if chat_id in _main_sessions:
            main = _main_sessions[chat_id]
            if main.thread is None or not main.thread.is_alive():
                main.busy = True
                main.thread = threading.Thread(target=_process_main_session, args=(main,), daemon=True)
                main.thread.start()
                return  # 不关闭并行会话
    # 没有等待消息，检查关闭
    for ps in list(_parallel_sessions.values()):
        if ps.parent_chat_id == chat_id and not ps.busy and ps.queue.empty():
            _close_parallel_nolock(ps.session_id)
            return


def _close_parallel(session_id: str):
    """关闭并行会话（外部调用，会获取锁）"""
    with _global_lock:
        _close_parallel_nolock(session_id)


def _close_parallel_nolock(session_id: str):
    """关闭并行会话（已在锁内调用，不再获取锁）"""
    ps = _parallel_sessions.pop(session_id, None)
    # 关闭前检查是否有等待消息需要主会话处理
    if ps and ps.parent_chat_id in _waiting_queue and not _waiting_queue[ps.parent_chat_id].empty():
        if ps.parent_chat_id in _main_sessions:
            main = _main_sessions[ps.parent_chat_id]
            if main.thread is None or not main.thread.is_alive():
                main.busy = True
                main.thread = threading.Thread(target=_process_main_session, args=(main,), daemon=True)
                main.thread.start()
    if ps:
        logger.info(f"[parallel_{session_id[:8]}] 并行会话关闭")


def handle_message(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    """处理飞书消息 - 多会话分发"""
    try:
        event = data.event
        message = event.message
        message_id = message.message_id
        chat_id = message.chat_id
        chat_type = message.chat_type
        message_type = message.message_type

        content = json.loads(message.content)
        text = content.get("text", "")

        if message.mentions:
            for mention in message.mentions:
                text = text.replace(f"@{mention.name}", "").strip()

        # 群聊中只处理 @机器人 的消息
        if chat_type == "group":
            if not message.mentions:
                return

        # ========== 文件/图片消息处理 ==========
        if message_type in ("file", "image"):
            with _chat_ids_lock:
                _known_chat_ids.add(chat_id)

            # 确保主会话存在（获取 workspace）
            with _global_lock:
                if chat_id not in _main_sessions:
                    session_id = get_session(chat_id) or ""
                    workspace = get_workspace(chat_id) or ""
                    main = MainSession(chat_id=chat_id, session_id=session_id, workspace=workspace)
                    _main_sessions[chat_id] = main
                    if chat_id not in _waiting_queue:
                        _waiting_queue[chat_id] = Queue()

            main = _main_sessions[chat_id]
            workspace = main.workspace if main.workspace else None

            # 确定下载目录
            if workspace:
                base_dir = os.path.join(workspace, ".downloads")
            else:
                base_dir = DEFAULT_DOWNLOAD_DIR
            os.makedirs(base_dir, exist_ok=True)

            # 解析 file_key / image_key
            file_key = content.get("file_key") or content.get("image_key", "")
            if not file_key:
                send_message(chat_id, "⚠️ 无法解析文件信息", get_token())
                return

            # 构造文件名
            timestamp = int(time.time())
            original_name = content.get("file_name", f"file_{timestamp}")
            if message_type == "image":
                original_name = f"image_{timestamp}.{content.get('image_type', 'png')}"
            safe_name = "".join(c for c in original_name if c.isalnum() or c in "._- ")
            save_path = os.path.join(base_dir, f"{chat_id}_{timestamp}_{safe_name}")

            # 发送状态卡片
            status_res = send_card_message(chat_id, "📥 正在下载文件...", get_token(), workspace=workspace)
            status_msg_id = status_res.get("data", {}).get("message_id", "")

            try:
                download_file_message(message_id, file_key, save_path, get_token())
                logger.info(f"[{chat_id[:8]}...] 文件下载成功: {save_path}")
            except Exception as e:
                logger.error(f"[{chat_id[:8]}...] 文件下载失败: {e}")
                if status_msg_id:
                    update_card_message(status_msg_id, f"❌ 文件下载失败: {e}", get_token(), workspace=workspace)
                else:
                    send_message(chat_id, f"❌ 文件下载失败: {e}", get_token())
                return

            # 合并文本和文件路径
            if text.strip():
                synthetic_text = f"{text}\n\n文件路径: {save_path}"
            else:
                synthetic_text = f"用户发送了一个文件: {safe_name}，已保存到 {save_path}，请读取并分析内容"

            # 记录日志
            main.log_message("user", f"[{message_type}] {safe_name} -> {save_path}", session_tag="file")

            # 更新状态卡片，进入分发流程
            if status_msg_id:
                update_card_message(status_msg_id, "🤔 思考中...", get_token(), workspace=workspace)

            # ========== 走正常分发逻辑 ==========
            queue_size = _waiting_queue[chat_id].qsize()
            if queue_size > 0:
                if queue_size >= MAX_WAITING_QUEUE:
                    send_message(chat_id, f"⚠️ 等待队列已满，请稍后再试", get_token())
                else:
                    _waiting_queue[chat_id].put((synthetic_text, message_id, chat_type))
                    send_message(chat_id, f"⏳ 已加入队列({queue_size+1}/{MAX_WAITING_QUEUE})", get_token())
                return

            if not main.busy and main.queue.empty():
                logger.info(f"[{chat_id[:8]}...] 分发→主会话")
                _dispatch_to_session(chat_id, synthetic_text, message_id, chat_type, "main")
            else:
                logger.info(f"[{chat_id[:8]}...] 主会话忙，检查并行")
                with _global_lock:
                    idle_parallel = None
                    for ps in _parallel_sessions.values():
                        if ps.parent_chat_id == chat_id and not ps.busy and ps.queue.empty():
                            idle_parallel = ps
                            break

                if idle_parallel:
                    logger.info(f"[{chat_id[:8]}...] 分发→并行会话 {idle_parallel.session_id[:8]}")
                    _dispatch_to_session(chat_id, synthetic_text, message_id, chat_type, "parallel")
                elif len(_parallel_sessions) < MAX_PARALLEL_SESSIONS:
                    new_sid = str(uuid.uuid4())
                    parallel = ParallelSession(session_id=new_sid, parent_chat_id=chat_id)
                    with _global_lock:
                        _parallel_sessions[new_sid] = parallel
                    logger.info(f"[{chat_id[:8]}...] 创建新并行会话 {new_sid[:8]}")
                    _dispatch_to_session(chat_id, synthetic_text, message_id, chat_type, "parallel")
                else:
                    if queue_size >= MAX_WAITING_QUEUE:
                        send_message(chat_id, f"⚠️ 等待队列已满，请稍后再试", get_token())
                    else:
                        _waiting_queue[chat_id].put((synthetic_text, message_id, chat_type))
                        send_message(chat_id, f"⏳ 已加入队列({queue_size+1}/{MAX_WAITING_QUEUE})", get_token())
            return

        # ========== 纯文本消息 ==========
        if not text:
            return

        # ========== 处理命令 ==========
        if text in ("/cancel", "/取消任务"):
            count = _cancel_all_tasks(chat_id)
            if count > 0:
                send_message(chat_id, f"✅ 已取消 {count} 个正在处理的任务", get_token())
                logger.info(f"[{chat_id[:8]}...] 取消了 {count} 个任务")
            else:
                send_message(chat_id, "ℹ️ 当前没有正在处理的任务", get_token())
            return

        if text.startswith("/setworkspace "):
            workspace_path = text.split(" ", 1)[1].strip()
            if not workspace_path:
                send_message(chat_id, "❌ 请指定工作空间路径，如：/setworkspace /path/to/project", get_token())
                return

            # 验证路径是否存在
            if not os.path.isdir(workspace_path):
                send_message(chat_id, f"❌ 路径不存在或不是目录：{workspace_path}", get_token())
                return

            save_workspace(chat_id, workspace_path)

            # 更新已存在的主会话
            with _global_lock:
                if chat_id in _main_sessions:
                    _main_sessions[chat_id].workspace = workspace_path

            send_message(chat_id, f"✅ 工作空间已设置：{workspace_path}\n\n后续对话将在此目录下进行。", get_token())
            logger.info(f"[{chat_id[:8]}...] 工作空间设置: {workspace_path}")
            return

        if text == "/workspace":
            current = get_workspace(chat_id)
            if current:
                send_message(chat_id, f"📁 当前工作空间：{current}", get_token())
            else:
                send_message(chat_id, "📁 未设置工作空间，使用默认目录。", get_token())
            return

        # ========== 正常消息分发 ==========
        with _chat_ids_lock:
            _known_chat_ids.add(chat_id)

        logger.info(f"收到消息 [{chat_id[:8]}...]: {text[:50]}...")

        # 确保主会话存在
        with _global_lock:
            if chat_id not in _main_sessions:
                session_id = get_session(chat_id) or ""
                workspace = get_workspace(chat_id) or ""
                main = MainSession(chat_id=chat_id, session_id=session_id, workspace=workspace)
                _main_sessions[chat_id] = main
                if chat_id not in _waiting_queue:
                    _waiting_queue[chat_id] = Queue()

        main = _main_sessions[chat_id]

        # ========== 分发逻辑 ==========
        queue_size = _waiting_queue[chat_id].qsize()
        logger.info(f"[{chat_id[:8]}...] 分发检查: busy={main.busy}, queue_empty={main.queue.empty()}, waiting_size={queue_size}")

        # 1. 等待队列有消息 → 新消息入等待队列
        if queue_size > 0:
            if queue_size >= MAX_WAITING_QUEUE:
                send_message(chat_id, f"⚠️ 等待队列已满（{queue_size}/{MAX_WAITING_QUEUE}），请稍后再试", get_token())
                main.log_message("system", f"队列满，消息被拒绝", session_tag="queue")
            else:
                _waiting_queue[chat_id].put((text, message_id, chat_type))
                send_message(chat_id, f"⏳ 已加入队列({queue_size+1}/{MAX_WAITING_QUEUE})，空闲时自动处理", get_token())
                main.log_message("user", text, session_tag=f"queue({queue_size+1})")
            return

        # 2. 等待队列空，检查各会话状态
        if not main.busy and main.queue.empty():
            # 主会话空闲 → 主会话处理
            logger.info(f"[{chat_id[:8]}...] 分发→主会话")
            _dispatch_to_session(chat_id, text, message_id, chat_type, "main")

        else:
            # 主会话忙，检查并行会话
            logger.info(f"[{chat_id[:8]}...] 主会话忙，检查并行")
            with _global_lock:
                idle_parallel = None
                for ps in _parallel_sessions.values():
                    if ps.parent_chat_id == chat_id and not ps.busy and ps.queue.empty():
                        idle_parallel = ps
                        break

            if idle_parallel:
                # 并行会话空闲 → 并行会话处理
                logger.info(f"[{chat_id[:8]}...] 分发→并行会话 {idle_parallel.session_id[:8]}")
                _dispatch_to_session(chat_id, text, message_id, chat_type, "parallel")
            elif len(_parallel_sessions) < MAX_PARALLEL_SESSIONS:
                # 没有并行会话 → 创建并行会话处理
                new_sid = str(uuid.uuid4())
                parallel = ParallelSession(session_id=new_sid, parent_chat_id=chat_id)
                with _global_lock:
                    _parallel_sessions[new_sid] = parallel
                logger.info(f"[{chat_id[:8]}...] 创建新并行会话 {new_sid[:8]}")
                _dispatch_to_session(chat_id, text, message_id, chat_type, "parallel")
            else:
                # 所有会话都忙 → 入等待队列
                if queue_size >= MAX_WAITING_QUEUE:
                    send_message(chat_id, f"⚠️ 等待队列已满，请稍后再试", get_token())
                    main.log_message("system", "队列满，消息被拒绝", session_tag="queue")
                else:
                    _waiting_queue[chat_id].put((text, message_id, chat_type))
                    send_message(chat_id, f"⏳ 已加入队列({queue_size+1}/{MAX_WAITING_QUEUE})，空闲时自动处理", get_token())
                    main.log_message("user", text, session_tag=f"queue({queue_size+1})")

    except Exception as e:
        logger.error(f"处理消息失败: {e}")


def main():
    while True:
        try:
            client = lark.ws.Client(
                APP_ID,
                APP_SECRET,
                event_handler=lark.EventDispatcherHandler.builder("", "")
                    .register_p2_im_message_receive_v1(handle_message)
                    .build(),
                log_level=lark.LogLevel.INFO,
            )

            logger.info("启动飞书长连接...")
            client.start()

        except Exception as e:
            logger.error(f"长连接异常退出: {e}")
        finally:
            with _ws_lock:
                _ws_connected = False

        logger.info("5秒后重连...")
        time.sleep(5)


if __name__ == "__main__":
    main()
