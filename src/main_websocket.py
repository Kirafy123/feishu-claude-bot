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
import logging
import logging.handlers
from dotenv import load_dotenv
load_dotenv()

import glob
import json
import logging
import re
import signal
import threading
import time
import uuid
from queue import Queue, Empty
from dataclasses import dataclass, field
from typing import Optional

import lark_oapi as lark
from lark_oapi.adapter.flask import *
from lark_oapi.api.im.v1 import *

from src.claude_code import chat_sync, PersistentClient, _cleanup_orphan_claude, _init_known_claude_pids
from src.feishu_utils.feishu_utils import (
    send_message, reply_message, update_card_message, send_card_message,
    download_file_message, upload_file_to_feishu, send_file_message, zip_folder
)
from src.data_base_utils import get_session, save_session, get_workspace, save_workspace

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")

DEFAULT_DOWNLOAD_DIR = str(Path(__file__).parent.parent / "data" / "downloads")

MAX_PARALLEL_SESSIONS = 2  # 最多 2 个并行会话
MAX_WAITING_QUEUE = 3       # 等待队列最多 3 条
MAX_SESSION_QUEUE = 10      # 会话队列最大容量

# 强制 UTF-8 输出，避免 Windows GBK 乱码
sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None
sys.stderr.reconfigure(encoding='utf-8') if hasattr(sys.stderr, 'reconfigure') else None

# 日志重定向到文件（RotatingFileHandler，5MB * 3 备份）
_logs_dir = str(Path(__file__).parent.parent / "logs")
os.makedirs(_logs_dir, exist_ok=True)
_log_file = os.path.join(_logs_dir, "service.log")
_file_handler = logging.handlers.RotatingFileHandler(
    _log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8',
)
_file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(threadName)s] %(message)s'))
_log_handlers = [_file_handler]
if sys.stdout is not None:
    _console_handler = logging.StreamHandler(sys.stdout)
    _console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(threadName)s] %(message)s'))
    _log_handlers.append(_console_handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(threadName)s] %(message)s',
    force=True,
    handlers=_log_handlers
)
logger = logging.getLogger(__name__)

# pythonw.exe 下 sys.stderr 为 None，安全包装避免崩溃
if sys.stderr is not None:
    try:
        sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', encoding='utf-8', buffering=1)
    except Exception:
        pass

# ---------- 连接状态跟踪 ----------
_known_chat_ids: set[str] = set()
_chat_ids_lock = threading.Lock()

_token_cache: dict = {"token": None, "time": 0}
_ws_connected = False
_ws_lock = threading.Lock()
_ws_disconnect_since: Optional[float] = None  # 上次断线时间，None 表示当前已连接或从未连接过
_ws_reconnect_time: float = 0.0            # 最近一次重连（非首连）的时刻
_last_reconnect_notify_time: float = 0.0   # 限流：上次发重连通知的时刻
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
            global _ws_connected, _ws_disconnect_since, _ws_reconnect_time
            global _first_connect_notified, _last_reconnect_notify_time
            if "connected to wss://" in msg:
                with _ws_lock:
                    was_connected = _ws_connected
                    _ws_connected = True
                    _ws_disconnect_since = None
                if not was_connected:
                    now = time.time()
                    with _first_connect_lock:
                        if not _first_connect_notified:
                            # 首次上线
                            _first_connect_notified = True
                            def _notify():
                                time.sleep(2)
                                notify_all_chats("🟢 Claude Code 已上线，可以开始使用")
                            threading.Thread(target=_notify, daemon=True).start()
                        else:
                            # 重连：记录重连时刻，并发通知（限流 5 分钟一次）
                            _ws_reconnect_time = now
                            if now - _last_reconnect_notify_time > 300:
                                _last_reconnect_notify_time = now
                                def _reconnect_notify():
                                    time.sleep(2)
                                    notify_all_chats("🟢 服务已重连，之前的消息将自动继续处理")
                                threading.Thread(target=_reconnect_notify, daemon=True).start()
            elif "receive message loop exit" in msg or "disconnect" in msg.lower():
                with _ws_lock:
                    was_connected = _ws_connected
                    _ws_connected = False
                    if was_connected and _ws_disconnect_since is None:
                        _ws_disconnect_since = time.time()
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
    queue: Queue = field(default_factory=lambda: Queue(maxsize=MAX_SESSION_QUEUE))
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
            chat_id = self.chat_id
            def _on_session_id_changed(new_sid: str):
                """session_id 变更时持久化到 DB"""
                try:
                    save_session(chat_id, new_sid)
                except Exception as e:
                    logger.error(f"[{chat_id[:8]}...] 保存 session_id 失败: {e}")
            self.persistent_client = PersistentClient(
                session_id=self.session_id if self.session_id else None,
                cwd=self.workspace if self.workspace else None,
                idle_timeout=1920,  # 32分钟无消息自动断开
                on_session_changed=_on_session_id_changed,
            )
        return self.persistent_client


@dataclass
class ParallelSession:
    """并行会话：按需创建，处理完自动关闭"""
    session_id: str
    parent_chat_id: str
    busy: bool = False
    queue: Queue = field(default_factory=lambda: Queue(maxsize=MAX_SESSION_QUEUE))
    thread: Optional[threading.Thread] = None


# 全局状态
_main_sessions: dict[str, MainSession] = {}
_parallel_sessions: dict[str, ParallelSession] = {}
_waiting_queue: dict[str, Queue] = {}
_global_lock = threading.Lock()
_shutting_down = False  # 关闭中标志，阻止新消息入队

# 消息去重：防止飞书重发导致同一消息被处理多次
_processed_messages: set[str] = set()
_processed_messages_lock = threading.Lock()

# 文件消息合并窗口：文件消息到达后，等待 N 秒让用户补充文本，避免文件和说明被分到不同会话
PENDING_MERGE_SECONDS = 60
_pending_attachments: dict[str, dict] = {}  # chat_id -> {paths, names, timer, message_id, chat_type, status_msg_id, workspace}
_pending_lock = threading.Lock()

# ========== 文件索取检测（Plan A） ==========

# 文件索取意图关键词
_FILE_REQUEST_PATTERNS = [
    r'发.*给我',
    r'发.*文件',
    r'把.*文件.*发',
    r'把.*发.*我',
    r'下载.*文件',
    r'给我.*文件',
    r'发送.*文件',
    r'发一下',
    r'发我',
    r'发过来',
    r'发.*过来',
    r'传给.*我',
    r'传.*给.*我',
    r'给.*我.*文件',
    r'我要.*文件',
    r'看看.*文件',
    r'打开.*文件',
]

# 支持的文件扩展名
_SUPPORTED_EXTENSIONS = (
    '.xlsx', '.xls', '.doc', '.docx', '.pdf', '.ppt', '.pptx',
    '.csv', '.txt', '.html', '.htm', '.json', '.xml', '.zip',
    '.rar', '.7z', '.tar', '.gz', '.md', '.py', '.sql', '.log',
    '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg', '.webp',
    '.mp4', '.mp3', '.wav', '.avi',
)

# 文件选择状态：chat_id -> {candidates, timer, original_text, access_token, workspace}
_file_selection_state: dict[str, dict] = {}
_file_selection_lock = threading.Lock()

def _cancel_all_tasks(chat_id: str) -> int:
    """
    取消指定 chat_id 的所有正在处理的任务（主会话 + 并行会话）。
    清空等待队列。返回取消的任务数。
    """
    count = 0
    with _global_lock:
        if chat_id in _main_sessions:
            main = _main_sessions[chat_id]
            if main.persistent_client:
                main.persistent_client.disconnect()
                main.persistent_client = None
            if main.busy:
                main.busy = False
                count += 1
            # 清空等待队列
            if chat_id in _waiting_queue:
                while not _waiting_queue[chat_id].empty():
                    try:
                        _waiting_queue[chat_id].get_nowait()
                    except Empty:
                        break
        for ps in list(_parallel_sessions.values()):
            if ps.parent_chat_id == chat_id and ps.busy:
                ps.busy = False
                count += 1
    return count


def shutdown() -> dict:
    """
    优雅关闭：取消所有任务、清空队列、断开所有持久客户端。
    返回清理统计信息。
    """
    with _global_lock:
        cancelled_main = 0
        cancelled_parallel = 0
        cleared_waiting = 0

        # 1. 断开所有主会话的持久客户端，清空队列
        for main in list(_main_sessions.values()):
            if main.persistent_client:
                main.persistent_client.disconnect()
                main.persistent_client = None
            if main.busy:
                main.busy = False
                cancelled_main += 1
            while not main.queue.empty():
                try:
                    main.queue.get_nowait()
                except Empty:
                    break

        # 2. 取消所有并行会话
        for ps in list(_parallel_sessions.values()):
            if ps.busy:
                ps.busy = False
                cancelled_parallel += 1
            while not ps.queue.empty():
                try:
                    ps.queue.get_nowait()
                except Empty:
                    break
        _parallel_sessions.clear()

        # 3. 清空等待队列
        for q in _waiting_queue.values():
            cleared_waiting += q.qsize()
        _waiting_queue.clear()

    logger.info(
        f"[shutdown] 清理完成: 主会话任务={cancelled_main}, "
        f"并行会话任务={cancelled_parallel}, 等待队列={cleared_waiting}"
    )

    # 断开后最后扫一遍残留孤儿进程（父进程已被 kill）
    _cleanup_orphan_claude()
    return {
        "cancelled_main": cancelled_main,
        "cancelled_parallel": cancelled_parallel,
        "cleared_waiting": cleared_waiting,
    }


def _sync_session_busy(session) -> None:
    """同步 busy 标志与线程实际状态，防止线程退出但 busy 仍为 True"""
    if session.busy and session.thread and not session.thread.is_alive():
        session.busy = False


def _handle_signal(signum, frame):
    """信号处理器：收到 SIGTERM/SIGINT 时优雅关闭。
    注意：信号处理器内不做阻塞网络调用，避免卡死。通知丢到后台线程。
    """
    logger.info("[signal] 收到终止信号，开始清理...")
    global _shutting_down
    _shutting_down = True
    try:
        stats = shutdown()
        logger.info(f"[signal] 清理结果: {stats}")
    except Exception as e:
        logger.error(f"[signal] shutdown 异常: {e}")
    # 后台发通知，最多等 3 秒
    def _bg_notify():
        try:
            notify_all_chats("🔧 Claude Code 服务正在重启，请稍后重试")
        except Exception as e:
            logger.error(f"[signal] 通知失败: {e}")
    t = threading.Thread(target=_bg_notify, daemon=True)
    t.start()
    t.join(timeout=3)
    import sys
    sys.exit(0)


# 注册信号处理（仅主线程有效）
# Windows 上 SIGTERM 不可靠，用 SIGBREAK（Ctrl+Break）代替
if os.name == 'nt' and hasattr(signal, 'SIGBREAK'):
    signal.signal(signal.SIGBREAK, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ---------- 文件上传回传 ----------

def _parse_file_paths(text: str, workspace: str = "") -> list[str]:
    r"""从 Claude 回复文本中提取文件/文件夹路径。

    支持格式：
    - 直接路径：C:\path\file.txt 或 /path/file.txt
    - 被引号/反引号包裹的路径
    - 相对路径（在 workspace 下补全）
    """
    paths = []

    # 1) Windows 绝对路径 + Unix 绝对路径
    pattern = re.compile(
        r'[`"\'(]*([A-Za-z]:(?:\\|/)[^\s"\'`<>|,*]+)[`"\')]*'
        r'|[`"\'(]*(/[^\s"\'`<>|,*]+)[`"\')]*',
    )
    for match in pattern.finditer(text):
        p = match.group(1) or match.group(2)
        p = p.rstrip('。，；')
        if os.path.exists(p):
            paths.append(p)

    # 2) 相对路径（如 output/file.xlsx），需要在 workspace 下补全
    if workspace:
        rel_pattern = re.compile(
            r'[`"\'(\s]([a-zA-Z0-9_一-鿀][^\s"\'`<>|,*]*\.[a-zA-Z]{2,6})[`"\')\s]*'
        )
        for match in rel_pattern.finditer(text):
            rel = match.group(1).rstrip('。，；')
            # 跳过已经是绝对路径的
            if os.path.isabs(rel):
                continue
            # 跳过 URL（包含 :// 的匹配）
            if '://' in rel:
                continue
            full = os.path.join(workspace, rel)
            if os.path.exists(full):
                paths.append(full)

    return paths


# 允许上传的路径白名单前缀（规范化后比较）
def _is_path_allowed(path: str, workspace: str = "") -> bool:
    """只允许 workspace、DEFAULT_DOWNLOAD_DIR、系统临时目录、当前 cwd 下的文件。
    防止把 /etc/... 或其他用户目录的文件误回传。
    """
    try:
        real = os.path.realpath(path)
    except Exception:
        return False
    allowed_roots = []
    if workspace:
        try:
            allowed_roots.append(os.path.realpath(workspace))
        except Exception:
            pass
    try:
        allowed_roots.append(os.path.realpath(DEFAULT_DOWNLOAD_DIR))
    except Exception:
        pass
    try:
        allowed_roots.append(os.path.realpath(os.getcwd()))
    except Exception:
        pass
    import tempfile as _tmp
    try:
        allowed_roots.append(os.path.realpath(_tmp.gettempdir()))
    except Exception:
        pass
    # 允许上传的额外目录
    extra_allowed = [
        r"D:\finance",
        r"D:\finance\2026年4月_经营报表分析",
        r"D:\finance\2026年4月_经营报表分析\output",
    ]
    for extra in extra_allowed:
        try:
            allowed_roots.append(os.path.realpath(extra))
        except Exception:
            pass
    for root in allowed_roots:
        if not root:
            continue
        try:
            if real == root or real.startswith(root + os.sep):
                return True
        except Exception:
            continue
    return False


def _extract_tool_file_paths(tool_calls: list[dict]) -> list[str]:
    """从 tool_calls 中提取写入/创建的文件路径。

    检测 Write（path/file_path）、Edit（file_path）、Bash（命令中的输出文件）。
    """
    paths = []
    for tc in tool_calls:
        name = tc.get("name", "")
        inp = tc.get("input", {})
        if name == "Write":
            p = inp.get("file_path") or inp.get("path")
            if p:
                paths.append(p)
        elif name == "Edit" and inp.get("file_path"):
            paths.append(inp["file_path"])
        elif name == "Bash" and inp.get("command"):
            cmd = inp["command"]
            # 从 Bash 命令中提取文件路径
            # 支持：cp, mv, cat >, curl -o, wget -O, tee, echo >, python script.py, >>
            # 捕获组统一支持绝对路径和相对路径（相对路径由 os.path.exists 过滤）
            path_capture = r'([A-Za-z]:(?:\\|/)[^\s"\'`<>|,*]+|/[^\s"\'`<>|,*]+|[a-zA-Z0-9_一-鿀][^\s"\'`<>|,]*\.[a-zA-Z]{2,6})'
            bash_patterns = [
                # cp/mv source destination
                r'(?:cp|mv)\s+(?:\S+\s+)*?' + path_capture + r'(?:\s*[;&|]|$)',
                # curl -o / wget -O file
                r'(?:curl\s+(?:-\S+\s+)*-o\s+|wget\s+(?:-\S+\s+)*-O\s+)' + path_capture,
                # cat > / cat >> / echo > / echo >> / tee > / tee >>
                # 用 (?:\S+\s+)*? 非贪婪匹配，在第一个 > 前停止，避免捕获输入文件
                r'(?:cat|echo|tee)\s+(?:\S+\s+)*?>{1,2}\s*' + path_capture,
                # python script.py output_file (脚本名后跟一个路径)
                r'python(?:3)?\s+\S+\.py\s+(?:[^&;|]+\s+)*?' + path_capture,
            ]
            for pat in bash_patterns:
                m = re.search(pat, cmd)
                if m:
                    paths.append(m.group(1))
                    break
    return [p for p in paths if os.path.exists(p)]


def _collect_file_paths(reply_text: str, tool_calls: list[dict], workspace: str = "") -> list[str]:
    """合并 tool_calls 路径和文本解析路径，去重排序，并应用白名单过滤。"""
    tool_paths = _extract_tool_file_paths(tool_calls)
    text_paths = _parse_file_paths(reply_text, workspace=workspace)
    logger.info(f"[文件回传] tool_paths={tool_paths}, text_paths={text_paths}")
    # 合并去重（保持顺序）
    seen = set()
    unique = []
    for p in tool_paths + text_paths:
        if p in seen:
            continue
        seen.add(p)
        if not _is_path_allowed(p, workspace):
            logger.warning(f"[文件回传] 路径不在白名单内，跳过: {p}")
            continue
        unique.append(p)
    return unique


def _derive_reply_filename(original_basename: str) -> str:
    """回传文件名：直接用 Claude 生成的文件名，去掉飞书下载的 oc_xxx_时间戳_ 前缀。"""
    base = os.path.basename(original_basename)
    m = re.match(r'^oc_[0-9a-f]+_\d+_(.+)$', base)
    if m:
        return m.group(1)
    return base


def _third_layer_detect(chat_id: str, reply_text: str, workspace: str = "") -> list[str]:
    """第三层检测：从 Claude 回复中提取提到的文件名，在 workspace 中搜索。

    当 tool_calls 中没有文件操作、回复中没有明确路径时触发。
    """
    if not workspace:
        return []

    mentioned_files = []
    pattern = re.compile(
        r'([\w一-鿿]+(?:\.[a-zA-Z]{2,6}))'
    )
    for m in pattern.finditer(reply_text):
        name = m.group(1)
        ext = os.path.splitext(name)[1].lower()
        if ext in _SUPPORTED_EXTENSIONS:
            mentioned_files.append(name)

    if not mentioned_files:
        return []

    mentioned_files = list(dict.fromkeys(mentioned_files))

    found_paths = []
    seen = set()
    for filename in mentioned_files:
        stem = os.path.splitext(filename)[0]
        candidates = _search_workspace_for_file(stem, workspace)
        for p in candidates:
            if p not in seen:
                seen.add(p)
                found_paths.append(p)

    return found_paths


def wait_for_file_ready(path: str, max_wait: float = 5.0, poll_interval: float = 0.5) -> tuple[bool, str]:
    """等待文件完全写入磁盘。

    通过检查文件大小稳定（连续两次读取相同）判断就绪。

    Returns:
        (True, "ready") — 文件就绪可上传
        (False, "reason") — 文件未就绪的原因
    """
    if not os.path.exists(path):
        return False, "文件不存在"

    # 空文件视为就绪（没有内容要写）
    try:
        if os.path.getsize(path) == 0:
            return True, "ready (empty)"
    except OSError:
        pass

    start = time.time()
    last_size = -1
    stable_count = 0
    current_size = 0

    while time.time() - start < max_wait:
        try:
            current_size = os.path.getsize(path)
        except OSError:
            # 文件可能被锁定，稍后重试
            time.sleep(poll_interval)
            continue

        if current_size == last_size and current_size > 0:
            stable_count += 1
            if stable_count >= 2:
                return True, "ready"
        else:
            stable_count = 0

        last_size = current_size
        time.sleep(poll_interval)

    if last_size > 0:
        # 文件存在但大小仍在变化，接受当前状态
        return True, "partial (accepted)"

    return False, f"等待超时 ({max_wait}s)，最终大小: {last_size}"


def _send_files_after_reply(chat_id: str, reply_text: str, access_token: str, tool_calls: list[dict], workspace: str = "") -> None:
    """合并 tool_calls 和文本解析得到文件路径，上传到飞书。"""
    paths = _collect_file_paths(reply_text, tool_calls, workspace=workspace)
    if not paths:
        # 第三层：现有检测未找到文件，尝试从回复中提取文件名并搜索
        logger.info(f"[文件回传] 现有检测未找到文件，触发第三层智能检测")
        paths = _third_layer_detect(chat_id, reply_text, workspace)
        if not paths:
            logger.info(f"[文件回传] 第三层检测也未找到文件")
            return
        logger.info(f"[文件回传] 第三层检测到 {len(paths)} 个文件: {paths}")

    logger.info(f"[文件回传] 检测到 {len(paths)} 个文件: {paths}")

    # 第三层检测到多个文件时，发送选择卡片让用户选择
    if len(paths) > 3:
        logger.info(f"[文件回传] 检测到多个文件({len(paths)}个)，发送选择卡片")
        _send_file_selection_card(chat_id, paths, reply_text[:50], access_token, workspace=workspace)
        return

    for p in paths:
        try:
            file_size = os.path.getsize(p)
            logger.info(f"[文件回传] 开始处理: {p} ({file_size} bytes)")

            upload_path = p
            raw_name = os.path.basename(p)

            # 文件夹 → 压缩
            if os.path.isdir(p):
                raw_name = os.path.basename(p) + '.zip'
                upload_path = zip_folder(p)
                if not os.path.exists(upload_path):
                    upload_path = p + '.zip'
                logger.info(f"[文件回传] 目录已压缩: {p} -> {upload_path}")

            # 等待文件就绪
            ready, reason = wait_for_file_ready(upload_path)
            if not ready:
                logger.warning(f"[文件回传] 文件未就绪: {upload_path} - {reason}")
                send_message(chat_id, f"⚠️ 文件可能未完全写入，无法回传：{raw_name}\n本地路径：{upload_path}", access_token)
                continue
            if reason == "partial (accepted)":
                logger.warning(f"[文件回传] 文件部分写入但仍发送: {upload_path}")

            # 按原始文件名 + 版本号重命名
            file_name = _derive_reply_filename(raw_name)
            logger.info(f"[文件回传] 重命名 {raw_name} -> {file_name}")

            # 上传（内置重试）
            result = upload_file_to_feishu(upload_path, access_token, timeout=120, file_name=file_name)
            if result.get("success"):
                file_key = result["file_key"]
                send_result = send_file_message(chat_id, file_key, file_name, access_token)
                if send_result.get("code") == 0:
                    send_message(chat_id, f"✅ 文件已发送：{file_name}", access_token)
                    logger.info(f"[文件回传] 完成: {p} -> {file_name}")
                else:
                    err = send_result.get("msg", "未知错误")
                    send_message(chat_id, f"⚠️ 文件已上传但发送消息失败：{err}\nfile_key: {file_key}", access_token)
                    logger.warning(f"[文件回传] 文件消息发送失败: {file_name} - {err}")
            else:
                send_message(chat_id, f"⚠️ 文件已生成，但上传失败：{result.get('error', '未知错误')}\n本地路径：{upload_path}", access_token)
                logger.error(f"[文件回传] 上传失败: {p} - {result.get('error')}")
        except Exception as e:
            send_message(chat_id, f"⚠️ 文件处理异常：{e}\n本地路径：{p}", access_token)
            logger.error(f"[文件回传] 处理异常: {p} - {e}", exc_info=True)


def _dispatch_to_session(chat_id: str, text: str, message_id: str, chat_type: str, target: str):
    """向指定会话分发消息"""
    if target == "main":
        main = _main_sessions[chat_id]
        # 检测崩溃：busy=True 但线程已死亡 → 重置状态
        if main.busy and main.thread and not main.thread.is_alive():
            main.busy = False
            main.thread = None
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
            # 只选属于当前 chat_id 且空闲的并行会话；优先复用
            parallel = None
            for ps in _parallel_sessions.values():
                if ps.parent_chat_id != chat_id:
                    continue
                # 检测崩溃
                if ps.busy and ps.thread and not ps.thread.is_alive():
                    ps.busy = False
                    ps.thread = None
                if not ps.busy and ps.queue.empty():
                    parallel = ps
                    break
            # 找不到空闲的就退而求其次：任何属于本 chat 的
            if parallel is None:
                for ps in _parallel_sessions.values():
                    if ps.parent_chat_id == chat_id:
                        parallel = ps
                        break
        if parallel is None:
            logger.error(f"[{chat_id[:8]}...] dispatch parallel 失败：找不到所属并行会话，回退主会话")
            _dispatch_to_session(chat_id, text, message_id, chat_type, "main")
            return
        parallel.queue.put((text, message_id, chat_type))
        if parallel.thread is None or not parallel.thread.is_alive():
            parallel.busy = True
            parallel.thread = threading.Thread(target=_process_parallel_session, args=(parallel,), daemon=True)
            parallel.thread.start()


def _is_file_request(text: str) -> bool:
    """判断用户消息是否为文件索取意图"""
    return any(re.search(p, text) for p in _FILE_REQUEST_PATTERNS)


def _extract_requested_filename(text: str) -> str | None:
    """从用户消息中提取他们想要的文件名（带扩展名）"""
    pattern = r'([\w一-鿿]+(?:\.[a-zA-Z]{2,6}))'
    matches = re.findall(pattern, text)
    for m in matches:
        ext = os.path.splitext(m)[1].lower()
        if ext in _SUPPORTED_EXTENSIONS:
            return m
    return None


def _search_workspace_for_file(filename: str, workspace: str) -> list[str]:
    """在 workspace 下搜索匹配文件名的文件，返回按修改时间降序排序的列表"""
    if not workspace or not os.path.isdir(workspace):
        return []
    pattern = os.path.join(workspace, "**", f"*{filename}*")
    matches = glob.glob(pattern, recursive=True)
    matches = [m for m in matches if not os.path.basename(m).startswith('.')]
    matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return matches


def _send_file_selection_card(chat_id: str, candidates: list[str], original_text: str,
                               access_token: str, workspace: str = "") -> None:
    """发送文件选择卡片消息，让用户从多个匹配中选择"""
    options_text = "\n".join(
        f"{i+1}. {os.path.basename(c)}" for i, c in enumerate(candidates[:5])
    )
    if len(candidates) > 5:
        options_text += f"\n... 等 {len(candidates)} 个文件"

    card_text = (
        f"找到以下匹配文件，请回复序号或输入更具体的文件名：\n\n"
        f"{options_text}\n\n"
        f"（60 秒内未选择将转 Claude 处理）"
    )
    send_card_message(chat_id, card_text, access_token, workspace=workspace)

    def _on_timeout():
        with _file_selection_lock:
            _file_selection_state.pop(chat_id, None)
        send_message(chat_id, "⏳ 文件选择超时，已转 Claude 处理", access_token)

    timer = threading.Timer(60, _on_timeout)
    timer.start()

    with _file_selection_lock:
        _file_selection_state[chat_id] = {
            "candidates": candidates,
            "timer": timer,
            "original_text": original_text,
            "access_token": access_token,
            "workspace": workspace,
        }


def _handle_file_selection_reply(chat_id: str, text: str) -> bool:
    """处理用户在文件选择中的回复。返回 True 表示已处理，False 表示需要继续转发 Claude。"""
    with _file_selection_lock:
        state = _file_selection_state.pop(chat_id, None)
    if not state:
        return False

    state["timer"].cancel()

    candidates = state["candidates"]
    access_token = state["access_token"]
    workspace = state["workspace"]

    if text.strip().isdigit():
        idx = int(text.strip()) - 1
        if 0 <= idx < len(candidates):
            target = candidates[idx]
            file_name = os.path.basename(target)
            result = upload_file_to_feishu(target, access_token, timeout=120, file_name=file_name)
            if result.get("success"):
                send_file_message(chat_id, result["file_key"], file_name, access_token)
                send_message(chat_id, f"✅ 已发送：{file_name}", access_token)
            else:
                send_message(chat_id, f"⚠️ 上传失败：{result.get('error', '未知错误')}", access_token)
            return True
        else:
            send_message(chat_id, f"⚠️ 序号超出范围，请重新输入（1-{len(candidates)}）", access_token)
            _send_file_selection_card(chat_id, candidates, state["original_text"], access_token, workspace)
            return True

    user_keyword = text.strip().lower()
    filtered = [p for p in candidates if user_keyword in os.path.basename(p).lower()]
    if len(filtered) == 1:
        target = filtered[0]
        file_name = os.path.basename(target)
        result = upload_file_to_feishu(target, access_token, timeout=120, file_name=file_name)
        if result.get("success"):
            send_file_message(chat_id, result["file_key"], file_name, access_token)
            send_message(chat_id, f"✅ 已发送：{file_name}", access_token)
        else:
            send_message(chat_id, f"⚠️ 上传失败：{result.get('error', '未知错误')}", access_token)
        return True
    elif len(filtered) > 1:
        _send_file_selection_card(chat_id, filtered, text, access_token, workspace)
        return True
    else:
        send_message(chat_id, f"⚠️ 未找到更匹配的文件，已转 Claude 处理", access_token)
        return False


def _route_message(chat_id: str, text: str, message_id: str, chat_type: str) -> None:
    """统一分发：主会话空闲→主；否则→并行/新并行/等待队列。
    供纯文本、文件消息、合并消息共用。
    调用前需保证 _main_sessions[chat_id] 和 _waiting_queue[chat_id] 已初始化。
    """
    main = _main_sessions[chat_id]
    queue_size = _waiting_queue[chat_id].qsize()

    if queue_size > 0:
        if queue_size >= MAX_WAITING_QUEUE:
            send_message(chat_id, f"⚠️ 等待队列已满（{queue_size}/{MAX_WAITING_QUEUE}），请稍后再试", get_token())
        else:
            _waiting_queue[chat_id].put((text, message_id, chat_type))
            send_message(chat_id, f"⏳ 已加入队列({queue_size+1}/{MAX_WAITING_QUEUE})，空闲时自动处理", get_token())
        return

    if not main.busy and main.queue.empty():
        logger.info(f"[{chat_id[:8]}...] 分发→主会话")
        _dispatch_to_session(chat_id, text, message_id, chat_type, "main")
        return

    logger.info(f"[{chat_id[:8]}...] 主会话忙，检查并行")
    with _global_lock:
        for ps in list(_parallel_sessions.values()):
            if ps.parent_chat_id == chat_id and ps.busy and ps.thread and not ps.thread.is_alive():
                ps.busy = False
                ps.thread = None
        idle_parallel = None
        for ps in _parallel_sessions.values():
            if ps.parent_chat_id == chat_id and not ps.busy and ps.queue.empty():
                idle_parallel = ps
                break

    if idle_parallel:
        logger.info(f"[{chat_id[:8]}...] 分发→并行会话 {idle_parallel.session_id[:8]}")
        _dispatch_to_session(chat_id, text, message_id, chat_type, "parallel")
    elif len(_parallel_sessions) < MAX_PARALLEL_SESSIONS:
        new_sid = str(uuid.uuid4())
        parallel = ParallelSession(session_id=new_sid, parent_chat_id=chat_id)
        with _global_lock:
            _parallel_sessions[new_sid] = parallel
        logger.info(f"[{chat_id[:8]}...] 创建新并行会话 {new_sid[:8]}")
        _dispatch_to_session(chat_id, text, message_id, chat_type, "parallel")
    else:
        if queue_size >= MAX_WAITING_QUEUE:
            send_message(chat_id, f"⚠️ 等待队列已满，请稍后再试", get_token())
        else:
            _waiting_queue[chat_id].put((text, message_id, chat_type))
            send_message(chat_id, f"⏳ 已加入队列({queue_size+1}/{MAX_WAITING_QUEUE})", get_token())


def _flush_pending_attachment(chat_id: str, extra_text: str = "") -> None:
    """把挂起的文件消息（可选合并新文本）真正分发出去。
    extra_text 非空表示用户在窗口内补充了说明。
    """
    with _pending_lock:
        pending = _pending_attachments.pop(chat_id, None)
    if not pending:
        return
    # 取消定时器
    timer = pending.get("timer")
    if timer:
        try:
            timer.cancel()
        except Exception:
            pass

    paths = pending["paths"]
    names = pending["names"]
    message_id = pending["message_id"]
    chat_type = pending["chat_type"]
    status_msg_id = pending.get("status_msg_id")
    workspace = pending.get("workspace")

    # 构造合并后的 prompt
    file_desc = "\n".join(f"- {n} -> {p}" for n, p in zip(names, paths))
    if extra_text.strip():
        synthetic_text = f"{extra_text}\n\n附带文件:\n{file_desc}"
    else:
        synthetic_text = f"用户发送了 {len(paths)} 个文件，请读取并分析内容:\n{file_desc}"

    # 定格状态卡片：不再用"思考中"（会跟会话线程的实时卡冲突），改成接收确认的终态文案
    if status_msg_id:
        try:
            preview = "、".join(names[:3])
            if len(names) > 3:
                preview += f" 等 {len(names)} 个文件"
            update_card_message(status_msg_id, f"📎 已接收文件：{preview}，开始处理...", get_token(), workspace=workspace)
        except Exception:
            pass

    main = _main_sessions.get(chat_id)
    if main:
        main.log_message("user", synthetic_text[:200], session_tag="file-merged" if extra_text.strip() else "file-timeout")

    _route_message(chat_id, synthetic_text, message_id, chat_type)
    return


def _schedule_pending_timeout(chat_id: str) -> None:
    """窗口到期未等到文本，按"仅文件"模式分发"""
    logger.info(f"[{chat_id[:8]}...] 文件合并窗口超时 ({PENDING_MERGE_SECONDS}s)，按无说明分发")
    _flush_pending_attachment(chat_id, extra_text="")


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
            # 如果距上次重连不超过 5 分钟，说明此消息可能是断线期间积压的，提示用户
            recently_reconnected = _ws_reconnect_time > 0 and (time.time() - _ws_reconnect_time < 300)
            initial_text = "🔄 刚刚重启，正在处理你的消息..." if recently_reconnected else "🤔 思考中..."
            status_res = send_card_message(main.chat_id, initial_text, get_token(), workspace=main.workspace)
            status_msg_id = status_res.get("data", {}).get("message_id", "")

            # 使用持久客户端
            pc = main.get_persistent_client()

            done = [False]  # 防止心跳覆盖结果卡片
            elapsed = [0]
            def on_heartbeat():
                if done[0]:
                    return
                elapsed[0] += 30
                # 检测 Claude 子进程是否存活
                if not pc.check_alive():
                    done[0] = True
                    main.persistent_client = None
                    try:
                        if status_msg_id:
                            update_card_message(
                                status_msg_id,
                                "❌ Claude 进程异常终止，请简化任务后重试。",
                                get_token(),
                                workspace=main.workspace,
                            )
                        else:
                            send_message(
                                main.chat_id,
                                "❌ Claude 进程异常终止，请简化任务后重试。",
                                get_token(),
                            )
                    except Exception:
                        pass
                    logger.error(f"[主会话] Claude 进程已死亡 [{main.chat_id[:8]}...]")
                    return
                if status_msg_id:
                    update_card_message(
                        status_msg_id,
                        f"⏳ 处理中…（已运行 {elapsed[0]} 秒）",
                        get_token(),
                        workspace=main.workspace,
                    )

            def on_reconnect():
                """Claude 子进程崩溃，PersistentClient 正在重连时回调"""
                if done[0]:
                    return
                try:
                    if status_msg_id:
                        update_card_message(
                            status_msg_id,
                            "🔄 Claude 连接中断，正在重连...",
                            get_token(),
                            workspace=main.workspace,
                        )
                except Exception:
                    pass

            message_with_hint = (
                message +
                "\n\n【注意】如果你创建、生成或读取了文件，请在回复末尾明确写出"
                "文件的完整路径，格式为：文件路径: /full/path/to/file.ext\n"
                "如果用户要求发送某个文件但你没有直接上传，请在回复中提到该文件的完整路径。"
            )

            reply, new_session_id, tool_calls = pc.chat_sync(message_with_hint, on_heartbeat=on_heartbeat, on_reconnect=on_reconnect)
            done[0] = True

            # session 含 thinking 签名等 API 400 错误 → 重置 session 重试一次
            if reply.startswith("API Error:") and ("thinking" in reply or "400" in reply):
                logger.warning(f"[主会话] API 错误，重置 session 重试: {reply[:100]}")
                if status_msg_id:
                    update_card_message(status_msg_id, "🔄 会话数据异常，正在重置并重试...", get_token(), workspace=main.workspace)
                pc.session_id = None
                main.session_id = ""
                save_session(main.chat_id, "")
                pc.disconnect()
                reply, new_session_id, tool_calls = pc.chat_sync(message_with_hint, on_heartbeat=on_heartbeat, on_reconnect=on_reconnect)

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

            # 上传回复中提到的文件
            try:
                _send_files_after_reply(main.chat_id, reply, get_token(), tool_calls, main.workspace)
            except Exception as e:
                logger.error(f"文件上传失败: {e}")

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

            message_with_hint = (
                message +
                "\n\n【注意】如果你创建、生成或读取了文件，请在回复末尾明确写出"
                "文件的完整路径，格式为：文件路径: /full/path/to/file.ext\n"
                "如果用户要求发送某个文件但你没有直接上传，请在回复中提到该文件的完整路径。"
            )

            reply, new_session_id, tool_calls = chat_sync(message_with_hint, session_id=parallel.session_id, cwd=parent_workspace)
            if new_session_id != parallel.session_id:
                parallel.session_id = new_session_id

            if status_msg_id:
                update_card_message(status_msg_id, reply, get_token(), workspace=parent_workspace)
            else:
                if chat_type == "group":
                    reply_message(message_id, reply)
                else:
                    send_message(parallel.parent_chat_id, reply, get_token())

            # 上传回复中提到的文件
            try:
                _send_files_after_reply(parallel.parent_chat_id, reply, get_token(), tool_calls, parent_workspace)
            except Exception as e:
                logger.error(f"文件上传失败: {e}")

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
    # 没有等待消息，先同步 busy 状态，再检查关闭
    for ps in list(_parallel_sessions.values()):
        if ps.parent_chat_id == chat_id:
            _sync_session_busy(ps)
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
    global _shutting_down
    try:
        event = data.event
        message = event.message
        message_id = message.message_id
        chat_id = message.chat_id

        # 消息去重：已处理过的 message_id 直接忽略
        with _processed_messages_lock:
            if message_id in _processed_messages:
                logger.info(f"[{chat_id[:8]}...] 消息 {message_id[:8]}... 已处理过，忽略重复")
                return
            _processed_messages.add(message_id)
            # 限制集合大小，避免内存泄漏：只删最老的 500 条
            if len(_processed_messages) > 1000:
                to_remove = list(_processed_messages)[:500]
                for old_id in to_remove:
                    _processed_messages.discard(old_id)
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
            if _shutting_down:
                send_message(chat_id, "⚠️ 服务正在关闭，请稍后再试", get_token())
                return
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
                download_file_message(message_id, file_key, save_path, get_token(), res_type=("image" if message_type == "image" else "file"))
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
                # 文件消息自带说明文字 → 直接正常分发，无需等待
                synthetic_text = f"{text}\n\n文件路径: {save_path}"
                main.log_message("user", f"[{message_type}] {safe_name} -> {save_path}", session_tag="file")
                if status_msg_id:
                    update_card_message(status_msg_id, f"📎 已接收文件：{safe_name}，开始处理...", get_token(), workspace=workspace)
                _route_message(chat_id, synthetic_text, message_id, chat_type)
                return

            # 文件消息无说明 → 进入 45s 合并窗口，等用户补充文字
            main.log_message("user", f"[{message_type}] {safe_name} -> {save_path}", session_tag="file-pending")
            with _pending_lock:
                existing = _pending_attachments.get(chat_id)
                if existing:
                    # 同一 chat 连发多个文件 → 累加，重置计时器
                    try:
                        existing["timer"].cancel()
                    except Exception:
                        pass
                    existing["paths"].append(save_path)
                    existing["names"].append(safe_name)
                    existing["message_id"] = message_id
                    existing["chat_type"] = chat_type
                    existing["status_msg_id"] = status_msg_id or existing.get("status_msg_id")
                    new_timer = threading.Timer(PENDING_MERGE_SECONDS, _schedule_pending_timeout, args=(chat_id,))
                    new_timer.daemon = True
                    existing["timer"] = new_timer
                    new_timer.start()
                else:
                    timer = threading.Timer(PENDING_MERGE_SECONDS, _schedule_pending_timeout, args=(chat_id,))
                    timer.daemon = True
                    _pending_attachments[chat_id] = {
                        "paths": [save_path],
                        "names": [safe_name],
                        "message_id": message_id,
                        "chat_type": chat_type,
                        "status_msg_id": status_msg_id,
                        "workspace": workspace,
                        "timer": timer,
                    }
                    timer.start()

            if status_msg_id:
                update_card_message(
                    status_msg_id,
                    f"📎 已收到文件 `{safe_name}`，等待你的说明（{PENDING_MERGE_SECONDS}s 内未补充将直接分析）",
                    get_token(),
                    workspace=workspace,
                )
            return

        # ========== 纯文本消息 ==========
        if not text:
            return

        # ========== 处理文件选择回复 ==========
        with _file_selection_lock:
            is_selection = chat_id in _file_selection_state
        if is_selection:
            handled = _handle_file_selection_reply(chat_id, text)
            if handled:
                return
            # 如果 _handle_file_selection_reply 返回 False，继续正常转发

        # ========== 处理命令 ==========
        if text in ("/cancel", "/取消任务"):
            count = _cancel_all_tasks(chat_id)
            if count > 0:
                send_message(chat_id, f"✅ 已取消 {count} 个正在处理的任务", get_token())
                logger.info(f"[{chat_id[:8]}...] 取消了 {count} 个任务")
            else:
                send_message(chat_id, "ℹ️ 当前没有正在处理的任务", get_token())
            return

        if text in ("/restart", "/重启"):
            stats = shutdown()
            msg = (
                f"✅ 服务已重置\n"
                f"取消主会话任务: {stats['cancelled_main']}\n"
                f"取消并行任务: {stats['cancelled_parallel']}\n"
                f"清空等待队列: {stats['cleared_waiting']}"
            )
            send_message(chat_id, msg, get_token())
            logger.info(f"[{chat_id[:8]}...] 服务重置: {stats}")
            return

        if text in ("/shutdown", "/关闭服务"):
            _shutting_down = True
            stats = shutdown()
            msg = (
                f"🔴 服务已关闭\n"
                f"取消主会话任务: {stats['cancelled_main']}\n"
                f"取消并行任务: {stats['cancelled_parallel']}\n"
                f"清空等待队列: {stats['cleared_waiting']}"
            )
            send_message(chat_id, msg, get_token())
            logger.info(f"[{chat_id[:8]}...] 服务关闭: {stats}")
            import sys
            sys.exit(0)

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

            # 更新已存在的主会话，并重建 PersistentClient（cwd 已变）
            with _global_lock:
                if chat_id in _main_sessions:
                    main = _main_sessions[chat_id]
                    main.workspace = workspace_path
                    if main.persistent_client:
                        main.persistent_client.disconnect()
                        main.persistent_client = None

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

        # ========== Plan A: 第一层文件索取检测 ==========
        if _is_file_request(text):
            filename = _extract_requested_filename(text)
            if filename:
                with _global_lock:
                    if chat_id not in _main_sessions:
                        session_id = get_session(chat_id) or ""
                        ws = get_workspace(chat_id) or ""
                        main = MainSession(chat_id=chat_id, session_id=session_id, workspace=ws)
                        _main_sessions[chat_id] = main
                        if chat_id not in _waiting_queue:
                            _waiting_queue[chat_id] = Queue()

                main = _main_sessions[chat_id]
                workspace = main.workspace

                candidates = _search_workspace_for_file(filename, workspace)
                if not candidates:
                    logger.info(f"[{chat_id[:8]}...] 文件索取但未找到: {filename}，转 Claude")
                elif len(candidates) == 1:
                    target = candidates[0]
                    file_name = os.path.basename(target)
                    send_message(chat_id, f"📎 找到文件：{file_name}", get_token())
                    result = upload_file_to_feishu(target, get_token(), timeout=120, file_name=file_name)
                    if result.get("success"):
                        send_file_message(chat_id, result["file_key"], file_name, get_token())
                        send_message(chat_id, f"✅ 已发送：{file_name}", get_token())
                        logger.info(f"[{chat_id[:8]}...] 第一层直接上传: {file_name}")
                    else:
                        send_message(chat_id, f"⚠️ 上传失败：{result.get('error', '未知错误')}", get_token())
                    return
                else:
                    logger.info(f"[{chat_id[:8]}...] 文件索取匹配 {len(candidates)} 个，发送选择卡片")
                    _send_file_selection_card(chat_id, candidates, text, get_token(), workspace)
                    return
            else:
                logger.info(f"[{chat_id[:8]}...] 识别为文件索取但未提取到文件名，转 Claude")

        # ========== 正常消息分发 ==========
        if _shutting_down:
            send_message(chat_id, "⚠️ 服务正在关闭，请稍后再试", get_token())
            return

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

        # 如果该 chat 有挂起的文件消息 → 用当前文本作为说明，立即合并分发
        with _pending_lock:
            has_pending = chat_id in _pending_attachments
        if has_pending:
            logger.info(f"[{chat_id[:8]}...] 命中文件合并窗口，合并当前文本分发")
            _flush_pending_attachment(chat_id, extra_text=text)
            return

        # ========== 分发逻辑 ==========
        logger.info(f"[{chat_id[:8]}...] 分发检查: busy={main.busy}, queue_empty={main.queue.empty()}, waiting_size={_waiting_queue[chat_id].qsize()}")
        _route_message(chat_id, text, message_id, chat_type)

    except Exception as e:
        logger.error(f"处理消息失败: {e}")


def _start_watchdog(hang_timeout: int = 600) -> None:
    """启动 watchdog 守护线程。

    连接断开后超过 hang_timeout 秒仍未重连（说明 SDK 卡死），
    通知飞书后强制以 exit code=1 退出进程，由 Task Scheduler 自动重启。
    """
    def _loop():
        while True:
            time.sleep(60)
            with _ws_lock:
                since = _ws_disconnect_since
            if since is None:
                continue
            elapsed = time.time() - since
            if elapsed > hang_timeout:
                logger.error(f"[watchdog] WebSocket 已断线 {elapsed:.0f}s 未重连，强制重启进程")
                try:
                    notify_all_chats("🔄 检测到长时间断线，正在自动重启...")
                except Exception:
                    pass
                time.sleep(3)
                os._exit(1)  # 强制杀死整个进程（含卡死的主线程），由外部守护重启

    t = threading.Thread(target=_loop, daemon=True, name="watchdog")
    t.start()
    logger.info(f"[watchdog] 已启动，断线超过 {hang_timeout}s 将强制重启")


def _write_pid_file():
    """将当前进程 PID 写入 .pid 文件，供 stop.bat / restart.bat 使用。"""
    pid_file = Path(__file__).parent.parent / ".pid"
    try:
        pid_file.write_text(str(os.getpid()), encoding='utf-8')
    except Exception as e:
        logger.error(f"写入 PID 文件失败: {e}")


def _check_duplicate_instance() -> bool:
    """检查是否已有实例在运行。返回 True 表示重复。"""
    pid_file = Path(__file__).parent.parent / ".pid"
    if not pid_file.exists():
        return False
    try:
        old_pid = int(pid_file.read_text(encoding='utf-8').strip())
        # 检查该 PID 是否存活
        import subprocess
        out = subprocess.check_output(
            ["tasklist", "/FI", f"PID eq {old_pid}", "/FO", "CSV", "/NH"],
            text=True, creationflags=subprocess.CREATE_NO_WINDOW, timeout=5,
        )
        # tasklist 找不到 PID 时会返回 "INFO: No tasks running with the specified criteria."
        # 只有真正找到进程才返回 CSV 行
        if "No tasks" in out or not out.strip():
            return False
        # 确认是 python 进程
        if "python" in out.lower():
            return True
        return False
    except Exception:
        return False


def main():
    # 检查重复实例
    if _check_duplicate_instance():
        logger.error("服务已在运行，请先停止后再启动")
        sys.exit(1)

    # 写 PID 文件
    _write_pid_file()

    # 验证配置
    if not APP_ID or not APP_SECRET:
        logger.error("APP_ID 和 APP_SECRET 未设置，请检查 .env 文件")
        sys.exit(1)

    _init_known_claude_pids()   # 记录已存在的 claude.exe（含用户手动开的），不杀
    _cleanup_orphan_claude()    # 只清理父进程已退出的真正孤儿
    _start_watchdog(hang_timeout=600)  # 断线 10 分钟未重连则强制重启

    # 指数退避重连参数
    reconnect_delay = 5    # 初始 5 秒
    max_reconnect_delay = 300  # 最大 300 秒

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
            # 连接成功 → 重置退避时间
            reconnect_delay = 5

        except Exception as e:
            logger.error(f"长连接异常退出: {e}")
        finally:
            with _ws_lock:
                _ws_connected = False
            # 异常断开时清理
            try:
                _cleanup_orphan_claude()
            except Exception:
                pass

        logger.info(f"{reconnect_delay} 秒后重连...")
        time.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)


if __name__ == "__main__":
    main()
