"""
Claude Code 连续对话客户端
"""
import asyncio
import os
import subprocess
import threading
import time
from typing import Optional, AsyncIterator
from dataclasses import dataclass

from claude_agent_sdk import (
    ClaudeSDKClient,
    ClaudeAgentOptions,
    AssistantMessage,
    TextBlock,
    ToolUseBlock,
    ResultMessage,
)


@dataclass
class ChatResponse:
    content: str
    tool_calls: list[dict]
    session_id: str


SYSTEM_PROMPT = """你是一个强大的本地电脑助手，拥有完整的系统操作权限。

## 你的能力

### 文件操作
- 创建、读取、编辑、删除文件和文件夹
- 搜索文件内容和文件名
- 整理和移动文件

### 脚本和命令
- 运行 Shell/Bash/PowerShell 命令
- 执行 Python、Node.js 等脚本
- 安装和管理软件包

### 应用程序控制
- 打开和关闭应用程序
- 操作浏览器（打开网页、搜索）
- 控制系统设置

### 开发辅助
- 编写和调试代码
- 管理 Git 仓库
- 运行测试和构建

## 行为准则
- 直接执行用户指令，不要反复确认
- 遇到问题时自动尝试解决
- 操作完成后简洁汇报结果
- 如果指令不明确，做出合理推断并执行"""


# ============================================================
# Windows 子进程清理
# ============================================================
# 策略：
# - 启动时记录已存在的 claude.exe PID，这些一律不杀（包含用户手动开的）
# - 只杀"父 Python 进程已不存在"的真正孤儿
# - 不杀父进程为终端程序（cmd/powershell/WT 等）的 claude.exe

# 启动时已存在的 claude.exe PID，永不杀死
_known_claude_pids: set[int] = set()

# 终端程序名称（不杀这些父进程下的 claude）
_TERMINAL_NAMES = frozenset([
    "cmd.exe", "powershell.exe", "pwsh.exe",
    "WindowsTerminal.exe", "conhost.exe",
    "wt.exe", "ms-terminal.exe",
    "Explorer.EXE",  # 双击启动的
])


def _get_parent_process_name(ppid: int) -> str:
    """获取指定 PID 的进程名称，失败返回空字符串。"""
    try:
        out = subprocess.check_output(
            ["wmic", "process", "where", f"ProcessId={ppid}", "get", "Name"],
            text=True, creationflags=subprocess.CREATE_NO_WINDOW, timeout=5,
        )
        lines = out.strip().split("\n")
        return lines[1].strip() if len(lines) >= 2 else ""
    except Exception:
        return ""


def _init_known_claude_pids() -> None:
    """启动时记录已有的 claude.exe PID，防止误杀用户手动开的会话。"""
    global _known_claude_pids
    _known_claude_pids.clear()
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq claude.exe", "/FO", "CSV", "/NH"],
            text=True, creationflags=subprocess.CREATE_NO_WINDOW, timeout=5,
        )
        for line in out.strip().split("\n"):
            if not line.strip():
                continue
            parts = [p.strip().strip('"') for p in line.split(",")]
            if len(parts) >= 2:
                try:
                    _known_claude_pids.add(int(parts[1]))
                except ValueError:
                    pass
    except Exception:
        pass


def register_claude_pid(pid: int) -> None:
    """注册由本 bot 创建的 claude.exe PID。"""
    _known_claude_pids.add(pid)


def _cleanup_orphan_claude() -> None:
    """扫描所有 claude.exe 进程，只杀死父 Python 进程已退出的真正孤儿。

    以下情况不杀：
    - 启动时已存在的 PID（含用户手动开的）
    - 父进程是终端程序
    - 父进程仍在运行
    """
    if os.name != "nt":
        return
    try:
        # 获取所有 python/pythonw 进程的 PID
        py_out = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV", "/NH"],
            text=True, creationflags=subprocess.CREATE_NO_WINDOW, timeout=5,
        )
        pyw_out = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq pythonw.exe", "/FO", "CSV", "/NH"],
            text=True, creationflags=subprocess.CREATE_NO_WINDOW, timeout=5,
        )
        py3_out = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq python3.12.exe", "/FO", "CSV", "/NH"],
            text=True, creationflags=subprocess.CREATE_NO_WINDOW, timeout=5,
        )
        pyw3_out = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq pythonw3.12.exe", "/FO", "CSV", "/NH"],
            text=True, creationflags=subprocess.CREATE_NO_WINDOW, timeout=5,
        )

        parent_pids: set[int] = set()
        for output in [py_out, pyw_out, py3_out, pyw3_out]:
            for line in output.strip().split("\n"):
                if not line.strip():
                    continue
                parts = [p.strip().strip('"') for p in line.split(",")]
                if len(parts) >= 2:
                    try:
                        parent_pids.add(int(parts[1]))
                    except ValueError:
                        pass

        # 获取所有 claude.exe 进程及其 PPID
        cl_out = subprocess.check_output(
            ["wmic", "process", "where", "name='claude.exe'", "get", "ProcessId,ParentProcessId"],
            text=True, creationflags=subprocess.CREATE_NO_WINDOW, timeout=5,
        )
        for line in cl_out.strip().split("\n")[1:]:
            parts = line.split()
            if len(parts) >= 2:
                try:
                    claude_pid = int(parts[0])
                    claude_ppid = int(parts[1])

                    # 启动时已存在的 → 不杀
                    if claude_pid in _known_claude_pids:
                        continue

                    # 父进程是终端程序 → 不杀
                    pname = _get_parent_process_name(claude_ppid)
                    if pname in _TERMINAL_NAMES:
                        continue

                    # 父进程仍在运行 → 不杀
                    if claude_ppid in parent_pids:
                        continue

                    # 父进程已不存在（或为 0/4 等系统进程） → 真正的孤儿，杀
                    if claude_ppid == 0 or claude_ppid == 4 or pname == "":
                        _kill_process_tree_windows(claude_pid)
                except ValueError:
                    pass
    except Exception:
        pass


class ConversationClient:
    """
    连续对话客户端

    Example:
        async with ConversationClient() as client:
            r1 = await client.chat("创建 hello.py")
            r2 = await client.chat("读取刚才的文件")
            print(client.session_id)

        # 恢复对话
        async with ConversationClient(session_id="xxx") as client:
            r = await client.chat("继续")

        # 指定工作目录
        async with ConversationClient(cwd="/path/to/project") as client:
            r = await client.chat("在这个目录下工作")
    """

    def __init__(
        self,
        session_id: str = None,
        allowed_tools: list[str] = None,
        permission_mode: str = "bypassPermissions",
        system_prompt: str = None,
        cwd: str = None,
    ):
        self._initial_session_id = session_id
        self.session_id: Optional[str] = session_id
        self.allowed_tools = allowed_tools or ["Read", "Write", "Edit", "Bash", "Glob", "Grep"]
        self.permission_mode = permission_mode
        self.system_prompt = system_prompt or SYSTEM_PROMPT
        self.cwd = cwd
        self._client: Optional[ClaudeSDKClient] = None

    async def connect(self):
        stderr_capture: list[str] = []
        def _stderr_cb(msg: str) -> None:
            stderr_capture.append(msg)
        options = ClaudeAgentOptions(
            resume=self._initial_session_id,
            allowed_tools=self.allowed_tools,
            permission_mode=self.permission_mode,
            system_prompt={"type": "preset", "preset": "claude_code", "append": self.system_prompt},
            cwd=self.cwd,
            stderr=_stderr_cb,
        )
        self._client = ClaudeSDKClient(options=options)
        try:
            await self._client.connect()
        except Exception:
            stderr_text = "".join(stderr_capture)
            if "No conversation found" in stderr_text or "terminated" in stderr_text.lower():
                self._initial_session_id = None
                self.session_id = None
                options.resume = None
                self._client = ClaudeSDKClient(options=options)
                await self._client.connect()
            else:
                raise

    async def chat(self, message: str) -> ChatResponse:
        """发送消息"""
        if not self._client:
            await self.connect()

        await self._client.query(message)

        response_text = []
        tool_calls = []

        async for msg in self._client.receive_response():
            if isinstance(msg, AssistantMessage):
                for block in msg.content:
                    if isinstance(block, TextBlock):
                        response_text.append(block.text)
                    elif isinstance(block, ToolUseBlock):
                        tool_calls.append({
                            "id": block.id,
                            "name": block.name,
                            "input": block.input,
                        })
            elif isinstance(msg, ResultMessage):
                self.session_id = msg.session_id

        return ChatResponse(
            content="\n".join(response_text),
            tool_calls=tool_calls,
            session_id=self.session_id or "",
        )

    async def disconnect(self):
        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                pass  # SDK 的 anyio/asyncio 兼容性问题，忽略
            self._client = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


def chat_sync(message: str, session_id: str = None, cwd: str = None) -> tuple[str, str, list[dict]]:
    """
    同步调用 Claude Code（在独立线程中运行，避免事件循环冲突）

    防泄露策略：
    - 每次调用前扫描并杀死无主的 claude.exe 残留进程
    - 第一次重试用较短超时（300s），第二次用 600s
    - 异常退出时强制杀死子进程树
    """
    import concurrent.futures

    # 调用前清理无主 claude.exe
    _cleanup_orphan_claude()

    def _run_in_thread(sid: str = None) -> tuple[str, str, list[dict]]:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async def _chat():
                async with ConversationClient(session_id=sid, cwd=cwd) as client:
                    r = await client.chat(message)
                    return r.content, r.session_id, r.tool_calls
            return loop.run_until_complete(_chat())
        except Exception:
            # 异常退出 → 可能留下子进程，强制清理
            # 注意：这里拿不到 PID，但 _cleanup_orphan_claude 下次调用会处理
            raise
        finally:
            loop.close()

    # 第一次尝试用传入的 session_id
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(_run_in_thread, session_id)
            return future.result(timeout=300)  # 重试路径用较短超时
    except Exception:
        # session_id 无效/不存在，清空后重试
        session_id = None

    # 第二次尝试：创建新会话
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(_run_in_thread, None)
        return future.result(timeout=600)


# ============================================================
# 持久客户端：保持连接，空闲超时自动断开
# ============================================================

class PersistentClient:
    """
    持久化的 Claude Code 客户端，带空闲超时自动断开。

    - 保持一个常驻事件循环和线程
    - 首次调用时 connect，后续消息复用同一进程
    - 超过 idle_timeout 秒无新消息，自动 disconnect
    - 下次消息到来时自动重连
    """

    def __init__(
        self,
        session_id: str = None,
        cwd: str = None,
        idle_timeout: int = 1920,
        allowed_tools: list[str] = None,
        permission_mode: str = "bypassPermissions",
        system_prompt: str = None,
        on_session_changed: Optional[callable] = None,
    ):
        self.session_id = session_id
        self.cwd = cwd
        self.idle_timeout = idle_timeout
        self.allowed_tools = allowed_tools or ["Read", "Write", "Edit", "Bash", "Glob", "Grep"]
        self.permission_mode = permission_mode
        self.system_prompt = system_prompt or SYSTEM_PROMPT
        self.on_session_changed = on_session_changed

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._client: Optional[ClaudeSDKClient] = None
        self._lock = threading.RLock()  # RLock 允许同一线程重复获取
        self._idle_timer: Optional[threading.Timer] = None
        self._connected = False
        self._in_use = False  # 标记是否有 chat 操作正在进行

    def _ensure_loop(self):
        """确保事件循环存在"""
        if self._loop is not None:
            return
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()

    def _reset_idle_timer(self):
        """重置空闲计时器"""
        if self._idle_timer:
            self._idle_timer.cancel()

    def _start_idle_timer(self):
        """启动空闲超时计时器"""
        self._idle_timer = threading.Timer(self.idle_timeout, self._idle_disconnect)
        self._idle_timer.daemon = True
        self._idle_timer.start()

    def _kill_process_tree(self):
        """通过 SDK transport 拿到真实 PID，强制杀死整个进程树。"""
        with self._lock:
            client = self._client
        if client is None:
            return
        transport = getattr(client, "_transport", None)
        if transport is None:
            return
        proc = getattr(transport, "_process", None)
        if proc is None:
            return
        try:
            pid = proc.pid
            _kill_process_tree_windows(pid)
        except Exception:
            pass

    def _idle_disconnect(self):
        """空闲超时断开 — 仅在无操作进行时才断开。"""
        with self._lock:
            # 有 chat 正在进行 → 不断开
            if self._in_use:
                return
            if not self._connected or self._client is None:
                return
            client = self._client
            loop = self._loop
            self._client = None
            self._connected = False

        try:
            coro = client.disconnect()
            asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=10)
        except Exception:
            pass
        self._kill_process_tree()

    def connect(self):
        """连接客户端，旧 session 过期时自动重连"""
        with self._lock:
            if self._connected:
                return
            self._ensure_loop()
        self._connect_inner()

    def _connect_inner(self, retry: bool = False):
        """实际连接逻辑，支持一次重试"""
        stderr_capture: list[str] = []

        def _stderr_cb(msg: str) -> None:
            stderr_capture.append(msg)

        try:
            with self._lock:
                options = ClaudeAgentOptions(
                    resume=self.session_id,
                    allowed_tools=self.allowed_tools,
                    permission_mode=self.permission_mode,
                    system_prompt={"type": "preset", "preset": "claude_code", "append": self.system_prompt},
                    cwd=self.cwd,
                    stderr=_stderr_cb,
                )
                client = ClaudeSDKClient(options=options)
                future = asyncio.run_coroutine_threadsafe(client.connect(), self._loop)
                future.result(timeout=30)
                with self._lock:
                    self._client = client
                    self._connected = True
        except Exception:
            stderr_text = "".join(stderr_capture)
            if not retry and ("No conversation found" in stderr_text or "terminated" in stderr_text.lower()):
                # 旧 session 过期/不存在 → 清除后重试
                with self._lock:
                    self.session_id = None
                if self.on_session_changed:
                    try:
                        self.on_session_changed(None)
                    except Exception:
                        pass
                self._connect_inner(retry=True)
            else:
                raise

    def chat_sync(self, message: str, on_heartbeat: Optional[callable] = None) -> tuple[str, str, list[dict]]:
        """发送消息并等待回复，session 失效时自动重试"""
        if not self._connected:
            self.connect()

        self._reset_idle_timer()

        # 标记为使用中，阻止 idle_disconnect
        with self._lock:
            self._in_use = True

        self._heartbeat_timer: Optional[threading.Timer] = None
        if on_heartbeat:
            def _tick():
                next_timer = threading.Timer(30, _tick)
                next_timer.daemon = True
                next_timer.start()
                self._heartbeat_timer = next_timer
                on_heartbeat()
            self._heartbeat_timer = threading.Timer(30, _tick)
            self._heartbeat_timer.daemon = True
            self._heartbeat_timer.start()

        try:
            result = self._do_chat_sync(message)
            self._start_idle_timer()
            return result
        except Exception as e:
            error_msg = str(e)
            if (error_msg.startswith("Cannot write to terminated process") or
                    "No conversation found" in error_msg or
                    "Command failed" in error_msg or
                    "timeout" in error_msg.lower() or
                    "terminated" in error_msg.lower()):
                self.disconnect()
                self.session_id = None
                if self.on_session_changed:
                    try:
                        self.on_session_changed(None)
                    except Exception:
                        pass
                time.sleep(2)
                self.connect()
                result = self._do_chat_sync(message)
                self._start_idle_timer()
                return result
            raise
        finally:
            if self._heartbeat_timer:
                self._heartbeat_timer.cancel()
                self._heartbeat_timer = None
            with self._lock:
                self._in_use = False

    def _do_chat_sync(self, message: str) -> tuple[str, str, list[dict]]:
        """实际执行聊天（无重试），返回 (回复文本, session_id, tool_calls)"""
        with self._lock:
            client = self._client
            loop = self._loop
            old_session_id = self.session_id

        async def _do_chat():
            await client.query(message)
            response_text = []
            tool_calls = []
            async for msg in client.receive_response():
                if isinstance(msg, AssistantMessage):
                    for block in msg.content:
                        if isinstance(block, TextBlock):
                            response_text.append(block.text)
                        elif isinstance(block, ToolUseBlock):
                            tool_calls.append({
                                "id": block.id,
                                "name": block.name,
                                "input": block.input,
                            })
                elif isinstance(msg, ResultMessage):
                    self.session_id = msg.session_id
            return "\n".join(response_text), self.session_id or "", tool_calls

        future = asyncio.run_coroutine_threadsafe(_do_chat(), loop)
        content, new_sid, tool_calls = future.result(timeout=600)

        # session_id 变更时通知调用方（持久化到 DB）
        if new_sid and new_sid != old_session_id and self.on_session_changed:
            try:
                self.on_session_changed(new_sid)
            except Exception:
                pass

        return content, new_sid, tool_calls

    def check_alive(self) -> bool:
        """检测底层 Claude 子进程是否存活。

        优先级：SDK transport 内部 _process → 按 session_id 匹配 claude.exe 进程。
        线程安全，可在心跳回调中调用。
        """
        with self._lock:
            if not self._connected or self._client is None:
                return False
            client = self._client

        # 方式1：检查 SDK transport 内部 subprocess
        transport = getattr(client, "_transport", None)
        if transport is not None:
            proc = getattr(transport, "_process", None)
            if proc is not None:
                try:
                    ret = proc.poll()
                    return ret is None  # None = 仍在运行
                except Exception:
                    pass

        # 方式2：按 session_id 模糊匹配系统进程
        if self.session_id:
            try:
                out = subprocess.check_output(
                    ["tasklist", "/FI", "IMAGENAME eq claude.exe", "/FO", "CSV", "/NH"],
                    text=True, creationflags=subprocess.CREATE_NO_WINDOW,
                )
                return self.session_id[:8] in out
            except Exception:
                pass

        return True  # 无法检测时保守返回 True

    @property
    def client(self):
        """返回底层 SDK 客户端引用（只读，供外部做细粒度检查）"""
        with self._lock:
            return self._client

    def disconnect(self):
        """主动断开连接"""
        if self._idle_timer:
            self._idle_timer.cancel()
            self._idle_timer = None
        with self._lock:
            if not self._connected or self._client is None:
                return
            client = self._client
            self._client = None
            self._connected = False
            self._in_use = False  # 重置使用状态
        try:
            asyncio.run_coroutine_threadsafe(client.disconnect(), self._loop).result(timeout=10)
        except Exception:
            pass
        self._kill_process_tree()
