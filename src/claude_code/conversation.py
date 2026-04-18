"""
Claude Code 连续对话客户端
"""
import asyncio
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
        permission_mode: str = "acceptEdits",
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
        options = ClaudeAgentOptions(
            resume=self._initial_session_id,
            allowed_tools=self.allowed_tools,
            permission_mode=self.permission_mode,
            system_prompt={"type": "preset", "preset": "claude_code", "append": self.system_prompt},
            cwd=self.cwd,
        )
        self._client = ClaudeSDKClient(options=options)
        await self._client.connect()

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


def chat_sync(message: str, session_id: str = None, cwd: str = None) -> tuple[str, str]:
    """
    同步调用 Claude Code（在独立线程中运行，避免事件循环冲突）

    Args:
        message: 用户消息
        session_id: 恢复之前的会话（可选）
        cwd: 工作目录路径（可选）

    Returns:
        (回复内容, session_id)
    """
    import concurrent.futures

    def _run_in_thread(sid: str = None) -> tuple[str, str]:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async def _chat():
                async with ConversationClient(session_id=sid, cwd=cwd) as client:
                    r = await client.chat(message)
                    return r.content, r.session_id
            return loop.run_until_complete(_chat())
        finally:
            loop.close()

    # 第一次尝试用传入的 session_id
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(_run_in_thread, session_id)
            return future.result(timeout=600)
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
        permission_mode: str = "acceptEdits",
        system_prompt: str = None,
    ):
        self.session_id = session_id
        self.cwd = cwd
        self.idle_timeout = idle_timeout
        self.allowed_tools = allowed_tools or ["Read", "Write", "Edit", "Bash", "Glob", "Grep"]
        self.permission_mode = permission_mode
        self.system_prompt = system_prompt or SYSTEM_PROMPT

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._client: Optional[ClaudeSDKClient] = None
        self._lock = threading.Lock()
        self._idle_timer: Optional[threading.Timer] = None
        self._connected = False

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

    def _idle_disconnect(self):
        """空闲超时断开"""
        with self._lock:
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

    def connect(self):
        """连接客户端"""
        with self._lock:
            if self._connected:
                return
            self._ensure_loop()

            options = ClaudeAgentOptions(
                resume=self.session_id,
                allowed_tools=self.allowed_tools,
                permission_mode=self.permission_mode,
                system_prompt={"type": "preset", "preset": "claude_code", "append": self.system_prompt},
                cwd=self.cwd,
            )
            client = ClaudeSDKClient(options=options)
            future = asyncio.run_coroutine_threadsafe(client.connect(), self._loop)
            future.result(timeout=30)
            self._client = client
            self._connected = True

    def chat_sync(self, message: str, on_heartbeat: Optional[callable] = None) -> tuple[str, str]:
        """发送消息并等待回复，session 失效时自动重试"""
        if not self._connected:
            self.connect()

        self._reset_idle_timer()

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

    def _do_chat_sync(self, message: str) -> tuple[str, str]:
        """实际执行聊天（无重试）"""
        with self._lock:
            client = self._client
            loop = self._loop

        async def _do_chat():
            await client.query(message)
            response_text = []
            async for msg in client.receive_response():
                if isinstance(msg, AssistantMessage):
                    for block in msg.content:
                        if isinstance(block, TextBlock):
                            response_text.append(block.text)
                elif isinstance(msg, ResultMessage):
                    self.session_id = msg.session_id
            return "\n".join(response_text), self.session_id or ""

        future = asyncio.run_coroutine_threadsafe(_do_chat(), loop)
        return future.result(timeout=1800)

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
        try:
            asyncio.run_coroutine_threadsafe(client.disconnect(), self._loop).result(timeout=10)
        except Exception:
            pass
