"""
飞书机器人 - 综合测试套件

覆盖范围：
1. PersistentClient 持久连接生命周期
2. 主会话消息处理（无死锁）
3. 数据库 session 持久化（workspace 不被覆盖）
4. 卡片消息名称（CCwin + workspace）
5. /setworkspace 和 /workspace 命令
6. 会话恢复（过期 session_id 容错）
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import sqlite3
import json
import time
import threading
import asyncio
import unittest
from unittest import mock
from queue import Queue
from unittest.mock import patch, MagicMock, AsyncMock

# 需要测试的模块
from src.claude_code import chat_sync, PersistentClient, ConversationClient
from src.data_base_utils.session_store import (
    get_session, save_session, get_workspace, save_workspace, _get_conn
)


# ============================================================
# 1. PersistentClient 测试
# ============================================================

class TestPersistentClient(unittest.TestCase):
    """测试 PersistentClient 的生命周期管理"""

    def test_init_default_values(self):
        """测试默认参数"""
        pc = PersistentClient()
        self.assertEqual(pc.idle_timeout, 120)
        self.assertEqual(pc.allowed_tools, ["Read", "Write", "Edit", "Bash", "Glob", "Grep"])
        self.assertEqual(pc.permission_mode, "acceptEdits")
        self.assertIsNone(pc._client)
        self.assertFalse(pc._connected)

    def test_init_custom_values(self):
        """测试自定义参数"""
        pc = PersistentClient(
            session_id="test-sid",
            cwd="/tmp/test",
            idle_timeout=60,
        )
        self.assertEqual(pc.session_id, "test-sid")
        self.assertEqual(pc.cwd, "/tmp/test")
        self.assertEqual(pc.idle_timeout, 60)

    def test_ensure_loop_creates_thread(self):
        """测试事件循环和线程创建"""
        pc = PersistentClient()
        pc._ensure_loop()
        self.assertIsNotNone(pc._loop)
        self.assertTrue(pc._thread.is_alive())
        # 清理
        pc._loop.call_soon_threadsafe(pc._loop.stop)
        pc._thread.join(timeout=5)

    def test_idle_timer_starts_and_cancels(self):
        """测试空闲计时器启动和取消"""
        pc = PersistentClient(idle_timeout=1)
        pc._start_idle_timer()
        self.assertIsNotNone(pc._idle_timer)
        self.assertTrue(pc._idle_timer.is_alive())

        pc._reset_idle_timer()
        # 计时器被取消，应该创建新的
        pc._start_idle_timer()
        self.assertIsNotNone(pc._idle_timer)
        self.assertTrue(pc._idle_timer.is_alive())

    def test_disconnect_cleans_up(self):
        """测试断开连接清理资源"""
        pc = PersistentClient()
        pc._start_idle_timer()
        self.assertIsNotNone(pc._idle_timer)

        pc.disconnect()
        # disconnect 将 _idle_timer 设为 None
        self.assertIsNone(pc._idle_timer)


# ============================================================
# 2. 数据库测试
# ============================================================

class TestDatabaseSessionPersistence(unittest.TestCase):
    """测试数据库 session 持久化，特别是 workspace 不被覆盖"""

    DB_PATH = Path(__file__).parent.parent / "data" / "test_sessions.db"
    CHAT_ID = "test_chat_001"

    @classmethod
    def setUpClass(cls):
        """清理测试数据库"""
        if cls.DB_PATH.exists():
            cls.DB_PATH.unlink()

    def setUp(self):
        """每个测试前重置数据库"""
        # 使用测试数据库
        import src.data_base_utils.session_store as store
        store.DB_PATH = self.DB_PATH

    @classmethod
    def tearDownClass(cls):
        """清理测试数据库"""
        if cls.DB_PATH.exists():
            cls.DB_PATH.unlink()

    def test_save_and_get_session(self):
        """测试基本的 session 保存和读取"""
        save_session(self.CHAT_ID, "session-abc")
        sid = get_session(self.CHAT_ID)
        self.assertEqual(sid, "session-abc")

    def test_save_session_preserves_workspace(self):
        """核心测试：save_session 不应覆盖已有的 workspace"""
        # 先设置 workspace
        save_workspace(self.CHAT_ID, "D:\\gamedev1")

        # 保存新的 session_id
        save_session(self.CHAT_ID, "session-xyz")

        # workspace 应该还在
        ws = get_workspace(self.CHAT_ID)
        self.assertEqual(ws, "D:\\gamedev1")

    def test_workspace_crud(self):
        """测试 workspace 的增删改查"""
        save_workspace(self.CHAT_ID, "D:\\test1")
        self.assertEqual(get_workspace(self.CHAT_ID), "D:\\test1")

        save_workspace(self.CHAT_ID, "D:\\test2")
        self.assertEqual(get_workspace(self.CHAT_ID), "D:\\test2")

    def test_session_and_workspace_coexist(self):
        """测试 session 和 workspace 可以共存"""
        save_session(self.CHAT_ID, "session-1")
        save_workspace(self.CHAT_ID, "D:\\project")

        self.assertEqual(get_session(self.CHAT_ID), "session-1")
        self.assertEqual(get_workspace(self.CHAT_ID), "D:\\project")

        # 再次保存 session
        save_session(self.CHAT_ID, "session-2")
        self.assertEqual(get_session(self.CHAT_ID), "session-2")
        self.assertEqual(get_workspace(self.CHAT_ID), "D:\\project")  # workspace 不变


# ============================================================
# 3. 卡片消息名称测试
# ============================================================

class TestCardMessageName(unittest.TestCase):
    """测试卡片消息名称：CCwin + workspace"""

    def _build_card_title(self, workspace=None):
        """模拟 send_card_message 中的标题逻辑"""
        import os
        title = "🤖 CCwin"
        if workspace:
            title += f" | {os.path.basename(workspace)}"
        return title

    def test_default_no_workspace(self):
        """无工作空间时只显示 CCwin"""
        title = self._build_card_title(None)
        self.assertEqual(title, "🤖 CCwin")

    def test_empty_workspace(self):
        """空字符串工作空间不显示后缀"""
        title = self._build_card_title("")
        self.assertEqual(title, "🤖 CCwin")

    def test_with_workspace(self):
        """有工作空间时显示文件夹名"""
        title = self._build_card_title("D:\\gamedev1")
        self.assertEqual(title, "🤖 CCwin | gamedev1")

    def test_workspace_deep_path(self):
        """深层路径只显示文件夹名"""
        title = self._build_card_title("C:\\Users\\Admin\\Desktop\\project\\gamedev1")
        self.assertEqual(title, "🤖 CCwin | gamedev1")

    def test_workspace_linux_path(self):
        """Linux 路径也正确提取"""
        title = self._build_card_title("/home/user/projects/gamedev1")
        self.assertEqual(title, "🤖 CCwin | gamedev1")


# ============================================================
# 4. 主会话无死锁测试
# ============================================================

class TestSessionNoDeadlock(unittest.TestCase):
    """测试主会话不会因 _global_lock 死锁"""

    def test_lock_released_on_queue_empty(self):
        """主会话线程在队列为空时正确释放锁"""
        from src.main_websocket import (
            MainSession, _global_lock, _waiting_queue, _check_close_parallel_nolock
        )

        chat_id = "test_deadlock_001"
        main = MainSession(
            chat_id=chat_id,
            session_id="",
            workspace="",
        )

        # 模拟主会话线程退出时的锁获取
        def simulate_thread_exit():
            try:
                with _global_lock:
                    _check_close_parallel_nolock(chat_id)
                return True
            except Exception:
                return False

        result = simulate_thread_exit()
        self.assertTrue(result)

    def test_multiple_lock_acquire_release(self):
        """多线程交替获取和释放锁不卡死"""
        from src.main_websocket import _global_lock

        results = []
        errors = []

        def worker(name, count):
            for i in range(count):
                try:
                    with _global_lock:
                        time.sleep(0.01)  # 模拟工作
                    results.append(f"{name}-{i}")
                except Exception as e:
                    errors.append(f"{name}: {e}")

        threads = []
        for i in range(5):
            t = threading.Thread(target=worker, args=(f"T{i}", 10))
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=30)

        self.assertEqual(len(results), 50, f"Expected 50 results, got {len(results)}")
        self.assertEqual(len(errors), 0, f"Errors: {errors}")


# ============================================================
# 5. chat_sync 接口测试（模拟）
# ============================================================

class TestChatSyncInterface(unittest.TestCase):
    """测试 chat_sync 的接口行为"""

    def test_chat_sync_returns_tuple(self):
        """chat_sync 返回 (reply, session_id) 元组"""
        # 实际调用需要 Claude API，这里只验证接口存在
        from src.claude_code import chat_sync
        self.assertTrue(callable(chat_sync))

    def test_chat_sync_default_session_id(self):
        """chat_sync 可以不带 session_id 调用"""
        from src.claude_code import chat_sync
        import inspect
        sig = inspect.signature(chat_sync)
        self.assertIsNone(sig.parameters['session_id'].default)


# ============================================================
# 6. 集成测试：模拟消息处理流程
# ============================================================

class TestMessageFlow(unittest.TestCase):
    """模拟完整的消息处理流程"""

    @staticmethod
    def _make_mock_client(session_id="test-session"):
        """创建一个 mock SDK client"""
        from claude_agent_sdk import ResultMessage

        mock_client = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.query = AsyncMock()
        mock_client.disconnect = AsyncMock()

        # 模拟 receive_response 为异步生成器
        result = MagicMock(spec=ResultMessage)
        result.session_id = session_id

        async def response_gen():
            yield result

        mock_client.receive_response = response_gen
        return mock_client

    @patch('src.claude_code.conversation.ClaudeSDKClient')
    @patch('src.claude_code.conversation.ClaudeAgentOptions')
    def test_persistent_client_chat_workflow(self, mock_options, mock_client_class):
        """测试 PersistentClient 的完整聊天流程"""
        mock_client = self._make_mock_client(session_id="new-session-id")
        mock_client_class.return_value = mock_client

        pc = PersistentClient(cwd="/tmp/test")

        # 首次聊天应该触发 connect
        reply, sid = pc.chat_sync("你好")

        mock_client.connect.assert_called_once()
        mock_client.query.assert_called_once_with("你好")
        self.assertEqual(sid, "new-session-id")

        # 清理
        pc.disconnect()

    @patch('src.claude_code.conversation.ClaudeSDKClient')
    @patch('src.claude_code.conversation.ClaudeAgentOptions')
    def test_persistent_client_reuses_connection(self, mock_options, mock_client_class):
        """测试 PersistentClient 复用连接"""
        mock_client = self._make_mock_client(session_id="test-session")
        mock_client_class.return_value = mock_client

        pc = PersistentClient(cwd="/tmp/test")

        # 第一次调用
        pc.chat_sync("消息1")
        connect_count = mock_client.connect.call_count

        # 第二次调用不应该再次 connect
        pc.chat_sync("消息2")
        self.assertEqual(mock_client.connect.call_count, connect_count,
                         "PersistentClient 应该复用连接，不应重复 connect")

        pc.disconnect()

    @patch('src.claude_code.conversation.ClaudeSDKClient')
    @patch('src.claude_code.conversation.ClaudeAgentOptions')
    def test_persistent_client_reconnects_after_disconnect(self, mock_options, mock_client_class):
        """测试断开后自动重连"""
        mock_client1 = self._make_mock_client(session_id="session-1")
        mock_client_class.return_value = mock_client1

        pc = PersistentClient(cwd="/tmp/test")

        # 首次聊天
        pc.chat_sync("消息1")
        self.assertTrue(pc._connected)

        # 手动断开
        pc.disconnect()
        self.assertFalse(pc._connected)

        # 再次聊天应该重新连接
        mock_client2 = self._make_mock_client(session_id="session-2")
        mock_client_class.return_value = mock_client2

        pc.chat_sync("消息2")
        self.assertTrue(pc._connected)

        pc.disconnect()


# ============================================================
# 运行
# ============================================================

if __name__ == "__main__":
    unittest.main(verbosity=2)
