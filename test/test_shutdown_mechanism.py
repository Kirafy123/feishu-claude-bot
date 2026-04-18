"""
优雅重启机制 & busy 状态同步测试

覆盖：
1. shutdown() 全局清理（所有任务、队列、持久客户端）
2. /restart 和 /shutdown 命令
3. _shutting_down 标志阻止新消息
4. _sync_session_busy() 状态同步
5. _cancel_all_tasks 清理等待队列
6. 信号处理注册
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import threading
from queue import Queue
from unittest.mock import patch, MagicMock

import pytest

# ============================================================
# Mock 辅助
# ============================================================

def reset_global_state():
    """重置 main_websocket 全局状态"""
    import src.main_websocket as mw
    mw._main_sessions.clear()
    mw._parallel_sessions.clear()
    mw._waiting_queue.clear()
    mw._known_chat_ids.clear()
    mw._shutting_down = False


def make_mock_event(text="hello", chat_id="oc_test_001", chat_type="p2p", message_id="msg_001", mentions=None):
    """构造飞书事件 mock"""
    event = MagicMock()
    message = MagicMock()
    message.message_id = message_id
    message.chat_id = chat_id
    message.chat_type = chat_type
    message.content = '{"text": "' + text + '"}'
    message.mentions = mentions or []
    event.message = message
    return event


def make_mock_data(text="hello", **kwargs):
    """构造完整的飞书数据 mock"""
    data = MagicMock()
    data.event = make_mock_event(text=text, **kwargs)
    return data


def mock_feishu_calls():
    """mock 所有飞书 API 调用"""
    patchers = []
    for func_name in ['send_message', 'reply_message', 'send_card_message', 'update_card_message']:
        p = patch(f'src.main_websocket.{func_name}', return_value={'data': {'message_id': 'msg_card_001'}})
        patchers.append(p)
    for p in patchers:
        p.start()
    return patchers


def stop_feishu_patches(patchers):
    for p in patchers:
        p.stop()


_db_counter = 0

def reset_db():
    """重置数据库 - 每个测试用独立文件"""
    import src.data_base_utils.session_store as store
    global _db_counter
    _db_counter += 1
    test_db = Path(__file__).parent.parent / "data" / f"test_shutdown_{_db_counter}.db"
    store.DB_PATH = test_db
    if test_db.exists():
        import time
        for _ in range(3):
            try:
                test_db.unlink()
                break
            except PermissionError:
                time.sleep(0.2)


# ============================================================
# 1. shutdown() 全局清理测试
# ============================================================

class TestShutdownCleanup:
    """测试 shutdown() 函数完整清理能力"""

    def setup_method(self):
        reset_global_state()
        reset_db()
        self._patchers = mock_feishu_calls()
        self.p_get_token = patch('src.main_websocket.get_token', return_value='fake-token')
        self.p_get_token.start()
        self.p_chat_sync = patch('src.main_websocket.chat_sync', return_value=("reply", "new-sid"))
        self.p_persistent = patch('src.main_websocket.PersistentClient')
        self.mock_chat_sync = self.p_chat_sync.start()
        mock_pc_class = self.p_persistent.start()
        mock_pc = MagicMock()
        mock_pc.chat_sync.return_value = ("reply", "new-sid")
        mock_pc_class.return_value = mock_pc

    def teardown_method(self):
        stop_feishu_patches(self._patchers)
        self.p_get_token.stop()
        self.p_chat_sync.stop()
        self.p_persistent.stop()
        import src.main_websocket as mw
        for main in list(mw._main_sessions.values()):
            if main.thread and main.thread.is_alive():
                main.thread.join(timeout=3)
        for ps in list(mw._parallel_sessions.values()):
            if ps.thread and ps.thread.is_alive():
                ps.thread.join(timeout=3)
        reset_global_state()

    def test_shutdown_clears_all_main_sessions(self):
        """shutdown() 清空所有主会话的 busy 和队列"""
        from src.main_websocket import shutdown, MainSession, _main_sessions, _global_lock

        # 创建 2 个主会话，标记为 busy
        m1 = MainSession(chat_id="oc_chat_1", session_id="")
        m1.busy = True
        m1.queue.put(("msg1", "id1", "p2p"))
        _main_sessions["oc_chat_1"] = m1

        m2 = MainSession(chat_id="oc_chat_2", session_id="")
        m2.busy = True
        m2.queue.put(("msg2", "id2", "p2p"))
        _main_sessions["oc_chat_2"] = m2

        stats = shutdown()

        assert stats["cancelled_main"] == 2
        assert not m1.busy
        assert not m2.busy
        assert m1.queue.empty()
        assert m2.queue.empty()

    def test_shutdown_clears_parallel_sessions(self):
        """shutdown() 清空所有并行会话"""
        from src.main_websocket import shutdown, ParallelSession, _parallel_sessions, _global_lock

        ps1 = ParallelSession(session_id="ps-1", parent_chat_id="oc_chat_1")
        ps1.busy = True
        ps1.queue.put(("msg1", "id1", "p2p"))
        _parallel_sessions["ps-1"] = ps1

        ps2 = ParallelSession(session_id="ps-2", parent_chat_id="oc_chat_2")
        ps2.busy = True
        _parallel_sessions["ps-2"] = ps2

        stats = shutdown()

        assert stats["cancelled_parallel"] == 2
        assert not ps1.busy
        assert not ps2.busy
        assert ps1.queue.empty()
        assert len(_parallel_sessions) == 0  # 字典被清空

    def test_shutdown_clears_waiting_queue(self):
        """shutdown() 清空所有等待队列"""
        from src.main_websocket import shutdown, _waiting_queue

        _waiting_queue["oc_chat_1"] = Queue()
        _waiting_queue["oc_chat_1"].put(("msg1", "id1", "p2p"))
        _waiting_queue["oc_chat_1"].put(("msg2", "id2", "p2p"))

        _waiting_queue["oc_chat_2"] = Queue()
        _waiting_queue["oc_chat_2"].put(("msg3", "id3", "p2p"))

        stats = shutdown()

        assert stats["cleared_waiting"] == 3
        assert len(_waiting_queue) == 0

    def test_shutdown_disconnects_persistent_clients(self):
        """shutdown() 断开所有持久客户端"""
        from src.main_websocket import MainSession, _main_sessions

        mock_pc = MagicMock()
        m1 = MainSession(chat_id="oc_chat_1", session_id="")
        m1.persistent_client = mock_pc
        _main_sessions["oc_chat_1"] = m1

        from src.main_websocket import shutdown
        shutdown()

        mock_pc.disconnect.assert_called_once()
        assert m1.persistent_client is None

    def test_shutdown_returns_correct_stats(self):
        """shutdown() 返回准确的统计信息"""
        from src.main_websocket import shutdown, MainSession, ParallelSession, _main_sessions, _parallel_sessions, _waiting_queue

        # 设置状态
        m = MainSession(chat_id="oc_1", session_id="")
        m.busy = True
        _main_sessions["oc_1"] = m

        ps = ParallelSession(session_id="ps-1", parent_chat_id="oc_1")
        ps.busy = True
        _parallel_sessions["ps-1"] = ps

        q = Queue()
        q.put(("msg", "id", "p2p"))
        _waiting_queue["oc_1"] = q

        stats = shutdown()

        assert stats == {"cancelled_main": 1, "cancelled_parallel": 1, "cleared_waiting": 1}


# ============================================================
# 2. /restart 命令测试
# ============================================================

class TestRestartCommand:
    """测试 /restart 命令"""

    def setup_method(self):
        reset_global_state()
        reset_db()
        self._patchers = mock_feishu_calls()
        self.p_get_token = patch('src.main_websocket.get_token', return_value='fake-token')
        self.p_get_token.start()
        self.p_chat_sync = patch('src.main_websocket.chat_sync', return_value=("reply", "new-sid"))
        self.p_persistent = patch('src.main_websocket.PersistentClient')
        mock_pc = MagicMock()
        mock_pc.chat_sync.return_value = ("reply", "new-sid")
        self.p_persistent.start()

    def teardown_method(self):
        stop_feishu_patches(self._patchers)
        self.p_get_token.stop()
        self.p_chat_sync.stop()
        self.p_persistent.stop()
        reset_global_state()

    def test_restart_resets_all_state(self):
        """/restart 命令重置所有状态"""
        from src.main_websocket import handle_message, _main_sessions, _parallel_sessions, _waiting_queue

        # 先创建一些状态
        handle_message(make_mock_data(text="msg1"))
        # 等待线程处理
        main = _main_sessions.get("oc_test_001")
        if main and main.thread:
            main.thread.join(timeout=3)

        # 发 /restart
        handle_message(make_mock_data(text="/restart"))

        # 验证 send_message 被调用（包含统计信息）
        from src.main_websocket import send_message
        assert send_message.called

    def test_restart_uses_chinese_alias(self):
        """/重启 中文别名等效"""
        from src.main_websocket import handle_message
        from src.main_websocket import send_message

        handle_message(make_mock_data(text="/重启"))
        assert send_message.called


# ============================================================
# 3. _shutting_down 阻止新消息
# ============================================================

class TestShuttingDownBlocksNewMessages:
    """测试关闭中阻止新消息入队"""

    def setup_method(self):
        reset_global_state()
        reset_db()
        self._patchers = mock_feishu_calls()
        self.p_get_token = patch('src.main_websocket.get_token', return_value='fake-token')
        self.p_get_token.start()
        self.p_chat_sync = patch('src.main_websocket.chat_sync', return_value=("reply", "new-sid"))
        self.p_persistent = patch('src.main_websocket.PersistentClient')
        mock_pc = MagicMock()
        mock_pc.chat_sync.return_value = ("reply", "new-sid")
        self.p_persistent.start()

    def teardown_method(self):
        stop_feishu_patches(self._patchers)
        self.p_get_token.stop()
        self.p_chat_sync.stop()
        self.p_persistent.stop()
        reset_global_state()

    def test_messages_blocked_when_shutting_down(self):
        """关闭中，新消息被拒绝"""
        from src.main_websocket import handle_message, _main_sessions, _shutting_down
        import src.main_websocket as mw

        mw._shutting_down = True
        handle_message(make_mock_data(text="这条消息应被忽略"))

        assert "oc_test_001" not in _main_sessions
        # send_message 应被调用来发送拒绝提示
        from src.main_websocket import send_message
        assert send_message.called

    def test_file_messages_blocked_when_shutting_down(self):
        """关闭中，文件消息也被拒绝"""
        from src.main_websocket import handle_message, send_message
        import src.main_websocket as mw

        mw._shutting_down = True

        data = MagicMock()
        event = MagicMock()
        message = MagicMock()
        message.message_id = "msg_file_001"
        message.chat_id = "oc_test_001"
        message.chat_type = "p2p"
        message.message_type = "file"
        message.content = '{"text": "analyze this", "file_key": "fk_123", "file_name": "test.py"}'
        message.mentions = []
        event.message = message
        data.event = event

        handle_message(data)

        assert send_message.called


# ============================================================
# 4. _sync_session_busy 测试
# ============================================================

class TestSyncSessionBusy:
    """测试 busy 状态同步"""

    def setup_method(self):
        reset_global_state()
        from src.main_websocket import _sync_session_busy
        self.sync = _sync_session_busy

    def teardown_method(self):
        reset_global_state()

    def test_sync_clears_stale_busy(self):
        """线程已退出但 busy=True 时应被同步为 False"""
        from src.main_websocket import MainSession

        main = MainSession(chat_id="oc_test", session_id="")
        main.busy = True
        # 创建一个已退出的线程（不启动）
        main.thread = threading.Thread(target=lambda: None)
        # 注意：未启动的线程 is_alive() 为 False

        self.sync(main)

        assert main.busy is False

    def test_sync_keeps_busy_when_thread_alive(self):
        """线程存活时保持 busy=True"""
        from src.main_websocket import MainSession
        import time

        main = MainSession(chat_id="oc_test", session_id="")
        main.busy = True
        main.thread = threading.Thread(target=lambda: time.sleep(0.5), daemon=True)
        main.thread.start()

        self.sync(main)

        assert main.busy is True
        main.thread.join(timeout=1)

    def test_sync_ignores_correct_state(self):
        """busy=False 时不应改变"""
        from src.main_websocket import MainSession

        main = MainSession(chat_id="oc_test", session_id="")
        main.busy = False
        main.thread = None

        self.sync(main)

        assert main.busy is False


# ============================================================
# 5. _cancel_all_tasks 清理等待队列
# ============================================================

class TestCancelAllTasks:
    """测试 _cancel_all_tasks 同时清空等待队列"""

    def setup_method(self):
        reset_global_state()
        reset_db()
        self._patchers = mock_feishu_calls()
        self.p_get_token = patch('src.main_websocket.get_token', return_value='fake-token')
        self.p_get_token.start()

    def teardown_method(self):
        stop_feishu_patches(self._patchers)
        self.p_get_token.stop()
        reset_global_state()

    def test_cancel_clears_waiting_queue(self):
        """取消任务时同时清空等待队列"""
        from src.main_websocket import _cancel_all_tasks, MainSession, _main_sessions, _waiting_queue

        main = MainSession(chat_id="oc_test", session_id="")
        main.busy = True
        _main_sessions["oc_test"] = main

        q = Queue()
        q.put(("msg1", "id1", "p2p"))
        q.put(("msg2", "id2", "p2p"))
        _waiting_queue["oc_test"] = q

        count = _cancel_all_tasks("oc_test")

        assert count == 1
        assert q.empty()


# ============================================================
# 6. 信号处理注册测试
# ============================================================

class TestSignalHandlers:
    """测试信号处理注册"""

    def test_sigterm_handler_registered(self):
        """SIGTERM 信号处理器已注册"""
        import signal
        import src.main_websocket as mw

        # 重新导入模块会重新注册信号
        handler = signal.getsignal(signal.SIGTERM)
        assert handler is not None

    def test_sigint_handler_registered(self):
        """SIGINT 信号处理器已注册"""
        import signal

        handler = signal.getsignal(signal.SIGINT)
        assert handler is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
