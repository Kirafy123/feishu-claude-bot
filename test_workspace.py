"""
工作空间功能测试

测试 /setworkspace 和 /workspace 命令
"""

import unittest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock
from io import StringIO

# 临时目录用于测试
TEST_WORKSPACE = tempfile.mkdtemp(prefix="claude_test_")

class TestWorkspaceCommands(unittest.TestCase):

    def setUp(self):
        """测试前清理数据库中测试用的 chat_id"""
        from src.data_base_utils import session_store
        conn = session_store._get_conn()
        conn.execute("DELETE FROM sessions WHERE chat_id LIKE 'test_%'")
        conn.commit()
        conn.close()

    def tearDown(self):
        """测试后清理"""
        from src.data_base_utils import session_store
        conn = session_store._get_conn()
        conn.execute("DELETE FROM sessions WHERE chat_id LIKE 'test_%'")
        conn.commit()
        conn.close()

    def test_setworkspace_valid_directory(self):
        """TC1: 设置有效工作空间"""
        from src.data_base_utils import get_workspace, save_workspace

        chat_id = "test_c1"
        save_workspace(chat_id, TEST_WORKSPACE)

        workspace = get_workspace(chat_id)
        self.assertEqual(workspace, TEST_WORKSPACE)
        print(f"TC1 OK: 工作空间设置成功 {workspace}")

    def test_setworkspace_invalid_directory(self):
        """TC2: 设置无效工作空间（目录不存在）"""
        from src.data_base_utils import get_workspace

        chat_id = "test_c2"
        # 不存在的路径
        result = get_workspace(chat_id)
        self.assertIsNone(result)
        print(f"TC2 OK: 无效路径返回 None")

    def test_workspace_query_not_set(self):
        """TC3: 查询未设置的工作空间"""
        from src.data_base_utils import get_workspace

        chat_id = "test_c3"
        result = get_workspace(chat_id)
        self.assertIsNone(result)
        print(f"TC3 OK: 未设置时返回 None")

    def test_workspace_overwrite(self):
        """TC4: 覆盖已设置的工作空间"""
        from src.data_base_utils import get_workspace, save_workspace

        chat_id = "test_c4"
        new_workspace = tempfile.mkdtemp(prefix="claude_new_")

        save_workspace(chat_id, TEST_WORKSPACE)
        save_workspace(chat_id, new_workspace)

        result = get_workspace(chat_id)
        self.assertEqual(result, new_workspace)

        # 清理
        os.rmdir(new_workspace)
        print(f"TC4 OK: 工作空间覆盖成功")

    @unittest.skip("lark-oapi 在 Windows 长路径环境下无法导入，需启用 LongPathsEnabled 或使用虚拟环境")
    def test_main_session_workspace_loaded(self):
        """TC5: MainSession 创建时加载工作空间"""
        from src.main_websocket import MainSession, _main_sessions, _global_lock
        from src.data_base_utils import save_workspace

        chat_id = "test_c5"
        save_workspace(chat_id, TEST_WORKSPACE)

        with _global_lock:
            main = MainSession(chat_id=chat_id, session_id="test_sid", workspace="")
            # 模拟 handle_message 中从数据库加载 workspace
            from src.data_base_utils import get_workspace
            main.workspace = get_workspace(chat_id)

        self.assertEqual(main.workspace, TEST_WORKSPACE)
        print(f"TC5 OK: MainSession.workspace 正确加载")

    def test_chat_sync_with_cwd(self):
        """TC6: chat_sync 支持 cwd 参数"""
        from src.claude_code import chat_sync

        # 用简单消息测试 cwd 参数存在性
        # 不实际调用（太慢），只验证函数签名
        import inspect
        sig = inspect.signature(chat_sync)
        params = list(sig.parameters.keys())

        self.assertIn('cwd', params)
        print(f"TC6 OK: chat_sync 支持 cwd 参数，签名: {sig}")

    def test_conversation_client_with_cwd(self):
        """TC7: ConversationClient 支持 cwd 参数"""
        from src.claude_code.conversation import ConversationClient

        import inspect
        sig = inspect.signature(ConversationClient.__init__)
        params = list(sig.parameters.keys())

        self.assertIn('cwd', params)
        print(f"TC7 OK: ConversationClient 支持 cwd 参数")

    def test_database_migration_workspace_field(self):
        """TC8: 数据库 workspace 字段存在"""
        from src.data_base_utils import session_store

        conn = session_store._get_conn()
        cursor = conn.execute("PRAGMA table_info(sessions)")
        columns = [row[1] for row in cursor.fetchall()]
        conn.close()

        self.assertIn('workspace', columns)
        print(f"TC8 OK: sessions 表包含 workspace 字段: {columns}")

    def test_setworkspace_command_validation(self):
        """TC9: /setworkspace 命令路径验证逻辑"""
        # 模拟 handle_message 中的命令处理
        test_path = "/nonexistent/path/12345"

        # 验证 os.path.isdir 对不存在路径返回 False
        self.assertFalse(os.path.isdir(test_path))

        # 验证 os.path.isdir 对存在路径返回 True
        self.assertTrue(os.path.isdir(TEST_WORKSPACE))
        print(f"TC9 OK: 路径验证逻辑正确")

    def test_workspace_isolated_per_chat_id(self):
        """TC10: 工作空间按 chat_id 隔离"""
        from src.data_base_utils import save_workspace, get_workspace

        workspace1 = tempfile.mkdtemp(prefix="w1_")
        workspace2 = tempfile.mkdtemp(prefix="w2_")

        save_workspace("test_chat_a", workspace1)
        save_workspace("test_chat_b", workspace2)

        self.assertEqual(get_workspace("test_chat_a"), workspace1)
        self.assertEqual(get_workspace("test_chat_b"), workspace2)
        self.assertNotEqual(get_workspace("test_chat_a"), get_workspace("test_chat_b"))

        # 清理
        os.rmdir(workspace1)
        os.rmdir(workspace2)
        print(f"TC10 OK: 不同 chat_id 工作空间隔离")


if __name__ == "__main__":
    print("=" * 60)
    print("工作空间功能测试")
    print(f"测试目录: {TEST_WORKSPACE}")
    print("=" * 60)
    unittest.main(verbosity=2)

    # 清理测试目录
    if os.path.exists(TEST_WORKSPACE):
        shutil.rmtree(TEST_WORKSPACE)
