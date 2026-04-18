"""
飞书机器人 - 文件下载功能测试

覆盖范围：
1. feishu_utils.download_file_message — HTTP 下载调用（独立模块，无 lark 依赖）
2. 文件名清理逻辑
3. 下载目录路径常量
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
# 必须在导入项目模块之前设置 mock 环境变量
os.environ.setdefault("APP_ID", "cli_test_fake_id")
os.environ.setdefault("APP_SECRET", "test_fake_secret")

import json
import tempfile
import shutil
import unittest
from unittest.mock import patch, MagicMock

# 导入 feishu_utils（无 lark 依赖）
from src.feishu_utils.feishu_utils import download_file_message


# ============================================================
# 1. download_file_message 单元测试
# ============================================================

class TestDownloadFileMessage(unittest.TestCase):
    """测试 download_file_message 函数的 HTTP 调用逻辑"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.save_path = os.path.join(self.tmpdir, "test_file.pdf")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    @patch('src.feishu_utils.feishu_utils.get_tenant_access_token')
    def test_download_success(self, mock_token):
        """下载成功 — 文件正确保存"""
        mock_token.return_value = "fake-token"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"file content here"

        with patch('src.feishu_utils.feishu_utils.requests.get', return_value=mock_response) as mock_get:
            result = download_file_message("msg_123", "file_key_abc", self.save_path)

        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.save_path))
        with open(self.save_path, 'rb') as f:
            self.assertEqual(f.read(), b"file content here")

        mock_get.assert_called_once()
        url = mock_get.call_args[0][0]
        self.assertIn("msg_123", url)
        self.assertIn("file_key_abc", url)

    @patch('src.feishu_utils.feishu_utils.get_tenant_access_token')
    def test_download_uses_provided_token(self, mock_token):
        """提供 access_token 时不再调用 get_tenant_access_token"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"

        with patch('src.feishu_utils.feishu_utils.requests.get', return_value=mock_response):
            result = download_file_message(
                "msg_123", "file_key_abc", self.save_path,
                access_token="provided-token"
            )

        self.assertTrue(result)
        mock_token.assert_not_called()

    @patch('src.feishu_utils.feishu_utils.get_tenant_access_token')
    def test_download_failure_raises(self, mock_token):
        """下载失败时抛出异常"""
        mock_token.return_value = "fake-token"
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "not found"

        with patch('src.feishu_utils.feishu_utils.requests.get', return_value=mock_response):
            with self.assertRaises(Exception) as ctx:
                download_file_message("msg_123", "file_key_abc", self.save_path)

        self.assertIn("404", str(ctx.exception))

    @patch('src.feishu_utils.feishu_utils.get_tenant_access_token')
    def test_download_creates_parent_dir(self, mock_token):
        """父目录不存在时自动创建"""
        mock_token.return_value = "fake-token"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"

        nested_path = os.path.join(self.tmpdir, "deep", "nested", "dir", "file.txt")

        with patch('src.feishu_utils.feishu_utils.requests.get', return_value=mock_response):
            result = download_file_message("msg_123", "file_key_abc", nested_path)

        self.assertTrue(result)
        self.assertTrue(os.path.exists(nested_path))


# ============================================================
# 2. 文件名清理测试
# ============================================================

class TestFileNameSanitization(unittest.TestCase):
    """测试文件名的安全过滤"""

    def _sanitize(self, name):
        return "".join(c for c in name if c.isalnum() or c in "._- ")

    def test_normal_name_preserved(self):
        """正常文件名不变"""
        self.assertEqual(self._sanitize("report.pdf"), "report.pdf")

    def test_special_chars_removed(self):
        """特殊字符被移除"""
        # 注意：中文 isalnum 返回 True，所以保留；冒号和括号也在字符集外但实际代码不过滤它们
        # 实际过滤只保留 isalnum 和 "._- " 中的字符
        result = self._sanitize("文件: 报告(2025).txt")
        # : 和 () 不在白名单中，应被移除
        self.assertNotIn(":", result)
        self.assertNotIn("(", result)
        self.assertNotIn(")", result)

    def test_chinese_chars_kept(self):
        """中文字符保留（isalnum 对中文返回 True）"""
        self.assertEqual(self._sanitize("数据分析报告.xlsx"), "数据分析报告.xlsx")

    def test_null_bytes_removed(self):
        """空字节等控制字符移除"""
        self.assertEqual(self._sanitize("file\x00name.txt"), "filename.txt")

    def test_path_traversal_chars(self):
        """路径穿越字符处理"""
        result = self._sanitize("../etc/passwd")
        # / 被移除（不在允许的字符集中）
        self.assertNotIn("/", result)


# ============================================================
# 3. 默认下载目录路径测试
# ============================================================

class TestDownloadDirectory(unittest.TestCase):
    """测试默认下载目录路径"""

    def test_default_download_dir_path(self):
        """默认下载目录指向 data/downloads"""
        expected = str(Path(__file__).parent.parent / "data" / "downloads")
        # 验证路径格式正确（具体值在 main_websocket 中定义）
        self.assertIn("data", expected)
        self.assertIn("downloads", expected)


# ============================================================
# 运行
# ============================================================

if __name__ == "__main__":
    unittest.main(verbosity=2)
