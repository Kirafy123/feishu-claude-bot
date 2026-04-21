"""
飞书文件回传可靠性测试

覆盖：
1. upload_file_to_feishu — 重试机制（mock HTTP）
2. wait_for_file_ready — 文件就绪等待
3. _parse_file_paths — 各种路径格式的提取
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import tempfile
import shutil
import unittest
from unittest.mock import patch, MagicMock

# Mock lark_oapi before importing main_websocket
sys.modules['lark_oapi'] = MagicMock()
sys.modules['lark_oapi.adapter'] = MagicMock()
sys.modules['lark_oapi.adapter.flask'] = MagicMock()
sys.modules['lark_oapi.api'] = MagicMock()
sys.modules['lark_oapi.api.im'] = MagicMock()
sys.modules['lark_oapi.api.im.v1'] = MagicMock()

# Must set env vars before importing feishu_utils
os.environ.setdefault("APP_ID", "cli_test_fake_id")
os.environ.setdefault("APP_SECRET", "test_fake_secret")

import requests
from src.feishu_utils.feishu_utils import upload_file_to_feishu


# ============================================================
# 1. upload_file_to_feishu 重试测试
# ============================================================

class TestUploadRetry(unittest.TestCase):
    """测试重试机制"""

    @patch('src.feishu_utils.feishu_utils.get_tenant_access_token')
    @patch('src.feishu_utils.feishu_utils.requests.post')
    def test_retry_on_timeout(self, mock_post, mock_token):
        """超时后重试 — 前两次超时，第三次成功"""
        mock_token.return_value = "fake-token"

        mock_success = MagicMock()
        mock_success.json.return_value = {"code": 0, "data": {"file_key": "fk_123"}}

        mock_post.side_effect = [
            requests.exceptions.Timeout(),
            requests.exceptions.Timeout(),
            mock_success,
        ]

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as f:
            f.write(b"test content")
            f.flush()
            tmp_path = f.name

        try:
            result = upload_file_to_feishu(tmp_path, timeout=1)
            self.assertTrue(result.get("success"))
            self.assertEqual(result.get("file_key"), "fk_123")
            self.assertEqual(mock_post.call_count, 3)
        finally:
            os.unlink(tmp_path)

    @patch('src.feishu_utils.feishu_utils.get_tenant_access_token')
    @patch('src.feishu_utils.feishu_utils.requests.post')
    def test_no_retry_on_api_error(self, mock_post, mock_token):
        """API 返回错误（非网络问题）不应重试"""
        mock_token.return_value = "fake-token"
        mock_response = MagicMock()
        mock_response.json.return_value = {"code": 50411, "msg": "file too large"}
        mock_post.return_value = mock_response

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as f:
            f.write(b"test content")
            f.flush()
            tmp_path = f.name

        try:
            result = upload_file_to_feishu(tmp_path, timeout=1)
            self.assertFalse(result.get("success"))
            self.assertEqual(mock_post.call_count, 1)
        finally:
            os.unlink(tmp_path)

    @patch('src.feishu_utils.feishu_utils.get_tenant_access_token')
    @patch('src.feishu_utils.feishu_utils.requests.post')
    def test_retry_exhausted(self, mock_post, mock_token):
        """重试耗尽"""
        mock_token.return_value = "fake-token"
        mock_post.side_effect = [requests.exceptions.Timeout()] * 3

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as f:
            f.write(b"test content")
            f.flush()
            tmp_path = f.name

        try:
            result = upload_file_to_feishu(tmp_path, timeout=1)
            self.assertFalse(result.get("success"))
            self.assertIn("超时", result.get("error", ""))
            self.assertEqual(mock_post.call_count, 3)
        finally:
            os.unlink(tmp_path)

    @patch('src.feishu_utils.feishu_utils.get_tenant_access_token')
    def test_file_too_large(self, mock_token):
        """超过 30MB 的文件不应上传"""
        mock_token.return_value = "fake-token"
        with tempfile.NamedTemporaryFile(delete=False) as f:
            tmp_path = f.name

        try:
            with patch('src.feishu_utils.feishu_utils.os.path.getsize') as mock_size:
                mock_size.return_value = 31 * 1024 * 1024  # 31MB
                result = upload_file_to_feishu(tmp_path)
                self.assertFalse(result.get("success"))
                self.assertIn("文件过大", result.get("error", ""))
        finally:
            os.unlink(tmp_path)

    @patch('src.feishu_utils.feishu_utils.get_tenant_access_token')
    def test_file_not_exists(self, mock_token):
        """不存在的文件应返回错误"""
        mock_token.return_value = "fake-token"
        result = upload_file_to_feishu("/nonexistent/path/file.xlsx")
        self.assertFalse(result.get("success"))
        self.assertIn("不存在", result.get("error", ""))


# ============================================================
# 2. wait_for_file_ready 测试
# ============================================================

class TestWaitForFileReady(unittest.TestCase):
    """测试文件就绪等待函数"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _get_func(self):
        """动态导入 wait_for_file_ready"""
        from src.main_websocket import wait_for_file_ready
        return wait_for_file_ready

    def test_existing_stable_file(self):
        """已存在且大小稳定的文件"""
        path = os.path.join(self.tmpdir, "stable.txt")
        with open(path, "w") as f:
            f.write("hello world")
        wait_for_file_ready = self._get_func()
        ready, reason = wait_for_file_ready(path, max_wait=2.0, poll_interval=0.3)
        self.assertTrue(ready)

    def test_nonexistent_file(self):
        """不存在的文件"""
        wait_for_file_ready = self._get_func()
        path = os.path.join(self.tmpdir, "does_not_exist.txt")
        ready, reason = wait_for_file_ready(path)
        self.assertFalse(ready)
        self.assertIn("不存在", reason)

    def test_empty_file(self):
        """空文件应视为就绪"""
        path = os.path.join(self.tmpdir, "empty.txt")
        with open(path, "w") as f:
            pass  # 创建空文件
        wait_for_file_ready = self._get_func()
        ready, reason = wait_for_file_ready(path, max_wait=1.0, poll_interval=0.2)
        self.assertTrue(ready)
        self.assertIn("empty", reason)


# ============================================================
# 3. _parse_file_paths 测试
# ============================================================

class TestParseFilePaths(unittest.TestCase):
    """测试从文本中提取文件路径的各种格式"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.tmpdir, "report.xlsx")
        with open(self.test_file, "w") as f:
            f.write("test")
        self.rel_dir = os.path.join(self.tmpdir, "output")
        os.makedirs(self.rel_dir, exist_ok=True)
        self.rel_file = os.path.join(self.rel_dir, "data.csv")
        with open(self.rel_file, "w") as f:
            f.write("test")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _get_func(self):
        """动态导入 _parse_file_paths"""
        from src.main_websocket import _parse_file_paths
        return _parse_file_paths

    def test_direct_windows_path(self):
        """直接 Windows 路径"""
        text = f"文件路径: {self.test_file}"
        _parse_file_paths = self._get_func()
        paths = _parse_file_paths(text)
        self.assertIn(self.test_file, paths)

    def test_path_in_backticks(self):
        """反引号包裹的路径"""
        text = f"已生成：`{self.test_file}`"
        _parse_file_paths = self._get_func()
        paths = _parse_file_paths(text)
        self.assertIn(self.test_file, paths)

    def test_path_in_quotes(self):
        """引号包裹的路径"""
        text = f'文件在 "{self.test_file}"'
        _parse_file_paths = self._get_func()
        paths = _parse_file_paths(text)
        self.assertIn(self.test_file, paths)

    def test_path_with_chinese_punctuation(self):
        """路径后有中文标点"""
        text = f"文件路径: {self.test_file}。"
        _parse_file_paths = self._get_func()
        paths = _parse_file_paths(text)
        self.assertIn(self.test_file, paths)

    def test_relative_path(self):
        """相对路径在 workspace 下补全"""
        text = "生成了 output/data.csv"
        _parse_file_paths = self._get_func()
        paths = _parse_file_paths(text, workspace=self.tmpdir)
        # os.path.join with a "/" relative component may produce mixed slashes on Windows;
        # compare using normpath.
        normalized_paths = [os.path.normpath(p) for p in paths]
        self.assertIn(os.path.normpath(self.rel_file), normalized_paths)

    def test_nonexistent_path(self):
        """不存在的路径不应被提取"""
        _parse_file_paths = self._get_func()
        text = "文件在 C:\\nonexistent\\file.xlsx"
        paths = _parse_file_paths(text)
        self.assertEqual(len(paths), 0)


if __name__ == "__main__":
    unittest.main()
