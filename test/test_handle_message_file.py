"""
飞书机器人 - handle_message 文件消息处理测试

需要导入 main_websocket，包含 lark SDK 依赖。
单独运行此文件测试 handle_message 的文件消息分支。
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
os.environ.setdefault("APP_ID", "cli_test_fake_id")
os.environ.setdefault("APP_SECRET", "test_fake_secret")

import json
import unittest
from unittest.mock import patch, MagicMock
from queue import Queue

# 单独导入 main_websocket
# 使用 exec 避免 lark_oapi 循环导入
import importlib.util
spec = importlib.util.spec_from_file_location(
    "main_websocket",
    str(Path(__file__).parent.parent / "src" / "main_websocket.py")
)
mw = importlib.util.module_from_spec(spec)
sys.modules["main_websocket"] = mw
spec.loader.exec_module(mw)


class TestHandleMessageFile(unittest.TestCase):
    """测试 handle_message 对文件消息的处理"""

    def setUp(self):
        mw._main_sessions.clear()
        mw._parallel_sessions.clear()
        mw._waiting_queue.clear()
        mw._known_chat_ids.clear()

    def _make_mock_event(self, chat_id="chat_001", chat_type="p2p",
                         message_type="file", content_dict=None, mentions=None):
        """构造飞书消息 mock"""
        if content_dict is None:
            content_dict = {"file_key": "file_key_abc", "file_name": "report.pdf"}

        message = MagicMock()
        message.message_id = "msg_123"
        message.chat_id = chat_id
        message.chat_type = chat_type
        message.message_type = message_type
        message.content = json.dumps(content_dict)
        message.mentions = mentions
        return message

    def _make_mock_data(self, message):
        data = MagicMock()
        data.event.message = message
        return data

    @patch.object(mw, 'get_token', return_value="token")
    @patch.object(mw, 'send_message')
    @patch.object(mw, 'download_file_message')
    def test_file_message_basic(self, mock_download, mock_send, mock_token):
        """纯文件消息（无附加文字）— 下载成功，交给 Claude 分析"""
        mock_download.return_value = True

        msg = self._make_mock_event()
        data = self._make_mock_data(msg)

        with patch.object(mw, '_dispatch_to_session') as mock_dispatch:
            mw.handle_message(data)

        mock_download.assert_called_once()
        args = mock_download.call_args[0]
        self.assertEqual(args[0], "msg_123")
        self.assertEqual(args[1], "file_key_abc")

        save_path = args[2]
        self.assertIn("downloads", save_path)
        self.assertIn("report.pdf", save_path)

        mock_dispatch.assert_called_once()
        synthetic_text = mock_dispatch.call_args[0][1]
        self.assertIn("report.pdf", synthetic_text)
        self.assertIn("已保存到", synthetic_text)

    @patch.object(mw, 'get_token', return_value="token")
    @patch.object(mw, 'send_message')
    @patch.object(mw, 'download_file_message')
    def test_file_with_text_merged(self, mock_download, mock_send, mock_token):
        """文件+附加文字 — 合并后发给 Claude"""
        mock_download.return_value = True

        content = {"file_key": "file_key_abc", "file_name": "data.xlsx", "text": "帮我分析这个文件里的数据趋势"}
        msg = self._make_mock_event(content_dict=content)
        data = self._make_mock_data(msg)

        with patch.object(mw, '_dispatch_to_session') as mock_dispatch:
            mw.handle_message(data)

        mock_dispatch.assert_called_once()
        synthetic_text = mock_dispatch.call_args[0][1]
        self.assertIn("帮我分析这个文件里的数据趋势", synthetic_text)
        self.assertIn("data.xlsx", synthetic_text)

    @patch.object(mw, 'get_token', return_value="token")
    @patch.object(mw, 'send_message')
    @patch.object(mw, 'download_file_message')
    def test_file_uses_workspace(self, mock_download, mock_send, mock_token):
        """有 workspace 时，文件下载到 workspace/.downloads/"""
        mock_download.return_value = True

        from src.data_base_utils import save_workspace
        save_workspace("chat_001", "D:\\gamedev1")

        msg = self._make_mock_event()
        data = self._make_mock_data(msg)

        with patch.object(mw, '_dispatch_to_session') as mock_dispatch:
            mw.handle_message(data)

        save_path = mock_download.call_args[0][2]
        self.assertIn("gamedev1", save_path)
        self.assertIn(".downloads", save_path)

    @patch.object(mw, 'get_token', return_value="token")
    @patch.object(mw, 'send_message')
    @patch.object(mw, 'download_file_message')
    def test_download_failure_sends_error(self, mock_download, mock_send, mock_token):
        """下载失败时发送错误通知"""
        mock_download.side_effect = Exception("下载失败: 404")

        msg = self._make_mock_event()
        data = self._make_mock_data(msg)

        mw.handle_message(data)

        mock_send.assert_called()
        call_args = str(mock_send.call_args[0])
        self.assertIn("下载失败", call_args)

    @patch.object(mw, 'get_token', return_value="token")
    @patch.object(mw, 'send_message')
    def test_missing_file_key(self, mock_send, mock_token):
        """消息中没有 file_key — 发送警告"""
        content = {"text": "only text"}
        msg = self._make_mock_event(message_type="file", content_dict=content)
        data = self._make_mock_data(msg)

        mw.handle_message(data)

        mock_send.assert_called()
        call_args = str(mock_send.call_args[0])
        self.assertIn("无法解析文件信息", call_args)

    @patch.object(mw, 'get_token', return_value="token")
    @patch.object(mw, 'send_message')
    @patch.object(mw, 'download_file_message')
    def test_image_message(self, mock_download, mock_send, mock_token):
        """图片消息 — 使用 image_key"""
        mock_download.return_value = True

        content = {"image_key": "img_key_xyz", "text": "描述这张图"}
        msg = self._make_mock_event(message_type="image", content_dict=content)
        data = self._make_mock_data(msg)

        with patch.object(mw, '_dispatch_to_session') as mock_dispatch:
            mw.handle_message(data)

        mock_download.assert_called_once()
        self.assertEqual(mock_download.call_args[0][1], "img_key_xyz")

        synthetic_text = mock_dispatch.call_args[0][1]
        self.assertIn("描述这张图", synthetic_text)


class TestGroupChatFileMessage(unittest.TestCase):
    """群聊文件消息过滤"""

    def setUp(self):
        mw._main_sessions.clear()
        mw._parallel_sessions.clear()
        mw._waiting_queue.clear()
        mw._known_chat_ids.clear()

    def _make_mock_event(self, chat_id="group_001", chat_type="group",
                         message_type="file", content_dict=None, mentions=None):
        if content_dict is None:
            content_dict = {"file_key": "key", "file_name": "file.pdf"}
        message = MagicMock()
        message.message_id = "msg_123"
        message.chat_id = chat_id
        message.chat_type = chat_type
        message.message_type = message_type
        message.content = json.dumps(content_dict)
        message.mentions = mentions
        return message

    def _make_mock_data(self, message):
        data = MagicMock()
        data.event.message = message
        return data

    @patch.object(mw, 'get_token', return_value="token")
    @patch.object(mw, 'send_message')
    @patch.object(mw, 'download_file_message')
    def test_group_file_message_with_mention(self, mock_download, mock_send, mock_token):
        """群聊中 @机器人 + 文件消息 — 应处理"""
        mock_download.return_value = True
        mention = MagicMock()
        mention.name = "BotName"

        msg = self._make_mock_event(mentions=[mention])
        data = self._make_mock_data(msg)

        with patch.object(mw, '_dispatch_to_session') as mock_dispatch:
            mw.handle_message(data)

        mock_download.assert_called_once()

    @patch.object(mw, 'get_token', return_value="token")
    @patch.object(mw, 'send_message')
    def test_group_file_message_without_mention_ignored(self, mock_send, mock_token):
        """群聊中文件消息但没有 @机器人 — 应忽略"""
        msg = self._make_mock_event(mentions=None)
        data = self._make_mock_data(msg)

        with patch.object(mw, '_dispatch_to_session') as mock_dispatch:
            mw.handle_message(data)

        mock_dispatch.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
