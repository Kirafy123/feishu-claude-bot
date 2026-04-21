"""
Plan A 文件索取检测测试

覆盖：
1. _is_file_request — 意图识别
2. _extract_requested_filename — 文件名提取
3. _search_workspace_for_file — 工作区搜索
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import tempfile
import shutil
import unittest
import re
import glob


# ============================================================
# 1. 意图识别测试
# ============================================================

_FILE_REQUEST_PATTERNS = [
    r'发.*给我', r'发.*文件', r'把.*文件.*发', r'把.*发.*我',
    r'下载.*文件', r'给我.*文件', r'发送.*文件', r'发一下',
    r'发我', r'发过来', r'发.*过来', r'传给.*我', r'传.*给.*我',
    r'给.*我.*文件', r'我要.*文件', r'看看.*文件', r'打开.*文件',
]

_SUPPORTED_EXTENSIONS = (
    '.xlsx', '.xls', '.doc', '.docx', '.pdf', '.ppt', '.pptx',
    '.csv', '.txt', '.html', '.htm', '.json', '.xml', '.zip',
    '.rar', '.7z', '.tar', '.gz', '.md', '.py', '.sql', '.log',
    '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg', '.webp',
    '.mp4', '.mp3', '.wav', '.avi',
)


def _is_file_request(text: str) -> bool:
    return any(re.search(p, text) for p in _FILE_REQUEST_PATTERNS)


def _extract_requested_filename(text: str) -> str | None:
    pattern = r'([\w一-鿿]+(?:\.[a-zA-Z]{2,6}))'
    matches = re.findall(pattern, text)
    for m in matches:
        ext = os.path.splitext(m)[1].lower()
        if ext in _SUPPORTED_EXTENSIONS:
            return m
    return None


def _search_workspace_for_file(filename: str, workspace: str) -> list[str]:
    if not workspace or not os.path.isdir(workspace):
        return []
    pattern = os.path.join(workspace, "**", f"*{filename}*")
    matches = glob.glob(pattern, recursive=True)
    matches = [m for m in matches if not os.path.basename(m).startswith('.')]
    matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return matches


class TestIsFileRequest(unittest.TestCase):
    """测试文件索取意图识别"""

    def test_send_to_me(self):
        self.assertTrue(_is_file_request("把经营报表发给我"))

    def test_send_file(self):
        self.assertTrue(_is_file_request("发文件给我"))

    def test_download_file(self):
        self.assertTrue(_is_file_request("下载文件"))

    def test_send_it_over(self):
        self.assertTrue(_is_file_request("发过来"))

    def test_pass_to_me(self):
        self.assertTrue(_is_file_request("传给我"))

    def test_i_want_file(self):
        self.assertTrue(_is_file_request("我要文件"))

    def test_send_it(self):
        self.assertTrue(_is_file_request("发一下"))

    def test_ba_file_send(self):
        self.assertTrue(_is_file_request("把文件发我"))

    def test_not_file_request(self):
        self.assertFalse(_is_file_request("今天天气怎么样"))

    def test_not_file_request2(self):
        self.assertFalse(_is_file_request("帮我写个脚本"))

    def test_not_file_request3(self):
        self.assertFalse(_is_file_request("分析一下这个数据"))


# ============================================================
# 2. 文件名提取测试
# ============================================================

class TestExtractRequestedFilename(unittest.TestCase):
    """测试从消息中提取文件名"""

    def test_exact_filename(self):
        result = _extract_requested_filename("把经营报表.xlsx发给我")
        # 正则匹配包含中文字符，"把经营报表.xlsx" 整体匹配，扩展名验证通过
        self.assertEqual(result, "把经营报表.xlsx")

    def test_filename_with_path(self):
        result = _extract_requested_filename("把 D:\\finance\\output\\报表.docx 发给我")
        self.assertEqual(result, "报表.docx")

    def test_no_filename(self):
        result = _extract_requested_filename("把上个月的报表发给我")
        self.assertIsNone(result)

    def test_multiple_filenames(self):
        result = _extract_requested_filename("把 a.xlsx 和 b.pdf 都发给我")
        self.assertEqual(result, "a.xlsx")  # 返回第一个匹配


# ============================================================
# 3. 工作区搜索测试
# ============================================================

class TestSearchWorkspaceForFile(unittest.TestCase):
    """测试工作区文件搜索"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.tmpdir, "output"), exist_ok=True)
        os.makedirs(os.path.join(self.tmpdir, ".hidden"), exist_ok=True)

        self.file1 = os.path.join(self.tmpdir, "经营报表.xlsx")
        self.file2 = os.path.join(self.tmpdir, "output", "经营报表_4月.xlsx")
        self.file3 = os.path.join(self.tmpdir, "output", "收支报表.xlsx")
        self.file4 = os.path.join(self.tmpdir, ".hidden", "secret.xlsx")

        for f in [self.file1, self.file2, self.file3, self.file4]:
            with open(f, "w") as fp:
                fp.write("test")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_exact_match(self):
        results = _search_workspace_for_file("经营报表.xlsx", self.tmpdir)
        self.assertIn(self.file1, results)

    def test_partial_match(self):
        results = _search_workspace_for_file("经营报表", self.tmpdir)
        self.assertEqual(len(results), 2)

    def test_hidden_files_excluded(self):
        results = _search_workspace_for_file("secret", self.tmpdir)
        self.assertEqual(len(results), 0)

    def test_no_match(self):
        results = _search_workspace_for_file("不存在的文件", self.tmpdir)
        self.assertEqual(len(results), 0)

    def test_empty_workspace(self):
        results = _search_workspace_for_file("test", "")
        self.assertEqual(len(results), 0)

    def test_none_workspace(self):
        results = _search_workspace_for_file("test", None)  # type: ignore
        self.assertEqual(len(results), 0)


if __name__ == "__main__":
    unittest.main()
