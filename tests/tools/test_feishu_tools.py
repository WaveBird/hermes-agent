"""Tests for feishu_doc_tool and feishu_drive_tool — registration, schema, and URL parsing."""

import importlib
import unittest

from tools.registry import registry

# Trigger tool discovery so feishu tools get registered
importlib.import_module("tools.feishu_doc_tool")
importlib.import_module("tools.feishu_drive_tool")


class TestFeishuToolRegistration(unittest.TestCase):
    """Verify feishu tools are registered and have valid schemas."""

    EXPECTED_TOOLS = {
        "feishu_doc_read": "feishu_doc",
        "feishu_drive_list_comments": "feishu_drive",
        "feishu_drive_list_comment_replies": "feishu_drive",
        "feishu_drive_reply_comment": "feishu_drive",
        "feishu_drive_add_comment": "feishu_drive",
        "feishu_drive_search_docs": "feishu_drive",
    }

    def test_all_tools_registered(self):
        for tool_name, toolset in self.EXPECTED_TOOLS.items():
            entry = registry.get_entry(tool_name)
            self.assertIsNotNone(entry, f"{tool_name} not registered")
            self.assertEqual(entry.toolset, toolset)


    def test_drive_tools_require_file_token(self):
        # search_docs is keyword-based, not file_token-based
        skip_file_token = {"feishu_doc_read", "feishu_drive_search_docs"}
        for tool_name in self.EXPECTED_TOOLS:
            if tool_name in skip_file_token:
                continue
            entry = registry.get_entry(tool_name)
            props = entry.schema["parameters"].get("properties", {})
            self.assertIn("file_token", props, f"{tool_name} missing file_token param")
            self.assertIn("file_type", props, f"{tool_name} missing file_type param")

    def test_search_docs_schema(self):
        """Verify feishu_drive_search_docs has the expected schema."""
        entry = registry.get_entry("feishu_drive_search_docs")
        self.assertIsNotNone(entry)
        props = entry.schema["parameters"].get("properties", {})
        self.assertIn("query", props, "search_docs missing query param")
        self.assertIn("page_size", props, "search_docs missing page_size param")
        required = entry.schema["parameters"].get("required", [])
        self.assertIn("query", required, "search_docs should require query")


class TestFeishuUrlParsing(unittest.TestCase):
    """Test _parse_feishu_url for wiki/docx/plain-token inputs."""

    def setUp(self):
        from tools.feishu_doc_tool import _parse_feishu_url
        self.parse = _parse_feishu_url

    def test_plain_token(self):
        kind, token = self.parse("LVVzdXET1o4fvNxSYVucewoCnXf")
        self.assertEqual(kind, "doc_token")
        self.assertEqual(token, "LVVzdXET1o4fvNxSYVucewoCnXf")

    def test_docx_url(self):
        kind, token = self.parse(
            "https://upiwgvvcb4.feishu.cn/docx/LVVzdXET1o4fvNxSYVucewoCnXf"
        )
        self.assertEqual(kind, "doc_token")
        self.assertEqual(token, "LVVzdXET1o4fvNxSYVucewoCnXf")

    def test_wiki_url(self):
        kind, token = self.parse(
            "https://upiwgvvcb4.feishu.cn/wiki/FLD2wzG6DiDuGzkFu2Qcskuonhh"
        )
        self.assertEqual(kind, "wiki_node")
        self.assertEqual(token, "FLD2wzG6DiDuGzkFu2Qcskuonhh")

    def test_lark_url(self):
        kind, token = self.parse(
            "https://example.larkoffice.com/docx/AbCdEfGhIjKlMnOpQrSt"
        )
        self.assertEqual(kind, "doc_token")

    def test_empty(self):
        kind, token = self.parse("")
        self.assertIsNone(kind)
        self.assertIsNone(token)

    def test_non_feishu_url(self):
        kind, token = self.parse("https://github.com/repo")
        self.assertIsNone(kind)

    def test_sheet_url(self):
        kind, token = self.parse(
            "https://xxx.feishu.cn/sheets/AbCdEfGhIjKlMnOpQrSt"
        )
        self.assertEqual(kind, "obj_token")

    def test_url_with_query_params(self):
        kind, token = self.parse(
            "https://xxx.feishu.cn/wiki/FLD2wzG6DiDuGzkFu2Qcskuonhh?from=from_copylink"
        )
        self.assertEqual(kind, "wiki_node")
        self.assertEqual(token, "FLD2wzG6DiDuGzkFu2Qcskuonhh")


if __name__ == "__main__":
    unittest.main()
