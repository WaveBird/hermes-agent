"""Feishu Document Tool -- read document content via Feishu/Lark API.

Provides ``feishu_doc_read`` for reading document content as plain text.
Accepts a full Feishu/Lark URL (wiki or docx) or a raw doc_token — the tool
auto-resolves wiki node_token to obj_token internally so callers never need
to load a skill or do manual token conversion.
Uses the same lazy-import + BaseRequest pattern as feishu_comment.py.
"""

import json
import logging
import re
import threading

from tools.registry import registry, tool_error, tool_result

logger = logging.getLogger(__name__)

# Thread-local storage for the lark client injected by feishu_comment handler.
_local = threading.local()


def set_client(client):
    """Store a lark client for the current thread (called by feishu_comment)."""
    _local.client = client


def get_client():
    """Return the lark client for the current thread, or the module-level client.

    Falls back to the module-level client injected by the Feishu adapter at
    connection time, so feishu tools work in the main gateway agent (not just
    in the comment-agent context).
    """
    thread_client = getattr(_local, "client", None)
    if thread_client is not None:
        return thread_client
    return _module_client


def set_module_client(client):
    """Set the module-level lark client (called by Feishu adapter on connect).

    Unlike ``set_client`` (thread-local), this persists across threads and
    makes the client available to the main gateway agent, not just the
    comment agent. Set to ``None`` on disconnect to release the reference.
    """
    global _module_client
    _module_client = client


# Module-level client injected by the Feishu adapter on connect.
# Falls back to None until the adapter calls set_module_client().
_module_client = None


# ---------------------------------------------------------------------------
# feishu_doc_read
# ---------------------------------------------------------------------------

_RAW_CONTENT_URI = "/open-apis/docx/v1/documents/:document_id/raw_content"
_WIKI_GET_NODE_URI = "/open-apis/wiki/v2/spaces/get_node"

# Matches feishu/lark document URLs and extracts doc_type + token.
_FEISHU_DOC_URL_RE = re.compile(
    r"(?:feishu\.cn|larkoffice\.com|larksuite\.com|lark\.suite\.com)"
    r"/(?P<doc_type>wiki|doc|docx|sheet|sheets|slides|mindnote|bitable|base|file)"
    r"/(?P<token>[A-Za-z0-9_-]{10,40})"
)


def _parse_feishu_url(url_or_token: str):
    """Parse a Feishu URL or raw token into (kind, token).

    Returns one of:
        ("doc_token", "<token>")   — ready to use as document_id
        ("wiki_node", "<token>")   — needs wiki API resolution first
        ("obj_token", "<token>")   — non-docx obj_token (sheet/slides/etc.)
        (None, None)               — unrecognized input
    """
    if not url_or_token:
        return None, None

    s = url_or_token.strip()

    # Plain token: no slash, no protocol — treat as doc_token directly
    if "/" not in s and not s.startswith("http"):
        return "doc_token", s

    # Full URL: extract type + token
    m = _FEISHU_DOC_URL_RE.search(s)
    if m:
        dt = m.group("doc_type")
        tk = m.group("token")
        if dt == "wiki":
            return "wiki_node", tk
        elif dt in ("docx", "doc"):
            return "doc_token", tk
        else:
            return "obj_token", tk

    return None, None


FEISHU_DOC_READ_SCHEMA = {
    "name": "feishu_doc_read",
    "description": (
        "Read the full content of a Feishu/Lark document as plain text. "
        "Accepts a full Feishu/Lark URL (wiki or docx), or a raw doc_token. "
        "Wiki URLs are auto-resolved: the wiki node_token is converted to "
        "the underlying doc_token via the wiki API before reading content."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "doc_token": {
                "type": "string",
                "description": (
                    "The document token or full URL. Accepts: "
                    "wiki URL (https://xxx.feishu.cn/wiki/xxx), "
                    "docx URL (https://xxx.feishu.cn/docx/xxx), "
                    "or a raw doc_token string."
                ),
            },
        },
        "required": ["doc_token"],
    },
}


def _check_feishu():
    # Use ``importlib.util.find_spec`` — it checks whether ``lark_oapi``
    # is importable without actually executing its ``__init__``.
    # Executing the real import here costs ~5 seconds (the SDK eagerly
    # loads websockets, dispatcher, every api/v2 model) and this probe
    # fires at every ``hermes`` startup during tool-availability
    # evaluation.  Correctness is preserved because the actual tool
    # handler still does the real import when invoked.
    import importlib.util
    try:
        return importlib.util.find_spec("lark_oapi") is not None
    except (ImportError, ValueError):
        return False


def _resolve_wiki_node(client, wiki_token: str):
    """Resolve a wiki node_token to (obj_token, obj_type) via wiki API.

    Returns (obj_token, obj_type) or (None, None) on failure.
    """
    try:
        from lark_oapi import AccessTokenType
        from lark_oapi.core.enum import HttpMethod
        from lark_oapi.core.model.base_request import BaseRequest
    except ImportError:
        return None, None

    request = (
        BaseRequest.builder()
        .http_method(HttpMethod.GET)
        .uri(_WIKI_GET_NODE_URI)
        .token_types({AccessTokenType.TENANT})
        .queries([("token", wiki_token)])
        .build()
    )
    response = client.request(request)

    code = getattr(response, "code", None)
    if code != 0:
        msg = getattr(response, "msg", "unknown error")
        logger.warning("Wiki node resolve failed: code=%s msg=%s token=%s", code, msg, wiki_token)
        return None, None

    raw = getattr(response, "raw", None)
    if raw and hasattr(raw, "content"):
        try:
            body = json.loads(raw.content)
            node = body.get("data", {}).get("node", {})
            return node.get("obj_token"), node.get("obj_type")
        except (json.JSONDecodeError, AttributeError):
            pass

    # Fallback: response.data
    data = getattr(response, "data", None)
    if data:
        if isinstance(data, dict):
            node = data.get("node", {})
            return node.get("obj_token"), node.get("obj_type")
        node = getattr(data, "node", None)
        if node:
            return getattr(node, "obj_token", None), getattr(node, "obj_type", None)

    return None, None


def _read_doc_content(client, doc_token: str) -> str:
    """Read raw content for a doc_token. Returns content string or raises."""
    try:
        from lark_oapi import AccessTokenType
        from lark_oapi.core.enum import HttpMethod
        from lark_oapi.core.model.base_request import BaseRequest
    except ImportError:
        return tool_error("lark_oapi not installed")

    request = (
        BaseRequest.builder()
        .http_method(HttpMethod.GET)
        .uri(_RAW_CONTENT_URI)
        .token_types({AccessTokenType.TENANT})
        .paths({"document_id": doc_token})
        .build()
    )
    response = client.request(request)

    code = getattr(response, "code", None)
    if code != 0:
        msg = getattr(response, "msg", "unknown error")
        return tool_error(f"Failed to read document: code={code} msg={msg}")

    raw = getattr(response, "raw", None)
    if raw and hasattr(raw, "content"):
        try:
            body = json.loads(raw.content)
            content = body.get("data", {}).get("content", "")
            return tool_result(success=True, content=content)
        except (json.JSONDecodeError, AttributeError):
            pass

    # Fallback: try response.data
    data = getattr(response, "data", None)
    if data:
        if isinstance(data, dict):
            content = data.get("content", "")
        else:
            content = getattr(data, "content", str(data))
        return tool_result(success=True, content=content)

    return tool_error("No content returned from document API")


def _handle_feishu_doc_read(args: dict, **kwargs) -> str:
    raw_input = args.get("doc_token", "").strip()
    if not raw_input:
        return tool_error("doc_token is required")

    client = get_client()
    if client is None:
        return tool_error("Feishu client not available (not in a Feishu comment context)")

    # Parse input: could be a URL (wiki/docx) or a plain token
    kind, token = _parse_feishu_url(raw_input)
    if kind is None:
        return tool_error(
            f"Unrecognized input: {raw_input!r}. "
            "Expected a Feishu/Lark URL or a doc_token."
        )

    # If it's a wiki URL, resolve node_token → obj_token first
    if kind == "wiki_node":
        obj_token, obj_type = _resolve_wiki_node(client, token or "")
        if not obj_token:
            return tool_error(
                f"Failed to resolve wiki node_token {token!r} to obj_token"
            )
        if obj_type and obj_type not in ("docx", "doc"):
            return tool_error(
                f"Wiki node resolves to type {obj_type!r}, "
                "not a text document (docx). Cannot read raw content."
            )
        doc_token = obj_token or ""
    elif kind == "obj_token":
        return tool_error(
            f"Token {token!r} is a {kind} type, not a docx document. "
            "Only wiki and docx URLs are supported."
        )
    else:
        # "doc_token" — use directly
        doc_token = token or ""

    return _read_doc_content(client, doc_token)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

registry.register(
    name="feishu_doc_read",
    toolset="feishu_doc",
    schema=FEISHU_DOC_READ_SCHEMA,
    handler=_handle_feishu_doc_read,
    check_fn=_check_feishu,
    requires_env=[],
    is_async=False,
    description="Read Feishu document content",
    emoji="\U0001f4c4",
)
