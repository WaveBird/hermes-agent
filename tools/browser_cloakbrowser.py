"""CloakBrowser backend for browser tool — anti-detection Chromium via Playwright.

This module provides a full implementation of browser operations using the
CloakBrowser Python package (stealth-patched Chromium). When
``CLOAKBROWSER_MODE=true`` is set, all browser tools route through here
instead of the agent-browser CLI.

Architecture:
    Playwright async API runs on a dedicated background asyncio event loop
    (one per session).  This avoids the "Sync API inside asyncio loop"
    error that occurs when the gateway's main loop detects a sync Playwright
    call.  Public functions are sync; they marshal calls to the background
    loop via ``asyncio.run_coroutine_threadsafe`` and return JSON strings.

Each agent task gets an isolated browser session (browser → context → page)
managed in-process via a thread-safe dict.  The browser launches in headed
mode by default so users can watch operations live.

Setup:
    1. ``pip install cloakbrowser`` (installs stealth Chromium ~300MB)
    2. Set ``CLOAKBROWSER_MODE=true`` in ``~/.hermes/.env``
    3. (Optional) ``CLOAKBROWSER_HEADLESS=false`` — visible by default
"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Coroutine

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_DEFAULT_TIMEOUT = 30_000  # ms for Playwright operations
_SNAPSHOT_MAX_CHARS = 80_000
_CLOAKBROWSER_CACHE_DIR = Path.home() / ".cloakbrowser"
_SCREENSHOTS_DIR = _CLOAKBROWSER_CACHE_DIR / "screenshots"
_NETWORK_CACHE_DIR = _CLOAKBROWSER_CACHE_DIR / "network"
_MAX_NETWORK_RECORDS = 200  # auto-flush threshold
_MAX_BODY_BYTES = 100_000    # max body size to store (100KB)

logger = logging.getLogger(__name__)


def is_cloakbrowser_mode() -> bool:
    """Return True when CloakBrowser backend is enabled and no CDP override."""
    if os.getenv("BROWSER_CDP_URL", "").strip():
        return False
    return os.getenv("CLOAKBROWSER_MODE", "").lower() in ("1", "true", "yes")


def _get_cloakbrowser_url() -> str:
    """Legacy helper — CloakBrowser runs in-process, not as a remote service."""
    return os.getenv("CLOAKBROWSER_URL", "")


# ---------------------------------------------------------------------------
# Headless config
# ---------------------------------------------------------------------------

def _is_headless() -> bool:
    """Determine headless mode.

    Priority:
        1. ``CLOAKBROWSER_HEADLESS`` env var (explicit)
        2. ``browser.cloakbrowser.headless`` config
        3. Default: ``True`` (headless for production; set False to see the window)
    """
    env_val = os.getenv("CLOAKBROWSER_HEADLESS", "").strip().lower()
    if env_val in ("1", "true", "yes"):
        return True
    if env_val in ("0", "false", "no"):
        return False
    try:
        from hermes_cli.config import load_config
        cfg = load_config().get("browser", {}).get("cloakbrowser", {})
        if isinstance(cfg, dict):
            val = cfg.get("headless")
            if val is not None:
                return bool(val)
    except Exception:
        pass
    return True


# ---------------------------------------------------------------------------
# Session management with background asyncio loop
# ---------------------------------------------------------------------------
# Maps task_id -> {
#   "browser": Browser (async),
#   "context": BrowserContext (async),
#   "page": Page (async),
#   "logs": List[str],
#   "loop": asyncio.AbstractEventLoop,
#   "thread": threading.Thread,
#   "ready": threading.Event,
# }
_sessions: Dict[str, Dict[str, Any]] = {}
_sessions_lock = threading.Lock()


def _tool_error(msg: str) -> str:
    return json.dumps({"success": False, "error": msg})


def _import_cloakbrowser_launch():
    """Lazy-import cloakbrowser.launch_async to avoid startup overhead."""
    from cloakbrowser import launch_async
    return launch_async


def _run_session_coro(task_id: str, coro: Coroutine) -> Any:
    """Run an async coroutine on a session's background event loop.

    Blocks the calling thread until the coroutine completes or times out.
    Raises the coroutine's exception on failure.
    """
    session = _sessions.get(task_id)
    if not session:
        raise RuntimeError(f"No CloakBrowser session for task {task_id}")
    loop: asyncio.AbstractEventLoop = session["loop"]
    fut = asyncio.run_coroutine_threadsafe(coro, loop)
    return fut.result(timeout=120)


async def _async_get_session(task_id: Optional[str], headless: bool) -> Dict[str, Any]:
    """Create or retrieve an async browser session.

    Must be called from the session's own background event loop.
    """
    task_id = task_id or "default"
    if task_id in _sessions:
        return _sessions[task_id]

    logger.info(
        "Launching CloakBrowser for task %s (headless=%s)",
        task_id, headless,
    )

    launch_async = _import_cloakbrowser_launch()

    # Read proxy from Hermes env vars (set via .env or hermes_proxy_on/off).
    # Pass it to Playwright as per-context proxy so Chromium uses Clash for
    # external sites while BYD internal traffic is handled by Clash's bypass rules.
    # Clear proxy env vars *before* launch_async so Chromium doesn't get a
    # blanket env-level proxy (which would route ALL traffic including BYD
    # internal sites through Clash, causing ERR_CONNECTION_RESET on misconfigured
    # Clash rules).  Playwright's per-context proxy is the correct mechanism.
    _proxy_vars = {}
    _proxy_url = None
    for _k in ['HTTPS_PROXY', 'HTTP_PROXY', 'ALL_PROXY', 'https_proxy', 'http_proxy', 'all_proxy']:
        _v = os.environ.get(_k)
        if _v:
            _proxy_vars[_k] = _v
            if _proxy_url is None:
                _proxy_url = _v  # first valid proxy wins
            del os.environ[_k]

    browser = await launch_async(headless=headless)

    # Restore proxy vars for the rest of the process
    os.environ.update(_proxy_vars)

    # Pass proxy to Playwright context level (not env level).
    # This lets Clash's bypass rules handle BYD internal traffic correctly,
    # while external sites go through the proxy.
    _context_kwargs = {"no_viewport": True}
    if _proxy_url:
        _context_kwargs["proxy"] = {"server": _proxy_url}
    context = await browser.new_context(**_context_kwargs)
    page = await context.new_page()

    # Create session dict first (so handlers can reference it)
    logs: List[str] = []
    session = {
        "browser": browser,
        "context": context,
        "page": page,
        "logs": logs,
        "network_enabled": False,  # Lazy: off by default
        "network_records": [],    # In-memory buffer
        "network_lock": threading.Lock(),
    }

    # Collect console messages
    page.on("console", lambda msg: logs.append(f"[{msg.type}] {msg.text}"))

    # Network capture now uses CDP Network.enable (registered on-demand via start action).
    # No page.on("response") / page.on("requestfailed") here — CDP captures everything.

    _sessions[task_id] = session
    return session


async def _async_destroy_session(task_id: str) -> None:
    """Close a browser session from within its event loop."""
    session = _sessions.pop(task_id, None)
    if not session:
        return
    try:
        await session["page"].close()
    except Exception:
        pass
    try:
        await session["context"].close()
    except Exception:
        pass
    try:
        await session["browser"].close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Session lifecycle (sync public API)
# ---------------------------------------------------------------------------

def _get_session(task_id: Optional[str]) -> Dict[str, Any]:
    """Create (if needed) and return a session dict, blocking.

    The session dict holds, among other things:
      - ``loop``: the dedicated asyncio event loop
      - ``page``: the async Playwright Page
    """
    tid = task_id or "default"
    with _sessions_lock:
        if tid in _sessions:
            return _sessions[tid]

    headless = _is_headless()

    # Spin up a background loop thread for this session
    ready = threading.Event()
    loop_holder: List[Optional[asyncio.AbstractEventLoop]] = [None]
    start_error: List[Optional[Exception]] = [None]

    def _bg_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop_holder[0] = loop
        ready.set()  # signal that the loop is ready
        try:
            # Run until stopped
            loop.run_forever()
        except Exception as e:
            start_error[0] = e
        finally:
            # Cancel remaining tasks
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            try:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            except Exception:
                pass
            loop.close()

    thread = threading.Thread(
        target=_bg_loop, daemon=True,
        name=f"cloakbrowser-{tid}",
    )
    thread.start()

    # Wait for the loop to start
    if not ready.wait(timeout=10.0):
        raise RuntimeError("CloakBrowser background loop failed to start")
    loop = loop_holder[0]
    if loop is None:
        raise RuntimeError("CloakBrowser background loop failed to start")
    if start_error[0]:
        raise start_error[0]

    # Create the browser session on the background loop
    session_raw: Dict[str, Any] = {"loop": loop, "thread": thread}

    try:
        real_session = _run_on_loop(loop, _async_get_session(tid, headless))
    except Exception:
        # Clean up the background loop
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        raise

    real_session["loop"] = loop
    real_session["thread"] = thread

    with _sessions_lock:
        _sessions[tid] = real_session

    return real_session


def _run_on_loop(loop: asyncio.AbstractEventLoop, coro: Coroutine) -> Any:
    """Run a coroutine on a background loop and block for the result."""
    fut = asyncio.run_coroutine_threadsafe(coro, loop)
    return fut.result(timeout=120)


# ---------------------------------------------------------------------------
# Cleanup / close (sync)
# ---------------------------------------------------------------------------

def cloakbrowser_soft_cleanup(task_id: Optional[str] = None) -> bool:
    """Try to gracefully close the session. Returns True if session existed."""
    tid = task_id or "default"
    with _sessions_lock:
        session = _sessions.get(tid)
    if not session:
        return False
    try:
        loop = session["loop"]
        _run_on_loop(loop, _async_destroy_session(tid))
        loop.call_soon_threadsafe(loop.stop)
        session["thread"].join(timeout=10)
    except Exception:
        pass
    return True


def cloakbrowser_close(task_id: Optional[str] = None) -> None:
    """Force-close a browser session."""
    tid = task_id or "default"
    cloakbrowser_soft_cleanup(tid)


# ---------------------------------------------------------------------------
# Network Capture — lazy, on-demand request/response recording
# ---------------------------------------------------------------------------
# Network records stored as JSONL files organized by domain + date:
#   ~/.cloakbrowser/network/{YYYY-MM-DD}/{domain}.jsonl
# Each record: {id, url, method, status, request_headers, response_headers,
#               resource_type, timestamp, body_ref (optional), error (optional)}

def _network_domain_from_url(url: str) -> str:
    """Extract a safe domain name for filesystem use."""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or "unknown"
        # Sanitize: replace problematic chars with underscore
        domain = re.sub(r"[^\w\-.]", "_", domain)
        return domain[:100] or "unknown"
    except Exception:
        return "unknown"


def _network_date_str(ts_ms: float) -> str:
    """Convert epoch milliseconds to YYYY-MM-DD string."""
    from datetime import datetime, timezone
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _network_record_to_jsonl(rec: dict) -> str:
    """Compact JSON serialization for a network record."""
    return json.dumps(rec, ensure_ascii=False, separators=(",", ":"))


def _flush_network_records_to_disk(records: List[dict], task_id: str) -> int:
    """Write captured network records to disk, organized by domain + date.

    Returns number of records flushed.
    """
    if not records:
        return 0
    _NETWORK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    flushed = 0
    # Group by (date, domain)
    groups: Dict[str, dict] = {}
    for rec in records:
        date_str = _network_date_str(rec.get("timestamp", time.time() * 1000))
        domain = _network_domain_from_url(rec.get("url", ""))
        key = f"{date_str}/{domain}"
        if key not in groups:
            groups[key] = {"date": date_str, "domain": domain, "records": []}
        groups[key]["records"].append(rec)

    for key, grp in groups.items():
        date_dir = _NETWORK_CACHE_DIR / grp["date"]
        date_dir.mkdir(parents=True, exist_ok=True)
        file_path = date_dir / f"{grp['domain']}.jsonl"
        # Append mode — each line is one record
        with open(file_path, "a", encoding="utf-8") as f:
            for rec in grp["records"]:
                f.write(_network_record_to_jsonl(rec) + "\n")
                flushed += 1
    return flushed


def _load_network_records_from_disk(
    domain_filter: Optional[str] = None,
    date_filter: Optional[str] = None,
    limit: int = 100,
) -> List[dict]:
    """Load network records from disk files, optionally filtered.

    Returns list of records (most recent first).
    """
    records = []
    try:
        if date_filter:
            # Load from specific date
            date_dir = _NETWORK_CACHE_DIR / date_filter
            if not date_dir.exists():
                return []
            for file in date_dir.glob("*.jsonl"):
                if domain_filter and file.stem != domain_filter:
                    continue
                with open(file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            records.append(json.loads(line))
        else:
            # Scan all available dates (most recent first)
            for date_dir in sorted(_NETWORK_CACHE_DIR.iterdir(), reverse=True):
                if not date_dir.is_dir():
                    continue
                for file in date_dir.glob("*.jsonl"):
                    if domain_filter and file.stem != domain_filter:
                        continue
                    with open(file, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                records.append(json.loads(line))
        # Sort by timestamp descending, apply limit
        records.sort(key=lambda r: r.get("timestamp", 0), reverse=True)
        return records[:limit]
    except Exception as e:
        logger.warning("Failed to load network records from disk: %s", e)
        return []


def _list_available_network_files() -> List[dict]:
    """List all available network capture files with metadata.

    Returns: [{date, domain, file_path, record_count, size_bytes}, ...]
    """
    files = []
    try:
        for date_dir in sorted(_NETWORK_CACHE_DIR.iterdir(), reverse=True):
            if not date_dir.is_dir():
                continue
            for file in date_dir.glob("*.jsonl"):
                size = file.stat().st_size
                # Count lines
                with open(file, "r", encoding="utf-8") as f:
                    count = sum(1 for _ in f)
                files.append({
                    "date": date_dir.name,
                    "domain": file.stem,
                    "file_path": str(file),
                    "record_count": count,
                    "size_bytes": size,
                })
    except Exception as e:
        logger.warning("Failed to list network files: %s", e)
    return files


def _get_network_record_by_id(request_id: str) -> Optional[dict]:
    """Find a specific network record by ID from disk files."""
    try:
        for date_dir in _NETWORK_CACHE_DIR.iterdir():
            if not date_dir.is_dir():
                continue
            for file in date_dir.glob("*.jsonl"):
                with open(file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            rec = json.loads(line)
                            if rec.get("id") == request_id:
                                return rec
    except Exception as e:
        logger.warning("Failed to find network record %s: %s", request_id, e)
    return None


# --- Response body storage (optional, on-demand) -------------------------

def _store_response_body(request_id: str, body: bytes, task_id: str) -> Optional[str]:
    """Store response body to disk, return file path if stored."""
    if len(body) > _MAX_BODY_BYTES:
        return None  # skip oversized bodies
    bodies_dir = _NETWORK_CACHE_DIR / "bodies"
    bodies_dir.mkdir(parents=True, exist_ok=True)
    body_path = bodies_dir / f"{request_id}.bin"
    try:
        body_path.write_bytes(body)
        return str(body_path)
    except Exception as e:
        logger.warning("Failed to store body for %s: %s", request_id, e)
        return None


def _load_response_body(request_id: str) -> Optional[str]:
    """Load stored response body, return content as string (if text)."""
    body_path = _NETWORK_CACHE_DIR / "bodies" / f"{request_id}.bin"
    if not body_path.exists():
        return None
    try:
        return body_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        # Try reading as bytes and decode
        try:
            data = body_path.read_bytes()
            return data.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning("Failed to load body for %s: %s", request_id, e)
            return None


def _clear_all_network_data() -> int:
    """Clear all stored network capture data (files + bodies).

    Returns number of files deleted.
    """
    deleted = 0
    try:
        for item in _NETWORK_CACHE_DIR.iterdir():
            if item.is_dir():
                for sub in item.iterdir():
                    if sub.is_file():
                        sub.unlink()
                        deleted += 1
                # Remove empty subdirs
                try:
                    item.rmdir()
                except OSError:
                    pass
            elif item.is_file():
                item.unlink()
                deleted += 1
    except Exception as e:
        logger.warning("Failed to clear network data: %s", e)
    return deleted


# --- CDP-based Network Capture (async helpers) ---------------------------
# Uses Chrome DevTools Protocol Network.enable to capture ALL requests,
# including subresources (CSS, JS, images, XHR, fetch, etc.).
# Response bodies are retrieved on-demand via Network.getResponseBody —
# the browser itself holds the data, no need to store files ourselves.

async def _async_start_network_cdp(session: dict) -> dict:
    """Create a CDP session, enable Network domain, and register event listeners.

    Returns a dict with the CDP session object and event registration status.
    """
    context = session.get("context")
    page = session.get("page")
    if not context or not page:
        raise RuntimeError("Browser session missing context/page")

    cdp = await context.new_cdp_session(page)
    session["cdp_session"] = cdp

    # Enable Network domain with reasonable buffer sizes
    await cdp.send("Network.enable", {
        "maxTotalBufferSize": 1000000,
        "maxResourceBufferSize": 500000,
        "maxPostDataSize": 65536,
    })

    # Register CDP event listeners — they populate session["network_records"]
    def _on_request_will_be_sent(evt):
        if not session.get("network_enabled", False):
            return
        try:
            request = evt.get("request", {})
            rec = {
                "id": evt.get("requestId", ""),
                "url": request.get("url", ""),
                "method": request.get("method", "GET"),
                "request_headers": request.get("headers", {}),
                "resource_type": evt.get("type", "Other"),
                "timestamp": evt.get("timestamp", time.time()),
                "wall_time": evt.get("wallTime", time.time()),
                "post_data": request.get("postData", None),
                "has_post_data": request.get("hasPostData", False),
                # Response fields filled later by responseReceived
                "status": None,
                "status_text": None,
                "response_headers": None,
                "mime_type": None,
                "body_available": False,
                "body_size": None,
                "error": None,
            }
            lock = session.get("network_lock")
            if lock:
                with lock:
                    session["network_records"].append(rec)
            else:
                session["network_records"].append(rec)
        except Exception as e:
            logger.debug("CDP Network.requestWillBeSent error: %s", e)

    def _on_response_received(evt):
        if not session.get("network_enabled", False):
            return
        try:
            resp = evt.get("response", {})
            req_id = evt.get("requestId", "")
            # Find matching request record and update it
            lock = session.get("network_lock")
            records = session.get("network_records", [])
            if lock:
                with lock:
                    for rec in records:
                        if rec.get("id") == req_id:
                            rec["status"] = resp.get("status")
                            rec["status_text"] = resp.get("statusText", "")
                            rec["response_headers"] = resp.get("headers", {})
                            rec["mime_type"] = resp.get("mimeType", "")
                            rec["body_available"] = True
                            rec["body_size"] = resp.get("bodySize", None)
                            break
            else:
                for rec in records:
                    if rec.get("id") == req_id:
                        rec["status"] = resp.get("status")
                        rec["status_text"] = resp.get("statusText", "")
                        rec["response_headers"] = resp.get("headers", {})
                        rec["mime_type"] = resp.get("mimeType", "")
                        rec["body_available"] = True
                        rec["body_size"] = resp.get("bodySize", None)
                        break
        except Exception as e:
            logger.debug("CDP Network.responseReceived error: %s", e)

    def _on_loading_finished(evt):
        if not session.get("network_enabled", False):
            return
        try:
            req_id = evt.get("requestId", "")
            lock = session.get("network_lock")
            records = session.get("network_records", [])
            if lock:
                with lock:
                    for rec in records:
                        if rec.get("id") == req_id:
                            rec["body_available"] = True
                            break
            else:
                for rec in records:
                    if rec.get("id") == req_id:
                        rec["body_available"] = True
                        break
        except Exception as e:
            logger.debug("CDP Network.loadingFinished error: %s", e)

    def _on_request_failed(evt):
        if not session.get("network_enabled", False):
            return
        try:
            req_id = evt.get("requestId", "")
            lock = session.get("network_lock")
            records = session.get("network_records", [])
            if lock:
                with lock:
                    for rec in records:
                        if rec.get("id") == req_id:
                            rec["status"] = 0
                            rec["status_text"] = "FAILED"
                            rec["error"] = evt.get("errorText", "Request failed")
                            rec["body_available"] = False
                            break
            else:
                for rec in records:
                    if rec.get("id") == req_id:
                        rec["status"] = 0
                        rec["status_text"] = "FAILED"
                        rec["error"] = evt.get("errorText", "Request failed")
                        rec["body_available"] = False
                        break
        except Exception as e:
            logger.debug("CDP Network.requestFailed error: %s", e)

    cdp.on("Network.requestWillBeSent", _on_request_will_be_sent)
    cdp.on("Network.responseReceived", _on_response_received)
    cdp.on("Network.loadingFinished", _on_loading_finished)
    cdp.on("Network.requestFailed", _on_request_failed)

    return {"cdp": cdp, "enabled": True}


async def _async_stop_network_cdp(session: dict) -> None:
    """Disable Network domain and detach the CDP session."""
    cdp = session.get("cdp_session")
    if cdp:
        try:
            await cdp.send("Network.disable")
        except Exception:
            pass
        try:
            await cdp.detach()
        except Exception:
            pass
        session["cdp_session"] = None


async def _async_get_response_body(session: dict, request_id: str) -> Optional[str]:
    """Retrieve response body from browser via CDP Network.getResponseBody.

    Returns the body content as a string, or None if unavailable.
    """
    cdp = session.get("cdp_session")
    if not cdp:
        return None
    try:
        result = await cdp.send("Network.getResponseBody", {"requestId": request_id})
        body = result.get("body", "")
        base64 = result.get("base64Encoded", False)
        if base64 and body:
            # Decode base64 body
            import base64 as b64mod
            return b64mod.b64decode(body).decode("utf-8", errors="replace")
        return body
    except Exception as e:
        logger.debug("CDP getResponseBody error for %s: %s", request_id, e)
        return None


# --- Public: cloakbrowser_network tool -----------------------------------

def cloakbrowser_network(
    action: str,
    domain: Optional[str] = None,
    method: Optional[str] = None,
    status: Optional[str] = None,
    resource_type: Optional[str] = None,
    request_id: Optional[str] = None,
    include_body: bool = False,
    limit: int = 50,
    task_id: Optional[str] = None,
) -> str:
    """Network capture tool for CloakBrowser.

    Actions:
      - "start": Enable network capture for this session
      - "stop": Disable capture and flush records to disk
      - "flush": Flush in-memory records to disk (capture continues)
      - "list": List captured records (memory + disk), filtered
      - "get": Get full details of a specific request by ID
      - "stats": Summary statistics of captured data
      - "files": List available network capture files by domain/date
      - "clear": Delete all stored network data

    Filters (for "list" action):
      - domain: Filter by domain (e.g., "google.com")
      - method: Filter by HTTP method (GET, POST, etc.)
      - status: Filter by status category ("2xx", "3xx", "4xx", "5xx", "failed")
      - resource_type: Filter by type (document, script, stylesheet, image, xhr, fetch)
      - limit: Max records to return (default 50)
    """
    try:
        tid = task_id or "default"
        action = action.lower().strip()

        # --- start: enable CDP-based capture ----------------------------
        if action == "start":
            session = _get_session(tid)
            session["network_enabled"] = True
            session["network_records"] = session.get("network_records", [])
            session["network_lock"] = session.get("network_lock") or threading.Lock()
            # Create CDP session and enable Network domain
            _run_session_coro(tid, _async_start_network_cdp(session))
            cdp_active = session.get("cdp_session") is not None
            return json.dumps({
                "success": True,
                "message": f"Network capture enabled for session '{tid}' via CDP",
                "backend": "cloakbrowser",
                "cdp_active": cdp_active,
            })

        # --- stop: disable CDP capture -----------------------------------
        elif action == "stop":
            with _sessions_lock:
                session = _sessions.get(tid)
            if not session:
                return json.dumps({"success": True, "message": "No session to stop"})
            session["network_enabled"] = False
            # Optionally flush remaining records to disk for persistence
            records = session.get("network_records", [])
            flushed = _flush_network_records_to_disk(records, tid)
            session["network_records"] = []
            # Disable CDP Network domain and detach session
            _run_session_coro(tid, _async_stop_network_cdp(session))
            return json.dumps({
                "success": True,
                "message": f"Network capture disabled. Flushed {flushed} records to disk.",
                "flushed_count": flushed,
                "cdp_detached": True,
            })

        # --- flush: write to disk but keep capturing ----------------------
        elif action == "flush":
            with _sessions_lock:
                session = _sessions.get(tid)
            if not session:
                return json.dumps({"success": False, "error": "No active session"})
            records = session.get("network_records", [])
            flushed = _flush_network_records_to_disk(records, tid)
            session["network_records"] = []
            return json.dumps({
                "success": True,
                "message": f"Flushed {flushed} records to disk. Capture continues.",
                "flushed_count": flushed,
            })

        # --- list: query records -----------------------------------------
        elif action == "list":
            # Combine in-memory + disk records
            all_records = []
            with _sessions_lock:
                session = _sessions.get(tid)
            if session:
                all_records.extend(session.get("network_records", []))
            # Load from disk with filters
            disk_records = _load_network_records_from_disk(
                domain_filter=domain,
                date_filter=None,  # all dates
                limit=limit * 2,   # allow room for filtering
            )
            all_records.extend(disk_records)
            # Apply filters
            filtered = []
            for rec in all_records:
                if domain and _network_domain_from_url(rec.get("url", "")) != domain:
                    continue
                if method and rec.get("method", "").upper() != method.upper():
                    continue
                if status:
                    st = rec.get("status", 0)
                    st_cat = "failed" if st == 0 else f"{st // 100}xx"
                    if st_cat != status.lower():
                        continue
                if resource_type and rec.get("resource_type") != resource_type:
                    continue
                filtered.append(rec)
            # Sort by timestamp descending, apply limit
            filtered.sort(key=lambda r: r.get("timestamp", 0), reverse=True)
            result = filtered[:limit]
            # Compact output: only key fields for list view
            compact = [
                {
                    "id": r.get("id"),
                    "url": r.get("url", "")[:120],
                    "method": r.get("method"),
                    "status": r.get("status"),
                    "resource_type": r.get("resource_type"),
                    "timestamp": r.get("timestamp"),
                }
                for r in result
            ]
            return json.dumps({
                "success": True,
                "count": len(compact),
                "total_available": len(filtered),
                "records": compact,
            })

        # --- get: full details of one request ----------------------------
        elif action == "get":
            if not request_id:
                return json.dumps({"success": False, "error": "request_id required for 'get' action"})
            # Find in memory
            rec = None
            with _sessions_lock:
                session = _sessions.get(tid)
            if session:
                for r in session.get("network_records", []):
                    if r.get("id") == request_id:
                        rec = copy.deepcopy(r)  # Deep copy to avoid mutating original
                        break
            # Find on disk
            if not rec:
                rec = _get_network_record_by_id(request_id)
            if not rec:
                return json.dumps({"success": False, "error": f"Request ID '{request_id}' not found"})
            # Optionally load body via CDP (on-demand, browser itself holds the data)
            if include_body and rec.get("body_available") and session and session.get("cdp_session"):
                body = _run_session_coro(tid, _async_get_response_body(session, request_id))
                rec["body"] = body[:5000] if body else None  # truncate for LLM context
            elif include_body:
                rec["body"] = None  # Body not available (not loaded yet or CDP not active)
            return json.dumps({"success": True, "record": rec})

        # --- stats: summary ----------------------------------------------
        elif action == "stats":
            stats = {"in_memory": 0, "on_disk": 0, "domains": [], "dates": [], "methods": {}, "statuses": {}}
            with _sessions_lock:
                session = _sessions.get(tid)
            if session:
                stats["in_memory"] = len(session.get("network_records", []))
                stats["capture_enabled"] = session.get("network_enabled", False)
                # Include in-memory records in method/status distribution
                for r in session.get("network_records", []):
                    m = r.get("method", "UNKNOWN")
                    stats["methods"][m] = stats["methods"].get(m, 0) + 1
                    st = r.get("status", 0)
                    st_cat = "failed" if st == 0 else f"{st // 100}xx"
                    stats["statuses"][st_cat] = stats["statuses"].get(st_cat, 0) + 1
            # Disk stats
            files = _list_available_network_files()
            stats["files"] = len(files)
            stats["on_disk"] = sum(f.get("record_count", 0) for f in files)
            stats["domains"] = sorted([d for d in set(f.get("domain") for f in files) if d])
            stats["dates"] = sorted([d for d in set(f.get("date") for f in files) if d], reverse=True)
            # Method/status distribution from disk sample (add to in-memory counts)
            sample = _load_network_records_from_disk(limit=200)
            for r in sample:
                m = r.get("method", "UNKNOWN")
                stats["methods"][m] = stats["methods"].get(m, 0) + 1
                st = r.get("status", 0)
                st_cat = "failed" if st == 0 else f"{st // 100}xx"
                stats["statuses"][st_cat] = stats["statuses"].get(st_cat, 0) + 1
            return json.dumps({"success": True, "stats": stats})

        # --- files: list available capture files -------------------------
        elif action == "files":
            files = _list_available_network_files()
            return json.dumps({
                "success": True,
                "count": len(files),
                "files": files,
                "network_dir": str(_NETWORK_CACHE_DIR),
            })

        # --- clear: delete all data --------------------------------------
        elif action == "clear":
            deleted = _clear_all_network_data()
            # Also clear in-memory
            with _sessions_lock:
                session = _sessions.get(tid)
            if session:
                session["network_records"] = []
            return json.dumps({
                "success": True,
                "message": f"Deleted {deleted} network capture files",
                "deleted_files": deleted,
            })

        else:
            return json.dumps({
                "success": False,
                "error": f"Unknown action '{action}'. Valid: start, stop, flush, list, get, stats, files, clear",
            })

    except Exception as e:
        return _tool_error(f"Network capture error: {e}")


# ---------------------------------------------------------------------------
# Snapshot helpers (async)
# ---------------------------------------------------------------------------

_JS_SNAPSHOT_SCRIPT = """
(() => {
    // Elements that are interactive or semantically important
    const INTERACTIVE_TAGS = new Set([
        'a', 'button', 'input', 'select', 'textarea', 'details', 'summary',
        'dialog', 'menu', 'menuitem', 'option', 'optgroup', 'fieldset',
        'form', 'label', 'legend', 'datalist', 'output',
        'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'img',
        'nav', 'header', 'footer', 'main', 'section', 'aside',
        'table', 'th', 'td', 'tr', 'ul', 'ol', 'li', 'dl', 'dt', 'dd',
        'video', 'audio', 'iframe', 'svg', 'canvas',
    ]);
    // Text containers only if they have meaningful direct text
    const TEXT_CONTAINERS = new Set([
        'p', 'pre', 'code', 'blockquote', 'figcaption',
        'article', 'strong', 'em', 'b', 'i', 'u', 's', 'sub', 'sup',
        'abbr', 'cite', 'dfn', 'kbd', 'mark', 'q', 'samp', 'small',
        'time', 'var',
    ]);
    const INTERACTIVE_ROLES = new Set([
        'button', 'link', 'textbox', 'searchbox', 'combobox', 'checkbox',
        'radio', 'switch', 'menuitem', 'menuitemcheckbox', 'menuitemradio',
        'tab', 'slider', 'spinbutton', 'scrollbar', 'treeitem',
        'option', 'listbox', 'dialog', 'gridcell', 'columnheader',
        'rowheader', 'heading', 'img', 'navigation', 'banner',
        'contentinfo', 'main', 'complementary', 'alert', 'alertdialog',
        'status', 'log', 'marquee', 'timer', 'tooltip',
    ]);
    const GLOBAL_ROLES = new Set([
        'banner', 'complementary', 'contentinfo', 'form', 'main',
        'navigation', 'region', 'search', 'alert', 'alertdialog',
        'dialog', 'status', 'log', 'marquee', 'timer', 'tooltip',
        'heading', 'img', 'button', 'link', 'listbox', 'menu',
        'menubar', 'meter', 'option', 'progressbar', 'radio',
        'radiogroup', 'scrollbar', 'searchbox', 'slider', 'spinbutton',
        'switch', 'tab', 'tablist', 'tabpanel', 'textbox',
        'tree', 'treegrid', 'treeitem', 'checkbox', 'combobox',
        'grid', 'gridcell', 'columnheader', 'rowheader', 'rowgroup',
        'definition', 'group', 'list', 'listitem', 'note',
        'paragraph', 'separator', 'table', 'term', 'presentation',
    ]);

    const lines = [];
    let idx = 0;

    function getRole(el) {
        const explicit = el.getAttribute('role');
        if (explicit) return explicit;
        const tag = el.tagName.toLowerCase();
        const roleMap = {
            'a': 'link', 'button': 'button', 'input': 'textbox',
            'select': 'combobox', 'textarea': 'textbox',
            'h1': 'heading', 'h2': 'heading', 'h3': 'heading',
            'h4': 'heading', 'h5': 'heading', 'h6': 'heading',
            'img': 'img', 'form': 'form', 'table': 'table',
            'ul': 'list', 'ol': 'list', 'li': 'listitem',
            'nav': 'navigation', 'header': 'banner',
            'footer': 'contentinfo', 'main': 'main',
            'aside': 'complementary', 'section': 'region',
            'dialog': 'dialog', 'summary': 'button',
            'details': 'group', 'meter': 'meter',
            'progress': 'progressbar', 'option': 'option',
            'datalist': 'listbox', 'label': 'label',
            'fieldset': 'group', 'legend': 'legend',
            'menu': 'menu', 'menuitem': 'menuitem',
            'p': 'paragraph', 'span': 'text', 'div': 'generic',
            'th': 'columnheader', 'td': 'cell', 'tr': 'row',
            'dl': 'list', 'dt': 'term', 'dd': 'definition',
            'iframe': 'iframe', 'video': 'video', 'audio': 'audio',
            'svg': 'graphic', 'canvas': 'canvas',
        };
        // Input type overrides
        if (tag === 'input') {
            const type = (el.getAttribute('type') || 'text').toLowerCase();
            const inputRoles = {
                'checkbox': 'checkbox', 'radio': 'radio',
                'search': 'searchbox', 'range': 'slider',
                'number': 'spinbutton', 'submit': 'button',
                'reset': 'button', 'button': 'button',
                'image': 'button', 'color': 'textbox',
                'email': 'textbox', 'password': 'textbox',
                'tel': 'textbox', 'text': 'textbox',
                'url': 'textbox', 'date': 'textbox',
                'time': 'textbox',
            };
            return inputRoles[type] || 'textbox';
        }
        return roleMap[tag] || 'generic';
    }

    function getName(el) {
        // aria-label / aria-labelledby first
        if (el.getAttribute('aria-label')) return el.getAttribute('aria-label');
        const labelledby = el.getAttribute('aria-labelledby');
        if (labelledby) {
            const labelEl = document.getElementById(labelledby);
            if (labelEl) return labelEl.textContent.trim().substring(0, 200);
        }
        // alt for images
        if (el.tagName.toLowerCase() === 'img' && el.alt) return el.alt;
        // title attr
        if (el.title) return el.title.substring(0, 200);
        // label element for inputs
        if (el.id) {
            const label = document.querySelector(`label[for="${el.id}"]`);
            if (label) return label.textContent.trim().substring(0, 200);
        }
        // placeholder for inputs
        const placeholder = el.getAttribute('placeholder');
        if (placeholder) return placeholder.substring(0, 200);
        // inner text (truncated)
        const text = el.textContent?.trim();
        if (text && text.length > 0) return text.substring(0, 200);
        // value for inputs
        if (el.value && el.tagName.toLowerCase() === 'input') return el.value.substring(0, 200);
        return '';
    }

    function isInteresting(el) {
        if (!el || el.nodeType !== 1) return false;
        const role = getRole(el);
        const tag = el.tagName.toLowerCase();
        // Always include explicit role elements
        if (el.getAttribute('role')) return true;
        // Interactive tags
        if (INTERACTIVE_TAGS.has(tag)) return true;
        // Text containers only if they have meaningful text
        if (TEXT_CONTAINERS.has(tag)) {
            const name = getName(el);
            if (name && name.length > 0) return true;
        }
        // ARIA roles
        if (INTERACTIVE_ROLES.has(role)) return true;
        // Global roles
        if (GLOBAL_ROLES.has(role)) return true;
        // Tabindex
        if (el.getAttribute('tabindex') !== null) return true;
        // Hidden elements
        if (el.getAttribute('aria-hidden') === 'true') return false;
        if (el.hidden) return false;
        // Visible elements with meaningful content
        const name = getName(el);
        if (name && name.length > 0 && role !== 'generic') return true;
        return false;
    }

    function walk(el, depth) {
        if (!el || el.nodeType !== 1) return;
        if (el.getAttribute('aria-hidden') === 'true') return;
        const style = window.getComputedStyle(el);
        if (style.display === 'none' || style.visibility === 'hidden') return;

        if (isInteresting(el)) {
            idx++;
            const role = getRole(el);
            const name = getName(el).substring(0, 120);
            const tag = el.tagName.toLowerCase();
            let line = `- ${role}`;
            if (name) line += ` "${name}"`;
            line += ` [@e${idx}]`;

            // Add extra properties
            if (tag === 'a' || role === 'link') {
                const href = el.href;
                if (href && href !== 'javascript:void(0)') {
                    line += `\n  /url: ${href}`;
                }
            }
            if (role === 'checkbox' || role === 'radio' || role === 'switch') {
                const checked = el.checked || el.getAttribute('aria-checked') === 'true';
                line += `\n  /checked: ${checked}`;
            }
            if (role === 'textbox' || role === 'searchbox' || tag === 'input' || tag === 'textarea') {
                const val = el.value || '';
                if (val) line += ` [${val.substring(0, 50)}]`;
                const ph = el.getAttribute('placeholder') || '';
                if (ph) line += ` [${ph.substring(0, 50)}]`;
                const disabled = el.disabled;
                if (disabled) line += ` [disabled]`;
                const hidden = el.type === 'hidden';
                if (hidden) line += ` [hidden]`;
            }

            lines.push(line);
        }

        // Walk children
        for (const child of el.children) {
            walk(child, depth + 1);
        }
    }

    // Start from body
    walk(document.body, 0);
    return lines.join('\\n');
})()
"""


async def _dismiss_overlays(page) -> None:
    """Try to dismiss cookie consent banners and overlays that block clicking."""
    try:
        js = """() => {
            const buttons = document.querySelectorAll('button, a, [role=button]');
            const texts = ['Accept', 'Accept All', 'Allow', 'I Agree', 'Agree',
                'Got it', 'OK', 'Close', 'Dismiss', 'Reject', 'Reject All',
                'Continue', 'Decline', '✕', '×', 'X', 'No thanks'];
            for (const btn of buttons) {
                const t = (btn.textContent || '').trim();
                if (texts.some(k => t === k || t.startsWith(k + ' ') || t.startsWith(k + '\\n'))) {
                    btn.click();
                    return 'dismissed: ' + t.substring(0, 40);
                }
            }
            return 'none';
        }"""
        result = await page.evaluate(js)
        if result and result != 'none':
            await page.wait_for_timeout(500)
            logger.info("Auto-dismissed overlay: %s", result)
    except Exception:
        pass


async def _async_get_snapshot(page) -> str:
    """Generate a snapshot string from the page using DOM traversal.

    Uses JavaScript evaluation instead of Playwright's accessibility API
    because the accessibility.snapshot() returns empty for many simple pages
    (e.g. example.com). DOM-based generation is more reliable and gives
    consistent results across all page types.
    """
    try:
        result = await page.evaluate(_JS_SNAPSHOT_SCRIPT)
        return result or ""
    except Exception as e:
        logger.warning("DOM snapshot failed: %s", e)
        return ""


# ---------------------------------------------------------------------------
# Tool implementations (sync → async bridge)
# ---------------------------------------------------------------------------

def cloakbrowser_navigate(url: str, task_id: Optional[str] = None) -> str:
    """Navigate to a URL and return snapshot + metadata as JSON."""
    try:
        session = _get_session(task_id)
        loop = session["loop"]

        async def _navigate():
            page = session["page"]
            await page.goto(url, timeout=_DEFAULT_TIMEOUT, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=5000)
            snap_text = await _async_get_snapshot(page)
            # Limit snapshot size
            if len(snap_text) > _SNAPSHOT_MAX_CHARS:
                snap_text = snap_text[:_SNAPSHOT_MAX_CHARS] + "\n... [truncated]"
            return {
                "snap_text": snap_text,
                "page_url": page.url,
                "page_title": await page.title(),
            }

        result = _run_on_loop(loop, _navigate())

        # JS snapshot already contains [@eN] annotations
        snap_text = result["snap_text"]
        element_count = len(re.findall(r'\[@e\d+\]', snap_text))

        # Cache the annotated snapshot for click/type operations
        session["_last_snapshot"] = snap_text

        return json.dumps({
            "success": True,
            "url": result["page_url"],
            "title": result["page_title"],
            "snapshot": snap_text,
            "element_count": element_count,
        })

    except Exception as e:
        return _tool_error(f"Navigation failed: {e}")


def cloakbrowser_snapshot(full: bool = False, task_id: Optional[str] = None,
                          user_task: Optional[str] = None) -> str:
    """Generate a snapshot of the current page."""
    try:
        session = _get_session(task_id)
        loop = session["loop"]

        async def _snapshot():
            page = session["page"]
            snap_text = await _async_get_snapshot(page)
            return snap_text

        snap_text = _run_on_loop(loop, _snapshot())

        if len(snap_text) > _SNAPSHOT_MAX_CHARS:
            snap_text = snap_text[:_SNAPSHOT_MAX_CHARS] + "\n... [truncated]"

        # JS snapshot already contains [@eN] annotations
        element_count = len(re.findall(r'\[@e\d+\]', snap_text))

        # Cache the annotated snapshot for click/type operations
        session["_last_snapshot"] = snap_text

        return json.dumps({
            "success": True,
            "title": "",
            "tree": snap_text,
            "element_count": element_count,
        })

    except Exception as e:
        return _tool_error(str(e))


# ── Click ──────────────────────────────────────────────────────────────

def _parse_ref_from_snapshot(ref: str, snapshot_text: str) -> tuple:
    """Parse role and text from a snapshot line matching @eN.

    Returns (role, text) tuple. role is the ARIA role (lowercase),
    text is the visible/accessible text label (may be empty).
    """
    lines = snapshot_text.split("\n")
    for line in lines:
        if f"[@{ref}]" in line:
            match = re.match(r'\s*- (\w+)\s+"([^"]*)"\s+\[@e\d+\]', line)
            if match:
                return (match.group(1).lower(), match.group(2))
            match2 = re.match(r'\s*- (\w+)\s+\[@e\d+\]', line)
            if match2:
                return (match2.group(1).lower(), "")
    return (None, None)


def cloakbrowser_click(ref: str, task_id: Optional[str] = None) -> str:
    """Click an element by ref via CloakBrowser.

    Uses Playwright locator API (which supports :has-text() pseudo-selectors)
    instead of JS evaluate (which only supports standard CSS selectors).
    """
    try:
        session = _get_session(task_id)
        loop = session["loop"]
        clean_ref = ref.lstrip("@")

        async def _click():
            page = session["page"]
            # Use cached annotated snapshot (from last navigate/snapshot call)
            snap_text = session.get("_last_snapshot", "")
            if not snap_text:
                snap_text = await _async_get_snapshot(page)
            # Auto-dismiss overlays/popups before clicking
            await _dismiss_overlays(page)
            role, text = _parse_ref_from_snapshot(clean_ref, snap_text)
            clicked = False

            if role and text:
                # Strategy 1a: get_by_role + name (most reliable)
                try:
                    locator = page.get_by_role(role, name=text)
                    if await locator.count() > 0:
                        await locator.first.click(timeout=_DEFAULT_TIMEOUT)
                        clicked = True
                except Exception:
                    pass

                # Strategy 1b: Playwright CSS selector with :has-text()
                if not clicked:
                    selector = _find_selector_for_ref(clean_ref, snap_text)
                    if selector:
                        try:
                            loc = page.locator(selector)
                            if await loc.count() > 0:
                                await loc.first.click(timeout=_DEFAULT_TIMEOUT)
                                clicked = True
                        except Exception:
                            pass

            elif role:
                # No text, just role
                try:
                    locator = page.get_by_role(role)
                    idx = int(clean_ref.replace("e", "")) - 1
                    if await locator.count() > idx:
                        await locator.nth(idx).click(timeout=_DEFAULT_TIMEOUT)
                        clicked = True
                except Exception:
                    pass

            # Strategy 2: Index-based click via query_selector_all (standard CSS)
            if not clicked:
                elements = await page.query_selector_all(
                    "button, a, input, [tabindex], [role=button], [role=link], "
                    "[role=textbox], [role=checkbox], [role=radio], select, textarea"
                )
                try:
                    idx = int(clean_ref.replace("e", "")) - 1
                    if 0 <= idx < len(elements):
                        await elements[idx].click(timeout=_DEFAULT_TIMEOUT)
                        clicked = True
                except (ValueError, IndexError):
                    pass

            return {
                "clicked": clicked,
                "url": page.url,
            }

        result = _run_on_loop(loop, _click())

        if result["clicked"]:
            return json.dumps({
                "success": True,
                "clicked": clean_ref,
                "url": result["url"],
            })

        return _tool_error(
            f"Could not find element @{clean_ref} on the page. "
            f"Try browser_snapshot first to see available elements.",
        )

    except Exception as e:
        return _tool_error(str(e))


def _find_selector_for_ref(ref: str, snapshot_text: str) -> Optional[str]:
    """Try to find a CSS selector for a given @eN ref in the snapshot text.

    Uses heuristic matching: finds the line with [@eN], extracts the role
    and text, then generates candidate selectors (Playwright-compatible).
    """
    lines = snapshot_text.split("\n")
    for line in lines:
        if f"[@{ref}]" in line:
            match = re.match(r'\s*- (\w+)\s+"([^"]*)"\s+\[@e\d+\]', line)
            if match:
                role = match.group(1).lower()
                text = match.group(2)
                role_to_selector = {
                    "button": "button",
                    "link": "a",
                    "textbox": "input, textarea",
                    "searchbox": "input[type='search'], input[type='text']",
                    "combobox": "select, [role='combobox']",
                    "checkbox": "input[type='checkbox'], [role='checkbox']",
                    "radio": "input[type='radio'], [role='radio']",
                    "heading": "h1, h2, h3, h4, h5, h6",
                    "menuitem": "[role='menuitem']",
                    "tab": "[role='tab']",
                    "listbox": "[role='listbox']",
                    "option": "[role='option']",
                }
                base_sel = role_to_selector.get(role, "*")
                if text:
                    sanitized_text = text.replace('"', '\\"')
                    if role == "button":
                        return f'button:has-text("{sanitized_text}")'
                    elif role == "link":
                        return f'a:has-text("{sanitized_text}")'
                    elif role in ("textbox", "searchbox"):
                        return f'{base_sel}[placeholder="{sanitized_text}"], {base_sel}[aria-label="{sanitized_text}"]'
                    else:
                        return f'{base_sel}[aria-label="{sanitized_text}"], {base_sel}:has-text("{sanitized_text}")'
                return base_sel

            match2 = re.match(r'\s*- (\w+)\s+\[@e\d+\]', line)
            if match2:
                role = match2.group(1).lower()
                role_to_selector = {
                    "button": "button",
                    "link": "a",
                    "textbox": "input, textarea",
                    "searchbox": "input[type='search']",
                    "combobox": "select, [role='combobox']",
                    "checkbox": "input[type='checkbox']",
                    "radio": "input[type='radio']",
                    "separator": "hr",
                    "img": "img",
                }
                return role_to_selector.get(role)
    return None


# ── Type ───────────────────────────────────────────────────────────────

def cloakbrowser_type(ref: str, text: str, task_id: Optional[str] = None) -> str:
    """Type text into an element."""
    try:
        session = _get_session(task_id)
        loop = session["loop"]
        clean_ref = ref.lstrip("@")

        async def _type():
            page = session["page"]
            # Use cached annotated snapshot
            snap_text = session.get("_last_snapshot", "")
            if not snap_text:
                snap_text = await _async_get_snapshot(page)
            role, el_text = _parse_ref_from_snapshot(clean_ref, snap_text)
            typed = False

            if role and el_text:
                try:
                    locator = page.get_by_role(role, name=el_text)
                    if await locator.count() > 0:
                        await locator.first.click(timeout=_DEFAULT_TIMEOUT)
                        await locator.first.fill(text, timeout=_DEFAULT_TIMEOUT)
                        typed = True
                except Exception:
                    pass

            if not typed:
                selector = _find_selector_for_ref(clean_ref, snap_text)
                if selector:
                    try:
                        loc = page.locator(selector)
                        if await loc.count() > 0:
                            await loc.first.click(timeout=_DEFAULT_TIMEOUT)
                            await loc.first.fill(text, timeout=_DEFAULT_TIMEOUT)
                            typed = True
                    except Exception:
                        pass

            # Fallback: type via keyboard into focused element
            if not typed:
                await page.keyboard.type(text, delay=50)

            return typed

        typed = _run_on_loop(loop, _type())
        return json.dumps({"success": typed, "typed": text[:50]})

    except Exception as e:
        return _tool_error(str(e))


# ── Scroll ─────────────────────────────────────────────────────────────

def cloakbrowser_scroll(direction: str, task_id: Optional[str] = None) -> str:
    """Scroll the page up or down."""
    try:
        session = _get_session(task_id)
        loop = session["loop"]

        async def _scroll():
            page = session["page"]
            if direction == "down":
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
            elif direction == "up":
                await page.evaluate("window.scrollBy(0, -window.innerHeight)")
            return page.url

        url = _run_on_loop(loop, _scroll())
        return json.dumps({"success": True, "url": url})

    except Exception as e:
        return _tool_error(str(e))


# ── Back ───────────────────────────────────────────────────────────────

def cloakbrowser_back(task_id: Optional[str] = None) -> str:
    """Navigate back in browser history."""
    try:
        session = _get_session(task_id)
        loop = session["loop"]

        async def _back():
            page = session["page"]
            await page.go_back(timeout=_DEFAULT_TIMEOUT, wait_until="domcontentloaded")
            return page.url

        url = _run_on_loop(loop, _back())
        return json.dumps({"success": True, "url": url})

    except Exception as e:
        return _tool_error(str(e))


# ── Press ──────────────────────────────────────────────────────────────

def cloakbrowser_press(key: str, task_id: Optional[str] = None) -> str:
    """Press a keyboard key."""
    try:
        session = _get_session(task_id)
        loop = session["loop"]

        async def _press():
            page = session["page"]
            await page.keyboard.press(key)
            return "ok"

        _run_on_loop(loop, _press())
        return json.dumps({"success": True})

    except Exception as e:
        return _tool_error(str(e))


# ── Console / Eval ─────────────────────────────────────────────────────

def cloakbrowser_console(clear: bool = False, expression: Optional[str] = None,
                         task_id: Optional[str] = None) -> str:
    """Get console output or evaluate JavaScript."""
    try:
        session = _get_session(task_id)
        loop = session["loop"]

        if expression:
            async def _eval():
                page = session["page"]
                result = await page.evaluate(expression)
                return result

            result = _run_on_loop(loop, _eval())
            return json.dumps({"success": True, "result": result})
        else:
            logs = session["logs"]
            if clear:
                logs.clear()
            return json.dumps({"success": True, "logs": list(logs)})

    except Exception as e:
        return _tool_error(str(e))


def cloakbrowser_eval(expression: str, task_id: Optional[str] = None) -> str:
    """Evaluate JavaScript in the page context."""
    return cloakbrowser_console(expression=expression, task_id=task_id)


# ── Images ─────────────────────────────────────────────────────────────

def cloakbrowser_get_images(task_id: Optional[str] = None) -> str:
    """Get a list of images on the page."""
    try:
        session = _get_session(task_id)
        loop = session["loop"]

        async def _get_imgs():
            page = session["page"]
            return await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('img')).map(img => ({
                    src: img.src || '',
                    alt: img.alt || '',
                    width: img.naturalWidth,
                    height: img.naturalHeight,
                }));
            }""")

        images = _run_on_loop(loop, _get_imgs())
        return json.dumps({"success": True, "images": images[:50]})

    except Exception as e:
        return _tool_error(str(e))


# ── Vision / Screenshot ────────────────────────────────────────────────

def cloakbrowser_vision(question: str = "", annotate: bool = False,
                        task_id: Optional[str] = None) -> str:
    """Take a screenshot and save it to disk."""
    try:
        session = _get_session(task_id)
        loop = session["loop"]

        async def _screenshot():
            _SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
            path = _SCREENSHOTS_DIR / f"cloakbrowser_{uuid.uuid4().hex[:12]}.png"
            page = session["page"]
            data = await page.screenshot(full_page=False, type="png")
            path.write_bytes(data)
            return str(path)

        screenshot_path = _run_on_loop(loop, _screenshot())

        # Check file size
        file_size = Path(screenshot_path).stat().st_size if Path(screenshot_path).exists() else 0

        return json.dumps({
            "success": True,
            "screenshot_path": screenshot_path,
            "description": f"CloakBrowser page screenshot ({file_size} bytes)",
            "backend": "cloakbrowser",
        })

    except Exception as e:
        return _tool_error(str(e))


# ── Click At ───────────────────────────────────────────────────────────

def cloakbrowser_click_at(x: float, y: float,
                          task_id: Optional[str] = None) -> str:
    """Click at specific pixel coordinates using Playwright's native mouse."""
    try:
        session = _get_session(task_id)
        loop = session["loop"]

        async def _click_at():
            page = session["page"]
            await page.mouse.click(x, y)
            return page.url

        url = _run_on_loop(loop, _click_at())
        return json.dumps({
            "success": True,
            "clicked_at": {"x": x, "y": y},
            "url": url,
        })

    except Exception as e:
        return _tool_error(str(e))