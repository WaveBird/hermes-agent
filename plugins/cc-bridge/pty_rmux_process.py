"""
CCProcessRmux — rmux + librmux + JSONL transcript alternative to CCProcess.

Same concept as pty_process.py (tmux) but uses rmux (Rust terminal multiplexer)
and its Python SDK librmux for session/pane management. JSONL tailing logic is
shared — both backends read ~/.claude/projects/<dir>/<sid>.jsonl for structured
events.

rmux advantages over plain tmux:
- Typed Python SDK (librmux) instead of raw subprocess calls
- pane.snapshot(), pane.wait_for_text(), pane.output_stream() APIs
- Cross-platform (native Windows ConPTY, no WSL needed)
- Claude Teammate Mode built-in

Requires: rmux binary on PATH + librmux pip package.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Helpers (shared with pty_process.py)
# --------------------------------------------------------------------------- #

def _short_hash(*parts: str) -> str:
    """Return an 8-char hex hash from the concatenated parts."""
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:8]


def _normalized_dir(workdir: str) -> str:
    """Return the CC-normalized project directory component.

    /home/vm/code → -home-vm-code, /tmp → -tmp
    """
    return "-" + workdir.replace("/", "-").lstrip("-")


def _ensure_rmux_daemon() -> None:
    """Ensure the rmux daemon is running.

    rmux start-server has an 'exit-empty' behavior: if there are no sessions,
    the daemon exits immediately. We work around this by creating a throwaway
    shell session first, which keeps the daemon alive. Subsequent real sessions
    (created via ensure_session) will coexist.

    This only needs to be called once per process lifetime.
    """
    global _DAEMON_ENSURED
    if _DAEMON_ENSURED:
        return
    try:
        # Check if daemon is already running (has sessions)
        result = subprocess.run(
            ["rmux", "list-sessions", "--json"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            _DAEMON_ENSURED = True
            return
        # Daemon not running — create a keepalive session to bootstrap it
        subprocess.run(
            ["rmux", "new-session", "-d", "-s", "_daemon_keepalive", "-c", "/tmp"],
            capture_output=True, timeout=5,
        )
        _DAEMON_ENSURED = True
        logger.info("rmux daemon bootstrapped via keepalive session")
    except Exception as exc:
        logger.warning("rmux daemon bootstrap failed: %s", exc)


_DAEMON_ENSURED = False


# --------------------------------------------------------------------------- #
# CCProcessRmux
# --------------------------------------------------------------------------- #

class CCProcessRmux:
    """Drive Claude Code via rmux + librmux + JSONL transcript tailing.

    Public interface mirrors :class:`CCProcess` from core.py so the two are
    drop-in interchangeable from the caller's perspective.
    """

    def __init__(
        self,
        workdir: str,
        session_id: Optional[str],
        mode: str,
        claude_executable: str = "claude",
        *,
        extra_env: Optional[Dict[str, str]] = None,
        on_event: Optional[Callable[[Dict], Any]] = None,
        on_exit: Optional[Callable[[Any], Any]] = None,
    ) -> None:
        self._workdir = workdir
        self._session_id = session_id
        self._mode = mode
        self._claude_executable = claude_executable
        self._extra_env = extra_env or {}
        self._on_event = on_event
        self._on_exit = on_exit

        # rmux state
        self._rmux = None          # librmux.Rmux client
        self._session = None       # librmux.Session handle
        self._pane = None          # librmux.Pane handle
        self._rmux_session_name: Optional[str] = None

        # JSONL tail state
        self._tail_task: Optional[asyncio.Task] = None
        self._discovery_task: Optional[asyncio.Task] = None
        self._jsonl_path: Optional[Path] = None
        self._byte_offset: int = 0
        self._captured_session_id: Optional[str] = None
        self._start_time: float = 0.0
        self._started: bool = False
        self._closed: bool = False

        # Checkpoints (mirrors CCProcess)
        self._checkpoints: List[Dict[str, Any]] = []

        # Pending approval (simplified for v1)
        self._pending_approval: Optional[Dict[str, Any]] = None

    # ----- public properties -------------------------------------------------

    @property
    def running(self) -> bool:
        """True when the rmux session exists AND the tail task is alive."""
        if not self._rmux_session_name or not self._started:
            return False
        # Check rmux session existence
        try:
            result = subprocess.run(
                ["rmux", "has-session", "-t", self._rmux_session_name],
                capture_output=True, text=True, timeout=5,
            )
            rmux_alive = result.returncode == 0
        except Exception:
            rmux_alive = False
        tail_alive = self._tail_task is not None and not self._tail_task.done()
        # If discovery task is running (new session), also count as running
        discovery_alive = (
            self._discovery_task is not None and not self._discovery_task.done()
        )
        return rmux_alive and (tail_alive or discovery_alive)

    @property
    def pending_approval(self) -> Optional[Dict]:
        """Current pending permission request, or None (simplified for v1)."""
        return self._pending_approval

    @property
    def checkpoints(self) -> list:
        """List of checkpoint dicts that carry a uuid."""
        return [cp for cp in self._checkpoints if cp.get("uuid")]

    # ----- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        """Launch claude CLI inside an rmux session and begin tailing."""
        if self._started:
            raise RuntimeError("CCProcessRmux.start() called more than once")

        _ensure_rmux_daemon()

        # Build unique rmux session name
        ts = str(int(time.time()))
        self._rmux_session_name = "ccb_" + _short_hash(
            self._workdir, self._session_id or "new", ts
        )

        # Build claude CLI command.
        # CC interactive mode: --permission-mode (NOT --mode).
        # resume: --resume <session_id>.
        cmd_parts = [self._claude_executable]
        if self._session_id:
            cmd_parts.extend(["--resume", self._session_id])
        if self._mode == "bypassPermissions":
            cmd_parts.append("--dangerously-skip-permissions")
        else:
            cmd_parts.extend(["--permission-mode", self._mode])
        claude_cmd = " ".join(cmd_parts)

        # Create rmux session with claude running inside.
        # We use subprocess directly for new-session because librmux's
        # ensure_session doesn't pass shell_command in a way that works
        # for multi-word commands with spaces.
        env = os.environ.copy()
        env.update(self._extra_env)
        result = subprocess.run(
            ["rmux", "new-session", "-d", "-s", self._rmux_session_name,
             "-c", self._workdir, claude_cmd],
            capture_output=True, text=True, timeout=10, env=env,
        )
        if result.returncode != 0:
            logger.error("rmux new-session failed: %s", result.stderr.strip())
            raise RuntimeError(f"rmux new-session failed: {result.stderr.strip()}")

        self._started = True
        self._start_time = time.time()

        # Connect librmux client for subsequent operations
        try:
            import librmux
            self._rmux = librmux.RMUX()
            self._session = self._rmux.session(self._rmux_session_name)
            self._pane = self._session.pane(0, 0)
        except Exception as exc:
            logger.warning("librmux connect failed (will use CLI fallback): %s", exc)

        # Begin transcript discovery / tailing
        if self._session_id:
            # Resume — we know the session ID, compute JSONL path directly
            self._captured_session_id = self._session_id
            projects_dir = (
                Path.home() / ".claude" / "projects" / _normalized_dir(self._workdir)
            )
            self._jsonl_path = projects_dir / f"{self._session_id}.jsonl"
            self._tail_task = asyncio.create_task(self._tail_loop())
        else:
            # New session — discover the JSONL file
            self._discovery_task = asyncio.create_task(self._discover_jsonl())

        logger.info(
            "CCProcessRmux started: rmux=%s session_id=%s mode=%s",
            self._rmux_session_name,
            self._session_id or "(new)",
            self._mode,
        )

    # ----- JSONL discovery (new sessions) ------------------------------------

    async def _discover_jsonl(self) -> None:
        """Watch the CC projects directory for a new .jsonl file after start."""
        projects_dir = (
            Path.home() / ".claude" / "projects" / _normalized_dir(self._workdir)
        )
        if not projects_dir.exists():
            for _ in range(50):
                await asyncio.sleep(0.2)
                if projects_dir.exists():
                    break
            else:
                logger.error("CC projects directory never appeared: %s", projects_dir)
                return

        pre_existing: set = set()
        for p in projects_dir.glob("*.jsonl"):
            pre_existing.add(p.name)

        deadline = self._start_time + 120
        while time.time() < deadline:
            for p in projects_dir.glob("*.jsonl"):
                if p.name not in pre_existing:
                    try:
                        mtime = p.stat().st_mtime
                    except OSError:
                        continue
                    if mtime >= self._start_time - 1:
                        self._jsonl_path = p
                        logger.info("Discovered CC session JSONL: %s", p)
                        self._tail_task = asyncio.create_task(self._tail_loop())
                        return
            await asyncio.sleep(0.3)

        logger.error("Timed out waiting for CC JSONL transcript to appear")

    # ----- JSONL tail loop ---------------------------------------------------

    async def _tail_loop(self) -> None:
        """Tail the JSONL transcript and route parsed lines to on_event."""
        if not self._jsonl_path:
            logger.error("_tail_loop started without a jsonl_path")
            return

        for _ in range(100):
            if self._jsonl_path.exists():
                break
            await asyncio.sleep(0.1)
        else:
            logger.error("JSONL file never appeared: %s", self._jsonl_path)
            return

        while True:
            try:
                new_data = await asyncio.to_thread(self._read_new_bytes)
                if new_data:
                    for line in new_data.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            logger.warning(
                                "Skipping malformed JSONL line: %.120s", line
                            )
                            continue
                        self._process_jsonl_record(record)
            except asyncio.CancelledError:
                logger.debug("Tail loop cancelled")
                return
            except Exception:
                logger.exception("Error in tail loop")

            # Check if rmux session is still alive
            try:
                result = subprocess.run(
                    ["rmux", "has-session", "-t", self._rmux_session_name],
                    capture_output=True, text=True, timeout=5,
                )
                if result.returncode != 0:
                    logger.info("rmux session gone — exiting tail loop")
                    break
            except Exception:
                logger.info("rmux check failed — exiting tail loop")
                break

            await asyncio.sleep(0.1)

        # Session ended — notify
        if self._on_exit:
            try:
                result = self._on_exit(None)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("on_exit callback raised")

    def _read_new_bytes(self) -> str:
        """Synchronously read new bytes from the JSONL file."""
        try:
            with open(self._jsonl_path, "r", encoding="utf-8") as f:
                f.seek(self._byte_offset)
                data = f.read()
                self._byte_offset = f.tell()
                return data
        except OSError:
            return ""

    # ----- JSONL record processing -------------------------------------------

    def _process_jsonl_record(self, record: Dict[str, Any]) -> None:
        """Parse a single JSONL record and emit events via on_event.

        CC interactive mode JSONL line types (verified against CC v2.1.87):
        - file-history-snapshot: file state snapshot (ignored)
        - user: {message: {role, content}} — user input; may contain tool_result
        - assistant: {message: {role, content: [{type: text|tool_use, ...}]}}
        - last-prompt: marks end of a user→assistant turn pair
        - summary: conversation summary (ignored)
        """
        rtype = record.get("type")

        # Capture session_id from any record that has it
        sid = record.get("sessionId")
        if sid and sid != self._captured_session_id:
            self._captured_session_id = sid
            self._emit_event(
                {"type": "result", "subtype": "session_id", "session_id": sid}
            )
            self._backfill_checkpoint_uuids(sid)

        if rtype == "assistant":
            message = record.get("message", {})
            content_blocks = message.get("content", [])
            if isinstance(content_blocks, str):
                self._emit_event({"type": "text", "text": content_blocks})
            elif isinstance(content_blocks, list):
                for block in content_blocks:
                    if not isinstance(block, dict):
                        continue
                    btype = block.get("type")
                    if btype == "text":
                        self._emit_event(
                            {"type": "text", "text": block.get("text", "")}
                        )
                    elif btype == "tool_use":
                        self._emit_event(
                            {
                                "type": "tool_use",
                                "id": block.get("id", ""),
                                "name": block.get("name", ""),
                                "input": block.get("input", {}),
                            }
                        )

        elif rtype == "user":
            message = record.get("message", {})
            content_blocks = message.get("content", [])
            if isinstance(content_blocks, list):
                for block in content_blocks:
                    if not isinstance(block, dict):
                        continue
                    btype = block.get("type")
                    if btype == "tool_result":
                        self._emit_event(
                            {
                                "type": "tool_result",
                                "tool_use_id": block.get("tool_use_id", ""),
                                "content": block.get("content", ""),
                            }
                        )

        elif rtype == "last-prompt":
            # Turn pair complete — emit a stop event
            self._emit_event({"type": "result", "subtype": "stop"})

    # ----- event emission ----------------------------------------------------

    def _emit_event(self, event: Dict[str, Any]) -> None:
        """Route an event dict to the on_event callback.

        The cc-bridge _on_cc_event is async, so _on_event is a coroutine
        function. We schedule it as a task instead of calling it directly.
        """
        if self._on_event:
            try:
                result = self._on_event(event)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                logger.exception(
                    "on_event callback raised for event %s", event.get("type")
                )

    # ----- checkpoint management ---------------------------------------------

    def _backfill_checkpoint_uuids(self, session_id: str) -> None:
        """Fill in uuids for checkpoints recorded before session_id was captured."""
        for cp in self._checkpoints:
            if not cp.get("uuid"):
                cp["uuid"] = f"{session_id}__{cp.get('seq', 0)}"

    # ----- user input --------------------------------------------------------

    async def send_text(self, text: str) -> None:
        """Send text to claude via rmux send-keys (literal) + Enter."""
        if not self._rmux_session_name:
            raise RuntimeError("CCProcessRmux not started")

        cp = {
            "seq": len(self._checkpoints),
            "text": text[:80],
            "uuid": "",
            "ts": time.time(),
        }
        if self._captured_session_id:
            cp["uuid"] = f"{self._captured_session_id}__{cp['seq']}"
        self._checkpoints.append(cp)

        # Try librmux first, fall back to CLI
        sent = False
        if self._pane is not None:
            try:
                # send_text sends literal text; then send Enter key
                self._pane.send_text(text)
                self._pane.send_keys("Enter")
                sent = True
            except Exception as exc:
                logger.warning("librmux send_text failed, falling back to CLI: %s", exc)

        if not sent:
            # CLI fallback: split long text into chunks
            chunk_size = 4000
            for i in range(0, len(text), chunk_size):
                chunk = text[i: i + chunk_size]
                await asyncio.to_thread(
                    subprocess.run,
                    ["rmux", "send-keys", "-t", self._rmux_session_name, "-l", chunk],
                    capture_output=True, text=True, timeout=10,
                )
            await asyncio.to_thread(
                subprocess.run,
                ["rmux", "send-keys", "-t", self._rmux_session_name, "Enter"],
                capture_output=True, text=True, timeout=10,
            )

    async def record_user_uuid(self, uuid_val: str, content: str = "") -> None:
        """Record a user message UUID for checkpoint tracking (PTY no-op)."""
        logger.debug("record_user_uuid: uuid=%s (PTY backend — stored only)", uuid_val)

    # ----- interrupt ---------------------------------------------------------

    async def interrupt(self) -> None:
        """Send Escape to the rmux pane (CC interprets as interrupt)."""
        if not self._rmux_session_name:
            return
        sent = False
        if self._pane is not None:
            try:
                self._pane.send_keys("Escape")
                sent = True
            except Exception as exc:
                logger.warning("librmux interrupt failed, falling back to CLI: %s", exc)
        if not sent:
            await asyncio.to_thread(
                subprocess.run,
                ["rmux", "send-keys", "-t", self._rmux_session_name, "Escape"],
                capture_output=True, text=True, timeout=10,
            )
        # Small delay then another Escape for reliability
        await asyncio.sleep(0.1)
        if self._pane is not None:
            try:
                self._pane.send_keys("Escape")
            except Exception:
                pass
        else:
            await asyncio.to_thread(
                subprocess.run,
                ["rmux", "send-keys", "-t", self._rmux_session_name, "Escape"],
                capture_output=True, text=True, timeout=10,
            )

    # ----- mode change -------------------------------------------------------

    async def set_mode(self, mode: str) -> None:
        """Store the new mode value. Caller handles restart."""
        logger.info("set_mode: %s → %s (stored; caller handles restart)", self._mode, mode)
        self._mode = mode

    # ----- permissions -------------------------------------------------------

    def resolve_permission(
        self,
        request_id: str,
        approved: bool,
        message: str = "",
        interrupt: bool = False,
        scope: str = "once",
        tool: str = "",
    ) -> bool:
        """Resolve a pending permission prompt in the rmux pane.

        Sends 'y' or 'n' to the rmux session. Simplified for v1.
        """
        if not self._rmux_session_name or not self._started:
            return False

        key = "y" if approved else "n"
        sent = False
        if self._pane is not None:
            try:
                self._pane.send_keys(key)
                self._pane.send_keys("Enter")
                self._pending_approval = None
                sent = True
            except Exception as exc:
                logger.warning("librmux resolve_permission failed: %s", exc)

        if not sent:
            try:
                subprocess.run(
                    ["rmux", "send-keys", "-t", self._rmux_session_name, key],
                    capture_output=True, text=True, timeout=5,
                )
                subprocess.run(
                    ["rmux", "send-keys", "-t", self._rmux_session_name, "Enter"],
                    capture_output=True, text=True, timeout=5,
                )
                self._pending_approval = None
                sent = True
            except Exception:
                logger.exception("resolve_permission CLI fallback failed")

        if sent:
            logger.info("resolve_permission: sent '%s' to rmux", key)
        return sent

    def deny_all_permissions(self) -> None:
        """Deny all pending permissions by sending 'n' to the rmux pane."""
        if self._rmux_session_name and self._started:
            try:
                if self._pane is not None:
                    self._pane.send_keys("n")
                    self._pane.send_keys("Enter")
                else:
                    subprocess.run(
                        ["rmux", "send-keys", "-t", self._rmux_session_name, "n"],
                        capture_output=True, text=True, timeout=5,
                    )
                    subprocess.run(
                        ["rmux", "send-keys", "-t", self._rmux_session_name, "Enter"],
                        capture_output=True, text=True, timeout=5,
                    )
                self._pending_approval = None
            except Exception:
                logger.exception("deny_all_permissions failed")

    # ----- rewind ------------------------------------------------------------

    async def rewind_files(self, user_message_id: str) -> bool:
        """Not supported in PTY mode. Return False with a warning."""
        logger.warning(
            "rewind_files is not supported in PTY mode (user_message_id=%s).",
            user_message_id,
        )
        return False

    # ----- cleanup -----------------------------------------------------------

    async def close(self) -> None:
        """Kill the rmux session, cancel background tasks, and call on_exit."""
        if self._closed:
            return
        self._closed = True

        # Cancel discovery task
        if self._discovery_task and not self._discovery_task.done():
            self._discovery_task.cancel()
            try:
                await self._discovery_task
            except asyncio.CancelledError:
                pass
            self._discovery_task = None

        # Cancel tail task
        if self._tail_task and not self._tail_task.done():
            self._tail_task.cancel()
            try:
                await self._tail_task
            except asyncio.CancelledError:
                pass
            self._tail_task = None

        # Kill rmux session
        if self._rmux_session_name:
            try:
                # Try librmux first
                if self._session is not None:
                    self._session.kill()
                else:
                    subprocess.run(
                        ["rmux", "kill-session", "-t", self._rmux_session_name],
                        capture_output=True, text=True, timeout=10,
                    )
            except Exception:
                logger.debug("rmux kill-session failed (session may already be gone)")
            self._rmux_session_name = None

        self._started = False

        # Notify exit
        if self._on_exit:
            try:
                result = self._on_exit(None)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("on_exit callback raised during close")

        logger.info("CCProcessRmux closed")
