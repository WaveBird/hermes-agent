"""
CCProcessPty — tmux + JSONL transcript alternative to the SDK-based CCProcess.

Drives the real `claude` CLI through a tmux session and tails the JSONL
transcript file that Claude Code writes under ~/.claude/projects/ to surface
structured events (text, tool_use, tool_result, result).

This approach preserves all native CLI features (slash commands, TUI, etc.)
that the Python SDK does not expose.

Only stdlib dependencies: subprocess, asyncio, json, os, time, uuid, pathlib,
logging, hashlib.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _short_hash(*parts: str) -> str:
    """Return an 8-char hex hash from the concatenated parts."""
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:8]


def _normalized_dir(workdir: str) -> str:
    """Return the CC-normalized project directory component.

    CC stores transcripts under ``~/.claude/projects/<normalized>/<sid>.jsonl``
    where *normalized* is the workdir path with every ``/`` replaced by ``-``,
    prefixed with ``-``.  Leading ``-`` from the root ``/`` is collapsed.
    For example ``/home/vm/code`` → ``-home-vm-code``, ``/tmp`` → ``-tmp``.
    """
    return "-" + workdir.replace("/", "-").lstrip("-")


def _run_sync(cmd: List[str], **kwargs) -> subprocess.CompletedProcess:
    """Synchronous subprocess run with sensible defaults."""
    return subprocess.run(cmd, capture_output=True, text=True, timeout=10, **kwargs)


async def _run_async(cmd: List[str], **kwargs) -> subprocess.CompletedProcess:
    """Async subprocess run with sensible defaults."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **kwargs,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
    return subprocess.CompletedProcess(
        cmd, proc.returncode, stdout.decode() if stdout else "", stderr.decode() if stderr else ""
    )


# ---------------------------------------------------------------------------
# CCProcessPty
# ---------------------------------------------------------------------------

class CCProcessPty:
    """Drive Claude Code via tmux + JSONL transcript tailing.

    Public interface mirrors :class:`CCProcess` from core.py so the two are
    drop-in interchangeable from the caller's perspective.
    """

    # ----- constructor -------------------------------------------------------

    def __init__(
        self,
        workdir: str,
        session_id: Optional[str],
        mode: str,
        claude_executable: str = "claude",
        *,
        extra_env: Optional[Dict[str, str]] = None,
        on_event: Optional[Callable[[Dict], Any]] = None,
        on_exit: Optional[Callable[[], Any]] = None,
    ) -> None:
        self._workdir = workdir
        self._session_id = session_id  # None ⇒ new session; str ⇒ resume
        self._mode = mode
        self._claude_executable = claude_executable
        self._extra_env = extra_env or {}
        self._on_event = on_event
        self._on_exit = on_exit

        # Internal state
        self._tmux_name: Optional[str] = None
        self._tail_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]
        self._discovery_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]
        self._jsonl_path: Optional[Path] = None
        self._byte_offset: int = 0
        self._captured_session_id: Optional[str] = None
        self._start_time: float = 0.0
        self._started: bool = False

        # Checkpoint tracking (mirrors CCProcess)
        self._checkpoints: List[Dict[str, Any]] = []

        # Pending approval (simplified — always None for v1)
        self._pending_approval: Optional[Dict[str, Any]] = None

    # ----- public properties -------------------------------------------------

    @property
    def running(self) -> bool:
        """True when the tmux session exists AND the tail task is alive."""
        if not self._tmux_name or not self._started:
            return False
        # Check tmux session existence
        try:
            result = _run_sync(["tmux", "has-session", "-t", self._tmux_name])
            tmux_alive = result.returncode == 0
        except Exception:
            tmux_alive = False
        tail_alive = self._tail_task is not None and not self._tail_task.done()
        return tmux_alive and tail_alive

    @property
    def pending_approval(self) -> Optional[Dict]:
        """Current pending permission request, or None.

        Simplified for v1 — always returns None.  Can be extended later with
        tmux prompt-detection logic.
        """
        return self._pending_approval

    @property
    def checkpoints(self) -> list:  # noqa: D401
        """List of checkpoint dicts that carry a uuid."""
        return [cp for cp in self._checkpoints if cp.get("uuid")]

    # ----- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        """Launch the claude CLI inside a new tmux session and begin tailing."""
        if self._started:
            raise RuntimeError("CCProcessPty.start() called more than once")

        # Build a unique tmux session name
        ts = str(int(time.time()))
        self._tmux_name = "ccb_" + _short_hash(self._workdir, self._session_id or "new", ts)

        # Build the claude CLI command.
        # CC interactive mode uses --permission-mode (NOT --mode).
        # resume: -r <session_id> (CC accepts -r or --resume).
        cmd = [self._claude_executable]
        if self._session_id:
            cmd.extend(["--resume", self._session_id])
        if self._mode == "bypassPermissions":
            cmd.append("--dangerously-skip-permissions")
        else:
            cmd.extend(["--permission-mode", self._mode])

        # Environment
        env = os.environ.copy()
        env.update(self._extra_env)

        # Start tmux session
        try:
            result = _run_sync(
                ["tmux", "new-session", "-d", "-s", self._tmux_name, "-c", self._workdir] + cmd,
                env=env,
            )
            if result.returncode != 0:
                logger.error("tmux new-session failed: %s", result.stderr.strip())
                raise RuntimeError(f"tmux new-session failed: {result.stderr.strip()}")
        except subprocess.TimeoutExpired:
            raise RuntimeError("tmux new-session timed out")

        self._started = True
        self._start_time = time.time()

        # Begin transcript discovery / tailing
        if self._session_id:
            # Resume — we already know the session ID and can compute the JSONL path
            self._captured_session_id = self._session_id
            projects_dir = Path.home() / ".claude" / "projects" / _normalized_dir(self._workdir)
            self._jsonl_path = projects_dir / f"{self._session_id}.jsonl"
            self._tail_task = asyncio.create_task(self._tail_loop())
        else:
            # New session — need to discover the JSONL file
            self._discovery_task = asyncio.create_task(self._discover_jsonl())

        logger.info(
            "CCProcessPty started: tmux=%s session_id=%s mode=%s",
            self._tmux_name,
            self._session_id or "(new)",
            self._mode,
        )

    # ----- JSONL discovery (new sessions) ------------------------------------

    async def _discover_jsonl(self) -> None:
        """Watch the CC projects directory for a new .jsonl file after start.

        The file stem is the CC session_id.  Once found, start tailing.
        The session_id is also captured from the ``sessionId`` field inside
        the JSONL records during _process_jsonl_record.
        """
        projects_dir = Path.home() / ".claude" / "projects" / _normalized_dir(self._workdir)
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

        # Give the file a moment to exist
        for _ in range(100):
            if self._jsonl_path.exists():
                break
            await asyncio.sleep(0.1)
        else:
            logger.error("JSONL file never appeared: %s", self._jsonl_path)
            return

        while True:
            try:
                # Read new bytes from our last offset
                new_data = await asyncio.to_thread(self._read_new_bytes)
                if new_data:
                    for line in new_data.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            logger.warning("Skipping malformed JSONL line: %.120s", line)
                            continue
                        self._process_jsonl_record(record)
            except asyncio.CancelledError:
                logger.debug("Tail loop cancelled")
                return
            except Exception:
                logger.exception("Error in tail loop")

            # Check if tmux session is still alive
            try:
                result = _run_sync(["tmux", "has-session", "-t", self._tmux_name])
                if result.returncode != 0:
                    logger.info("tmux session gone — exiting tail loop")
                    break
            except Exception:
                logger.info("tmux check failed — exiting tail loop")
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
        """Synchronously read bytes from the JSONL file starting at _byte_offset."""
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

        Note: Unlike SDK mode, interactive CC does NOT write a ``result``
        type line. We detect turn completion via ``last-prompt`` or the
        absence of further ``assistant`` lines after a short idle period.
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
                        self._emit_event({"type": "text", "text": block.get("text", "")})
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
                logger.exception("on_event callback raised for event %s", event.get("type"))

    # ----- checkpoint management ---------------------------------------------

    def _backfill_checkpoint_uuids(self, session_id: str) -> None:
        """Fill in the session-derived uuid for checkpoints recorded before
        the session_id was captured (mirrors CCProcess behaviour)."""
        for cp in self._checkpoints:
            if not cp.get("uuid"):
                cp["uuid"] = f"{session_id}__{cp.get('seq', 0)}"

    # ----- user input --------------------------------------------------------

    async def send_text(self, text: str) -> None:
        """Send text to the claude CLI via tmux send-keys and press Enter."""
        if not self._tmux_name:
            raise RuntimeError("CCProcessPty not started")

        # Record a checkpoint before sending
        cp = {
            "seq": len(self._checkpoints),
            "content": text,
            "uuid": "",
        }
        if self._captured_session_id:
            cp["uuid"] = f"{self._captured_session_id}__{cp['seq']}"
        self._checkpoints.append(cp)

        # Use tmux send-keys with -l for literal text, then Enter
        # Split long text into chunks to avoid tmux argument limits (~4KB safe)
        chunk_size = 4000
        for i in range(0, len(text), chunk_size):
            chunk = text[i : i + chunk_size]
            result = await _run_async(
                ["tmux", "send-keys", "-t", self._tmux_name, "-l", chunk]
            )
            if result.returncode != 0:
                logger.warning("tmux send-keys -l failed: %s", result.stderr.strip())

        # Press Enter
        await _run_async(["tmux", "send-keys", "-t", self._tmux_name, "Enter"])

    async def record_user_uuid(self, uuid_val: str, content: str = "") -> None:
        """Record a user message UUID for checkpoint tracking.

        This is a no-op for the PTY backend because we don't have the SDK's
        UUID mechanism, but we keep the interface for compatibility.
        """
        # In the SDK backend, this maps a CC conversation UUID to our checkpoint.
        # For PTY mode we store it as metadata for potential future use.
        logger.debug("record_user_uuid: uuid=%s (PTY backend — stored only)", uuid_val)

    # ----- interrupt ---------------------------------------------------------

    async def interrupt(self) -> None:
        """Send Escape to the tmux pane, which CC interprets as interrupt."""
        if not self._tmux_name:
            return
        # Send Escape twice — first opens command palette, second dismisses it
        # In practice a single Escape is enough to interrupt CC's current op
        await _run_async(["tmux", "send-keys", "-t", self._tmux_name, "Escape"])
        # Small delay then another Escape to ensure interrupt
        await asyncio.sleep(0.1)
        await _run_async(["tmux", "send-keys", "-t", self._tmux_name, "Escape"])

    # ----- mode change -------------------------------------------------------

    async def set_mode(self, mode: str) -> None:
        """Store the new mode value.

        For the PTY backend, actually switching mode requires restarting the
        claude process.  The caller handles restart logic; we just persist the
        value so a subsequent start() will use it.
        """
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
        """Resolve a pending permission prompt in the tmux pane.

        Sends 'y' or 'n' (or Enter for some prompts) to the tmux session.
        Returns True if we believe we sent the key, False otherwise.

        For v1 this is simplified — we don't track request_ids, we just
        send the appropriate keystroke.
        """
        if not self._tmux_name:
            logger.warning("resolve_permission called but no tmux session")
            return False

        if not self._started:
            return False

        # Send the appropriate key
        if approved:
            key = "y"
        else:
            key = "n"

        try:
            result = _run_sync(["tmux", "send-keys", "-t", self._tmux_name, key])
            if result.returncode == 0:
                # Also send Enter to confirm (some prompts need it)
                _run_sync(["tmux", "send-keys", "-t", self._tmux_name, "Enter"])
                self._pending_approval = None
                logger.info("resolve_permission: sent '%s' to tmux", key)
                return True
            else:
                logger.warning("tmux send-keys for permission failed: %s", result.stderr.strip())
                return False
        except Exception:
            logger.exception("resolve_permission failed")
            return False

    def deny_all_permissions(self) -> None:
        """Deny all pending permissions by sending 'n' to the tmux pane.

        For v1 we just send 'n' — there's no queue of pending requests.
        """
        if self._tmux_name and self._started:
            try:
                _run_sync(["tmux", "send-keys", "-t", self._tmux_name, "n"])
                _run_sync(["tmux", "send-keys", "-t", self._tmux_name, "Enter"])
                self._pending_approval = None
            except Exception:
                logger.exception("deny_all_permissions failed")

    # ----- rewind ------------------------------------------------------------

    async def rewind_files(self, user_message_id: str) -> bool:
        """Rewind files to the state before a given user message.

        The PTY backend does not have a direct equivalent of the SDK's
        file-rewind capability.  Return False with a warning.
        """
        logger.warning(
            "rewind_files is not supported in PTY mode (user_message_id=%s). "
            "Use the SDK backend for file rewinding.",
            user_message_id,
        )
        return False

    # ----- cleanup -----------------------------------------------------------

    async def close(self) -> None:
        """Kill the tmux session, cancel background tasks, and call on_exit."""
        # Cancel discovery task if still running
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

        # Kill tmux session
        if self._tmux_name:
            try:
                await _run_async(["tmux", "kill-session", "-t", self._tmux_name])
            except Exception:
                logger.debug("tmux kill-session failed (session may already be gone)")
            self._tmux_name = None

        self._started = False

        # Notify exit
        if self._on_exit:
            try:
                result = self._on_exit(None)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("on_exit callback raised during close")

        logger.info("CCProcessPty closed")
