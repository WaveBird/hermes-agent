"""cc-bridge.core — Claude Code 会话管理（基于 claude-agent-sdk）。

与 agents-to-im 的 sdk-provider 对齐：
- 用 ``claude-agent-sdk`` (Python) 的 ``ClaudeSDKClient`` 长驻会话。
- ``can_use_tool`` 异步回调：收到工具权限请求时，把请求放入待审批队列并
  阻塞等待，直到飞书侧审批卡回调 resolve（或超时 deny）。
- 事件流（文本增量 / tool_use / result / 模式变化）以 SDK 消息对象回调给上层。

本模块不接触飞书发送层；绑定表 / 工作目录恢复逻辑保留。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

try:
    from claude_agent_sdk import (
        ClaudeAgentOptions,
        ClaudeSDKClient,
        PermissionResultAllow,
        PermissionResultDeny,
    )
except Exception:  # noqa: BLE001  (SDK 未安装时插件仍能 import，启动时给出提示)
    ClaudeSDKClient = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# 审批超时（毫秒）与 agents-to-im 对齐
PERMISSION_TIMEOUT_S = 15 * 60


# --------------------------------------------------------------------------- #
# Data classes
# --------------------------------------------------------------------------- #

@dataclass
class SessionMeta:
    """一个 CC 会话的元信息（全局会话注册表条目）。"""
    id: str                # CC session uuid（= ~/.claude/projects/<dir>/<id>.jsonl）
    title: str = ""        # 会话标题（-n 命名 或 首条 user 消息提炼）
    workdir: str = ""      # 会话工作目录
    mode: str = "default"
    # 占用锁：当前占用它的飞书话题；None=空闲可被任意话题接管
    owner_thread: Optional[str] = None
    # 该话题最后一条回复的 message_id（用于生成占用跳转链接）
    owner_msg_id: str = ""
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class CCBinding:
    """一个话题（Hermes 会话）到 CC 会话集合的绑定。"""
    thread_id: str          # 飞书话题 omt_xxx
    chat_id: str            # 所在群 chat_id (发流式卡/审批卡用)
    workdir: str            # 该会话工作目录
    mode: str = "default"   # claude 权限模式
    topic_title: str = ""   # Hermes 话题自己的标题（非 CC 会话标题）
    active_session_id: str = ""   # 当前 active 的 CC 会话 id（空=话题还没开会话）
    visited_sessions: List[str] = field(default_factory=list)  # 本话题用过的 CC 会话 id（快切）
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    # ---- 兼容旧字段 cc_session_id（迁移用）----
    @property
    def cc_session_id(self) -> str:
        # 向后兼容: 旧 reader 直接读 self.cc_session_id 时给 active_session_id
        return self.active_session_id

    @cc_session_id.setter
    def cc_session_id(self, value: str) -> None:
        self.active_session_id = value

    # 运行时（不持久化）
    proc: Optional["CCProcess"] = None
    tool_cards: Dict[str, str] = field(default_factory=dict)  # tool_use_id -> 活动 card message_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "thread_id": self.thread_id,
            "chat_id": self.chat_id,
            "workdir": self.workdir,
            "mode": self.mode,
            "topic_title": self.topic_title,
            "active_session_id": self.active_session_id,
            "visited_sessions": list(self.visited_sessions),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CCBinding":
        b = cls(
            thread_id=d["thread_id"],
            chat_id=d.get("chat_id", ""),
            workdir=d.get("workdir", ""),
            mode=d.get("mode", "default"),
            topic_title=d.get("topic_title", ""),
        )
        # 迁移: 旧数据只有 cc_session_id -> active_session_id
        b.active_session_id = d.get("active_session_id", "") or d.get("cc_session_id", "") or ""
        b.visited_sessions = list(d.get("visited_sessions", []) or [])
        if b.active_session_id and b.active_session_id not in b.visited_sessions:
            b.visited_sessions.insert(0, b.active_session_id)
        b.created_at = d.get("created_at", time.time())
        b.updated_at = d.get("updated_at", time.time())
        return b


class SessionRegistry:
    """全局 CC 会话注册表: session_id -> SessionMeta。跨所有话题唯一。

    会话是全局资源，一个 CC 会话同一时刻至多被一个话题 active（占用锁）。
    序列化到 plugin-data/cc-bridge/sessions.json。
    """

    def __init__(self, data_dir: Optional[str] = None):
        default = Path(
            os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))
        ) / "plugin-data" / "cc-bridge"
        self.dir = Path(data_dir) if data_dir else default
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / "sessions.json"
        self.sessions: Dict[str, SessionMeta] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            for d in data:
                try:
                    m = SessionMeta(**{k: d.get(k) for k in (
                        "id", "title", "workdir", "mode", "owner_thread",
                        "owner_msg_id", "created_at", "updated_at")})
                    self.sessions[m.id] = m
                except Exception:  # noqa: BLE001
                    logger.warning("skip bad session meta", exc_info=True)
        except Exception:  # noqa: BLE001
            logger.warning("failed to load session registry", exc_info=True)

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps([m.__dict__ for m in self.sessions.values()],
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self.path)

    def get(self, session_id: str) -> Optional[SessionMeta]:
        return self.sessions.get(session_id)

    def put(self, meta: SessionMeta) -> None:
        self.sessions[meta.id] = meta
        self.save()

    def owner(self, session_id: str) -> Optional[str]:
        m = self.get(session_id)
        return m.owner_thread if m else None

    def is_occupied_by(self, session_id: str, other_thread: str) -> bool:
        """该会话是否被 other_thread 之外的话题占用。"""
        o = self.owner(session_id)
        return bool(o) and o != other_thread

    def release(self, session_id: str, by_thread: str) -> None:
        """话题释放会话占用（如 /stop /close 时）。幂等。"""
        m = self.get(session_id)
        if m and m.owner_thread == by_thread:
            m.owner_thread = None
            m.owner_msg_id = ""
            self.save()


def new_session_id() -> str:
    return str(uuid.uuid4())


def _normalize_project_dir(workdir: str) -> str:
    """把 workdir 转成 CC 项目目录名（~/.claude/projects/<normalized>/）。

    CC 用绝对路径每段 '-' join（不含首 '/'），如 /home/xx/a → -home-xx-a。
    """
    p = os.path.abspath(os.path.expanduser(workdir)).strip("/")
    return "-" + p.replace("/", "-")


def _extract_cmd_placeholder(f: Path) -> str:
    """空壳会话（无真实 user 内容）的占位标题：取其执行过的首个命令名。

    CC terminal /resume 不过滤任何会话；这里对齐为 `(命令: /usage)` 式标注，
    完全无内容则显示文件时间。
    """
    import re as _re
    try:
        with open(f, encoding="utf-8", errors="ignore") as fh:
            for i, line in enumerate(fh):
                if i > 40:
                    break
                try:
                    rec = json.loads(line)
                except Exception:  # noqa: BLE001
                    continue
                if rec.get("type") != "user":
                    continue
                c = (rec.get("message", {}) or {}).get("content")
                if isinstance(c, str):
                    m = _re.search(r"<command-name>/([\w:-]+)", c)
                    if m:
                        return f"(命令: /{m.group(1)})"
    except Exception:  # noqa: BLE001
        pass
    ts = time.strftime("%m-%d %H:%M", time.localtime(f.stat().st_mtime))
    return f"(空会话 · {ts})"


def list_all_project_sessions(per_project: int = 5,
                              max_projects: int = 5) -> List[dict]:
    """全局枚举 ~/.claude/projects/ 下所有项目及其最近会话（布布 2026-09-14）。

    返回 [{dir, workdir, mtime, sessions: [SessionMeta×per_project]}]，
    项目按目录 mtime 新→旧取前 max_projects 个；每项目内会话 mtime 降序。
    project 目录名反解真实路径：`-home-vm-x` → `/home/vm/x`（首段补根 `/`，
    与 CC normalized 规则一致——路径本身含 `-` 时显示为归一化形式，不影响 id 定位）。
    """
    home = str(Path.home())
    base = Path(home) / ".claude" / "projects"
    if not base.exists():
        return []
    out: List[dict] = []
    dirs = [p for p in base.iterdir()
            if p.is_dir() and any(p.glob("*.jsonl"))]
    dirs.sort(key=lambda p: -max(
        (f.stat().st_mtime for f in p.glob("*.jsonl")), default=0))
    for d in dirs[:max_projects]:
        items: List[SessionMeta] = []
        real_cwd = ""
        files = sorted(d.glob("*.jsonl"),
                       key=lambda f: -f.stat().st_mtime)[:per_project]
        for f in files:
            try:
                mt = f.stat().st_mtime
                title = _extract_session_title(f) or _extract_cmd_placeholder(f)
                if not real_cwd:
                    real_cwd = _session_cwd(f)
                items.append(SessionMeta(
                    id=f.stem, title=title, workdir=real_cwd or "",
                    created_at=mt, updated_at=mt))
            except Exception:  # noqa: BLE001
                continue
        if items:
            wd = real_cwd or _project_dir_to_workdir(d.name)
            for it in items:
                it.workdir = wd
            out.append({"dir": d.name, "workdir": wd,
                        "mtime": items[0].updated_at, "sessions": items})
    return out


def _session_cwd(f: Path) -> str:
    """从 session jsonl 记录的 cwd 字段取真实项目路径（比目录名反解可靠）。"""
    try:
        with open(f, encoding="utf-8", errors="ignore") as fh:
            for i, line in enumerate(fh):
                if i > 40:
                    break
                try:
                    rec = json.loads(line)
                except Exception:  # noqa: BLE001
                    continue
                c = rec.get("cwd")
                if isinstance(c, str) and c.strip():
                    return c.strip()
    except Exception:  # noqa: BLE001
        pass
    return ""


def _project_dir_to_workdir(dirname: str) -> str:
    """CC 项目目录名 → 反解为可读路径。-home-vm-a-b → /home/vm/a/b。

    注意：路径本身含 '-' 时会被拆错，仅作 cwd 缺失时的兜底显示。
    """
    parts = [p for p in dirname.split("-") if p]
    return "/" + "/".join(parts) if parts else dirname


def list_project_sessions(workdir: str, limit: int = 50) -> List[SessionMeta]:
    """枚举 ~/.claude/projects/<normalized>/ 下该 workdir 的所有历史 CC 会话。

    每个 .jsonl 文件 = 一个 CC 会话，按 mtime 新→旧排序。从 user 记录提取标题
    （跳过 <local-command-caveat> 系统前缀）。与 list_recent_projects 同源思路。
    """
    home = str(Path.home())
    for base in (
        Path(home) / ".claude" / "projects",
        Path(os.environ.get("CLAUDE_CONFIG_DIR", str(Path(home) / ".claude"))) / "projects",
    ):
        if not base.exists():
            continue
        dirname = _normalize_project_dir(workdir)
        d = base / dirname
        if not d.exists():
            continue
        out: List[SessionMeta] = []
        for f in d.glob("*.jsonl"):
            try:
                mt = f.stat().st_mtime
                sid = f.stem
                title = _extract_session_title(f)
                if not title:
                    # 与 CC terminal /resume 对齐：不过滤任何会话；
                    # 纯命令空壳用其命令名做占位标题（布布 2026-09-14 拍板）
                    title = _extract_cmd_placeholder(f)
            except Exception:  # noqa: BLE001
                continue
            out.append(SessionMeta(
                id=sid, title=title, workdir=workdir,
                created_at=mt, updated_at=mt,
            ))
        out.sort(key=lambda m: -m.updated_at)
        return out[:limit]
    return []


def _extract_session_title(f: Path) -> str:
    """从 CC session jsonl 首段提取标题：-n 命名（name 字段）或第一条真实 user 文本。

    无任何真实用户内容（纯命令/空壳会话）返回 ""，由调用方过滤——
    这类会话没有任何信息量，resume 回去也无上下文（布布 2026-09-13 拍板）。
    """
    try:
        with open(f, encoding="utf-8", errors="ignore") as fh:
            for i, line in enumerate(fh):
                if i > 30:
                    break
                try:
                    rec = json.loads(line)
                except Exception:  # noqa: BLE001
                    continue
                # 顶层 name（-n 起的会话名）
                name = rec.get("name")
                if isinstance(name, str) and name.strip():
                    return name.strip()
                if rec.get("type") == "user":
                    msg = rec.get("message", {}) or {}
                    c = msg.get("content")
                    if not isinstance(c, str) and isinstance(c, list):
                        texts = [b.get("text", "") for b in c
                                 if isinstance(b, dict) and b.get("type") == "text"]
                        c = "\n".join(texts)
                    if isinstance(c, str) and c.strip() and \
                            not c.startswith("<local-command-caveat>") and \
                            not c.startswith("<command-name>") and \
                            not c.startswith("<"):
                        return c.strip()[:60]
    except Exception:  # noqa: BLE001
        pass
    return ""


# --------------------------------------------------------------------------- #
# Claude Code session driver (claude-agent-sdk)
# --------------------------------------------------------------------------- #

class CCProcess:
    """一个话题的 Claude Code 长驻会话。

    内部用 ``ClaudeSDKClient`` 维护连接；``send_text`` 每次 ``query()`` 发送一轮，
    事件经后台 ``receive_messages`` 循环路由到 ``on_event`` 回调。
    审批由 ``can_use_tool`` 回调触发，等待飞书侧通过 ``resolve_permission`` 落定。
    """

    def __init__(
        self,
        workdir: str,
        session_id: Optional[str],
        mode: str,
        claude_executable: str = "claude",
        *,
        extra_env: Optional[Dict[str, str]] = None,
        on_event: Optional[Callable[[Any], Awaitable[None]]] = None,
        on_exit: Optional[Callable[[Any], Awaitable[None]]] = None,
    ):
        if ClaudeSDKClient is None:
            raise RuntimeError(
                "claude-agent-sdk 未安装。请在 Hermes venv 运行: "
                "pip install claude-agent-sdk==0.2.141"
            )
        self.workdir = workdir
        self.session_id = session_id
        self.mode = mode
        self.executable = claude_executable
        self.extra_env = extra_env or {}
        self.on_event = on_event                # async def on_event(msg) -> None
        self.on_exit = on_exit                  # async def on_exit(err) -> None

        self._client: Optional[ClaudeSDKClient] = None
        self._recv_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        # /rewind 检查点: 本进程内每条用户消息的 uuid(replay 回传后回填)
        self._checkpoints: list = []

        self.last_active: float = time.time()
        self.closed = False

        # 待审批队列: request_id -> {"future", "tool", "input"}
        self._pending_perms: Dict[str, Dict[str, Any]] = {}

    @property
    def running(self) -> bool:
        return self._client is not None and self._recv_task is not None and not self._recv_task.done()

    @property
    def pending_approval(self) -> Optional[Dict[str, Any]]:
        """供上层读取最近 / 首个待审批请求（兼容旧 _handle_cc_action 分支）。"""
        if not self._pending_perms:
            return None
        k = next(iter(self._pending_perms))
        p = self._pending_perms[k]
        return {"req": k, "tool": p.get("tool"), "input": p.get("input")}

    async def start(self) -> None:
        """连接 Claude 并启动接收循环。resume=session_id 延续上下文。"""
        env = os.environ.copy()
        # 去掉可能把 AI 请求误路由到代理的变量
        for k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
            env.pop(k, None)
        env.setdefault("DISABLE_AUTOUPDATER", "1")
        env.update(self.extra_env)
        # 指定可执行文件（如配置了 claude_executable）
        if self.executable and self.executable != "claude":
            env["CLAUDE_AGENT_SDK_CLI_PATH"] = self.executable

        async def _can_use_tool(tool_name: str, input_: Any, ctx: Any) -> Any:
            """SDK 权限回调：挂起直到飞书侧审批卡给出决定。"""
            # bypass 会话：SDK 存在 can_use_tool 回调时其优先级高于
            # --permission-mode，CLI 端 bypass 被架空 → 此处直接放行
            if self.mode == "bypassPermissions":
                return PermissionResultAllow(updated_input=None)
            t0 = time.time()
            req_id = str(getattr(ctx, "tool_use_id", None) or uuid.uuid4())
            logger.info("cc[%s] permission ask %s %.60s (idle %.3fs)",
                        self.session_id or "?", tool_name, str(input_),
                        t0 - self.last_active)
            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            self._pending_perms[req_id] = {
                "future": fut, "tool": tool_name, "input": input_,
                "created": time.time(),
            }
            try:
                # 通知上层弹审批卡
                if self.on_event:
                    try:
                        await self.on_event({
                            "type": "permission_request",
                            "request_id": req_id,
                            "tool": tool_name,
                            "input": input_,
                        })
                    except Exception:  # noqa: BLE001
                        logger.warning("cc: permission card emit failed", exc_info=True)
                # 阻塞等待用户决定
                try:
                    decision = await asyncio.wait_for(fut, timeout=PERMISSION_TIMEOUT_S)
                except asyncio.TimeoutError:
                    decision = {"behavior": "deny", "message": "审批超时（15 分钟），已拒绝。"}
                if decision and decision.get("behavior") == "allow":
                    allow_kwargs: Dict[str, Any] = {}
                    if decision.get("updated_input"):
                        allow_kwargs["updated_input"] = decision["updated_input"]
                    ups = decision.get("updated_permissions")
                    if ups:
                        # "Allow Always"：会话级规则随本次结果一起提交给 SDK
                        allow_kwargs["updated_permissions"] = ups
                    return PermissionResultAllow(**allow_kwargs)
                return PermissionResultDeny(
                    message=decision.get("message", "Denied by user") if decision else "Denied by user",
                    interrupt=bool(decision.get("interrupt")),
                )
            finally:
                self._pending_perms.pop(req_id, None)

        opts = ClaudeAgentOptions(
            cwd=self.workdir,
            # 兜底归一化：历史脏值(bypass 等)会让 CLI 启动即失败
            permission_mode=(
                self.mode if self.mode in (
                    "default", "acceptEdits", "plan", "bypassPermissions",
                    "dontAsk", "auto") else "default"),
            can_use_tool=_can_use_tool,
            include_partial_messages=True,
            env=env,
            # /rewind 支持: 文件检查点 + 用户消息 uuid 回传(replay-user-messages)
            enable_file_checkpointing=True,
            extra_args={"replay-user-messages": None},
            # 仅在有真实 CC session id 时 resume —— 占位 uuid 会导致 CLI 报错退出
            resume=(self.session_id or None) if self.session_id else None,
            cli_path=self.executable if self.executable and self.executable != "claude" else None,
        )
        client = ClaudeSDKClient(opts)
        await client.connect()
        self._client = client
        self._recv_task = asyncio.create_task(self._recv_loop())
        self.last_active = time.time()

    async def _recv_loop(self) -> None:
        """后台持续消费 SDK 消息，路由到 on_event。"""
        try:
            async for msg in self._client.receive_messages():  # type: ignore[union-attr]
                self.last_active = time.time()
                if self.on_event:
                    try:
                        await self.on_event(msg)
                    except Exception:  # noqa: BLE001
                        logger.warning("cc: on_event callback error", exc_info=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("cc: receive loop error: %s", exc)
            if self.on_exit:
                try:
                    await self.on_exit(exc)
                except Exception:  # noqa: BLE001
                    logger.warning("cc: on_exit error", exc_info=True)

    async def send_text(self, text: str) -> None:
        """向正在进行的会话发送一条用户消息。"""
        if not self.running:
            raise RuntimeError("CC 会话未在运行")
        self.last_active = time.time()
        self._checkpoints.append({
            "text": text[:80], "uuid": "", "ts": time.time(),
        })
        await self._client.query(text)  # type: ignore[union-attr]

    async def record_user_uuid(self, uuid: str, content: str = "") -> None:
        """回填 UserMessage.uuid → 最旧的未回填 checkpoint(按发送顺序对齐)。

        replay-user-messages 的回传顺序与 query 发送顺序一致, 所以必须从队首
        找第一个空位; 若从新往旧填, 两条同时待定时会交错错配。
        """
        for cp in self._checkpoints:
            if not cp["uuid"]:
                cp["uuid"] = uuid
                if content and not cp["text"]:
                    cp["text"] = content[:80]
                return

    @property
    def checkpoints(self) -> list:
        """已捕获 uuid 的检查点(旧→新)。"""
        return [cp for cp in self._checkpoints if cp["uuid"]]

    async def rewind_files(self, user_message_id: str) -> bool:
        """把工作区文件恢复到指定用户消息之前的状态(SDK control 协议)。"""
        if not self.running or not self._client:
            return False
        try:
            rw = getattr(self._client, "rewind_files", None)
            if not rw:
                logger.warning("cc: SDK 无 rewind_files（版本过低）")
                return False
            await rw(user_message_id)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("cc: rewind_files(%s) failed: %s", user_message_id[:8], exc)
            return False

    async def interrupt(self) -> None:
        """中断当前输出（等价于终端 Esc）。"""
        if not self.running or not self._client:
            return
        self.last_active = time.time()
        try:
            await self._client.interrupt()
        except Exception:  # noqa: BLE001
            logger.warning("cc: interrupt failed", exc_info=True)

    async def set_mode(self, mode: str) -> None:
        """运行中热切换权限模式（对应审批卡底部模式切换按钮）。"""
        self.mode = mode
        if not self.running or not self._client:
            return
        try:
            # SDK 的 set_permission_mode 仅在部分 CLI 版本可用；失败则记录
            set_mode = getattr(self._client, "set_permission_mode", None)
            if set_mode:
                await set_mode(mode)
        except Exception:  # noqa: BLE001
            logger.warning("cc: set_permission_mode failed（将随下次会话生效）", exc_info=True)

    def resolve_permission(self, request_id: str, approved: bool,
                           message: str = "", interrupt: bool = False,
                           scope: str = "once", tool: str = "") -> bool:
        """飞书侧审批卡回调 → 解除 can_use_tool 的挂起。

        返回 True 表示成功解析到对应请求。scope="always" 时通过 updated_permissions
        把该工具加入会话级 allow 规则，后续同类调用不再审批。
        """
        entry = self._pending_perms.get(request_id)
        if not entry:
            # 兼容：也可能传空 req，尝试放行首个待审
            if not request_id and self._pending_perms:
                request_id = next(iter(self._pending_perms))
                entry = self._pending_perms.get(request_id)
        if not entry:
            return False
        fut: asyncio.Future = entry["future"]
        if fut.done():
            return False
        tool_name = tool or entry.get("tool") or ""
        if approved:
            result: Dict[str, Any] = {"behavior": "allow", "updated_input": None}
            if scope == "always" and tool_name:
                try:
                    from claude_agent_sdk import PermissionUpdate  # noqa: PLC0415
                    from claude_agent_sdk.types import PermissionRuleValue  # noqa: PLC0415
                    result["updated_permissions"] = [
                        PermissionUpdate(
                            type="addRules",
                            rules=[PermissionRuleValue(tool_name=tool_name)],
                            behavior="allow",
                            destination="session",
                        )
                    ]
                except Exception:  # noqa: BLE001
                    logger.warning("cc: build always-permission failed，退化为 once", exc_info=True)
        else:
            result = {"behavior": "deny", "message": message or "Denied by user",
                      "interrupt": interrupt}
        entry_wait = time.time() - (entry.get("created") or time.time())
        logger.info("cc[%s] permission resolved %s approved=%s waited=%.1fs",
                    getattr(self, "session_id", "?"), request_id, approved,
                    entry_wait)
        fut.set_result(result)
        return True

    def deny_all_permissions(self) -> None:
        """关闭/清理时拒绝所有挂起的审批请求。"""
        for req_id, entry in list(self._pending_perms.items()):
            fut = entry.get("future")
            if fut and not fut.done():
                fut.set_result({"behavior": "deny", "message": "会话已关闭", "interrupt": False})
        self._pending_perms.clear()

    async def close(self) -> None:
        """优雅结束会话。"""
        if self.closed:
            return
        self.closed = True
        self.deny_all_permissions()
        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if self._client:
            try:
                await self._client.disconnect()
            except Exception:  # noqa: BLE001
                logger.warning("cc: disconnect error", exc_info=True)
            self._client = None
        if self.on_exit:
            try:
                await self.on_exit(None)
            except Exception:  # noqa: BLE001
                logger.warning("cc: on_exit error", exc_info=True)


# --------------------------------------------------------------------------- #
# Binding store
# --------------------------------------------------------------------------- #

class BindingStore:
    """thread_id → CCBinding 的 JSON 持久化绑定表。"""

    def __init__(self, data_dir: Optional[str] = None):
        default = Path(
            os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))
        ) / "plugin-data" / "cc-bridge"
        self.dir = Path(data_dir) if data_dir else default
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / "thread_bindings.json"
        self.bindings: Dict[str, CCBinding] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            for d in data:
                try:
                    b = CCBinding.from_dict(d)
                    self.bindings[b.thread_id] = b
                except Exception:  # noqa: BLE001
                    logger.warning("skip bad binding", exc_info=True)
        except Exception:  # noqa: BLE001
            logger.warning("failed to load bindings", exc_info=True)

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps([b.to_dict() for b in self.bindings.values()],
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self.path)

    def get(self, thread_id: str) -> Optional[CCBinding]:
        return self.bindings.get(thread_id)

    def put(self, binding: CCBinding) -> None:
        self.bindings[binding.thread_id] = binding
        self.save()

    def remove(self, thread_id: str) -> Optional[CCBinding]:
        b = self.bindings.pop(thread_id, None)
        if b is not None:
            self.save()
        return b

    def all(self) -> List[CCBinding]:
        return list(self.bindings.values())


def new_session_id() -> str:
    return str(uuid.uuid4())


def list_recent_projects(default_workdir: Optional[str] = None, limit: int = 10) -> List[Dict[str, str]]:
    """扫描 ~/.claude/projects/*.jsonl 恢复最近工作目录。返回 [{label, value}]。

    复用 agents-to-im 的 recent-workspaces 思路：按 jsonl mtime 聚合 normalized cwd。
    cwd 记录在每个会话文件的首行元数据里，只读前几行即可；
    全量逐行解析大 jsonl 会拖死调用方（/cc:new 时可阻塞数秒）。
    """
    projects: Dict[str, float] = {}
    home = str(Path.home())
    for base in (
        Path(home) / ".claude" / "projects",
        Path(os.environ.get("CLAUDE_CONFIG_DIR", str(Path(home) / ".claude"))) / "projects",
    ):
        if not base.exists():
            continue
        for f in base.glob("*/*.jsonl"):
            try:
                mt = f.stat().st_mtime
                cwd = None
                with open(f, encoding="utf-8", errors="ignore") as fh:
                    for line in fh:
                        try:
                            rec = json.loads(line)
                        except Exception:  # noqa: BLE001
                            continue
                        c = rec.get("cwd")
                        if isinstance(c, str) and c:
                            cwd = c
                            break  # cwd 元数据在首行附近，无需读完整文件
                if cwd and os.path.isdir(cwd):
                    projects[cwd] = max(projects.get(cwd, 0), mt)
            except Exception:  # noqa: BLE001
                continue

    items = sorted(projects.items(), key=lambda kv: -kv[1])
    opts: List[Dict[str, str]] = []
    seen = set()
    for path, _ in items:
        if path in seen:
            continue
        seen.add(path)
        opts.append({
            "label": f"{Path(path).name} · {path}",
            "value": path,
        })
        if len(opts) >= limit:
            break

    if default_workdir and os.path.isdir(default_workdir) and default_workdir not in seen:
        opts.insert(0, {"label": f"{Path(default_workdir).name} · {default_workdir}",
                        "value": default_workdir})
    return opts