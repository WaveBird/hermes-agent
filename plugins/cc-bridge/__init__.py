"""cc-bridge — 把 Claude Code 桥接到飞书话题。

注册 ``pre_gateway_dispatch`` hook：识别 bound 话题(/cc 会话)的消息，拦截并转给
该话题的 Claude Code 子进程；CC 的流式输出、权限请求、tools 活动经飞书卡片推回。
非 bound 话题 / 非 cc 消息原样放行。

安全边界：
- 只有显式 /cc:new 建过会话的话题才被拦截；未建过的话题完全不受影响。
- CC 权限默认走审批卡 (default 模式)；审批卡内提供模式切换快捷按钮。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import textwrap
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import cards
from . import core
from .core import (BindingStore, CCBinding, CCProcess, SessionMeta,
                   SessionRegistry)

logger = logging.getLogger(__name__)

# instance 状态（单进程内常驻）
_store: Optional[BindingStore] = None
_registry: Optional[SessionRegistry] = None
_claude_executable: str = "claude"
_default_workdir: str = ""
_default_mode: str = "default"
_show_tool_cards: bool = True
_stream_cards: bool = True
_idle_kill_seconds: int = 0
_reaper_started: bool = False

# 注册的 hook 回调引用（用于注销）
_STATE = {
    "store": None,
    "streams": {},       # thread_id -> _StreamSession (流式卡状态)
    "pending_name": {},  # thread_id -> /new 暂存的名字
    "sid_announced": {}, # thread_id -> True (session id 已补发)
}

# fire-and-forget 任务的强引用集：asyncio 仅弱引用 task，
# 不持引用的任务可能在执行中被 GC 静默吞掉（官方文档明确警告）
_BG_TASKS: set = set()


def _fire_and_forget(coro) -> None:
    try:
        loop = asyncio.get_event_loop()
        task = loop.create_task(coro)
    except RuntimeError:
        task = asyncio.create_task(coro)

    def _log_task_exc(t: "asyncio.Task") -> None:
        _BG_TASKS.discard(t)
        if not t.cancelled() and t.exception() is not None:
            logger.warning("cc: background task failed: %s",
                           t.exception(), exc_info=t.exception())

    _BG_TASKS.add(task)
    task.add_done_callback(_log_task_exc)


class _StreamSession:
    """一个线程的流式输出状态：把 CC 增量文本逐步推到一张流式卡。"""
    def __init__(self, adapter, chat_id: str, thread_id: str, binding: CCBinding):
        self.adapter = adapter
        self.chat_id = chat_id
        self.thread_id = thread_id
        self.binding = binding
        self.card_active = False
        self.message_id: Optional[str] = None
        self.buffer: List[str] = []
        self._flush_timer: Optional[asyncio.TimerHandle] = None
        self._last_flush = 0.0
        self._lock = asyncio.Lock()

    async def start(self, first_text: str = "") -> None:
        """创建流式卡（若配置开启）。

        注意：send_streaming_card 内部走 message.create——带 metadata.thread_id
        会触发 create(receive_id_type=thread_id)，该路径实测被飞书拒绝
        （99992402）。因此这里先解析话题锚点，用 reply_to 定位到线程内。

        所有流式卡内容头部加 💠 Claude Code 来源标识。
        """
        if not _stream_cards:
            return
        _cc_tag = _SOURCE_TAGS.get("cc", "")
        try:
            anchor = await _thread_anchor(self.adapter, self.chat_id, self.thread_id)
            display = first_text or "💭 思考中..."
            if _cc_tag:
                display = f"{_cc_tag}\n\n{display}"
            result = await self.adapter.send_streaming_card(
                self.chat_id,
                display,
                reply_to=anchor,
                metadata={"thread_id": self.thread_id} if anchor else None,
            )
            self.card_active = bool(getattr(result, "success", False))
            self.message_id = getattr(result, "message_id", None) or self.message_id
            if not self.message_id:
                self.card_active = False
                logger.warning("cc: streaming card create failed: %s",
                               getattr(result, "error", ""))
        except Exception as exc:  # noqa: BLE001
            logger.warning("cc: send streaming card failed, fallback to plain: %s", exc)
            self.card_active = False

    async def append(self, text: str) -> None:
        if not self.card_active or not self.message_id:
            return
        self.buffer.append(text)
        now = asyncio.get_event_loop().time()
        if now - self._last_flush >= 0.25 or len("".join(self.buffer)) > 500:
            await self._flush()

    async def _flush(self) -> None:
        if not self.buffer or not self.message_id:
            return
        async with self._lock:
            text = "".join(self.buffer)
            self.buffer = []
            if not text.strip():
                return
            try:
                await self.adapter.edit_message(
                    self.chat_id,
                    self.message_id,
                    text,
                    finalize=False,
                )
                self._last_flush = asyncio.get_event_loop().time()
            except Exception as exc:  # noqa: BLE001
                logger.warning("cc: streaming card update failed: %s", exc)

    async def finish(self, final_text: str) -> None:
        """结束流式卡，落地最终内容。"""
        if not self.message_id:
            return
        async with self._lock:
            try:
                await self.adapter.edit_message(
                    self.chat_id,
                    self.message_id,
                    final_text,
                    finalize=True,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("cc: finalize streaming card failed: %s", exc)
        self.card_active = False
        self.message_id = None


# --------------------------------------------------------------------------- #
# 入口：register(ctx)
# --------------------------------------------------------------------------- #

def register(ctx) -> None:
    """loader 入口：建 store、读配置、注册 hook。"""
    global _store, _claude_executable, _default_workdir, _default_mode
    global _show_tool_cards, _stream_cards, _idle_kill_seconds

    _claude_executable = str(ctx.get_config("runtime.claude_executable", "claude") or "claude")
    _default_workdir = str(ctx.get_config("runtime.default_workdir", "") or "")
    _default_mode = str(ctx.get_config("runtime.mode", "default") or "default")
    _show_tool_cards = bool(ctx.get_config("bridge.show_tool_cards", True))
    _stream_cards = bool(ctx.get_config("bridge.stream_cards", True))
    try:
        _idle_kill_seconds = int(ctx.get_config("bridge.idle_kill_seconds", 0) or 0)
    except (TypeError, ValueError):
        _idle_kill_seconds = 0

    data_dir = str(ctx.get_config("data_dir", "") or "")
    _store = BindingStore(data_dir=data_dir or None)
    _registry = SessionRegistry(data_dir=data_dir or None)

    ctx.register_hook("pre_gateway_dispatch", _pre_gateway_dispatch)
    if _idle_kill_seconds > 0:
        global _reaper_started
        if not _reaper_started:
            _reaper_started = True
            _fire_and_forget(_idle_reaper_loop())
    logger.info("cc-bridge ready (claude=%s default_mode=%s store=%s)",
                _claude_executable, _default_mode, _store.dir)


async def _idle_reaper_loop() -> None:
    """空闲 CC 进程回收（idle_kill_seconds>0 时启用；每分钟扫一次）。"""
    while True:
        await asyncio.sleep(60)
        try:
            now = time.time()
            for b in list(_store.all()):
                proc = b.proc
                if proc is not None and proc.running \
                        and now - getattr(proc, "last_active", now) > _idle_kill_seconds:
                    logger.info("cc[%s] idle %ss, reaping", b.thread_id, _idle_kill_seconds)
                    await proc.close()
                    b.proc = None
                    _store.put(b)
        except Exception:  # noqa: BLE001
            logger.warning("cc: idle reaper error", exc_info=True)


# --------------------------------------------------------------------------- #
# 统一会话恢复助手（topic ↔ CC session 映射持久化 + 消息进入时检查恢复）
# --------------------------------------------------------------------------- #

_FAIL_THRESH = 3          # 连续复活失败 N 次后判定映射失效, 停止自动重试
_fail_counts: Dict[str, int] = {}   # thread_id -> 连续失败次数(内存态)

async def ensure_cc_session(binding: CCBinding) -> bool:
    """确保 binding.proc 正在运行；空则用持久化的 cc_session_id 静默恢复。

    返回 True=进程可用。所有"消息进来先检查恢复"的路径统一走这里，
    与各命令自带的明确报错解耦。
    """
    if binding.proc is not None and binding.proc.running:
        _fail_counts.pop(binding.thread_id, None)
        return True
    sid = (binding.active_session_id or "").strip()
    if not sid:
        return False                      # 从未有过会话：不算失败，交上层引导 /new
    if _fail_counts.get(binding.thread_id, 0) >= _FAIL_THRESH:
        return False                      # 映射已判失效：不再无限复活
    try:
        await _start_process_for_binding(binding)
    except Exception as exc:  # noqa: BLE001
        _fail_counts[binding.thread_id] = _fail_counts.get(binding.thread_id, 0) + 1
        logger.warning("cc[%s] auto-resume failed (%d/%d): %s",
                       binding.thread_id,
                       _fail_counts[binding.thread_id], _FAIL_THRESH, exc)
        return False
    _fail_counts.pop(binding.thread_id, None)
    logger.info("cc[%s] resumed persisted session %s…", binding.thread_id, sid[:8])
    return True


def mark_session_invalid(thread_id: str) -> None:
    """resume 已确认失效（如 CLI 报 no such session）：清计数放行重建。"""
    _fail_counts.pop(thread_id, None)


# --------------------------------------------------------------------------- #
# pre_gateway_dispatch 钩子
# --------------------------------------------------------------------------- #

def _pre_gateway_dispatch(event, gateway=None, session_store=None, **kw) -> Optional[Dict[str, Any]]:
    """主拦截点。bound 话题消息 → CC；CC 卡片回调 → 处理；其余放行。

    返回 {"action":"skip"} 表示已处理（不再进普通 agent turn）。
    """
    try:
        source = getattr(event, "source", None)
        if source is None:
            return None
        text = getattr(event, "text", "") or ""
        thread_id = getattr(source, "thread_id", None) or ""
        chat_id = getattr(source, "chat_id", "") or ""

        # 卡片回调（/cc 相关的合成事件；卡片只出现在已绑定话题里）
        if text.startswith("/card "):
            return _handle_card_callback(text, source, gateway) or None

        # 绑定话题：全部接管 —— 裸命令映射 CC、/cc:* 直达、普通消息路由 CC。
        # （这是唯一处理 CC 命令的话题类型：通过 /cc:new 建立绑定的话题）
        binding = _store.get(thread_id) if (thread_id and _store) else None
        if binding is not None:
            stripped = text.strip()
            # 审批快捷回复：仅一个待审批请求时 1/2/3 映射 允许一次/总是允许/拒绝
            if (
                stripped in ("1", "2", "3")
                and binding.proc is not None
                and len(getattr(binding.proc, "_pending_perms", {})) == 1
            ):
                scope = {"1": "once", "2": "always"}.get(stripped)
                approved = stripped in ("1", "2")
                p = binding.proc.pending_approval or {}
                cc = {"req": p.get("req", ""), "tool": p.get("tool", "")}
                if scope:
                    cc["scope"] = scope
                _fire_and_forget(_resolve_permission(thread_id, cc, approved=approved))
                return {"action": "skip", "reason": f"cc-bridge quick approve {stripped}"}
            # 显式 /cc:* → 直接分发
            if stripped.startswith("/cc:"):
                _fire_and_forget(_dispatch_cc_command(stripped, thread_id, chat_id, gateway, source))
                return {"action": "skip", "reason": f"cc-bridge command {stripped}"}
            # 裸斜杠命令：白名单（桥接管理面）→ /cc:* 处理器；
            # 其余 /xxx 原样透传 CC 本体 —— prompt 型命令(/model /compact /cost
            # /context /review 等)在 headless 下原生可执行，与 terminal 体验一致。
            m_bare = re.match(r"^/([\w:-]+)(?:\s+(.*))?$", stripped, re.DOTALL) if stripped.startswith("/") else None
            if m_bare and m_bare.group(1).lower() in _BARE_CMDS:
                verb_text = f"/cc:{m_bare.group(1).lower()}" + (f" {m_bare.group(2)}" if m_bare.group(2) else "")
                _fire_and_forget(_dispatch_cc_command(verb_text, thread_id, chat_id, gateway, source))
                return {"action": "skip", "reason": f"cc-bridge bare cmd {stripped}"}
            msg_mid = str(getattr(event, "message_id", "") or "")
            _fire_and_forget(_route_to_cc(binding, text, gateway, source,
                                          message_id=msg_mid))
            return {"action": "skip", "reason": f"cc-bridge bound thread {thread_id}"}

        # 私聊(DM, 无话题)：CC 命令必须带 /cc: 前缀（含 /cc:new —— 自动开话题并绑定）。
        # 所有裸斜杠命令一律放行 Hermes 原生命令体系。
        dm_text = text.strip()
        if not thread_id and dm_text.startswith("/cc:"):
            _fire_and_forget(_dispatch_cc_command(dm_text, "", chat_id, gateway, source))
            return {"action": "skip", "reason": f"cc-bridge dm command {dm_text}"}

        # 未绑定话题及其他场景：完全放行 Hermes —— 不接受任何 cc 命令，
        # 与正常话题无差别（布布 2026-09-13 拍板）。CC 会话唯一入口 = 私聊 /cc:new。
        return None
    except Exception as exc:  # noqa: BLE001
        logger.warning("cc-bridge hook error: %s", exc, exc_info=True)
        return None


# --------------------------------------------------------------------------- #
# 命令分发
# --------------------------------------------------------------------------- #

_CC_CMD_RE = re.compile(r"^/cc:(\w+)(?:\s+(.*))?$", re.DOTALL)

# 绑定话题内可直接使用的裸斜杠命令别名（与 /cc:<verb> 等价）。
# 仅白名单内的会被桥接接管；其余 /xxx 原样转发给 CC 本体处理。
_BARE_CMDS = frozenset({"new", "reset", "cd", "mode", "stop", "status",
                        "resume", "help", "rewind"})


async def _dispatch_cc_command(cmd_text: str, thread_id: str, chat_id: str,
                               gateway, source) -> None:
    logger.info("cc: dispatch %r tid=%r chat=%s adapter=%s",
                cmd_text, thread_id, chat_id,
                "yes" if _get_adapter(gateway) else "NO")
    m = _CC_CMD_RE.match(cmd_text.strip())
    if not m:
        return
    verb, arg = m.group(1), (m.group(2) or "").strip()
    adapter = _get_adapter(gateway)
    if adapter is None:
        logger.warning("cc: no feishu adapter")
        return
    try:
        if verb == "new":
            await _cmd_new(thread_id, chat_id, adapter, source, arg)
        elif verb == "cd":
            await _cmd_cd(thread_id, adapter, arg)
        elif verb == "reset":
            await _cmd_reset(thread_id, chat_id, adapter)
        elif verb == "resume":
            await _cmd_resume(thread_id, chat_id, adapter, arg)
        elif verb == "mode":
            await _cmd_mode(thread_id, chat_id, adapter, arg)
        elif verb == "rewind":
            await _cmd_rewind(thread_id, chat_id, adapter, arg)
        elif verb == "stop":
            await _cmd_stop(thread_id, chat_id, adapter)
        elif verb == "status":
            await _cmd_status(thread_id, chat_id, adapter)
        elif verb == "help":
            binding = _store.get(thread_id) if thread_id else None
            scope = "" if binding is not None else "\n（未绑定话题——先 /cc:new `<目录>` 开一个会话）"
            try:
                from .help_text import build_help  # type: ignore
            except ImportError:
                sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
                from help_text import build_help  # type: ignore
            await _send_to_thread(adapter, chat_id, build_help() + scope, thread_id or "")
        else:
            await _send_to_thread(adapter, chat_id,
                                  "未知 cc 命令，支持：new / cd / reset / resume / mode / stop / status",
                                  thread_id or "")
    except Exception as exc:  # noqa: BLE001
        logger.warning("cc command error: %s", exc)
        try:
            await _send_to_thread(adapter, chat_id,
                                  f"⚠️ cc 命令执行失败：{exc}",
                                  thread_id or "")
        except Exception:  # noqa: BLE001
            pass


async def _cmd_new(thread_id: str, chat_id: str, adapter, source, arg: str) -> None:
    """/cc:new [会话名]：在当前话题新建一个 CC 会话（等同 claude 终端命令）。

    - 无参 /new: 新建会话，workdir 沿用绑定已有或弹卡选择；提示文本。
    - /new <名字>: 新建会话并命名（对齐 claude -n）。
    - thread_id 为空（私聊/主群直发）→ 先建话题线程再继续。
    """
    session_name = arg.strip() if arg else ""
    # 解析可能带上的 /new 目录形式（历史兼容）：不强制
    if not thread_id:
        anchor_text = f"🆕 Claude Code 会话{(' · ' + session_name) if session_name else ''}"
        new_tid = await _create_topic_thread(adapter, chat_id, anchor_text)
        if not new_tid:
            await adapter.send(
                chat_id,
                "⚠️ 创建话题失败（需要 im:message 权限）。请手动发起一个话题后重试 /cc:new。",
            )
            return
        thread_id = new_tid

    binding = _store.get(thread_id)
    if binding is None:
        # 首次绑定：有默认 workdir → 直接建；否则：
        # - /new 带了名字 → 不弹卡等交互，用最近项目第一个做目录直接建
        #   （命名意图明确 = 用户想立刻得到会话，布布 2026-09-13 实测反馈）
        # - 无名字 → 弹工作目录选择卡
        workdir = _default_workdir or ""
        if not workdir and session_name:
            try:
                projects = await asyncio.to_thread(core.list_recent_projects, None)
                if projects:
                    workdir = projects[0]["value"]
            except Exception:  # noqa: BLE001
                pass
            workdir = workdir or str(Path.home())
        if not workdir:
            workspaces = await asyncio.to_thread(
                core.list_recent_projects, None)
            card = cards.build_workdir_card(workspaces, thread_id=thread_id)
            # 记住本次 /new 的名字，workdir 卡选完后补上命名
            if session_name:
                _STATE.setdefault("pending_name", {})[thread_id] = session_name
            await _send_card(adapter, chat_id, card, thread_id=thread_id)
            return
        await _start_session(thread_id, chat_id, adapter, workdir,
                             cc_session_id="", mode=_default_mode,
                             session_name=session_name)
        return

    # 已有绑定：在话题里新增一个 CC 会话为 active（不再拒绝）
    binding.updated_at = time.time()
    # 保留 workdir，新建会话
    await _start_session(thread_id, chat_id, adapter, binding.workdir,
                         cc_session_id="", mode=binding.mode,
                         session_name=session_name)


async def _create_topic_thread(adapter, chat_id: str, anchor_text: str) -> Optional[str]:
    """在会话 chat_id 里发锚点消息并以 reply_in_thread=True 回复它开出话题线程。

    返回新话题的 thread_id。优先取回复响应里的 data.thread_id —— 飞书为话题
    分配独立的 omt_ 线程 ID，入站消息携带的就是它（实测 root_id/锚点 mid 与
    入站 thread_id 不是同一个值，绑定必须用 omt_ 才能命中）；拿不到再降级锚点 mid。
    """
    try:
        response = await adapter._feishu_send_with_retry(
            chat_id=chat_id,
            msg_type="text",
            payload=json.dumps({"text": anchor_text}, ensure_ascii=False),
            reply_to=None,
            metadata=None,
        )
        result = adapter._finalize_send_result(response, "cc-bridge anchor failed")
        if not result.success or not result.message_id:
            logger.warning("cc: anchor send failed: %s", getattr(result, "error", ""))
            return None
        anchor_mid = result.message_id
        welcome = ("**Claude Code 会话就绪**\n在这里直接对话，此话题的所有消息"
                   "都会转给 CC。\n/cd 换目录 · /mode 切权限 · /reset 新会话")
        thread_response = await adapter._feishu_send_with_retry(
            chat_id=chat_id,
            msg_type="post",
            payload=json.dumps({
                "zh_cn": {
                    "title": "",
                    "content": [[{"tag": "text", "text": welcome}]],
                }
            }, ensure_ascii=False),
            reply_to=anchor_mid,
            # adapter 仅在 metadata.thread_id 非空时才置 reply_in_thread=True 开新线程
            metadata={"thread_id": anchor_mid},
        )
        tresult = adapter._finalize_send_result(thread_response, "cc-bridge thread seed failed")
        if not tresult.success:
            logger.warning("cc: thread seed failed: %s", getattr(tresult, "error", ""))
            return anchor_mid
        # 响应 body 里带 thread_id (omt_...) —— 与后续入站消息 source.thread_id 同源
        resp_data = getattr(getattr(thread_response, "data", None), "__dict__", None) or {}
        thread_tid = (
            getattr(getattr(thread_response, "data", None), "thread_id", None)
            or resp_data.get("thread_id")
        )
        tid = str(thread_tid).strip() if thread_tid else ""
        if tid:
            logger.info("cc: created topic thread %s (anchor %s)", tid, anchor_mid)
            return tid
        logger.warning("cc: no thread_id in reply response; binding to anchor %s", anchor_mid)
        return anchor_mid
    except Exception as exc:  # noqa: BLE001
        logger.warning("cc: create topic thread failed: %s", exc, exc_info=True)
        return None


async def _cmd_cd(thread_id: str, adapter, arg: str) -> None:
    binding = _store.get(thread_id)
    if not binding or not binding.proc:
        await _basic_reply(adapter, binding, "当前话题没有活跃 Claude Code 会话。")
        return
    if not arg:
        await _basic_reply(adapter, binding, f"当前工作目录：`{binding.workdir}`")
        return
    import os
    newdir = os.path.abspath(os.path.expanduser(arg))
    if not os.path.isdir(newdir):
        await _basic_reply(adapter, binding, f"目录不存在：{arg}")
        return
    # 切目录必须先关旧进程再开新的：
    # (a) 并发两个 client 同 id resume 会互相写 transcript；
    # (b) session 隶属原 cwd 项目，换目录后不可跨项目 resume
    old = binding.proc
    if old:
        try:
            await old.close()
        except Exception:  # noqa: BLE001
            pass
    binding.workdir = newdir
    binding.active_session_id = ""  # 新目录=新 CC 项目上下文
    binding.visited_sessions = []   # 旧目录会话会话集不再适用
    binding.topic_title = ""
    binding.updated_at = time.time()
    # 重置补发标记，让新目录首轮回捕后重新补发 session id
    _STATE.setdefault("sid_announced", {}).pop(thread_id, None)
    await _start_process_for_binding(binding)
    _store.put(binding)
    await _basic_reply(adapter, binding, f"✅ 已切换工作目录到 `{newdir}`")


async def _cmd_reset(thread_id: str, chat_id: str, adapter) -> None:
    binding = _store.get(thread_id)
    if not binding:
        await _send_to_thread(adapter, chat_id,
                              "该话题没有 Claude Code 会话。使用 /cc:new 创建。",
                              thread_id)
        return
    # 关旧进程，开新 session（保留话题+workdir）
    old = binding.proc
    if old:
        try:
            await old.close()
        except Exception:  # noqa: BLE001
            pass
    # 释放旧 active 会话的全局占用（供其他话题 resume）——须在清空 active 之前
    if _registry is not None and binding.active_session_id:
        _registry.release(binding.active_session_id, thread_id)
    binding.active_session_id = ""  # 真实 id 由回捕写入；空值=不 resume
    binding.topic_title = ""  # 新会话清空旧主题
    binding.updated_at = time.time()
    # 重置补发标记，让首轮回捕后重新补发 session id
    _STATE.setdefault("sid_announced", {}).pop(thread_id, None)
    await _start_process_for_binding(binding)
    _store.put(binding)
    await _send_to_thread(adapter, chat_id,
                          f"🔄 已重置会话（新 session）。工作目录：`{binding.workdir}`",
                          thread_id)


def _lookup_session(binding: CCBinding, arg: str,
                    sessions: List[SessionMeta]) -> Optional[SessionMeta]:
    """解析 /resume 目标: -c(最近)/UUID/标题子串/数字(1=最近)。"""
    a = (arg or "").strip()
    if not a or a in ("-c", "--continue"):
        return sessions[0] if sessions else None      # 最近
    if a.isdigit():
        i = int(a) - 1
        return sessions[i] if 0 <= i < len(sessions) else None  # 序号
    # UUID 精确
    for s in sessions:
        if s.id == a:
            return s
    # 标题子串（大小写不敏感）
    al = a.lower()
    for s in sessions:
        if al in (s.title or "").lower():
            return s
    return None


async def _swap_to_session(thread_id: str, chat_id: str, adapter,
                           binding: CCBinding, target: SessionMeta) -> None:
    """把 binding 的 active 会话切换到 target（含并发占用检查）。

    不可夺占：若 target 已被其他话题占用，只给跳转链接，不抢。
    否则关旧 proc、换 active、重新拉起（resume target.id）。
    """
    from . import cards
    if target is None:
        await _send_to_thread(adapter, chat_id,
                              "目标会话不存在。可用 /resume 查看列表。", thread_id)
        return
    # 并发锁检查：被其他话题占用 → 只给跳转
    if _registry is not None and _registry.is_occupied_by(target.id, thread_id):
        owner_tid = _registry.owner(target.id)
        # 构造跳转链接：占用话题最后一条消息
        link = _build_applink(adapter, owner_tid or "", chat_id)
        await _send_to_thread(
            adapter, chat_id,
            f"⚠️ 该会话正被 **另一个话题** 占用（不可同时使用）。\n"
            f"[点击跳转到占用它的话题]({link or '#'})，在那里 /cc:stop 释放后再回来 /resume。\n"
            f"会话：{target.title or target.id[:8]}",
            thread_id)
        return
    # 释放旧 active 的占用（若本话题持有）
    old_sid = binding.active_session_id
    if _registry is not None and old_sid and old_sid != target.id:
        _registry.release(old_sid, thread_id)
    # 关旧 proc
    old = binding.proc
    if old:
        try:
            await old.close()
        except Exception:  # noqa: BLE001
            pass
    # 切 active + 占用 + 重启
    binding.active_session_id = target.id
    binding.workdir = target.workdir or binding.workdir
    if target.id not in binding.visited_sessions:
        binding.visited_sessions.insert(0, target.id)
    binding.updated_at = time.time()
    _acquire_session(binding, target.id)
    await _start_process_for_binding(binding)
    _store.put(binding)
    await _send_to_thread(
        adapter, chat_id,
        f"✅ 已切换到 CC 会话：**{target.title or target.id[:8]}**\n"
        f"(session `{target.id[:8]}…`) 上下文已接上，直接继续对话。",
        thread_id)


def _build_applink(adapter, owner_thread: str, chat_id: str) -> str:
    """为目标话题最后一条回复生成跳转链接。

    优先用 registry 存的 owner_msg_id 构造飞书 applink；拿不到 message_id 时
    尝试取锚点消息。格式: https://applink.feishu.cn/client/chat/open?chatId=..&msgId=..
    """
    try:
        owner_msg = None
        # 用会话列表里 owner_thread 对应的最后 msg id
        for m in (_registry.sessions.values() if _registry else []):
            if m.owner_thread == owner_thread and m.owner_msg_id:
                owner_msg = m.owner_msg_id
                break
        # adapter 的 applink 基址
        base = getattr(adapter, "applink_base", None) or \
            "https://applink.feishu.cn/client/chat/open"
        from urllib.parse import urlencode
        if owner_msg:
            q = urlencode({"chatId": chat_id, "msgId": owner_msg})
            return f"{base}?{q}"
        # 无 msgId: 退化为锚点消息
        if owner_thread:
            q = urlencode({"chatId": chat_id, "topicId": owner_thread})
            return f"{base}?{q}"
    except Exception:  # noqa: BLE001
        pass
    return ""


async def _cmd_resume(thread_id: str, chat_id: str, adapter, arg: str) -> None:
    """/resume：恢复/切换 CC 会话（对齐 claude --resume）。

    无参 → 列该 workdir 所有历史会话（卡片）；-c → 最近；
    <id/UUID|标题子串|数字> → 按指定恢复。被占用只给跳转不抢。
    """
    binding = _store.get(thread_id)
    if binding is None:
        await _send_to_thread(adapter, chat_id,
                              "该话题还没开会话。先用 /new 在某个目录建一个。",
                              thread_id)
        return
    workdir = binding.workdir or _default_workdir or ""
    sessions = await asyncio.to_thread(core.list_project_sessions, workdir)
    # 补上 registry 里命名过但 jsonl 尚无真实内容的会话（/new 名字 刚建、
    # 还没对话落盘）：从磁盘列表里排除空壳后，这类只剩 registry 条目，
    # 不补就会被过滤掉看不见。
    if _registry is not None:
        listed = {s.id for s in sessions}
        for m in _registry.sessions.values():
            if (m.workdir == workdir and m.title
                    and m.id not in listed):
                sessions.insert(0, SessionMeta(
                    id=m.id, title=m.title, workdir=workdir,
                    mode=m.mode, owner_thread=m.owner_thread,
                    owner_msg_id=m.owner_msg_id,
                    created_at=m.created_at, updated_at=m.updated_at))
        sessions.sort(key=lambda s: -s.updated_at)
    if not sessions:
        await _send_to_thread(
            adapter, chat_id,
            f"`{workdir}` 下还没有历史 CC 会话。用 /new 新建一个。",
            thread_id)
        return
    # 无参 → 列卡片
    if not (arg or "").strip():
        from . import cards
        active = binding.active_session_id
        occupied_map: Dict[str, str] = {}
        if _registry is not None:
            for s in sessions:
                o = _registry.owner(s.id)
                if o and o != thread_id:
                    occupied_map[s.id] = _build_applink(adapter, o, chat_id)
        card = cards.build_session_picker_card(
            sessions, active=active, occupied_map=occupied_map,
            thread_id=thread_id, chat_id=chat_id)
        await _send_card(adapter, chat_id, card, thread_id=thread_id)
        return
    # 有目标 → 解析并切换
    target = _lookup_session(binding, arg, sessions)
    await _swap_to_session(thread_id, chat_id, adapter, binding, target)


async def _cmd_mode(thread_id: str, chat_id: str, adapter, arg: str) -> None:
    binding = _store.get(thread_id)
    if not binding:
        await _send_to_thread(adapter, chat_id,
                              "当前话题没有 Claude Code 会话。", thread_id)
        return
    mode_map = {"default": "default", "accept": "acceptEdits", "acceptEdits": "acceptEdits",
                "plan": "plan", "bypass": "bypassPermissions", "bypasspermissions": "bypassPermissions"}
    mode = mode_map.get(arg.lower(), "")
    if not mode:
        card = cards.build_mode_picker_card(binding.mode, thread_id=binding.thread_id)
        await _send_card(adapter, binding.chat_id, card, thread_id=binding.thread_id)
        return
    await _switch_mode(binding, mode, adapter, chat_id, thread_id)


async def _cmd_rewind(thread_id: str, chat_id: str, adapter, arg: str) -> None:
    """/cc:rewind [N|list] — 恢复文件到第 N 条用户消息之前(无参=列出检查点)。"""
    binding = _store.get(thread_id)
    if not binding or not await ensure_cc_session(binding):
        if binding and binding.active_session_id:
            await _basic_reply(adapter, binding,
                               "⚠️ Claude Code 会话恢复失败，试试 /resume 或 /cc:new。")
        else:
            await _basic_reply(adapter, CCBinding(thread_id=thread_id, chat_id=chat_id,
                                                  workdir="", mode="default"),
                               "当前没有 Claude Code 会话。先用 /new <目录> 开一个。")
        return
    cps = getattr(binding.proc, "checkpoints", [])
    if not arg or arg.lower() in ("list", "?"):
        if not cps:
            await _basic_reply(adapter, binding,
                               "暂无可用检查点（本进程内发过对话才有；重启后清零）。")
            return
        lines = ["**可回退的检查点**（数字=第几条用户消息）"]
        for i, cp in enumerate(cps, 1):
            lines.append(f"- **{i}** · {cp['text'][:40]}")
        await _send_to_thread(adapter, chat_id, "\n".join(lines), thread_id)
        return
    try:
        n = int(arg)
    except ValueError:
        await _basic_reply(adapter, binding, "用法：/rewind [序号]，先 /rewind 查看列表。")
        return
    if not (1 <= n <= len(cps)):
        await _basic_reply(
            adapter, binding,
            f"序号超出范围（1-{len(cps)}）。先发 /rewind 查看可用检查点。")
        return
    target = cps[n - 1]
    ok = await binding.proc.rewind_files(target["uuid"])
    if ok:
        # 回滚后该消息之后的检查点作废(会话仍继续, 但文件状态已回到那一步)。
        # checkpoints property 过滤了未回填 uuid 的条目，cps 序号与
        # _checkpoints 原始索引不一致，须按 uuid 定位再切。
        raw = binding.proc._checkpoints
        target_idx = -1
        for i, cp in enumerate(raw):
            if cp["uuid"] == target["uuid"]:
                target_idx = i
                break
        if target_idx >= 0:
            del raw[target_idx + 1:]
        await _basic_reply(adapter, binding,
                           f"⏪ 已把文件恢复到第 {n} 条消息「{target['text'][:30]}」之前的状态。"
                           "对话上下文未变，如需连同对话一起重置请用 /reset。")
    else:
        await _basic_reply(adapter, binding,
                           "⚠️ 回滚失败（查看 gateway 日志定位）。")


async def _cmd_stop(thread_id: str, chat_id: str, adapter) -> None:
    binding = _store.get(thread_id)
    if not binding:
        await _send_to_thread(adapter, chat_id,
                              "当前没有运行中的 Claude Code 会话。", thread_id)
        return
    if not await ensure_cc_session(binding):
        if not binding.active_session_id:
            await _send_to_thread(adapter, chat_id,
                                  "当前没有运行中的 Claude Code 会话。", thread_id)
            return
        await _basic_reply(
            adapter, binding,
            "⚠️ 会话恢复失败，无法发送中断。试试 /cc:new。")
        return
    proc = binding.proc
    if proc is not None and proc.running:
        await proc.interrupt()
        # 释放该会话的全局占用（供其他话题 /resume）
        if _registry is not None and binding.active_session_id:
            _registry.release(binding.active_session_id, thread_id)
        try:
            res = await _basic_reply(
                adapter, binding,
                "⏹ 已发送中断，该 CC 会话已释放（其他话题可 /resume）。")
            ok = bool(getattr(res, "success", True)) if not callable(
                getattr(res, "success", None)) else callable(res.success) and res.success()
            if not ok:
                logger.warning("cc[%s] /stop 确认消息发送失败", thread_id)
        except Exception:  # noqa: BLE001
            logger.warning("cc[%s] /stop 确认消息异常", thread_id, exc_info=True)
    else:
        # 进程不在运行：释放占用锁，避免其他话题 /resume 被卡
        if _registry is not None and binding.active_session_id:
            _registry.release(binding.active_session_id, thread_id)
        await _basic_reply(
            adapter, binding,
            "⚠️ 会话进程未在运行，已释放占用。用 /status 查看，或 /reset 重开。")


def _project_display_name(workdir: str) -> str:
    """项目显示名：取路径末段（/home/vm/code/x → x）；根目录用归一化形式。"""
    wd = (workdir or "").rstrip("/")
    if not wd or wd == "/":
        return "(root)"
    seg = wd.rsplit("/", 1)[-1]
    return f"~/{wd.split('/', 3)[3] if wd.count('/') >= 3 else seg}" \
        if wd.startswith(str(Path.home())) and wd.count("/") > 3 else seg


async def _cmd_status(thread_id: str, chat_id: str, adapter) -> None:
    binding = _store.get(thread_id)
    if not binding:
        # 私聊(DM 无绑定)：全局项目+会话总览卡（布布 2026-09-14 拍板）
        # 最近 5 project × 各 5 会话；已关联话题 → applink 跳转；
        # 未关联的会话 → 「▶ 打开」按钮，点击自动建话题并接入该 CC 会话。
        projects = await asyncio.to_thread(
            core.list_all_project_sessions, 5, 5)
        sid2thread: Dict[str, Tuple[str, str]] = {}
        for b in (_store.all() if _store else []):
            if b.active_session_id and b.active_session_id not in sid2thread:
                sid2thread[b.active_session_id] = (b.thread_id, b.chat_id)
            for vs in b.visited_sessions:
                if vs and vs not in sid2thread:
                    sid2thread[vs] = (b.thread_id, b.chat_id)
        blocks: List[Dict[str, Any]] = []
        for proj in projects:
            pname = _project_display_name(proj["workdir"])
            plines = []
            open_buttons: List[Dict[str, str]] = []
            n_open = 0
            for s in proj["sessions"][:5]:
                meta_t = _registry.get(s.id) if _registry is not None else None
                title = ((meta_t.title if meta_t and meta_t.title else "")
                         or s.title)
                rel = sid2thread.get(s.id)
                short = title if len(title) <= 30 else title[:28] + "…"
                if rel:
                    link = _build_applink(adapter, rel[0], rel[1])
                    plines.append(f"- 💬 [{short}]({link})")
                else:
                    n_open += 1
                    plines.append(f"({n_open}) {short}")
                    open_buttons.append({"title": short, "sid": s.id,
                                         "workdir": s.workdir})
            import datetime as _dt
            mt_s = _dt.datetime.fromtimestamp(proj["mtime"]).strftime("%m-%d %H:%M")
            blocks.append({
                "md": f"{pname} · {mt_s}\n" + "\n".join(plines),
                "open_buttons": open_buttons,
                "chat_id": chat_id,
            })
        card = cards.build_status_block_card(blocks)
        try:
            sr = await adapter._feishu_send_with_retry(
                chat_id=chat_id,
                msg_type="interactive",
                payload=json.dumps(card, ensure_ascii=False),
                reply_to=None,
                metadata=None,
            )
            _succ = getattr(sr, "success", True)
            ok = bool(_succ() if callable(_succ) else _succ)
            if not ok:
                logger.warning("cc: status card rejected: code=%s msg=%s",
                               getattr(sr, "code", "?"), getattr(sr, "error", ""))
            else:
                logger.info("cc: status card sent to %s", chat_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("cc: status card send failed: %s", exc)
            await adapter.send(chat_id, "**Claude Code 会话**\n\n"
                               + "\n\n".join(b["md"] for b in blocks))
        return
    running = binding.proc is not None and binding.proc.running
    title_line = f"\n- 主题：{binding.topic_title}" if binding.topic_title else ""
    active_sid = binding.active_session_id
    active_txt = f"`{active_sid[:8]}…`" if active_sid else "（无）"
    # 本话题用过的会话列表
    visited_lines = []
    for i, sid in enumerate(binding.visited_sessions, 1):
        mark = " ← 当前" if sid == active_sid else ""
        tag = ""
        if _registry is not None:
            m = _registry.get(sid)
            tag = (" · " + m.title) if (m and m.title) else ""
        visited_lines.append(f"{i}. `{sid[:8]}…`{tag}{mark}")
    hist = "\n".join(visited_lines) or "（无）"
    await _send_to_thread(
        adapter, chat_id,
        f"**Claude Code 会话**\n"
        f"- 工作目录：`{binding.workdir}`\n"
        f"- 权限模式：`{binding.mode}`\n"
        f"- 当前 CC 会话：{active_txt}\n"
        f"- 进程状态：{'✅ 运行中' if running else '❌ 未运行'}{title_line}\n"
        f"- 本话题用过的会话：\n{hist}\n\n"
        f"用 /resume 切换到其他 CC 会话，/new 新建，/stop 释放当前。",
        thread_id)


# --------------------------------------------------------------------------- #
# 会话启动 & CC 输出路由
# --------------------------------------------------------------------------- #

async def _start_session(thread_id: str, chat_id: str, adapter, workdir: str,
                         cc_session_id: Optional[str] = None, mode: Optional[str] = None,
                         session_name: Optional[str] = None) -> None:
    import os
    import time as _t
    wd = os.path.abspath(os.path.expanduser(workdir))
    if not os.path.isdir(wd):
        await _send_to_thread(adapter, chat_id,
                              f"目录不存在：{workdir}，请选择有效目录。",
                              thread_id)
        return
    binding = _store.get(thread_id)
    if binding is None:
        binding = CCBinding(
            thread_id=thread_id,
            chat_id=chat_id,
            workdir=wd,
            # 不预生成占位 id：空值→启动时不 resume，等 SystemMessage 回捕真实 id
            active_session_id=cc_session_id or "",
            mode=mode or _default_mode,
        )
    else:
        binding.chat_id = chat_id
        binding.workdir = wd
        binding.mode = mode or binding.mode
        # /cc:new = 新建会话：清掉旧 active_session_id，避免
        # _start_process_for_binding 拿旧 id 走 resume 恢复上下文
        if not cc_session_id:
            binding.active_session_id = ""
            # 重置 session id 补发标记，让首次对话后重新补发
            _STATE.setdefault("sid_announced", {}).pop(thread_id, None)
    # 新会话 naming（需要真正新建时才用）。框架启动后会回捕真实 id 到 active。
    if session_name:
        _STATE.setdefault("pending_name", {})[thread_id] = session_name
    binding.created_at = binding.created_at or _t.time()
    binding.updated_at = _t.time()
    await _start_process_for_binding(binding)
    _store.put(binding)
    # session id 在首次对话 ResultMessage 里才回捕（connect 不发消息），
    # /new 时如实告知"会话已就绪"，首次对话后自动补发 session id
    name_txt = f"，名称：`{session_name}`" if session_name else ""
    await _send_to_thread(
        adapter, chat_id,
        f"🚀 已新建 Claude Code 会话（工作目录：`{wd}`，模式：`{binding.mode}`{name_txt}）。"
        "直接在这个话题下发消息即可与其交互。",
        thread_id)


async def _start_process_for_binding(binding: CCBinding) -> None:
    """为绑定启动（或重启）CC 会话，接好 on_event/on_exit。

    用 active_session_id 决定 resume 哪个 CC 会话；并在全局 registry 里
    用当前话题占用它（并发锁）。会话空闲时 resume=None 开新会话。
    """
    sid = binding.active_session_id
    # 占用锁：本话题接管该 CC 会话（即使进程级重启，占用关系不变）
    _acquire_session(binding, sid)
    proc = CCProcess(
        binding.workdir,
        sid or None,          # 空=新会话，不 resume
        binding.mode,
        _claude_executable,
        on_event=lambda msg: _on_cc_event(binding, msg),
        on_exit=lambda err: _on_cc_exit(binding, err),
    )
    binding.proc = proc
    await proc.start()


def _acquire_session(binding: CCBinding, session_id: str) -> None:
    """把 CC 会话标记为被本话题占用（写全局 registry）。无会话 id 则跳过。"""
    if not session_id or _registry is None:
        return
    meta = _registry.get(session_id)
    if meta is None:
        meta = SessionMeta(id=session_id, workdir=binding.workdir,
                           title=binding.topic_title or "")
    meta.owner_thread = binding.thread_id
    meta.workdir = binding.workdir
    meta.updated_at = time.time()
    _registry.put(meta)


def _record_new_session(binding: CCBinding, session_id: str) -> None:
    """CC 会话真实 id 回捕时，登记进全局 registry（含命名/标题）。"""
    if not session_id or _registry is None:
        return
    meta = _registry.get(session_id)
    if meta is None:
        meta = SessionMeta(id=session_id, workdir=binding.workdir,
                           title=binding.topic_title or "", mode=binding.mode)
    meta.owner_thread = binding.thread_id
    meta.workdir = binding.workdir
    meta.mode = binding.mode
    meta.updated_at = time.time()
    # 命名来自 -n（/new <名字>），由 pending name 提供；无则用回捕时话题标题
    pname = _STATE.get("pending_name", {}).pop(binding.thread_id, "")
    if pname:
        meta.title = pname
    elif not meta.title:
        meta.title = binding.topic_title or ""
    _registry.put(meta)


async def _on_cc_event(binding: CCBinding, msg: Any) -> None:
    """处理 SDK 消息对象，路由到卡片/审批/流式。

    SDK 0.2.141 的 Message 联合类型：
      StreamEvent / AssistantMessage / ResultMessage / SystemMessage / UserMessage / ...
    """
    try:
        mtype = type(msg).__name__
    except Exception:  # noqa: BLE001
        mtype = ""

    # ---- 内部 dict 事件: permission_request（can_use_tool 回调触发）----
    if isinstance(msg, dict):
        etype = msg.get("type") or ""
        if etype == "permission_request":
            await _handle_permission(binding, msg)
        return

    # ---- 流式增量: StreamEvent.event 里 content_block_delta ----
    if mtype == "StreamEvent":
        event = getattr(msg, "event", None) or {}
        ev_type = event.get("type") if isinstance(event, dict) else None
        if ev_type == "content_block_delta":
            delta = event.get("delta") or {}
            if delta.get("type") == "text_delta" and delta.get("text"):
                await _push_text(binding, delta["text"], allow_stream=True)
        elif ev_type == "content_block_start":
            block = event.get("content_block") or {}
            if block.get("type") == "tool_use":
                await _handle_tool_use(binding, {
                    "id": block.get("id", ""),
                    "name": block.get("name", ""),
                    "input": block.get("input", {}),
                })
        return

    # ---- 完整 assistant 消息: tool_use 参数补全 → 回填活动卡；文本忽略避免重复 ----
    if mtype == "AssistantMessage":
        try:
            from claude_agent_sdk.types import ToolUseBlock  # noqa: PLC0415
        except Exception:  # noqa: BLE001
            return
        content = list(getattr(msg, "content", []) or [])
        for block in content:
            if isinstance(block, ToolUseBlock):
                await _update_tool_card(binding, {
                    "id": getattr(block, "id", ""),
                    "name": getattr(block, "name", ""),
                    "input": getattr(block, "input", {}) or {},
                })
        return

    # ---- user 回传消息: tool_result → 完成活动卡; 普通 user 消息 uuid → checkpoint ----
    if mtype == "UserMessage":
        proc = binding.proc
        u_uuid = str(getattr(msg, "uuid", "") or "")
        # 只回填普通文本型(不含 tool_result)的用户消息 —— 与 send_text 一一对应
        content_raw = getattr(msg, "content", None)
        has_tool_result = False
        try:
            from claude_agent_sdk.types import ToolResultBlock  # noqa: PLC0415
            if isinstance(content_raw, list):
                for blk in content_raw:
                    if isinstance(blk, ToolResultBlock):
                        has_tool_result = True
                        tuid = str(getattr(blk, "tool_use_id", "") or "")
                        is_err = bool(getattr(blk, "is_error", False))
                        raw = getattr(blk, "content", None)
                        await _complete_tool_card(binding, tuid, is_err, raw)
        except ImportError:
            pass
        if u_uuid and not has_tool_result and proc is not None:
            record = getattr(proc, "record_user_uuid", None)
            if record:
                txt = ""
                if isinstance(content_raw, str):
                    txt = content_raw
                elif isinstance(content_raw, list):
                    txt = " ".join(str(getattr(b, "text", "")) for b in content_raw)
                await record(u_uuid, txt)
        return

    # ---- result: 一轮结束，落地完整文本 + 用量附言 + 主题记录 ----
    if mtype == "ResultMessage":
        # 真实 CC session id 回捕（resume 关键；SystemMessage 无此字段，
        # ResultMessage/AssistantMessage 才带）
        sid = getattr(msg, "session_id", None) or ""
        sid_just_captured = False
        if sid and sid != binding.active_session_id:
            # 回捕: 记录到全局 registry + visited，供 /resume 列表与并发锁
            _record_new_session(binding, sid)
            binding.active_session_id = sid
            if sid not in binding.visited_sessions:
                binding.visited_sessions.insert(0, sid)
            if _store:
                _store.put(binding)
            sid_just_captured = True
        result_text = getattr(msg, "result", None) or ""
        usage_tail = _usage_tail(msg)
        err_mark = "\n\n> ⚠️ 本轮返回出错" if bool(getattr(msg, "is_error", False)) else ""
        final_text = (str(result_text) + usage_tail + err_mark) if result_text else ""
        if final_text:
            await _push_text(binding, final_text, allow_stream=False, final=True)
        else:
            await _finish_stream(binding)
        # CC 生命周期表情：一轮完成 → 撤 OnIt 打 Done/CrossMark
        _fire_and_forget(_cc_mark_done(
            binding.thread_id, ok=not bool(getattr(msg, "is_error", False))))
        # 会话主题：首轮成功后记录（供 /cc:status 显示）
        if result_text and not binding.topic_title:
            title = _derive_topic_title(str(result_text))
            if title:
                binding.topic_title = title
                if _store:
                    _store.put(binding)
        # 首轮回捕 session id 时补发提示（/new 时还拿不到）
        if sid_just_captured and not _STATE.get("sid_announced", {}).get(binding.thread_id):
            _STATE.setdefault("sid_announced", {})[binding.thread_id] = True
            adapter = await _current_adapter()
            if adapter:
                await _send_to_thread(
                    adapter, binding.chat_id,
                    f"📋 CC session id：`{sid[:12]}…`（`/cc:status` 可随时查看）",
                    binding.thread_id)
        return

    # ---- system: 记录 session_id（SystemMessage.data 里可能带）----
    if mtype == "SystemMessage":
        data = getattr(msg, "data", None) or {}
        sid = (data.get("session_id") if isinstance(data, dict) else "") or ""
        if sid and sid != binding.active_session_id:
            _record_new_session(binding, sid)
            binding.active_session_id = sid
            if sid not in binding.visited_sessions:
                binding.visited_sessions.insert(0, sid)
            if _store:
                _store.put(binding)
        return

    # ---- 其余消息忽略 ----
    return


def _usage_tail(result_msg: Any) -> str:
    """从 ResultMessage 提取用量，格式化为流式卡尾部附言（无数据返回空）。

    版式（布布 2026-09-14）：数字用千分位、tokens≥1k 显示 k、
    模型与费用并一行；↑↓ 符号区分输入输出方向。
      > 📊 ↑263 ↓60 tok · cache 1.2k
      > 💠 Claude Code · glm-5.2 · $0.0619
    """
    usage = getattr(result_msg, "usage", None)
    cost = getattr(result_msg, "total_cost_usd", None)

    # CC SDK usage dict 用 camelCase（inputTokens/outputTokens/cacheReadInputTokens），
    # 但部分 provider 可能用 snake_case —— 两种都查
    def _g(src: Any, *names: str) -> int:
        v = None
        if isinstance(src, dict):
            for n in names:
                v = src.get(n)
                if v is not None:
                    break
        elif src is not None:
            for n in names:
                v = getattr(src, n, None)
                if v is not None:
                    break
        return int(v) if isinstance(v, (int, float)) else 0

    def _n(v: int) -> str:
        return f"{v:,}" if v < 10000 else (f"{v/1000:.1f}k" if v < 1000000 else f"{v/1000000:.2f}M")

    inp = _g(usage, "input_tokens", "inputTokens")
    out = _g(usage, "output_tokens", "outputTokens")
    cache_r = _g(usage, "cache_read_input_tokens", "cacheReadInputTokens",
                  "cache_read_tokens", "cacheReadTokens")
    parts = []
    if inp or out:
        pieces = [f"↑{_n(inp)}", f"↓{_n(out)}"]
        parts.append(" ".join(pieces) + " tok")
    if cache_r:
        parts.append(f"cache {_n(cache_r)}")
    body = ("📊 " + " · ".join(parts)) if parts else ""

    sig_parts = []
    mu = getattr(result_msg, "model_usage", None)
    model_name = ""
    if isinstance(mu, dict) and mu:
        try:
            best = max(
                mu.items(),
                key=lambda kv: int((kv[1] or {}).get("outputTokens", 0) or 0),
            )
            model_name = str(best[0])
        except Exception:  # noqa: BLE001
            model_name = next(iter(mu), "")
    if model_name:
        sig_parts.append(model_name)
    if isinstance(cost, (int, float)) and cost > 0:
        sig_parts.append(f"${cost:.4f}")
    sig = "💠 " + " · ".join(["Claude Code"] + sig_parts)
    if not body and len(sig_parts) == 0:
        return ""          # 无任何可用信息 → 不加尾巴（旧行为）
    tail = "\n\n> " + (f"{body}\n> {sig}" if body else sig.lstrip("💠 "))
    return tail

def _derive_topic_title(text: str, max_len: int = 24) -> str:
    """从结果文本提炼会话主题（首个非空行，截断）。"""
    for line in text.splitlines():
        s = line.strip().lstrip("#*->• ").strip()
        if len(s) >= 6:
            return (s[:max_len] + "…") if len(s) > max_len else s
    return ""


def _text_of(obj: Dict[str, Any]) -> str:
    for k in ("text", "content", "result", "message"):
        v = obj.get(k)
        if isinstance(v, str):
            return v
    return ""


_MD_HINT_RE = re.compile(r"\*\*|``|`[^`\n]+`|\[.+\]\(.+\)|^#{1,6}\s", re.MULTILINE)


def _build_markdown_post_payload(text: str) -> str:
    """post 型 payload —— 与 Hermes 私聊 send 的 md 通道完全同源。

    实测(2026-09-13 布布确认): post 富文本的 ``md`` 元素在话题里渲染
    完整标准 markdown(行内代码/围栏码块/列表), 明显强于 interactive 卡
    的 lark_md 子集。两类内容必须拆成独立 md 元素:
    - 围栏代码块: 同元素混排会吞尾随内容(adapter #52786 已知)
    - 表格: 同元素内与粗体/列表混杂时表格解析失效(MIXED 实证), 必须独占元素
      且前置空行(SEPARATED 实证正常)
    """
    rows: List[List[Dict[str, str]]] = []
    cur: List[str] = []

    def _flush() -> None:
        t = "\n".join(cur).strip("\n")
        cur.clear()
        if t.strip():
            rows.append([{"tag": "md", "text": t}])

    def _flush_tbl() -> None:
        # 表格独占元素 + 前置空行(SEPARATED 实证: 混排失效, 独立+空行正常)
        if tbl:
            t = "\n".join(tbl)
            tbl.clear()
            rows.append([{"tag": "md", "text": "\n" + t}])

    block: List[str] = []  # 当前围栏代码块(含首尾 fence 行)
    tbl: List[str] = []    # 当前 GFM 表格连续段(| 开头的行)
    for line in text.split("\n"):
        s = line.strip()
        if s.startswith("```") and not block:
            _flush_tbl()
            _flush()
            block = [line]
        elif block:
            block.append(line)
            if len(block) > 1 and s.startswith("```"):
                rows.append([{"tag": "md", "text": "\n".join(block)}])
                block = []
        elif s.startswith("|") and "|" in s[1:]:
            _flush()
            tbl.append(line)
        else:
            _flush_tbl()
            cur.append(line)
    _flush_tbl()
    if block:  # 未闭合的围栏, 原样保留
        rows.append([{"tag": "md", "text": "\n".join(block)}])
    _flush()
    return json.dumps({"zh_cn": {"content": rows}}, ensure_ascii=False)


_SOURCE_TAGS = {
    "bridge": "🤖 **cc-bridge**",
    "cc": "💠 **Claude Code**",
}


async def _send_to_thread(adapter, chat_id: str, text: str, thread_id: str,
                          *, source: str = "bridge") -> Any:
    """把文本/卡片消息送进话题线程。

    ``source`` 决定消息头部来源标识：
    - ``"bridge"``(默认) → 🤖 cc-bridge（桥接管理消息）
    - ``"cc"``           → 💠 Claude Code（CC 回复）
    - ``""``             → 不加标识

    飞书的 ``message.create(receive_id_type=thread_id)`` 实测返回 99992402
    field validation failed（即便 receive_id 是真实的 omt_ 线程 ID）——
    所以带 metadata.thread_id 的 create 路径会静默失败。可靠路径是对话题内
    已知消息 reply(reply_in_thread=True)：回复锚点即落在话题里。
    返回原始 response（成功时 data.message_id 可用），全失败返回 None。

    渲染规则(2026-09-13 实测定死): 有锚点优先走 **post 富文本 md 元素**
    ——与私聊 render 完全同源, 标准全量 markdown; 仅当无锚点或 post 被 API
    拒绝时降级 interactive 卡(lark_md 樋缺子集), 最后才是纯 text。
    """
    anchor = await _thread_anchor(adapter, chat_id, thread_id)
    _tag = _SOURCE_TAGS.get(source, "")
    if _tag:
        text = f"{_tag}\n\n{text}"

    def _note(resp) -> Any:
        # 记录该话题最后一条成功回复的 message_id（占用跳转锚点用）
        try:
            mid = getattr(getattr(resp, "data", None), "message_id", "") if resp else ""
            if mid and _registry is not None:
                for m in _registry.sessions.values():
                    if m.owner_thread == thread_id:
                        m.owner_msg_id = mid
                _registry.save()
        except Exception:  # noqa: BLE001
            pass
        return resp

    if anchor and _MD_HINT_RE.search(text):
        try:
            return _note(await adapter._feishu_send_with_retry(
                chat_id=chat_id,
                msg_type="post",
                payload=_build_markdown_post_payload(text[:9000]),
                reply_to=anchor,
                metadata={"thread_id": thread_id},
            ))
        except Exception as exc:  # noqa: BLE001
            logger.warning("cc: thread post-md send failed: %s", exc)
            # 落到下方 interactive 卡兜底重试
    if anchor and _MD_HINT_RE.search(text):
        card = {
            "config": {"wide_screen_mode": False},
            "elements": [{"tag": "markdown", "content": text[:9000]}],
        }
        try:
            return _note(await adapter._feishu_send_with_retry(
                chat_id=chat_id,
                msg_type="interactive",
                payload=json.dumps(card, ensure_ascii=False),
                reply_to=anchor,
                metadata={"thread_id": thread_id},
            ))
        except Exception as exc:  # noqa: BLE001
            logger.warning("cc: thread md-card send failed: %s", exc)
            # 落到下方普通文本兜底重试
    if anchor:
        try:
            return _note(await adapter._feishu_send_with_retry(
                chat_id=chat_id,
                msg_type="text",
                payload=json.dumps({"text": text}, ensure_ascii=False),
                reply_to=anchor,
                metadata={"thread_id": thread_id},
            ))
        except Exception as exc:  # noqa: BLE001
            logger.warning("cc: thread reply send failed: %s", exc)
            return None
    # 无锚点可用 → 直接 create 落主聊天（至少不丢内容）
    try:
        return await adapter._feishu_send_with_retry(
            chat_id=chat_id,
            msg_type="text",
            payload=json.dumps({"text": text}, ensure_ascii=False),
            reply_to=None,
            metadata=None,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("cc: chat fallback send failed: %s", exc)
    return None


_ANCHOR_CACHE: Dict[str, str] = {}


async def _thread_anchor(adapter, chat_id: str, thread_id: str) -> Optional[str]:
    """查/缓存话题锚点消息 id：优先绑定表里的 root，否则列表 API 拉一条话题成员。

    lark message.list 以 thread_id 过滤 container/?type=thread；拿首条 mid 做
    reply 目标（reply 到话题内任何一条消息都会落回本线程）。结果按 (chat,thread) 缓存。
    """
    key = f"{chat_id}:{thread_id}"
    hit = _ANCHOR_CACHE.get(key)
    if hit:
        return hit
    client = getattr(adapter, "_client", None)
    if client is None or not thread_id.startswith("omt_"):
        return None

    async def _resolve() -> Optional[str]:
        from lark_oapi.api.im.v1 import ListMessageRequest  # noqa: PLC0415
        req = (ListMessageRequest.builder()
               .container_id_type("thread").container_id(thread_id)
               .sort_type("ByCreateTimeAsc").page_size(5).build())
        resp = await asyncio.to_thread(client.im.v1.message.list, req)
        items = getattr(getattr(resp, "data", None), "items", None) or []
        for it in items:
            mid = getattr(it, "message_id", "")
            if mid and mid != thread_id:
                return mid
        return None

    try:
        mid = await _resolve()
    except Exception as exc:  # noqa: BLE001
        logger.warning("cc: list thread messages failed: %s", exc)
        mid = None
    if mid:
        _ANCHOR_CACHE[key] = mid
    return mid


async def _push_text(binding: CCBinding, text: str, *, allow_stream: bool, final: bool = False) -> None:
    """把 CC 文本推给该话题。优先流式卡，退化整段文本。"""
    adapter, stream = await _stream_context(binding)
    if adapter is None:
        return
    if final:
        # 无活跃流式卡（或建卡失败）→ 直接发完整文本；否则落地到卡上
        if stream is None or not stream.card_active or not stream.message_id:
            _STATE.get("streams", {}).pop(binding.thread_id, None)
            if text.strip():
                resp = await _send_to_thread(adapter, binding.chat_id, text,
                                             binding.thread_id, source="cc")
                ok = bool(getattr(getattr(resp, "data", None), "message_id", ""))
                if resp is not None and not ok:
                    logger.warning("cc: thread send rejected (code=%s msg=%s)",
                                   getattr(resp, "code", "?"), getattr(resp, "msg", ""))
            return
        await _finish_stream(binding, final_text=text)
        return
    if allow_stream and _stream_cards:
        if stream is not None and not stream.card_active:
            # 首段真实文本：启动流式卡
            await stream.start(text)
            if stream.card_active:
                return  # 首段已在 start 里渲染，后续走 append
        if stream is not None and stream.card_active:
            await stream.append(text)
            return
    # 流式不可用/关闭 → 静默丢弃增量；ResultMessage 自带完整文本，
    # 由 final=True 分支整段落地（避免每 delta 一条消息刷屏）
    return


async def _finish_stream(binding: CCBinding, final_text: Optional[str] = None) -> None:
    stream = _STATE.get("streams", {}).get(binding.thread_id)
    if stream is not None:
        if final_text:
            await stream.finish(final_text)
        else:
            # 无最终文本：落地已缓冲内容。注意先拼后 flush——_flush 会清空 buffer
            buffered = "".join(stream.buffer)
            if buffered.strip():
                await stream._flush()
                await stream.finish(buffered)
            else:
                # 清理空卡
                await stream.finish("")
    _STATE.get("streams", {}).pop(binding.thread_id, None)


async def _on_cc_exit(binding: CCBinding, returncode: Optional[int]) -> None:
    logger.info("cc[%s] exited rc=%s", binding.thread_id, returncode)
    # 异常崩溃路径不会经过 proc.close()，这里兜底拒绝所有挂起审批，
    # 避免飞书审批卡变成点击无效的僵尸卡
    if binding.proc is not None:
        try:
            binding.proc.deny_all_permissions()
        except Exception:  # noqa: BLE001
            pass
    binding.proc = None
    await _finish_stream(binding)
    # 进程退出：清掉可能残留的处理中表情（按返回码定 ✅/❌）
    _fire_and_forget(_cc_mark_done(
        binding.thread_id, ok=(returncode in (0, None))))
    _store.put(binding)


async def _stream_context(binding: CCBinding):
    """返回 (adapter, _StreamSession or None)。流式卡会话惰性建立并缓存。"""
    adapter = await _current_adapter()
    if adapter is None:
        return None, None
    streams = _STATE.setdefault("streams", {})
    stream = streams.get(binding.thread_id)
    if stream is None:
        stream = _StreamSession(adapter, binding.chat_id, binding.thread_id, binding)
        streams[binding.thread_id] = stream
    else:
        # 缓存里的 adapter 可能旧，刷新
        stream.adapter = adapter
    return adapter, stream


# --------------------------------------------------------------------------- #
# 权限请求 & 活动卡
# --------------------------------------------------------------------------- #

def _tool_input_brief(tool: str, inp: Any, *, max_len: int = 400) -> str:
    """按工具类型提取关键参数做预览（对齐 agents-to-im buildToolInputPreview）。

    Bash → command；文件/搜索类 → file_path/query/pattern/url；
    其余 → 前 N 个字段带 key。避免全量转储把敏感值刷进群聊。
    """
    if not isinstance(inp, dict) or not inp:
        return ""
    record = inp
    val = record.get("command") if tool == "Bash" else None
    if not isinstance(val, str):
        for k in ("file_path", "filePath", "path", "query", "pattern", "url", "notebook_path"):
            v = record.get(k)
            if isinstance(v, str) and v:
                val = v
                break
    if isinstance(val, str) and val.strip():
        return val.strip()[:max_len]
    parts = [f"{k}: {str(v)[:120]}" for k, v in list(record.items())[:6]]
    return "\n".join(parts)[:max_len]


async def _handle_permission(binding: CCBinding, req_event: Dict[str, Any]) -> None:
    """CC 权限请求 → 审批卡。

    req_event 由 CCProcess 内部的 can_use_tool 回调触发并携带:
      {type:"permission_request", request_id, tool, input}
    审批卡按钮触发 _handle_cc_action → proc.resolve_permission 解除挂起。
    """
    req_id = str(req_event.get("request_id") or req_event.get("req") or uuid4())
    tool = str(req_event.get("tool") or "Tool")
    inp = req_event.get("input") or {}
    brief = _tool_input_brief(tool, inp)
    # 内容自带代码围栏时改用缩进展示，避免破坏卡片 markdown 结构
    shown = textwrap.indent(brief, "    ") if (brief and "```" in brief) else brief
    message = f"**{tool}** 请求执行许可。\n\n```\n{shown or '(无参数)'}\n```"
    cards_payload = cards.build_permission_card(message, req_id, binding.mode, tool=tool,
                                                thread_id=binding.thread_id)
    adapter = await _current_adapter()
    if adapter is None:
        return
    await _send_card(adapter, binding.chat_id, cards_payload,
                     thread_id=binding.thread_id)


async def _handle_tool_use(binding: CCBinding, obj: Dict[str, Any]) -> None:
    """StreamEvent 起始（参数未齐）→ 发活动卡。完整参数由 AssistantMessage 回填。"""
    if not _show_tool_cards:
        return
    tuid = str(obj.get("id") or "")
    name = str(obj.get("name") or "Tool")
    brief = _tool_input_brief(name, obj.get("input") or {}, max_len=240)

    # StreamEvent 和 AssistantMessage 都可能先到，卡已登记则不重发
    if tuid and tuid in binding.tool_cards:
        return

    card = cards.build_activity_card(name, "执行中", brief or "(等待参数...)", status="running")
    adapter = await _current_adapter()
    if adapter is None:
        return
    mid = await _send_card(adapter, binding.chat_id, card, thread_id=binding.thread_id)
    if tuid and mid:
        binding.tool_cards[tuid] = json.dumps({"mid": mid, "name": name})
        # 防泄漏：一轮最多跟 N 张活动卡
        if len(binding.tool_cards) > 30:
            for k in list(binding.tool_cards.keys())[:-10]:
                binding.tool_cards.pop(k, None)


async def _update_tool_card(binding: CCBinding, obj: Dict[str, Any]) -> None:
    """AssistantMessage 完整 tool_use → PATCH 活动卡回填真实参数（若卡还在）。"""
    if not _show_tool_cards:
        return
    tuid = str(obj.get("id") or "")
    rec = binding.tool_cards.get(tuid)
    if not rec:
        # 无起始卡（极端时序）：直接按发卡路径补一张
        await _handle_tool_use(binding, obj)
        return
    try:
        info = json.loads(rec)
    except Exception:  # noqa: BLE001
        return
    mid = info.get("mid", "")
    if not mid:
        return
    name = str(obj.get("name") or "Tool")
    brief = _tool_input_brief(name, obj.get("input") or {}, max_len=400)
    card = cards.build_activity_card(name, "执行中", brief or "(无参数)", status="running")
    adapter = await _current_adapter()
    if adapter is None:
        return
    await _patch_interactive_card(adapter, mid, card)


async def _complete_tool_card(binding: CCBinding, tool_use_id: str, is_error: bool,
                              result_raw: Any) -> None:
    """tool_result 回来后 PATCH 对应活动卡为完成/失败。"""
    if not _show_tool_cards or not tool_use_id:
        return
    rec = binding.tool_cards.pop(tool_use_id, None)
    if not rec:
        return
    try:
        info = json.loads(rec)
    except Exception:  # noqa: BLE001
        info = {"mid": rec, "name": "Tool"}
    mid, name = info.get("mid", ""), info.get("name", "Tool")
    if not mid:
        return
    adapter = await _current_adapter()
    if adapter is None:
        return
    # 结果摘要
    summary = ""
    try:
        if isinstance(result_raw, str):
            summary = result_raw
        elif isinstance(result_raw, list):
            texts = []
            for b in result_raw:
                # SDK block 对象（TextBlock）或 plain dict 两种形态都要接住
                t = None
                if isinstance(b, dict):
                    if b.get("type") in (None, "text"):
                        t = b.get("text")
                else:
                    t = getattr(b, "text", None)
                if isinstance(t, str):
                    texts.append(t)
            summary = "\n".join(texts) if texts else json.dumps(
                [b if isinstance(b, dict) else str(getattr(b, "model_dump", lambda: str(b))()) for b in result_raw][:3]
            )[:300]
        else:
            summary = str(result_raw)[:300]
    except Exception:  # noqa: BLE001
        summary = ""
    summary = summary.strip()[:500]
    icon = "❌" if is_error else "✅"
    title_word = "失败" if is_error else "已完成"
    if summary and "```" in summary:
        shown = textwrap.indent(summary, "    ")  # 防内容围栏破坏卡片 markdown
        extra = f"\n\n---\n**输出**\n{shown}"
    else:
        extra = f"\n\n---\n**输出**\n```\n{summary}\n```" if summary else ""
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"💠 {icon} {cards.tool_icon(name)} {name} · {title_word}"},
            "template": "red" if is_error else "green",
        },
        "elements": [{"tag": "markdown", "content": (f"**{icon} `{name}` 执行{title_word}**{extra}")[:4000]}],
    }
    ok = await _patch_interactive_card(adapter, mid, card)
    if not ok:
        # patch 失败降级：发一条轻量文本
        mark = "✅ 完成" if not is_error else "❌ 失败"
        txt = f"{mark}" + (f" — {summary[:120]}" if summary else "")
        try:
            await _send_to_thread(adapter, binding.chat_id, txt, binding.thread_id)
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------- #
# 卡片回调
# --------------------------------------------------------------------------- #

def _handle_card_callback(text: str, source, gateway) -> Optional[Dict[str, Any]]:
    """解析合成 /card 事件，识别 cc_action 并处理。

    合成格式 '/card <tag> {json}'；对 tag 变体（含空格/直接 JSON 开头）做容错：
    找到第一个 '{' 起 JSON 解析。
    """
    try:
        rest = text[len("/card "):] if text.startswith("/card ") else text
        brace = rest.find("{")
        if brace < 0:
            return None  # 无 JSON 载荷，非 cc 卡片
        action_value = json.loads(rest[brace:])
        cc = cards.extract_cc_action(action_value)
        if cc is None:
            return None  # 非 cc 卡片，放行
    except Exception:  # noqa: BLE001
        return None

    # 卡片回调事件不带 thread_id（飞书合成 /card 事件 source.thread_id=None），
    # 用按钮 value 里塞的 tid 定位话题绑定
    thread_id = str(cc.get("tid") or "") or (getattr(source, "thread_id", None) or "")
    chat_id = getattr(source, "chat_id", "") or ""
    _fire_and_forget(_handle_cc_action(cc, thread_id, chat_id, gateway, source))
    return {"action": "skip", "reason": "cc-bridge card callback"}


async def _handle_cc_action(cc: Dict[str, Any], thread_id: str, chat_id: str,
                            gateway, source) -> None:
    adapter = _get_adapter(gateway)
    if adapter is None:
        return
    action = cc.get("cc_action")
    try:
        if action == cards.CC_ACTION_NEW_START:
            # workdir 卡的目录按钮 value 自带 cc_workdir；无值则退默认目录
            wd = _extract_select_value(cc) or _default_workdir or str(Path.home())
            # 带名字的 /new 在弹工作目录卡时暂存了命名，选完目录后补上
            name = (_STATE.get("pending_name", {}).pop(thread_id, None)
                    if _STATE else None)
            await _start_session(thread_id, chat_id, adapter, wd,
                                 session_name=name or None)
        elif action == cards.CC_ACTION_NEW_CANCEL:
            await _send_to_thread(adapter, chat_id, "已取消创建 Claude Code 会话。",
                                  thread_id)
        elif action == cards.CC_ACTION_PERM_ALLOW:
            await _resolve_permission(thread_id, cc, approved=True)
        elif action == cards.CC_ACTION_PERM_DENY:
            await _resolve_permission(thread_id, cc, approved=False)
        elif action == cards.CC_ACTION_MODE:
            mode = cc.get("mode", "")
            binding = _store.get(thread_id) if _store else None
            if not binding:
                await _send_to_thread(adapter, chat_id,
                                      f"模式已切换为 `{mode}`（未绑定会话，下次 /cc:new 生效）",
                                      thread_id)
            else:
                req = cc.get("req") or ""
                await _switch_mode(binding, mode, adapter, chat_id, thread_id,
                                  resolve_req=req)
        elif action == cards.CC_ACTION_SESSION_OPEN:
            # DM /cc:status 点击未关联话题的 CC 会话 → 新建话题并接入（布布 2026-09-14）
            sid = (cc.get("cc_session_id") or "").strip()
            wd = (cc.get("workdir") or "").strip() or _default_workdir \
                or str(Path.home())
            if not sid:
                await adapter.send(chat_id, "缺少会话 ID，无法打开。")
                return
            # 找标题（磁盘/registry 兜底），用作新话题锚点文案
            title = ""
            m0 = _registry.get(sid) if _registry is not None else None
            if m0 and m0.title:
                title = m0.title
            if not title:
                sessions = await asyncio.to_thread(
                    core.list_project_sessions, wd)
                t0 = next((s for s in sessions if s.id == sid), None)
                title = t0.title if t0 else "Claude Code 会话"
            new_tid = await _create_topic_thread(
                adapter, chat_id,
                f"↩️ 恢复 Claude Code 会话 · {title[:40]}")
            if not new_tid:
                await adapter.send(
                    chat_id, "创建话题失败，请稍后重试或用 /cc:new。")
                return
            # 新 binding + 切换到目标会话（resume 接管全部逻辑）
            nb = _store.get(new_tid)
            if nb is None:
                nb = CCBinding(thread_id=new_tid, chat_id=chat_id,
                               workdir=wd, mode=_default_mode or "default",
                               active_session_id="", visited_sessions=[])
                nb.created_at = time.time()
                nb.updated_at = nb.created_at
                _store.put(nb)
            target = core.SessionMeta(id=sid, title=title, workdir=wd)
            await _swap_to_session(new_tid, chat_id, adapter, nb, target)
        elif action == cards.CC_ACTION_RESUME_SELECT:
            sid = (cc.get("cc_session_id") or "").strip()
            binding = _store.get(thread_id) if _store else None
            if sid and binding:
                # 从 CC 盘或 registry 解析该会话的标题，构造 target 并切换
                workdir = binding.workdir or _default_workdir or ""
                sessions = await asyncio.to_thread(core.list_project_sessions, workdir)
                target = next((s for s in sessions if s.id == sid), None)
                if target is None:
                    # registry 里兜底
                    m = _registry.get(sid) if _registry else None
                    from .core import SessionMeta
                    target = SessionMeta(id=sid, title=(m.title if m else ""),
                                         workdir=workdir)
                await _swap_to_session(thread_id, chat_id, adapter, binding, target)
            else:
                await _send_to_thread(adapter, chat_id,
                                      "该话题还没绑定会话，无法恢复。先用 /cc:new。",
                                      thread_id)
        else:
            await _send_to_thread(adapter, chat_id,
                                  f"未识别的 cc 卡片操作：{action}",
                                  thread_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("cc card action error: %s", exc, exc_info=True)


def _extract_select_value(cc: Dict[str, Any]) -> str:
    """从卡片回调里取 select_static 的值。飞书 select 的 value 可能以 form-value 形式在 event。"""
    # select_value 通常在 action.value.option 或单独字段；这里接收常见形式
    for key in ("option", "value", "select_value", "cc_workdir"):
        v = cc.get(key)
        if isinstance(v, str) and v:
            return v
    return ""


def _normalize_cc_mode(mode: str) -> str:
    """归一化历史/别名模式名 → CLI 合法值。脏值("bypass"等)会令
    set_permission_mode 抛错甚至 spawn 失败(布布 2026-09-14 卡住根因)。"""
    m = (mode or "").strip()
    aliases = {
        "default": "default",
        "accept": "acceptEdits", "acceptedits": "acceptEdits",
        "plan": "plan",
        "bypass": "bypassPermissions",
        "bypasspermissions": "bypassPermissions",
        "dontask": "dontAsk", "auto": "auto",
    }
    if m in ("default", "acceptEdits", "plan", "bypassPermissions",
             "dontAsk", "auto"):
        return m
    return aliases.get(m.lower(), "default")


def _needs_restart_for_mode(old_mode: str, new_mode: str) -> bool:
    """判断模式切换是否需要重启 CC 进程。

    bypass ↔ 非 bypass 必须重启：
    - 切到 bypass：CLI 拒绝运行中热切到 --dangerously-skip-permissions，
      且 can_use_tool 回调优先级高于 permission_mode，热切后 bypass 被架空。
    - 切离 bypass：bypass 进程的 can_use_tool 直接放行(不走审批卡)，
      热切回 default 后审批回调仍不会生效，必须重启恢复完整审批流程。
    - 非 bypass 之间(default↔acceptEdits↔plan)：SDK set_permission_mode 可用，无需重启。
    """
    old = _normalize_cc_mode(old_mode)
    new = _normalize_cc_mode(new_mode)
    bypass = "bypassPermissions"
    return (old == bypass) != (new == bypass)


async def _switch_mode(binding: CCBinding, mode: str,
                       adapter, chat_id: str, thread_id: str,
                       *, resolve_req: str = "") -> None:
    """统一模式切换入口：文本命令(/cc:mode)和按钮(CC_ACTION_MODE)共用。

    bypass ↔ 非 bypass 方向 → 重启进程(resume 同一 CC 会话不丢上下文)；
    非 bypass 之间 → SDK 热切换(set_permission_mode)。
    """
    mode = _normalize_cc_mode(mode)
    old_mode = binding.mode
    if mode == old_mode:
        await _send_to_thread(adapter, chat_id,
                              f"当前已是 `{mode}` 模式，无需切换。",
                              thread_id)
        return

    if _needs_restart_for_mode(old_mode, mode):
        sid0 = binding.active_session_id or ""
        # 关旧进程
        old_proc = binding.proc
        if old_proc is not None:
            try:
                old_proc.deny_all_permissions()
                await old_proc.close()
            except Exception:  # noqa: BLE001
                logger.warning("cc[%s] %s→%s 重启: 旧进程关闭异常",
                               thread_id, old_mode, mode, exc_info=True)
        binding.proc = None
        binding.mode = mode
        binding.updated_at = time.time()
        if _store:
            _store.put(binding)
        # 以新模式重启，resume 同一 CC 会话
        await _start_process_for_binding(binding)
        arrow = f"{old_mode} → {mode}"
        note = (f"⚡ 已重启 CC 进程（{arrow}）"
                + (f"，已恢复会话 {sid0[:8]}…" if sid0 else "")
                + "\n💡 模式切换只对新请求生效，请重新发送你的指令")
        await _send_to_thread(adapter, chat_id, note, thread_id)
    else:
        # 非 bypass 之间：SDK 热切换
        await _set_mode(binding, mode)
        await _send_to_thread(adapter, chat_id,
                              f"✅ 已切换权限模式为 `{mode}`（热切换，无需重启）",
                              thread_id)

    # 审批卡上切换并放行当前请求（按钮路径）
    if resolve_req and binding.proc and binding.proc.pending_approval:
        p = binding.proc.pending_approval
        if p.get("req") == resolve_req:
            await _resolve_permission(thread_id, {"req": resolve_req}, approved=True)


async def _set_mode(binding: CCBinding, mode: str) -> None:
    """切换权限模式：更新绑定 + 如进程运行则通知 SDK 热切换。

    热切换失败时回滚持久化值——存了非法值会让下次 resume 的
    permission_mode 直接把 CC 进程炸掉（曾卡死话题，2026-09-14）。
    """
    mode = _normalize_cc_mode(mode)
    old_mode = binding.mode
    binding.mode = mode
    binding.updated_at = time.time()
    if _store:
        _store.put(binding)
    proc = binding.proc
    if proc and proc.running:
        try:
            await proc.set_mode(mode)
        except Exception as exc:  # noqa: BLE001
            logger.warning("cc: set_mode %s rejected (%s)，回滚为 %s",
                           mode, exc, old_mode)
            binding.mode = _normalize_cc_mode(old_mode)
            binding.updated_at = time.time()
            if _store:
                _store.put(binding)


async def _resolve_permission(thread_id: str, cc: Dict[str, Any], approved: bool) -> None:
    """把审批结果通过 SDK 回调写回 CC 进程。"""
    binding = _store.get(thread_id) if _store else None
    if not binding or not binding.proc:
        return
    req = cc.get("req", "")
    scope = cc.get("scope", "once")  # once | always
    ok = binding.proc.resolve_permission(req, approved, scope=scope, tool=cc.get("tool", ""))
    if ok:
        logger.info("cc[%s] permission %s resolved (approved=%s scope=%s)",
                    thread_id, req or "(first)", approved, scope)


# --------------------------------------------------------------------------- #
# 路由普通话题消息 → CC
# --------------------------------------------------------------------------- #

async def _route_to_cc(binding: CCBinding, text: str, gateway, source,
                       *, message_id: str = "") -> None:
    if not await ensure_cc_session(binding):
        # 有历史 session 但恢复失败（或已判失效）：提示一次，不无限重试
        if binding.active_session_id and not (binding.proc and binding.proc.running):
            adapter = _get_adapter(gateway)
            fails = _fail_counts.get(binding.thread_id, 0)
            tip = ("⚠️ Claude Code 会话恢复失败"
                   + (f"（已连续 {fails} 次，映射可能失效）" if fails >= _FAIL_THRESH else "")
                   + "。发 /reset 可开新会话。")
            if adapter:
                try:
                    await _send_to_thread(adapter, binding.chat_id, tip,
                                          binding.thread_id)
                except Exception:  # noqa: BLE001
                    pass
            return
        # 从未有过会话：静默返回（正常情况不会到这——路由前必有绑定+启动）
        return
    try:
        await binding.proc.send_text(text)
    except Exception as exc:  # noqa: BLE001
        adapter = _get_adapter(gateway)
        if adapter:
            await _send_to_thread(adapter, binding.chat_id,
                                  f"⚠️ 发送给 Claude Code 失败：{exc}",
                                  binding.thread_id)
        return
    if message_id:
        await _cc_mark_working(message_id, binding.thread_id, gateway)


# --------------------------------------------------------------------------- #
# 发送辅助
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# CC 生命周期表情（布布 2026-09-14：hermes 的 Typing 是 agent 响应指示；
# CC 转发后 agent turn 不跑 → 无任何指示。CC 用独立表情 OnIt=处理中,
# Done=完成, CrossMark=失败, 与 hermes 区分）
# --------------------------------------------------------------------------- #

_CC_EMOJI_WORKING = "OnIt"       # 🔵 收到并处理中（飞书官方 emoji_type）
_CC_EMOJI_DONE = "Done"          # ✅ 本轮回复完成
_CC_EMOJI_FAIL = "CrossMark"     # ❌ 本轮失败/进程异常退出

# thread_id → {"mid": message_id, "rid": reaction_id}（进行中的那个）
_cc_working_reactions: Dict[str, Dict[str, str]] = {}


async def _cc_reaction_add(adapter, mid: str, emoji: str) -> Optional[str]:
    """给消息加 CC 状态表情，返回 reaction_id（失败 None）。"""
    if not adapter or not mid:
        return None
    try:
        return await adapter._add_reaction(mid, emoji)
    except Exception:  # noqa: BLE001
        logger.warning("cc: add reaction %s failed", emoji, exc_info=True)
        return None


async def _cc_reaction_remove(adapter, mid: str, rid: str) -> bool:
    if not adapter or not mid or not rid:
        return False
    try:
        return await adapter._remove_reaction(mid, rid)
    except Exception:  # noqa: BLE001
        return False


async def _cc_mark_working(message_id: str, thread_id: str, gateway) -> None:
    """转发成功 → 给触发消息打 🔵(OnIt)。已有进行中的先清掉。"""
    adapter = _get_adapter(gateway)
    if not adapter or not message_id:
        return
    prev = _cc_working_reactions.pop(thread_id, None)
    if prev:
        await _cc_reaction_remove(adapter, prev["mid"], prev["rid"])
    rid = await _cc_reaction_add(adapter, message_id, _CC_EMOJI_WORKING)
    if rid:
        _cc_working_reactions[thread_id] = {"mid": message_id, "rid": rid}


async def _cc_mark_done(thread_id: str, *, ok: bool = True) -> None:
    """一轮结束 → 撤 🔵 打 ✅/❌。adapter 从运行中 runner 找。"""
    info = _cc_working_reactions.pop(thread_id, None)
    if not info:
        return
    adapter = await _current_adapter()
    if not adapter:
        return
    await _cc_reaction_remove(adapter, info["mid"], info["rid"])
    await _cc_reaction_add(adapter, info["mid"],
                           _CC_EMOJI_DONE if ok else _CC_EMOJI_FAIL)


def _get_adapter(gateway):
    """从 gateway runner 取 feishu adapter。"""
    try:
        from gateway.config import Platform  # noqa: PLC0415
        adapters = getattr(gateway, "adapters", None)
        if adapters is None:
            return None
        adapter = adapters.get(Platform.FEISHU)
        return adapter
    except Exception:  # noqa: BLE001
        return None


async def _current_adapter():
    """尝试从当前运行的 gateway 取 adapter（无显式 gateway 时）。"""
    try:
        from gateway.platforms.feishu.adapter import FeishuAdapter  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        pass
    # 通过 sys.modules 找活 runner
    import sys  # noqa: PLC0415
    main_mod = sys.modules.get("__main__")
    ref = getattr(main_mod, "_gateway_runner_ref", None) if main_mod else None
    runner = ref() if callable(ref) else None
    if runner is None:
        grun = sys.modules.get("gateway.run")
        ref2 = getattr(grun, "_gateway_runner_ref", None) if grun else None
        runner = ref2() if callable(ref2) else None
    return _get_adapter(runner)


async def _send_card(adapter, chat_id: str, card: Dict[str, Any], *, thread_id: str = "") -> Optional[str]:
    """以 msg_type=interactive 直发交互卡，返回 message_id（失败 None）。

    adapter.send() 只走 text/post 分支，不识别卡片 JSON；因此借 adapter 的
    lark client 经 _feishu_send_with_retry 直发（同进程内使用其内部方法，
    不修改 adapter 代码）。话题定位用 reply_to 锚点——create(receive_id_type=
    thread_id) 实测被飞书拒绝（99992402），不能依赖 metadata.thread_id。
    """
    payload = json.dumps(card, ensure_ascii=False)
    anchor = await _thread_anchor(adapter, chat_id, thread_id) if thread_id else None
    try:
        response = await adapter._feishu_send_with_retry(
            chat_id=chat_id,
            msg_type="interactive",
            payload=payload,
            reply_to=anchor,
            # 仅在确有锚点时才带 thread 标记(reply_in_thread=True)
            metadata={"thread_id": thread_id} if anchor else None,
        )
        result = adapter._finalize_send_result(response, "cc-bridge send card failed")
        return (result.message_id or None) if result.success else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("cc: send interactive card failed: %s", exc)
        return None


async def _patch_interactive_card(adapter, message_id: str, card: Dict[str, Any]) -> bool:
    """PATCH 已发送的交互卡（活动卡状态流转用）。成功 True。"""
    client = getattr(adapter, "_client", None)
    if client is None or not message_id:
        return False
    try:
        from lark_oapi.api.im.v1 import PatchMessageRequest, PatchMessageRequestBody  # noqa: PLC0415
        body = PatchMessageRequestBody.builder().content(json.dumps(card, ensure_ascii=False)).build()
        request = PatchMessageRequest.builder().message_id(message_id).request_body(body).build()
        response = await asyncio.to_thread(client.im.v1.message.patch, request)
        ok = getattr(response, "success", False)
        if not ok:
            logger.warning("cc: patch card %s failed: %s", message_id[:12], getattr(response, "msg", ""))
        return bool(ok)
    except Exception as exc:  # noqa: BLE001
        logger.warning("cc: patch interactive card failed: %s", exc)
        return False


async def _basic_reply(adapter, binding: Optional[CCBinding], text: str) -> None:
    if adapter is None or binding is None:
        return
    # 统一走话题投递: 带 thread_id 的 create(receive_id_type=thread_id) 会被飞书
    # 拒绝(99992402), 必须经锚点 reply 落线程 —— 直发只会静默丢失。
    # 默认 source="bridge" 加 🤖 cc-bridge 来源标识
    return await _send_to_thread(
        adapter, binding.chat_id, text, binding.thread_id)


def uuid4() -> str:
    import uuid
    return str(uuid.uuid4())
