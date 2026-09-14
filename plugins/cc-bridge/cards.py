"""cc-bridge.cards — 飞书交互卡片构造。

卡片 JSON 1.0 交互卡 (msg_type=interactive)。由插件经 adapter 的 lark client
以 ``msg_type=interactive`` 直发；按钮 value 在点击时被飞书 adapter 合成
``/card <tag> <json>`` 事件，经过 pre_gateway_dispatch 钩子流转。

所有按钮 value 统一携带 ``tid``(话题ID) —— 卡片回调事件不携带 thread_id，
必须靠 value 把点击路由回正确的会话。
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


def _btn(text: str, value: Dict[str, Any], btn_type: str = "default") -> Dict[str, Any]:
    """构造一个按钮，value 携带 cc_action 等标记。"""
    return {
        "tag": "button",
        "text": {"tag": "plain_text", "content": text},
        "type": btn_type,
        "value": value,
    }


# --------------------------------------------------------------------------- #
# Workdir 选择卡（/cc:new / 顶部触发）
# --------------------------------------------------------------------------- #

def build_workdir_card(
    workspaces: List[Dict[str, str]],
    header_title: str = "🤖 cc-bridge · 创建 Claude Code 会话",
    note: str = "点击目录直接在该话题启动会话：",
    thread_id: str = "",
) -> Dict[str, Any]:
    """workdir 选择卡：最近项目渲染为按钮列表。

    不用 select_static+独立按钮 —— 飞书裸卡片按钮回调不带所选 option 值
    （除非走 form 容器提交），点启动时会拿不到目录。每个目录一个按钮、
    value 自带完整路径，确定性行为。
    """
    actions = [
        _btn(w["label"], {"cc_action": CC_ACTION_NEW_START, "tid": thread_id,
                          "cc_workdir": w["value"]},
             "primary" if i == 0 else "default")
        for i, w in enumerate(workspaces[:8])
    ]
    elements: List[Dict[str, Any]] = [{"tag": "markdown", "content": note}]
    if actions:
        # 每行最多 2 个按钮，避免溢出
        for i in range(0, len(actions), 2):
            elements.append({"tag": "action", "actions": actions[i:i + 2]})
    else:
        elements.append({"tag": "markdown", "content": "_未找到历史目录，可 `/cc:new /绝对/路径` 直接指定_"})
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": header_title},
            "template": "blue",
        },
        "elements": elements,
    }


def build_session_picker_card(
    sessions: List["SessionMeta"],
    active: str = "",
    occupied_map: Optional[Dict[str, str]] = None,
    thread_id: str = "",
    chat_id: str = "",
    header_title: str = "🤖 cc-bridge · 恢复 Claude Code 会话",
) -> Dict[str, Any]:
    """CC 会话选择卡（/resume 无参）。

    每行一个会话：状态标（✅当前 / 🟡占用·跳转 / ⚪空闲）+ 标题 + id前8 + 按钮。
    - 空闲会话：按钮「在此到达」→ CC_ACTION_RESUME_SELECT
    - 当前 active：标 ✅ 无按钮
    - 其他话题占用：把链接显示在行内（不可点按钮）
    """
    occupied_map = occupied_map or {}
    elements: List[Dict[str, Any]] = [{
        "tag": "markdown",
        "content": f"共 {len(sessions)} 个历史 CC 会话。\n_点击「在此到达」把该会话切到本话题。_",
    }]
    for i, s in enumerate(sessions, 1):
        sid8 = s.id[:8]
        title = s.title or "(未命名)"
        if s.id == active:
            status = "✅ 当前"
            actions: List[Dict[str, Any]] = []
        elif s.id in occupied_map:
            status = "🟡 占用中"
            link = occupied_map[s.id]
            actions = []  # 占用不可夺占：只显示跳转链接，不放按钮
        else:
            status = "⚪ 空闲"
            actions = [_btn("在此到达", {
                "cc_action": CC_ACTION_RESUME_SELECT,
                "tid": thread_id, "cc_session_id": s.id,
            }, "primary")]
        row = f"**{i}. {title}**  ({sid8}…) · {status}"
        if s.id in occupied_map:
            row += f"\n   ↳ [其他话题占用中，点击跳转]({occupied_map[s.id]}) "
        elements.append({"tag": "markdown", "content": row})
        if actions:
            elements.append({"tag": "action", "actions": actions})
    return {
        "config": {"wide_screen_mode": True},
        "header": {"title": {"tag": "plain_text", "content": header_title},
                   "template": "blue"},
        "elements": elements,
    }


# --------------------------------------------------------------------------- #
# 审批卡（CC 需要权限执行工具时）
# --------------------------------------------------------------------------- #

def build_permission_card(
    message: str,
    permission_request_id: str,
    session_mode: str = "default",
    tool: str = "",
    thread_id: str = "",
) -> Dict[str, Any]:
    """CC 权限请求 → 审批卡。按钮值带 cc 标记 + request id + 工具名 + 话题ID。

    底部附模式切换快捷按钮（当前会话的热切模式）。
    """
    base = {"req": permission_request_id, "mode": session_mode, "tool": tool, "tid": thread_id}
    actions = [
        _btn("✅ Allow Once", {**base, "cc_action": "cc_perm_allow", "scope": "once"}, "primary"),
        _btn("✅ Always", {**base, "cc_action": "cc_perm_allow", "scope": "always"}),
        _btn("❌ Deny", {**base, "cc_action": "cc_perm_deny", "scope": "once"}, "danger"),
    ]

    mode_actions = [
        _btn(m, {"cc_action": "cc_mode", "mode": m, "tid": thread_id})
        for m in _MODE_CHOICES
    ]

    elements: List[Dict[str, Any]] = [
        {"tag": "markdown", "content": f"**Claude Code 请求权限**\n\n{message}"},
        {"tag": "action", "actions": actions},
        {"tag": "hr"},
        {"tag": "markdown", "content": f"*当前模式：`{session_mode}` → 切换并放行本次：*"},
        {"tag": "action", "actions": mode_actions},
    ]

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "🔐 Claude Code Permission"},
            "template": "orange",
        },
        "elements": elements,
    }


# 权限模式合法值(SDK PermissionMode Literal + CLI 校验名单)。历史版本发过
# "bypass" 这种缩写被 CLI 拒绝(Cannot set permission mode), 统一用全名。
_MODE_CHOICES = ("default", "acceptEdits", "plan", "bypassPermissions")


def build_mode_picker_card(session_mode: str = "default", thread_id: str = "") -> Dict[str, Any]:
    """独立模式切换卡（/cc:mode 无参数时）。"""
    actions = [
        _btn(m, {"cc_action": "cc_mode", "mode": m, "tid": thread_id})
        for m in _MODE_CHOICES
    ]
    elements: List[Dict[str, Any]] = [
        {"tag": "markdown", "content": f"**切换权限模式**（当前：`{session_mode}`）"},
        {"tag": "action", "actions": actions},
    ]
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "⚙️ Claude Code Mode"},
            "template": "blue",
        },
        "elements": elements,
    }


# --------------------------------------------------------------------------- #
# 活动卡（tool_use 状态流转: running → completed / failed）
# --------------------------------------------------------------------------- #

_TOOL_ICONS = {
    "Bash": "🛠",
    "Read": "📖",
    "Edit": "✏️",
    "Write": "📝",
    "Glob": "🔍",
    "Grep": "🔍",
    "TodoWrite": "📋",
    "WebFetch": "🌐",
    "WebSearch": "🌐",
    "Agent": "🤖",
}


def tool_icon(tool_name: str) -> str:
    return _TOOL_ICONS.get(tool_name, "🔧")


def build_activity_card(
    tool_name: str,
    title: str,
    content: str,
    status: str = "running",   # running | completed | failed
    extra: str = "",           # 结果摘要（completed/failed 时拼接）
) -> Dict[str, Any]:
    template = "blue" if status == "running" else ("green" if status == "completed" else "red")
    icon = {"running": "⏳", "completed": "✅", "failed": "❌"}.get(status, "⏳")
    body = f"**{icon} {tool_name}** · {title}\n\n{content}"
    if extra:
        body += f"\n\n---\n{extra}"
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"{icon} {tool_icon(tool_name)} {tool_name} · {title}"},
            "template": template,
        },
        "elements": [
            {"tag": "markdown", "content": body[:4000]},
        ],
    }


# --------------------------------------------------------------------------- #
# 辅助
# --------------------------------------------------------------------------- #

def card_to_payload(card: Dict[str, Any]) -> str:
    return json.dumps(card, ensure_ascii=False)


def extract_cc_action(value: Any) -> Optional[Dict[str, Any]]:
    """从卡片回调 value 里提取 cc_action 标记；不是 CC 的返回 None。"""
    if isinstance(value, dict):
        action = value.get("cc_action")
        if action:
            return value
    return None


# 卡片按钮 value 的键
CC_ACTION_NEW_START = "cc_new_start"
CC_ACTION_NEW_CANCEL = "cc_new_cancel"
CC_ACTION_PERM_ALLOW = "cc_perm_allow"
CC_ACTION_PERM_DENY = "cc_perm_deny"
CC_ACTION_MODE = "cc_mode"
CC_ACTION_RESUME_SELECT = "cc_resume_select"
# DM /cc:status 里点击未关联话题的 CC 会话 → 新建话题并接入该会话
CC_ACTION_SESSION_OPEN = "cc_session_open"


def build_status_block_card(
    blocks: List[Dict[str, Any]],
    header_title: str = "🤖 cc-bridge · Claude Code 会话",
) -> Dict[str, Any]:
    """DM /cc:status 全局总览卡（CardKit 2.0 伪表格版，布布 2026-09-14）。

    每 project —— 大字加粗头行「📁 项目路径：xxx · 时间」+
    灰底表头「会话|操作」+ hr 分隔数据行；未关联话题的行右侧 ↩ sm
    按钮（回调新建话题接入），已绑定行标题即 applink、操作列 💬。
    """
    def _open_btn(o: Dict[str, str], chat_id: str) -> Dict[str, Any]:
        return {
            "tag": "button",
            "size": "sm",
            "type": "default",
            "text": {"tag": "plain_text", "content": "恢复"},
            "behaviors": [{
                "type": "callback",
                "value": {"cc_action": CC_ACTION_SESSION_OPEN,
                          "tid": "",
                          "chat_id": chat_id,
                          "cc_session_id": o["sid"],
                          "workdir": o.get("workdir", "")},
            }],
        }

    body: List[Dict[str, Any]] = []
    for blk in blocks:
        lines = [l.strip() for l in blk["md"].split("\n")[1:] if l.strip()]
        head_line = blk["md"].split("\n", 1)[0].strip()
        head_parts = [p.strip() for p in head_line.split("·")]
        proj_disp = head_parts[0]
        tail_meta = " · ".join(head_parts[1:]) if len(head_parts) > 1 else ""
        if not lines:
            continue
        # 项目头: 大字加粗 + 📁 说明标题前缀; 时间放次级小字
        head_md = f"**📁 项目路径：{proj_disp}**"
        sub_md = f"<font color='grey'>{tail_meta}</font>" if tail_meta else ""

        opens = list(blk.get("open_buttons", []))
        seq_it = iter(range(1, len(opens) + 1))

        rows_els: List[Dict[str, Any]] = []
        first_data = True
        n_rows = 0
        for l in lines:
            is_link = "](" in l
            if not first_data:
                rows_els.append({"tag": "hr"})
            first_data = False
            if is_link:
                title_md = l.lstrip("-💬 ") or "（未命名）"
                right_el: Dict[str, Any] = {"tag": "markdown", "content": "💬"}
            else:
                import re as _re
                mm = _re.match(r"^\(?\d+\)?[.、)）]?\s*(.*)$", l)
                raw_t = (mm.group(1) if mm else l).strip() or "（空会话）"
                ob = None
                if opens:
                    idx = next(seq_it, None)
                    if idx is not None:
                        ob = opens[idx - 1]
                short_t = raw_t if len(raw_t) <= 30 else raw_t[:28] + "…"
                n_rows += 1
                title_md = f"{n_rows}. {short_t}"
                right_el = (_open_btn(ob, blk.get("chat_id", ""))
                            if ob is not None
                            else {"tag": "markdown", "content": ""})
            rows_els.append({
                "tag": "column_set", "flex_mode": "none",
                "columns": [
                    {"tag": "column", "width": "weighted", "weight": 4,
                     "vertical_align": "center",
                     "elements": [{"tag": "markdown", "content": title_md}]},
                    {"tag": "column", "width": "auto",
                     "vertical_align": "center",
                     "elements": [right_el]},
                ]})
        if not rows_els:
            continue
        # 表头插到数据行最前(灰底)
        rows_els.insert(0, {
            "tag": "column_set", "flex_mode": "none",
            "background_style": "grey",
            "columns": [
                {"tag": "column", "width": "weighted", "weight": 4,
                 "vertical_align": "center",
                 "elements": [{"tag": "markdown", "content": "**会话**"}]},
                {"tag": "column", "width": "auto",
                 "vertical_align": "center",
                 "elements": [{"tag": "markdown", "content": "**操作**"}]},
            ]})
        # 头部块: 大字项目行 + 次级时间行
        head_block: List[Dict[str, Any]] = [
            {"tag": "markdown", "content": head_md}]
        if sub_md:
            head_block.append({"tag": "markdown", "content": sub_md})
        body.extend(head_block)
        body.extend(rows_els)
    if not body:
        body = [{"tag": "markdown",
                 "content": "还没有任何 CC 会话。发 `/cc:new` 新建一个。"}]
    return {
        "schema": "2.0",
        "config": {"update_multi": True},
        "header": {"title": {"tag": "plain_text", "content": header_title},
                   "template": "blue"},
        "body": {"elements": body},
    }
