"""生成 /help 输出: 三栏结构(桥接管理 / CC 核心 / 插件&技能命令), 中文说明。"""


def _core_desc():
    return {
        "add-dir": "添加额外工作目录",
        "agents": "查看/管理子代理配置",
        "clear": "清空对话历史并释放上下文",
        "compact": "压缩历史为摘要（可带指令）",
        "config": "打开配置面板",
        "context": "可视化上下文占用",
        "copy": "复制 Claude 上次回复",
        "cost": "本次会话费用与耗时",
        "doctor": "诊断 CC 安装与环境",
        "effort": "设置推理力度 (effort level)",
        "export": "导出当前对话",
        "files": "列出上下文中的文件",
        "hooks": "查看 hook 配置",
        "insights": "生成会话分析报告",
        "loop": "定时循环执行命令",
        "mcp": "MCP 服务器管理",
        "memory": "编辑记忆文件",
        "model": "查看/切换模型",
        "plan": "进入 plan 模式或查看计划",
        "pr-comments": "获取 GitHub PR 评论",
        "reload-plugins": "重载插件变更",
        "rename": "重命名当前会话",
        "resume": "恢复历史会话",
        "review": "审查一个 Pull Request",
        "schedule": "定时远程任务管理",
        "security-review": "对当前分支做安全审查",
        "skills": "列出可用技能",
        "stats": "使用统计与活跃度",
        "status": "版本/模型/账号状态(C版为面板型)",
        "tag": "给会话打标签",
        "usage": "套餐用量余量",
        "vim": "Vim 编辑模式开关",
    }


def build_help() -> str:
    core = _core_desc()
    bridge = [
        ("/new [名字]", "新建 CC 会话（无参自动命名；可带名字；私聊中自动建话题）"),
        ("/cd", "切换工作目录（重启 CC 进程）"),
        ("/reset", "重置当前话题的 CC 会话"),
        ("/resume", "列历史 CC 会话；/resume -c 恢复最近；/resume <id|标题|数字> 指定恢复"),
        ("/mode", "权限模式 default/acceptEdits/plan/bypassPermissions"),
        ("/rewind", "回滚文件到第 N 条消息前（无参列出检查点）"),
        ("/stop", "停止当前 CC 进程并释放会话（其他话题可 /resume）"),
        ("/status", "查看当前话题的 CC 会话状态"),
        ("/help", "本帮助"),
    ]
    lines1 = ["**🧩 桥接管理**（本插件处理，不进 CC）"]
    # post 通道(与私聊同源)支持完整标准 markdown, 行内代码/列表均可渲染
    lines1 += [f"- `{cmd}` — {desc}" for cmd, desc in bridge]
    lines2 = ["", "**⚡ CC 核心命令**（透传 headless 原生执行）"]
    lines2 += [f"- `/{name}` {desc}" for name, desc in core.items()]
    lines3 = ["", "**📚 插件 / 技能型命令**（跟随项目 .claude 目录，原样透传）",
              "`/commit` `/diff` `/simplify` `/batch` `/btw` `/brief` "
              "`/ship-audit` `/migration-review` `/pyright` `/nohup` …"]
    return "\n".join(lines1 + lines2 + lines3)


if __name__ == "__main__":
    print(build_help())
