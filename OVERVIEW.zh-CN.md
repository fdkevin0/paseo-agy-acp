# Antigravity ACP

通过原生 Agent Client Protocol 将 Antigravity 接入 Paseo。Provider 显示为 **Antigravity (ACP)**，支持会话导入、历史恢复、权限审批和模型配置，可与 Paseo 内置的 Antigravity Provider 共存。

## 工作原理

插件在 daemon 主机上运行本地已安装的 `agy acp`，将其通知转换为 Paseo 时间线卡片。文件系统和终端工具仍由 agy 执行，原生审批决策通过 Paseo 传递。

## 环境要求与设置

需要 Paseo 0.15.0+、Node.js 22.18+、npm，以及已在 daemon 主机完成登录的 agy CLI 1.5.3+。请先通过 agy 登录，再使用插件。Provider ID 为 `agy`，接受 Paseo 的命令和环境变量覆盖配置。模型、执行模式、思考选项及斜杠命令均来自 agy。插件不会安装 CLI、修改登录凭据或全局跳过审批。

Linux 已通过冒烟测试，其他操作系统尚未验证。设置和使用说明见 [README](README.zh-CN.md)。

## 支持能力与数据流向

支持文本、图片、附件上下文、附加系统提示词、原生命令、权限决策、会话持久化、工具卡片和子会话时间线。提示词、工具结果及配置的 MCP 服务由本地 agy 进程处理，再由 agy 与其配置的模型服务通信。插件读取本地 Brain 日志与 `.system_generated` 结构化状态以构建时间线，事件发送给已连接的 Paseo 客户端。

Composer 上下文弹窗展示上下文占用和订阅配额。用量源 `agy` 也可通过 `paseo usage ls --json` 查看，提供 Gemini 与 Claude/GPT 模型组的 5 小时和每周配额、重置时间，以及已知的订阅等级。它调用 agy 的 `/usage` 命令，不启动模型回合。账号标签来自本地登录日志；插件不读取令牌内容，也不会在用量报告中暴露令牌。

临时探测会话和首次发送提示词之前就关闭的新会话会尽可能清理；导入的会话及已发送提示词的会话予以保留。关闭导入的会话或已发送提示词的会话会停止其本地运行时。

## 已知限制

没有原生 `usage_update` 测量值时，或回合进入 RPC 返回后的 Reactive Wakeup 阶段（此时 `agy` 不再发送 `usage_update` 通知）时，上下文占用基于 `transcript_full.jsonl` 的单步 Token 计数计算。当前 agy 的 print 模式不接受自定义 ACP 目录参数；带有此类参数的配额查询会返回错误，不会改查其他账号。子会话时间线依赖原生 Brain 状态与日志，不提供独立的会话控制。

暂不支持运行中追加指令、回退、归档、结构化输出或 Artifacts 面板。仅当前台回合、挂起审批、活跃子会话、后台任务及常驻 `IsDaemon` 定时任务均处于空闲状态时，才允许宿主执行运行时空闲自动回收（`session.can_stop_runtime`）。
