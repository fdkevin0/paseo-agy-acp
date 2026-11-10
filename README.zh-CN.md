# paseo-agy-acp

[English](README.md) · 简体中文

通过原生 `agy` Agent Client Protocol (ACP) 在 [Paseo](https://paseo.sh) 中使用 Antigravity。支持从 Paseo 桌面端或移动端打开与恢复会话、审批工具权限、切换模型及查看上下文与订阅配额用量。

插件 ID 为 `agy-acp`；Provider ID 为 `agy`，显示名称为 **Antigravity (ACP)**。

## 前置条件

- Paseo **0.15.0 或更高版本**，且已启用插件功能。
- daemon 主机已安装 Node.js **22.18 或更高版本** 及 npm。
- daemon 主机已安装并完成登录的 Antigravity CLI (`agy`) **1.5.3 或更高版本**。使用本插件前，请先在终端运行 `agy` 完成账号登录。

## 安装

从 GitHub 安装：

```sh
paseo plugin install git:fdkevin0/paseo-agy-acp
```

从 npm 安装：

```sh
paseo plugin install npm:@fdkevin/paseo-agy-acp
```

从本地克隆安装：

```sh
git clone https://github.com/fdkevin0/paseo-agy-acp.git
paseo plugin install ./paseo-agy-acp
```

依赖安装与本地自检流程见[开发设置](docs/development.md#development-setup)。若连接远程 daemon，请附加 `--host <endpoint>`；本地源码路径与 `agy` 可执行文件必须位于该远程主机上。

## 使用方法

```sh
paseo run --provider agy "Explain this repository"
```

在 Paseo 中创建 Agent 并选择 **Antigravity (ACP)**，即可使用由 `agy` 提供的模型、模式、思考等级与原生斜杠命令，或通过会话导入功能恢复已有的原生会话。

可在 Composer 上下文弹窗中查看上下文占用与订阅配额，或通过 CLI 查询账号配额：

```sh
paseo usage ls --json
```

插件管理命令：

| 命令                           | 用途                      |
| ------------------------------ | ------------------------- |
| `paseo plugin ls`              | 检查 `agy-acp` 运行状态。 |
| `paseo plugin reload agy-acp`  | 修改本地安装后重新加载。  |
| `paseo plugin disable agy-acp` | 禁用插件。                |
| `paseo plugin remove agy-acp`  | 移除插件配置。            |

### 与内置 Provider 共存

内置的 `antigravity` Provider 可与 `agy` 同时启用。若希望隐藏内置 Provider，可将以下片段合并到 daemon 主机的 `~/.paseo/config.json` 并重新加载配置：

```json
{
  "agents": {
    "providers": {
      "antigravity": { "enabled": false }
    }
  }
}
```

已有 Agent 会保留其原有的 Provider，不会自动迁移。删除该配置项或将其设为 `true` 即可重新启用内置 Provider。

## 支持能力

| 能力     | 行为说明                                                                                                                     |
| -------- | ---------------------------------------------------------------------------------------------------------------------------- |
| 会话     | 创建、列出、导入并恢复原生会话。                                                                                             |
| 提示词   | 支持文本、图片、斜杠命令与附件上下文。                                                                                       |
| 配置     | 原生模型、模式（`default`、`accept-edits`、`plan`）与 **Full Access** 全权限模式、按模型区分的思考等级选项及附加系统提示词。 |
| 权限审批 | 原生审批选项（展示工具参数与详情卡片）、MCP `toolPolicy` 预批准，或 **Full Access** 模式下的免确认自动放行。                 |
| 时间线   | 流式回复、推理过程、执行计划与丰富工具卡片。                                                                                 |
| 上下文   | Token 计数、上下文窗口占用及已报告的美元费用。                                                                               |
| 订阅配额 | Gemini 与 Claude/GPT 模型组的 5 小时及每周配额窗口。                                                                         |
| 子 Agent | 原生多层级子会话链接与只读 Brain 日志时间线卡片。                                                                            |

暂不支持运行中追加指令（steering）、回退（rewind）、归档操作、结构化输出或 Artifacts 面板。子会话卡片不提供独立的会话控制。纯前台回合优先采用原生 `usage_update` 上下文通知；当未收到 `usage_update` 或回合进入 RPC 返回后的 Reactive Wakeup 阶段时，上下文占用基于 `transcript_full.jsonl` 的单步 Token 计数计算（缺失时回退至回合计数）。当前 `agy` 的 print 模式不接受用于配额查询的自定义 ACP 目录参数。详见[参考文档](docs/reference.md)。

## 更多信息

- [参考文档](docs/reference.md)：启动配置、工具映射、持久化、权限审批、上下文与配额行为说明。
- [开发与发布](docs/development.md)：请求流程、源码结构、测试验证、打包与发布流程。
- [插件市场概览 (English)](OVERVIEW.md) · [插件市场概览 (简体中文)](OVERVIEW.zh-CN.md)

## 许可证

[MIT](LICENSE)。本项目为独立集成插件，与 Google 或 Paseo 无隶属关系。
