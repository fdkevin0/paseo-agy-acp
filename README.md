# paseo-agy-acp

English · [简体中文](README.zh-CN.md)

Use Antigravity in [Paseo](https://paseo.sh) through the native `agy` Agent Client Protocol (ACP). Open and resume conversations, approve tools, select models, and track context and subscription usage from Paseo desktop or mobile clients.

The plugin ID is `agy-acp`; its provider is `agy`, displayed as **Antigravity (ACP)**.

## Prerequisites

- Paseo **0.15.0 or later**, with plugins enabled.
- Node.js **22.18 or later** and npm on the daemon host.
- Antigravity CLI (`agy`) **1.5.16 or later**, already authenticated on that host (run `agy` in a terminal to sign in first).

## Install

```sh
# From GitHub
paseo plugin install git:fdkevin0/paseo-agy-acp

# From npm
paseo plugin install npm:@fdkevin/paseo-agy-acp

# From a local clone
git clone https://github.com/fdkevin0/paseo-agy-acp.git
paseo plugin install ./paseo-agy-acp
```

See [development setup](docs/development.md#development-setup) for local dependencies and checks. For a remote daemon, add `--host <endpoint>`; local source paths and the `agy` executable must exist on that host.

## Usage

```sh
paseo run --provider agy "Explain this repository"
```

In Paseo, create an agent and select **Antigravity (ACP)**. Choose from the models, modes, thinking options, and slash commands supplied by `agy`, or use session import to resume an existing native conversation.

View context occupancy and subscription quota in the Composer context popover, or from the CLI with `paseo usage ls --json`.

Manage the plugin:

| Command                        | Purpose                                     |
| ------------------------------ | ------------------------------------------- |
| `paseo plugin ls`              | Check that `agy-acp` is running.            |
| `paseo plugin reload agy-acp`  | Reload after changing a local installation. |
| `paseo plugin disable agy-acp` | Disable the plugin.                         |
| `paseo plugin remove agy-acp`  | Remove its plugin configuration.            |

### Coexist with the built-in provider

The built-in `antigravity` provider can remain enabled alongside `agy`. To hide it, merge this fragment into the daemon's `~/.paseo/config.json` and reload the configuration:

```json
{
  "agents": {
    "providers": {
      "antigravity": { "enabled": false }
    }
  }
}
```

Existing agents keep their original provider; they are not migrated. Remove the override or set it to `true` to re-enable the built-in provider.

## Capabilities

| Capability    | Behavior                                                                                                                                 |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Sessions      | Create, list, import, and restore native conversations.                                                                                  |
| Prompts       | Text, images, slash commands, and attachment context.                                                                                    |
| Configuration | Native models, modes (`default`, `auto` when advertised, `accept-edits`, `plan`) plus **Full Access**, thinking choices, system prompts. |
| Permissions   | Native approval cards with arguments, MCP `toolPolicy` preapprovals, or unattended auto-approval in **Full Access** mode.                |
| Timeline      | Streaming responses, reasoning, plans, and rich tool cards.                                                                              |
| Context       | Token counters, context occupancy, and reported USD costs.                                                                               |
| Subscription  | Gemini and Claude/GPT 5-hour and weekly quota windows.                                                                                   |
| Subagents     | Native multi-level child links and read-only Brain transcript cards.                                                                     |

## Essential limitations

- **Unsupported Paseo features:** Steering, rewind, archive operations, structured output, Artifacts panels, and independent subagent session control are unsupported.
- **MCP cards:** Recognized calls display as `server.tool` and use exact `toolPolicy` grants; result-image extraction is not enabled. See [MCP behavior](docs/reference.md#mcp-identity-and-preapprovals).
- **Context and quota:** Context occupancy stays unchanged or unknown when no reliable snapshot is available; cumulative prompt counters remain separate. Current `agy` print mode rejects custom ACP directory flags for quota queries. See [context occupancy](docs/reference.md#context-window-occupancy) and [subscription quota](docs/reference.md#subscription-quota).

## More information

- [Reference](docs/reference.md): launch settings, modes, permissions, tool cards, persistence, context, and quota behavior.
- [Development](docs/development.md): setup, source layout, verification, packaging, and releases.
- [Architecture](docs/architecture.md): ACP, Brain, SQLite, and runtime pooling internals for contributors.
- [Plugin listing overview](OVERVIEW.md) · [简体中文](OVERVIEW.zh-CN.md)

## License

[MIT](LICENSE). This independent integration is not affiliated with Google or Paseo.
