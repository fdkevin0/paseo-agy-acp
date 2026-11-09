# paseo-agy-acp

English · [简体中文](README.zh-CN.md)

Use Antigravity in [Paseo](https://paseo.sh) through the native `agy` Agent Client Protocol. Open and resume conversations, approve tools, select models and track usage from Paseo desktop or mobile clients.

The plugin ID is `agy-acp`; its provider is `agy`, displayed as **Antigravity (ACP)**.

## Prerequisites

- Paseo **0.15.0 or later**, with plugins enabled.
- Node.js **22.18 or later** and npm on the daemon host.
- Antigravity CLI (`agy`) **1.5.3 or later**, already authenticated on that host. Run `agy` in a terminal to sign in before using the plugin.

## Install

From GitHub:

```sh
paseo plugin install git:fdkevin0/paseo-agy-acp
```

From npm:

```sh
paseo plugin install npm:@fdkevin/paseo-agy-acp
```

From a local clone:

```sh
git clone https://github.com/fdkevin0/paseo-agy-acp.git
paseo plugin install ./paseo-agy-acp
```

See [development setup](docs/development.md#development-setup) for dependencies and local checks. For a remote daemon, add `--host <endpoint>`; local source paths and the agy executable must exist on that host.

## Usage

```sh
paseo run --provider agy "Explain this repository"
```

In Paseo, create an agent and select **Antigravity (ACP)**. Choose from the models, modes, thinking options and slash commands supplied by agy. Use session import to resume an existing native conversation.

View context occupancy and subscription quota in the Composer context popover, or check account quota from the CLI:

```sh
paseo usage ls --json
```

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

| Capability    | Behavior                                                                                                                                  |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Sessions      | Create, list, import and restore native conversations.                                                                                    |
| Prompts       | Text, images, slash commands and attachment context.                                                                                      |
| Configuration | Native models, modes (`default`, `accept-edits`, `plan`) plus **Full Access**, per-model thinking choices and appended system prompts.    |
| Permissions   | Native approval options with tool arguments, details, MCP `toolPolicy` preapprovals, or unattended auto-approval in **Full Access** mode. |
| Timeline      | Streaming responses, reasoning, plans and rich tool cards.                                                                                |
| Context       | Token counters, context occupancy and reported USD costs.                                                                                 |
| Subscription  | Gemini and Claude/GPT 5-hour and weekly quota windows.                                                                                    |
| Subagents     | Native multi-level child links and read-only Brain transcript cards.                                                                      |

Steering, rewind, archive operations, structured output and Artifacts panels are unsupported. Child cards do not provide independent session control. When agy supplies no native `usage_update` notification, context occupancy is computed from per-step token counters in `transcript_full.jsonl` (falling back to prompt counters); current agy print mode rejects custom ACP directory flags for quota queries. See [reference limitations](docs/reference.md) for details.

## More information

- [Reference](docs/reference.md): launch settings, tool mappings, persistence, permissions, context and quota behavior.
- [Development](docs/development.md): request flow, source layout, verification, packaging and releases.
- [Plugin listing overview](OVERVIEW.md) · [简体中文](OVERVIEW.zh-CN.md)

## License

[MIT](LICENSE). This independent integration is not affiliated with Google or Paseo.
