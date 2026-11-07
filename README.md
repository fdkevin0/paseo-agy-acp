# paseo-agy-acp

An Antigravity ACP provider for [Paseo](https://paseo.sh). Use native agy sessions, permission prompts, model controls and rich tool timelines from Paseo desktop or mobile clients.

- Plugin ID: `agy-acp`
- Provider ID: `agy`
- Display name: **Antigravity (ACP)**

## Prerequisites

- Paseo **0.15.0 or later**, with plugins enabled.
- Node.js **22.18 or later** and npm on the daemon host.
- Antigravity CLI (`agy`) **1.4.12 or later**, authenticated on that host. The plugin probes `agy --version`; it does not perform login.

## Install

From a local checkout:

```sh
npm ci --ignore-scripts
npm run check
npm test
paseo plugin install /absolute/path/to/paseo-agy-acp
paseo plugin ls
```

Once the repository or package has been published, install from GitHub or npm:

```sh
paseo plugin install git:fdkevin0/paseo-agy-acp
paseo plugin install npm:@fdkevin0/paseo-agy-acp
```

These are distribution addresses, not a claim that a release is already available. Git installation runs the manifest's locked production dependency preparation. For a remote daemon, add `--host <endpoint>`; the source path and agy executable must exist on that host.

## Usage

```sh
paseo run --provider agy "Explain this repository"
```

In Paseo, create an agent and choose **Antigravity (ACP)**. The model, mode, thinking selector and slash commands come from the native CLI. Use Paseo's session import to open an existing agy conversation.

The built-in `antigravity` provider remains available alongside `agy`. To hide it after validating this plugin, merge this fragment into the daemon's `~/.paseo/config.json` and reload the configuration:

```json
{
  "agents": {
    "providers": {
      "antigravity": { "enabled": false }
    }
  }
}
```

Existing agents keep their original provider. This plugin does not migrate them. Re-enable the built-in provider by removing that override or setting it to `true`.

## Capabilities

| Capability    | Behavior                                                         |
| ------------- | ---------------------------------------------------------------- |
| Sessions      | Create, list, import and restore through native ACP persistence  |
| Prompts       | Text, images, commands and readable attachment context           |
| Configuration | Native models, modes and per-model thinking choices              |
| Permissions   | Native options with tool arguments/details; no implicit bypass   |
| Timeline      | Streaming responses, reasoning, plans and rich tool cards        |
| Usage         | Token counters, context usage and USD costs when supplied by agy |
| Subagents     | Native child links and read-only Brain transcript cards          |

Steering, rewind, archive/unarchive, structured output and custom system prompts are not implemented. Artifacts panels and recursive child discovery are not implemented. Child transcript cards are a projection of native logs, not an independently controllable ACP session.

## Verification

```sh
npm run check
npm test
npm run format:check
npm pack --dry-run
```

Tests use a local ACP fixture and temporary files, without calling a model or consuming quota. Changes to the local plugin require `paseo plugin reload agy-acp`; verify `running` using `paseo plugin ls`. Disable it with `paseo plugin disable agy-acp`.

## Documentation

- [Reference](docs/reference.md): launch configuration, tool mappings, permissions and data flow.
- [Development](docs/development.md): tests, troubleshooting, packaging and release setup.
- [Plugin overview](OVERVIEW.md) · [简体中文](OVERVIEW.zh-CN.md)

## License

[MIT](LICENSE). This independent integration is not affiliated with Google or Paseo.
