# Antigravity ACP

Connect Antigravity to Paseo through its native Agent Client Protocol. The provider appears as **Antigravity (ACP)** and supports conversation import, history, permissions and model controls. It can coexist with Paseo's built-in Antigravity provider.

## How it works

The plugin runs the locally installed `agy acp` on the daemon host and translates its notifications into Paseo timeline cards. Filesystem and terminal tools remain owned by agy, with native approval decisions passed through Paseo.

## Requirements and setup

Requires Paseo 0.15.0+, Node.js 22.18+, npm and an authenticated agy CLI 1.4.12+ on the daemon host. Sign in through agy before using the plugin. The provider ID is `agy`; it accepts Paseo's command and environment overrides. Model, mode, thinking options and slash commands come from agy. The plugin does not install the CLI, change login credentials or globally bypass approvals.

Linux has been smoke-tested; other operating systems have not been verified. See the [README](README.md) for setup and usage.

## Capabilities and data flow

Supports text, images, attachment context, native commands, permission decisions, persistent sessions, rich tool cards and child conversation timelines. Prompt content, tool results and configured MCP services are handled by the local agy process, which communicates with its configured model service. The plugin reads local Brain child transcripts to populate timelines; events go to connected Paseo clients.

The Composer context popover shows context occupancy and subscription quota. Usage Source `agy`, also available through `paseo usage ls --json`, reports Gemini and Claude/GPT 5-hour and weekly quotas, reset times and the subscription tier when known. It calls agy's `/usage` command without starting a model turn. Account labels come from local login logs; token contents are not read by the plugin or exposed in usage reports.

Temporary discovery sessions and new sessions closed before their first prompt are cleaned up where possible. Imported and prompted conversations are retained. Explicitly closing a session releases its local runtime.

## Known limits

Context occupancy is an estimate when native measurements are unavailable. Current agy print mode rejects custom ACP directory flags; quota queries with those flags return an error instead of querying another account. Subagent timelines depend on native logs and do not provide independent session control.

Steering, rewind, archive operations, structured output, custom system prompts, recursive child discovery and Artifacts panels are unsupported. Automatic idle runtime cleanup is disabled because ACP cannot confirm that background work has stopped.
