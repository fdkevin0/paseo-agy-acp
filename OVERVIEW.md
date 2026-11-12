# Antigravity ACP

Connect Antigravity to Paseo through its native Agent Client Protocol. The provider appears as **Antigravity (ACP)** and supports conversation import, history, permissions and model controls. It can coexist with Paseo's built-in Antigravity provider.

## How it works

The plugin runs the locally installed `agy acp` on the daemon host and translates its notifications into Paseo timeline cards. Filesystem and terminal tools remain owned by agy, with native approval decisions passed through Paseo.

## Requirements and setup

Requires Paseo 0.15.0+, Node.js 22.18+, npm and an authenticated agy CLI 1.5.3+ on the daemon host. Sign in through agy before using the plugin. The provider ID is `agy`; it accepts Paseo's command and environment overrides. Model, mode, thinking options and slash commands come from agy. The plugin does not install the CLI, change login credentials or globally bypass approvals.

Linux has been smoke-tested; other operating systems have not been verified. See the [README](https://github.com/fdkevin0/paseo-agy-acp#readme) for setup and usage.

## Capabilities and data flow

Supports text, images, attachment context, appended system prompts, native commands, permission decisions, persistent sessions, rich tool cards and child conversation timelines. Prompt content, tool results and configured MCP services are handled by the local agy process, which communicates with its configured model service. The plugin reads local Brain transcripts and structured `.system_generated` state to populate timelines; events go to connected Paseo clients.

The Composer context popover shows context occupancy and subscription quota. Usage Source `agy`, also available through `paseo usage ls --json`, reports Gemini and Claude/GPT 5-hour and weekly quotas, reset times and the subscription tier when known. It calls agy's `/usage` command without starting a model turn. Account labels come from local login logs; token contents are not read by the plugin or exposed in usage reports.

Temporary discovery sessions and new sessions closed before their first prompt are cleaned up where possible. Imported and prompted conversations are retained. Closing imported or prompted sessions stops their local runtime.

## Known limits

When native `usage_update` measurements are unavailable, or after a turn enters post-RPC Reactive Wakeup where `agy` emits no further `usage_update` notifications, context occupancy is computed from per-step token counters in `transcript_full.jsonl`. Current agy print mode rejects custom ACP directory flags; quota queries with those flags return an error instead of querying another account. Subagent timelines depend on native Brain artifacts and do not provide independent session control.

Steering, rewind, archive operations, structured output and Artifacts panels are unsupported. Idle runtime cleanup (`session.can_stop_runtime`) is permitted only when no foreground turn, pending approval, active subagent, background task, or standing `IsDaemon` schedule is running.
