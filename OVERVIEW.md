# Antigravity ACP

Connect Antigravity to Paseo through its native Agent Client Protocol. The provider appears as **Antigravity (ACP)** and exposes native conversation import, history, permissions and model controls.

## How it works

The plugin runs `agy acp` on the daemon host and translates its notifications into Paseo timeline cards. Filesystem and terminal tools remain owned by agy. The existing Antigravity provider can remain available alongside it.

## Requirements and configuration

Requires Paseo 0.15.0+, Node.js 22.18+, npm and an authenticated agy CLI 1.4.12+ on the daemon host. Provider ID `agy` accepts Paseo's normal command and environment overrides. Model, mode and thinking choices are discovered from agy. The plugin does not change login credentials or globally disable approvals.

## Capabilities and data flow

Supports text, images, attachment context, commands, permission decisions, native session persistence, usage reporting and rich tool cards. Prompt content and configured MCP servers go to the local agy process, which communicates with its configured model service. Local child transcripts under the native Brain directory are read to populate subagent timelines. Events are delivered to connected Paseo clients.

Catalog discovery attempts to close its temporary native session and remove only that session's generated files; cleanup failures do not hide the model list. Normal conversation data is retained. Launch environments are hashed for cache identity, not written into cache keys in plain text.

## Known limits

Subagent tracking requires a native conversation ID in the tool result. Brain is a read-only log projection: child tool cards follow the running/completed/failed states recorded in the native log and rely on positional result ordering. Recursive child discovery and Artifacts panels are not implemented. Steering, rewind, archive operations, structured output and custom system prompts are unsupported. Runtime cleanup is conservatively vetoed because ACP cannot prove that background work has stopped; explicitly closing the session releases its process.
