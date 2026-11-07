# Reference

## Launch and discovery

Register `agy` through plugin `agy-acp`. The default command is `agy acp`. Paseo resolves the executable, argument prefix and complete environment before connecting. The session environment is applied on top of the launch environment. Session MCP servers are translated to ACP stdio/HTTP/SSE configurations.

Provider status runs the executable with `--version`, with a five-second timeout, and requires a parseable version at least 1.4.12. Catalog identity hashes the command, arguments and sorted environment using SHA-256. Within a connection, concurrent catalog requests for the same working directory share a promise; failed probes may be retried.

A probe opens an unprompted native session, reads its configuration, calls `session/close`, stops the process, and removes that UUID's `.db`, `.db-wal`, `.db-shm` and Brain directory. IDs must be UUIDs. Cleanup refuses symlink targets and does not traverse symlink directories. It never deletes shared summary databases or normal sessions. Cleanup is best-effort: native close or filesystem errors do not discard an already-discovered catalog. Files are removed only after the process stops; failed cleanup may leave temporary artifacts.

The native data directory defaults to `$HOME/.gemini/antigravity-cli`. The `--gemini-dir` / `--gemini_dir` and `--app-data-dir` / `--app_data_dir` launch flags are respected for cleanup and child tracking. Other vendor directory overrides are not interpreted.

## Configuration and capabilities

Model, mode and thinking controls derive from native `configOptions`. Where the legacy model inventory is present, effort variants constrain each model's thinking choices. Fixed-effort IDs hide the selector. The displayed context-window fallback is 131,072 for GPT-OSS models and 256,000 for other models; live usage provides the native size when available.

Opening a session changes configuration only when a requested value differs from the native state. Every successful open publishes effective configuration before `session.ready`.

Advertised capabilities: `prompt.message`, `prompt.command`, `prompt.image`, `session.configure`, `session.list`, `session.persistence`, `session.can_stop_runtime`, `permission`, `session.subsession`. The connection negotiates these with Paseo and validates every incoming operation against the negotiated set.

Non-image attachments become text containing the supplied paths, titles, URLs, bodies or review comments. Uploaded files are referenced by daemon-side path; the plugin does not silently read arbitrary file contents. agy can use its own file tools subject to its permissions.

## Tool cards

| Native arguments or content        | Paseo card                                  |
| ---------------------------------- | ------------------------------------------- |
| `CommandLine` / `command`          | Shell command, working directory and output |
| File path / native location        | Read card with available output             |
| Write arguments                    | Write card with supplied content            |
| Edit/replace arguments or ACP diff | Edit card with old/new text or diff         |
| Query / pattern                    | Search card                                 |
| `invoke_subagent` / `send_message` | Subagent description, result and child link |
| Unrecognized tool                  | Plain text preserving input/output          |

Partial ACP tool updates are merged before rendering. Every streamed update replaces the full item using a stable ID. Running tools receive a completed, canceled or failed terminal snapshot before the turn ends. Missing failure detail gets a non-empty diagnostic.

## History and subagents

Persistence stores the native session ID. On replay, native history arrives before `session.ready`; live echoed user chunks are suppressed because Paseo already received the user message with its client ID. Skip-history loads suppress both timeline rows and historical child discovery. A notification queue is drained before readiness and turn completion.

Child `conversationId` values are validated and translated to provider-scoped child IDs. The plugin polls the corresponding `.system_generated/logs/transcript.jsonl`, retaining incomplete UTF-8 records between reads. Realpath containment prevents reading outside Brain. Each row has stable item IDs based on its step index; thoughts, text and tool calls become native timeline items. Planner tool calls first appear as running cards. Subsequent `GENERIC` rows complete or fail those cards in FIFO order, preserving call IDs and attaching output. Tool results do not become assistant bubbles. System/checkpoint metadata is ignored and native error messages become error items. This read-only view depends on agy preserving the positional result ordering. Native child control, recursive discovery and Artifacts UI are not provided.

## Permissions, usage and lifetime

Permission events include the native title, input, detail card and available actions. An action must match both the supplied ID and allow/deny intent. No match cancels the native request. Malformed permission fields are safely rejected with a canceled outcome before any approval is queued. Interrupt, close and runtime failure resolve outstanding permissions as canceled. `clearPendingPermissions` does the same before handling a new prompt; it does not make an active session accept a second turn.

Usage notifications require finite nonnegative used tokens and a positive context size. Only USD costs populate `totalCostUsd`. Prompt token counters are merged with the latest context usage and emitted before turn completion.

Control RPCs time out after 30 seconds. Prompt generation remains open until completion, interruption or process failure. Shutdown first ends stdin, then escalates process termination with a bounded wait. Automatic idle cleanup is vetoed because native background tasks cannot be safely queried. Explicit close releases the process and all Brain polling; the saved conversation remains resumable.
