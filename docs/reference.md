# Reference

For installation and your first request, start with the [installation guide](../README.md#install). For source layout and verification, see [development](development.md).

## Launch and data directories

Plugin `agy-acp` registers provider `agy`. The default command is `agy acp`. Paseo resolves the executable, argument prefix and launch environment; session environment overrides are applied on top. Session MCP servers are translated into ACP stdio, HTTP or SSE configurations; non-empty session system prompts are forwarded via `_meta.antigravity.systemPrompt.append`.

| Setting                            | Behavior                                                                                     |
| ---------------------------------- | -------------------------------------------------------------------------------------------- |
| Command and argument overrides     | Use Paseo's provider launch configuration to choose an executable and argument prefix.       |
| `HOME`                             | Session environment takes precedence over launch environment, then the host home directory.  |
| `--gemini-dir`, `--gemini_dir`     | Override the Gemini directory; default is `$HOME/.gemini`.                                   |
| `--app-data-dir`, `--app_data_dir` | Override the application directory; default is `antigravity-cli` under the Gemini directory. |
| Relative directory paths           | Resolve against the session's working directory.                                             |

The resolved native directory is used for temporary artifact cleanup, Brain tracking and quota account identity. Other vendor directory overrides are not interpreted. See [subscription quota](#subscription-quota) for agy print-mode restrictions on directory flags.

### Availability and catalog discovery

Provider status runs the executable with `--version`, with a five-second timeout, requires a parseable version of at least 1.5.3, and caches successful checks for 60 seconds per launch key. Catalog identity hashes the command, arguments and sorted environment using SHA-256; environment values are not included in plain-text cache keys. Within a connection, concurrent catalog requests for the same working directory share a promise. Failed probes may be retried and do not replace remembered usage launches.

### Temporary session cleanup

Catalog discovery opens an unprompted native session, reads its configuration, calls `session/close`, stops the ACP process, and removes that closed session's temporary artifacts. New sessions closed before their first prompt use the same cleanup. Imported and prompted conversations are retained.

Only the closed session's validated UUID is eligible for deletion:

| Artifact under the native data directory      | Cleanup                                              |
| --------------------------------------------- | ---------------------------------------------------- |
| `conversations/<id>.db`, `.db-wal`, `.db-shm` | Remove after `session/close` and process termination |
| `brain/<id>/`                                 | Remove that session's directory after process stop   |
| Shared summary databases and other sessions   | Retain                                               |

Cleanup refuses symlink targets and does not traverse symlink directories. Native close and filesystem cleanup are best-effort; errors do not discard an already-discovered catalog and may leave temporary artifacts.

## Models, modes, thinking and commands

Model, mode and thinking controls derive from native `configOptions`, enriched with Paseo mode visuals and an unattended **Full Access** (`full-access`) mode. Selecting `full-access` launches `agy acp` with `--dangerously-skip-permissions` (hot-reloading the session via `session/load` when toggled on an open session), sets the native mode to `accept-edits`, and auto-approves any remaining permission requests using `allow_once`; switching back to another mode relaunches without `--dangerously-skip-permissions` and restores normal prompts. Legacy model metadata constrains effort variants to each model's supported choices; fixed-effort models hide the thinking selector. Opening a session changes configuration only when a requested value differs from native state. Effective configuration is published before `session.ready`.

The provider waits up to one second for the current native session's initial `available_commands_update` before readiness so draft and live composers can query fresh commands. Command lists are not cached across sessions. An empty list also resolves the wait; close or failure releases it. If the deadline expires, readiness proceeds without an initial list. Changed command lists from later updates are forwarded; duplicate lists within a session are suppressed. Catalog probes wait up to 100 ms for commands before closing their temporary session.

The connection negotiates these capabilities with Paseo and validates incoming operations against the negotiated set: `prompt.message`, `prompt.command`, `prompt.image`, `session.configure`, `session.list`, `session.persistence`, `session.can_stop_runtime`, `permission`, and `session.subsession`.

## Attachments and tool cards

Non-image attachments become text containing supplied paths, titles, URLs, bodies or review comments. Uploaded files are referenced by daemon-side path; the plugin does not silently read arbitrary file contents. agy can use its own file tools subject to native permissions.

| Native arguments or content        | Paseo card                                   |
| ---------------------------------- | -------------------------------------------- |
| `CommandLine` / `command`          | Shell command, working directory and output. |
| File path / native location        | Read card with available output.             |
| Write arguments                    | Write card with supplied content.            |
| Edit/replace arguments or ACP diff | Edit card with old/new text or a diff.       |
| Query / pattern                    | Search card.                                 |
| `invoke_subagent` / `send_message` | Subagent description, result and child link. |
| Unrecognized tool                  | Plain text preserving input/output.          |

Partial ACP tool updates are merged into full snapshots using stable item IDs. Running tools receive a completed, canceled or failed terminal snapshot before the turn ends. Missing failure detail gets a non-empty diagnostic.

## Persistence, history and subagents

Persistence stores the native session ID. Replayed native history arrives before `session.ready`. Live echoed user chunks are suppressed because Paseo already received the user message with its client ID. Skip-history loads suppress timeline rows and historical child discovery. A notification queue is drained before readiness and turn completion.

### Brain state and transcript projection

Child `conversationId` values are validated and translated to provider-scoped child IDs. The plugin reads `brain/<id>/.system_generated/logs/transcript_full.jsonl` (which preserves native JSON tool arguments, full untruncated text, and per-step token counters), retaining incomplete UTF-8 records between reads, along with structured state files under `brain/<id>/.system_generated/subagents/*.json` and `brain/<id>/.system_generated/messages/` (`*.json` and `read.json`). Realpath containment prevents reading outside Brain. Message identities use the native step index. Tool call IDs are correlated via `sourceMetadata.tool.toolCall.id` / `stepIndex` in `.system_generated/messages/*.json` and `spawnStepIndex` in `.system_generated/subagents/*.json`, with positional fallback within a prompt or history replay.

An ACP prompt response can precede native background work and Reactive Wakeup responses. The plugin anchors each prompt to its new `USER_INPUT` and splits stream ownership by phase: while the foreground `session/prompt` RPC is running (`foregroundIdleReached === false`), ACP owns text, thought and tool streaming while Brain only enriches completed foreground tools (such as omitted `invoke_subagent` child IDs and `manage_subagents` cancellations). Once the initial foreground `PLANNER_RESPONSE` completes (`foregroundIdleReached === true`) and a subsequent Reactive Wakeup `SYSTEM_MESSAGE` arrives (`wokeUp === true`), Brain takes over full projection for the remainder of the turn until all structured subagent states (`subagents/*.json`), inbox messages (`messages/read.json`), and background tasks settle.

When the ACP turn streamed model output and the session's Brain directory exists, the plugin waits up to 2 seconds for agy to flush the turn's `USER_INPUT` row; native commands that write no transcript row, and sessions without a Brain directory, complete immediately. A missing transcript always completes the turn rather than failing it. Background tasks, subagents that have not settled (finished subagents stay `SUBAGENT_STATE_ALIVE` until killed), tools of a woken turn and pending approvals may keep a turn open indefinitely (interrupt stops them). Any other wait ends after 120 seconds without transcript or inbox activity: the turn completes with an error item instead of hanging on an unrecognized final row.

Subagent lifecycle is tracked directly from `.system_generated/subagents/*.json` (only `SUBAGENT_STATE_ALIVE` counts as active; `SUBAGENT_STATE_KILLED` cancels the watcher; other states are inactive) and `.system_generated/messages/read.json` across the session and its owned descendants, without creating recursive UI cards. An outstanding child's terminal failure fails the waiting turn rather than leaving it running. Only a restart notice from the native `system` sender clears tracked background tasks; quoted notice text in child reports does not.

| Transcript row     | Projection                                                                                                                                                                                                                                                            |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `USER_INPUT`       | User message.                                                                                                                                                                                                                                                         |
| `PLANNER_RESPONSE` | Thoughts, assistant text and initially running tool cards.                                                                                                                                                                                                            |
| `GENERIC`          | Consume one pending tool in FIFO order. `RUNNING`/`WAITING` rows are final for the log: a background task id is tracked until its result arrives, otherwise the tool stays running. `ERROR` fails, `CANCELED`/`INTERRUPTED` cancel, other terminal statuses complete. |
| `SYSTEM_MESSAGE`   | Match background results and child notifications; preserve subsequent wakeup responses.                                                                                                                                                                               |
| `CHECKPOINT`       | Ignore metadata.                                                                                                                                                                                                                                                      |
| `ERROR_MESSAGE`    | Error item.                                                                                                                                                                                                                                                           |

agy appends each step once, when it reaches `RUNNING`, `WAITING` or `DONE` (or fails), and occasionally rewrites the whole file in place (`O_TRUNC`, same inode). The reader detects such rewrites by size and by re-checking the first bytes and the bytes before its offset, then re-projects the file; a step is emitted again only when its type or status changed. Rows in `PENDING`, `QUEUED` or `GENERATING` can only come from a rewrite and are always superseded by a later row for the same step, so they are ignored.

Tool results do not become assistant bubbles. Child watchers emit completed or failed turn events, and successful native `manage_subagents` cancellation stops the affected watchers and emits canceled events. New native child turns restart lifecycle tracking. Cancellation (historical or live) first drains the child rows already written, so the child timeline is complete before its canceled turn, and finishes before the parent turn or session readiness is reported. Canceled children stay registered: they are never reopened and are closed with their parent. A later `send_message` to a child stopped by interruption resumes watching the same child session without replaying its history; killed children are not resumed. Independent child control, recursive card discovery and Artifacts panels are unsupported.

## Permissions and runtime lifecycle

Permission events include the native title, input, detail card and available actions. An action must match both the supplied ID and allow/deny intent; no match cancels the request. Malformed fields, mismatched native session IDs, and requests during close or after runtime failure receive a canceled outcome before any approval is queued. Notifications are also checked against the current native ID, including buffered notifications flushed after opening. Interrupt, close and runtime failure resolve outstanding permissions as canceled. `clearPendingPermissions` does the same before handling a new prompt; it does not permit a second concurrent turn.

Interrupt always sends `session/cancel` to stop foreground generation. When no background tasks, unread inbox wakeups, or active subagents exist, `session/cancel` completes in place without restarting the ACP process. When background work or subagents are active, interrupt also stops the session's ACP process and its descendant process groups, then reloads the same native session with its model, thinking level, mode and launch configuration preserved, because agy 1.5.3 leaves background tasks alive after `session/cancel`. Conversation history is retained; a reload failure requires reopening the session.

Control RPCs time out after 30 seconds. Prompt generation remains open until completion, interruption or process failure. On Unix, shutdown snapshots the ACP process's descendant process groups before ending stdin and refreshes the snapshot before sending `SIGTERM`/`SIGKILL` so process groups spawned during the graceful shutdown window are also terminated. The snapshot uses `ps` (procps/BSD, with a BusyBox fallback); where `ps` is unavailable, shutdown still terminates agy's own process group and never fails because of it. Explicit close stops Brain polling and terminates the session's ACP process while preserving imported and prompted conversations. Paseo's automatic idle cleanup is vetoed via `session.can_stop_runtime: false` so active sessions are not stopped externally while background work may be running.

Protocol disconnection terminates the child even if the process is still alive. Provider connection shutdown terminates all active transports and waits for shutdown to finish.

Steering, rewind, archive operations and structured output are unsupported.

## Context window occupancy

Paseo's context meter needs both `contextWindowUsedTokens` and a positive `contextWindowMaxTokens`. Valid native `usage_update` notifications take precedence for the current turn; used tokens must be finite and nonnegative. Only USD costs populate `totalCostUsd`.

agy prompt responses report cumulative token counters across internal model steps, whereas `transcript_full.jsonl` records exact per-step `input_tokens`, `cache_read_tokens` and `output_tokens` on each `PLANNER_RESPONSE`. When no valid `usage_update` notification arrives during a turn, the plugin computes context window occupancy from the final `PLANNER_RESPONSE` step in `transcript_full.jsonl`:

```text
usedTokens = lastStep.inputTokens + lastStep.cachedReadTokens + lastStep.cachedWriteTokens + lastStep.outputTokens
```

`cachedWriteTokens` is taken from the step's `cache_write_tokens` when agy records it and is otherwise zero; agy 1.5.3 records only the first three counters. Per-step and cumulative ACP counters are never mixed in one sum.

When no step-level counters are available in `transcript_full.jsonl`, it falls back to `inputTokens + cachedReadTokens + cachedWriteTokens + outputTokens` (or `totalTokens` if zero) from the ACP prompt response. Invalid counters are treated as zero.

| Model                        | Fallback capacity |
| ---------------------------- | ----------------- |
| IDs beginning with `gpt-oss` | 131,072 tokens.   |
| Other models                 | 256,000 tokens.   |

Prompt token counters are merged with context usage and emitted before turn completion. Context occupancy and subscription quota are separate measurements.

## Subscription quota

Usage Source `agy` supplies the Composer context popover and `paseo usage ls --json`. Add `--refresh` to bypass the host's cached report.

### Account discovery

Global discovery considers observed account directories and the default data directory. Session discovery only serves provider `agy`, resolving the directory using the session environment and a matching remembered launch. Paseo's usage scope has no launch command; if several custom launches match the same environment, the latest observation wins.

Account keys are SHA-256 hashes of data directories. A non-empty `antigravity-oauth-token` file or a remembered launch makes a directory discoverable. Labels use the latest login match in `cli.log` and up to ten recent `log/cli-*.log` files, reading at most 64 KiB per file. Missing logs fall back to `Antigravity`. Token contents are not read or copied into usage inputs.

Successful catalog discovery and session opening remember `_meta.antigravity.tier.name` as `planLabel`. The label is absent until observed and can be stale after an account change.

### Query execution

Fetching runs the launch executable with `-p /usage --output-format json` and a 15-second timeout. The `acp` subcommand and transport flags are removed; explicit directory arguments, the remembered working directory and launch/session environment overrides are preserved. If the remembered workspace no longer exists, execution falls back to the host home directory only when the resolved account directory stays identical and the executable is not a relative path. Cwd-dependent directory flags produce a diagnostic instead of silently switching accounts. Other launch arguments remain unchanged. No directory flags are added for default accounts. Current agy print mode rejects ACP directory flags, so such launches return a command error instead of querying a different account.

The CLI may contact its quota service but does not start a model turn. The plugin does not modify login credentials.

### Windows and outcomes

Each validated `command.data.groups` bucket becomes a scoped window:

| Native field         | Reported behavior                                                                  |
| -------------------- | ---------------------------------------------------------------------------------- |
| Group name           | Scope label and normalized ID, such as `gemini_models` or `claude_and_gpt_models`. |
| `window: 5h`         | Five-hour window (18,000 seconds).                                                 |
| `window: weekly`     | Weekly window (604,800 seconds).                                                   |
| Unknown window       | Preserve native window identity and labels without inventing a duration.           |
| `remaining_fraction` | Convert to a rounded, clamped used percentage; absent fractions stay unknown.      |
| `reset_time`         | Reset timestamp, or no timestamp when absent.                                      |

Windows are included in the default summary. Usage above 90% has a danger tone; 70–90% has a warning tone; lower known usage is OK and unknown usage uses the default tone.

| Outcome                                             | Usage report                                                      |
| --------------------------------------------------- | ----------------------------------------------------------------- |
| Valid quota buckets                                 | `available`, with windows and the observed plan label when known. |
| No buckets                                          | `unavailable` with `no_quota`.                                    |
| Command, authentication, timeout or parsing failure | `error` with a diagnostic.                                        |
| Previously observed login file disappears           | Raise a login-store error.                                        |
| Remembered launch has never exposed a token file    | Let the CLI try its OS-backed login store.                        |
