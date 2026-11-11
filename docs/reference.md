# Reference

For installation and your first request, start with the [installation guide](../README.md#install). For source layout and verification, see [development](development.md).

## Launch and data directories

Plugin `agy-acp` registers provider `agy`. The default command is `agy acp`. Paseo resolves the executable, argument prefix and launch environment; session environment overrides are applied on top. Session MCP servers are translated into ACP stdio, HTTP or SSE configurations; non-empty session system prompts are forwarded via `_meta.antigravity.systemPrompt.append`.

| Setting                            | Behavior                                                                                                                       |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Command and argument overrides     | Use Paseo's provider launch configuration to choose an executable and argument prefix.                                         |
| `HOME`                             | Session environment takes precedence over launch environment, then the host home directory.                                    |
| `--gemini-dir`, `--gemini_dir`     | Override the Gemini directory; relative paths resolve against the realpath of the session's `cwd`. Default is `$HOME/.gemini`. |
| `--app-data-dir`, `--app_data_dir` | Override the application directory; relative paths resolve under the resolved Gemini directory. Default is `antigravity-cli`.  |

The resolved native directory is used for temporary artifact cleanup, Brain tracking and quota account identity. Flag-like argument values (starting with `-` without `=`) are ignored so misplaced flags cannot become directory paths. Other vendor directory overrides are not interpreted. See [subscription quota](#subscription-quota) for agy print-mode restrictions on directory flags.

### Availability and catalog discovery

Provider status runs the executable with `--version`, with a five-second timeout, requires a parseable stable version of at least 1.5.3 (pre-release `1.5.3-*` builds below 1.5.3 are rejected), and caches successful checks for 60 seconds per launch key (bounded to 100 entries). Catalog identity hashes the command, arguments and sorted environment using SHA-256; environment values are not included in plain-text cache keys. Within a connection, catalog requests for the same working directory share the first successful probe (bounded to 100 directories per connection); failed probes are evicted so subsequent requests retry, and failed probes do not replace remembered usage launches.

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

Model, mode and thinking controls derive from native `configOptions`, enriched with Paseo mode visuals and an unattended **Full Access** (`full-access`) mode. Selecting `full-access` launches `agy acp` with `--dangerously-skip-permissions` (hot-reloading the session via `session/load` when toggled on an open session), sets the native mode to `accept-edits` when advertised (falling back to `default`), and auto-approves remaining `'tool'`, `'mode'` and `'plan'` permission requests using `allow_once` (`'question'` prompts always require user selection); if a native `config_option_update` switches the mode to `'plan'`, `fullAccess` is cleared automatically, and switching back to another mode relaunches without `--dangerously-skip-permissions` and restores normal prompts. Preapproved MCP grants in `config.toolPolicy.preapproved` are also auto-approved with `allow_once` for both direct MCP tool names (`mcp__<server>__<tool>`, `<server>:<tool>`, and `default_api:mcp__<server>__<tool>`) and `call_mcp_tool` (`default_api:call_mcp_tool`) invocations. Legacy model metadata constrains effort variants to each model's supported choices; fixed-effort models hide the thinking selector. Opening a session changes configuration only when a requested value differs from native state. Effective configuration is published before `session.ready`.

The provider waits up to one second for the current native session's initial `available_commands_update` before readiness so draft and live composers can query fresh commands. Command lists are not cached across sessions. An empty list also resolves the wait; close or failure releases it. If the deadline expires, readiness proceeds without an initial list. Changed command lists from later updates are forwarded; duplicate lists within a session are suppressed.

The connection negotiates these capabilities with Paseo and validates incoming operations against the negotiated set: `prompt.message`, `prompt.command`, `prompt.image`, `session.configure`, `session.list`, `session.persistence`, `session.can_stop_runtime`, `permission`, `permission.tool_policy`, and `session.subsession`.

## Attachments and tool cards

Non-image attachments become text containing supplied paths, titles, external resource links (`title` + `url`), bodies or review comments. Uploaded files are referenced by daemon-side path; the plugin does not silently read arbitrary file contents. agy can use its own file tools subject to native permissions.

| Native arguments or content                                | Paseo card                                             |
| ---------------------------------------------------------- | ------------------------------------------------------ |
| `CommandLine` / `command`                                  | Shell command, working directory and output.           |
| File path / native location                                | Read card with available output.                       |
| `read_url_content` / `kind: 'fetch'` (`Url` / `url`)       | Fetch card with target URL and retrieved content.      |
| Write arguments (`ArtifactMetadata.RequestFeedback`: plan) | Plan card when feedback is requested, otherwise Write. |
| Edit/replace arguments or ACP diff                         | Edit card with old/new text or a diff.                 |
| Query / pattern                                            | Search card.                                           |
| `invoke_subagent` / `send_message`                         | Subagent description, result and child link.           |
| `ask_question`                                             | Plain-text formatted question options and answer.      |
| Unrecognized tool                                          | Plain text preserving input/output.                    |

Partial ACP tool updates are merged into full snapshots using stable item IDs. Denied tool permissions mark stranded tool cards `canceled` instead of `completed`; other running tools receive a completed, canceled or failed terminal snapshot before the turn ends. Missing failure detail gets a non-empty diagnostic.

## Persistence, history and subagents

Persistence stores the native session ID (`version: 1`). Replayed native history (`history: 'replay'`) uses `session/load` and arrives before `session.ready`. Skip-history loads (`history: 'skip'`) use `session/resume` when advertised in `sessionCapabilities.resume` (falling back to `session/load` while suppressing timeline rows and historical child discovery). Live echoed user chunks are suppressed because Paseo already received the user message with its client ID. After a turn enters Reactive Wakeup, `agy` replays those post-RPC wakeup steps on the next `session/prompt` before emitting that turn's `user_message_chunk`; `wakeup.pendingReplay` / `suppressEcho` suppresses those replayed chunks until the new `user_message_chunk` boundary arrives. A notification queue is drained before readiness and turn completion.

### Brain state and transcript projection

**Brain** refers to `agy`'s per-conversation local directory (`<appDataDir>/brain/<nativeSessionId>/`), whose `.system_generated/` subdirectory stores structured transcripts, inbox messages and subagent descriptors. **Reactive Wakeup** refers to `agy`'s asynchronous continuation mechanism: when a background task finishes, a timer fires, or a subagent sends a message _after_ the initial foreground `session/prompt` RPC has already returned, `agy` resumes model execution in the background and appends new steps to Brain without sending foreground ACP notifications.

Child `conversationId` values are validated and translated to provider-scoped child IDs (`<sessionId>:child:<nativeChildId>`). The plugin reads `brain/<id>/.system_generated/logs/transcript_full.jsonl` (which preserves native JSON tool arguments, full untruncated text, and per-step token counters), retaining incomplete UTF-8 records between reads, along with structured state files under `brain/<id>/.system_generated/subagents/*.json` and `brain/<id>/.system_generated/messages/` (`*.json` and `read.json`). Realpath containment (including `lstat` checks rejecting symlink leaves and symlinked `appDataDir` roots) prevents reading or deleting outside Brain. Message identities use the native step index. Tool call IDs are correlated via `sourceMetadata.tool.toolCall.id` / `stepIndex` in `.system_generated/messages/*.json` and `spawnStepIndex` in `.system_generated/subagents/*.json`, with positional fallback within a prompt or history replay.

An ACP prompt response can precede native background work and Reactive Wakeup responses. The plugin anchors each prompt to its new `USER_INPUT` and splits stream ownership by phase: while the foreground `session/prompt` RPC is running (`foregroundIdleReached === false`), ACP owns text, thought and tool streaming while Brain only enriches completed foreground tools (such as omitted `invoke_subagent` child IDs and `manage_subagents` cancellations). Once the initial foreground `PLANNER_RESPONSE` completes (`foregroundIdleReached === true`) and a subsequent Reactive Wakeup `SYSTEM_MESSAGE` arrives (`wokeUp === true`), Brain takes over full projection for the remainder of the turn until all structured subagent states (`subagents/*.json`), inbox messages (`messages/read.json`), and background tasks settle.

Interactive `ask_question` prompts follow a two-path architecture because `agy` handles them differently across phases:

- **Foreground path**: During an active `session/prompt` RPC, `agy` sends a JSON-RPC `elicitation/create` request over stdio. Because `agy`'s multi-select item schema (`items: { anyOf }` without `type: 'string'`) fails ACP SDK 1.9's strict `CreateElicitationRequest` validation, the stdio `TransformStream` intercepts `elicitation/create`, surfaces a Paseo `'question'` permission card (`QuestionCard`), and translates the user's selected option IDs or custom text back into `{ action: 'accept' | 'decline' | 'cancel', content }`.
- **Woken-turn path**: During a post-RPC Reactive Wakeup, `agy` cannot send `elicitation/create` over an idle RPC stream; instead it appends a `PLANNER_RESPONSE` containing an `ask_question` tool call to `transcript_full.jsonl` without a settling `GENERIC` row. `settleBrainTurn` detects this unsettled tail call, surfaces the Paseo `'question'` permission card, and—once answered—drives a synthetic continuation `session/prompt` (`formatQuestionPromptAnswer`), pre-staging the answer (`preStagedQuestion` / `handledElicitations`) so any replayed `elicitation/create` during that continuation reuses the user's answer without prompting twice.

When the ACP turn streamed model output and the session's Brain directory exists, the plugin waits up to 2 seconds for agy to flush the turn's `USER_INPUT` row; native commands that write no transcript row, and sessions without a Brain directory, complete immediately. A missing transcript follows the same bounded waiting policy when a known retry is unresolved; otherwise it completes the turn rather than failing it. Background tasks, subagents that have not settled (finished subagents stay `SUBAGENT_STATE_ALIVE` until killed) and pending approvals may keep a turn open indefinitely (interrupt stops them). Active woken-turn steps (pending tools and post-wakeup or post-tool model generation, including context compaction) wait up to 15 minutes per step (`wokenStallTimeoutMs`), while any other wait ends after 120 seconds (`stallTimeoutMs`) without transcript or inbox activity: when a stall timeout expires, the turn fails with an error item and interrupts the native session instead of hanging on an unrecognized final row.

Retry warnings are recognized only for the observed `ERROR_MESSAGE` / `DONE` system forms: the exact stream-interruption notice, `API error (attempt N): ...` with a positive attempt number, and the exact empty-model-output validation notice. Unknown errors and explicit `ERROR` statuses still fail with their original message. Brain has no structured retry-exhaustion event, so a retry gets a bounded 15-minute silent window after its latest distinct warning step, matching the existing silent woken-model window. Replaying that step does not extend the deadline. Root settling, child watchers and runtime-work checks share this retry policy; the ordinary 120-second stall policy remains separate. Genuine later warnings refresh the window, and exhaustion preserves the latest underlying error. A completed model response, a new tool-bearing response or a correlated tool execution/result releases retry state; unrelated transcript rows do not. The root retry clock is not run while its foreground ACP prompt RPC is still in flight. The 15-minute value is a local resource policy, not proof of native exhaustion or a promise that silent generation can never exceed it. Tracked background processes, standing daemons and active descendants are not stopped merely because the model retry timed out; interruption still checks and stops their owner where required.

A `PLANNER_RESPONSE` with `status: DONE` can legitimately end without assistant text or tools. Completion uses its status and outstanding tools/messages/children, never the presence of prose. A trailing retry warning or an incomplete response still prevents completion; explicit cancellation remains authoritative. Brain does not expose a separate child turn-outcome marker, so an as-yet unwritten later error cannot be distinguished from a valid quiet ending by inspecting thinking text. If a new response supersedes tool calls that never logged a result, their visible cards are closed as canceled rather than falsely completed.

Subagent lifecycle is tracked directly from `.system_generated/subagents/*.json` (only `SUBAGENT_STATE_ALIVE` counts as active; `SUBAGENT_STATE_KILLED` cancels the watcher; other states are inactive) and `.system_generated/messages/read.json` across the session and its owned descendants. Nested subagents spawned by a child are linked under their direct parent subsession ID (`<root>:child:<childId>`). An outstanding child's terminal failure fails the waiting turn rather than leaving it running. Only a header-anchored restart notice from the native `system` sender clears tracked background tasks and calls still awaiting a result; quoted notice text in child reports does not. A new `PLANNER_RESPONSE` also drops calls of the previous response that never logged a result, since child transcripts have no `USER_INPUT` to reset them.

| Transcript row     | Projection                                                                                                                                                                                                                                                                                                                                                        |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `USER_INPUT`       | User message.                                                                                                                                                                                                                                                                                                                                                     |
| `PLANNER_RESPONSE` | Thoughts, assistant text and initially running tool cards.                                                                                                                                                                                                                                                                                                        |
| `GENERIC`          | Match pending tool by `stepIndex` first, then first non-`ask_question` pending call, then FIFO. `RUNNING`/`WAITING` rows register background task IDs (`IsDaemon: true` schedules are tracked separately so idle turns settle); `manage_task` `kill` clears the killed task ID. `ERROR` fails, `CANCELED`/`INTERRUPTED` cancel, other terminal statuses complete. |
| `SYSTEM_MESSAGE`   | Anchor `sender=<id>` headers to match background task results, scheduled timer triggers and child notifications; preserve subsequent wakeup responses.                                                                                                                                                                                                            |
| `CHECKPOINT`       | Shift pending tool `stepIndex` references (`>= checkpoint` by `+1`). A `DONE` checkpoint emits a native completed compaction item with stable session/step identity; other statuses emit no compaction item.                                                                                                                                                      |
| `ERROR_MESSAGE`    | Error item.                                                                                                                                                                                                                                                                                                                                                       |

Root history replay keeps all ACP snapshots and their relative order. Checkpoints are inserted only into gaps proved by native call IDs or unique, complete, same-type text that appears in consistent order in both sources. Identity evidence belongs to each projected item: a pending tool records whether its ID came from that tool's native ID or its corresponding step metadata, and each terminal result inherits that source. An ID appearing on another tool cannot upgrade a positional fallback into an anchor; generated-looking strings are not blacklisted when actually provided natively. Ordinal tool-ID fallback is not a history anchor. Fragmented or repeated text, conflicting native IDs, missing anchors, or ACP-only items in a gap do not justify a position. Unlocated checkpoints produce one standard info notification per checkpoint per replay: history contains compaction, but its exact position is unknown. These notices omit a timestamp rather than imply that compaction happened at the end. Other independently proved gaps still retain native compaction markers and their original timestamps. This is a limitation of AGY's missing cross-source identity, not exact ordering parity with Codex.

ACP tool results and errors take precedence when provided. Raw-output and content presence follow each channel's latest explicit update: running null raw output and empty content arrays are placeholders, while explicit terminal empty/null results remain authoritative. Replacing running content with an empty array cannot leave an earlier content-present flag stuck on, and an update to content does not erase a separately provided raw result. Brain can fill missing results using the original ACP parameters, or restore missing child ownership metadata, without replacing an ACP terminal outcome. Brain-only completed tools retain their existing recovery path; histories with no completed checkpoints keep their previous replay behavior.

Completed compaction items use the standard Paseo renderer in both Brain projection and history replay, preserving the native timestamp. No loading state, manual/automatic trigger or post-compaction occupancy is inferred from checkpoint counters. Foreground root checkpoints are projected during existing post-RPC settling (including command-only transcripts); child watchers and Reactive Wakeup use their existing readers. Root RPC cancellation or failure currently skips settling, so checkpoints on those paths become visible on history replay rather than immediately. Live foreground observation and canceled/error final-drain parity are separate follow-up work.

agy appends each step once, when it reaches `RUNNING`, `WAITING` or `DONE` (or fails), and occasionally rewrites the whole file in place (`O_TRUNC`, same inode). The reader detects such rewrites by size, by re-checking the first bytes and the bytes before its offset, and when mid-`O_TRUNC` reads encounter corrupt partial lines, then resets to byte `0` and re-projects the file; `StepDedupe.fresh` hashes `(type, status, content, thinking, tool_calls)` with SHA-1 so a step is re-emitted only when its status or payload actually changed. For CHECKPOINT, only the first observed DONE per retained step identity emits completion; summary changes and later status regressions do not complete it again. A proven snapshot deletion or replacement by another event type releases that identity for reuse. Rows in `PENDING`, `QUEUED` or `GENERATING` can only come from a rewrite and are always superseded by a later row for the same step, so they are ignored.

An empty or torn rewrite observation does not delete delivered-step identities. Every complete JSONL record is immediately available to live, cancellation and history readers, even when the next record is partial. `reset` tells projectors to rebuild after a cursor reset; a separate `snapshotStepIndices` set authorizes dedupe pruning only after a nonempty replacement has no partial tail. The reader accumulates only step indices across partial reads, resetting that candidate set on another truncation or file replacement. Missing indices are then removed from the dedupe map, and changed or subsequently reused indices remain visible. The map retains one digest per observed step, not transcript bodies or every historical revision; an empty file retains the preceding snapshot's identities. A new watcher/session owns a new map. AGY supplies no generation or rewrite-commit marker: clearing a file and restoring identical rows within the same watcher is indistinguishable from rewriting history and does not create new events. A newline-complete nonempty prefix is still treated as a replacement snapshot; a writer pausing between complete records cannot be distinguished from deletion without an upstream commit marker.

Tool results do not become assistant bubbles. Child watchers emit `session.ready` immediately after `session.opened`, emit `session.turn(started)` lazily on their first active step, and emit completed or failed turn events when settled; successful native `manage_subagents` cancellation stops the affected watchers and emits canceled events. New native child turns restart lifecycle tracking. Cancellation (historical or live) first drains the child rows already written, so the child timeline is complete before its canceled turn, and finishes before the parent turn or session readiness is reported. Canceled children stay registered: they are never reopened and are closed with their parent. A later `send_message` to a child stopped by interruption resumes watching the same child session without replaying its history, after the previous watcher has drained; killed children are not resumed. Independent child control and Artifacts panels are unsupported.

## Permissions and runtime lifecycle

Permission events include the native title, input, detail card, classified `kind` (`question`, `mode`, `plan`, or `tool`), and available actions. An action must match both the supplied ID and allow/deny intent (preferring `allow_once` / `reject_once` when no action ID is specified); no match cancels the request. A `deny` response marks the corresponding tool call ID as denied (converting live ACP `failed` updates and stranded tool cards to `canceled` without error, and settling `end_turn` immediately whether `agy` flushes the denied `GENERIC` `ERROR` row or leaves it unflushed until the next turn), and with `interrupt: true` also interrupts the active turn. Malformed fields, mismatched native session IDs, and requests during close or after runtime failure receive a canceled outcome before any approval is queued. Notifications are also checked against the current native ID, including buffered notifications flushed after opening. Interrupt, close and runtime failure resolve outstanding permissions as canceled. `clearPendingPermissions` does the same before handling a new prompt; it does not permit a second concurrent turn.

Interrupt always sends `session/cancel` to stop foreground generation. When no background tasks, unread inbox wakeups, or active subagents exist (standing `IsDaemon: true` schedules are excluded so foreground interrupts do not kill them), `session/cancel` completes in place without restarting the ACP process. When background work or subagents are active, interrupt also stops the session's ACP process and its descendant process groups, then reloads the same native session with its model, thinking level, mode and launch configuration preserved, because agy 1.5.3 leaves background tasks alive after `session/cancel`. Conversation history is retained; a reload failure requires reopening the session.

Control RPCs time out after 30 seconds. Prompt generation remains open until completion, interruption or process failure. On Unix, shutdown snapshots the ACP process's descendant process groups before closing `stdin`, waits up to `500 ms` for graceful exit on EOF, then refreshes the snapshot and sends `SIGKILL` to tracked descendant process groups, followed by `SIGKILL` to the direct child at `2,000 ms` and a hard resolution deadline at `2,500 ms`. The snapshot uses `ps` (procps/BSD, with a BusyBox fallback); where `ps` is unavailable, shutdown still terminates agy's own process group and never fails because of it. Explicit close (including a close arriving while `session.open` is still in flight) stops Brain polling and terminates the session's ACP process while preserving imported and prompted conversations (unless `config.persist === false`, which removes temporary artifacts on close). Paseo's idle runtime check (`session.can_stop_runtime`) returns `true` only when no foreground turn, pending approval, running subagent watcher, active background task, or standing `IsDaemon` schedule is present.

Protocol disconnection drains `stderr` and exit status before closing the readable stream, then terminates the child even if the process is still alive. Provider connection shutdown awaits all in-flight operations, terminates all active transports, and waits for shutdown to finish.

Steering, rewind, archive operations and structured output are unsupported.

## Context window occupancy

Paseo's context meter needs both `contextWindowUsedTokens` and a positive `contextWindowMaxTokens`. Valid native `usage_update` notifications take precedence during the foreground phase; later Reactive Wakeup planner snapshots can update occupancy while retaining the same model's known native capacity; used tokens must be finite and nonnegative. Only USD costs populate `totalCostUsd`.

agy prompt responses report cumulative token counters across internal model steps, whereas `transcript_full.jsonl` records exact per-step `input_tokens`, `cache_read_tokens` and `output_tokens` on each `PLANNER_RESPONSE`. When no valid foreground `usage_update` arrives, or later Reactive Wakeup steps execute, the plugin computes context window occupancy from the latest reliable `PLANNER_RESPONSE` usage snapshot in `transcript_full.jsonl`:

```text
usedTokens = lastStep.inputTokens + lastStep.cachedReadTokens + lastStep.cachedWriteTokens + lastStep.outputTokens
```

`cachedWriteTokens` is taken from the step's `cache_write_tokens` when agy records it and is otherwise zero; agy 1.5.3 records only the first three counters. Per-step and cumulative ACP counters are never mixed in one sum. When a turn concludes with Reactive Wakeup steps, its wakeup token totals are recorded in `carriedWakeup` and subtracted from `agy`'s cumulative ACP prompt counters on the next turn so woken tokens are not double-counted.

When no reliable step-level counters are available, context occupancy stays unchanged or unknown; cumulative ACP prompt counters never substitute for context occupancy. Cancellation and prompt errors recover only snapshots after the current turn boundary. Restoration can recover the latest historical snapshot when native context is absent. Cache-only model steps with positive usage are valid snapshots; invalid and all-zero placeholder counters are ignored. A native `usage_update` explicitly reporting zero remains valid. Snapshot-only updates never replace turn consumption or pending wakeup deductions. Restoration publishes context after applying requested model configuration.

| Model                        | Fallback capacity |
| ---------------------------- | ----------------- |
| IDs beginning with `gpt-oss` | 131,072 tokens.   |
| Other models                 | 256,000 tokens.   |

Snapshot recovery reads complete records backwards in 64 KiB blocks, stops at this turn's boundary, and retains at most one JSONL record (8 MiB limit). If no snapshot exists, a historical restore can still scan the full file; it does not retain the full transcript. Cancel/error recovery also records the highest observed complete step so a later turn cannot claim canceled history as its own.

ACP `session/cancel` is a notification, not a server acknowledgement. Recovery waits for local interruption/relaunch work and drains pending ACP notifications after the original prompt resolves or rejects, then reads the complete records currently available. A runtime that writes Brain after those observable boundaries cannot be guaranteed to have flushed: no sleep or invented acknowledgement is used. Later history restoration can recover late counters without charging them to a new turn.

Prompt token counters are merged with context usage and emitted before turn completion. Context occupancy and subscription quota are separate measurements.

## Subscription quota

Usage Source `agy` supplies the Composer context popover and `paseo usage ls --json`. Add `--refresh` to bypass the host's cached report.

### Account discovery

Global discovery considers observed account directories (bounded to 100 entries) and the default data directory. Session discovery serves provider IDs `agy` and `antigravity`, resolving the directory using the session environment and a matching remembered launch. Paseo's usage scope has no launch command; if several custom launches match the same environment, the latest observation wins.

Account keys are SHA-256 hashes of data directories. A non-empty `antigravity-oauth-token` file or a remembered launch makes a directory discoverable, except that directories where a previously observed OAuth token file was removed (`hadLogin && !hasToken`) are skipped during discovery until signed in again. Labels use the latest login match across `cli.log` (always retained) plus up to ten most recent `log/cli-*.log` and `log/acp-server-*.log` files, reading at most 64 KiB from the tail of each file. Missing logs fall back to `Antigravity`. Token contents are not read or copied into usage inputs.

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
