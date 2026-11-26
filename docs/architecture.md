# Architecture

Internal design and invariants of `paseo-agy-acp` targeting `agy >= 1.5.16` and `@getpaseo/plugin` `0.16.x`. See [reference](reference.md) for operator contracts and [development](development.md) for setup, source layout, and releases.

## Data planes and containment

The plugin coordinates three local `agy` planes:

1. **ACP JSON-RPC over stdio (`server/transport.ts`):** Live session RPCs (`initialize`, `session/new`, `session/load`, `session/resume`, `session/list`, `session/close`, `session/prompt`, `session/cancel`), config options, streaming chunks, tool snapshots, permissions, and `elicitation/create`.
2. **Brain directory (`<appDataDir>/brain/<nativeId>/.system_generated/`):** JSONL transcript (`logs/transcript_full.jsonl`), inbox (`messages/*.json`, `read.json`), and subagents (`subagents/*.json`) for post-RPC Reactive Wakeup, background settlement, result/child recovery, and per-step token snapshots.
3. **Conversation SQLite DB (`<appDataDir>/conversations/<nativeId>.db`):** Read-only source for server compaction windows (`gen_metadata`) and hidden upstream API errors (`steps` `step_type = 17` / `refusal`).

All filesystem and SQLite paths enforce realpath containment (`isContainedIn`, `resolveBrainPath`) with `lstat` checks rejecting symlink leaves and symlinked `appDataDir` roots (`BrainEscapeError`).

## Identity model

- `sessionId` — Paseo root session ID; `nativeId` — `agy` UUIDv4 (`sessionIdPattern`).
- `<sessionId>:child:<nativeChildId>` — Provider-scoped read-only subsession ID; grandchildren link under their direct parent subsession ID.
- `<childUUID>:<origCallId>` — Child-forwarded `toolCallId` emitted by `agy >= 1.5.16` on the root ACP stream for subagents and `run_workflow`.
- **Launch/pool key (`server/preflight.ts`, `server/runtime.ts`):** Shared `hashLaunchKey` hashes `(command, args, sorted merged env, executable realpath/dev/ino/size/mtimeNs)` with SHA-256 — resolving case-insensitive `PATH`/`Path`, Windows `PATHEXT`, and default install locations without storing plain-text env values.

## Runtime pooling and process shutdown

- **`SharedControlRuntime` (`server/runtime.ts`):** Lazily starts one 5-minute idle-reaped (`IdleTimer`) `agy acp` process per connection for cwd-independent `catalog`/`sessions` probes (tracked via `TransportTracker`). Serializes concurrent `catalog` probes to avoid `models`/`tier` cross-contamination and retires on fingerprint change, exit, or `closeDisposableSession` failure. Cwd-dependent launches use one-shot runtimes.
- **`StandbyRuntimePool` (`server/runtime.ts`):** 4-slot LRU pool (5-minute `IdleTimer` reap, `TransportTracker`-tracked) keyed by `hashLaunchKey`. Warms `initialize` (+ `session/list`) in the background for exclusive 1:1 checkout via `acquireRuntime`. `warm()` and `recycle()` refuse `--dangerously-skip-permissions` and cwd-dependent launches. Closed unprompted sessions recycle only when ready, idle, zero-MCP, non-Full-Access, cwd-independent, and `closeDisposableSession` succeeds.
- **Availability cache (`server/preflight.ts`):** Coalesces concurrent `--version` probes (5 s timeout, stable `>= 1.5.16`) and caches up to 100 entries for 60 s by binary fingerprint.
- **Process-group shutdown (`server/transport.ts`):** On Unix, snapshots descendant process groups within a 200 ms `ps` budget (BusyBox fallback, broken-`ps` latch, 50 ms stdout flush on early child exit), closes `stdin` (300 ms graceful wait), refreshes and `SIGKILL`s descendant groups, `SIGKILL`s the child at 650 ms, and enforces a 1,100 ms hard deadline. Disconnects drain `stderr` and exit status before closing streams.

## Session state machine and epoch fencing (`server/session.ts`)

- **Lifecycle phases (`SessionPhase`):** Explicit `phase` (`buffering`, `loading`, `ready`, `reloading`, `closing`, `silenced`) plus monotonic `opened`, `replayHistory`, and `failureError`. Predicates (`isUnavailable`, `isReloading`, `isSuppressingReplay`, `assertAlive`) gate all incoming notifications, permissions, and requests. `acquireRuntime` unifies standby checkout and cold-start callback binding across initial open and reload. During `reloading`, `config_option_update` updates internal metadata without publishing transient state.
- **Per-turn `activeTurn`:** Groups turn-scoped state (turn ID, `running`, abort controller, completion guard, foreground ACP segments, insertion-ordered `Set<string>` of tool call IDs, `deniedToolCallIds`, `handledElicitations`, `wakeupQuestionDeclined`, `observation`, `turnEstimatedTokens`) into one nullable struct cleared to `null` after `terminalize()` in `finishTurnOnce`. Replay snapshots and IDs stay in a separate collector cleared by `replayTools()`.
- **Interrupt & recovery:** Because `agy >= 1.5.16` `session/cancel` (`ForceStopTree`) uses a 5 s grace period that can leave detached groups and `SUBAGENT_STATE_ALIVE` metadata behind:
  1. Fast path probes Brain within 200 ms (excluding standing `IsDaemon: true` daemons); if idle and no reload is in flight, sends `session/cancel` and awaits convergence within 350 ms.
  2. Otherwise drains child watchers (150 ms cap, force-canceling stragglers), sends 100 ms best-effort `session/cancel`, kills the process tree within Paseo's 2 s budget, and reloads asynchronously (`session/resume` / `session/load`) preserving model, thinking level, and `fullAccess`. Subsequent configure/prompt calls serialize behind recovery.
- **`RuntimeEpoch` (`server/brain.ts`):** Captured monotonically on retirement, reload, and persisted reopen (`afterStepIndex`, `preEpochReadIds`, `ignoredMessages` by signature, `ignoredSubagentIds` unless touched post-epoch) so pre-stop artifacts never poison later turns.
- **Idle runtime gate (`session.can_stop_runtime`):** Returns `false` while opening, loading, reloading, closing, control-active, turning, interrupting, holding pending permissions, tracking running child watchers (`subagents.hasRunningChildren()`), or while `hasActiveBackgroundWork(..., { includeDaemons: true })` sees post-epoch tasks, daemons, non-daemon cron schedules, unread wakeups, woken turns, unresolved retries, active descendants, or Brain read errors.

## Tool normalization, MCP, and child isolation

- **Normalization (`server/common.ts`):** `normalizeToolCandidate` prefers explicit raw `name` over `title`, strips `Run ` wrappers and `default_api:`, and feeds native lifecycle matching (`isNativeToolName`), `isAskQuestionName`, `classifyPlanModeTool`, and MCP resolution. `isForeignToolIdentity` prevents `mcp_*`, `call_mcp_tool`, and foreign `<ns>:<tool>` names from triggering `switch_mode` / `mode` / `plan`.
- **MCP (`server/mcp.ts`):** Matches only `call_mcp_tool` (`ServerName`/`ToolName` + aliases), unique configured `mcp_<server>_<tool>` prefixes, and built-in `mcp_paseo_<tool>` fallback when no configured server matches.
- **ACP v1 & display (`server/mapping.ts`):** `Timeline` tracks raw-input/output presence independently from content blocks so top-level `null`/omission preserves previous values. `stripToolSummary` removes top-level string `toolSummary` from display-only `unknown` cards and permission inputs without mutating raw replay snapshots.
- **Child-prefixed `<childUUID>:<callId>` isolation (`server/session.ts`):** Drops child-prefixed `tool_call`/`tool_call_update` on root live and during `session/load` (`SubagentRegistry` renders child timelines from Brain). Forwards child-prefixed permissions and elicitations only while `activeTurn` exists without mutating root `wakeupQuestionDeclined` or `handledElicitations`; cancels outside an active turn.
- **Elicitation patch (`server/transport.ts`, `server/questions.ts`):** Injects missing array item `type` on `agy`'s multi-select `elicitation/create` schemas before SDK validation and maps forms to/from `QuestionCard`s.

## Brain pipeline, transcript recovery, and reconciliation

### Incremental I/O (`server/brain-io.ts`)

- **`TranscriptReader`:** Splits complete lines on `0x0a` (`Buffer.indexOf(10)`), retaining split UTF-8 tails. Detects in-place `O_TRUNC` rewrites via size shrink, head/pre-offset probes, or corrupt partial lines, resetting to byte 0 (`reset: true`).
- **Atomic rollback:** Failed reads deliver zero rows and roll back cursor, remainder, probes, and pending `snapshotStepIndices` together.
- **`StepDedupe`:** SHA-1 digests `(type, source, status, content, thinking, tool_calls, error)` to skip unchanged replays. Emits `CHECKPOINT` completion once per retained index; `BrainNode.pollTranscript` handles reset, pruning of missing indices and retry clocks (`pruneRetrySteps`) after a complete non-empty replacement snapshot (`snapshotStepIndices`), and row ingestion. Ignores transient `PENDING`/`QUEUED`/`GENERATING` rows.
- **EOF tail scan:** Turn-end, cancel, and restore usage recovery reads backwards in 64 KiB blocks (8 MiB record cap), accepting a complete newline-less EOF JSON record without authorizing snapshot pruning.

### Projection, tasks, and reconciliation (`server/brain-projector.ts`, `server/brain-observer.ts`, `server/brain.ts`)

| Transcript row     | State transition                                                                                                              |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `USER_INPUT`       | Turn boundary anchor and user message.                                                                                        |
| `PLANNER_RESPONSE` | Thoughts, text, running tools, per-step `extractStepUsage`.                                                                   |
| `GENERIC`          | Matches pending tool (`stepIndex` -> non-`ask_question` -> FIFO); registers tasks/daemons/cron in `tasks`.                    |
| `SYSTEM_MESSAGE`   | Line-anchored `extractHeaderMessageSenders` matches completions/reports; skips pure `planexecute` (`countRowMessages === 0`). |
| `CHECKPOINT`       | Shifts pending `stepIndex >= checkpoint` by `+1`; emits compaction on `DONE` with `extractCheckpointPreTokens`.               |
| `ERROR_MESSAGE`    | Splits recognized retry warnings (`isRetryWarningMessage`) from terminal errors.                                              |

- **Unified `tasks` map (`TrackedTask`):** Combines blocking tasks, non-daemon `CronExpression` schedules (counting down `MaxIterations` ticks), and `IsDaemon: true` standing daemons so daemon `run_command` results update their tool cards while keeping `hasBackground()` (`!isDaemon`), `hasDaemons()` (`isDaemon`), and `TimerCondition` cancellation (`!isDaemon && !isCron`) distinct. `manage_task` `kill` clears the task.
- **Root observation (`BrainTurnObservation`, `ModelUsageObserver`):** Single reader from prompt start through settling; compares 4-field `StepUsage` directly, publishes reliable foreground context and checkpoints while buffering rows until ACP IDs/denials/elicitations drain. Transient I/O faults recover; validation/projection faults are terminal.
- **`ForegroundReconciler`:** Matches foreground ACP segments against Brain (tolerating added paragraph breaks) and tracks post-RPC ACP frames so intra-RPC work and silent trailing `DONE` rows are not double-billed as post-RPC Reactive Wakeup.
- **Wait & retry timers (`settleBrainTurn`, `watchBrain`):** 2 s `USER_INPUT` flush grace; 15 min `wokenStallTimeoutMs` and `RETRY_IDLE_TIMEOUT_MS` (anchored to `mtimeMs` on initial read, `Date.now()` live, preserved across `O_TRUNC` unless dropped/mutated); 120 s `stallTimeoutMs` with >10 s sleep-jump `lastActivity` reset.
- **Subagents & re-wakeup floor (`server/subagents.ts`, `server/brain-turns.ts`):** `SubagentRegistry` tracks `SUBAGENT_STATE_ALIVE` / `SUBAGENT_STATE_KILLED` (draining history before cancel). On `send_message` re-wakeup, `readTurnBoundaryBefore` (`TurnBoundaryCache`, 256 entries, keyed by `mtimeMs`/`size`/`lstat` dir signature; fail-closed on non-missing errors) advances the child's monotonic `TurnFloor` past prior-turn failures. Header-anchored `system` restart notices and new child `PLANNER_RESPONSE` rows clear abandoned calls.

## History replay alignment and SQLite inspection

- **Replay (`server/history.ts`):** Preserves ACP snapshots and order. Tools carry verified identity provenance (`tool_id` | `step_metadata` | `acp_input`); `alignReplayTools` skips `GENERIC` `INVALID` rows and aligns forked suffixes. `mergeCompactionHistory` inserts checkpoints only into gaps proved by verified tool IDs or unique normalized text; unlocated checkpoints emit one timestamp-less info notice. Supplied ACP outputs/errors take precedence over Brain recovery.
- **SQLite (`server/conversation-db.ts`):** `withConversationDb` enforces containment, rejects symlinks, verifies `fstat` inode parity, and opens read-only with a 50 ms busy timeout (fail-closed on missing tables/corruption).
  - `readConversationCompactionWindow`: Stat-cached on `.db`/`.db-wal`, scans `gen_metadata` by `idx DESC` above monotonic `genIdx`, breaking immediately on `maxTokens` while continuing past `estimatedTokens`-only rows so newer estimates pair with the active model's window without leaking older estimates across model switches.
  - `readConversationStepError` / `classifyStepError`: Diffs `steps` against `captureConversationStepBaseline` to classify trailing `step_type = 17` / `refusal` errors (`QUOTA_EXHAUSTED` with reset times / `parseGoDurationMs`, `INVALID_THINKING_SIGNATURE`, structured `agy` API errors).
