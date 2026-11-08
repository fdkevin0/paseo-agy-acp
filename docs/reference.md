# Reference

For installation and your first request, start with the [installation guide](../README.md#install). For source layout and verification, see [development](development.md).

## Launch and data directories

Plugin `agy-acp` registers provider `agy`. The default command is `agy acp`. Paseo resolves the executable, argument prefix and launch environment; session environment overrides are applied on top. Session MCP servers are translated into ACP stdio, HTTP or SSE configurations.

| Setting                            | Behavior                                                                                     |
| ---------------------------------- | -------------------------------------------------------------------------------------------- |
| Command and argument overrides     | Use Paseo's provider launch configuration to choose an executable and argument prefix.       |
| `HOME`                             | Session environment takes precedence over launch environment, then the host home directory.  |
| `--gemini-dir`, `--gemini_dir`     | Override the Gemini directory; default is `$HOME/.gemini`.                                   |
| `--app-data-dir`, `--app_data_dir` | Override the application directory; default is `antigravity-cli` under the Gemini directory. |
| Relative directory paths           | Resolve against the session's working directory.                                             |

The resolved native directory is used for temporary artifact cleanup, Brain tracking and quota account identity. Other vendor directory overrides are not interpreted. See [subscription quota](#subscription-quota) for agy print-mode restrictions on directory flags.

### Availability and catalog discovery

Provider status runs the executable with `--version`, with a five-second timeout, and requires a parseable version of at least 1.4.12. Catalog identity hashes the command, arguments and sorted environment using SHA-256; environment values are not included in plain-text cache keys. Within a connection, concurrent catalog requests for the same working directory share a promise. Failed probes may be retried and do not replace remembered usage launches.

### Temporary session cleanup

Catalog discovery opens an unprompted native session, reads its configuration, calls `session/close`, then stops the process. New sessions closed before their first prompt use the same cleanup. Imported and prompted conversations are retained.

Only the closed session's validated UUID is eligible for deletion:

| Artifact under the native data directory      | Cleanup                                                  |
| --------------------------------------------- | -------------------------------------------------------- |
| `conversations/<id>.db`, `.db-wal`, `.db-shm` | Remove after the process stops.                          |
| `brain/<id>/`                                 | Remove that session's directory after the process stops. |
| Shared summary databases and other sessions   | Retain.                                                  |

Cleanup refuses symlink targets and does not traverse symlink directories. Native close and filesystem cleanup are best-effort; errors do not discard an already-discovered catalog and may leave temporary artifacts.

## Models, modes, thinking and commands

Model, mode and thinking controls derive from native `configOptions`. Legacy model metadata constrains effort variants to each model's supported choices; fixed-effort models hide the thinking selector. Opening a session changes configuration only when a requested value differs from native state. Effective configuration is published before `session.ready`.

The provider waits up to one second for the initial `available_commands_update` before readiness so draft and live composers can query native commands. An empty list also resolves the wait; close or failure releases it. If the deadline expires, readiness proceeds without an initial list. Later updates are forwarded, but Paseo may have already cached the draft command list.

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

### Brain transcript projection

Child `conversationId` values are validated and translated to provider-scoped child IDs. The plugin polls the corresponding `brain/<id>/.system_generated/logs/transcript.jsonl`, retaining incomplete UTF-8 records between reads. Realpath containment prevents reading outside Brain. Items have stable identities based on the native step index.

| Transcript row                 | Projection                                                                              |
| ------------------------------ | --------------------------------------------------------------------------------------- |
| `USER_INPUT`                   | User message.                                                                           |
| `PLANNER_RESPONSE`             | Thoughts, assistant text and initially running tool cards.                              |
| `GENERIC`                      | Complete or fail pending tools in FIFO order, preserving call IDs and attaching output. |
| `SYSTEM_MESSAGE`, `CHECKPOINT` | Ignore metadata.                                                                        |
| `ERROR_MESSAGE`                | Error item.                                                                             |

Tool results do not become assistant bubbles. This read-only projection depends on agy preserving positional result ordering and exposing a native conversation ID. Independent child control, recursive discovery and Artifacts panels are unsupported.

## Permissions and runtime lifecycle

Permission events include the native title, input, detail card and available actions. An action must match both the supplied ID and allow/deny intent; no match cancels the request. Malformed fields are rejected with a canceled outcome before any approval is queued. Interrupt, close and runtime failure resolve outstanding permissions as canceled. `clearPendingPermissions` does the same before handling a new prompt; it does not permit a second concurrent turn.

Control RPCs time out after 30 seconds. Prompt generation remains open until completion, interruption or process failure. Shutdown ends stdin, then escalates process termination with a bounded wait. Explicit close releases the process and all Brain polling while preserving imported and prompted conversations. Automatic idle cleanup is vetoed because native background tasks cannot be safely queried.

Steering, rewind, archive operations, structured output and custom system prompts are unsupported.

## Context window occupancy

Paseo's context meter needs both `contextWindowUsedTokens` and a positive `contextWindowMaxTokens`. Valid native `usage_update` notifications take precedence for the current turn; used tokens must be finite and nonnegative. Only USD costs populate `totalCostUsd`.

agy prompt responses report uncached input and cache reads/writes as disjoint counters summed across internal model steps. When no valid context notification arrives during a turn, the plugin estimates occupancy as:

```text
singleStepInput = round((inputTokens + cachedReadTokens + cachedWriteTokens) / steps)
usedTokens = singleStepInput + outputTokens
```

If that sum is zero, it falls back to `round(totalTokens / steps)`. Invalid counters are treated as zero. A turn starts with one step; a new model message, thought or tool call following a completed/failed tool result adds a step. This estimates occupancy from aggregate counters rather than measuring the last model request exactly.

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

Fetching runs the launch executable with `-p /usage --output-format json` and a 15-second timeout. The `acp` subcommand and transport flags are removed; explicit directory arguments, the remembered working directory and launch/session environment overrides are preserved. No directory flags are added for default accounts. Current agy print mode rejects ACP directory flags, so such launches return a command error instead of querying a different account.

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
