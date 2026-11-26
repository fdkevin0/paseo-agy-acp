# Reference

Start with the [installation guide](../README.md#install) to install and run the plugin. See [development](development.md) for contributor setup and releases, and [architecture](architecture.md) for internal invariants.

## Launch and data directories

Plugin `agy-acp` registers provider `agy` (default command: `agy acp`). Paseo resolves the executable, argument prefix, and launch environment; session environment overrides apply on top. Session MCP servers translate into ACP stdio, HTTP, or SSE configs, and non-empty session system prompts forward via `_meta.antigravity.systemPrompt.append`.

| Setting                            | Behavior                                                                                                    |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Command & argument overrides       | Paseo provider launch config chooses the executable and argument prefix.                                    |
| `HOME` / `USERPROFILE`             | Session env overrides launch env (`HOME`, then `USERPROFILE`, ignoring empty strings), then host home.      |
| `--gemini-dir`, `--gemini_dir`     | Override Gemini directory; relative paths resolve against session `cwd` realpath. Default: `$HOME/.gemini`. |
| `--app-data-dir`, `--app_data_dir` | Override app directory; relative paths resolve under Gemini dir. Default: `antigravity-cli`.                |

Flag-like values (starting with `-` without `=`) are ignored so misplaced flags never become directory paths. Other vendor directory flags are not interpreted. See [subscription quota](#subscription-quota) for print-mode directory restrictions.

### Availability and runtime pooling

- **Minimum version:** Availability checks run `<command> --version` (5 s timeout) and require a stable version `>= 1.5.16` (pre-releases like `1.5.16-*` are rejected). Every ACP runtime also validates `agentInfo.version` during `initialize`.
- **Caching:** Successful probes coalesce and cache for 60 s per resolved launch and executable fingerprint (realpath, device, inode, size, mtime). Replacing, downgrading, or removing the binary invalidates cached status and pooled runtimes immediately. Cache keys use SHA-256 hashes (no plain-text env values).
- **Pooling:** Cwd-independent launches reuse one 5-minute idle-reaped control runtime for catalog and session-list probes (sharing in-flight catalog probes per `cwd`), and warm up to 4 isolated standby runtimes for non-Full-Access interactive opens and relaunches. Relative executable paths, relative `--gemini-dir` paths, and `--dangerously-skip-permissions` opt out of pooling.
- **Missing `cwd`:** Catalog and session-list probes fall back to resolved `HOME` when cwd-independent; otherwise they fail with `Working directory does not exist`.

### Temporary session cleanup

Catalog probes and unprompted new sessions close via `session/close` and remove only their own validated UUID artifacts under the data directory:

| Artifact under data directory                 | Cleanup                                                          |
| --------------------------------------------- | ---------------------------------------------------------------- |
| `conversations/<id>.db`, `.db-wal`, `.db-shm` | Removed after native close (and process stop when not recycled). |
| `brain/<id>/`                                 | Removed after native close (and process stop when not recycled). |
| Shared summary DBs & other sessions           | Retained.                                                        |

- Healthy idle unprompted processes (cwd-independent, non-Full-Access, zero MCP servers) recycle into the standby pool while artifacts are deleted; all other closes stop their process first.
- Imported and prompted conversations remain on disk, and prompted processes are never recycled. Cleanup refuses symlinks and is best-effort.

## Models, modes, thinking and commands

| Mode ID        | Paseo Visual                  | Behavior                                                                        |
| -------------- | ----------------------------- | ------------------------------------------------------------------------------- |
| `default`      | `Shield` / `safe`             | Standard native approval prompts.                                               |
| `auto`         | `ShieldCheck` / `moderate`    | Advertised when supplied by `agy`; native automatic execution policy.           |
| `accept-edits` | `ShieldPlus` / `moderate`     | Native edit auto-acceptance.                                                    |
| `plan`         | `ShieldEllipsis` / `planning` | Native planning mode.                                                           |
| `full-access`  | **Full Access**               | Runs `--dangerously-skip-permissions` and auto-approves `tool`, `mode`, `plan`. |

- **Full Access:** Sets native mode to `accept-edits` (or `default`). Interactive `question` prompts always require user selection. Toggling `full-access` on an open session reloads the runtime; if `agy` switches native mode to `plan`, Full Access clears automatically.
- **Thinking & apply order:** Per-model effort choices (`low`, `medium`, `high`, `xhigh` / `Extra`, `max`) derive from `agy`'s advertised models and `thought_level` options; fixed-effort models (such as `Orchestrator`) report an empty list. Open and configure apply `model -> thought_level -> mode`, changing only differing values:
  - Fixed-effort models ignore a bundled thinking option on open/switch and reject standalone thinking updates.
  - New opens ignore unsupported thinking values while validating `model` and `mode`; persisted restores also skip retired/unadvertised `model` and `mode` values to retain native state.
- **Slash commands:** Waits up to 1 s before readiness for the session's initial command list (never cached across sessions). Duplicate lists are suppressed; later updates forward live.
- **Negotiated capabilities:** `prompt.message`, `prompt.command`, `prompt.image`, `session.configure`, `session.list`, `session.persistence`, `session.can_stop_runtime`, `permission`, `permission.tool_policy`, and `session.subsession`.

## Permissions and runtime lifecycle

- **Permission cards:** Show native title, display-sanitized input (stripping redundant `toolSummary`), detail card, actions, and kind (`question`, `mode`, `plan`, or `tool`). Native `agy.security.warning` metadata appears in the description (up to 512 chars) and marks warned allow actions as danger.
- **Decisions:** Responses must match both action ID and allow/deny intent (defaulting to `allow_once` / `reject_once`); unmatched responses cancel. Denying a tool marks it `canceled` without error; `interrupt: true` also interrupts the turn.
- **Interactive questions:** `ask_question` runs via ACP elicitation during foreground turns and wakeups; declining cancels the active wakeup.
- **Subagent & workflow forwarding:** In `agy` 1.5.16+, child approvals and questions (`<childId>:<callId>`) forward on the root session during an active turn while child tool cards stay on the child's own timeline. Requests outside an active turn, malformed payloads, mismatched session IDs, or requests during close/failure cancel automatically.
- **Interrupt & recovery:** When no background tasks, active subagents, unread wakeups, or retries are running, interrupt sends `session/cancel` (350 ms fast path). Otherwise it stops the process tree within Paseo's 2 s budget and reloads the session asynchronously with model, thinking, and mode preserved; subsequent configure/prompt calls queue behind recovery.
- **Timeouts & hidden errors:** Control RPCs time out at 30 s; history replay (`session/load`) uses a 30 s wire-idle timeout capped at 5 min; prompt turns stay open until settled or interrupted. After a turn finishes (or returns `refusal`), the local conversation database is checked for trailing upstream API errors (e.g. `QUOTA_EXHAUSTED` with reset times, `INVALID_THINKING_SIGNATURE`).
- **Shutdown & idle cleanup:** Unix shutdown terminates detached descendant process groups even without `ps`. Idle runtime cleanup is allowed only when no turn, lifecycle transition, pending approval, active subagent, background task, non-daemon cron schedule, standing daemon, unread wakeup, or unresolved retry is active.
- **Unsupported:** Steering, rewind, archive operations, structured output, Artifacts panels, and independent subagent session control.

## Attachments and tool cards

Dedicated cards require a known native tool identity or trusted ACP kind hint; argument keys alone never classify unknown MCP tools.

| Native tool or arguments                                           | Paseo card                                                                     |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `run_command` (`CommandLine` / `command`)                          | Shell card (command, working directory, output).                               |
| File read (`Path` / native location)                               | Read card (path, range, content).                                              |
| `read_url_content` / `kind: 'fetch'` (`Url` / `url`)               | Fetch card (target URL, content).                                              |
| File write (`ArtifactMetadata.RequestFeedback`) / `exit_plan_mode` | Plan card when feedback requested or non-empty plan supplied; else Write card. |
| File edit/replace or ACP diff                                      | Edit card (old/new text or diff).                                              |
| Search query / pattern                                             | Search card.                                                                   |
| `invoke_subagent` / `send_message`                                 | Subagent card (description, result, child session link).                       |
| `ask_question`                                                     | Plain-text question options and answer.                                        |
| `update_steps`                                                     | Plain-text `Steps` checklist (`[x]`/`[-]`/`[ ]`/`[~]`/`[!]`; else `unknown`).  |
| Unrecognized / MCP tool                                            | Structured `unknown` JSON card preserving input and output separately.         |

- **ACP v1 updates & terminalization:** Partial updates merge by call ID. Top-level `null` or omitted `rawInput`/`rawOutput` means "not supplied" on initial calls and leaves prior values unchanged on updates; non-null values (`false`, `0`, `""`, `[]`, `{}`) replace the channel. When no raw output is supplied, ACP content blocks are preserved. Denied or interrupted tools map to `canceled` without error; genuine failures report `failed` with a diagnostic.
- **Non-image attachments:** Text, paths, titles, resource links, bodies, and review comments forward as prompt text. Uploaded files pass as daemon-side paths; arbitrary files are never read silently.

### MCP identity and preapprovals

One resolver governs live cards, `toolPolicy` preapprovals, and replay:

- **Recognized forms:** `call_mcp_tool` (optionally prefixed with `default_api:` or `Run `) with `ServerName`/`ToolName` (or camelCase/lowercase aliases); flattened native `mcp_<server>_<tool>` matched uniquely against configured session MCP servers; and built-in `mcp_paseo_<tool>` fallback when no configured server prefix matches (preserving explicit servers like `paseo_custom`).
- **Fail-closed matching:** Explicit raw `name` overrides `title`. Recognized calls display as `server.tool`, preserve arguments/casing, and match `config.toolPolicy.preapproved` by exact case-sensitive `(server, tool)`. Display labels never become permission keys, and overlapping server prefixes fail closed to require approval.
- **Output & `agy` limits:** Output stays on the `unknown` JSON card (no result-image extraction; status follows ACP/Brain, not nested `isError`). Upstream `agy` enforces an ~1,500-byte lazy-load description threshold, 60 s HTTP header timeout, and 180 s MCP tool execution timeout.

## Persistence, history and subagents

- **Restore:** Persists native UUID (`version: 1`). `history: 'replay'` loads history before `session.ready`; `history: 'skip'` prefers `session/resume` and suppresses replayed rows. New sessions require `cwd` to exist; reopening a persisted session with a deleted `cwd` falls back to `HOME` for read-only history while rejecting new prompts with `Working directory does not exist`.
- **Background work & Reactive Wakeup:** On `agy` 1.5.16+, non-daemon tasks, subagents, and non-daemon cron schedules remain inside the active prompt turn. Standing daemons (`IsDaemon: true`) allow foreground settlement and stream post-turn **Reactive Wakeup** replies, tools, and approvals.
- **Wait & retry limits:** 2 s flush grace for turn start when Brain exists; 15 min per active woken step or recognized transient model retry; 120 s for other idle waits (auto-adjusted across host sleep). Timeouts fail the turn and interrupt `agy`.
- **Subagents:** Multi-level children link as `<sessionId>:child:<nativeChildId>`. Child failures fail waiting parent turns after flushing child history; re-waking a child isolates it from earlier-turn failures. Native `manage_subagents` cancellation drains history before marking the child canceled.
- **History & compaction:** Replay preserves ACP snapshots and order, filling missing results/child links from Brain. Verified compaction checkpoints emit standard Paseo compaction items with native timestamps and `preTokens` (`input + cacheRead + cacheWrite`); unlocated checkpoints emit a timestamp-less info notice.

## Context window occupancy

1. **Capacity (`contextWindowMaxTokens`):** Uses the server auto-compaction window from the local conversation database (updating live on model switches), falling back to `usage_update.size`, and omitted when neither is known.
2. **Used tokens (`contextWindowUsedTokens`):**
   - Foreground native `usage_update` takes precedence during the active turn.
   - Otherwise (and during post-turn Reactive Wakeup), computed from the latest valid Brain `PLANNER_RESPONSE` step: `inputTokens + cachedReadTokens + cachedWriteTokens + outputTokens`.
   - Falls back last to the database's per-turn estimate; cumulative prompt counters never substitute for occupancy.
3. **Edge cases:** Cache-only positive steps are valid; all-zero placeholder counters are ignored (explicit zero `usage_update` remains valid). Cancel, error, and restore recover snapshots within turn boundaries. Only USD populates `totalCostUsd`.

## Subscription quota

Usage Source `agy` backs the Composer popover and `paseo usage ls --json` (`--refresh` bypasses host cache).

- **Discovery:** Scans remembered directories (up to 100) plus the default data directory (serving `agy` and `antigravity` session scopes). Account keys are SHA-256 hashes of resolved data directories. A directory is discoverable via a remembered launch, a login line in symlink-contained `cli.log` or up to 10 recent `log/cli-*.log` / `log/acp-server-*.log` files, or a non-empty `antigravity-oauth-token` file. Labels use the latest login email (default `Antigravity`); token contents are never read or exposed. Observed tier (`planLabel`) populates after catalog discovery or session open.
- **Execution:** Runs `<command> -p /usage --output-format json` (15 s timeout), stripping `acp` and transport flags while keeping directory args and env overrides. Non-default directories without launch context return an error rather than querying the default account; interactive sign-in prompts abort immediately. Deleted workspaces fall back to `HOME` only when cwd-independent. Current `agy` print mode rejects ACP directory flags and returns a command error.

Each valid bucket maps to a window (>90% danger, 70–90% warning; disabled buckets skipped):

| Native field            | Reported behavior                                                         |
| ----------------------- | ------------------------------------------------------------------------- |
| Group name              | Scope label and normalized ID (`gemini_models`, `claude_and_gpt_models`). |
| `window: 5h` / `weekly` | 18,000 s / 604,800 s window; unknown windows preserve native labels.      |
| `remaining_fraction`    | Rounded, clamped used percentage; absent stays unknown.                   |
| `reset_time`            | Trimmed reset timestamp, or omitted when absent/blank.                    |

| Outcome                                                   | Usage report                                                 |
| --------------------------------------------------------- | ------------------------------------------------------------ |
| Valid quota buckets                                       | `available` with windows and observed plan label when known. |
| No buckets / Gemini API key without quota (`exit code 2`) | `unavailable` with `no_quota` (plus CLI diagnostic).         |
| Command, CLI flag, auth, timeout, or parse failure        | `error` with diagnostic.                                     |
| No token file, login line, or remembered launch           | Omitted from discovery.                                      |
| Non-default account without launch context                | `error` (never falls back to default account).               |
| Login store removed while CLI still answers               | Reports CLI result; token file is not authoritative.         |
