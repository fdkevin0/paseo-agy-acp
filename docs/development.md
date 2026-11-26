# Development and releases

Start with the [installation guide](../README.md#install) to use the plugin. See [reference](reference.md) for operator behavior and [architecture](architecture.md) for internal invariants.

## Development setup

```sh
npm ci --ignore-scripts
```

Paseo compiles TypeScript directly; no separate build step is needed. Git/local plugin installs prune dev dependencies, so rerun `npm ci --ignore-scripts` if dev tools were removed.

## Architecture overview

- `index.server.ts` registers provider `agy` and the shared `AgyUsageSource`.
- `SharedControlRuntime` reuses one idle-reaped `agy acp` process for cwd-independent `catalog`/`sessions` probes; `StandbyRuntimePool` pre-warms isolated processes for interactive sessions.
- `SessionManager` bridges ACP stdio events, permissions, and elicitations with Paseo while `BrainTurnObservation`, `settleBrainTurn`, and `SubagentRegistry` reconcile Brain logs and SQLite conversation state.
- Subscription quota queries run `agy -p /usage --output-format json` independently without starting a model turn.

See [architecture](architecture.md) for state machines, `RuntimeEpoch` fencing, `O_TRUNC` transcript recovery, and replay alignment.

## Source layout

| File                                                                        | Responsibility                                                                                                                   |
| --------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `index.server.ts`                                                           | Register provider `agy` and shared usage source.                                                                                 |
| `server/common.ts`                                                          | Shared primitives, tool normalization, path/errno guards, bounded maps, env.                                                     |
| `server/provider.ts`                                                        | Provider contract, capability negotiation, catalog dedupe, shutdown.                                                             |
| `server/runtime.ts`                                                         | ACP init/version guard, `SharedControlRuntime`, `StandbyRuntimePool`, `TransportTracker`, `IdleTimer`, `closeDisposableSession`. |
| `server/session.ts`                                                         | `SessionPhase` + `activeTurn` state machine, `acquireRuntime`, prompts, permissions, interrupt recovery.                         |
| `server/subagents.ts`                                                       | Subagent ownership, multi-level subsession links, child Brain watchers.                                                          |
| `server/questions.ts`                                                       | Normalize `elicitation/create` schemas and `ask_question` `QuestionCard`s.                                                       |
| `server/permissions.ts`                                                     | Permission validation, classification, security warnings, MCP `toolPolicy`.                                                      |
| `server/transport.ts`                                                       | ACP stdio child process, elicitation schema patch, process-group shutdown.                                                       |
| `server/mapping.ts`                                                         | Timeline state machine, models/modes/thinking, attachments, context usage.                                                       |
| `server/mcp.ts`                                                             | MCP identity resolution (`call_mcp_tool`, `mcp_<server>_<tool>`, `mcp_paseo_*`).                                                 |
| `server/history.ts`                                                         | Replay tool alignment and verified compaction checkpoint insertion.                                                              |
| `server/brain.ts`                                                           | `RuntimeEpoch`, `BackgroundWorkProbe`, `ForegroundReconciler`, turn settling.                                                    |
| `server/brain-turns.ts`                                                     | Parent-turn correlation, `TurnBoundaryCache`, child re-wakeup turn floors.                                                       |
| `server/brain-projector.ts`                                                 | Brain transcript row projection, unified tasks/daemons, inbox matching.                                                          |
| `server/brain-observer.ts`                                                  | Incremental foreground `BrainTurnObservation` and `ModelUsageObserver`.                                                          |
| `server/brain-io.ts`                                                        | Symlink-safe Brain readers, `O_TRUNC` `TranscriptReader`, `StepDedupe`.                                                          |
| `server/conversation-db.ts`                                                 | Read-only stat-cached SQLite reader (`gen_metadata` window, `steps` errors).                                                     |
| `server/preflight.ts`                                                       | Version probes, `hashLaunchKey` fingerprints, data dir, artifact cleanup.                                                        |
| `server/usage.ts`                                                           | Account discovery, bounded login-log reads, subscription quota queries.                                                          |
| `tests/helpers.ts`, `tests/brain-fixtures.ts`                               | Event collectors, `deferred`, `waitForPidExit`, signal gates, `HarnessSessionManager`, `wrapConnectAcp`, Brain/SQLite fixtures.  |
| `tests/mock-acp.ts`, `tests/mock-usage.ts`                                  | Offline stdio ACP and `/usage` CLI substitutes.                                                                                  |
| `tests/*-acp.ts` (`compat`, `interrupt`, `transport`, `compaction-history`) | Focused native `agy` models for correlation, interrupts, shutdown, replay.                                                       |
| `tests/index.test.ts`, `tests/*.test.ts`                                    | Tier 1 (`test:unit`) offline tests using Node's native TypeScript runner.                                                        |
| `tests/live/*.test.ts`                                                      | Tier 2 (`test:live`) real local `agy` CLI boundary checks in temp homes.                                                         |
| `tests/workflows.test.ts`                                                   | Runs `.github/workflows/release.yml` shell blocks against local stubs.                                                           |
| `.github/workflows/`                                                        | CI (`npm run verify`) and tagged npm/GitHub release automation.                                                                  |

## Verification

### Offline and live checks

```sh
npm run format
npm run verify
npm run test:live
```

- `npm run verify` runs `format:check`, `tsc --noEmit`, `npm test` (`test:unit`, Tier 1 offline), and `npm pack --dry-run --ignore-scripts`.
- `npm run test:live` runs Tier 2 (`tests/live/*.test.ts`) against the local `agy` CLI in isolated temp homes.
- Offline tests need no login, network, or quota; workflow tests require Bash and stub `curl`, `gh`, and `npm`. Sanitized recordings live in `tests/fixtures/`.

### Live Paseo smoke checks

With an authenticated `agy` installation and a test daemon:

```sh
paseo plugin install .
paseo plugin ls
paseo plugin reload agy-acp
paseo provider models agy --json
paseo usage ls --json --refresh
paseo plugin logs agy-acp
```

Confirm `running` without plugin errors, a model catalog, and an `available` quota report (tier populates after live discovery or session open). Draft commands should appear before the first prompt, and closing an unprompted draft should leave no temporary artifacts.

For a full smoke check, open a plan-mode agent in a disposable directory, send a text-only prompt, then close and import/reopen to verify history. Test approval, denial, cancel, and tool cards separately. Desktop/mobile UI is a manual check.

## Packaging, CI, and releases

### Dependency locks and package contents

Keep Paseo host libraries and types in `devDependencies`; `@agentclientprotocol/sdk` is the runtime dependency. After dependency or version changes, run `npm install --package-lock-only --ignore-scripts` and review `npm-shrinkwrap.json`.

`paseo-plugin.json` includes `build` (`npm ci --omit=dev --ignore-scripts`) for Git/local installs; the release workflow strips only `build` before packing for npm (repeat this in a disposable checkout for manual packing). Check `npm pack --dry-run --ignore-scripts` and an actual tarball to ensure manifest, icons, README/OVERVIEW pairs, license, `docs/`, `index.server.ts`, `server/`, and `npm-shrinkwrap.json` ship while tests, fixtures, caches, and credentials stay out. See Paseo's [plugin publishing guide](https://paseo.sh/docs/plugins/publishing).

### CI and releases

- `ci.yml` runs on PRs, `main` pushes, and manual dispatch on Node 26 (`npm ci --ignore-scripts` + `npm run verify`).
- `release.yml` runs on `vX.Y.Z` tag pushes or manual tag dispatch, rejecting branches, mismatched tags, and non-stable versions. HTTP 404 from npm publishes with OIDC provenance ([Trusted Publishing](https://docs.npmjs.com/trusted-publishers/) for `fdkevin0/paseo-agy-acp`, `release.yml`, `id-token: write`, npm >=11.5.1); HTTP 200 skips publish and runs GitHub Release recovery. Other HTTP/network errors fail closed.

Release steps:

1. Bump `package.json` version and `PACKAGE_VERSION` in `server/common.ts`, then sync `npm-shrinkwrap.json`.
2. Run `npm run verify`, `npm run test:live`, inspect the tarball, and run live smoke checks.
3. Commit with a Conventional Commit message, push, and push the matching `vX.Y.Z` tag (never move a published tag or overwrite a published version).
4. Verify the published package on a test daemon before submitting to Paseo's registry.

### Release recovery

If npm publish succeeded but GitHub Release creation failed, rerun or dispatch `gh workflow run release.yml --ref vX.Y.Z`. It skips republishing, preserves any existing release, and creates a missing one with `gh release create --verify-tag`. Publish a new version rather than modifying a published tag or package.

## Documentation conventions

Use progressive disclosure:

- **`README.md` / `README.zh-CN.md`:** Lean user entry point (install, quick start, capabilities, essential limits, links to `docs/`). Maintain 100% EN/ZH parity.
- **`OVERVIEW.md` / `OVERVIEW.zh-CN.md`:** Factual Paseo registry overview (no install CLI commands). Maintain 100% EN/ZH parity.
- **`docs/reference.md`:** Scannable user/operator reference (English). Avoid internal code symbols and changelog prose.
- **`docs/architecture.md`:** Contributor architecture and invariant reference for `agy >= 1.5.16` (English).
- **`docs/development.md`:** Contributor setup, source layout, verification, packaging, and release runbook (English).
- Link rather than duplicate, keep every linked user document in `package.json`'s `files`, and use sample identities/paths only.
