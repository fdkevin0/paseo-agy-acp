# Development and releases

Start with the [installation guide](../README.md#install) to use the plugin. For configuration, protocol behavior and operational limits, see the [reference](reference.md).

## Development setup

From a clone, install the locked development dependencies:

```sh
npm ci --ignore-scripts
```

Paseo compiles the TypeScript plugin; no separate build output is required. Git/local plugin preparation installs production dependencies only, so rerun the command above if preparation removed development tools.

## Request and session flow

1. `index.server.ts` registers provider `agy` and a shared subscription usage source. The provider negotiates capabilities and creates a session manager for each connection.
2. Catalog discovery opens a disposable ACP session; session listing needs no native session. Normal opens create or load a native session, apply requested configuration and wait for that session's live commands before publishing readiness. Command lists are never seeded from previous sessions.
3. Prompts go to agy over stdio. Notifications and permission requests must match the current native session and are ignored or canceled during shutdown. Valid notifications become timeline snapshots, permissions, configuration and usage events; agy owns tool execution.
4. Permission responses resolve the matching native request. Child conversation IDs start read-only Brain transcript and lifecycle tracking. ACP and Brain tool snapshots share one identity via structured `.system_generated/` metadata (`sourceMetadata.tool.toolCall.id` and `spawnStepIndex`, with invocation-order fallback); canonical Brain results also fill missing ACP tool output during history replay. Pending ACP notifications are drained before the turn ends; Brain tracking keeps the turn open for native background work and Reactive Wakeup responses that outlive the ACP prompt response.
5. Close cancels pending approvals and stops Brain polling. New unprompted sessions and catalog probes close natively, stop their process, and remove only their own temporary artifacts. Other closes stop the process while preserving imported and prompted conversations. Protocol failure also terminates the child; connection shutdown waits for all owned transports.
6. Subscription queries independently invoke agy's local `/usage` command using observed account and tier metadata. They do not start a model turn.

## Source layout

| File                                                                      | Responsibility                                                                         |
| ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `index.server.ts`                                                         | Register the provider and shared usage source.                                         |
| `server/provider.ts`                                                      | Provider contract, capability negotiation and cached catalog discovery.                |
| `server/session.ts`                                                       | Session manager, notifications, prompts, commands and lifecycle.                       |
| `server/permissions.ts`                                                   | Validate permission requests before queuing approvals.                                 |
| `server/transport.ts`                                                     | ACP stdio process, metadata capture and control deadlines.                             |
| `server/mapping.ts`                                                       | Timeline, configuration, attachment and context usage mapping.                         |
| `server/brain.ts`                                                         | Incremental transcript reading, projection and background turn completion.             |
| `server/preflight.ts`                                                     | Version checks, cache identity, data directories and scoped cleanup.                   |
| `server/usage.ts`                                                         | Account discovery, bounded login-log reads and subscription quota windows.             |
| `tests/mock-acp.ts`, `tests/mock-usage.ts`                                | Offline CLI substitutes.                                                               |
| `tests/compat-acp.ts`, `tests/interrupt-acp.ts`, `tests/transport-acp.ts` | Native agy behavior models for Brain compatibility, interruption and process shutdown. |
| `tests/*.test.ts`                                                         | Mapping, permissions, lifecycle, discovery and quota regression tests.                 |
| `tests/workflows.test.ts`                                                 | Execute release workflow scripts against local command substitutes.                    |
| `.github/workflows/`                                                      | CI checks and tagged npm/GitHub releases.                                              |

## Verification

### Offline checks

```sh
npm run format
npm run format:check
npm run check
npm test
npm pack --dry-run --ignore-scripts
```

`tests/compat-acp.ts` models observed native notifications: display titles, omitted tool output and transcript calls without IDs. `tests/interrupt-acp.ts` models background work that outlives ordinary cancellation, so regression tests verify process shutdown and restoration rather than trusting a canceled UI state. `tests/transport-acp.ts` starts a detached command that outlives ACP EOF, so shutdown tests verify process-group cleanup, including hosts without `ps`.

Tests run with Node's native TypeScript support and temporary files; they require no login or model quota. Workflow tests require Bash and stub `curl`, `gh` and `npm`, so they neither contact registries nor publish artifacts.

Coverage includes tool terminalization, PascalCase arguments, FIFO and structured Brain results, history suppression, permissions, attachment mapping, fresh commands before readiness, stale notification/permission rejection, cancellation races, conditional interruption (fast-path `session/cancel` vs. background-owner relaunch), native step statuses (each step logged once; `CANCELED`/`INTERRUPTED`; superseded `PENDING`/`QUEUED`/`GENERATING` rows), in-place `O_TRUNC` rewrites (including an empty file mid-rewrite), stall timeouts and what suspends them, transcript flush grace, child cancellation ordering, killed-child close without reopening, resumed child watchers, protocol disconnect shutdown, catalog concurrency and cleanup containment. Usage tests cover per-step context occupancy from `transcript_full.jsonl`, native-measurement precedence, account discovery, quota mapping, deleted-workspace fallback and error handling. Workflow tests exercise tag/version validation, registry failures, manifest preparation and GitHub Release recovery.

### Live Paseo smoke checks

Use an authenticated agy installation and a test daemon:

```sh
paseo plugin install .
paseo plugin ls
paseo plugin reload agy-acp
paseo provider models agy --json
paseo usage ls --json --refresh
paseo plugin logs agy-acp
```

Require `running` with no plugin error, a model catalog and an `available` agy quota report. The tier appears after successful native catalog discovery or session opening; a cached model list alone may not populate it. Draft commands should be available before the first prompt, and closing an unprompted draft session should leave no new session artifacts.

For a full conversation smoke test, use a disposable working directory, open a plan-mode agent, request a text-only reply, then close and import/reopen the native session to verify history. Exercise permission approval, denial, cancellation and rich tool cards separately. Model prompts consume account quota. Desktop/mobile UI acceptance is a manual check; offline tests do not verify layout or service access.

## Packaging, CI, and releases

### Dependency locks and package contents

Keep Paseo host libraries and type-only dependencies in `devDependencies`; the ACP SDK is a runtime dependency. `npm-shrinkwrap.json` locks Git preparation and the installed npm dependency tree. After dependency or version changes, synchronize it with `npm install --package-lock-only --ignore-scripts` and review the diff. npm automatically includes the shrinkwrap in published packages.

The source manifest's `build` runs `npm ci --omit=dev --ignore-scripts` for Git/local installations. npm already installs production dependencies, so the release workflow removes only `build` from `paseo-plugin.json` before packing and publishing. It preserves the other manifest fields and changes only the release runner's checkout. Manual npm packaging must perform the same preparation in a disposable checkout.

Inspect `npm pack --dry-run --ignore-scripts` for the manifest, icon, both overviews, README, license, linked docs, server entry, all imported runtime files and shrinkwrap. Tests, workflow fixtures, local transcripts, caches and credentials must stay outside the tarball. Also inspect an actual tarball before a release. Follow Paseo's [plugin publishing guide](https://paseo.sh/docs/plugins/publishing) when preparing a registry submission.

### CI and tag validation

`ci.yml` checks pull requests, pushes to `main` and manual runs on Node 22.18 and 24. It installs the locked dependencies, checks formatting and types, runs the offline suite and verifies package contents.

`release.yml` runs for version-tag pushes or manual dispatch on an existing tag. It rejects branch dispatches, mismatched tags and non-stable versions before contacting npm. The tag must equal `v` plus `package.json`'s stable `X.Y.Z` version. Only a registry HTTP 404 means the version is absent; HTTP 200 marks it as already published. Authentication failures, server errors and timeouts stop the workflow.

For an unpublished version, the job installs dependencies, verifies formatting/types/tests, prepares the npm manifest and checks the package before publishing publicly with provenance. Existing versions skip npm preparation/publication and proceed to GitHub Release recovery. Ordinary branch pushes do not trigger this release workflow.

### Trusted Publishing and release procedure

Configure npm [Trusted Publishing](https://docs.npmjs.com/trusted-publishers/) for owner `fdkevin0`, repository `paseo-agy-acp` and workflow filename `release.yml`. The publish job grants `id-token: write` and installs npm >=11.5.1. An optional `NPM_TOKEN` repository secret is passed as `NODE_AUTH_TOKEN` for token-based authentication. Repository ownership, npm package permissions and publisher setup must be configured before releasing.

1. Bump the stable package version and synchronize the shrinkwrap.
2. Run offline checks, inspect the prepared tarball and complete live smoke checks.
3. Commit with a Conventional Commit message and review the final source and package contents.
4. Push the reviewed version commit and matching `vX.Y.Z` tag when ready to publish. Never move a published tag or overwrite a published npm version.
5. Verify the exact published package on a test daemon before submitting it to Paseo's registry.

### Release recovery

If npm publication succeeds but GitHub Release creation fails, rerun the tagged workflow or dispatch it with `gh workflow run release.yml --ref vX.Y.Z`. It detects the published npm version and skips republishing. An existing GitHub Release is left alone; a missing one is created with `gh release create --verify-tag`, so recovery cannot silently create a tag at the wrong commit. A failed create command fails the job.

Resolve registry or authentication errors before retrying. If a published artifact is wrong, publish a corrected new version instead of changing the old tag or package.

## Documentation conventions

Use progressive disclosure:

- Keep the README focused on installation, the first successful run, capabilities and essential limitations.
- Put launch settings and protocol/operational details in the reference; keep implementation, testing and release instructions here.
- Keep listing overviews factual and user-facing, with purpose, operation, requirements, data flow and limits. They must not contain plugin-add or npm-install command examples.
- Maintain `README.zh-CN.md` and `OVERVIEW.zh-CN.md` as complete translations of `README.md` and `OVERVIEW.md`; keep the rest of the project documentation in English.
- Link to deeper explanations instead of duplicating them, and include every linked user document in the package's `files` list.
- Use sample identities and paths; never commit account logs, local credentials or personal workstation paths.
