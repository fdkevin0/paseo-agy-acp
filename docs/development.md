# Development and releases

## Layout

- `index.server.ts`: Paseo contribution entry.
- `server/provider.ts`: registration, capability negotiation and cached discovery.
- `server/session.ts`: session manager, notifications, turn lifecycle and permission handling.
- `server/permissions.ts`: permission request validation before pending work is created.
- `server/transport.ts`: ACP stdio process and control deadlines.
- `server/mapping.ts`: timeline, configuration, attachment and usage mapping.
- `server/brain.ts`: child transcript projection and tailing.
- `server/preflight.ts`: version check, cache identity and scoped probe cleanup.
- `tests/`: fixture ACP server and regression tests.

## Local checks

```sh
npm ci --ignore-scripts
npm run format
npm run check
npm test
npm pack --dry-run
```

Tests require no login or model quota. They cover tool terminalization, history suppression, permission decisions, concurrent catalog discovery, cleanup containment, attachment mapping, usage and malformed input. Brain fixtures cover FIFO tool results, native PascalCase arguments and ignored metadata rows. Discovery tests also cover failed native close requests. Test Brain polling uses a short interval instead of waiting a second per assertion.

For integration, install the directory on a test daemon and require `running` from `paseo plugin ls`. After editing, reload `agy-acp`. `paseo plugin logs agy-acp` supplies initialization diagnostics. Local build preparation installs production dependencies; rerun `npm ci --ignore-scripts` if development tools were removed.

A real smoke test should open a plan-mode agent, request a text-only reply without tools, close it, then import/reopen the native session and verify history. Permission and tool tests should use a disposable working directory. Mobile/desktop layout acceptance is a separate manual check of Paseo's native cards.

## Packaging

Paseo host libraries remain development dependencies. ACP SDK is a runtime dependency. `npm-shrinkwrap.json` locks both Git preparation and the npm installation tree; keep it synchronized with package metadata using `npm install --package-lock-only --ignore-scripts` after dependency changes. npm automatically includes the shrinkwrap in the published package.

The Git manifest runs `npm ci --omit=dev --ignore-scripts`. Do not ship a second dependency installation step inside the npm artifact: the release workflow removes the Git-only `build` field from its checked-out manifest immediately before packing/publishing. This affects only the release runner, not the Git source. If publishing manually, perform that change in a disposable checkout first.

Inspect `npm pack --dry-run` for the manifest, SVG, English/Chinese overviews, license, docs, server entry, runtime source and shrinkwrap. Tests, caches and local transcripts must not be included. Overviews describe behavior without installation commands.

## CI and release

CI installs the locked tree, checks formatting/types, runs tests and checks the package on Node 22.18 and 24. It runs for pull requests and pushes to `main`.

The release workflow publishes only a tag matching the stable package version, such as `v0.1.0`. It verifies the same checks, publishes a public npm package with provenance, and creates a GitHub Release. Configure npm trusted publishing for `fdkevin0/paseo-agy-acp` and `.github/workflows/release.yml`, or supply a scoped `NPM_TOKEN` secret if needed. Repository creation, registry ownership, credentials and the first publish are external setup; adding this workflow does not perform a release.

Before a release, update `package.json` and the shrinkwrap, inspect the tarball, validate with a real agy/Paseo session, and push the reviewed version commit and tag. Re-running an existing version skips npm publication and can finish the GitHub Release step. Do not overwrite a published version.
