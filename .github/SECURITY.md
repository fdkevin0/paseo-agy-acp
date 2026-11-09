# Security Policy

## Supported Versions

Only the latest published release of `@fdkevin/paseo-agy-acp` receives security updates.

| Version | Supported |
| ------- | --------- |
| Latest  | Yes       |
| Older   | No        |

## Reporting a Vulnerability

Please **do not** report security vulnerabilities through public GitHub issues.

Use [GitHub Private Vulnerability Reporting](https://github.com/fdkevin0/paseo-agy-acp/security/advisories/new) to submit a report privately. Include:

- Affected plugin version, Paseo version, Node.js version, and `agy` CLI version.
- Reproduction steps or a minimal proof of concept.
- Relevant sanitized logs (never include OAuth tokens, local credentials, or private transcripts).

## Trust Boundary and Scope

`paseo-agy-acp` runs inside the Paseo plugin host and communicates with the locally installed `agy` CLI over stdio and local data directories (`$HOME/.gemini/antigravity-cli` or configured overrides).

### In Scope

- **Path traversal or containment bypass** when reading Brain state (`brain/<id>/.system_generated/`) or cleaning up unprompted temporary session artifacts (`conversations/<id>.db*` and `brain/<id>/`), including symlink traversal.
- **Permission bypass or spoofing** where tool approvals, session IDs, or mode transitions (`full-access` / `--dangerously-skip-permissions`) execute without matching user intent or valid session binding.
- **Credential or environment leakage** in cache keys, usage reports, or error diagnostics (for example, exposing `antigravity-oauth-token` contents).
- **Orphaned process execution** where ACP child processes or descendant process groups escape session termination or interrupt cleanup.

### Out of Scope

- Vulnerabilities inside the upstream `agy` binary, upstream model backends, or the Paseo host daemon itself.
- Actions performed by `agy` tools after the user explicitly approves a tool request or enables **Full Access** (`full-access`) mode.
- Local attacks requiring pre-existing code execution under the same OS user account running the Paseo daemon.
