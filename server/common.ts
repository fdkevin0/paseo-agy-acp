/** Shared primitives, type guards, bounded caches, and formatting helpers. */
import { sep } from 'node:path'

// Paseo's server bundler forbids importing root files outside client/, server/, or shared/;
// tests/index.test.ts enforces parity with package.json#version.
export const PACKAGE_VERSION = '0.1.8'

export const sessionIdPattern =
  /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i

/** Drop agy's `default_api:` namespace from native tool names. */
export function cleanToolName(name: string): string {
  return name.replace(/^default_api:/, '')
}

/** Strip agy's outer ACP `Run <name>` title wrapper, then drop `default_api:`. */
function unwrapToolTitle(title: string): string {
  return cleanToolName(title.replace(/^Run\s+/i, ''))
}

/** Normalize a tool identity from an explicit raw name or an ACP `Run <name>` title. */
export function normalizeToolCandidate(
  explicitRawName: string | undefined,
  title: string | null | undefined,
): string {
  return explicitRawName !== undefined
    ? cleanToolName(explicitRawName)
    : unwrapToolTitle(title ?? '')
}

/** Match native lifecycle identities, including agy's ACP `Run` title wrapper. */
export function isNativeToolName(
  name: string,
  expected:
    | 'invoke_subagent'
    | 'send_message'
    | 'manage_subagents'
    | 'manage_task'
    | 'schedule',
): boolean {
  return unwrapToolTitle(name) === expected
}

/** Whether a native tool name is agy's interactive question tool. */
export function isAskQuestionName(name: string): boolean {
  return /^ask_question$/i.test(cleanToolName(name))
}

type PlanModeToolName = 'switch_mode' | 'exit_plan_mode' | 'enter_plan_mode'

/** Whether a tool name or ACP title explicitly identifies a non-native MCP tool. */
function isForeignToolIdentity(
  explicitRawName: string | undefined,
  title: string | null | undefined,
): boolean {
  const raw = normalizeToolCandidate(explicitRawName, title)
  if (/^mcp_|^call_mcp_tool$/i.test(raw)) return true
  const colon = raw.indexOf(':')
  if (colon <= 0) return false
  const prefix = raw.slice(0, colon).toLowerCase()
  return (
    prefix !== 'contrib' &&
    prefix !== 'default_api' &&
    /^[a-z0-9_-]+:[a-z0-9_.-]+$/i.test(raw)
  )
}

/**
 * Classify agy's mode/plan handoff tools across raw names, `contrib:`, `Run ` titles,
 * and unnamed native `kind === 'switch_mode'` snapshots (rejecting foreign MCP identities).
 */
export function classifyPlanModeTool(
  explicitRawName: string | undefined,
  title: string | null | undefined,
  kind?: string | null,
): PlanModeToolName | undefined {
  const match = normalizeToolCandidate(explicitRawName, title).match(
    /^(?:contrib:)?(switch_mode|exit_plan_mode|enter_plan_mode)$/i,
  )
  if (match) return match[1].toLowerCase() as PlanModeToolName
  if (
    explicitRawName === undefined &&
    kind === 'switch_mode' &&
    !isForeignToolIdentity(explicitRawName, title)
  )
    return 'switch_mode'
  return undefined
}

/** Narrow opaque protocol payloads before reading vendor fields. */
export function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {}
}

export function str(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined
}

export function num(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

export function nonnegative(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0
}

export function text(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

/** Copy defined string entries from `process.env` when constructing a launch override. */
export function definedProcessEnv(): Record<string, string> {
  const env: Record<string, string> = {}
  for (const [key, value] of Object.entries(process.env)) {
    if (typeof value === 'string') env[key] = value
  }
  return env
}

/**
 * Merge defined environment entries, deduplicating case-variant keys (e.g. `Path` vs `PATH`)
 * on case-insensitive platforms so overrides cleanly replace inherited entries.
 */
export function mergeEnv(
  base: Readonly<Record<string, string | undefined>> | undefined,
  override?: Readonly<Record<string, string | undefined>>,
  caseInsensitive = process.platform === 'win32',
): Record<string, string> {
  const result: Record<string, string> = {}
  const lowerToKey = caseInsensitive ? new Map<string, string>() : undefined
  const apply = (source?: Readonly<Record<string, string | undefined>>) => {
    if (!source) return
    for (const [key, value] of Object.entries(source)) {
      if (typeof value !== 'string') continue
      if (lowerToKey) {
        const lower = key.toLowerCase()
        const prev = lowerToKey.get(lower)
        if (prev !== undefined && prev !== key) delete result[prev]
        lowerToKey.set(lower, key)
      }
      result[key] = value
    }
  }
  apply(base)
  apply(override)
  return result
}

/** Break V8 SlicedString parent retention for short substrings retained across polls or caches. */
export function detachString(value: string): string {
  return value.length === 0
    ? ''
    : Buffer.from(value, 'utf16le').toString('utf16le')
}

/** Truncate by Unicode code points and detach from parent string backing. */
export function truncateDetached(value: string, maxChars = 512): string {
  const chars = [...value]
  return detachString(
    chars.length > maxChars
      ? `${chars.slice(0, maxChars - 1).join('')}…`
      : value,
  )
}

/** Whether a resolved path stays strictly inside `base`. */
export function isContainedIn(target: string, base: string): boolean {
  const prefix = base === sep ? sep : base + sep
  return target.startsWith(prefix)
}

/** Whether an errno exception indicates a missing path or non-directory parent. */
export function isMissing(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | undefined)?.code
  return code === 'ENOENT' || code === 'ENOTDIR'
}

/** Insert or refresh `key` in `map`, evicting oldest entries when exceeding `limit`. */
export function setBounded<K, V>(
  map: Map<K, V>,
  key: K,
  value: V,
  limit: number,
): void {
  map.delete(key)
  while (map.size >= limit) {
    const oldest = map.keys().next()
    if (oldest.done) break
    map.delete(oldest.value)
  }
  map.set(key, value)
}

/** Extract a concise human-readable error message, preserving ACP `RequestError.data` details. */
export function formatErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    const base = error.message
    const data = (error as { data?: unknown }).data
    const detail =
      text(data) ?? text(record(data).details) ?? text(record(data).message)
    if (detail && !base.includes(detail)) {
      return `${base}: ${detail}`
    }
    return base
  }
  return String(error)
}
