/** Shared primitives, type guards, bounded caches, and formatting helpers. */

// Paseo's server bundler forbids importing root files outside client/, server/, or shared/;
// tests/index.test.ts enforces parity with package.json#version.
export const PACKAGE_VERSION = '0.1.1'

export const sessionIdPattern =
  /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i

/** Drop agy's `default_api:` namespace from native tool names. */
export function cleanToolName(name: string): string {
  return name.replace(/^default_api:/, '')
}

/** Match native lifecycle identities, including agy's legacy ACP `Run` title. */
export function isNativeToolName(
  name: string,
  expected:
    | 'invoke_subagent'
    | 'send_message'
    | 'manage_subagents'
    | 'manage_task'
    | 'schedule',
): boolean {
  const cleaned = cleanToolName(name)
  return cleaned === expected || cleaned === `Run ${expected}`
}

/** Whether a native tool name is agy's interactive question tool. */
export function isAskQuestionName(name: string): boolean {
  return /^ask_(?:question|user)$/i.test(cleanToolName(name))
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
