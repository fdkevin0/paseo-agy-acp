import { lstat, realpath } from 'node:fs/promises'
import { dirname, join, sep } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { BrainEscapeError, isMissing, isRetryWarning } from './brain-io.ts'
import { detachString, sessionIdPattern, setBounded } from './common.ts'

export type ConversationDbStat = {
  dbPath: string
  dev: bigint
  ino: bigint
  fingerprint: string
}

export type CompactionWindowResult = {
  maxTokens?: number
  estimatedTokens?: number
  genIdx: number
}

export type ReadConversationCompactionWindowOptions = {
  openDb?: (path: string) => DatabaseSync
}

function readProtoVarint(
  buf: Uint8Array,
  start: number,
): [bigint, number] | undefined {
  let val = 0n
  let shift = 0n
  let pos = start
  while (pos < buf.length && shift <= 63n) {
    const b = buf[pos++]
    val |= BigInt(b & 0x7f) << shift
    if ((b & 0x80) === 0) return [val, pos]
    shift += 7n
  }
  return undefined
}

function readProtoVarintPath(
  buf: Uint8Array,
  path: readonly number[],
): number | undefined {
  if (path.length === 0) return undefined
  let pos = 0
  let match: number | undefined
  const maxSafe = BigInt(Number.MAX_SAFE_INTEGER)
  while (pos < buf.length) {
    const tagRead = readProtoVarint(buf, pos)
    if (!tagRead) return undefined
    const [tag, afterTag] = tagRead
    pos = afterTag
    const fieldNumber = Number(tag >> 3n)
    const wireType = Number(tag & 7n)
    if (wireType === 0) {
      const valRead = readProtoVarint(buf, pos)
      if (!valRead) return undefined
      const [val, afterVal] = valRead
      pos = afterVal
      if (
        path.length === 1 &&
        fieldNumber === path[0] &&
        val > 0n &&
        val <= maxSafe
      ) {
        match = Number(val)
      }
    } else if (wireType === 1) {
      pos += 8
      if (pos > buf.length) return undefined
    } else if (wireType === 2) {
      const lenRead = readProtoVarint(buf, pos)
      if (!lenRead) return undefined
      const [rawLen, afterLen] = lenRead
      if (rawLen < 0n || rawLen > maxSafe) return undefined
      const len = Number(rawLen)
      pos = afterLen
      if (pos + len > buf.length) return undefined
      if (path.length > 1 && fieldNumber === path[0]) {
        const nested = readProtoVarintPath(
          buf.subarray(pos, pos + len),
          path.slice(1),
        )
        if (nested !== undefined) match = nested
      }
      pos += len
    } else if (wireType === 5) {
      pos += 4
      if (pos > buf.length) return undefined
    } else {
      return undefined
    }
  }
  return match
}

export async function resolveConversationDbStat(
  brainRoot: string,
  nativeId: string,
): Promise<ConversationDbStat> {
  const appRoot = await realpath(dirname(brainRoot))
  const convCandidate = join(appRoot, 'conversations')
  const convInfo = await lstat(convCandidate)
  if (convInfo.isSymbolicLink() || !convInfo.isDirectory())
    throw new BrainEscapeError()
  const convRoot = await realpath(convCandidate)
  const appPrefix = appRoot === sep ? sep : appRoot + sep
  if (!convRoot.startsWith(appPrefix)) throw new BrainEscapeError()
  const candidate = join(convRoot, `${nativeId}.db`)
  const info = await lstat(candidate, { bigint: true })
  if (info.isSymbolicLink() || !info.isFile()) throw new BrainEscapeError()
  let walFingerprint = 'missing'
  for (const suffix of ['-wal', '-shm'] as const) {
    try {
      const auxInfo = await lstat(`${candidate}${suffix}`, { bigint: true })
      if (auxInfo.isSymbolicLink() || !auxInfo.isFile())
        throw new BrainEscapeError()
      if (suffix === '-wal') {
        walFingerprint = `${auxInfo.dev}:${auxInfo.ino}:${auxInfo.size}:${auxInfo.mtimeNs}`
      }
    } catch (error) {
      if (error instanceof BrainEscapeError) throw error
      if (!isMissing(error)) throw error
    }
  }
  const resolved = await realpath(candidate)
  const prefix = convRoot === sep ? sep : convRoot + sep
  if (!resolved.startsWith(prefix)) throw new BrainEscapeError()
  return {
    dbPath: resolved,
    dev: info.dev,
    ino: info.ino,
    fingerprint: `${info.dev}:${info.ino}:${info.size}:${info.mtimeNs}|${walFingerprint}`,
  }
}

const GEN_MAX_CONTEXT_TOKENS_PATH = [1, 9, 10, 4] as const
const GEN_ESTIMATED_TOKENS_PATH = [1, 9, 10, 1] as const
// gen_metadata [1, 5] (`model_cost`, float32) is intentionally not extracted into Paseo's
// `totalCostUsd`: it is a per-generation scalar with no currency field, conflicting with
// Paseo's cumulative USD cost contract (verified only via ACP usage_update.cost.currency === 'USD').
const SQLITE_BUSY_TIMEOUT_MS = 50
const COMPACTION_CACHE_LIMIT = 128

type CachedCompactionWindow = {
  fingerprint: string
  minGenIdx: number
  result: CompactionWindowResult
}

const compactionCache = new Map<string, CachedCompactionWindow>()

function defaultOpenDb(path: string): DatabaseSync {
  return new DatabaseSync(path, {
    readOnly: true,
    timeout: SQLITE_BUSY_TIMEOUT_MS,
  })
}

/** Read server-issued max_context_tokens and estimated_tokens_used from conversations/<nativeId>.db gen_metadata after minGenIdx. */
export async function readConversationCompactionWindow(
  brainRoot: string,
  nativeId: string,
  minGenIdx = -1,
  options?: ReadConversationCompactionWindowOptions,
): Promise<CompactionWindowResult> {
  if (!sessionIdPattern.test(nativeId)) return { genIdx: minGenIdx }
  let stat: ConversationDbStat
  try {
    stat = await resolveConversationDbStat(brainRoot, nativeId)
  } catch (error) {
    if (error instanceof BrainEscapeError) throw error
    return { genIdx: minGenIdx }
  }
  const hit = compactionCache.get(stat.dbPath)
  if (hit && hit.fingerprint === stat.fingerprint) {
    if (minGenIdx === hit.minGenIdx) return { ...hit.result }
    if (minGenIdx >= hit.result.genIdx) return { genIdx: minGenIdx }
  }
  let genIdx = minGenIdx
  let db: DatabaseSync | undefined
  try {
    const openDb = options?.openDb ?? defaultOpenDb
    db = openDb(stat.dbPath)
    const hasTable = db
      .prepare(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'gen_metadata'",
      )
      .get()
    let maxTokens: number | undefined
    let estimatedTokens: number | undefined
    if (hasTable !== undefined) {
      // Single snapshot-atomic query strictly ordered by idx DESC:
      // - Prevents cross-query WAL snapshot splits between max(idx) and row data.
      // - Avoids size-prioritized ordering that would skip the full latest row for stale older rows.
      const stmt = db.prepare(
        'SELECT idx, data FROM gen_metadata WHERE idx > ? ORDER BY idx DESC',
      )
      for (const row of stmt.iterate(minGenIdx) as Iterable<{
        idx?: unknown
        data?: unknown
      }>) {
        if (
          typeof row.idx === 'number' &&
          Number.isInteger(row.idx) &&
          row.idx >= 0
        ) {
          genIdx = Math.max(genIdx, row.idx)
        }
        if (!(row.data instanceof Uint8Array)) continue
        if (maxTokens === undefined) {
          maxTokens = readProtoVarintPath(row.data, GEN_MAX_CONTEXT_TOKENS_PATH)
        }
        if (estimatedTokens === undefined) {
          estimatedTokens = readProtoVarintPath(
            row.data,
            GEN_ESTIMATED_TOKENS_PATH,
          )
        }
        if (maxTokens !== undefined) break
      }
    }
    const result: CompactionWindowResult = {
      ...(maxTokens !== undefined ? { maxTokens } : {}),
      ...(estimatedTokens !== undefined ? { estimatedTokens } : {}),
      genIdx,
    }
    setBounded(
      compactionCache,
      stat.dbPath,
      { fingerprint: stat.fingerprint, minGenIdx, result: { ...result } },
      COMPACTION_CACHE_LIMIT,
    )
    return result
  } catch {
    return { genIdx }
  } finally {
    db?.close()
  }
}

export interface StepErrorBaseline {
  readonly maxIdx: number
  readonly dev?: bigint
  readonly ino?: bigint
}

export type HiddenApiErrorKind =
  'quota_exhausted' | 'invalid_thinking_signature' | 'api_error' | 'refusal'

export interface HiddenApiError {
  readonly kind: HiddenApiErrorKind
  readonly message: string
  readonly retryAfterMs?: number
  readonly resetAt?: string
  readonly statusCode?: number
  readonly statusName?: string
  readonly reachedModel: boolean
}

const GO_DURATION_PATTERN = /^(?:\d+(?:\.\d+)?(?:ns|[µμ]s|ms|s|m|h))+$/
const GO_DURATION_PART_PATTERN = /(\d+(?:\.\d+)?)(ns|[µμ]s|ms|s|m|h)/g
const MAX_ERROR_MESSAGE_LENGTH = 512
const INVALID_THINKING_SIGNATURE_PATTERN =
  /\bmessages\.\d+\.content\.\d+(?:\.thinking\.signature: Field required|: Invalid `signature` in `thinking` block)/
const QUOTA_EXHAUSTED_PATTERN =
  /\breason:QUOTA_EXHAUSTED\b|\bRESOURCE_EXHAUSTED\b/
const REFUSAL_QUOTA_CUE_PATTERN =
  /\b(?:quota|rate[\s_-]?limit|too many requests|429)\b/i
const QUOTA_RESET_TIMESTAMP_PATTERN =
  /quotaResetTimeStamp["'=:\s\x00-\x1f]+([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+Z?)/
const QUOTA_RESET_DELAY_PATTERN =
  /(?:quotaResetDelay|retryDelay)["'=:\s\x00-\x1f]+([0-9.]+(?:ns|[µμ]s|ms|s|m|h)(?:[0-9.]+(?:ns|[µμ]s|ms|s|m|h))*)/
const GO_ERROR_PATTERN = /Error (\d{3}), Message: (.+?), Status: ([A-Z_]+)/s
const AGY_ERROR_PATTERN = /(?<![A-Z_])([A-Z_]+) \(code (\d{3})\): (\P{Cc}+)/u

/** Parse a nonnegative Go duration string (e.g. `1h2m3.5s`, `500µs`, `500μs`) into milliseconds. */
export function parseGoDurationMs(raw: string): number | undefined {
  if (!GO_DURATION_PATTERN.test(raw)) return undefined
  let totalMs = 0
  for (const match of raw.matchAll(GO_DURATION_PART_PATTERN)) {
    const value = Number(match[1])
    if (!Number.isFinite(value) || value < 0) return undefined
    const unit = match[2]
    if (unit === 'ns') totalMs += value / 1_000_000
    else if (unit === 'µs' || unit === 'μs') totalMs += value / 1_000
    else if (unit === 'ms') totalMs += value
    else if (unit === 's') totalMs += value * 1_000
    else if (unit === 'm') totalMs += value * 60_000
    else if (unit === 'h') totalMs += value * 3_600_000
    else return undefined
  }
  return Number.isFinite(totalMs) && totalMs >= 0 ? totalMs : undefined
}

function sanitizePrintable(raw: string): string {
  return raw
    .replace(/[\p{Cc}\uFFFD]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

function capDetachedMessage(raw: string): string {
  return detachString(
    raw.length > MAX_ERROR_MESSAGE_LENGTH
      ? `${raw.slice(0, MAX_ERROR_MESSAGE_LENGTH - 1)}…`
      : raw,
  )
}

function isNonTerminalRetryPayload(raw: string): boolean {
  const trimmed = raw.trim()
  if (
    isRetryWarning({
      type: 'ERROR_MESSAGE',
      status: 'DONE',
      content: trimmed,
    })
  ) {
    return true
  }
  const stripped = raw
    .replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\uFFFD]+/g, '')
    .trim()
  return (
    stripped !== trimmed &&
    isRetryWarning({
      type: 'ERROR_MESSAGE',
      status: 'DONE',
      content: stripped,
    })
  )
}

function parseStructuredApiError(text: string):
  | {
      statusCode: number
      statusName: string
      detail: string
    }
  | undefined {
  const goMatch = text.match(GO_ERROR_PATTERN)
  if (goMatch) {
    return {
      statusCode: Number(goMatch[1]),
      detail: sanitizePrintable(goMatch[2]),
      statusName: goMatch[3],
    }
  }
  const agyMatch = text.match(AGY_ERROR_PATTERN)
  if (agyMatch) {
    return {
      statusName: agyMatch[1],
      statusCode: Number(agyMatch[2]),
      detail: sanitizePrintable(agyMatch[3]),
    }
  }
  return undefined
}

/** Classify a hidden SQLite error step (`step_type === 17`) or ACP `refusal` stop reason. */
export function classifyStepError(input: {
  stepType?: number
  payloadText?: string
  stopReason?: string
  reachedModel: boolean
}): HiddenApiError | undefined {
  const isErrorStep = input.stepType === 17
  const isRefusal = input.stopReason === 'refusal'
  if (!isErrorStep && !isRefusal) return undefined

  const text = input.payloadText ?? ''
  if (isErrorStep && !isRefusal && text && isNonTerminalRetryPayload(text)) {
    return undefined
  }

  const structured = text ? parseStructuredApiError(text) : undefined

  if (text && INVALID_THINKING_SIGNATURE_PATTERN.test(text)) {
    return {
      kind: 'invalid_thinking_signature',
      message: detachString(
        'Corrupted thinking block signature remains in native session history. Start a new session to continue.',
      ),
      ...(structured
        ? {
            statusCode: structured.statusCode,
            statusName: detachString(structured.statusName),
          }
        : {}),
      reachedModel: input.reachedModel,
    }
  }

  const isQuota =
    (text !== '' && QUOTA_EXHAUSTED_PATTERN.test(text)) ||
    structured?.statusCode === 429 ||
    structured?.statusName === 'RESOURCE_EXHAUSTED' ||
    structured?.statusName === 'QUOTA_EXHAUSTED' ||
    (isRefusal && text !== '' && REFUSAL_QUOTA_CUE_PATTERN.test(text))
  if (isQuota) {
    const resetAtMatch = text.match(QUOTA_RESET_TIMESTAMP_PATTERN)?.[1]
    const delayRaw = text.match(QUOTA_RESET_DELAY_PATTERN)?.[1]
    const retryAfterMs =
      delayRaw !== undefined ? parseGoDurationMs(delayRaw) : undefined
    const details: string[] = []
    if (resetAtMatch) details.push(`resets at ${resetAtMatch}`)
    if (retryAfterMs !== undefined && delayRaw)
      details.push(`retry in ${delayRaw}`)
    const message =
      details.length > 0
        ? `Quota exhausted or rate limited (${details.join(', ')}).`
        : 'Quota exhausted or rate limited.'
    return {
      kind: 'quota_exhausted',
      message: capDetachedMessage(message),
      ...(retryAfterMs !== undefined ? { retryAfterMs } : {}),
      ...(resetAtMatch ? { resetAt: detachString(resetAtMatch) } : {}),
      ...(structured
        ? {
            statusCode: structured.statusCode,
            statusName: detachString(structured.statusName),
          }
        : {}),
      reachedModel: input.reachedModel,
    }
  }

  if (structured) {
    const summary = structured.detail
      ? `${structured.statusName} (${structured.statusCode}): ${structured.detail}`
      : `${structured.statusName} (${structured.statusCode})`
    return {
      kind: 'api_error',
      message: capDetachedMessage(summary),
      statusCode: structured.statusCode,
      statusName: detachString(structured.statusName),
      reachedModel: input.reachedModel,
    }
  }

  if (isRefusal) {
    return input.reachedModel
      ? {
          kind: 'refusal',
          message: detachString('Model refused to complete the turn.'),
          reachedModel: true,
        }
      : {
          kind: 'quota_exhausted',
          message: detachString(
            'Request was refused before reaching the model (quota exhausted or rate limited).',
          ),
          reachedModel: false,
        }
  }

  const summary = sanitizePrintable(text)
  return {
    kind: 'api_error',
    message: capDetachedMessage(
      summary || 'Upstream API error recorded by agy',
    ),
    reachedModel: input.reachedModel,
  }
}

function decodeSqlText(value: unknown): string {
  if (typeof value === 'string') return value
  if (value instanceof Uint8Array) return Buffer.from(value).toString('utf8')
  return ''
}

/** Fail-closed pre-turn baseline capturing max `steps.idx` and file `(dev, ino)` identity. */
export async function captureConversationStepBaseline(
  brainRoot: string,
  nativeId: string,
  options?: { openDb?: (path: string) => DatabaseSync },
): Promise<StepErrorBaseline> {
  if (!sessionIdPattern.test(nativeId)) return { maxIdx: -1 }
  let stat: ConversationDbStat
  try {
    stat = await resolveConversationDbStat(brainRoot, nativeId)
  } catch {
    return { maxIdx: -1 }
  }
  let db: DatabaseSync | undefined
  try {
    const openDb = options?.openDb ?? defaultOpenDb
    db = openDb(stat.dbPath)
    const hasSteps = db
      .prepare(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'steps'",
      )
      .get()
    if (hasSteps === undefined) return { maxIdx: -1 }
    const row = db
      .prepare('SELECT COALESCE(MAX(idx), -1) AS maxIdx FROM steps')
      .get() as { maxIdx?: unknown } | undefined
    const maxIdx =
      typeof row?.maxIdx === 'number' &&
      Number.isInteger(row.maxIdx) &&
      row.maxIdx >= -1
        ? row.maxIdx
        : -1
    return { maxIdx, dev: stat.dev, ino: stat.ino }
  } catch {
    return { maxIdx: -1 }
  } finally {
    db?.close()
  }
}

/** Fail-closed post-turn check for hidden SQLite `steps` API errors newer than `baseline`. */
export async function readConversationStepError(
  brainRoot: string,
  nativeId: string,
  baseline: StepErrorBaseline,
  stopReason: string | undefined,
  options?: { openDb?: (path: string) => DatabaseSync },
): Promise<HiddenApiError | undefined> {
  const fallback = () =>
    stopReason === 'refusal'
      ? classifyStepError({ stopReason: 'refusal', reachedModel: false })
      : undefined
  if (!sessionIdPattern.test(nativeId)) return fallback()
  let stat: ConversationDbStat
  try {
    stat = await resolveConversationDbStat(brainRoot, nativeId)
  } catch {
    return fallback()
  }
  if (
    (baseline.dev !== undefined || baseline.ino !== undefined) &&
    (stat.dev !== baseline.dev || stat.ino !== baseline.ino)
  ) {
    return fallback()
  }
  let db: DatabaseSync | undefined
  try {
    const openDb = options?.openDb ?? defaultOpenDb
    db = openDb(stat.dbPath)
    const columns = db.prepare('PRAGMA table_info(steps)').all() as Array<{
      name?: unknown
    }>
    if (columns.length === 0) return fallback()
    const hasErrorDetails = columns.some((col) => col.name === 'error_details')
    const latest = db
      .prepare(
        hasErrorDetails
          ? 'SELECT step_type AS type, substr(step_payload, 1, 16384) AS payload, substr(error_details, 1, 8192) AS errDetails FROM steps WHERE idx > ? ORDER BY idx DESC LIMIT 1'
          : 'SELECT step_type AS type, substr(step_payload, 1, 16384) AS payload FROM steps WHERE idx > ? ORDER BY idx DESC LIMIT 1',
      )
      .get(baseline.maxIdx) as
      { type?: unknown; payload?: unknown; errDetails?: unknown } | undefined
    const reachedModel =
      db
        .prepare('SELECT 1 FROM steps WHERE idx > ? AND step_type = 15 LIMIT 1')
        .get(baseline.maxIdx) !== undefined
    const stepType =
      typeof latest?.type === 'number' && Number.isInteger(latest.type)
        ? latest.type
        : undefined
    const payloadPart = decodeSqlText(latest?.payload)
    const errDetailsPart = decodeSqlText(latest?.errDetails)
    const payloadText =
      payloadPart && errDetailsPart
        ? `${payloadPart}\n${errDetailsPart}`
        : payloadPart || errDetailsPart || undefined
    return classifyStepError({
      stepType,
      payloadText,
      stopReason,
      reachedModel,
    })
  } catch {
    return fallback()
  } finally {
    db?.close()
  }
}
