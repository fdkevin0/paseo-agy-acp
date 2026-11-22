import { lstat, realpath } from 'node:fs/promises'
import { dirname, join, sep } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { BrainEscapeError, isMissing } from './brain-io.ts'
import { sessionIdPattern, setBounded } from './common.ts'

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
    if (minGenIdx === hit.minGenIdx) return hit.result
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
      { fingerprint: stat.fingerprint, minGenIdx, result },
      COMPACTION_CACHE_LIMIT,
    )
    return result
  } catch {
    return { genIdx }
  } finally {
    db?.close()
  }
}
