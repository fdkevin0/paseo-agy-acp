/** Session revert orchestration: token validation, DB/Brain conversation forking, and file rollback. */
import { randomUUID } from 'node:crypto'
import {
  copyFile,
  lstat,
  mkdir,
  readFile,
  readdir,
  realpath,
  rename,
  rm,
  writeFile,
} from 'node:fs/promises'
import { join, sep } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { z } from 'zod'
import { readTranscriptRows } from './brain.ts'
import {
  findCheckpointForUserStep,
  forkSessionCheckpoints,
  restoreSessionFileCheckpoint,
  type TurnFileCheckpoint,
} from './checkpoints.ts'
import { record, sessionIdPattern } from './common.ts'
import { normalizeUserText, type HistoryEntry } from './history.ts'

export type RevertScope = 'files' | 'conversation' | 'both'

export const AgyRevertTokenSchema = z.object({
  version: z.literal(1),
  kind: z.literal('agy-acp-turn'),
  clientMessageId: z.string().min(1).optional(),
  afterStepIndex: z.number().int().min(-1).optional(),
  stepIndex: z.number().int().min(0).optional(),
  turnOrdinal: z.number().int().min(0).optional(),
  checkpointId: z.string().min(1).optional(),
  text: z.string().optional(),
})

export type AgyRevertToken = z.infer<typeof AgyRevertTokenSchema>

const STEP_INDEXED_TABLES = [
  'steps',
  'gen_metadata',
  'executor_metadata',
  'parent_references',
  'battle_mode_infos',
] as const

interface UserStepRow {
  stepIndex: number
  text: string
}

function isMissingError(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | undefined)?.code
  return code === 'ENOENT' || code === 'ENOTDIR'
}

function assertSafeNativeId(nativeId: string) {
  if (!sessionIdPattern.test(nativeId)) {
    throw new Error(`Invalid session ID: ${nativeId}`)
  }
}

async function assertWithinDir(rootDir: string, targetPath: string) {
  const base = await realpath(rootDir)
  const resolved = await realpath(targetPath)
  const prefix = base === sep ? sep : base + sep
  if (!resolved.startsWith(prefix)) {
    throw new Error(`Path escapes directory: ${targetPath}`)
  }
  return resolved
}

export function makeRevertToken(fields: {
  clientMessageId?: string
  afterStepIndex?: number
  stepIndex?: number
  turnOrdinal?: number
  checkpointId?: string
  text?: string
}): AgyRevertToken {
  const token: AgyRevertToken = {
    version: 1,
    kind: 'agy-acp-turn',
  }
  if (fields.clientMessageId !== undefined)
    token.clientMessageId = fields.clientMessageId
  if (fields.afterStepIndex !== undefined)
    token.afterStepIndex = fields.afterStepIndex
  if (fields.stepIndex !== undefined) token.stepIndex = fields.stepIndex
  if (fields.turnOrdinal !== undefined) token.turnOrdinal = fields.turnOrdinal
  if (fields.checkpointId !== undefined)
    token.checkpointId = fields.checkpointId
  if (fields.text !== undefined) token.text = fields.text
  return token
}

export function parseRevertToken(token: unknown): AgyRevertToken {
  const parsed = AgyRevertTokenSchema.safeParse(token)
  if (!parsed.success) {
    throw new Error('Invalid or missing revert token for this message')
  }
  return parsed.data
}

export function extractUserStepRows(
  rows: readonly Record<string, unknown>[],
): UserStepRow[] {
  const result: UserStepRow[] = []
  for (const row of rows) {
    if (row.type !== 'USER_INPUT' || typeof row.content !== 'string') continue
    if (!row.content) continue
    const stepIndex = Number(row.step_index)
    if (!Number.isInteger(stepIndex) || stepIndex < 0) continue
    result.push({
      stepIndex,
      text: normalizeUserText(row.content),
    })
  }
  return result
}

/** Resolve the native step_index at which the target user turn starts (exclusive upper bound to keep). */
export async function resolveCutoffStepIndex(
  brainRoot: string,
  nativeId: string,
  token: AgyRevertToken,
): Promise<number> {
  assertSafeNativeId(nativeId)
  const rows = await readTranscriptRows(brainRoot, nativeId).catch((error) => {
    if (isMissingError(error)) return [] as Record<string, unknown>[]
    throw error
  })
  const userSteps = extractUserStepRows(rows)

  if (token.stepIndex !== undefined) {
    if (userSteps.length === 0) return token.stepIndex
    const exact = userSteps.find((s) => s.stepIndex === token.stepIndex)
    if (exact) {
      return exact.stepIndex <= userSteps[0].stepIndex ? 0 : exact.stepIndex
    }
  }

  if (userSteps.length === 0) {
    if (token.turnOrdinal === 0 || token.afterStepIndex === -1) return 0
    throw new Error('Unable to locate target turn in session transcript')
  }

  let matched: UserStepRow | undefined

  if (token.afterStepIndex !== undefined) {
    const afterMatches = userSteps.filter(
      (s) => s.stepIndex > token.afterStepIndex!,
    )
    if (afterMatches.length > 0) {
      matched =
        (token.text !== undefined
          ? afterMatches.find((s) => s.text === token.text)
          : undefined) ?? afterMatches[0]
    }
  }

  if (
    !matched &&
    token.turnOrdinal !== undefined &&
    token.turnOrdinal < userSteps.length
  ) {
    const candidate = userSteps[token.turnOrdinal]
    if (token.text === undefined || candidate.text === token.text) {
      matched = candidate
    }
  }

  if (!matched && token.text !== undefined) {
    const byText = userSteps.filter((s) => s.text === token.text)
    if (byText.length === 1) matched = byText[0]
  }

  if (!matched) {
    throw new Error('Unable to locate target turn in session transcript')
  }

  // Rewinding the very first user turn leaves no conversation history; start clean.
  return matched.stepIndex <= userSteps[0].stepIndex ? 0 : matched.stepIndex
}

async function filterJsonlTranscript(
  srcPath: string,
  destPath: string,
  cutoffStepIndex: number,
) {
  const raw = await readFile(srcPath, 'utf8')
  const kept: string[] = []
  let truncated = false
  for (const line of raw.split(/\r?\n/)) {
    if (!line.trim()) continue
    try {
      const parsed = JSON.parse(line) as unknown
      const stepIndex = Number(record(parsed).step_index)
      if (Number.isInteger(stepIndex) && stepIndex >= cutoffStepIndex) {
        truncated = true
        continue
      }
      if (truncated && !Number.isInteger(stepIndex)) continue
      kept.push(line)
    } catch {
      if (!truncated) kept.push(line)
    }
  }
  await writeFile(destPath, kept.length > 0 ? `${kept.join('\n')}\n` : '')
}

async function filterBrainSystemSubagents(
  srcDir: string,
  destDir: string,
  cutoffStepIndex: number,
) {
  await mkdir(destDir, { recursive: true })
  const entries = await readdir(srcDir, { withFileTypes: true })
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith('.json')) continue
    const srcFile = join(srcDir, entry.name)
    try {
      const raw = JSON.parse(await readFile(srcFile, 'utf8')) as Record<
        string,
        unknown
      >
      const spawnStepIndex = Number(raw.spawnStepIndex)
      if (
        Number.isInteger(spawnStepIndex) &&
        spawnStepIndex >= cutoffStepIndex
      ) {
        continue
      }
    } catch {
      // Keep unreadable descriptor files as best effort.
    }
    await copyFile(srcFile, join(destDir, entry.name))
  }
}

async function filterBrainMessages(
  srcDir: string,
  destDir: string,
  cutoffStepIndex: number,
) {
  await mkdir(destDir, { recursive: true })
  const entries = await readdir(srcDir, { withFileTypes: true })
  for (const entry of entries) {
    if (!entry.isFile()) continue
    const srcFile = join(srcDir, entry.name)
    if (entry.name.endsWith('.json') && entry.name !== 'read.json') {
      try {
        const raw = JSON.parse(await readFile(srcFile, 'utf8')) as Record<
          string,
          unknown
        >
        const tool = record(raw.tool)
        const stepIndex = Number(tool.stepIndex)
        if (Number.isInteger(stepIndex) && stepIndex >= cutoffStepIndex) {
          continue
        }
      } catch {
        // Keep unreadable message files as best effort.
      }
    }
    await copyFile(srcFile, join(destDir, entry.name))
  }
}

async function copyBrainDirectoryFiltered(
  srcDir: string,
  destDir: string,
  cutoffStepIndex: number,
  relParts: readonly string[] = [],
): Promise<void> {
  const entries = await readdir(srcDir, { withFileTypes: true })
  await mkdir(destDir, { recursive: true })
  for (const entry of entries) {
    if (entry.isSymbolicLink()) continue
    const srcEntry = join(srcDir, entry.name)
    const destEntry = join(destDir, entry.name)
    const nextParts = [...relParts, entry.name]
    const relPath = nextParts.join('/')

    if (relPath === '.system_generated/subagents' && entry.isDirectory()) {
      await filterBrainSystemSubagents(srcEntry, destEntry, cutoffStepIndex)
      continue
    }
    if (relPath === '.system_generated/messages' && entry.isDirectory()) {
      await filterBrainMessages(srcEntry, destEntry, cutoffStepIndex)
      continue
    }
    if (
      relParts.length === 2 &&
      relParts[0] === '.system_generated' &&
      relParts[1] === 'steps'
    ) {
      const numericName = Number.parseInt(
        entry.name.replace(/\.[^.]+$/, ''),
        10,
      )
      if (
        Number.isInteger(numericName) &&
        String(numericName) === entry.name.replace(/\.[^.]+$/, '') &&
        numericName >= cutoffStepIndex
      ) {
        continue
      }
    }

    if (entry.isDirectory()) {
      await copyBrainDirectoryFiltered(
        srcEntry,
        destEntry,
        cutoffStepIndex,
        nextParts,
      )
    } else if (entry.isFile()) {
      if (
        relPath === '.system_generated/logs/transcript.jsonl' ||
        relPath === '.system_generated/logs/transcript_full.jsonl'
      ) {
        await filterJsonlTranscript(srcEntry, destEntry, cutoffStepIndex)
      } else {
        await copyFile(srcEntry, destEntry)
      }
    }
  }
}

/**
 * Fork the native Agy SQLite DB + Brain directory up to `cutoffStepIndex` (keeping `idx < cutoffStepIndex`),
 * or allocate a brand-new empty native session when `cutoffStepIndex <= 0`.
 */
export async function forkNativeConversation(
  appDataDir: string,
  oldNativeId: string,
  cutoffStepIndex: number,
  createFreshSession: () => Promise<string>,
): Promise<string> {
  assertSafeNativeId(oldNativeId)
  if (cutoffStepIndex <= 0) {
    return await createFreshSession()
  }

  const conversationsDir = join(appDataDir, 'conversations')
  const oldDbPath = join(conversationsDir, `${oldNativeId}.db`)
  const oldDbStat = await lstat(oldDbPath).catch((error) => {
    if (isMissingError(error)) return undefined
    throw error
  })

  if (!oldDbStat) {
    // No native SQLite DB yet; only create a fresh session if there's nothing to fork.
    return await createFreshSession()
  }
  await assertWithinDir(conversationsDir, oldDbPath)

  const newNativeId = randomUUID()
  const targetDbPath = join(conversationsDir, `${newNativeId}.db`)
  const tempDbPath = join(
    conversationsDir,
    `${newNativeId}.db.tmp-${process.pid}`,
  )
  const newBrainDir = join(appDataDir, 'brain', newNativeId)
  let remainingSteps = -1

  try {
    await mkdir(conversationsDir, { recursive: true })
    const sourceDb = new DatabaseSync(oldDbPath)
    try {
      sourceDb.exec('PRAGMA busy_timeout = 3000')
      const escapedTemp = tempDbPath.replaceAll("'", "''")
      sourceDb.exec(`VACUUM INTO '${escapedTemp}'`)
    } finally {
      sourceDb.close()
    }

    const tempDb = new DatabaseSync(tempDbPath)
    try {
      tempDb.exec('PRAGMA busy_timeout = 3000')
      const tableRows = tempDb
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .all() as Array<{ name: unknown }>
      const tables = new Set(
        tableRows
          .map((r) => r.name)
          .filter((n): n is string => typeof n === 'string'),
      )

      if (tables.has('steps')) {
        const countRow = tempDb
          .prepare('SELECT COUNT(*) AS cnt FROM steps WHERE idx < ?')
          .get(cutoffStepIndex) as { cnt?: number | bigint } | undefined
        remainingSteps = Number(countRow?.cnt ?? 0)
      }

      if (remainingSteps === 0) {
        tempDb.close()
        await rm(tempDbPath, { force: true })
        return await createFreshSession()
      }

      tempDb.exec('BEGIN IMMEDIATE')
      try {
        for (const table of STEP_INDEXED_TABLES) {
          if (!tables.has(table)) continue
          tempDb
            .prepare(`DELETE FROM ${table} WHERE idx >= ?`)
            .run(cutoffStepIndex)
        }
        if (tables.has('trajectory_meta')) {
          const cols = tempDb
            .prepare('PRAGMA table_info(trajectory_meta)')
            .all() as Array<{ name: unknown }>
          const colNames = new Set(
            cols
              .map((c) => c.name)
              .filter((n): n is string => typeof n === 'string'),
          )
          if (colNames.has('cascade_id') && colNames.has('trajectory_id')) {
            tempDb
              .prepare(
                'UPDATE trajectory_meta SET cascade_id = ?, trajectory_id = ?',
              )
              .run(newNativeId, randomUUID())
          } else if (colNames.has('cascade_id')) {
            tempDb
              .prepare('UPDATE trajectory_meta SET cascade_id = ?')
              .run(newNativeId)
          }
        }
        tempDb.exec('COMMIT')
      } catch (error) {
        tempDb.exec('ROLLBACK')
        throw error
      }
      tempDb.exec('PRAGMA journal_mode = DELETE')
    } finally {
      if (tempDb.isOpen) tempDb.close()
    }

    await rename(tempDbPath, targetDbPath)

    const brainDir = join(appDataDir, 'brain')
    const oldBrainDir = join(brainDir, oldNativeId)
    const oldBrainStat = await lstat(oldBrainDir).catch((error) => {
      if (isMissingError(error)) return undefined
      throw error
    })
    let retainedCutoff = cutoffStepIndex
    if (oldBrainStat?.isDirectory()) {
      await assertWithinDir(brainDir, oldBrainDir)
      await copyBrainDirectoryFiltered(
        oldBrainDir,
        newBrainDir,
        cutoffStepIndex,
      )
      const keptRows = await readTranscriptRows(brainDir, newNativeId).catch(
        () => [] as Record<string, unknown>[],
      )
      const keptUserSteps = extractUserStepRows(keptRows)
      if (keptUserSteps.length > 0) {
        retainedCutoff = keptUserSteps[keptUserSteps.length - 1].stepIndex
      }
    }

    await forkSessionCheckpoints(
      appDataDir,
      oldNativeId,
      newNativeId,
      retainedCutoff,
    )
    return newNativeId
  } catch (error) {
    await Promise.all([
      rm(tempDbPath, { force: true }).catch(() => undefined),
      rm(`${tempDbPath}-wal`, { force: true }).catch(() => undefined),
      rm(`${tempDbPath}-shm`, { force: true }).catch(() => undefined),
      rm(targetDbPath, { force: true }).catch(() => undefined),
      rm(newBrainDir, { recursive: true, force: true }).catch(() => undefined),
    ])
    throw error
  }
}

export async function revertSession(options: {
  appDataDir: string
  nativeId: string
  scope: RevertScope
  token: unknown
  checkpoints: readonly TurnFileCheckpoint[]
  createFreshSession: () => Promise<string>
}): Promise<{ newNativeId?: string; cutoffStepIndex?: number }> {
  const { appDataDir, nativeId, scope, checkpoints, createFreshSession } =
    options
  const parsed = parseRevertToken(options.token)
  const brainRoot = join(appDataDir, 'brain')

  let checkpointId = parsed.checkpointId
  let cutoffStepIndex: number | undefined

  if (scope !== 'files' || checkpointId === undefined) {
    cutoffStepIndex = await resolveCutoffStepIndex(brainRoot, nativeId, parsed)
  }

  if (scope === 'files' || scope === 'both') {
    if (!checkpointId && cutoffStepIndex !== undefined) {
      const rows = await readTranscriptRows(brainRoot, nativeId).catch(
        () => [] as Record<string, unknown>[],
      )
      const userSteps = extractUserStepRows(rows)
      const userPos =
        cutoffStepIndex === 0 && userSteps.length > 0
          ? 0
          : userSteps.findIndex((u) => u.stepIndex === cutoffStepIndex)
      const targetStep =
        userPos >= 0
          ? userSteps[userPos].stepIndex
          : (parsed.stepIndex ?? cutoffStepIndex)
      const prevUserStep = userPos > 0 ? userSteps[userPos - 1].stepIndex : -1
      checkpointId = findCheckpointForUserStep(
        checkpoints,
        prevUserStep,
        targetStep,
      )?.id
    }
    if (!checkpointId) {
      throw new Error('No workspace file checkpoint recorded for this message')
    }
    // Restore files before forking the conversation so a missing checkpoint leaves history intact.
    await restoreSessionFileCheckpoint(appDataDir, nativeId, checkpointId)
  }

  if (scope === 'files') {
    return {}
  }

  const resolvedCutoff =
    cutoffStepIndex ??
    (await resolveCutoffStepIndex(brainRoot, nativeId, parsed))
  const newNativeId = await forkNativeConversation(
    appDataDir,
    nativeId,
    resolvedCutoff,
    createFreshSession,
  )
  return { newNativeId, cutoffStepIndex: resolvedCutoff }
}

/** Attach `revertToken` to replayed `user_message` entries aligned against Brain user steps and saved checkpoints. */
export function attachReplayRevertTokens(
  entries: HistoryEntry[],
  rows: readonly Record<string, unknown>[],
  checkpoints: readonly TurnFileCheckpoint[],
): HistoryEntry[] {
  const userSteps = extractUserStepRows(rows)
  const byStep = new Map<
    number,
    { ordinal: number; stepIndex: number; text: string }
  >()
  userSteps.forEach((step, ordinal) => {
    byStep.set(step.stepIndex, { ...step, ordinal })
  })

  let fallbackCursor = 0
  return entries.map((entry) => {
    if (entry.item.type !== 'user_message') return entry
    const normalizedText = normalizeUserText(entry.item.text)
    let matchedOrdinal: number | undefined
    let matchedStepIndex: number | undefined

    const stepMatch = entry.item.id.match(/:step:(\d+)$/)
    if (stepMatch) {
      const stepNum = Number.parseInt(stepMatch[1], 10)
      const found = byStep.get(stepNum)
      if (found) {
        matchedOrdinal = found.ordinal
        matchedStepIndex = found.stepIndex
        if (found.ordinal >= fallbackCursor) fallbackCursor = found.ordinal + 1
      } else {
        matchedStepIndex = stepNum
      }
    }

    if (matchedOrdinal === undefined) {
      for (let i = fallbackCursor; i < userSteps.length; i++) {
        if (userSteps[i].text === normalizedText) {
          matchedOrdinal = i
          matchedStepIndex = userSteps[i].stepIndex
          fallbackCursor = i + 1
          break
        }
      }
    }

    if (matchedOrdinal === undefined && fallbackCursor < userSteps.length) {
      const fallback = userSteps[fallbackCursor]
      if (fallback) {
        matchedOrdinal = fallbackCursor
        matchedStepIndex = fallback.stepIndex
        fallbackCursor++
      }
    }

    let checkpointId: string | undefined
    if (matchedStepIndex !== undefined) {
      const prevUserStep =
        matchedOrdinal !== undefined && matchedOrdinal > 0
          ? userSteps[matchedOrdinal - 1].stepIndex
          : -1
      checkpointId = findCheckpointForUserStep(
        checkpoints,
        prevUserStep,
        matchedStepIndex,
      )?.id
    }

    const revertToken = makeRevertToken({
      stepIndex: matchedStepIndex,
      turnOrdinal: matchedOrdinal,
      checkpointId,
      text: normalizedText,
    })

    return {
      ...entry,
      item: {
        ...entry.item,
        revertToken,
      },
    }
  })
}
