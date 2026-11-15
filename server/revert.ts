/** Session revert orchestration: token validation, DB/Brain conversation forking, and Git file rollback. */
import { join } from 'node:path'
import { z } from 'zod'
import {
  conversationForkNeedsReload,
  deleteConversationSummaryRow,
  extractUserStepRows,
  forkAgyConversationStorage,
  type UserStepRow,
} from './agy-fork.ts'
import { readTranscriptRows } from './brain.ts'
import {
  findCheckpointForUserStep,
  forkSessionCheckpoints,
  isGitWorkspace,
  readSessionCheckpoints,
  restoreSessionFileCheckpoint,
  validateSessionFileCheckpoint,
  type TurnFileCheckpoint,
} from './checkpoints.ts'
import { assertSafeNativeId, isMissingError, num } from './common.ts'
import { normalizeUserText, type HistoryEntry } from './history.ts'
import { removeSessionArtifacts } from './preflight.ts'

export { extractUserStepRows, type UserStepRow }

export type RevertScope = 'files' | 'conversation' | 'both'

const AgyRevertTokenSchema = z.object({
  version: z.literal(1),
  kind: z.literal('agy-acp-turn'),
  clientMessageId: z.string().min(1).optional(),
  afterStepIndex: z.number().int().min(-1).optional(),
  stepIndex: z.number().int().min(0).optional(),
  turnOrdinal: z.number().int().min(0).optional(),
  checkpointId: z.string().min(1).optional(),
  text: z.string().optional(),
})

type AgyRevertToken = z.infer<typeof AgyRevertTokenSchema>

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

function parseRevertToken(token: unknown): AgyRevertToken {
  const parsed = AgyRevertTokenSchema.safeParse(token)
  if (!parsed.success) {
    throw new Error('Invalid or missing revert token for this message')
  }
  return parsed.data
}

/** Resolve the native step_index at which the target turn starts (exclusive upper bound to keep). */
async function resolveCutoffStepIndex(
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
  const stepIndices = rows
    .map((r) => num(r.step_index))
    .filter(
      (n): n is number => n !== undefined && Number.isInteger(n) && n >= 0,
    )
  const maxStepIndex = stepIndices.length > 0 ? Math.max(...stepIndices) : -1
  const normalizeCutoff = (stepIndex: number): number =>
    userSteps.length === 0 || stepIndex <= userSteps[0].stepIndex
      ? 0
      : stepIndex

  if (token.stepIndex !== undefined) {
    if (userSteps.length === 0) return token.stepIndex
    const exact = userSteps.find((s) => s.stepIndex === token.stepIndex)
    if (exact) return normalizeCutoff(exact.stepIndex)
  }

  if (token.afterStepIndex !== undefined) {
    const postSteps = stepIndices.filter((idx) => idx > token.afterStepIndex!)
    if (postSteps.length > 0) {
      const minPostStep = Math.min(...postSteps)
      const afterMatches = userSteps.filter(
        (s) => s.stepIndex > token.afterStepIndex!,
      )
      const exactText =
        token.text !== undefined
          ? afterMatches.find((s) => s.text === token.text)
          : undefined
      if (exactText) {
        const hasEarlierUserAfter = afterMatches.some(
          (s) => s.stepIndex < exactText.stepIndex,
        )
        return normalizeCutoff(
          token.afterStepIndex >= 0 && !hasEarlierUserAfter
            ? Math.min(minPostStep, exactText.stepIndex)
            : exactText.stepIndex,
        )
      }
      return normalizeCutoff(minPostStep)
    }
    if (token.afterStepIndex === -1) return 0
    if (token.afterStepIndex >= 0 && token.afterStepIndex <= maxStepIndex) {
      return normalizeCutoff(token.afterStepIndex + 1)
    }
  }

  if (userSteps.length === 0) {
    if (token.turnOrdinal === 0) return 0
    throw new Error('Unable to locate target turn in session transcript')
  }

  let matched: UserStepRow | undefined

  if (token.turnOrdinal !== undefined && token.turnOrdinal < userSteps.length) {
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
  return normalizeCutoff(matched.stepIndex)
}

export async function removeForkedConversation(
  appDataDir: string,
  nativeId: string,
): Promise<void> {
  assertSafeNativeId(nativeId)
  await Promise.all([
    removeSessionArtifacts(appDataDir, nativeId).catch(() => undefined),
    deleteConversationSummaryRow(appDataDir, nativeId).catch(() => undefined),
  ])
}

async function resolveTargetCheckpointId(
  appDataDir: string,
  nativeId: string,
  brainRoot: string,
  cutoffStepIndex: number | undefined,
  parsed: AgyRevertToken,
  checkpointsOverride?: readonly TurnFileCheckpoint[],
): Promise<string | undefined> {
  if (parsed.checkpointId !== undefined) return parsed.checkpointId
  if (cutoffStepIndex === undefined) return undefined

  const [checkpoints, rows] = await Promise.all([
    checkpointsOverride ?? readSessionCheckpoints(appDataDir, nativeId),
    readTranscriptRows(brainRoot, nativeId).catch(
      () => [] as Record<string, unknown>[],
    ),
  ])
  const userSteps = extractUserStepRows(rows)
  const prevUserStep =
    userSteps.filter((u) => u.stepIndex < cutoffStepIndex).at(-1)?.stepIndex ??
    -1
  const targetUserStep = userSteps.find(
    (u) => u.stepIndex >= cutoffStepIndex,
  )?.stepIndex
  const targetStep = targetUserStep ?? parsed.stepIndex ?? cutoffStepIndex
  return findCheckpointForUserStep(checkpoints, prevUserStep, targetStep)?.id
}

export async function revertSession(options: {
  appDataDir: string
  nativeId: string
  cwd?: string
  scope: RevertScope
  token: unknown
  checkpoints?: readonly TurnFileCheckpoint[]
  supportsReload?: boolean
  deferFileRestore?: boolean
  createFreshSession: () => Promise<string>
}): Promise<{
  newNativeId?: string
  cutoffStepIndex?: number
  checkpointId?: string
}> {
  const { appDataDir, nativeId, cwd, scope, createFreshSession } = options
  const parsed = parseRevertToken(options.token)
  const brainRoot = join(appDataDir, 'brain')

  let checkpointId = parsed.checkpointId
  let cutoffStepIndex: number | undefined

  if (scope !== 'files' || checkpointId === undefined) {
    cutoffStepIndex = await resolveCutoffStepIndex(brainRoot, nativeId, parsed)
  }

  if (
    scope !== 'files' &&
    options.supportsReload === false &&
    (await conversationForkNeedsReload(appDataDir, nativeId, cutoffStepIndex!))
  ) {
    throw new Error('agy does not support session/load')
  }

  if (scope === 'files' || scope === 'both') {
    if (cwd !== undefined && !(await isGitWorkspace(cwd))) {
      throw new Error('Workspace file rollback requires a Git repository')
    }
    if (!checkpointId) {
      checkpointId = await resolveTargetCheckpointId(
        appDataDir,
        nativeId,
        brainRoot,
        cutoffStepIndex,
        parsed,
        options.checkpoints,
      )
    }
    if (!checkpointId) {
      throw new Error('No workspace file checkpoint recorded for this message')
    }
    // Validate checkpoint readability before forking so missing backups leave history and files intact.
    await validateSessionFileCheckpoint(appDataDir, nativeId, checkpointId)
  }

  if (scope === 'files') {
    await restoreSessionFileCheckpoint(appDataDir, nativeId, checkpointId!)
    return {}
  }

  const { newNativeId, forked, retainedCutoffStepIndex } =
    await forkAgyConversationStorage({
      appDataDir,
      oldNativeId: nativeId,
      cutoffStepIndex: cutoffStepIndex!,
      createFreshSession,
    })

  if (forked) {
    try {
      await forkSessionCheckpoints(
        appDataDir,
        nativeId,
        newNativeId,
        retainedCutoffStepIndex,
      )
    } catch (error) {
      if (newNativeId !== nativeId) {
        await removeForkedConversation(appDataDir, newNativeId).catch(
          () => undefined,
        )
      }
      throw error
    }
  }

  if (scope === 'both' && !options.deferFileRestore) {
    try {
      await restoreSessionFileCheckpoint(appDataDir, nativeId, checkpointId!)
    } catch (error) {
      if (newNativeId !== nativeId) {
        await removeForkedConversation(appDataDir, newNativeId).catch(
          () => undefined,
        )
      }
      throw error
    }
  }

  return {
    newNativeId,
    cutoffStepIndex: cutoffStepIndex!,
    ...(scope === 'both' && checkpointId ? { checkpointId } : {}),
  }
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
