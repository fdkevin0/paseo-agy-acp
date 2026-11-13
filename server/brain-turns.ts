import { stat } from 'node:fs/promises'
import { isNativeToolName, sessionIdPattern } from './common.ts'
import {
  isMissing,
  readInboxState,
  readSubagents,
  readTranscriptRows,
  resolveBrainPath,
  type InboxState,
  type SubagentRecord,
} from './brain-io.ts'
import { BrainNode } from './brain-projector.ts'

type InboxMessage = InboxState['messages'][number]

export type ChildTurnState =
  | { status: 'pending' }
  | {
      status: 'active'
      minStepIndex: number
      awaitingTurnWakeup: boolean
    }

export type TaggedWakeupMessage = InboxMessage & {
  tool: { conversationId: string; stepIndex: number; name?: string }
}

type CachedNodeBoundary = {
  mtimeMs: number
  size: number
  beforeStep: number
  emptyRows?: boolean
  localBoundary: number
  grandParentId?: string
  initStepIndex?: number
  tagged: readonly TaggedWakeupMessage[]
  wakeupSteps: readonly number[]
}

export type TurnBoundaryCache = Map<string, CachedNodeBoundary>

export function selectParentWakeups(inbox: InboxState, parentId?: string) {
  if (!parentId)
    return {
      messages: [] as InboxMessage[],
      tagged: [] as TaggedWakeupMessage[],
      untaggedCount: 0,
      allTagged: false,
    }
  const messages = inbox.messages.filter(
    (m) =>
      m.sender === parentId &&
      !isNativeToolName(m.tool?.name ?? '', 'invoke_subagent') &&
      !m.hideFromUser &&
      m.priority !== 'MESSAGE_PRIORITY_LOW',
  )
  const tagged = messages
    .filter(
      (m): m is TaggedWakeupMessage =>
        m.tool?.conversationId === parentId && m.tool.stepIndex !== undefined,
    )
    .sort((a, b) => a.tool.stepIndex - b.tool.stepIndex)
  const untaggedCount = messages.length - tagged.length
  return {
    messages,
    tagged,
    untaggedCount,
    allTagged: tagged.length > 0 && untaggedCount === 0,
  }
}

export function resolveTurnWakeupStartStep(
  tagged: readonly TaggedWakeupMessage[],
  wakeupSteps: readonly number[],
  parentBoundary: number,
  ingestedCount: number,
): number | undefined {
  const turnTagged = tagged
    .slice(0, ingestedCount)
    .filter((m) => m.tool.stepIndex > parentBoundary).length
  return turnTagged > 0 ? wakeupSteps[ingestedCount - turnTagged] : undefined
}

/**
 * Compute the child transcript step floor (`TurnFloor`) for the current parent turn, or `'pending'`
 * when the parent re-woke a child spawned in an earlier turn whose first wakeup step has not landed yet.
 *
 * Correlation invariant: `agy` tags each `send_message` inbox file with `tool.conversationId` and
 * `tool.stepIndex` (the parent's `GENERIC` step). Messages with `tool.stepIndex <= parentMinStepIndex`
 * belong to prior parent turns; messages with `tool.stepIndex > parentMinStepIndex` belong to the active
 * parent turn. A single untagged fallback handles legacy/synthetic fixtures that omit `tool.stepIndex`.
 */
export function resolveChildTurnState(
  parentNode: BrainNode,
  parentMinStepIndex: number,
  subRecord: SubagentRecord | undefined,
  childNode: BrainNode,
  childInbox: InboxState,
): ChildTurnState {
  const spawnedBeforeTurn =
    subRecord !== undefined && subRecord.spawnStepIndex <= parentMinStepIndex
  if (!spawnedBeforeTurn) {
    return {
      status: 'active',
      minStepIndex: childNode.turnStartStep,
      awaitingTurnWakeup: false,
    }
  }
  const expectedTurnWakeups =
    parentNode.projector.countCompletedWakeups(
      childNode.nativeId,
      parentMinStepIndex,
    ) +
    parentNode.projector.countPendingWakeups(
      childNode.nativeId,
      parentMinStepIndex,
    )
  const { messages, tagged, untaggedCount } = selectParentWakeups(
    childInbox,
    parentNode.nativeId,
  )
  const taggedPriorCount = tagged.filter(
    (m) => m.tool.stepIndex <= parentMinStepIndex,
  ).length
  const taggedTurnCount =
    untaggedCount === 0 ? tagged.length - taggedPriorCount : 0
  const requiredTurnWakeups = Math.max(taggedTurnCount, expectedTurnWakeups)
  if (requiredTurnWakeups === 0) return { status: 'pending' }
  const untaggedPriorCount =
    untaggedCount > 0
      ? Math.max(0, messages.length - expectedTurnWakeups - taggedPriorCount)
      : 0
  const priorWakeups = Math.max(
    parentNode.countPriorWakeups(childNode.nativeId, parentMinStepIndex),
    taggedPriorCount + untaggedPriorCount,
  )
  const wakeupSteps = childNode.wakeupStepsFrom(parentNode.nativeId)
  const startStep = wakeupSteps[priorWakeups]
  if (startStep === undefined) return { status: 'pending' }
  return {
    status: 'active',
    minStepIndex: Math.max(childNode.turnStartStep, startStep - 1),
    awaitingTurnWakeup: wakeupSteps.length < priorWakeups + requiredTurnWakeups,
  }
}

/**
 * Resolve the last turn boundary step (`< beforeStep`) for `nodeId`, recursing up ancestor
 * subagent wakeup boundaries when `nodeId` is itself a re-woken child.
 *
 * Uses `TurnBoundaryCache` keyed by `transcript_full.jsonl` `(mtimeMs, size, beforeStep)` so missing,
 * empty, or unchanged ancestor transcripts do not re-read inbox/subagents/transcripts on every poll tick.
 */
export async function readTurnBoundaryBefore(
  root: string,
  nodeId: string,
  beforeStep: number,
  cache?: TurnBoundaryCache,
  visiting = new Set<string>(),
): Promise<{ boundary: number; ready: boolean }> {
  if (!sessionIdPattern.test(nodeId) || visiting.has(nodeId))
    return { boundary: -1, ready: true }
  visiting.add(nodeId)
  try {
    const transcriptPath = await resolveBrainPath(
      root,
      nodeId,
      'logs/transcript_full.jsonl',
    )
    let fileStat: { mtimeMs: number; size: number }
    try {
      const s = await stat(transcriptPath)
      fileStat = { mtimeMs: s.mtimeMs, size: s.size }
    } catch (error) {
      if (isMissing(error)) return { boundary: -1, ready: false }
      throw error
    }
    if (fileStat.size === 0) return { boundary: -1, ready: false }
    let entry = cache?.get(nodeId)
    if (
      !entry ||
      entry.mtimeMs !== fileStat.mtimeMs ||
      entry.size !== fileStat.size ||
      entry.beforeStep !== beforeStep
    ) {
      const [inbox, subagents, rows] = await Promise.all([
        readInboxState(root, nodeId),
        readSubagents(root, nodeId),
        readTranscriptRows(root, nodeId).catch(() => []),
      ])
      if (rows.length === 0) {
        cache?.set(nodeId, {
          mtimeMs: fileStat.mtimeMs,
          size: fileStat.size,
          beforeStep,
          emptyRows: true,
          localBoundary: -1,
          tagged: [],
          wakeupSteps: [],
        })
        return { boundary: -1, ready: false }
      }
      const init = inbox.messages.find((m) =>
        isNativeToolName(m.tool?.name ?? '', 'invoke_subagent'),
      )
      const node = new BrainNode(nodeId, nodeId, {
        allowInitialSpawnPrompt: Boolean(init),
      })
      node.projector.syncMetadata(inbox, subagents)
      node.projector.observeRows(rows)
      for (const row of rows) {
        const stepIndex = Number(row.step_index)
        if (!Number.isInteger(stepIndex) || stepIndex >= beforeStep) continue
        node.ingest(row)
      }
      const grandParentId = init?.tool?.conversationId
      const { tagged, allTagged } = selectParentWakeups(inbox, grandParentId)
      const wakeupSteps = grandParentId
        ? node.wakeupStepsFrom(grandParentId).filter((s) => s < beforeStep)
        : []
      entry = {
        mtimeMs: fileStat.mtimeMs,
        size: fileStat.size,
        beforeStep,
        localBoundary: node.turnStartStep,
        grandParentId: allTagged ? grandParentId : undefined,
        initStepIndex: init?.tool?.stepIndex,
        tagged: allTagged ? tagged : [],
        wakeupSteps,
      }
      if (!grandParentId || allTagged) {
        cache?.set(nodeId, entry)
      }
    } else if (entry.emptyRows) {
      return { boundary: -1, ready: false }
    }
    let boundary = entry.localBoundary
    let ready = true
    if (
      entry.grandParentId &&
      entry.initStepIndex !== undefined &&
      entry.tagged.length > 0 &&
      entry.wakeupSteps.length > 0
    ) {
      const ingestedCount = Math.min(
        entry.tagged.length,
        entry.wakeupSteps.length,
      )
      const lastWakeParentStep = entry.tagged[ingestedCount - 1].tool.stepIndex
      const grand = await readTurnBoundaryBefore(
        root,
        entry.grandParentId,
        lastWakeParentStep,
        cache,
        visiting,
      )
      ready = grand.ready
      if (grand.boundary >= 0 && entry.initStepIndex <= grand.boundary) {
        const startStep = resolveTurnWakeupStartStep(
          entry.tagged,
          entry.wakeupSteps,
          grand.boundary,
          ingestedCount,
        )
        if (startStep !== undefined) boundary = Math.max(boundary, startStep)
      }
    }
    return { boundary, ready }
  } finally {
    visiting.delete(nodeId)
  }
}
