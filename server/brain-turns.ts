import { readdir, stat } from 'node:fs/promises'
import { join } from 'node:path'
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
  metaSignature: string
  beforeStep: number
  emptyRows?: boolean
  localBoundary: number
  grandParentId?: string
  initStepIndex?: number
  tagged: readonly TaggedWakeupMessage[]
  untaggedCount: number
  wakeupSteps: readonly number[]
  allWakeupCount: number
  node?: BrainNode
}

async function readDirSignature(
  root: string,
  nodeId: string,
  subdir: string,
): Promise<string> {
  try {
    const dir = await resolveBrainPath(root, nodeId, subdir)
    const names = (await readdir(dir)).sort()
    const entries = await Promise.all(
      names.map(async (name) => {
        const s = await stat(join(dir, name)).catch(() => undefined)
        return s ? `${name}:${s.size}:${s.mtimeMs}` : `${name}:missing`
      }),
    )
    return entries.join('|')
  } catch {
    return ''
  }
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

function countPriorWakeups(
  tagged: readonly TaggedWakeupMessage[],
  untaggedCount: number,
  parentBoundary: number,
  expectedTurnWakeups: number,
  recordedPriorWakeups: number,
): number {
  const taggedPriorCount = tagged.filter(
    (m) => m.tool.stepIndex <= parentBoundary,
  ).length
  const taggedTurnCount = tagged.length - taggedPriorCount
  const untaggedPriorCount = Math.max(
    0,
    untaggedCount - Math.max(0, expectedTurnWakeups - taggedTurnCount),
  )
  return Math.max(recordedPriorWakeups, taggedPriorCount + untaggedPriorCount)
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
  const { tagged, untaggedCount } = selectParentWakeups(
    childInbox,
    parentNode.nativeId,
  )
  const taggedPriorCount = tagged.filter(
    (m) => m.tool.stepIndex <= parentMinStepIndex,
  ).length
  const taggedTurnCount = tagged.length - taggedPriorCount
  const requiredTurnWakeups = Math.max(taggedTurnCount, expectedTurnWakeups)
  if (requiredTurnWakeups === 0) return { status: 'pending' }
  const priorWakeups = countPriorWakeups(
    tagged,
    untaggedCount,
    parentMinStepIndex,
    expectedTurnWakeups,
    parentNode.countPriorWakeups(childNode.nativeId, parentMinStepIndex),
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
 * Uses `TurnBoundaryCache` keyed by node/cutoff and validated by transcript `(mtimeMs, size)` and
 * inbox/subagent `metaSignature` so missing,
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
    let fileStat: { mtimeMs: number; size: number }
    try {
      const transcriptPath = await resolveBrainPath(
        root,
        nodeId,
        'logs/transcript_full.jsonl',
      )
      const s = await stat(transcriptPath)
      fileStat = { mtimeMs: s.mtimeMs, size: s.size }
    } catch (error) {
      if (isMissing(error)) return { boundary: -1, ready: false }
      throw error
    }
    if (fileStat.size === 0) return { boundary: -1, ready: false }
    const metaSignature = `${await readDirSignature(root, nodeId, 'messages')}||${await readDirSignature(root, nodeId, 'subagents')}`
    const cacheKey = `${nodeId}:${beforeStep}`
    let entry = cache?.get(cacheKey)
    if (
      !entry ||
      entry.mtimeMs !== fileStat.mtimeMs ||
      entry.size !== fileStat.size ||
      entry.metaSignature !== metaSignature ||
      entry.beforeStep !== beforeStep
    ) {
      const [inbox, subagents, rows] = await Promise.all([
        readInboxState(root, nodeId),
        readSubagents(root, nodeId),
        readTranscriptRows(root, nodeId).catch(() => []),
      ])
      if (rows.length === 0) {
        cache?.set(cacheKey, {
          mtimeMs: fileStat.mtimeMs,
          size: fileStat.size,
          metaSignature,
          beforeStep,
          emptyRows: true,
          localBoundary: -1,
          tagged: [],
          untaggedCount: 0,
          wakeupSteps: [],
          allWakeupCount: 0,
        })
        return { boundary: -1, ready: false }
      }
      const init = inbox.messages.find((m) =>
        isNativeToolName(m.tool?.name ?? '', 'invoke_subagent'),
      )
      const grandParentId = init?.tool?.conversationId
      const node = new BrainNode(nodeId, nodeId, {
        allowInitialSpawnPrompt: Boolean(init),
      })
      node.projector.syncMetadata(inbox, subagents)
      node.projector.observeRows(rows)
      let localBoundary = -1
      let wakeupSteps: readonly number[] = []
      let capturedBefore = false
      for (const row of rows) {
        const stepIndex = Number(row.step_index)
        if (!Number.isInteger(stepIndex)) continue
        if (!capturedBefore && stepIndex >= beforeStep) {
          localBoundary = node.turnStartStep
          wakeupSteps = grandParentId
            ? [...node.wakeupStepsFrom(grandParentId)]
            : []
          capturedBefore = true
        }
        node.ingest(row)
      }
      if (!capturedBefore) {
        localBoundary = node.turnStartStep
        wakeupSteps = grandParentId
          ? [...node.wakeupStepsFrom(grandParentId)]
          : []
      }
      const { tagged, untaggedCount } = selectParentWakeups(
        inbox,
        grandParentId,
      )
      const allWakeupCount = grandParentId
        ? node.wakeupStepsFrom(grandParentId).length
        : 0
      entry = {
        mtimeMs: fileStat.mtimeMs,
        size: fileStat.size,
        metaSignature,
        beforeStep,
        localBoundary,
        grandParentId,
        initStepIndex: init?.tool?.stepIndex,
        tagged,
        untaggedCount,
        wakeupSteps,
        allWakeupCount,
        node,
      }
      cache?.set(cacheKey, entry)
    } else if (entry.emptyRows) {
      return { boundary: -1, ready: false }
    }
    let boundary = entry.localBoundary
    let ready = true
    if (entry.grandParentId && entry.initStepIndex !== undefined) {
      if (entry.untaggedCount === 0) {
        if (entry.tagged.length > 0 && entry.wakeupSteps.length > 0) {
          const ingestedCount = Math.min(
            entry.tagged.length,
            entry.wakeupSteps.length,
          )
          const lastWakeParentStep =
            entry.tagged[ingestedCount - 1].tool.stepIndex
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
            if (startStep !== undefined)
              boundary = Math.max(boundary, startStep)
          }
        }
      } else {
        const beforeCount = entry.wakeupSteps.length
        if (
          entry.allWakeupCount === beforeCount &&
          entry.allWakeupCount < entry.tagged.length + entry.untaggedCount
        )
          return { boundary, ready: false }
        let grandBeforeStep = Number.MAX_SAFE_INTEGER
        const grandCache = cache ?? new Map<string, CachedNodeBoundary>()
        while (true) {
          const grand = await readTurnBoundaryBefore(
            root,
            entry.grandParentId,
            grandBeforeStep,
            grandCache,
            visiting,
          )
          if (!grand.ready) return { boundary, ready: false }
          ready = grand.ready
          if (grand.boundary < 0 || entry.initStepIndex > grand.boundary) break
          const grandNode = grandCache.get(
            `${entry.grandParentId}:${grandBeforeStep}`,
          )?.node
          if (!grandNode) return { boundary, ready: false }
          const priorInGrand = grandNode.countPriorWakeups(
            nodeId,
            grand.boundary,
          )
          if (priorInGrand >= beforeCount) {
            grandBeforeStep = grand.boundary
            continue
          }
          const expectedTurnWakeups =
            grandNode.projector.countCompletedWakeups(nodeId, grand.boundary) +
            grandNode.projector.countPendingWakeups(nodeId, grand.boundary)
          if (expectedTurnWakeups > 0) {
            const beforeTagged = entry.tagged.filter(
              (m) => m.tool.stepIndex < grandBeforeStep,
            )
            const untaggedBeforeCount = Math.max(
              0,
              Math.min(entry.untaggedCount, beforeCount - beforeTagged.length),
            )
            const priorWakeups = countPriorWakeups(
              beforeTagged,
              untaggedBeforeCount,
              grand.boundary,
              expectedTurnWakeups,
              priorInGrand,
            )
            const startStep = entry.wakeupSteps[priorWakeups]
            if (startStep !== undefined)
              boundary = Math.max(boundary, startStep)
            break
          }
          // Step back past an unrelated later ancestor turn that did not wake nodeId.
          if (priorInGrand === 0) {
            if (entry.tagged.length > 0 || entry.untaggedCount > 0)
              return { boundary, ready: false }
            break
          }
          grandBeforeStep = grand.boundary
        }
      }
    }
    return { boundary, ready }
  } finally {
    visiting.delete(nodeId)
  }
}
