import { lstat, readdir, stat } from 'node:fs/promises'
import { join } from 'node:path'
import {
  isMissing,
  isNativeToolName,
  sessionIdPattern,
  setBounded,
} from './common.ts'
import {
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

type TaggedWakeupMessage = InboxMessage & {
  tool: { conversationId: string; stepIndex: number; name?: string }
}

const BOUNDARY_CACHE_LIMIT = 256

type CachedNodeBoundary = {
  mtimeMs: number
  size: number
  metaSignature: string
  emptyRows?: boolean
  localBoundary: number
  grandParentId?: string
  initStepIndex?: number
  tagged: readonly TaggedWakeupMessage[]
  wakeupSteps: readonly number[]
}

export type TurnBoundaryCache = Map<string, CachedNodeBoundary>

type ReWokenTurn =
  /** The child was spawned inside the active parent turn, so nothing earlier needs skipping. */
  | { status: 'spawned-in-turn' }
  /** The parent re-woke the child, but this turn's wakeup rows have not landed in the transcript yet. */
  | { status: 'awaiting-row' }
  /** The child was spawned in an earlier turn and re-woken into the active one. */
  | { status: 're-woken'; startStep: number }

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
        const s = await lstat(join(dir, name)).catch(() => undefined)
        return s ? `${name}:${s.size}:${s.mtimeMs}` : `${name}:missing`
      }),
    )
    return entries.join('|')
  } catch (error) {
    // A missing metadata dir means the session has no inbox/subagents yet; anything else is a real fault.
    if (isMissing(error)) return ''
    throw error
  }
}

/** Return sorted parent wakeup messages tagged with `tool.conversationId` and `tool.stepIndex`. */
export function selectParentWakeups(
  inbox: InboxState,
  parentId?: string,
): TaggedWakeupMessage[] {
  if (!parentId) return []
  return inbox.messages
    .filter(
      (m): m is TaggedWakeupMessage =>
        m.sender === parentId &&
        !isNativeToolName(m.tool?.name ?? '', 'invoke_subagent') &&
        !m.hideFromUser &&
        m.priority !== 'MESSAGE_PRIORITY_LOW' &&
        m.tool?.conversationId === parentId &&
        m.tool.stepIndex !== undefined,
    )
    .sort((a, b) => a.tool.stepIndex - b.tool.stepIndex)
}

/**
 * Classify a child wakeup against the parent turn boundary and resolve where the active turn starts.
 *
 * `parentBoundary` is the parent's last turn boundary step (`-1` when it has no earlier turn). A wakeup
 * tagged at or before it belongs to a turn the child already ran; the child must resume at the first
 * wakeup step tagged after it, or the earlier turn's failure would be re-attributed to this one.
 */
export function resolveReWokenTurn(
  tagged: readonly TaggedWakeupMessage[],
  wakeupSteps: readonly number[],
  parentBoundary: number,
  initStepIndex: number,
): ReWokenTurn {
  if (parentBoundary < 0 || initStepIndex > parentBoundary)
    return { status: 'spawned-in-turn' }
  const ingestedCount = Math.min(tagged.length, wakeupSteps.length)
  const turnTaggedCount = tagged
    .slice(0, ingestedCount)
    .filter((m) => m.tool.stepIndex > parentBoundary).length
  if (turnTaggedCount === 0) return { status: 'awaiting-row' }
  return {
    status: 're-woken',
    startStep: wakeupSteps[ingestedCount - turnTaggedCount],
  }
}

/**
 * Compute the child transcript step floor (`TurnFloor`) for the current parent turn, or `'pending'`
 * when the parent re-woke a child spawned in an earlier turn whose first wakeup step has not landed yet.
 */
export function resolveChildTurnState(
  parentNode: BrainNode,
  parentMinStepIndex: number,
  subRecord: SubagentRecord | undefined,
  childNode: BrainNode,
  childInbox: InboxState,
): ChildTurnState {
  if (
    subRecord === undefined ||
    subRecord.spawnStepIndex > parentMinStepIndex
  ) {
    return {
      status: 'active',
      minStepIndex: childNode.currentTurnFloor(),
      awaitingTurnWakeup: false,
    }
  }
  const tagged = selectParentWakeups(childInbox, parentNode.nativeId)
  if (!tagged.some((m) => m.tool.stepIndex > parentMinStepIndex)) {
    return { status: 'pending' }
  }
  const wakeupSteps = childNode.wakeupStepsFrom(parentNode.nativeId)
  const turn = resolveReWokenTurn(
    tagged,
    wakeupSteps,
    parentMinStepIndex,
    subRecord.spawnStepIndex,
  )
  if (turn.status !== 're-woken') return { status: 'pending' }
  return {
    status: 'active',
    minStepIndex: Math.max(childNode.currentTurnFloor(), turn.startStep - 1),
    awaitingTurnWakeup: wakeupSteps.length < tagged.length,
  }
}

async function statTranscript(
  root: string,
  nodeId: string,
): Promise<{ mtimeMs: number; size: number } | undefined> {
  try {
    const transcriptPath = await resolveBrainPath(
      root,
      nodeId,
      'logs/transcript_full.jsonl',
    )
    const s = await stat(transcriptPath)
    return s.size > 0 ? { mtimeMs: s.mtimeMs, size: s.size } : undefined
  } catch (error) {
    if (isMissing(error)) return undefined
    throw error
  }
}

async function loadNodeBoundary(
  root: string,
  nodeId: string,
  beforeStep: number,
  fileStat: { mtimeMs: number; size: number },
  metaSignature: string,
  cache?: TurnBoundaryCache,
): Promise<CachedNodeBoundary> {
  const [inbox, subagents, rows] = await Promise.all([
    readInboxState(root, nodeId),
    readSubagents(root, nodeId),
    readTranscriptRows(root, nodeId).catch((error) => {
      if (isMissing(error)) return []
      throw error
    }),
  ])
  const cacheKey = `${nodeId}:${beforeStep}`
  if (rows.length === 0) {
    const empty: CachedNodeBoundary = {
      mtimeMs: fileStat.mtimeMs,
      size: fileStat.size,
      metaSignature,
      emptyRows: true,
      localBoundary: -1,
      tagged: [],
      wakeupSteps: [],
    }
    if (cache) setBounded(cache, cacheKey, empty, BOUNDARY_CACHE_LIMIT)
    return empty
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
    wakeupSteps = grandParentId ? [...node.wakeupStepsFrom(grandParentId)] : []
  }
  const entry: CachedNodeBoundary = {
    mtimeMs: fileStat.mtimeMs,
    size: fileStat.size,
    metaSignature,
    localBoundary,
    grandParentId,
    initStepIndex: init?.tool?.stepIndex,
    tagged: selectParentWakeups(inbox, grandParentId),
    wakeupSteps,
  }
  if (cache) setBounded(cache, cacheKey, entry, BOUNDARY_CACHE_LIMIT)
  return entry
}

/**
 * Resolve the last turn boundary step (`< beforeStep`) for `nodeId`, recursing up ancestor
 * subagent wakeup boundaries when `nodeId` is itself a re-woken child.
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
    const fileStat = await statTranscript(root, nodeId)
    if (!fileStat) return { boundary: -1, ready: false }
    const metaSignature = `${await readDirSignature(root, nodeId, 'messages')}||${await readDirSignature(root, nodeId, 'subagents')}`
    const cached = cache?.get(`${nodeId}:${beforeStep}`)
    const fresh =
      cached &&
      cached.mtimeMs === fileStat.mtimeMs &&
      cached.size === fileStat.size &&
      cached.metaSignature === metaSignature
        ? cached
        : await loadNodeBoundary(
            root,
            nodeId,
            beforeStep,
            fileStat,
            metaSignature,
            cache,
          )
    if (fresh.emptyRows) return { boundary: -1, ready: false }
    let boundary = fresh.localBoundary
    if (
      !fresh.grandParentId ||
      fresh.initStepIndex === undefined ||
      fresh.tagged.length === 0 ||
      fresh.wakeupSteps.length === 0
    ) {
      return { boundary, ready: true }
    }
    const ingestedCount = Math.min(
      fresh.tagged.length,
      fresh.wakeupSteps.length,
    )
    const lastWakeParentStep = fresh.tagged[ingestedCount - 1].tool.stepIndex
    const grand = await readTurnBoundaryBefore(
      root,
      fresh.grandParentId,
      lastWakeParentStep,
      cache,
      visiting,
    )
    const turn = resolveReWokenTurn(
      fresh.tagged,
      fresh.wakeupSteps,
      grand.boundary,
      fresh.initStepIndex,
    )
    if (turn.status === 're-woken')
      boundary = Math.max(boundary, turn.startStep)
    return { boundary, ready: grand.ready }
  } finally {
    visiting.delete(nodeId)
  }
}
