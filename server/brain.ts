import {
  BrainTurnObservation,
  ModelUsageObserver,
  type BrainRead,
} from './brain-observer.ts'
import { stat } from 'node:fs/promises'
import { join } from 'node:path'
import {
  formatErrorMessage,
  isNativeToolName,
  sessionIdPattern,
} from './common.ts'
import {
  Timeline,
  addStepUsage,
  promptUsage,
  type StepUsage,
} from './mapping.ts'
import type {
  ProviderEvent,
  ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'
import {
  sameNormalizedQuestions,
  type NormalizedQuestion,
} from './questions.ts'
import type { ToolCall } from '@agentclientprotocol/sdk/unstable'
import { alignReplayTools } from './history.ts'
import {
  StepDedupe,
  countRowMessages,
  extractRowMessageSignatures,
  extractStepUsage,
  isRestartNotice,
  isRetryWarning,
  isWakeupMessage,
  readInboxState,
  readLastStepIndex,
  readSubagents,
  readTranscriptRows,
  RETRY_IDLE_TIMEOUT_MS,
  TranscriptReader,
  type InboxState,
  type SubagentRecord,
} from './brain-io.ts'
import {
  BrainNode,
  activeChildIdsForTurn,
  type PendingTool,
} from './brain-projector.ts'
import {
  readTurnBoundaryBefore,
  resolveChildTurnState,
  resolveReWokenTurn,
  selectParentWakeups,
  type ChildTurnState,
  type TurnBoundaryCache,
} from './brain-turns.ts'

type BackgroundWalk = {
  /** Standing IsDaemon schedules are excluded unless the caller asks whether an idle runtime can stop. */
  includeDaemons?: boolean
  retryTimeoutMs?: number
  runtimeEpoch?: RuntimeEpoch
  /** Sessions already on the walk, so a child cycle cannot recurse forever. */
  visiting?: Set<string>
  /** A subagent's first read decides its own turn phase instead of the parent's. */
  child?: boolean
  /** The parent turn that spawned or woke this child. */
  parent?: {
    node: BrainNode
    minStepIndex: number
    subRecord?: SubagentRecord
  }
}

export type RuntimeEpoch = {
  afterStepIndex: number
  preEpochReadIds: ReadonlySet<string>
  ignoredMessages: ReadonlyMap<string, string>
  ignoredSubagentIds: ReadonlySet<string>
}

function adjustInboxForEpoch(
  inbox: InboxState,
  epoch?: RuntimeEpoch,
  postEpochSignatures?: ReadonlyMap<string, number>,
): InboxState {
  if (!epoch) return inbox
  const unconsumedHighPriority =
    epoch.ignoredMessages.size > 0 || epoch.preEpochReadIds.size > 0
      ? inbox.unconsumedHighPriority.filter(
          (m) =>
            !epoch.ignoredMessages.has(m.id) &&
            !epoch.preEpochReadIds.has(m.id),
        )
      : inbox.unconsumedHighPriority
  let freshCount = 0
  const freshBySig = new Map<string, number>()
  const ignoredBySig = new Map<string, number>()
  for (const m of inbox.messages) {
    if (!inbox.readIds.has(m.id) || !isWakeupMessage(m)) continue
    if (epoch.preEpochReadIds.has(m.id)) continue
    const ignoredSig = epoch.ignoredMessages.get(m.id)
    if (ignoredSig !== undefined) {
      ignoredBySig.set(ignoredSig, (ignoredBySig.get(ignoredSig) ?? 0) + 1)
    } else {
      freshCount++
      freshBySig.set(m.signature, (freshBySig.get(m.signature) ?? 0) + 1)
    }
  }
  let consumedNonUserCount = freshCount
  for (const [sig, ignoredCount] of ignoredBySig) {
    const logged = postEpochSignatures?.get(sig) ?? 0
    const fresh = freshBySig.get(sig) ?? 0
    consumedNonUserCount += Math.min(ignoredCount, Math.max(0, logged - fresh))
  }
  if (
    unconsumedHighPriority.length === inbox.unconsumedHighPriority.length &&
    consumedNonUserCount === inbox.consumedNonUserCount
  )
    return inbox
  return {
    ...inbox,
    unconsumedHighPriority,
    consumedNonUserCount,
  }
}

/** Snapshot pre-restart Brain artifacts so a restarted/reopened runtime only tracks work created after this boundary. */
export function computeRuntimeEpoch(
  rows: readonly Record<string, unknown>[],
  inbox: InboxState,
  subagents: readonly SubagentRecord[],
  minAfterStep = -1,
  previous?: RuntimeEpoch,
): RuntimeEpoch | undefined {
  let afterStepIndex = Math.max(minAfterStep, previous?.afterStepIndex ?? -1)
  for (const row of rows) {
    const step = Number(row.step_index)
    if (Number.isInteger(step) && step > afterStepIndex) afterStepIndex = step
  }
  const preEpochReadIds = new Set(previous?.preEpochReadIds)
  for (const m of inbox.messages) {
    if (inbox.readIds.has(m.id) && isWakeupMessage(m)) preEpochReadIds.add(m.id)
  }
  const ignoredMessages = new Map<string, string>()
  if (previous) {
    for (const [id, sig] of previous.ignoredMessages) {
      if (!preEpochReadIds.has(id)) ignoredMessages.set(id, sig)
    }
  }
  for (const m of inbox.unconsumedHighPriority) {
    if (!preEpochReadIds.has(m.id)) ignoredMessages.set(m.id, m.signature)
  }
  const ignoredSubagentIds = new Set(previous?.ignoredSubagentIds)
  for (const s of subagents) {
    if (s.state === 'SUBAGENT_STATE_ALIVE')
      ignoredSubagentIds.add(s.conversationId)
  }
  if (
    afterStepIndex < 0 &&
    preEpochReadIds.size === 0 &&
    ignoredMessages.size === 0 &&
    ignoredSubagentIds.size === 0
  )
    return undefined
  return {
    afterStepIndex,
    preEpochReadIds,
    ignoredMessages,
    ignoredSubagentIds,
  }
}

/** Read and snapshot pre-restart Brain artifacts for a restarted or reopened runtime. */
export async function captureRuntimeEpoch(
  root: string,
  nativeId: string,
  options?: {
    minAfterStep?: number
    previous?: RuntimeEpoch
    readSubagents?: typeof readSubagents
  },
): Promise<RuntimeEpoch | undefined> {
  if (!sessionIdPattern.test(nativeId)) return undefined
  const readSubs = options?.readSubagents ?? readSubagents
  const [rows, inbox, subagents, tailStep] = await Promise.all([
    readTranscriptRows(root, nativeId),
    readInboxState(root, nativeId),
    readSubs(root, nativeId),
    readLastStepIndex(root, nativeId).catch(() => -1),
  ])
  return computeRuntimeEpoch(
    rows,
    inbox,
    subagents,
    Math.max(options?.minAfterStep ?? -1, tailStep),
    options?.previous,
  )
}

function applyEpochFloor(node: BrainNode, restartAfterStep?: number) {
  if (restartAfterStep !== undefined && restartAfterStep >= 0) {
    node.turnStartStep = restartAfterStep
    node.promptPhase = 'foreground_idle'
  }
}

async function pollAndIngestProbeNode(
  root: string,
  node: BrainNode,
  runtimeEpoch?: RuntimeEpoch,
  postEpochSignatures?: Map<string, number>,
): Promise<{
  rawInbox: InboxState
  subagents: SubagentRecord[]
  rowCount: number
}> {
  const restartAfterStep = runtimeEpoch?.afterStepIndex
  const {
    inbox: rawInbox,
    subagents,
    rows,
    observedAt,
  } = await node.pollTranscript(root, {
    retryEmptyReset: true,
    resetInitialReadOnReset: true,
    onReset: () => {
      applyEpochFloor(node, restartAfterStep)
      postEpochSignatures?.clear()
    },
  })
  const eligibleRows =
    restartAfterStep !== undefined && restartAfterStep >= 0
      ? rows.filter((row) => Number(row.step_index) > restartAfterStep)
      : rows
  if (postEpochSignatures) {
    for (const row of eligibleRows) {
      for (const sig of extractRowMessageSignatures(row)) {
        postEpochSignatures.set(sig, (postEpochSignatures.get(sig) ?? 0) + 1)
      }
    }
  }
  node.projector.observeRows(eligibleRows)
  for (const row of eligibleRows) {
    // The restarted runtime left no woken turn running.
    if (isRestartNotice(row)) node.resetPhase()
    node.ingest(row, observedAt)
  }
  return { rawInbox, subagents, rowCount: rows.length }
}

async function evaluateActiveBackgroundWork(
  root: string,
  node: BrainNode,
  rawInbox: InboxState,
  subagents: readonly SubagentRecord[],
  options: BackgroundWalk,
  visiting: Set<string>,
  postEpochSignatures?: ReadonlyMap<string, number>,
): Promise<boolean> {
  const isChild = options.child === true
  const parentContext = options.parent
  const inbox = isChild
    ? rawInbox
    : adjustInboxForEpoch(rawInbox, options.runtimeEpoch, postEpochSignatures)
  if (inbox.unconsumedHighPriority.length > 0) return true
  if (
    node.projector.hasBackground() ||
    (options.includeDaemons && node.projector.hasDaemons())
  )
    return true
  const childTurn: ChildTurnState = parentContext
    ? resolveChildTurnState(
        parentContext.node,
        parentContext.minStepIndex,
        parentContext.subRecord,
        node,
        inbox,
      )
    : {
        status: 'active',
        minStepIndex: node.currentTurnFloor(),
        awaitingTurnWakeup: false,
      }
  if (childTurn.status === 'pending') return true
  const nodeMinStep = childTurn.minStepIndex
  // A timed-out model retry does not stop tracked processes or descendants.
  if (!node.failure(options.retryTimeoutMs, nodeMinStep)) {
    if (node.retryPending(nodeMinStep)) return true
    if (
      isChild
        ? childTurn.awaitingTurnWakeup ||
          node.hasUnprocessedMessages(inbox) ||
          !node.isSettled(inbox, nodeMinStep)
        : (node.foregroundIdleReached && node.hasUnprocessedMessages(inbox)) ||
          node.isWokenActive()
    )
      return true
  }
  const activeChildIds = activeChildIdsForTurn(
    subagents,
    node.projector,
    nodeMinStep,
    isChild ? undefined : options.runtimeEpoch?.ignoredSubagentIds,
  )
  const childOptions = options.runtimeEpoch
    ? { ...options, runtimeEpoch: undefined }
    : options
  for (const childId of activeChildIds) {
    const subRecord = subagents.find((s) => s.conversationId === childId)
    if (
      await hasActiveBackgroundWork(root, childId, {
        ...childOptions,
        visiting,
        child: true,
        parent: { node, minStepIndex: nodeMinStep, subRecord },
      })
    )
      return true
  }
  return false
}

type BackgroundWorkProbeOptions = {
  includeDaemons?: boolean
  retryTimeoutMs?: number
  runtimeEpoch?: RuntimeEpoch
}

/**
 * Stateful incremental background-work probe for one root session.
 * Retains a warm TranscriptReader cursor and BrainNode across polls so long transcripts
 * are not re-read from byte 0 on every interrupt or idle-stop check.
 */
export class BackgroundWorkProbe {
  private readonly createReader: () => TranscriptReader
  private node?: BrainNode
  private epoch?: RuntimeEpoch
  private postEpochSignatures?: Map<string, number>
  private lane: Promise<void> = Promise.resolve()

  constructor(
    createReader: () => TranscriptReader = () => new TranscriptReader(),
  ) {
    this.createReader = createReader
  }

  private ensureNode(nativeId: string, runtimeEpoch?: RuntimeEpoch): BrainNode {
    if (
      !this.node ||
      this.node.nativeId !== nativeId ||
      this.epoch !== runtimeEpoch
    ) {
      const node = new BrainNode(nativeId, nativeId, {
        allowInitialSpawnPrompt: false,
        reader: this.createReader(),
      })
      this.node = node
      this.epoch = runtimeEpoch
      this.postEpochSignatures = runtimeEpoch ? new Map() : undefined
      applyEpochFloor(node, runtimeEpoch?.afterStepIndex)
    }
    return this.node
  }

  private serialize<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.lane.then(fn, fn)
    this.lane = next.then(
      () => undefined,
      () => undefined,
    )
    return next
  }

  async hasActiveWork(
    root: string,
    nativeId: string,
    options: BackgroundWorkProbeOptions = {},
  ): Promise<boolean> {
    if (!sessionIdPattern.test(nativeId)) return false
    return this.serialize(async () => {
      const node = this.ensureNode(nativeId, options.runtimeEpoch)
      const { rawInbox, subagents } = await pollAndIngestProbeNode(
        root,
        node,
        this.epoch,
        this.postEpochSignatures,
      )
      return evaluateActiveBackgroundWork(
        root,
        node,
        rawInbox,
        subagents,
        options,
        new Set([nativeId]),
        this.postEpochSignatures,
      )
    })
  }

  async sync(
    root: string,
    nativeId: string,
    runtimeEpoch?: RuntimeEpoch,
  ): Promise<void> {
    if (!sessionIdPattern.test(nativeId)) return
    return this.serialize(async () => {
      const node = this.ensureNode(nativeId, runtimeEpoch)
      await pollAndIngestProbeNode(
        root,
        node,
        this.epoch,
        this.postEpochSignatures,
      )
    })
  }
}

/** Determine whether the session has running background tasks or active subagents. Standing IsDaemon schedules are excluded by default so foreground interrupts do not kill them, and included when checking whether an idle runtime can stop. */
export async function hasActiveBackgroundWork(
  root: string,
  nativeId: string,
  options: BackgroundWalk = {},
): Promise<boolean> {
  const visiting = options.visiting ?? new Set<string>()
  const isChild = options.child === true
  if (!sessionIdPattern.test(nativeId) || visiting.has(nativeId)) return false
  visiting.add(nativeId)
  try {
    const node = new BrainNode(nativeId, nativeId, {
      allowInitialSpawnPrompt: isChild,
    })
    const runtimeEpoch = isChild ? undefined : options.runtimeEpoch
    const postEpochSignatures = runtimeEpoch
      ? new Map<string, number>()
      : undefined
    applyEpochFloor(node, runtimeEpoch?.afterStepIndex)
    const { rawInbox, subagents, rowCount } = await pollAndIngestProbeNode(
      root,
      node,
      runtimeEpoch,
      postEpochSignatures,
    )
    if (isChild && rowCount === 0) return true
    return await evaluateActiveBackgroundWork(
      root,
      node,
      rawInbox,
      subagents,
      options,
      visiting,
      postEpochSignatures,
    )
  } finally {
    visiting.delete(nativeId)
  }
}

/** Resolve a child's turn phase against the parent turn that spawned or woke it. */
function childTurnOf(
  parentNode: BrainNode,
  parentMinStepIndex: number,
  parentSubagents: readonly SubagentRecord[],
  childId: string,
  child: BrainNode,
  childInbox: InboxState,
): ChildTurnState {
  return resolveChildTurnState(
    parentNode,
    parentMinStepIndex,
    parentSubagents.find((s) => s.conversationId === childId),
    child,
    childInbox,
  )
}

type ChildSettlement = { settled: boolean; error?: string; stalled?: boolean }

async function settleChildren(options: {
  root: string
  parentNode: BrainNode
  parentInbox: InboxState
  activeChildIds: readonly string[]
  parentSubagents: readonly SubagentRecord[]
  parentMinStepIndex: number
  visiting?: Set<string>
  retryTimeoutMs?: number
}): Promise<ChildSettlement> {
  const { root, parentNode, parentInbox, parentSubagents, parentMinStepIndex } =
    options
  const visiting = options.visiting ?? new Set<string>()
  const retryTimeoutMs = options.retryTimeoutMs ?? RETRY_IDLE_TIMEOUT_MS
  if (visiting.has(parentNode.nativeId)) return { settled: true }
  visiting.add(parentNode.nativeId)
  try {
    let settled = true
    for (const childId of options.activeChildIds) {
      if (!sessionIdPattern.test(childId) || visiting.has(childId)) continue
      let child = parentNode.children.get(childId)
      if (!child) {
        child = new BrainNode(childId)
        parentNode.children.set(childId, child)
      }
      const {
        inbox: childInbox,
        subagents: childSubagents,
        rows,
        missing,
        observedAt,
      } = await child.pollTranscript(root)
      if (missing) {
        const childTurn = childTurnOf(
          parentNode,
          parentMinStepIndex,
          parentSubagents,
          childId,
          child,
          childInbox,
        )
        if (childTurn.status === 'active') {
          const failure = child.failure(retryTimeoutMs, childTurn.minStepIndex)
          if (failure)
            return {
              settled: false,
              error: `Subagent ${childId} failed: ${failure}`,
              stalled: child.retryPending(childTurn.minStepIndex),
            }
        }
        settled = false
        continue
      }
      child.projector.observeRows(rows)
      for (const row of rows) child.ingest(row, observedAt)
      const childTurn = childTurnOf(
        parentNode,
        parentMinStepIndex,
        parentSubagents,
        childId,
        child,
        childInbox,
      )
      if (childTurn.status === 'pending') {
        settled = false
        continue
      }
      const childMinStep = childTurn.minStepIndex
      const waitingForWakeup =
        !child.retryPending(childMinStep) &&
        (childTurn.awaitingTurnWakeup ||
          child.hasUnprocessedMessages(childInbox))
      if (!waitingForWakeup) {
        const failure = child.failure(retryTimeoutMs, childMinStep)
        if (failure)
          return {
            settled: false,
            error: `Subagent ${childId} failed: ${failure}`,
            stalled: child.retryPending(childMinStep),
          }
      }
      if (
        waitingForWakeup ||
        childTurn.awaitingTurnWakeup ||
        !child.isSettled(childInbox, childMinStep)
      )
        settled = false
      const grandchildIds = activeChildIdsForTurn(
        childSubagents,
        child.projector,
        childMinStep,
      )
      const nested = await settleChildren({
        ...options,
        visiting,
        parentNode: child,
        parentInbox: childInbox,
        activeChildIds: grandchildIds,
        parentSubagents: childSubagents,
        parentMinStepIndex: childMinStep,
      })
      if (nested.error) return nested
      if (!nested.settled) settled = false
      // This child's report must reach the parent's transcript before the child counts as settled.
      if (
        parentInbox.unconsumedHighPriority.some((m) => m.sender === childId) ||
        parentNode.consumedMessages < parentInbox.consumedNonUserCount
      )
        settled = false
    }
    return { settled }
  } finally {
    visiting.delete(parentNode.nativeId)
  }
}

export type ForegroundAcpSegment =
  | { type: 'reasoning'; text: string }
  | { type: 'assistant_message'; text: string }
  | { type: 'tool_call'; callId: string }

type ForegroundCursor = { index: number; offset: number }

export function advanceForegroundCursor(
  segments: readonly ForegroundAcpSegment[],
  cursor: ForegroundCursor,
  item: ForegroundAcpSegment,
  allowSkip: boolean,
): ForegroundCursor | undefined {
  if (item.type === 'tool_call') {
    if (!allowSkip) {
      const seg = segments[cursor.index]
      return cursor.offset === 0 &&
        seg?.type === 'tool_call' &&
        seg.callId === item.callId
        ? { index: cursor.index + 1, offset: 0 }
        : undefined
    }
    for (let j = cursor.index; j < segments.length; j++) {
      const seg = segments[j]
      if (seg?.type === 'tool_call' && seg.callId === item.callId)
        return { index: j + 1, offset: 0 }
    }
    return undefined
  }
  const current = segments[cursor.index]
  if (current?.type === item.type) {
    // agy inserts a paragraph break before some assistant chunks.
    const offset = current.text.startsWith(item.text, cursor.offset)
      ? cursor.offset
      : item.type === 'assistant_message' &&
          current.text.startsWith('\n\n', cursor.offset) &&
          current.text.startsWith(item.text, cursor.offset + 2)
        ? cursor.offset + 2
        : undefined
    if (offset !== undefined) {
      const end = offset + item.text.length
      return end === current.text.length
        ? { index: cursor.index + 1, offset: 0 }
        : { index: cursor.index, offset: end }
    }
    if (allowSkip) {
      const pos = current.text.indexOf(item.text, cursor.offset)
      if (pos >= 0) {
        const end = pos + item.text.length
        return end === current.text.length
          ? { index: cursor.index + 1, offset: 0 }
          : { index: cursor.index, offset: end }
      }
    }
  }
  if (allowSkip) {
    for (let j = cursor.index + 1; j < segments.length; j++) {
      const seg = segments[j]
      if (seg?.type !== item.type) continue
      const pos = seg.text.indexOf(item.text, 0)
      if (pos >= 0) {
        const end = pos + item.text.length
        return end === seg.text.length
          ? { index: j + 1, offset: 0 }
          : { index: j, offset: end }
      }
    }
  }
  return undefined
}

type ForegroundReconcilePhase =
  'foreground' | 'reconciling_in_rpc' | 'post_rpc_wakeup'

/**
 * Reconciles Brain transcript steps against ACP output observed during the foreground session/prompt RPC.
 *
 * Provenance (`agy` 1.5.16+ `(*Server).promptTurn` / `outOfTurnCallbacks` / `sendStepUpdatesForGen`):
 * non-daemon background tasks, subagents, non-daemon crons, and high-priority inbox continuations stay
 * inside the open `session/prompt` RPC, stream via in-turn `sendStepUpdatesForGen` (`RecordStepUsage`),
 * and sum into `PromptResponse.usage` while marking steps consumed. Out-of-turn wakeups (`IsDaemon: true`
 * or external messages arriving after `session/prompt` returns) stream via `outOfTurnCallbacks`
 * (`MarkStepUsageConsumed`, never `RecordStepUsage`). Decoupling this state machine from `node.wokeUp`
 * preserves `node.isWokenWorking()` stall timeouts and unstreamed `SYSTEM_MESSAGE` background completions
 * without re-emitting or double-billing steps already covered by the foreground ACP stream.
 */
class ForegroundReconciler {
  private readonly foregroundAcp: readonly ForegroundAcpSegment[]
  private readonly hasPostRpcAcp?: () => boolean
  private phase: ForegroundReconcilePhase = 'foreground'
  private cursor: ForegroundCursor = { index: 0, offset: 0 }
  private sawRetrySinceMatch = false

  constructor(
    foregroundAcp: readonly ForegroundAcpSegment[],
    hasPostRpcAcp?: () => boolean,
  ) {
    this.foregroundAcp = foregroundAcp
    this.hasPostRpcAcp = hasPostRpcAcp
  }

  get postRpc(): boolean {
    return this.phase === 'post_rpc_wakeup'
  }

  reset(): void {
    this.phase = 'foreground'
    this.cursor = { index: 0, offset: 0 }
    this.sawRetrySinceMatch = false
  }

  observeRowMeta(row: Record<string, unknown>): void {
    if (isRetryWarning(row)) this.sawRetrySinceMatch = true
  }

  /** Advance reconciliation state for one ingested row and return whether it belongs to post-RPC wakeup. */
  reconcile(
    row: Record<string, unknown>,
    items: readonly ForegroundAcpSegment[],
    nodeWokeUp: boolean,
  ): boolean {
    if (this.phase === 'post_rpc_wakeup') return true
    if (!nodeWokeUp) {
      this.phase = 'foreground'
      this.advanceForegroundMatchables(items)
      return false
    }
    this.phase = 'reconciling_in_rpc'
    if (row.type !== 'PLANNER_RESPONSE') return false
    if (this.isPlannerRowCoveredByAcp(items)) return false
    this.phase = 'post_rpc_wakeup'
    return true
  }

  private advanceForegroundMatchables(
    items: readonly ForegroundAcpSegment[],
  ): void {
    for (const item of items) {
      const next = advanceForegroundCursor(
        this.foregroundAcp,
        this.cursor,
        item,
        true,
      )
      if (next) {
        this.cursor = next
        this.sawRetrySinceMatch = false
      }
    }
  }

  private isPlannerRowCoveredByAcp(
    items: readonly ForegroundAcpSegment[],
  ): boolean {
    if (items.length === 0) {
      return (
        this.cursor.index < this.foregroundAcp.length ||
        (this.foregroundAcp.length > 0 && this.hasPostRpcAcp?.() === false)
      )
    }
    let next: ForegroundCursor | undefined = this.cursor
    for (let i = 0; i < items.length && next; i++) {
      next = advanceForegroundCursor(
        this.foregroundAcp,
        next,
        items[i],
        i === 0 && this.sawRetrySinceMatch,
      )
    }
    if (!next) return false
    this.cursor = next
    this.sawRetrySinceMatch = false
    return true
  }
}

type QuestionAnswer = {
  answerText: string | null
  declined: boolean
  callId?: string
}

/** Follow background tasks and post-RPC Reactive Wakeup steps in transcript_full.jsonl until the turn settles. */
export async function settleBrainTurn(
  root: string,
  nativeId: string,
  sessionId: string,
  onItem: (
    item: ProviderTimelineItem,
    timestamp?: string,
    toolResult?: { rawOutput: unknown },
  ) => void,
  {
    signal,
    minStepIndex = -1,
    pollIntervalMs = 200,
    acpToolCallIds = [],
    foregroundAcp = [],
    hasPostRpcAcp,
    drainAcp,
    toolSnapshot,
    hasToolResult,
    deniedToolCallIds,
    shouldUpdateTool,
    onStep,
    transcriptGraceMs = 0,
    stallTimeoutMs = 120_000,
    wokenStallTimeoutMs = 15 * 60_000,
    retryTimeoutMs = RETRY_IDLE_TIMEOUT_MS,
    isWaitingOnUser,
    isWaitingOnQuestion,
    takeQuestionAnswer,
    isWakeupQuestionDeclined,
    onSubagents,
    observation,
    onWakeupState,
    mcpServerNames,
    runtimeEpoch,
  }: {
    mcpServerNames?: readonly string[]
    observation?: BrainTurnObservation
    onWakeupState?: (state: {
      wokeUp: boolean
      wakeupUsage?: StepUsage
    }) => void
    runtimeEpoch?: RuntimeEpoch
    onSubagents?: (subagents: readonly SubagentRecord[]) => void
    signal?: AbortSignal
    minStepIndex?: number
    pollIntervalMs?: number
    /** Live collection of ACP toolCallIds observed in this turn (mutated as post-RPC wakeup tool calls arrive). */
    acpToolCallIds?: Iterable<string>
    /** Ordered ACP output observed before session/prompt returned, used to detect in-RPC inbox continuations. */
    foregroundAcp?: readonly ForegroundAcpSegment[]
    /** Whether post-RPC out-of-turn ACP notifications arrived after session/prompt resolved. */
    hasPostRpcAcp?: () => boolean
    drainAcp?: () => Promise<void>
    toolSnapshot?: (id: string) => Readonly<ToolCall> | undefined
    hasToolResult?: (id: string) => boolean
    deniedToolCallIds?: ReadonlySet<string>
    shouldUpdateTool?: (
      item: Extract<ProviderTimelineItem, { type: 'tool_call' }>,
    ) => boolean
    /** Invoked with the highest step_index observed so far. */
    onStep?: (stepIndex: number) => void
    /** How long to wait for agy to flush this turn's USER_INPUT row before treating the turn as transcript-less. */
    transcriptGraceMs?: number
    /** Give up when nothing tracked is running and the transcript stays idle without a final response. */
    stallTimeoutMs?: number
    /** Upper bound on a single silent woken-turn step (tool execution or post-wakeup/post-tool LLM generation). */
    wokenStallTimeoutMs?: number
    /** Maximum silence after the latest distinct retry warning, independent of ordinary stalls. */
    retryTimeoutMs?: number
    /** Pending approvals legitimately block agy; they suspend the stall timeout. */
    isWaitingOnUser?: () => boolean
    isWaitingOnQuestion?: () => boolean
    /** Match an already resolved ACP elicitation to its Brain row, without asking again. */
    takeQuestionAnswer?: (
      questions: readonly NormalizedQuestion[],
      wokeUp: boolean,
      callId: string,
    ) => QuestionAnswer | undefined
    isWakeupQuestionDeclined?: () => boolean
  } = {},
): Promise<{
  status: 'completed' | 'canceled' | 'failed'
  stalled?: boolean
  error?: string
  lastStepUsage?: StepUsage
  wokeUp?: true
  wakeupUsage?: StepUsage
}> {
  if (!sessionIdPattern.test(nativeId)) return { status: 'completed' }
  const effectiveMinStepIndex = Math.max(
    minStepIndex,
    runtimeEpoch?.afterStepIndex ?? -1,
  )
  const fgReconciler = new ForegroundReconciler(foregroundAcp, hasPostRpcAcp)
  const foregroundToolIds = foregroundAcp.flatMap((seg) =>
    seg.type === 'tool_call' ? [seg.callId] : [],
  )
  const answersByTool = new WeakMap<PendingTool, QuestionAnswer>()
  const answersByCallId = new Map<
    string,
    {
      questions: readonly NormalizedQuestion[]
      answer: QuestionAnswer
    }
  >()
  const answerForQuestion = (
    questions: readonly NormalizedQuestion[],
    tool: PendingTool,
  ) => {
    const cached = answersByCallId.get(tool.callId)
    if (cached && sameNormalizedQuestions(cached.questions, questions))
      return cached.answer
    const local = answersByTool.get(tool)
    const answer =
      local && (!tool.callId || !local.callId || tool.callId === local.callId)
        ? local
        : takeQuestionAnswer?.(questions, fgReconciler.postRpc, tool.callId)
    if (answer) {
      if (tool.callId) answersByCallId.set(tool.callId, { questions, answer })
      else answersByTool.set(tool, answer)
    }
    return answer
  }
  const node = new BrainNode(nativeId, sessionId, {
    onQuestionSettled: answerForQuestion,
    reader: observation?.reader,
    allowInitialSpawnPrompt: false,
    mcpServerNames,
    turnFloor: minStepIndex,
  })
  const ownsObservation = !observation
  const stream =
    observation ??
    new BrainTurnObservation(
      root,
      nativeId,
      sessionId,
      effectiveMinStepIndex,
      {
        isCurrent: () => !signal?.aborted,
        onUsage: () => {},
        onItem,
      },
      pollIntervalMs,
      node.reader,
      runtimeEpoch?.afterStepIndex ?? -1,
    )
  stream.phase = 'settling'
  const startedAt = Date.now()
  let lastActivity = startedAt
  let lastTick = startedAt
  const expectedPollMs = Math.max(pollIntervalMs, stream.pollIntervalMs)
  let fingerprint = ''
  let foldVersion = 0
  let initial = true
  // Once anchored, a rewrite caught mid-O_TRUNC (empty or partial file) is not "no turn".
  let anchored = false
  let preamble: Record<string, unknown>[] = []
  let appliedHistoricalMessages = 0
  let lastStepIndex = -1
  const dedupe = new StepDedupe()
  const wakeupSteps = new Map<number, StepUsage>()
  const computeWakeupState = () => {
    let wakeupUsage: StepUsage | undefined
    for (const usage of wakeupSteps.values())
      wakeupUsage = addStepUsage(wakeupUsage, usage)
    return { wokeUp: fgReconciler.postRpc, wakeupUsage }
  }
  let inbox!: InboxState
  let subagents: SubagentRecord[] = []
  const finish = (
    status: 'completed' | 'canceled' | 'failed',
    error?: string,
  ) => {
    const { wokeUp, wakeupUsage } = computeWakeupState()
    return {
      status,
      ...(error !== undefined ? { error } : {}),
      lastStepUsage: stream.usage.latest,
      ...(wokeUp ? { wokeUp: true as const } : {}),
      ...(wakeupUsage ? { wakeupUsage } : {}),
    }
  }
  const withinGrace = () => Date.now() - startedAt < transcriptGraceMs
  const stall = (timeoutMs = stallTimeoutMs) => {
    const error = `Stopped waiting for agy: transcript_full.jsonl was idle for ${Math.round(timeoutMs / 1000)}s without a final response.`
    onItem({
      type: 'error',
      id: `${sessionId}:step:${lastStepIndex}:stalled`,
      message: error,
    })
    return { ...finish('failed', error), stalled: true as const }
  }
  // Without a Brain directory this session never persists a transcript; there is nothing to wait for.
  const sessionDirExists = () =>
    stat(join(root, nativeId)).then(
      (info) => info.isDirectory(),
      () => false,
    )
  const emitLiveTool = (
    item: ProviderTimelineItem,
    timestamp?: string,
    backgroundCompletion = false,
  ) => {
    if (item.type !== 'tool_call' || item.status === 'running' || !item.callId)
      return
    if (deniedToolCallIds?.has(item.callId)) {
      if (shouldUpdateTool?.(item))
        onItem({ ...item, status: 'canceled', error: null }, timestamp, {
          rawOutput: null,
        })
      return
    }
    const snapshot = toolSnapshot?.(item.callId)
    const acpSuppliedResult = hasToolResult
      ? hasToolResult(item.callId)
      : snapshot != null &&
        (snapshot.rawOutput != null ||
          (snapshot.content != null && snapshot.content.length > 0))
    // An unstreamed child card is only useful if it opens or resumes the child.
    if (
      backgroundCompletion ||
      shouldUpdateTool?.(item) ||
      (snapshot ? !acpSuppliedResult : item.detail.type !== 'sub_agent')
    )
      onItem(item, timestamp, { rawOutput: node.projector.toolRawOutput(item) })
  }
  let foregroundPrefixIds: readonly (string | undefined)[] = []
  const resolveLiveTools = (rows: readonly Record<string, unknown>[]) => {
    const slots = node.projector.observeRows(rows)
    const liveToolCallIds = [...acpToolCallIds]
    const snapshots = new Map<string, Readonly<ToolCall>>()
    for (const id of liveToolCallIds) {
      const snapshot = toolSnapshot?.(id)
      if (snapshot) snapshots.set(id, snapshot)
    }
    const align = (ids: readonly string[], targetSlots = slots) =>
      alignReplayTools(
        [],
        inbox.stepCallIds,
        subagents,
        ids,
        snapshots,
        targetSlots,
      )
    const turn = align(liveToolCallIds)
    node.projector.bindLiveToolCallIds(turn.alignedIds, turn.verifiedAcpToolIds)
    // ACP can send post-response calls while Brain still trails the full turn.
    // Verify the positional foreground prefix for billing only; it cannot bind tool identity.
    foregroundPrefixIds = []
    const validSlots = slots.filter((slot) => !slot.invalid)
    const prefixCount = Math.min(validSlots.length, foregroundToolIds.length)
    const prefix = foregroundToolIds.slice(0, prefixCount)
    if (
      validSlots.length < liveToolCallIds.length &&
      prefixCount > 0 &&
      prefix.every((id, i) => id === liveToolCallIds[i])
    ) {
      const prefixSlots = slots.slice(0, validSlots[prefixCount - 1].slot + 1)
      const foreground = align(prefix, prefixSlots)
      foregroundPrefixIds = foreground.alignedIds.map((id) =>
        id && foreground.verifiedAcpToolIds.has(id) ? id : undefined,
      )
    }
    const resolved = node.projector.resolveToolIdentities()
    if (resolved.length) foldVersion++
    for (const item of resolved) emitLiveTool(item)
  }
  const consume = async (read: BrainRead) => {
    if (!stream.acceptsRows() || signal?.aborted) return
    if (drainAcp) await drainAcp()
    if (!stream.acceptsRows() || signal?.aborted) return
    const metadata = await node.pollMetadata(root)
    if (!stream.acceptsRows() || signal?.aborted) return
    inbox = adjustInboxForEpoch(
      metadata.inbox,
      runtimeEpoch,
      stream.postEpochSignatures,
    )
    subagents = metadata.subagents
    onSubagents?.(subagents)
    const nextFingerprint = `${metadata.inbox.messages.length}:${metadata.inbox.readIds.size}:${subagents
      .map((s) => `${s.conversationId}=${s.state}`)
      .join(',')}`
    if (nextFingerprint !== fingerprint) {
      fingerprint = nextFingerprint
      foldVersion++
      lastActivity = Date.now()
    }
    const rows = read.rows
    const observedAt = node.initialReadDone ? undefined : read.mtimeMs
    if (read.snapshotStepIndices) {
      dedupe.prune(read.snapshotStepIndices)
      node.pruneRetrySteps(read.snapshotStepIndices)
      for (const step of wakeupSteps.keys())
        if (!read.snapshotStepIndices.has(step)) wakeupSteps.delete(step)
      onWakeupState?.(computeWakeupState())
    }
    if (read.reset) {
      initial = true
      preamble = []
      node.reset(inbox, subagents)
      fgReconciler.reset()
      appliedHistoricalMessages = 0
    }
    if (!initial) resolveLiveTools(rows)
    const historicalDelta =
      stream.historicalMessages - appliedHistoricalMessages
    node.consumedMessages += historicalDelta
    appliedHistoricalMessages = stream.historicalMessages
    if (historicalDelta) foldVersion++
    if (rows.length || read.reset) foldVersion++
    if (rows.length) lastActivity = Date.now()
    let slice = rows
    if (initial) {
      // Keep rows already consumed by the cursor until this turn's USER_INPUT is visible.
      preamble.push(...rows)
      let start = -1
      for (let i = 0; i < preamble.length; i++) {
        if (
          preamble[i].type === 'USER_INPUT' &&
          Number(preamble[i].step_index) > effectiveMinStepIndex
        ) {
          start = i
          break
        }
      }
      if (start < 0) return
      initial = false
      anchored = true
      const preambleSlice = preamble.slice(0, start)
      for (const row of preambleSlice)
        node.consumedMessages += countRowMessages(row, false)
      slice = preamble.slice(start)
      preamble = []
      resolveLiveTools(slice)
    }
    if (slice.length > 0) node.initialReadDone = true
    for (const row of slice) {
      const stepIndex = Number(row.step_index)
      if (row.type === 'USER_INPUT') fgReconciler.reset()
      fgReconciler.observeRowMeta(row)
      const items = node.ingest(
        row,
        read.observedAtByStep?.get(stepIndex) ?? observedAt,
      )
      const usage = extractStepUsage(row)
      const matchables: ForegroundAcpSegment[] = items.filter(
        (item) =>
          item.type === 'reasoning' || item.type === 'assistant_message',
      )
      if (row.type === 'PLANNER_RESPONSE') {
        for (const callId of node.projector.plannerToolCallIds(
          foregroundPrefixIds,
        ))
          matchables.push({ type: 'tool_call', callId })
      }
      const postRpcStep = fgReconciler.reconcile(row, matchables, node.wokeUp)
      stream.wokeUp ||= fgReconciler.postRpc
      if (usage && postRpcStep && row.status === 'DONE')
        wakeupSteps.set(stepIndex, usage)
      else if (row.type === 'PLANNER_RESPONSE') wakeupSteps.delete(stepIndex)
      if (usage || fgReconciler.postRpc) onWakeupState?.(computeWakeupState())
      if (Number.isInteger(stepIndex) && stepIndex > lastStepIndex) {
        lastStepIndex = stepIndex
        onStep?.(lastStepIndex)
      }
      if (dedupe.fresh(row)) {
        const timestamp =
          typeof row.created_at === 'string' ? row.created_at : undefined
        for (const item of items) {
          if (item.type === 'tool_call')
            emitLiveTool(item, timestamp, row.type === 'SYSTEM_MESSAGE')
          else if (item.type === 'error') onItem(item, timestamp)
        }
      }
    }
  }
  const deferReaderFault = () =>
    stream.hasReaderFault &&
    (isWakeupQuestionDeclined?.() || isWaitingOnQuestion?.())
  try {
    await stream.attach(consume)
    if (ownsObservation) stream.start()
    await stream.poll().catch((error) => {
      if (!signal?.aborted && !deferReaderFault()) throw error
    })
    while (true) {
      const now = Date.now()
      // Reset the stall watchdog across host sleep/suspension gaps that jump past a normal poll wake.
      if (now - lastTick > expectedPollMs + 10_000) {
        lastActivity = now
      }
      lastTick = now
      const revision = stream.revision
      if (signal?.aborted) {
        if (
          signal.reason instanceof Error &&
          signal.reason.name !== 'AbortError'
        )
          throw signal.reason
        return finish('canceled')
      }
      if (stream.error && !deferReaderFault()) throw stream.error
      if (!stream.acceptsRows())
        throw new Error('Brain observation stopped during turn')
      if (isWakeupQuestionDeclined?.()) return finish('canceled')
      if (stream.error) {
        // A native question can remain pending long enough for transient I/O to recover.
        await stream.wait(revision, signal)
        continue
      }
      if (initial) {
        if (
          stream.missing &&
          !node.retryPending() &&
          (!withinGrace() || !(await sessionDirExists()))
        )
          return { status: 'completed' }
        if (!anchored && !withinGrace()) return { status: 'completed' }
        if (anchored && Date.now() - lastActivity >= stallTimeoutMs)
          return stall()
        await stream.wait(revision, signal)
        continue
      }
      const failure = node.failure(retryTimeoutMs)
      if (failure)
        return {
          ...finish('failed', failure),
          ...(node.retryPending() ? { stalled: true } : {}),
        }
      node.settleAbortedDenial(deniedToolCallIds)
      while (takeQuestionAnswer) {
        const question =
          node.projector.consumePendingQuestion(answerForQuestion)
        if (!question) break
        if (fgReconciler.postRpc) {
          node.markQuestionAnswered()
          lastActivity = Date.now()
        } else {
          const completed = node.projector.completePendingQuestion(
            question.tool,
            question.answer.answerText,
            question.answer.declined ? 'canceled' : 'completed',
          )
          emitLiveTool(completed)
          node.markQuestionSettled()
        }
      }
      const checkedVersion = foldVersion
      const childMinStep = node.currentTurnFloor()
      const activeChildIds = activeChildIdsForTurn(
        subagents,
        node.projector,
        childMinStep,
        runtimeEpoch?.ignoredSubagentIds,
      )
      const childStatus = await settleChildren({
        root,
        parentNode: node,
        parentInbox: inbox,
        activeChildIds,
        parentSubagents: subagents,
        parentMinStepIndex: childMinStep,
        retryTimeoutMs,
      })
      if (signal?.aborted || checkedVersion !== foldVersion) continue
      if (childStatus.error)
        return {
          ...finish('failed', childStatus.error),
          ...(childStatus.stalled ? { stalled: true } : {}),
        }
      if (
        childStatus.settled &&
        node.isSettled(inbox) &&
        !node.projector.hasPendingQuestion() &&
        !isWaitingOnUser?.()
      ) {
        if (drainAcp) await drainAcp()
        if (
          signal?.aborted ||
          checkedVersion !== foldVersion ||
          isWaitingOnUser?.() ||
          isWakeupQuestionDeclined?.()
        )
          continue
        resolveLiveTools([])
        if (activeChildIds.length > 0) {
          const latestInbox = adjustInboxForEpoch(
            await readInboxState(root, nativeId),
            runtimeEpoch,
            stream.postEpochSignatures,
          )
          if (node.hasUnprocessedMessages(latestInbox)) continue
        }
        if (checkedVersion !== foldVersion) continue
        return finish('completed')
      }
      // Background tasks, live children and user approvals may run indefinitely; woken-turn tools and
      // post-wakeup/post-tool LLM generation get a bounded longer window (wokenStallTimeoutMs) before stalling.
      const tracking =
        node.projector.hasBackground() ||
        !childStatus.settled ||
        isWaitingOnUser?.() === true
      const idleLimitMs = node.isWokenWorking()
        ? wokenStallTimeoutMs
        : stallTimeoutMs
      if (tracking || node.retryPending()) lastActivity = Date.now()
      else if (Date.now() - lastActivity >= idleLimitMs) {
        return stall(idleLimitMs)
      }
      await stream.wait(revision, signal)
    }
  } finally {
    if (ownsObservation) await stream.dispose()
  }
}

export type BrainWatchHandle = (() => void) & {
  /** Drain rows written so far, then end the active child turn as canceled. */
  cancel: () => Promise<void>
  /** Poll once immediately so terminal child states are observed before sibling cleanup. */
  flush: () => Promise<void>
  /** Immediately mark the child turn canceled and stop polling without awaiting drain. */
  forceCancel: () => void
  isRunning: () => boolean
  /** Stopped or canceled: no further rows are read. */
  isStopped: () => boolean
  /** Stopped, or canceling and draining its last rows. */
  isStopping: () => boolean
  /** Where a replacement watcher for the same child session should resume. */
  resumePoint: () => { afterStep: number; turnSeq: number }
  ready: Promise<void>
}

/** Tail complete JSONL records; the returned handle stops future emissions or cancels an active child turn. */
// shortcut: JSONL is polled every second; use filesystem notifications if many simultaneous child sessions make polling costly.
export function watchBrain(
  root: string,
  nativeId: string,
  sessionId: string,
  emit: (event: ProviderEvent) => void,
  pollIntervalMs = 1000,
  isParentTurnActive?: () => boolean,
  /** Resume an existing child session: earlier steps are only projected, and turn IDs continue. */
  resume?: { afterStep: number; turnSeq: number },
  onSubagents?: (subagents: readonly SubagentRecord[]) => void,
  retryTimeoutMs = RETRY_IDLE_TIMEOUT_MS,
  mcpServerNames: readonly string[] = [],
  reader?: TranscriptReader,
): BrainWatchHandle {
  if (!sessionIdPattern.test(nativeId))
    throw new Error('Invalid Brain session ID')
  const timeline = new Timeline(sessionId, mcpServerNames)
  const node = new BrainNode(nativeId, sessionId, {
    mcpServerNames,
    turnFloor: resume?.afterStep ?? -1,
    reader,
  })
  let stopped = false
  let inflight: Promise<void> | undefined
  let cancelling: Promise<void> | undefined
  let warned = false
  let running = !resume
  let turnStartedEmitted = Boolean(resume)
  let turnSeq = resume?.turnSeq ?? 1
  let turnId = `${sessionId}:turn:${turnSeq}`
  let lastStepIndex = resume?.afterStep ?? -1
  const boundaryCache: TurnBoundaryCache = new Map()
  const dedupe = new StepDedupe()
  const childUsage = new ModelUsageObserver(
    resume?.afterStep ?? -1,
    (usage) => {
      if (stopped) return false
      const { contextWindowUsedTokens } = promptUsage(null, { lastStep: usage })
      emit({
        type: 'session.usage',
        sessionId,
        turnId: running ? turnId : undefined,
        usage: { contextWindowUsedTokens },
      })
    },
  )
  function ensureTurnStarted() {
    if (running && !turnStartedEmitted && !stopped) {
      turnStartedEmitted = true
      emit({
        type: 'session.turn',
        sessionId,
        turnId,
        state: 'started',
      })
    }
  }
  function finishTurn(
    state: 'completed' | 'failed' | 'canceled',
    error?: string,
  ) {
    if (!running || stopped || (cancelling && state !== 'canceled')) return
    ensureTurnStarted()
    running = false
    for (const item of timeline.terminalize(state, error))
      emit({ type: 'timeline.item', sessionId, item })
    emit({
      type: 'session.turn',
      sessionId,
      turnId,
      state,
      ...(state === 'failed' && error ? { error: { message: error } } : {}),
    })
  }
  /** One poll at a time; concurrent callers share the pass in flight. */
  function poll(): Promise<void> {
    if (stopped) return Promise.resolve()
    inflight ??= pollOnce().finally(() => {
      inflight = undefined
    })
    return inflight
  }
  async function pollOnce() {
    try {
      const {
        inbox,
        subagents,
        rows,
        reset,
        missing,
        snapshotStepIndices,
        observedAt,
      } = await node.pollTranscript(root, {
        onReset: () => boundaryCache.clear(),
        onSnapshot: (steps) => dedupe.prune(steps),
      })
      onSubagents?.(subagents)
      if (missing) {
        if (node.retryPending()) {
          const failure = node.failure(retryTimeoutMs)
          if (failure) finishTurn('failed', failure)
        } else if (isParentTurnActive && !isParentTurnActive())
          finishTurn('completed')
        return
      }
      // The initial spawn message names the conversation that owns this session's turns; every
      // correlation below keys off it so the inbox filter and the transcript-row sender check agree.
      const init = inbox.messages.find((m) =>
        isNativeToolName(m.tool?.name ?? '', 'invoke_subagent'),
      )
      const parentId = init?.tool?.conversationId
      const tagged = selectParentWakeups(inbox, parentId)
      node.projector.observeRows(rows)
      for (const row of rows) {
        if (stopped) return
        const stepIndex = Number(row.step_index)
        // A rewrite re-delivers earlier steps; StepDedupe.fresh hashes type/source/status/content/thinking/tool_calls/error.
        const replayed =
          !dedupe.fresh(row) ||
          (resume !== undefined && stepIndex <= resume.afterStep)
        if (Number.isInteger(stepIndex) && stepIndex > lastStepIndex)
          lastStepIndex = stepIndex
        const items = node.ingest(row, observedAt)
        if (replayed) continue
        if (
          !running &&
          !isRestartNotice(row) &&
          (row.type === 'USER_INPUT' ||
            row.type === 'SYSTEM_MESSAGE' ||
            row.type === 'PLANNER_RESPONSE')
        ) {
          if (Number.isInteger(stepIndex)) node.advanceTurnFloor(stepIndex - 1)
          running = true
          turnId = `${sessionId}:turn:${++turnSeq}`
          turnStartedEmitted = true
          emit({
            type: 'session.turn',
            sessionId,
            turnId,
            state: 'started',
          })
        } else if (running && items.length > 0) {
          ensureTurnStarted()
        }
        const timestamp =
          typeof row.created_at === 'string' ? row.created_at : undefined
        for (const item of items) {
          timeline.track(item)
          emit({
            type: 'timeline.item',
            sessionId,
            item,
            timestamp,
          })
        }
      }
      childUsage.observe({ rows, reset, snapshotStepIndices })
      const wakeupSteps = parentId ? node.wakeupStepsFrom(parentId) : []
      const parentTurnActive = isParentTurnActive?.() !== false
      let awaitingCrossTurnEntry = false
      if (
        parentId &&
        init?.tool?.stepIndex !== undefined &&
        tagged.length > 0
      ) {
        const latestWakeParentStep = tagged[tagged.length - 1].tool.stepIndex
        const resolved = await readTurnBoundaryBefore(
          root,
          parentId,
          latestWakeParentStep,
          boundaryCache,
        )
        if (!resolved.ready) {
          awaitingCrossTurnEntry = parentTurnActive
        } else {
          const turn = resolveReWokenTurn(
            tagged,
            wakeupSteps,
            resolved.boundary,
            init.tool.stepIndex,
          )
          if (turn.status === 're-woken')
            node.advanceTurnFloor(turn.startStep - 1)
          else
            awaitingCrossTurnEntry =
              turn.status === 'awaiting-row' &&
              parentTurnActive &&
              tagged.length > wakeupSteps.length
        }
      }
      if (awaitingCrossTurnEntry) return
      const childMinStep = node.currentTurnFloor()
      const activeChildIds = activeChildIdsForTurn(
        subagents,
        node.projector,
        childMinStep,
      )
      const childStatus = await settleChildren({
        root,
        parentNode: node,
        parentInbox: inbox,
        activeChildIds,
        parentSubagents: subagents,
        parentMinStepIndex: childMinStep,
        retryTimeoutMs,
      })
      if (childStatus.error) finishTurn('failed', childStatus.error)
      const hasPendingParentWakeup =
        tagged.length > wakeupSteps.length && isParentTurnActive?.() !== false
      if (
        running &&
        (node.retryPending() ||
          (!hasPendingParentWakeup && !node.hasUnprocessedMessages(inbox)))
      ) {
        const failure = node.failure(retryTimeoutMs)
        if (failure) {
          finishTurn('failed', failure)
        } else if (childStatus.settled && node.isSettled(inbox)) {
          const latestInbox =
            activeChildIds.length > 0
              ? await readInboxState(root, nativeId)
              : inbox
          if (!node.hasUnprocessedMessages(latestInbox)) finishTurn('completed')
        }
      }
    } catch (error) {
      if (!warned && !stopped) {
        warned = true
        emit({
          type: 'timeline.item',
          sessionId,
          item: {
            type: 'error',
            id: `${sessionId}:brain-error`,
            message: `Cannot read child transcript: ${formatErrorMessage(error)}`,
          },
        })
      }
    }
  }
  const timer = setInterval(() => {
    void poll()
  }, pollIntervalMs)
  timer.unref()
  const ready = poll()
  return Object.assign(
    () => {
      stopped = true
      clearInterval(timer)
    },
    {
      ready,
      isRunning() {
        return running && !stopped
      },
      isStopped() {
        return stopped
      },
      isStopping() {
        return stopped || cancelling !== undefined
      },
      resumePoint() {
        return { afterStep: lastStepIndex, turnSeq }
      },
      async flush() {
        if (stopped || cancelling) return
        await inflight?.catch(() => undefined)
        if (stopped || cancelling) return
        await poll().catch(() => undefined)
      },
      cancel() {
        if (stopped) return Promise.resolve()
        clearInterval(timer)
        // Read what the child wrote before it was killed so its timeline is complete.
        cancelling ??= (async () => {
          await inflight?.catch(() => undefined)
          await poll().catch(() => undefined)
          finishTurn('canceled')
          stopped = true
        })()
        return cancelling
      },
      forceCancel() {
        if (stopped) return
        clearInterval(timer)
        finishTurn('canceled')
        stopped = true
      },
    },
  )
}
