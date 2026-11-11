import { stat } from 'node:fs/promises'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { formatErrorMessage, sessionIdPattern } from './common.ts'
import { Timeline, addStepUsage, type StepUsage } from './mapping.ts'
import type {
  ProviderEvent,
  ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'
import type { NormalizedQuestion } from './questions.ts'
import {
  StepDedupe,
  countRowMessages,
  extractStepUsage,
  isMissing,
  isRestartNotice,
  readInboxState,
  readLastStepIndex,
  readSubagents,
  readTranscriptRows,
  RETRY_IDLE_TIMEOUT_MS,
  type InboxState,
  type SubagentRecord,
} from './brain-io.ts'
import {
  BrainNode,
  BrainTranscriptProjector,
  activeChildIdsForTurn,
  type PendingQuestion,
  type QuestionOutcome,
} from './brain-projector.ts'

export {
  BrainTranscriptProjector,
  countRowMessages,
  readInboxState,
  readLastStepIndex,
  readSubagents,
  readTranscriptRows,
  type SubagentRecord,
}

/** Determine whether the session has running background tasks or active subagents. Standing IsDaemon schedules are excluded by default so foreground interrupts do not kill them, and included when checking whether an idle runtime can stop. */
export async function hasActiveBackgroundWork(
  root: string,
  nativeId: string,
  options: { includeDaemons?: boolean; retryTimeoutMs?: number } = {},
  visiting = new Set<string>(),
  isChild = false,
): Promise<boolean> {
  if (!sessionIdPattern.test(nativeId) || visiting.has(nativeId)) return false
  visiting.add(nativeId)
  try {
    const node = new BrainNode(nativeId)
    const [{ inbox, subagents }, rows] = await Promise.all([
      node.pollMetadata(root),
      readTranscriptRows(root, nativeId),
    ])
    if (isChild && rows.length === 0) return true
    const observedAt = await stat(
      join(root, nativeId, '.system_generated/logs/transcript_full.jsonl'),
    ).then(
      (info) => info.mtimeMs,
      () => Date.now(),
    )
    for (const row of rows) {
      // The restarted runtime left no woken turn running.
      if (isRestartNotice(row)) node.resetPhase()
      node.ingest(row, observedAt)
    }
    if (inbox.unconsumedHighPriority.length > 0) return true
    if (
      node.projector.hasBackground() ||
      (options.includeDaemons && node.projector.hasDaemons())
    )
      return true
    // A timed-out model retry does not stop tracked processes or descendants.
    if (!node.failure(options.retryTimeoutMs)) {
      if (node.retryPending()) return true
      if (
        isChild
          ? node.hasUnprocessedMessages(inbox) || !node.isSettled(inbox)
          : (node.foregroundIdleReached &&
              node.hasUnprocessedMessages(inbox)) ||
            node.isWokenWorking()
      )
        return true
    }
    const activeChildIds = activeChildIdsForTurn(
      subagents,
      node.projector,
      isChild ? -1 : node.turnStartStep,
    )
    for (const childId of activeChildIds) {
      if (await hasActiveBackgroundWork(root, childId, options, visiting, true))
        return true
    }
    return false
  } finally {
    visiting.delete(nativeId)
  }
}

async function settleChildren(
  root: string,
  parentNode: BrainNode,
  parentInbox: InboxState,
  activeChildIds: readonly string[],
  visiting = new Set<string>(),
  retryTimeoutMs = RETRY_IDLE_TIMEOUT_MS,
): Promise<{ settled: boolean; error?: string; stalled?: boolean }> {
  if (visiting.has(parentNode.nativeId)) return { settled: true }
  visiting.add(parentNode.nativeId)
  try {
    let settled = true
    for (const childId of activeChildIds) {
      if (!sessionIdPattern.test(childId) || visiting.has(childId)) continue
      let child = parentNode.children.get(childId)
      if (!child) {
        child = new BrainNode(childId)
        parentNode.children.set(childId, child)
      }
      const { inbox: childInbox, subagents: childSubagents } =
        await child.pollMetadata(root)
      try {
        const { rows, reset } = await child.reader.read(root, childId)
        if (reset) child.reset(childInbox, childSubagents)
        for (const row of rows) child.ingest(row)
      } catch (error) {
        if (isMissing(error)) {
          const failure = child.failure(retryTimeoutMs)
          if (failure)
            return {
              settled: false,
              error: `Subagent ${childId} failed: ${failure}`,
              stalled: child.retryPending(),
            }
          settled = false
          continue
        }
        throw error
      }
      if (!child.retryPending() && child.hasUnprocessedMessages(childInbox)) {
        settled = false
        continue
      }
      const failure = child.failure(retryTimeoutMs)
      if (failure)
        return {
          settled: false,
          error: `Subagent ${childId} failed: ${failure}`,
          stalled: child.retryPending(),
        }
      if (!child.isSettled(childInbox)) settled = false
      const grandchildIds = activeChildIdsForTurn(
        childSubagents,
        child.projector,
        -1,
      )
      const nested = await settleChildren(
        root,
        child,
        childInbox,
        grandchildIds,
        visiting,
        retryTimeoutMs,
      )
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

function pause(ms: number, signal?: AbortSignal) {
  if (signal?.aborted) return Promise.resolve()
  return delay(ms, undefined, { ref: false, signal }).catch(() => undefined)
}

/** Follow background tasks and post-RPC Reactive Wakeup steps in transcript_full.jsonl until the turn settles. */
export async function settleBrainTurn(
  root: string,
  nativeId: string,
  sessionId: string,
  onItem: (item: ProviderTimelineItem, timestamp?: string) => void,
  {
    signal,
    minStepIndex = -1,
    pollIntervalMs = 200,
    acpToolCallIds = [],
    deniedToolCallIds,
    shouldEmitPreWakeupTool,
    emitForeground = false,
    onStep,
    transcriptGraceMs = 0,
    stallTimeoutMs = 120_000,
    wokenStallTimeoutMs = 15 * 60_000,
    retryTimeoutMs = RETRY_IDLE_TIMEOUT_MS,
    isWaitingOnUser,
    onQuestion,
    onQuestionSettled,
    onSubagents,
  }: {
    onSubagents?: (subagents: readonly SubagentRecord[]) => void
    signal?: AbortSignal
    minStepIndex?: number
    pollIntervalMs?: number
    acpToolCallIds?: readonly string[]
    deniedToolCallIds?: ReadonlySet<string>
    shouldEmitPreWakeupTool?: (
      item: Extract<ProviderTimelineItem, { type: 'tool_call' }>,
    ) => boolean
    /** Emit foreground assistant/reasoning/tool items from the transcript if ACP suppressed them. */
    emitForeground?: boolean
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
    /** Handle an unlogged ask_question step discovered at the tail of the transcript. */
    onQuestion?: (question: PendingQuestion) => Promise<QuestionOutcome>
    /** Invoked when a GENERIC row settles a logged ask_question step directly. */
    onQuestionSettled?: (
      questions: readonly NormalizedQuestion[],
      wokeUp: boolean,
    ) => void
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
  const node = new BrainNode(nativeId, sessionId, onQuestionSettled)
  const startedAt = Date.now()
  let lastActivity = startedAt
  let fingerprint = ''
  let initial = true
  // Once anchored, a rewrite caught mid-O_TRUNC (empty or partial file) is not "no turn".
  let anchored = false
  let preamble: Record<string, unknown>[] = []
  let lastStepIndex = -1
  const dedupe = new StepDedupe()
  let lastStepUsage: StepUsage | undefined
  let wakeupUsage: StepUsage | undefined
  const finish = (
    status: 'completed' | 'canceled' | 'failed',
    error?: string,
  ) => ({
    status,
    ...(error !== undefined ? { error } : {}),
    lastStepUsage,
    ...(node.wokeUp ? { wokeUp: true as const } : {}),
    ...(wakeupUsage ? { wakeupUsage } : {}),
  })
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
  while (true) {
    if (signal?.aborted) {
      if (signal.reason instanceof Error && signal.reason.name !== 'AbortError')
        throw signal.reason
      return finish('canceled')
    }
    const { inbox, subagents } = await node.pollMetadata(root)
    onSubagents?.(subagents)
    const nextFingerprint = `${inbox.messages.length}:${inbox.readIds.size}:${subagents
      .map((s) => `${s.conversationId}=${s.state}`)
      .join(',')}`
    if (nextFingerprint !== fingerprint) {
      fingerprint = nextFingerprint
      lastActivity = Date.now()
    }
    let rows: Record<string, unknown>[]
    try {
      const result = await node.reader.read(root, nativeId)
      rows = result.rows
      if (result.snapshotStepIndices) dedupe.prune(result.snapshotStepIndices)
      if (result.reset) {
        initial = true
        preamble = []
        wakeupUsage = undefined
        node.reset(inbox, subagents)
      }
    } catch (error) {
      if (!isMissing(error)) throw error
      if (node.retryPending()) {
        const failure = node.failure(retryTimeoutMs)
        if (failure) return { ...finish('failed', failure), stalled: true }
        await pause(pollIntervalMs, signal)
        continue
      }
      // A missing transcript cannot be settled from; it only delays completion while agy may still flush it.
      if (!withinGrace() || !(await sessionDirExists()))
        return { status: 'completed' }
      await pause(pollIntervalMs, signal)
      continue
    }
    if (rows.length) lastActivity = Date.now()
    let slice = rows
    if (initial) {
      // Keep rows already consumed by the cursor until this turn's USER_INPUT is visible.
      preamble.push(...rows)
      let start = -1
      for (let i = preamble.length - 1; i >= 0; i--) {
        if (
          preamble[i].type === 'USER_INPUT' &&
          Number(preamble[i].step_index) > minStepIndex
        ) {
          start = i
          break
        }
      }
      if (start < 0) {
        if (!anchored && !withinGrace()) return { status: 'completed' }
        // A rewrite that dropped this turn altogether is bounded like any other stall.
        if (anchored && Date.now() - lastActivity >= stallTimeoutMs)
          return stall()
        await pause(pollIntervalMs, signal)
        continue
      }
      initial = false
      anchored = true
      for (const row of preamble.slice(0, start))
        node.consumedMessages += countRowMessages(row)
      node.projector.bindToolCallIds(acpToolCallIds)
      slice = preamble.slice(start)
      preamble = []
    }
    for (const row of slice) {
      const stepIndex = Number(row.step_index)
      const items = node.ingest(row)
      const usage = extractStepUsage(row)
      if (usage) {
        lastStepUsage = usage
        if (node.wokeUp && row.status === 'DONE')
          wakeupUsage = addStepUsage(wakeupUsage, usage)
      }
      if (Number.isInteger(stepIndex) && stepIndex > lastStepIndex) {
        lastStepIndex = stepIndex
        onStep?.(lastStepIndex)
      }
      if (dedupe.fresh(row)) {
        const timestamp =
          typeof row.created_at === 'string' ? row.created_at : undefined
        for (const item of items) {
          if (item.type === 'user_message') continue
          if (node.wokeUp || emitForeground || item.type === 'error') {
            onItem(item, timestamp)
          } else if (
            item.type === 'tool_call' &&
            item.status !== 'running' &&
            (shouldEmitPreWakeupTool ? shouldEmitPreWakeupTool(item) : true)
          ) {
            onItem(item, timestamp)
          }
        }
      }
    }
    if (signal?.aborted) continue
    const failure = node.failure(retryTimeoutMs)
    if (failure)
      return {
        ...finish('failed', failure),
        ...(node.retryPending() ? { stalled: true } : {}),
      }
    node.settleAbortedDenial(deniedToolCallIds)
    let continuedQuestion = false
    // A question at the head of the pending queue blocks agy until it is answered.
    // Without a handler (direct callers and tests) it is closed as already handled.
    while (true) {
      const question = node.projector.consumePendingQuestion()
      if (!question) break
      const outcome: QuestionOutcome = onQuestion
        ? await onQuestion({ ...question, wokeUp: node.wokeUp })
        : { status: 'already_handled' }
      if (signal?.aborted || outcome.status === 'canceled') {
        onItem(
          node.projector.completePendingQuestion(
            question.tool,
            null,
            'canceled',
          ),
        )
        return finish('canceled')
      }
      const completed = node.projector.completePendingQuestion(
        question.tool,
        outcome.answerText ?? null,
      )
      if (outcome.status === 'answered') {
        onItem(completed)
        node.markQuestionContinuation()
        lastActivity = Date.now()
        continuedQuestion = true
        break
      }
      // Answered through elicitation/create: only surface the card where its tool call is shown.
      if (
        node.wokeUp ||
        emitForeground ||
        (completed.type === 'tool_call' &&
          shouldEmitPreWakeupTool?.(completed) !== false)
      )
        onItem(completed)
      node.markQuestionSettled()
    }
    if (continuedQuestion) continue
    const activeChildIds = activeChildIdsForTurn(
      subagents,
      node.projector,
      minStepIndex,
    )
    const childStatus = await settleChildren(
      root,
      node,
      inbox,
      activeChildIds,
      new Set(),
      retryTimeoutMs,
    )
    if (signal?.aborted) continue
    if (childStatus.error)
      return {
        ...finish('failed', childStatus.error),
        ...(childStatus.stalled ? { stalled: true } : {}),
      }
    if (childStatus.settled && node.isSettled(inbox)) {
      if (activeChildIds.length > 0) {
        if (node.hasUnprocessedMessages(await readInboxState(root, nativeId)))
          continue
      }
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
    await pause(pollIntervalMs, signal)
  }
}

export type BrainWatchHandle = (() => void) & {
  /** Drain rows written so far, then end the active child turn as canceled. */
  cancel: () => Promise<void>
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
): BrainWatchHandle {
  if (!sessionIdPattern.test(nativeId))
    throw new Error('Invalid Brain session ID')
  const timeline = new Timeline(sessionId)
  const node = new BrainNode(nativeId, sessionId)
  let stopped = false
  let inflight: Promise<void> | undefined
  let cancelling: Promise<void> | undefined
  let warned = false
  let running = !resume
  let turnStartedEmitted = Boolean(resume)
  let turnSeq = resume?.turnSeq ?? 1
  let turnId = `${sessionId}:turn:${turnSeq}`
  let lastStepIndex = resume?.afterStep ?? -1
  const dedupe = new StepDedupe()
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
      const { inbox, subagents } = await node.pollMetadata(root)
      onSubagents?.(subagents)
      const { rows, reset, snapshotStepIndices } = await node.reader.read(
        root,
        nativeId,
      )
      if (snapshotStepIndices) dedupe.prune(snapshotStepIndices)
      if (reset) {
        node.reset(inbox, subagents)
      }
      for (const row of rows) {
        if (stopped) return
        const stepIndex = Number(row.step_index)
        // A rewrite re-delivers earlier steps; StepDedupe.fresh hashes type/status/content/thinking/tool_calls.
        const replayed =
          !dedupe.fresh(row) ||
          (resume !== undefined && stepIndex <= resume.afterStep)
        if (Number.isInteger(stepIndex) && stepIndex > lastStepIndex)
          lastStepIndex = stepIndex
        const items = node.ingest(row)
        if (replayed) continue
        if (
          !running &&
          !isRestartNotice(row) &&
          (row.type === 'USER_INPUT' ||
            row.type === 'SYSTEM_MESSAGE' ||
            row.type === 'PLANNER_RESPONSE')
        ) {
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
      const activeChildIds = activeChildIdsForTurn(
        subagents,
        node.projector,
        -1,
      )
      const childStatus = await settleChildren(
        root,
        node,
        inbox,
        activeChildIds,
        new Set(),
        retryTimeoutMs,
      )
      if (childStatus.error) finishTurn('failed', childStatus.error)
      if (
        running &&
        (node.retryPending() || !node.hasUnprocessedMessages(inbox))
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
      if (isMissing(error)) {
        if (node.retryPending()) {
          const failure = node.failure(retryTimeoutMs)
          if (failure) finishTurn('failed', failure)
        } else if (isParentTurnActive && !isParentTurnActive())
          finishTurn('completed')
        return
      }
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
    },
  )
}
