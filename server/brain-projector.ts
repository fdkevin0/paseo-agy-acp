import {
  cleanToolName,
  isNativeToolName,
  isAskQuestionName,
  record,
  sessionIdPattern,
} from './common.ts'
import { toolDetail } from './mapping.ts'
import type { ProviderTimelineItem } from '@getpaseo/plugin/server/provider'
import {
  normalizeAskQuestionInput,
  type NormalizedQuestion,
} from './questions.ts'
import {
  SUBAGENT_ALIVE,
  TranscriptReader,
  countRowMessages,
  isCompletePlannerResponse,
  isPlanExecuteNotice,
  isRetryWarning,
  RETRY_IDLE_TIMEOUT_MS,
  isRestartNotice,
  readInboxState,
  readSubagents,
  stepErrorMessage,
  type InboxState,
  type SubagentRecord,
} from './brain-io.ts'

export type NativeToolIdentitySource = 'tool_id' | 'step_metadata' | 'acp_input'

export type ObservedToolSlot = {
  slot: number
  stepIndex: number
  args: unknown
  toolId?: string
  isQuestion: boolean
  invalid: boolean
}

export type PendingTool = {
  id: string
  callId: string
  nativeIdentitySource?: NativeToolIdentitySource
  name: string
  args: Record<string, unknown>
  rawInput: unknown
  stepIndex: number
  slotIndex: number
  isQuestion?: boolean
  questions?: NormalizedQuestion[]
  questionConsumed?: boolean
  acpIndex?: number
  fallbackId?: string
  genericSeen?: boolean
  deferredCompletion?: {
    output: unknown
    status: 'completed' | 'failed' | 'canceled'
    text: string
  }
  questionCompleted?: boolean
}

export type PendingQuestion = {
  tool: PendingTool
  questions: NormalizedQuestion[]
  wokeUp: boolean
  beginContinuation?: (answerText: string) => void
}

export type QuestionOutcome =
  | { status: 'answered' | 'already_handled'; answerText?: string }
  | { status: 'canceled' }

const backgroundTaskPattern =
  /Tool is running as a background task with task id:\s*(\S+)/
const systemTaskResultPattern =
  /\bsender=(\S+)[^\n]*\scontent=Task id "([^"]+)" (finished|was canceled|failed) with result:\n([\s\S]*?)(?:\nLog: file:\/\/\S+)?(?=\n(?:\d+\.\s+)?\[Message\]\s+|\s*<\/SYSTEM_MESSAGE>|\s*$)/g
const messageHeaderSenderPattern =
  /(?:^|\n)(?:\d+\.\s+)?\[Message\]\s+(?:timestamp=\S+\s+)?sender=(\S+)/g

/** Logged while still running: the transcript never records this step's own result. */
const runningStepStatuses = new Set(['RUNNING', 'WAITING'])

function terminalToolStatus(
  status: unknown,
): 'completed' | 'failed' | 'canceled' {
  if (status === 'ERROR') return 'failed'
  if (status === 'CANCELED' || status === 'INTERRUPTED') return 'canceled'
  return 'completed'
}

/** A checkpoint takes a step index between a response and its GENERIC rows; shift expected rows past it. */
export function shiftPendingPastCheckpoint(
  pending: readonly { stepIndex: number }[],
  checkpointStep: number,
) {
  for (const p of pending) if (p.stepIndex >= checkpointStep) p.stepIndex++
}

/** Match by stepIndex first so an unlogged WAITING ask_question never steals a sibling tool's GENERIC row. */
export function takePendingForGeneric<
  T extends { stepIndex: number; isQuestion?: boolean },
>(pending: T[], genericStep: number): T | undefined {
  let matchIndex = pending.findIndex((p) => p.stepIndex === genericStep)
  if (matchIndex < 0) {
    matchIndex = pending.findIndex((p) => !p.isQuestion)
    if (matchIndex < 0) matchIndex = 0
  }
  return pending.splice(matchIndex, 1)[0]
}

/** Project transcript_full.jsonl steps and correlate tools with structured Brain metadata. */
export class BrainTranscriptProjector {
  private pending: PendingTool[] = []
  private nativeIdentities = new WeakMap<
    ProviderTimelineItem,
    NativeToolIdentitySource
  >()
  private toolRawOutputs = new WeakMap<ProviderTimelineItem, unknown>()
  private lastBackgroundGeneric?: {
    callId: string
    source: NativeToolIdentitySource
  }
  private background = new Map<string, PendingTool>()
  private daemons = new Map<string, number>()
  private touchedSubagents = new Set<string>()
  private stepCallIds = new Map<number, string>()
  private stepSubagents = new Map<number, SubagentRecord>()
  private callSubagents = new Map<string, SubagentRecord>()
  private observedSlots: ObservedToolSlot[] = []
  private observedPending: ObservedToolSlot[] = []
  private slotIndex = 0
  private toolIndex = 0
  private deferAnonymous = false
  private retired: PendingTool[] = []
  private acpToolCallIds: readonly (string | undefined)[] = []
  private verifiedAcpToolIds?: ReadonlySet<string>
  private lastErrorCallId?: string
  private sessionId: string
  private onQuestionSettledByGeneric?: (
    questions: readonly NormalizedQuestion[],
    tool: PendingTool,
  ) => void

  constructor(
    sessionId: string,
    onQuestionSettledByGeneric?: (
      questions: readonly NormalizedQuestion[],
      tool: PendingTool,
    ) => void,
  ) {
    this.sessionId = sessionId
    this.onQuestionSettledByGeneric = onQuestionSettledByGeneric
  }

  nativeToolIdentitySource(
    item: ProviderTimelineItem,
  ): NativeToolIdentitySource | undefined {
    return this.nativeIdentities.get(item)
  }

  toolRawOutput(item: ProviderTimelineItem): unknown {
    return this.toolRawOutputs.get(item)
  }

  backgroundGenericAnchor():
    { callId: string; source: NativeToolIdentitySource } | undefined {
    return this.lastBackgroundGeneric
  }

  bindToolCallIds(
    ids: readonly (string | undefined)[],
    startIndex = 0,
    verifiedAcpToolIds?: ReadonlySet<string>,
  ) {
    this.acpToolCallIds = ids
    this.toolIndex = startIndex
    this.verifiedAcpToolIds = verifiedAcpToolIds
  }

  /**
   * Pre-scan transcript rows so tool slots know their checkpoint-shifted GENERIC step indexes before projection.
   * Invariant: every row passed here must also be passed to `project()` (in order, exactly once), because
   * `slotIndex` in `project()` indexes into `observedSlots`; skipping either side desyncs slot lookups.
   */
  observeRows(
    rows: readonly Record<string, unknown>[],
  ): readonly ObservedToolSlot[] {
    for (const raw of rows) {
      const row = record(raw)
      if (!Number.isInteger(row.step_index)) continue
      const stepIndex = Number(row.step_index)
      switch (row.type) {
        case 'USER_INPUT':
          this.observedPending = []
          break
        case 'SYSTEM_MESSAGE':
          if (isRestartNotice(row)) this.observedPending = []
          break
        case 'ERROR_MESSAGE':
          if (!isRetryWarning(row)) this.observedPending = []
          break
        case 'CHECKPOINT':
          shiftPendingPastCheckpoint(this.observedPending, stepIndex)
          break
        case 'GENERIC': {
          const matched = takePendingForGeneric(this.observedPending, stepIndex)
          if (matched) {
            matched.stepIndex = stepIndex
            if (row.status === 'INVALID') matched.invalid = true
          }
          break
        }
        case 'PLANNER_RESPONSE':
          this.observedPending = []
          if (Array.isArray(row.tool_calls))
            row.tool_calls.forEach((value, index) => {
              const tool = record(value)
              const cleanName = cleanToolName(String(tool.name ?? 'Tool'))
              const slot: ObservedToolSlot = {
                slot: this.observedSlots.length,
                stepIndex: stepIndex + index + 1,
                args: tool.args,
                toolId:
                  typeof tool.id === 'string' && tool.id ? tool.id : undefined,
                isQuestion: isAskQuestionName(cleanName),
                invalid: false,
              }
              this.observedSlots.push(slot)
              this.observedPending.push(slot)
            })
          break
      }
    }
    return this.observedSlots
  }

  private effectiveStepIndex(slotIndex: number, stepIndex: number): number {
    return this.observedSlots[slotIndex]?.stepIndex ?? stepIndex
  }

  syncMetadata(inbox: InboxState, subagents: readonly SubagentRecord[]) {
    this.stepCallIds = new Map(inbox.stepCallIds)
    this.stepSubagents.clear()
    this.callSubagents.clear()
    for (const sub of subagents) {
      this.stepSubagents.set(sub.spawnStepIndex, sub)
      if (sub.spawnCallId) {
        this.stepCallIds.set(sub.spawnStepIndex, sub.spawnCallId)
        this.callSubagents.set(sub.spawnCallId, sub)
      }
    }
  }

  hasPending(): boolean {
    return this.retired.length > 0 || this.pending.some((p) => !p.isQuestion)
  }

  hasDeniedOutcome(
    denied: ReadonlySet<string>,
    lastStep?: Record<string, unknown>,
  ): boolean {
    if (denied.size === 0) return false
    if (this.pending.some((p) => denied.has(p.callId))) return true
    return (
      lastStep?.type === 'GENERIC' &&
      lastStep.status === 'ERROR' &&
      this.lastErrorCallId !== undefined &&
      denied.has(this.lastErrorCallId)
    )
  }

  clearPending() {
    this.pending = []
  }

  hasBackground(): boolean {
    return this.background.size > 0
  }

  hasDaemons(): boolean {
    return this.daemons.size > 0
  }

  consumePendingQuestion() {
    const next = this.pending.find((p) => !p.questionConsumed)
    if (!next?.isQuestion) return undefined
    next.questionConsumed = true
    return { tool: next, questions: next.questions ?? [] }
  }

  completePendingQuestion(
    pending: PendingTool,
    output: unknown,
    status: 'completed' | 'failed' | 'canceled' = 'completed',
  ): ProviderTimelineItem {
    pending.questionCompleted = true
    return this.finishTool(
      pending,
      output,
      status,
      typeof output === 'string' ? output : '',
    )
  }

  private forgetTask(taskId: string) {
    this.background.delete(taskId)
    this.daemons.delete(taskId)
  }

  touchedSubagentIds(): ReadonlySet<string> {
    const ids = new Set(this.touchedSubagents)
    for (const p of this.pending) {
      if (
        isNativeToolName(p.name, 'send_message') &&
        typeof p.args.Recipient === 'string' &&
        sessionIdPattern.test(p.args.Recipient)
      ) {
        ids.add(p.args.Recipient)
      }
    }
    return ids
  }

  private finishTool(
    pending: PendingTool,
    output: unknown,
    status: 'completed' | 'failed' | 'canceled',
    text: string,
  ): ProviderTimelineItem {
    const base = {
      type: 'tool_call' as const,
      id: pending.id,
      callId: pending.callId,
      name: pending.name,
      detail: toolDetail({
        title: pending.name,
        rawInput: pending.rawInput,
        rawOutput: output,
      }),
    }
    if (
      status === 'completed' &&
      (isNativeToolName(pending.name, 'invoke_subagent') ||
        isNativeToolName(pending.name, 'send_message')) &&
      base.detail.type === 'sub_agent' &&
      base.detail.childSessionId
    ) {
      this.touchedSubagents.add(base.detail.childSessionId)
    }
    const item: ProviderTimelineItem =
      status === 'failed'
        ? {
            ...base,
            status: 'failed',
            error: { message: text || 'Tool execution failed' },
          }
        : { ...base, status, error: null }
    if (pending.nativeIdentitySource)
      this.nativeIdentities.set(item, pending.nativeIdentitySource)
    this.toolRawOutputs.set(item, output)
    return item
  }

  private completeGeneric(
    pending: PendingTool,
    row: Record<string, unknown>,
  ): ProviderTimelineItem[] {
    const text = typeof row.content === 'string' ? row.content : ''
    const stepIndex = Number(row.step_index)
    if (runningStepStatuses.has(String(row.status))) {
      if (pending.callId && pending.nativeIdentitySource)
        this.lastBackgroundGeneric = {
          callId: pending.callId,
          source: pending.nativeIdentitySource,
        }
      const taskId = text.match(backgroundTaskPattern)?.[1]
      if (taskId) {
        if (pending.args.IsDaemon === true) {
          const max = Number(pending.args.MaxIterations)
          this.daemons.set(
            taskId,
            Number.isInteger(max) && max > 0 ? max : Infinity,
          )
        } else {
          this.background.set(taskId, pending)
        }
      }
      return []
    }
    if (pending.isQuestion && pending.questions) {
      this.onQuestionSettledByGeneric?.(pending.questions, pending)
    }
    if (
      isNativeToolName(pending.name, 'manage_task') &&
      pending.args.Action === 'kill' &&
      typeof pending.args.TaskId === 'string' &&
      terminalToolStatus(row.status) === 'completed'
    ) {
      this.forgetTask(pending.args.TaskId)
    }
    const subagent =
      this.callSubagents.get(pending.callId) ??
      this.stepSubagents.get(stepIndex)
    const rawOutput =
      isNativeToolName(pending.name, 'invoke_subagent') &&
      subagent &&
      (!text || !text.includes(subagent.conversationId))
        ? { conversationId: subagent.conversationId }
        : (row.content ?? row.error ?? null)
    const status = terminalToolStatus(row.status)
    this.lastErrorCallId = status === 'failed' ? pending.callId : undefined
    return this.finishOrDefer(pending, rawOutput, status, text)
  }
  private finishOrDefer(
    pending: PendingTool,
    output: unknown,
    status: 'completed' | 'failed' | 'canceled',
    text: string,
  ): ProviderTimelineItem[] {
    const item = this.finishTool(pending, output, status, text)
    if (pending.callId) return [item]
    pending.deferredCompletion = { output, status, text }
    return []
  }
  private startTool(pending: PendingTool): ProviderTimelineItem {
    const item: ProviderTimelineItem = {
      type: 'tool_call',
      id: pending.id,
      callId: pending.callId,
      name: pending.name,
      detail: toolDetail({
        title: pending.name,
        rawInput: pending.rawInput,
        rawOutput: null,
      }),
      status: 'running',
      error: null,
    }
    if (pending.nativeIdentitySource)
      this.nativeIdentities.set(item, pending.nativeIdentitySource)
    return item
  }
  private verifiedAcpId(acpIndex: number | undefined): string | undefined {
    const id =
      acpIndex === undefined ? undefined : this.acpToolCallIds[acpIndex]
    return id && (!this.verifiedAcpToolIds || this.verifiedAcpToolIds.has(id))
      ? id
      : undefined
  }
  /** Anonymous continuation tools wait for ACP/metadata identity; only the RPC boundary permits fallback. */
  resolveToolIdentities(deferAnonymous: boolean): ProviderTimelineItem[] {
    this.deferAnonymous = deferAnonymous
    const items: ProviderTimelineItem[] = []
    for (const pending of [...this.retired, ...this.pending]) {
      if (pending.callId) continue
      const stepId = this.stepCallIds.get(
        this.effectiveStepIndex(pending.slotIndex, pending.stepIndex),
      )
      const acpId = this.verifiedAcpId(pending.acpIndex)
      const callId =
        stepId ?? acpId ?? (deferAnonymous ? undefined : pending.fallbackId)
      if (!callId) continue
      pending.callId = callId
      pending.nativeIdentitySource = stepId
        ? 'step_metadata'
        : acpId && this.verifiedAcpToolIds?.has(acpId)
          ? 'acp_input'
          : undefined
      pending.id = `${this.sessionId}:tool:${callId}`
      const retired = this.retired.includes(pending)
      if (retired) this.retired = this.retired.filter((p) => p !== pending)
      if (pending.deferredCompletion) {
        this.pending = this.pending.filter((p) => p !== pending)
        const { output, status, text } = pending.deferredCompletion
        if (status === 'failed') this.lastErrorCallId = callId
        items.push(
          this.startTool(pending),
          this.finishTool(pending, output, status, text),
        )
      } else
        items.push(
          retired && !pending.genericSeen
            ? this.finishTool(pending, null, 'canceled', '')
            : this.startTool(pending),
        )
    }
    return items
  }
  project(value: unknown, continuingQuestion = false): ProviderTimelineItem[] {
    this.lastBackgroundGeneric = undefined
    const row = record(value)
    if (!Number.isInteger(row.step_index)) return []
    const stepIndex = Number(row.step_index)
    const id = `${this.sessionId}:step:${stepIndex}`
    const text = typeof row.content === 'string' ? row.content : ''
    switch (row.type) {
      case 'USER_INPUT':
        this.pending = []
        this.retired = []
        this.lastErrorCallId = undefined
        if (!continuingQuestion) this.touchedSubagents.clear()
        return text ? [{ type: 'user_message', id, text }] : []
      case 'SYSTEM_MESSAGE': {
        this.lastErrorCallId = undefined
        if (isRestartNotice(row)) {
          // Stopped calls never log a result; child transcripts have no USER_INPUT to reset them.
          this.pending = []
          this.retired = []
          this.background.clear()
          this.daemons.clear()
          this.touchedSubagents.clear()
          return []
        }
        const taskResults = [...text.matchAll(systemTaskResultPattern)]
        const finishedIds = new Set<string>()
        for (const match of taskResults) {
          finishedIds.add(match[1])
          finishedIds.add(match[2])
          this.daemons.delete(match[1])
          this.daemons.delete(match[2])
        }
        const senders = [...text.matchAll(messageHeaderSenderPattern)].map(
          (m) => m[1],
        )
        const canceledSchedules: ProviderTimelineItem[] = []
        for (const sender of senders) {
          const remaining = this.daemons.get(sender)
          if (remaining !== undefined && !finishedIds.has(sender)) {
            if (remaining <= 1) this.daemons.delete(sender)
            else this.daemons.set(sender, remaining - 1)
          }
          for (const [taskId, task] of this.background)
            if (
              isNativeToolName(task.name, 'schedule') &&
              !finishedIds.has(taskId) &&
              (sender === taskId ||
                task.args.TimerCondition === 'any' ||
                task.args.TimerCondition === sender)
            ) {
              this.background.delete(taskId)
              if (sender !== taskId) {
                canceledSchedules.push(
                  ...this.finishOrDefer(task, null, 'canceled', ''),
                )
              }
            }
        }
        if (taskResults.length > 0 || canceledSchedules.length > 0) {
          const completed: ProviderTimelineItem[] = [...canceledSchedules]
          for (const [, senderId, quotedId, outcome, output] of taskResults) {
            const key = this.background.has(quotedId) ? quotedId : senderId
            const pending = this.background.get(key)
            if (!pending) continue
            this.background.delete(key)
            const status =
              outcome === 'failed'
                ? 'failed'
                : outcome === 'was canceled'
                  ? 'canceled'
                  : 'completed'
            completed.push(
              ...this.finishOrDefer(pending, output, status, output),
            )
          }
          if (completed.length > 0 || taskResults.length > 0) return completed
        }
        if (stepIndex === 0 && /\bsender=[a-f0-9-]{36}(?=\s|$)/i.test(text)) {
          const promptMatch = text.match(
            /\bcontent=([\s\S]*?)(?:\s*<\/SYSTEM_MESSAGE>)?\s*$/,
          )
          const promptText = promptMatch?.[1]?.trim()
          return promptText
            ? [{ type: 'user_message', id, text: promptText }]
            : []
        }
        return []
      }
      case 'CHECKPOINT':
        shiftPendingPastCheckpoint(this.pending, stepIndex)
        return row.status === 'DONE'
          ? [
              {
                type: 'compaction',
                id: `${id}:compaction`,
                status: 'completed',
              },
            ]
          : []
      case 'ERROR_MESSAGE':
        if (isRetryWarning(row)) return []
        this.retired = []
        this.pending = []
        this.lastErrorCallId = undefined
        this.background.clear()
        this.daemons.clear()
        this.touchedSubagents.clear()
        return [
          {
            type: 'error',
            id: `${id}:error`,
            message: stepErrorMessage(row, 'Subagent error'),
          },
        ]
      case 'GENERIC': {
        const pending = takePendingForGeneric(this.pending, stepIndex)
        if (!pending || pending.questionCompleted) return []
        pending.stepIndex = stepIndex
        if (!pending.callId && this.deferAnonymous) {
          pending.genericSeen = true
          this.retired.push(pending)
        }
        return this.completeGeneric(pending, row)
      }
      case 'PLANNER_RESPONSE': {
        // A new model response means calls of the previous one without a GENERIC row were stopped;
        // keeping them would misattribute this response's results in FIFO order.
        this.retired.push(
          ...this.pending.filter((p) => !p.callId && !p.questionCompleted),
        )
        const items: ProviderTimelineItem[] = this.pending
          .filter((p) => p.callId && !p.questionCompleted)
          .map((p) => this.finishTool(p, null, 'canceled', ''))
        this.pending = []
        this.lastErrorCallId = undefined
        if (typeof row.thinking === 'string' && row.thinking)
          items.push({
            type: 'reasoning',
            id: `${id}:thought`,
            text: row.thinking,
          })
        if (text) items.push({ type: 'assistant_message', id, text })
        if (Array.isArray(row.tool_calls))
          row.tool_calls.forEach((value, index) => {
            const tool = record(value)
            const args = record(tool.args)
            const genericStep = stepIndex + index + 1
            const slotIndex = this.slotIndex++
            const acpIndex = this.toolIndex++
            const acpId = this.verifiedAcpId(acpIndex)
            const toolId =
              typeof tool.id === 'string' && tool.id ? tool.id : undefined
            const stepId = this.stepCallIds.get(
              this.effectiveStepIndex(slotIndex, genericStep),
            )
            const callId =
              toolId ??
              stepId ??
              acpId ??
              (this.deferAnonymous ? '' : `tool-${stepIndex}-${index}`)
            const nativeIdentitySource = toolId
              ? 'tool_id'
              : stepId
                ? 'step_metadata'
                : acpId && this.verifiedAcpToolIds?.has(acpId)
                  ? 'acp_input'
                  : undefined
            const cleanName = cleanToolName(String(tool.name ?? 'Tool'))
            const pending: PendingTool = {
              id: callId ? `${this.sessionId}:tool:${callId}` : '',
              callId,
              acpIndex,
              nativeIdentitySource,
              fallbackId: `tool-${stepIndex}-${index}`,
              name: cleanName,
              args,
              rawInput: tool.args,
              stepIndex: genericStep,
              slotIndex,
              ...(isAskQuestionName(cleanName)
                ? {
                    isQuestion: true,
                    questions: normalizeAskQuestionInput(args),
                  }
                : {}),
            }
            this.pending.push(pending)
            if (callId) items.push(this.startTool(pending))
          })
        return items
      }
      default:
        return []
    }
  }
}

export function activeChildIdsForTurn(
  subagents: readonly SubagentRecord[],
  projector: BrainTranscriptProjector,
  minStepIndex: number,
): string[] {
  const touched = projector.touchedSubagentIds()
  const ids = new Set<string>()
  for (const sub of subagents) {
    if (
      sub.state === SUBAGENT_ALIVE &&
      (sub.spawnStepIndex > minStepIndex || touched.has(sub.conversationId))
    ) {
      ids.add(sub.conversationId)
    }
  }
  return [...ids]
}

export type BrainPromptPhase =
  'foreground' | 'foreground_active' | 'foreground_idle' | 'woken'

export class BrainNode {
  readonly reader: TranscriptReader
  projector: BrainTranscriptProjector
  readonly children = new Map<string, BrainNode>()
  consumedMessages = 0
  turnStartStep = -1
  promptPhase: BrainPromptPhase = 'foreground'
  // Turn-wide projection ownership survives synthetic continuation resets of promptPhase.
  wokeUp = false
  continuingQuestion = false
  questionSettled = false
  abortedByDenial = false
  /** Last step seen when a question was answered; agy's reply must come after it. */
  answeredQuestionStep = -1
  lastStep?: Record<string, unknown>
  private retrySince?: number
  private retryError?: string
  private retryStep = -1
  readonly nativeId: string
  readonly sessionId: string
  private onQuestionSettled?: (
    questions: readonly NormalizedQuestion[],
    wokeUp: boolean,
    occurrence: string,
  ) => void

  constructor(
    nativeId: string,
    sessionId = nativeId,
    onQuestionSettled?: (
      questions: readonly NormalizedQuestion[],
      wokeUp: boolean,
      occurrence: string,
    ) => void,
    reader = new TranscriptReader(),
  ) {
    this.reader = reader
    this.nativeId = nativeId
    this.sessionId = sessionId
    this.onQuestionSettled = onQuestionSettled
    this.projector = this.createProjector()
  }

  private createProjector() {
    return new BrainTranscriptProjector(this.sessionId, (questions, tool) =>
      this.onQuestionSettled?.(questions, this.wokeUp, tool.callId),
    )
  }

  reset(inbox: InboxState, subagents: readonly SubagentRecord[]) {
    this.projector = this.createProjector()
    this.projector.syncMetadata(inbox, subagents)
    this.consumedMessages = 0
    this.turnStartStep = -1
    this.lastStep = undefined
    this.resetPhase()
  }

  get foregroundIdleReached(): boolean {
    return (
      this.promptPhase === 'foreground_idle' || this.promptPhase === 'woken'
    )
  }

  get wokeUpSinceLastPrompt(): boolean {
    return this.promptPhase === 'woken'
  }

  /** Forget the wakeup phase: a new USER_INPUT, a transcript rewrite or (for hasActiveBackgroundWork) a runtime restart. */
  resetPhase() {
    this.retrySince = undefined
    this.retryError = undefined
    this.retryStep = -1
    this.promptPhase = 'foreground'
    this.wokeUp = false
    this.continuingQuestion = false
    this.questionSettled = false
    this.abortedByDenial = false
    this.answeredQuestionStep = -1
  }

  settleAbortedDenial(denied?: ReadonlySet<string>) {
    if (
      this.promptPhase !== 'woken' &&
      denied &&
      this.projector.hasDeniedOutcome(denied, this.lastStep)
    ) {
      this.projector.clearPending()
      this.abortedByDenial = true
      this.promptPhase = 'foreground_idle'
    }
  }

  markQuestionContinuation(syntheticPrompt = false) {
    this.continuingQuestion = true
    this.questionSettled = false
    this.wokeUp = true
    if (syntheticPrompt) this.promptPhase = 'foreground'
    this.answeredQuestionStep = Number(this.lastStep?.step_index ?? -1)
  }

  markQuestionSettled() {
    this.questionSettled = true
  }

  async pollMetadata(root: string) {
    const [inbox, subagents] = await Promise.all([
      readInboxState(root, this.nativeId),
      readSubagents(root, this.nativeId),
    ])
    this.projector.syncMetadata(inbox, subagents)
    return { inbox, subagents }
  }

  /** Fold one transcript row into wakeup phase, message accounting, projection and the last step. */
  ingest(
    row: Record<string, unknown>,
    observedAt = Date.now(),
  ): ProviderTimelineItem[] {
    this.questionSettled = false
    this.abortedByDenial = false
    if (row.type === 'USER_INPUT' || isRestartNotice(row))
      this.turnStartStep = Number(row.step_index)
    // A restart notice or a question-continuation USER_INPUT does not end a woken turn:
    // the rows after it are still Brain's to project.
    const continuingQuestion =
      row.type === 'USER_INPUT' && this.continuingQuestion
    const rowMessages = countRowMessages(row)
    // shortcut: zero counts exclude user/low-priority headers; revisit if agy mixes headerless wakeups with handoffs.
    const purePlanExecuteNotice = isPlanExecuteNotice(row) && rowMessages === 0
    if (row.type === 'USER_INPUT') {
      if (this.continuingQuestion) {
        this.continuingQuestion = false
        this.promptPhase = 'foreground'
      } else this.resetPhase()
    } else if (
      row.type === 'SYSTEM_MESSAGE' &&
      !isRestartNotice(row) &&
      !purePlanExecuteNotice &&
      this.promptPhase === 'foreground_idle'
    ) {
      this.promptPhase = 'woken'
      this.wokeUp = true
    }
    if (isRetryWarning(row)) {
      const createdAt = Date.parse(String(row.created_at ?? ''))
      const step = Number(row.step_index)
      if (Number.isInteger(step) && step > this.retryStep) {
        this.retrySince = Number.isFinite(createdAt)
          ? Math.min(createdAt, observedAt)
          : observedAt
        this.retryStep = step
        this.retryError = stepErrorMessage(row)
      }
    } else if (
      row.type === 'ERROR_MESSAGE' ||
      isRestartNotice(row) ||
      (row.type === 'GENERIC' &&
        this.projector.hasPending() &&
        [
          'DONE',
          'RUNNING',
          'WAITING',
          'ERROR',
          'CANCELED',
          'INTERRUPTED',
        ].includes(String(row.status))) ||
      (row.type === 'PLANNER_RESPONSE' &&
        (isCompletePlannerResponse(row) ||
          (Array.isArray(row.tool_calls) && row.tool_calls.length > 0)))
    ) {
      this.retrySince = undefined
      this.retryError = undefined
    }
    this.consumedMessages += rowMessages
    const items = this.projector.project(row, continuingQuestion)
    if (row.type !== 'CHECKPOINT') this.lastStep = row
    if (this.promptPhase !== 'woken') {
      if (
        this.promptPhase === 'foreground' &&
        row.type === 'PLANNER_RESPONSE'
      ) {
        this.promptPhase = 'foreground_active'
      }
      if (purePlanExecuteNotice || this.projector.hasPending()) {
        if (this.promptPhase === 'foreground_idle')
          this.promptPhase = 'foreground_active'
      } else if (
        this.promptPhase !== 'foreground' &&
        (isCompletePlannerResponse(row) || this.projector.hasBackground())
      ) {
        this.promptPhase = 'foreground_idle'
      }
    }
    return items
  }

  retryPending(): boolean {
    return this.retrySince !== undefined
  }

  failure(timeoutMs = RETRY_IDLE_TIMEOUT_MS): string | undefined {
    if (
      this.retrySince !== undefined &&
      Date.now() - this.retrySince >= timeoutMs
    )
      return `${this.retryError} No retry recovery was observed for ${Math.round(timeoutMs / 1000)}s; stopped waiting locally.`
    if (
      this.lastStep?.type === 'ERROR_MESSAGE' &&
      !isRetryWarning(this.lastStep)
    )
      return stepErrorMessage(this.lastStep)
    return undefined
  }

  hasUnprocessedMessages(inbox: InboxState): boolean {
    return (
      inbox.unconsumedHighPriority.length > 0 ||
      this.consumedMessages < inbox.consumedNonUserCount
    )
  }

  isWokenWorking(): boolean {
    return (
      this.wokeUp &&
      !this.abortedByDenial &&
      (this.projector.hasPending() ||
        (this.lastStep?.type === 'SYSTEM_MESSAGE' &&
          !isRestartNotice(this.lastStep)) ||
        (this.lastStep?.type === 'GENERIC' &&
          this.lastStep.status !== 'CANCELED' &&
          this.lastStep.status !== 'INTERRUPTED'))
    )
  }

  isSettled(inbox: InboxState): boolean {
    return (
      !this.retryPending() &&
      !this.hasUnprocessedMessages(inbox) &&
      !this.projector.hasPending() &&
      !this.projector.hasBackground() &&
      // An answered question's own ask-only response is not agy's reply to the answer.
      Number(this.lastStep?.step_index ?? -1) > this.answeredQuestionStep &&
      (isCompletePlannerResponse(this.lastStep) ||
        this.questionSettled ||
        (this.promptPhase !== 'woken' && this.abortedByDenial))
    )
  }
}
