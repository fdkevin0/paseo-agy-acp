import {
  cleanToolName,
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
  isRetryWarning,
  RETRY_IDLE_TIMEOUT_MS,
  isRestartNotice,
  readInboxState,
  readSubagents,
  stepErrorMessage,
  type InboxState,
  type SubagentRecord,
} from './brain-io.ts'

export type PendingTool = {
  id: string
  callId: string
  name: string
  args: Record<string, unknown>
  stepIndex: number
  isQuestion?: boolean
  questions?: NormalizedQuestion[]
  questionConsumed?: boolean
  questionCompleted?: boolean
}

export type PendingQuestion = {
  tool: PendingTool
  questions: NormalizedQuestion[]
  wokeUp: boolean
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

/** Project transcript_full.jsonl steps and correlate tools with structured Brain metadata. */
export class BrainTranscriptProjector {
  private pending: PendingTool[] = []
  private background = new Map<string, PendingTool>()
  private daemons = new Map<string, number>()
  private touchedSubagents = new Set<string>()
  private stepCallIds = new Map<number, string>()
  private stepSubagents = new Map<number, SubagentRecord>()
  private callSubagents = new Map<string, SubagentRecord>()
  private toolIndex = 0
  private acpToolCallIds: readonly string[] = []
  private lastErrorCallId?: string
  private sessionId: string
  private onQuestionSettledByGeneric?: (
    questions: readonly NormalizedQuestion[],
  ) => void

  constructor(
    sessionId: string,
    onQuestionSettledByGeneric?: (
      questions: readonly NormalizedQuestion[],
    ) => void,
  ) {
    this.sessionId = sessionId
    this.onQuestionSettledByGeneric = onQuestionSettledByGeneric
  }

  bindToolCallIds(ids: readonly string[]) {
    this.acpToolCallIds = ids
    this.toolIndex = 0
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
    return this.pending.some((p) => !p.isQuestion)
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
        p.name.includes('send_message') &&
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
        rawInput: pending.args,
        rawOutput: output,
      }),
    }
    if (
      status === 'completed' &&
      base.detail.type === 'sub_agent' &&
      base.detail.childSessionId
    ) {
      this.touchedSubagents.add(base.detail.childSessionId)
    }
    return status === 'failed'
      ? {
          ...base,
          status: 'failed',
          error: { message: text || 'Tool execution failed' },
        }
      : { ...base, status, error: null }
  }

  project(value: unknown): ProviderTimelineItem[] {
    const row = record(value)
    if (!Number.isInteger(row.step_index)) return []
    const stepIndex = Number(row.step_index)
    const id = `${this.sessionId}:step:${stepIndex}`
    const text = typeof row.content === 'string' ? row.content : ''
    switch (row.type) {
      case 'USER_INPUT':
        this.pending = []
        this.lastErrorCallId = undefined
        this.touchedSubagents.clear()
        return text ? [{ type: 'user_message', id, text }] : []
      case 'SYSTEM_MESSAGE': {
        this.lastErrorCallId = undefined
        if (isRestartNotice(row)) {
          // Stopped calls never log a result; child transcripts have no USER_INPUT to reset them.
          this.pending = []
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
              task.name === 'schedule' &&
              !finishedIds.has(taskId) &&
              (sender === taskId ||
                task.args.TimerCondition === 'any' ||
                task.args.TimerCondition === sender)
            ) {
              this.background.delete(taskId)
              if (sender !== taskId) {
                canceledSchedules.push(
                  this.finishTool(task, null, 'canceled', ''),
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
            completed.push(this.finishTool(pending, output, status, output))
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
        // A checkpoint takes a step index between a response and its GENERIC rows; shift expected rows past it.
        for (const p of this.pending) {
          if (p.stepIndex >= stepIndex) p.stepIndex++
        }
        return []
      case 'ERROR_MESSAGE':
        if (isRetryWarning(row)) return []
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
        // Match by stepIndex first so an unlogged WAITING ask_question never steals a sibling tool's GENERIC row.
        let matchIndex = this.pending.findIndex(
          (p) => p.stepIndex === stepIndex,
        )
        if (matchIndex < 0) {
          matchIndex = this.pending.findIndex((p) => !p.isQuestion)
          if (matchIndex < 0) matchIndex = 0
        }
        const [pending] = this.pending.splice(matchIndex, 1)
        if (!pending || pending.questionCompleted) return []
        if (runningStepStatuses.has(String(row.status))) {
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
          this.onQuestionSettledByGeneric?.(pending.questions)
        }
        if (
          pending.name.includes('manage_task') &&
          pending.args.Action === 'kill' &&
          typeof pending.args.TaskId === 'string' &&
          terminalToolStatus(row.status) === 'completed'
        ) {
          this.forgetTask(pending.args.TaskId)
        }
        const stepCallId = this.stepCallIds.get(stepIndex)
        const subagent =
          this.callSubagents.get(pending.callId) ??
          (stepCallId ? this.callSubagents.get(stepCallId) : undefined) ??
          this.stepSubagents.get(stepIndex) ??
          this.stepSubagents.get(pending.stepIndex)
        const rawOutput =
          pending.name === 'invoke_subagent' &&
          subagent &&
          (!text || !text.includes(subagent.conversationId))
            ? { conversationId: subagent.conversationId }
            : (row.content ?? row.error ?? null)
        const status = terminalToolStatus(row.status)
        this.lastErrorCallId = status === 'failed' ? pending.callId : undefined
        return [this.finishTool(pending, rawOutput, status, text)]
      }
      case 'PLANNER_RESPONSE': {
        // A new model response means calls of the previous one without a GENERIC row were stopped;
        // keeping them would misattribute this response's results in FIFO order.
        const items: ProviderTimelineItem[] = this.pending
          .filter((p) => !p.questionCompleted)
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
            const acpId = this.acpToolCallIds[this.toolIndex++]
            const callId =
              (typeof tool.id === 'string' && tool.id ? tool.id : undefined) ??
              this.stepCallIds.get(genericStep) ??
              acpId ??
              `tool-${stepIndex}-${index}`
            const cleanName = cleanToolName(String(tool.name ?? 'Tool'))
            const pending: PendingTool = {
              id: `${this.sessionId}:tool:${callId}`,
              callId,
              name: cleanName,
              args,
              stepIndex: genericStep,
              ...(isAskQuestionName(cleanName)
                ? {
                    isQuestion: true,
                    questions: normalizeAskQuestionInput(args),
                  }
                : {}),
            }
            this.pending.push(pending)
            items.push({
              type: 'tool_call',
              id: pending.id,
              callId: pending.callId,
              name: pending.name,
              detail: toolDetail({
                title: pending.name,
                rawInput: args,
                rawOutput: null,
              }),
              status: 'running',
              error: null,
            })
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

export class BrainNode {
  readonly reader = new TranscriptReader()
  projector: BrainTranscriptProjector
  readonly children = new Map<string, BrainNode>()
  consumedMessages = 0
  turnStartStep = -1
  seenPlannerResponse = false
  foregroundIdleReached = false
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
  ) => void

  constructor(
    nativeId: string,
    sessionId = nativeId,
    onQuestionSettled?: (
      questions: readonly NormalizedQuestion[],
      wokeUp: boolean,
    ) => void,
  ) {
    this.nativeId = nativeId
    this.sessionId = sessionId
    this.onQuestionSettled = onQuestionSettled
    this.projector = this.createProjector()
  }

  private createProjector() {
    return new BrainTranscriptProjector(this.sessionId, (questions) =>
      this.onQuestionSettled?.(questions, this.wokeUp),
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

  /** Forget the wakeup phase: a new USER_INPUT, a transcript rewrite or (for hasActiveBackgroundWork) a runtime restart. */
  resetPhase() {
    this.retrySince = undefined
    this.retryError = undefined
    this.retryStep = -1
    this.seenPlannerResponse = false
    this.foregroundIdleReached = false
    this.wokeUp = false
    this.continuingQuestion = false
    this.questionSettled = false
    this.abortedByDenial = false
    this.answeredQuestionStep = -1
  }

  settleAbortedDenial(denied?: ReadonlySet<string>) {
    if (
      !this.wokeUp &&
      denied &&
      this.projector.hasDeniedOutcome(denied, this.lastStep)
    ) {
      this.projector.clearPending()
      this.abortedByDenial = true
      this.foregroundIdleReached = true
    }
  }

  markQuestionContinuation() {
    this.continuingQuestion = true
    this.questionSettled = false
    this.wokeUp = true
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
    if (row.type === 'USER_INPUT') {
      if (this.continuingQuestion) this.continuingQuestion = false
      else this.resetPhase()
    } else if (
      row.type === 'SYSTEM_MESSAGE' &&
      !isRestartNotice(row) &&
      this.foregroundIdleReached
    )
      this.wokeUp = true
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
    this.consumedMessages += countRowMessages(row)
    const items = this.projector.project(row)
    if (row.type !== 'CHECKPOINT') this.lastStep = row
    if (row.type === 'PLANNER_RESPONSE') this.seenPlannerResponse = true
    if (
      this.seenPlannerResponse &&
      !this.projector.hasPending() &&
      (isCompletePlannerResponse(row) || this.projector.hasBackground())
    )
      this.foregroundIdleReached = true
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
        (!this.wokeUp && this.abortedByDenial))
    )
  }
}
