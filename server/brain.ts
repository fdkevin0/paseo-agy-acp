import { open, readdir, readFile, realpath } from 'node:fs/promises'
import { join, sep } from 'node:path'
import {
  Timeline,
  record,
  toolDetail,
  sessionIdPattern,
  type StepUsage,
} from './mapping.ts'
import type {
  ProviderEvent,
  ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'

type PendingTool = {
  id: string
  callId: string
  name: string
  args: Record<string, unknown>
  stepIndex: number
}

export type SubagentRecord = {
  conversationId: string
  state: 'SUBAGENT_STATE_ALIVE' | 'SUBAGENT_STATE_KILLED'
  spawnStepIndex: number
  spawnCallId?: string
}

export type BrainMessageRecord = {
  id: string
  sender: string
  priority?: string
  hideFromUser?: boolean
  tool?: {
    conversationId?: string
    stepIndex?: number
    callId?: string
    name?: string
  }
}

export type InboxState = {
  messages: BrainMessageRecord[]
  readIds: Set<string>
  unconsumedHighPriority: BrainMessageRecord[]
  consumedNonUserCount: number
  taskCallIds: Map<string, string>
  stepCallIds: Map<number, string>
}

const backgroundTaskPattern =
  /Tool is running as a background task with task id:\s*(\S+)/
const systemTaskResultPattern =
  /\bsender=(\S+)[^\n]*\scontent=Task id "([^"]+)" (finished|was canceled|failed) with result:\n([\s\S]*?)(?:\nLog: file:\/\/\S+)?(?=\n(?:\d+\.\s+)?\[Message\]\s+|\s*<\/SYSTEM_MESSAGE>|\s*$)/g
const restartNoticePattern =
  /\bsender=system\b[^\n]*\scontent=\[Notice\] All your subagents and background tasks have been stopped/

function isCompletePlannerResponse(step: Record<string, unknown> | undefined) {
  return (
    step?.type === 'PLANNER_RESPONSE' &&
    step.status === 'DONE' &&
    (!Array.isArray(step.tool_calls) || step.tool_calls.length === 0)
  )
}

function stepErrorMessage(
  step: Record<string, unknown> | undefined,
  fallback = 'Turn failed',
) {
  if (typeof step?.content === 'string' && step.content) return step.content
  if (typeof step?.error === 'string' && step.error) return step.error
  const errObj = record(step?.error)
  if (typeof errObj.message === 'string' && errObj.message)
    return errObj.message
  return fallback
}

function extractStepUsage(row: Record<string, unknown>): StepUsage | undefined {
  if (row.type !== 'PLANNER_RESPONSE') return undefined
  const input =
    typeof row.input_tokens === 'number' && Number.isFinite(row.input_tokens)
      ? row.input_tokens
      : undefined
  const output =
    typeof row.output_tokens === 'number' && Number.isFinite(row.output_tokens)
      ? row.output_tokens
      : undefined
  const cacheRead =
    typeof row.cache_read_tokens === 'number' &&
    Number.isFinite(row.cache_read_tokens)
      ? row.cache_read_tokens
      : 0
  if (input === undefined && output === undefined) return undefined
  return {
    inputTokens: input ?? 0,
    outputTokens: output ?? 0,
    cachedReadTokens: cacheRead,
  }
}

function countRowMessages(row: Record<string, unknown>): number {
  if (row.type !== 'SYSTEM_MESSAGE' || typeof row.content !== 'string') return 0
  if (
    Number(row.step_index) === 0 &&
    /\bsender=[a-f0-9-]{36}(?=\s|$)/i.test(row.content)
  )
    return 0
  return (
    row.content.match(
      /(?:^|\n)(?:\d+\.\s+)?\[Message\]\s+(?:timestamp=\S+\s+)?sender=(?!user\b)\S+/g,
    )?.length ?? 0
  )
}

async function resolveBrainPath(
  root: string,
  nativeId: string,
  subpath: string,
) {
  const base = await realpath(root)
  const path = await realpath(
    join(base, nativeId, '.system_generated', subpath),
  )
  if (!path.startsWith(base + sep))
    throw new Error('Brain artifact escapes its directory')
  return path
}

/** Read structured inbox messages and read.json state from .system_generated/messages. */
export async function readInboxState(
  root: string,
  nativeId: string,
): Promise<InboxState> {
  const empty: InboxState = {
    messages: [],
    readIds: new Set(),
    unconsumedHighPriority: [],
    consumedNonUserCount: 0,
    taskCallIds: new Map(),
    stepCallIds: new Map(),
  }
  if (!sessionIdPattern.test(nativeId)) return empty
  let dir: string
  try {
    dir = await resolveBrainPath(root, nativeId, 'messages')
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return empty
    throw error
  }
  const entries = await readdir(dir).catch(() => [] as string[])
  const readIds = new Set<string>()
  if (entries.includes('read.json')) {
    try {
      const parsed = record(
        JSON.parse(await readFile(join(dir, 'read.json'), 'utf8')),
      )
      for (const [id, isRead] of Object.entries(parsed))
        if (isRead === true) readIds.add(id)
    } catch {
      /* Ignore partially written read.json and re-poll on next tick. */
    }
  }
  const messages: BrainMessageRecord[] = []
  const taskCallIds = new Map<string, string>()
  const stepCallIds = new Map<number, string>()
  for (const name of entries) {
    if (!name.endsWith('.json') || name === 'read.json') continue
    try {
      const raw = record(JSON.parse(await readFile(join(dir, name), 'utf8')))
      const id =
        typeof raw.id === 'string' && raw.id ? raw.id : name.slice(0, -5)
      const sender = typeof raw.sender === 'string' ? raw.sender : ''
      const toolMeta = record(record(raw.sourceMetadata).tool)
      const toolCall = record(toolMeta.toolCall)
      const callId =
        typeof toolCall.id === 'string' && toolCall.id ? toolCall.id : undefined
      const stepIndex =
        typeof toolMeta.stepIndex === 'number' &&
        Number.isInteger(toolMeta.stepIndex)
          ? toolMeta.stepIndex
          : undefined
      const convId =
        typeof toolMeta.conversationId === 'string'
          ? toolMeta.conversationId
          : undefined
      if (convId === nativeId && callId) {
        if (sender) taskCallIds.set(sender, callId)
        if (stepIndex !== undefined) stepCallIds.set(stepIndex, callId)
      }
      messages.push({
        id,
        sender,
        priority: typeof raw.priority === 'string' ? raw.priority : undefined,
        hideFromUser: raw.hideFromUser === true,
        ...(convId || stepIndex !== undefined || callId
          ? {
              tool: {
                conversationId: convId,
                stepIndex,
                callId,
                name:
                  typeof toolCall.name === 'string' ? toolCall.name : undefined,
              },
            }
          : {}),
      })
    } catch {
      /* Ignore partially written message files and re-poll on next tick. */
    }
  }
  const isWakeupMessage = (m: BrainMessageRecord) =>
    m.sender !== 'user' && m.tool?.name !== 'invoke_subagent'
  const unconsumedHighPriority = messages.filter(
    (m) =>
      isWakeupMessage(m) &&
      !m.hideFromUser &&
      m.priority !== 'MESSAGE_PRIORITY_LOW' &&
      !readIds.has(m.id),
  )
  const consumedNonUserCount = messages.filter(
    (m) => isWakeupMessage(m) && readIds.has(m.id),
  ).length
  return {
    messages,
    readIds,
    unconsumedHighPriority,
    consumedNonUserCount,
    taskCallIds,
    stepCallIds,
  }
}

/** Read structured subagent descriptors from .system_generated/subagents. */
export async function readSubagents(
  root: string,
  nativeId: string,
): Promise<SubagentRecord[]> {
  if (!sessionIdPattern.test(nativeId)) return []
  let dir: string
  try {
    dir = await resolveBrainPath(root, nativeId, 'subagents')
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return []
    throw error
  }
  const entries = await readdir(dir).catch(() => [] as string[])
  const loaded = await Promise.all(
    entries
      .filter((name) => name.endsWith('.json'))
      .map(async (name): Promise<SubagentRecord | undefined> => {
        try {
          const raw = record(
            JSON.parse(await readFile(join(dir, name), 'utf8')),
          )
          const conversationId =
            typeof raw.conversationId === 'string'
              ? raw.conversationId
              : name.slice(0, -5)
          if (!sessionIdPattern.test(conversationId)) return undefined
          const state =
            raw.state === 'SUBAGENT_STATE_KILLED'
              ? 'SUBAGENT_STATE_KILLED'
              : 'SUBAGENT_STATE_ALIVE'
          let spawnStepIndex =
            typeof raw.spawnStepIndex === 'number' &&
            Number.isInteger(raw.spawnStepIndex)
              ? raw.spawnStepIndex
              : 0
          let spawnCallId: string | undefined
          const childInbox = await readInboxState(root, conversationId)
          const initMessage = childInbox.messages.find(
            (m) =>
              m.tool?.conversationId === nativeId &&
              m.tool.name === 'invoke_subagent',
          )
          if (initMessage?.tool?.callId) spawnCallId = initMessage.tool.callId
          if (initMessage?.tool?.stepIndex !== undefined)
            spawnStepIndex = initMessage.tool.stepIndex
          return {
            conversationId,
            state,
            spawnStepIndex,
            spawnCallId,
          }
        } catch {
          /* Ignore partially written subagent descriptor files. */
          return undefined
        }
      }),
  )
  return loaded.filter((r): r is SubagentRecord => r !== undefined)
}

/** Project transcript_full.jsonl steps and correlate tools with structured Brain metadata. */
export class BrainTranscriptProjector {
  private pending: PendingTool[] = []
  private background = new Map<string, PendingTool>()
  private touchedSubagents = new Set<string>()
  private stepCallIds = new Map<number, string>()
  private stepSubagents = new Map<number, SubagentRecord>()
  private callSubagents = new Map<string, SubagentRecord>()
  private toolIndex = 0
  private acpToolCallIds: readonly string[] = []
  private sessionId: string

  constructor(sessionId: string) {
    this.sessionId = sessionId
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
    return this.pending.length > 0
  }

  hasBackground(): boolean {
    return this.background.size > 0
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
        this.touchedSubagents.clear()
        return text ? [{ type: 'user_message', id, text }] : []
      case 'SYSTEM_MESSAGE': {
        if (restartNoticePattern.test(text)) {
          this.background.clear()
          this.touchedSubagents.clear()
          return []
        }
        const taskResults = [...text.matchAll(systemTaskResultPattern)]
        const finishedIds = new Set<string>()
        for (const match of taskResults) {
          finishedIds.add(match[1])
          finishedIds.add(match[2])
        }
        const senders = [...text.matchAll(/\bsender=(\S+)/g)].map((m) => m[1])
        for (const sender of senders) {
          for (const [taskId, task] of this.background)
            if (
              task.name === 'schedule' &&
              !finishedIds.has(taskId) &&
              (sender === taskId ||
                task.args.TimerCondition === 'any' ||
                task.args.TimerCondition === sender)
            )
              this.background.delete(taskId)
        }
        if (taskResults.length > 0) {
          const completed: ProviderTimelineItem[] = []
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
          return completed
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
        return []
      case 'ERROR_MESSAGE':
        this.pending = []
        this.background.clear()
        this.touchedSubagents.clear()
        return [
          {
            type: 'error',
            id: `${id}:error`,
            message: stepErrorMessage(row, 'Subagent error'),
          },
        ]
      case 'GENERIC': {
        const pending = this.pending.shift()
        if (!pending) return []
        const subagent =
          this.callSubagents.get(pending.callId) ??
          this.stepSubagents.get(stepIndex) ??
          this.stepSubagents.get(pending.stepIndex)
        if (row.status === 'RUNNING') {
          const taskId = text.match(backgroundTaskPattern)?.[1]
          if (taskId && pending.args.IsDaemon !== true)
            this.background.set(taskId, pending)
          return []
        }
        const rawOutput =
          pending.name === 'invoke_subagent' &&
          subagent &&
          (!text || !text.includes(subagent.conversationId))
            ? { conversationId: subagent.conversationId }
            : (row.content ?? row.error ?? null)
        return [
          this.finishTool(
            pending,
            rawOutput,
            row.status === 'ERROR' ? 'failed' : 'completed',
            text,
          ),
        ]
      }
      case 'PLANNER_RESPONSE': {
        const items: ProviderTimelineItem[] = []
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
            const pending: PendingTool = {
              id: `${this.sessionId}:tool:${callId}`,
              callId,
              name: String(tool.name ?? 'Tool'),
              args,
              stepIndex: genericStep,
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

async function resolveTranscriptPath(root: string, nativeId: string) {
  return resolveBrainPath(root, nativeId, 'logs/transcript_full.jsonl')
}

/** One cursor owns partial records across polls, path replacement and truncation. */
class TranscriptReader {
  private identity = ''
  private offset = 0
  private remainder = ''

  async read(root: string, nativeId: string) {
    const path = await resolveTranscriptPath(root, nativeId)
    const file = await open(path, 'r')
    const rows: Record<string, unknown>[] = []
    try {
      const stat = await file.stat()
      const identity = `${path}:${stat.dev}:${stat.ino}`
      const reset = identity !== this.identity || stat.size < this.offset
      if (reset) {
        this.identity = identity
        this.offset = 0
        this.remainder = ''
      }
      while (this.offset < stat.size) {
        const buffer = Buffer.alloc(
          Math.min(stat.size - this.offset, 1024 * 1024),
        )
        const { bytesRead } = await file.read(
          buffer,
          0,
          buffer.length,
          this.offset,
        )
        if (!bytesRead) break
        this.offset += bytesRead
        // Preserve bytes until a complete line is available, including split UTF-8 characters.
        this.remainder += buffer.subarray(0, bytesRead).toString('latin1')
        const lines = this.remainder.split('\n')
        this.remainder = lines.pop()!
        if (this.remainder.length > 8 * 1024 * 1024)
          throw new Error('Brain JSONL line exceeds 8 MiB')
        for (const line of lines) {
          if (line.length > 8 * 1024 * 1024)
            throw new Error('Brain JSONL line exceeds 8 MiB')
          if (line.trim())
            rows.push(
              record(JSON.parse(Buffer.from(line, 'latin1').toString('utf8'))),
            )
        }
      }
      return { rows, reset }
    } finally {
      await file.close()
    }
  }
}

export async function readTranscriptRows(
  root: string,
  nativeId: string,
): Promise<Record<string, unknown>[]> {
  if (!sessionIdPattern.test(nativeId)) return []
  try {
    return (await new TranscriptReader().read(root, nativeId)).rows
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return []
    throw error
  }
}

export async function readLastStepIndex(
  root: string,
  nativeId: string,
): Promise<number> {
  const rows = await readTranscriptRows(root, nativeId)
  let max = -1
  for (const row of rows) {
    const step = Number(row.step_index)
    if (Number.isInteger(step) && step > max) max = step
  }
  return max
}

function activeChildIdsForTurn(
  subagents: readonly SubagentRecord[],
  projector: BrainTranscriptProjector,
  minStepIndex: number,
): string[] {
  const touched = projector.touchedSubagentIds()
  const ids = new Set<string>()
  for (const sub of subagents) {
    if (
      sub.state === 'SUBAGENT_STATE_ALIVE' &&
      (sub.spawnStepIndex > minStepIndex || touched.has(sub.conversationId))
    ) {
      ids.add(sub.conversationId)
    }
  }
  return [...ids]
}

class BrainNode {
  readonly reader = new TranscriptReader()
  projector: BrainTranscriptProjector
  readonly children = new Map<string, BrainNode>()
  consumedMessages = 0
  lastStep?: Record<string, unknown>
  readonly nativeId: string
  readonly sessionId: string

  constructor(nativeId: string, sessionId = nativeId) {
    this.nativeId = nativeId
    this.sessionId = sessionId
    this.projector = new BrainTranscriptProjector(sessionId)
  }

  reset(inbox: InboxState, subagents: readonly SubagentRecord[]) {
    this.projector = new BrainTranscriptProjector(this.sessionId)
    this.projector.syncMetadata(inbox, subagents)
    this.consumedMessages = 0
    this.lastStep = undefined
  }

  async pollMetadata(root: string) {
    const [inbox, subagents] = await Promise.all([
      readInboxState(root, this.nativeId),
      readSubagents(root, this.nativeId),
    ])
    this.projector.syncMetadata(inbox, subagents)
    return { inbox, subagents }
  }

  hasUnprocessedMessages(inbox: InboxState): boolean {
    return (
      inbox.unconsumedHighPriority.length > 0 ||
      this.consumedMessages < inbox.consumedNonUserCount
    )
  }

  isSettled(inbox: InboxState): boolean {
    return (
      !this.hasUnprocessedMessages(inbox) &&
      !this.projector.hasPending() &&
      !this.projector.hasBackground() &&
      isCompletePlannerResponse(this.lastStep)
    )
  }
}

/** Determine whether the session has running background tasks or active subagents that require process termination on interrupt. */
export async function hasActiveBackgroundWork(
  root: string,
  nativeId: string,
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
    if (inbox.unconsumedHighPriority.length > 0) return true
    if (isChild && rows.length === 0) return true
    let minTurnStepIndex = -1
    for (const row of rows) {
      node.consumedMessages += countRowMessages(row)
      node.projector.project(row)
      if (row.type !== 'CHECKPOINT') node.lastStep = row
      if (
        !isChild &&
        (row.type === 'USER_INPUT' ||
          (row.type === 'SYSTEM_MESSAGE' &&
            restartNoticePattern.test(String(row.content ?? ''))))
      ) {
        minTurnStepIndex = Number(row.step_index)
      }
    }
    if (node.projector.hasBackground()) return true
    if (isChild) {
      if (node.consumedMessages < inbox.consumedNonUserCount) return true
      if (
        node.projector.hasPending() ||
        (!isCompletePlannerResponse(node.lastStep) &&
          node.lastStep?.type !== 'ERROR_MESSAGE')
      )
        return true
    }
    const activeChildIds = activeChildIdsForTurn(
      subagents,
      node.projector,
      minTurnStepIndex,
    )
    for (const childId of activeChildIds) {
      if (await hasActiveBackgroundWork(root, childId, visiting, true))
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
): Promise<{ settled: boolean; error?: string }> {
  if (visiting.has(parentNode.nativeId)) return { settled: true }
  visiting.add(parentNode.nativeId)
  try {
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
        for (const row of rows) {
          child.consumedMessages += countRowMessages(row)
          child.projector.project(row)
          if (row.type !== 'CHECKPOINT') child.lastStep = row
        }
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT')
          return { settled: false }
        throw error
      }
      if (child.hasUnprocessedMessages(childInbox)) return { settled: false }
      if (child.lastStep?.type === 'ERROR_MESSAGE')
        return {
          settled: false,
          error: `Subagent ${childId} failed: ${stepErrorMessage(child.lastStep)}`,
        }
      if (!child.isSettled(childInbox)) return { settled: false }
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
      )
      if (nested.error) return nested
      if (!nested.settled) return { settled: false }
      const hasUnconsumedReport = parentInbox.messages.some(
        (m) =>
          m.sender === childId &&
          !m.hideFromUser &&
          m.priority !== 'MESSAGE_PRIORITY_LOW' &&
          !parentInbox.readIds.has(m.id),
      )
      if (
        hasUnconsumedReport ||
        parentNode.consumedMessages < parentInbox.consumedNonUserCount
      )
        return { settled: false }
    }
    return { settled: true }
  } finally {
    visiting.delete(parentNode.nativeId)
  }
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
    foregroundMaxStepIndex,
    pollIntervalMs = 200,
    acpToolCallIds = [],
    shouldEmitPreWakeupTool,
  }: {
    signal?: AbortSignal
    minStepIndex?: number
    foregroundMaxStepIndex?: number
    pollIntervalMs?: number
    acpToolCallIds?: readonly string[]
    shouldEmitPreWakeupTool?: (
      item: Extract<ProviderTimelineItem, { type: 'tool_call' }>,
    ) => boolean
  } = {},
): Promise<{
  status: 'completed' | 'canceled' | 'failed'
  wokeUp: boolean
  error?: string
  lastStepUsage?: StepUsage
}> {
  if (!sessionIdPattern.test(nativeId))
    return { status: 'completed', wokeUp: false }
  const node = new BrainNode(nativeId, sessionId)
  let initial = true
  let seenPlannerResponse = false
  let foregroundIdleReached = false
  let wokeUp = false
  let emittedStep = -1
  let lastStepUsage: StepUsage | undefined
  while (true) {
    if (signal?.aborted) {
      if (signal.reason instanceof Error && signal.reason.name !== 'AbortError')
        throw signal.reason
      return { status: 'canceled', wokeUp, lastStepUsage }
    }
    const { inbox, subagents } = await node.pollMetadata(root)
    let rows: Record<string, unknown>[]
    try {
      const result = await node.reader.read(root, nativeId)
      rows = result.rows
      if (result.reset) {
        initial = true
        seenPlannerResponse = false
        foregroundIdleReached = false
        wokeUp = false
        node.reset(inbox, subagents)
      }
    } catch (error) {
      if (
        (error as NodeJS.ErrnoException).code === 'ENOENT' &&
        !wokeUp &&
        inbox.unconsumedHighPriority.length === 0 &&
        subagents.length === 0
      )
        return { status: 'completed', wokeUp: false }
      throw error
    }
    let slice = rows
    if (initial) {
      let start = -1
      for (let i = rows.length - 1; i >= 0; i--) {
        if (
          rows[i].type === 'USER_INPUT' &&
          Number(rows[i].step_index) > minStepIndex
        ) {
          start = i
          break
        }
      }
      if (start < 0) return { status: 'completed', wokeUp: false }
      initial = false
      for (const row of rows.slice(0, start))
        node.consumedMessages += countRowMessages(row)
      node.projector.bindToolCallIds(acpToolCallIds)
      slice = rows.slice(start)
    }
    for (const row of slice) {
      const stepIndex = Number(row.step_index)
      node.consumedMessages += countRowMessages(row)
      if (
        row.type === 'SYSTEM_MESSAGE' &&
        !restartNoticePattern.test(String(row.content ?? '')) &&
        foregroundIdleReached &&
        (foregroundMaxStepIndex === undefined ||
          stepIndex > foregroundMaxStepIndex)
      )
        wokeUp = true
      const items = node.projector.project(row)
      const usage = extractStepUsage(row)
      if (usage) lastStepUsage = usage
      if (stepIndex > emittedStep) {
        emittedStep = stepIndex
        const timestamp =
          typeof row.created_at === 'string' ? row.created_at : undefined
        for (const item of items) {
          if (wokeUp || item.type === 'error') {
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
      if (row.type !== 'CHECKPOINT') node.lastStep = row
      if (row.type === 'PLANNER_RESPONSE') seenPlannerResponse = true
      if (
        seenPlannerResponse &&
        !node.projector.hasPending() &&
        (isCompletePlannerResponse(row) || row.type === 'GENERIC')
      )
        foregroundIdleReached = true
    }
    if (node.lastStep?.type === 'ERROR_MESSAGE') {
      return {
        status: 'failed',
        wokeUp,
        error: stepErrorMessage(node.lastStep, 'Turn failed'),
        lastStepUsage,
      }
    }
    const activeChildIds = activeChildIdsForTurn(
      subagents,
      node.projector,
      minStepIndex,
    )
    const childStatus = await settleChildren(root, node, inbox, activeChildIds)
    if (childStatus.error)
      return {
        status: 'failed',
        wokeUp,
        error: childStatus.error,
        lastStepUsage,
      }
    if (childStatus.settled && node.isSettled(inbox)) {
      if (activeChildIds.length > 0) {
        const latestInbox = await readInboxState(root, nativeId)
        if (
          latestInbox.unconsumedHighPriority.length > 0 ||
          node.consumedMessages < latestInbox.consumedNonUserCount
        )
          continue
      }
      return { status: 'completed', wokeUp, lastStepUsage }
    }
    await new Promise<void>((resolve) => {
      const onAbort = () => {
        clearTimeout(timer)
        resolve()
      }
      const timer = setTimeout(() => {
        signal?.removeEventListener('abort', onAbort)
        resolve()
      }, pollIntervalMs)
      timer.unref()
      signal?.addEventListener('abort', onAbort, { once: true })
    })
  }
}

export type BrainWatchHandle = (() => void) & {
  cancel: () => void
  isRunning: () => boolean
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
): BrainWatchHandle {
  if (!sessionIdPattern.test(nativeId))
    throw new Error('Invalid Brain session ID')
  const timeline = new Timeline(sessionId)
  const node = new BrainNode(nativeId, sessionId)
  let stopped = false
  let busy = false
  let warned = false
  let running = true
  let turnSeq = 1
  let turnId = `${sessionId}:turn:${turnSeq}`
  function finishTurn(
    state: 'completed' | 'failed' | 'canceled',
    error?: string,
  ) {
    if (!running || stopped) return
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
  async function poll() {
    if (stopped || busy) return
    busy = true
    try {
      const { inbox, subagents } = await node.pollMetadata(root)
      const { rows, reset } = await node.reader.read(root, nativeId)
      if (reset) node.reset(inbox, subagents)
      for (const row of rows) {
        if (stopped) return
        node.consumedMessages += countRowMessages(row)
        const isSystemNotice =
          row.type === 'SYSTEM_MESSAGE' &&
          restartNoticePattern.test(String(row.content ?? ''))
        if (
          !running &&
          !isSystemNotice &&
          (row.type === 'USER_INPUT' ||
            row.type === 'SYSTEM_MESSAGE' ||
            row.type === 'PLANNER_RESPONSE')
        ) {
          running = true
          turnId = `${sessionId}:turn:${++turnSeq}`
          emit({
            type: 'session.turn',
            sessionId,
            turnId,
            state: 'started',
          })
        }
        const timestamp =
          typeof row.created_at === 'string' ? row.created_at : undefined
        for (const item of node.projector.project(row)) {
          timeline.track(item)
          emit({
            type: 'timeline.item',
            sessionId,
            item,
            timestamp,
          })
        }
        if (row.type !== 'CHECKPOINT') node.lastStep = row
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
      )
      if (childStatus.error) finishTurn('failed', childStatus.error)
      if (running && !node.hasUnprocessedMessages(inbox)) {
        if (node.lastStep?.type === 'ERROR_MESSAGE') {
          finishTurn(
            'failed',
            stepErrorMessage(node.lastStep, 'Subagent error'),
          )
        } else if (childStatus.settled && node.isSettled(inbox)) {
          const latestInbox =
            activeChildIds.length > 0
              ? await readInboxState(root, nativeId)
              : inbox
          if (!node.hasUnprocessedMessages(latestInbox)) finishTurn('completed')
        }
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        if (isParentTurnActive && !isParentTurnActive()) finishTurn('completed')
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
            message: `Cannot read child transcript: ${String(error)}`,
          },
        })
      }
    } finally {
      busy = false
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
      cancel() {
        finishTurn('canceled')
        stopped = true
        clearInterval(timer)
      },
    },
  )
}
