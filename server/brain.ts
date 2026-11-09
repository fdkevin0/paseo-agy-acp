import {
  open,
  readdir,
  readFile,
  realpath,
  stat,
  type FileHandle,
} from 'node:fs/promises'
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

type SubagentRecord = {
  conversationId: string
  /** Native lifecycle state; only SUBAGENT_STATE_ALIVE counts as active work. */
  state: string
  spawnStepIndex: number
  spawnCallId?: string
}

const SUBAGENT_ALIVE = 'SUBAGENT_STATE_ALIVE'

type BrainMessageRecord = {
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

type InboxState = {
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

/**
 * agy logs a step once it reaches RUNNING, WAITING or DONE (or fails) and never appends it again.
 * Only full O_TRUNC rewrites can contain these earlier statuses, and a later row for the same step
 * always supersedes them, so readers drop them entirely.
 */
const unloggedStepStatuses = new Set(['PENDING', 'QUEUED', 'GENERATING'])
/** Logged while still running: the transcript never records this step's own result. */
const runningStepStatuses = new Set(['RUNNING', 'WAITING'])

function isMissing(error: unknown) {
  return (error as NodeJS.ErrnoException).code === 'ENOENT'
}

function isUnloggedStep(row: Record<string, unknown>) {
  return typeof row.status === 'string' && unloggedStepStatuses.has(row.status)
}

function terminalToolStatus(
  status: unknown,
): 'completed' | 'failed' | 'canceled' {
  if (status === 'ERROR') return 'failed'
  if (status === 'CANCELED' || status === 'INTERRUPTED') return 'canceled'
  return 'completed'
}

function isCompletePlannerResponse(step: Record<string, unknown> | undefined) {
  return (
    step?.type === 'PLANNER_RESPONSE' &&
    step.status === 'DONE' &&
    (!Array.isArray(step.tool_calls) || step.tool_calls.length === 0)
  )
}

function isRestartNotice(row: Record<string, unknown> | undefined) {
  return (
    row?.type === 'SYSTEM_MESSAGE' &&
    restartNoticePattern.test(String(row.content ?? ''))
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
  const count = (value: unknown) =>
    typeof value === 'number' && Number.isFinite(value) && value >= 0
      ? value
      : undefined
  const input = count(row.input_tokens)
  const output = count(row.output_tokens)
  if (input === undefined && output === undefined) return undefined
  const cacheWrite = count(row.cache_write_tokens)
  return {
    inputTokens: input ?? 0,
    outputTokens: output ?? 0,
    cachedReadTokens: count(row.cache_read_tokens) ?? 0,
    ...(cacheWrite !== undefined ? { cachedWriteTokens: cacheWrite } : {}),
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

type CachedMessage = {
  mtimeMs: number
  size: number
  record: BrainMessageRecord
}
const messageCache = new Map<string, CachedMessage>()
const MESSAGE_CACHE_LIMIT = 4096

/** Parse a write-once inbox message into its light record (bodies and signatures are dropped). */
function parseMessage(raw: Record<string, unknown>, fallbackId: string) {
  const toolMeta = record(record(raw.sourceMetadata).tool)
  const toolCall = record(toolMeta.toolCall)
  const callId =
    typeof toolCall.id === 'string' && toolCall.id ? toolCall.id : undefined
  const stepIndex =
    typeof toolMeta.stepIndex === 'number' &&
    Number.isInteger(toolMeta.stepIndex)
      ? toolMeta.stepIndex
      : undefined
  const conversationId =
    typeof toolMeta.conversationId === 'string'
      ? toolMeta.conversationId
      : undefined
  const message: BrainMessageRecord = {
    id: typeof raw.id === 'string' && raw.id ? raw.id : fallbackId,
    sender: typeof raw.sender === 'string' ? raw.sender : '',
    priority: typeof raw.priority === 'string' ? raw.priority : undefined,
    hideFromUser: raw.hideFromUser === true,
    ...(conversationId || stepIndex !== undefined || callId
      ? {
          tool: {
            conversationId,
            stepIndex,
            callId,
            name: typeof toolCall.name === 'string' ? toolCall.name : undefined,
          },
        }
      : {}),
  }
  return message
}

/** Read a message file, re-parsing it only when its mtime or size changes. */
async function readMessageCached(path: string, fallbackId: string) {
  const info = await stat(path)
  const hit = messageCache.get(path)
  if (hit && hit.mtimeMs === info.mtimeMs && hit.size === info.size)
    return hit.record
  const message = parseMessage(
    record(JSON.parse(await readFile(path, 'utf8'))),
    fallbackId,
  )
  messageCache.delete(path)
  messageCache.set(path, {
    mtimeMs: info.mtimeMs,
    size: info.size,
    record: message,
  })
  if (messageCache.size > MESSAGE_CACHE_LIMIT)
    messageCache.delete(messageCache.keys().next().value!)
  return message
}

/** A subagent's spawning call never changes once its init message exists. */
const spawnCache = new Map<string, { callId?: string; stepIndex?: number }>()

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
    if (isMissing(error)) return empty
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
      const message = await readMessageCached(
        join(dir, name),
        name.slice(0, -5),
      )
      const { sender, tool } = message
      if (tool?.conversationId === nativeId && tool.callId) {
        if (sender) taskCallIds.set(sender, tool.callId)
        if (tool.stepIndex !== undefined)
          stepCallIds.set(tool.stepIndex, tool.callId)
      }
      messages.push(message)
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
    if (isMissing(error)) return []
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
          // A missing state means the descriptor predates lifecycle tracking; unknown states are not active.
          const state =
            typeof raw.state === 'string' && raw.state
              ? raw.state
              : SUBAGENT_ALIVE
          let spawnStepIndex =
            typeof raw.spawnStepIndex === 'number' &&
            Number.isInteger(raw.spawnStepIndex)
              ? raw.spawnStepIndex
              : 0
          const spawnKey = `${root}\0${nativeId}\0${conversationId}`
          let spawn = spawnCache.get(spawnKey)
          if (!spawn) {
            const childInbox = await readInboxState(root, conversationId)
            const init = childInbox.messages.find(
              (m) =>
                m.tool?.conversationId === nativeId &&
                m.tool.name === 'invoke_subagent',
            )?.tool
            if (init) {
              spawn = { callId: init.callId, stepIndex: init.stepIndex }
              spawnCache.set(spawnKey, spawn)
            }
          }
          const spawnCallId = spawn?.callId
          if (spawn?.stepIndex !== undefined) spawnStepIndex = spawn.stepIndex
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
        if (isRestartNotice(row)) {
          // Stopped calls never log a result; child transcripts have no USER_INPUT to reset them.
          this.pending = []
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
        // Every tool call is logged exactly once, so each GENERIC row consumes one call in FIFO order.
        const pending = this.pending.shift()
        if (!pending) return []
        if (runningStepStatuses.has(String(row.status))) {
          const taskId = text.match(backgroundTaskPattern)?.[1]
          if (taskId && pending.args.IsDaemon !== true)
            this.background.set(taskId, pending)
          return []
        }
        const subagent =
          this.callSubagents.get(pending.callId) ??
          this.stepSubagents.get(stepIndex) ??
          this.stepSubagents.get(pending.stepIndex)
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
            terminalToolStatus(row.status),
            text,
          ),
        ]
      }
      case 'PLANNER_RESPONSE': {
        // A new model response means calls of the previous one without a GENERIC row were stopped;
        // keeping them would misattribute this response's results in FIFO order.
        this.pending = []
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

const REWRITE_PROBE_BYTES = 4096

/**
 * One cursor owns partial records across polls, path replacement and in-place rewrites.
 * agy rewrites transcript_full.jsonl with O_TRUNC on the same inode, so besides inode and size
 * changes the reader re-checks the first bytes and the bytes just before its offset.
 */
class TranscriptReader {
  private identity = ''
  private offset = 0
  private remainder = ''
  private head = Buffer.alloc(0)
  private tail = Buffer.alloc(0)

  private async matches(file: FileHandle, probe: Buffer, position: number) {
    if (!probe.length) return true
    const buffer = Buffer.alloc(probe.length)
    const { bytesRead } = await file.read(buffer, 0, probe.length, position)
    return bytesRead === probe.length && buffer.equals(probe)
  }

  /** Keep the probes equal to bytes actually parsed, so a rewrite racing this read is still noticed. */
  private remember(chunk: Buffer) {
    if (this.head.length < REWRITE_PROBE_BYTES)
      this.head = Buffer.concat([
        this.head,
        chunk.subarray(0, REWRITE_PROBE_BYTES - this.head.length),
      ])
    this.tail =
      chunk.length >= REWRITE_PROBE_BYTES
        ? Buffer.from(chunk.subarray(chunk.length - REWRITE_PROBE_BYTES))
        : Buffer.concat([this.tail, chunk]).subarray(-REWRITE_PROBE_BYTES)
  }

  async read(root: string, nativeId: string) {
    const path = await resolveTranscriptPath(root, nativeId)
    const file = await open(path, 'r')
    const rows: Record<string, unknown>[] = []
    try {
      const stat = await file.stat()
      const identity = `${path}:${stat.dev}:${stat.ino}`
      const reset =
        identity !== this.identity ||
        stat.size < this.offset ||
        !(await this.matches(file, this.head, 0)) ||
        !(await this.matches(file, this.tail, this.offset - this.tail.length))
      if (reset) {
        this.identity = identity
        this.offset = 0
        this.remainder = ''
        this.head = Buffer.alloc(0)
        this.tail = Buffer.alloc(0)
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
        this.remember(buffer.subarray(0, bytesRead))
        // Preserve bytes until a complete line is available, including split UTF-8 characters.
        this.remainder += buffer.subarray(0, bytesRead).toString('latin1')
        const lines = this.remainder.split('\n')
        this.remainder = lines.pop()!
        if (this.remainder.length > 8 * 1024 * 1024)
          throw new Error('Brain JSONL line exceeds 8 MiB')
        for (const line of lines) {
          if (line.length > 8 * 1024 * 1024)
            throw new Error('Brain JSONL line exceeds 8 MiB')
          if (!line.trim()) continue
          const row = record(
            JSON.parse(Buffer.from(line, 'latin1').toString('utf8')),
          )
          if (!isUnloggedStep(row)) rows.push(row)
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
    if (isMissing(error)) return []
    throw error
  }
}

/** Step index of the newest transcript row; reads only the file tail since rows are appended in step order. */
export async function readLastStepIndex(
  root: string,
  nativeId: string,
): Promise<number> {
  if (!sessionIdPattern.test(nativeId)) return -1
  let file
  try {
    file = await open(await resolveTranscriptPath(root, nativeId), 'r')
  } catch (error) {
    if (isMissing(error)) return -1
    throw error
  }
  try {
    const { size } = await file.stat()
    for (let window = 64 * 1024; ; window *= 4) {
      const length = Math.min(size, window)
      const buffer = Buffer.alloc(length)
      const { bytesRead } = await file.read(buffer, 0, length, size - length)
      const lines = buffer.subarray(0, bytesRead).toString('utf8').split('\n')
      // The first line may be cut by the window unless it starts the file.
      const complete = length < size ? lines.slice(1) : lines
      for (let i = complete.length - 1; i >= 0; i--) {
        if (!complete[i].trim()) continue
        try {
          const step = Number(record(JSON.parse(complete[i])).step_index)
          if (Number.isInteger(step)) return step
        } catch {
          /* A torn trailing write; look at the previous record. */
        }
      }
      if (length >= size) return -1
    }
  } finally {
    await file.close()
  }
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
      sub.state === SUBAGENT_ALIVE &&
      (sub.spawnStepIndex > minStepIndex || touched.has(sub.conversationId))
    ) {
      ids.add(sub.conversationId)
    }
  }
  return [...ids]
}

/** Remembers each step's delivered (type, status), so a rewrite re-delivers only changed steps. */
class StepDedupe {
  private seen = new Map<number, string>()

  /** Whether this row is new or changed since its step was last seen. */
  fresh(row: Record<string, unknown>): boolean {
    const step = Number(row.step_index)
    const signature = `${String(row.type)}:${String(row.status ?? '')}`
    if (this.seen.get(step) === signature) return false
    this.seen.set(step, signature)
    return true
  }
}

class BrainNode {
  readonly reader = new TranscriptReader()
  projector: BrainTranscriptProjector
  readonly children = new Map<string, BrainNode>()
  consumedMessages = 0
  lastStep?: Record<string, unknown>
  seenPlannerResponse = false
  foregroundIdle = false
  wokeUp = false
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
    this.seenPlannerResponse = false
    this.foregroundIdle = false
    this.wokeUp = false
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
  ingest(row: Record<string, unknown>): ProviderTimelineItem[] {
    if (row.type === 'USER_INPUT' || isRestartNotice(row)) {
      this.seenPlannerResponse = false
      this.foregroundIdle = false
      this.wokeUp = false
    } else if (row.type === 'SYSTEM_MESSAGE' && this.foregroundIdle) {
      this.wokeUp = true
    }
    this.consumedMessages += countRowMessages(row)
    const items = this.projector.project(row)
    if (row.type !== 'CHECKPOINT') this.lastStep = row
    if (row.type === 'PLANNER_RESPONSE') this.seenPlannerResponse = true
    if (
      this.seenPlannerResponse &&
      !this.projector.hasPending() &&
      (isCompletePlannerResponse(row) || row.type === 'GENERIC')
    )
      this.foregroundIdle = true
    return items
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
      if (!isChild && (row.type === 'USER_INPUT' || isRestartNotice(row))) {
        minTurnStepIndex = Number(row.step_index)
      }
      node.ingest(row)
    }
    if (node.projector.hasBackground()) return true
    // A failed step is finished; an unsettled child or woken parent turn is still working outside ACP prompt RPC.
    if (
      isChild
        ? node.hasUnprocessedMessages(inbox) ||
          (!node.isSettled(inbox) && node.lastStep?.type !== 'ERROR_MESSAGE')
        : node.isWokenWorking()
    )
      return true
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
        for (const row of rows) child.ingest(row)
      } catch (error) {
        if (isMissing(error)) return { settled: false }
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
      // This child's report must reach the parent's transcript before the child counts as settled.
      if (
        parentInbox.unconsumedHighPriority.some((m) => m.sender === childId) ||
        parentNode.consumedMessages < parentInbox.consumedNonUserCount
      )
        return { settled: false }
    }
    return { settled: true }
  } finally {
    visiting.delete(parentNode.nativeId)
  }
}

function pause(ms: number, signal?: AbortSignal) {
  return new Promise<void>((resolve) => {
    // An abort during the previous poll has already fired; do not wait for it again.
    if (signal?.aborted) return resolve()
    const onAbort = () => {
      clearTimeout(timer)
      resolve()
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    timer.unref()
    signal?.addEventListener('abort', onAbort, { once: true })
  })
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
    shouldEmitPreWakeupTool,
    transcriptGraceMs = 0,
    stallTimeoutMs = 120_000,
    wokenStallTimeoutMs = 15 * 60_000,
    isWaitingOnUser,
  }: {
    signal?: AbortSignal
    minStepIndex?: number
    pollIntervalMs?: number
    acpToolCallIds?: readonly string[]
    shouldEmitPreWakeupTool?: (
      item: Extract<ProviderTimelineItem, { type: 'tool_call' }>,
    ) => boolean
    /** How long to wait for agy to flush this turn's USER_INPUT row before treating the turn as transcript-less. */
    transcriptGraceMs?: number
    /** Give up when nothing tracked is running and the transcript stays idle without a final response. */
    stallTimeoutMs?: number
    /** Upper bound on a single silent woken-turn step (tool execution or post-wakeup/post-tool LLM generation). */
    wokenStallTimeoutMs?: number
    /** Pending approvals legitimately block agy; they suspend the stall timeout. */
    isWaitingOnUser?: () => boolean
  } = {},
): Promise<{
  status: 'completed' | 'canceled' | 'failed'
  stalled?: boolean
  error?: string
  lastStepUsage?: StepUsage
}> {
  if (!sessionIdPattern.test(nativeId)) return { status: 'completed' }
  const node = new BrainNode(nativeId, sessionId)
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
  const withinGrace = () => Date.now() - startedAt < transcriptGraceMs
  const stall = (timeoutMs = stallTimeoutMs) => {
    const error = `Stopped waiting for agy: transcript_full.jsonl was idle for ${Math.round(timeoutMs / 1000)}s without a final response.`
    onItem({
      type: 'error',
      id: `${sessionId}:step:${lastStepIndex}:stalled`,
      message: error,
    })
    return {
      status: 'failed' as const,
      stalled: true,
      error,
      lastStepUsage,
    }
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
      return { status: 'canceled', lastStepUsage }
    }
    const { inbox, subagents } = await node.pollMetadata(root)
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
      if (result.reset) {
        initial = true
        preamble = []
        node.reset(inbox, subagents)
      }
    } catch (error) {
      if (!isMissing(error)) throw error
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
      if (usage) lastStepUsage = usage
      if (Number.isInteger(stepIndex) && stepIndex > lastStepIndex)
        lastStepIndex = stepIndex
      if (dedupe.fresh(row)) {
        const timestamp =
          typeof row.created_at === 'string' ? row.created_at : undefined
        for (const item of items) {
          if (node.wokeUp || item.type === 'error') {
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
    if (node.lastStep?.type === 'ERROR_MESSAGE') {
      return {
        status: 'failed',
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
        error: childStatus.error,
        lastStepUsage,
      }
    if (childStatus.settled && node.isSettled(inbox)) {
      if (activeChildIds.length > 0) {
        if (node.hasUnprocessedMessages(await readInboxState(root, nativeId)))
          continue
      }
      return { status: 'completed', lastStepUsage }
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
    if (tracking) lastActivity = Date.now()
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
  let turnSeq = resume?.turnSeq ?? 1
  let turnId = `${sessionId}:turn:${turnSeq}`
  let lastStepIndex = resume?.afterStep ?? -1
  const dedupe = new StepDedupe()
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
      const { rows, reset } = await node.reader.read(root, nativeId)
      if (reset) node.reset(inbox, subagents)
      for (const row of rows) {
        if (stopped) return
        const stepIndex = Number(row.step_index)
        // A rewrite re-delivers earlier steps; only a new (step, status) is new child activity.
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
          emit({
            type: 'session.turn',
            sessionId,
            turnId,
            state: 'started',
          })
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
      if (isMissing(error)) {
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
