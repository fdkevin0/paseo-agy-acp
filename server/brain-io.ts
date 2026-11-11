import {
  lstat,
  open,
  readdir,
  readFile,
  realpath,
  type FileHandle,
} from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { join, sep } from 'node:path'
import {
  isAskQuestionName,
  nonnegative,
  record,
  sessionIdPattern,
  setBounded,
} from './common.ts'
import type { StepUsage } from './mapping.ts'

export type SubagentRecord = {
  conversationId: string
  /** Native lifecycle state; only SUBAGENT_STATE_ALIVE counts as active work. */
  state: string
  spawnStepIndex: number
  spawnCallId?: string
}

export const SUBAGENT_ALIVE = 'SUBAGENT_STATE_ALIVE'

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
  stepCallIds: Map<number, string>
}

const restartNoticePattern =
  /\bsender=system\b[^\n]*\scontent=\[Notice\] All your subagents and background tasks have been stopped/

const rowMessageHeaderPattern =
  /(?:^|\n)(?:\d+\.\s+)?\[Message\]\s+(?:timestamp=\S+\s+)?sender=(\S+)(?:\s+priority=(\S+))?/g

/**
 * agy logs a step once it reaches RUNNING, WAITING or DONE (or fails) and never appends it again.
 * Only full O_TRUNC rewrites can contain these earlier statuses, and a later row for the same step
 * always supersedes them, so readers drop them entirely.
 */
const unloggedStepStatuses = new Set(['PENDING', 'QUEUED', 'GENERATING'])

class BrainEscapeError extends Error {
  constructor() {
    super('Brain artifact escapes its directory')
    this.name = 'BrainEscapeError'
  }
}

export function isMissing(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | undefined)?.code
  return code === 'ENOENT' || code === 'ENOTDIR'
}

function isUnloggedStep(row: Record<string, unknown>): boolean {
  return typeof row.status === 'string' && unloggedStepStatuses.has(row.status)
}

export function isCompletePlannerResponse(
  step: Record<string, unknown> | undefined,
): boolean {
  return (
    step?.type === 'PLANNER_RESPONSE' &&
    step.status === 'DONE' &&
    (!Array.isArray(step.tool_calls) ||
      step.tool_calls.length === 0 ||
      step.tool_calls.every((call) =>
        isAskQuestionName(String(record(call).name ?? '')),
      ))
  )
}

export function isRestartNotice(
  row: Record<string, unknown> | undefined,
): boolean {
  return (
    row?.type === 'SYSTEM_MESSAGE' &&
    restartNoticePattern.test(String(row.content ?? ''))
  )
}

export function stepErrorMessage(
  step: Record<string, unknown> | undefined,
  fallback = 'Turn failed',
): string {
  if (typeof step?.content === 'string' && step.content) return step.content
  if (typeof step?.error === 'string' && step.error) return step.error
  const errObj = record(step?.error)
  if (typeof errObj.message === 'string' && errObj.message)
    return errObj.message
  return fallback
}

export function extractStepUsage(
  row: Record<string, unknown>,
): StepUsage | undefined {
  if (row.type !== 'PLANNER_RESPONSE') return undefined
  const input = nonnegative(row.input_tokens) ? row.input_tokens : undefined
  const output = nonnegative(row.output_tokens) ? row.output_tokens : undefined
  if (
    input === undefined &&
    output === undefined &&
    !nonnegative(row.cache_read_tokens) &&
    !nonnegative(row.cache_write_tokens)
  )
    return undefined
  const cacheWrite = nonnegative(row.cache_write_tokens)
    ? row.cache_write_tokens
    : undefined
  const cachedRead = nonnegative(row.cache_read_tokens)
    ? row.cache_read_tokens
    : 0
  if ((input ?? 0) + (output ?? 0) + cachedRead + (cacheWrite ?? 0) === 0)
    return undefined
  return {
    inputTokens: input ?? 0,
    outputTokens: output ?? 0,
    cachedReadTokens: cachedRead,
    ...(cacheWrite !== undefined ? { cachedWriteTokens: cacheWrite } : {}),
  }
}

export function countRowMessages(row: Record<string, unknown>): number {
  if (
    row.type !== 'SYSTEM_MESSAGE' ||
    typeof row.content !== 'string' ||
    isRestartNotice(row)
  )
    return 0
  let count = 0
  for (const match of row.content.matchAll(rowMessageHeaderPattern)) {
    const sender = match[1]
    const priority = match[2]
    if (sender !== 'user' && priority !== 'MESSAGE_PRIORITY_LOW') {
      count++
    }
  }
  if (
    Number(row.step_index) === 0 &&
    /\bsender=[a-f0-9-]{36}(?=\s|$)/i.test(row.content)
  ) {
    return Math.max(0, count - 1)
  }
  return count
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

async function readSafeJsonFile(
  path: string,
): Promise<Record<string, unknown>> {
  const info = await lstat(path)
  if (info.isSymbolicLink()) throw new BrainEscapeError()
  return record(JSON.parse(await readFile(path, 'utf8')))
}

/** Read a message file, re-parsing it only when its mtime or size changes. */
async function readMessageCached(path: string, fallbackId: string) {
  const info = await lstat(path)
  if (info.isSymbolicLink()) throw new BrainEscapeError()
  const hit = messageCache.get(path)
  if (hit && hit.mtimeMs === info.mtimeMs && hit.size === info.size)
    return hit.record
  const message = parseMessage(
    record(JSON.parse(await readFile(path, 'utf8'))),
    fallbackId,
  )
  setBounded(
    messageCache,
    path,
    {
      mtimeMs: info.mtimeMs,
      size: info.size,
      record: message,
    },
    MESSAGE_CACHE_LIMIT,
  )
  return message
}

/** A subagent's spawning call never changes once its inbox has been populated. */
const SPAWN_CACHE_LIMIT = 4096
const spawnCache = new Map<string, { callId?: string; stepIndex?: number }>()

export async function resolveBrainPath(
  root: string,
  nativeId: string,
  subpath: string,
): Promise<string> {
  const base = await realpath(root)
  const path = await realpath(
    join(base, nativeId, '.system_generated', subpath),
  )
  const prefix = base === sep ? sep : base + sep
  if (!path.startsWith(prefix)) throw new BrainEscapeError()
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
      const parsed = await readSafeJsonFile(join(dir, 'read.json'))
      for (const [id, isRead] of Object.entries(parsed))
        if (isRead === true) readIds.add(id)
    } catch (error) {
      if (error instanceof BrainEscapeError) throw error
      /* Ignore partially written read.json and re-poll on next tick. */
    }
  }
  const messageEntries = entries.filter(
    (name) => name.endsWith('.json') && name !== 'read.json',
  )
  const loaded = await Promise.all(
    messageEntries.map(async (name) => {
      try {
        return await readMessageCached(join(dir, name), name.slice(0, -5))
      } catch (error) {
        if (error instanceof BrainEscapeError) throw error
        /* Ignore partially written message files and re-poll on next tick. */
        return undefined
      }
    }),
  )
  const messages: BrainMessageRecord[] = []
  const stepCallIds = new Map<number, string>()
  for (const message of loaded) {
    if (!message) continue
    const { tool } = message
    if (
      tool?.conversationId === nativeId &&
      tool.callId &&
      tool.stepIndex !== undefined
    ) {
      stepCallIds.set(tool.stepIndex, tool.callId)
    }
    messages.push(message)
  }
  const isWakeupMessage = (m: BrainMessageRecord) =>
    m.sender !== 'user' &&
    m.tool?.name !== 'invoke_subagent' &&
    !m.hideFromUser &&
    m.priority !== 'MESSAGE_PRIORITY_LOW'
  const unconsumedHighPriority = messages.filter(
    (m) => isWakeupMessage(m) && !readIds.has(m.id),
  )
  const consumedNonUserCount = messages.filter(
    (m) => isWakeupMessage(m) && readIds.has(m.id),
  ).length
  return {
    messages,
    readIds,
    unconsumedHighPriority,
    consumedNonUserCount,
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
          const raw = await readSafeJsonFile(join(dir, name))
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
              setBounded(spawnCache, spawnKey, spawn, SPAWN_CACHE_LIMIT)
            } else if (childInbox.messages.length > 0) {
              spawn = {}
              setBounded(spawnCache, spawnKey, spawn, SPAWN_CACHE_LIMIT)
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
        } catch (error) {
          if (error instanceof BrainEscapeError) throw error
          /* Ignore partially written subagent descriptor files. */
          return undefined
        }
      }),
  )
  return loaded.filter((r): r is SubagentRecord => r !== undefined)
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
export class TranscriptReader {
  private identity = ''
  private offset = 0
  private remainder = ''
  private head = Buffer.alloc(0)
  private tail = Buffer.alloc(0)

  private clearCursor(identity = '') {
    this.identity = identity
    this.offset = 0
    this.remainder = ''
    this.head = Buffer.alloc(0)
    this.tail = Buffer.alloc(0)
  }

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
        this.clearCursor(identity)
      }
      const startedAtZero = this.offset === 0 && this.remainder === ''
      try {
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
      } catch (error) {
        if (error instanceof SyntaxError) {
          const latestStat = await file.stat().catch(() => null)
          const racedRewrite =
            !startedAtZero ||
            !latestStat ||
            latestStat.size !== stat.size ||
            !(await this.matches(file, this.head, 0).catch(() => false))
          if (racedRewrite) {
            this.clearCursor()
            return { rows: [], reset: true }
          }
        }
        throw error
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

/** Walk complete JSONL records backwards without parsing unrelated history or splitting UTF-8. */
async function* readTranscriptTail(root: string, nativeId: string) {
  if (!sessionIdPattern.test(nativeId)) return
  let file
  try {
    file = await open(await resolveTranscriptPath(root, nativeId), 'r')
  } catch (error) {
    if (isMissing(error)) return
    throw error
  }
  try {
    let end = (await file.stat()).size
    let prefix = Buffer.alloc(0)
    while (end > 0) {
      const start = Math.max(0, end - 64 * 1024)
      const chunk = Buffer.alloc(end - start)
      const { bytesRead } = await file.read(chunk, 0, chunk.length, start)
      const bytes = Buffer.concat([chunk.subarray(0, bytesRead), prefix])
      let lineEnd = bytes.length
      for (
        let i = bytes.lastIndexOf(10);
        i >= 0;
        i = bytes.lastIndexOf(10, i - 1)
      ) {
        const line = bytes.subarray(i + 1, lineEnd)
        lineEnd = i
        if (line.length > 8 * 1024 * 1024)
          throw new Error('Brain JSONL line exceeds 8 MiB')
        // Tail snapshots may end at a complete JSON value without LF; this does not commit a reader snapshot.
        if (line.length) {
          try {
            yield record(JSON.parse(line.toString('utf8')))
          } catch (error) {
            if (!(error instanceof SyntaxError)) throw error
          }
        }
        if (i === 0) break
      }
      prefix = Buffer.from(bytes.subarray(0, lineEnd))
      if (prefix.length > 8 * 1024 * 1024)
        throw new Error('Brain JSONL line exceeds 8 MiB')
      end = start
    }
    if (prefix.length) {
      try {
        yield record(JSON.parse(prefix.toString('utf8')))
      } catch (error) {
        if (!(error instanceof SyntaxError)) throw error
      }
    }
  } finally {
    await file.close()
  }
}

/** Step index of the newest complete transcript row. */
export async function readLastStepIndex(
  root: string,
  nativeId: string,
): Promise<number> {
  for await (const row of readTranscriptTail(root, nativeId)) {
    const step = Number(row.step_index)
    if (Number.isInteger(step)) return step
  }
  return -1
}

/** Remembers each step's delivered shape, so a rewrite re-delivers only changed steps. */
export class StepDedupe {
  private seen = new Map<number, string>()

  prune(rows: readonly Record<string, unknown>[]) {
    const keep = new Set(rows.map((r) => Number(r.step_index)))
    for (const step of this.seen.keys())
      if (!keep.has(step)) this.seen.delete(step)
  }

  /** Whether this row is new or changed since its step was last seen. */
  fresh(row: Record<string, unknown>): boolean {
    const step = Number(row.step_index)
    // Store a digest, not the payload: long-lived watchers would otherwise retain every step's full text.
    const signature = createHash('sha1')
      .update(
        JSON.stringify([
          row.type,
          row.status ?? '',
          row.content ?? '',
          row.thinking ?? '',
          row.tool_calls ?? null,
        ]),
      )
      .digest('base64')
    if (this.seen.get(step) === signature) return false
    this.seen.set(step, signature)
    return true
  }
}

/** Latest positive model snapshot and observed turn boundary; cumulative usage is never context. */
export async function readLatestStepUsage(
  root: string,
  nativeId: string,
  minStepIndex = -1,
): Promise<{ lastStepUsage?: StepUsage; lastStepIndex: number }> {
  let lastStepIndex = -1
  for await (const row of readTranscriptTail(root, nativeId)) {
    if (!Number.isInteger(row.step_index)) continue
    const step = Number(row.step_index)
    lastStepIndex = Math.max(lastStepIndex, step)
    if (step <= minStepIndex) break
    const lastStepUsage = extractStepUsage(row)
    if (lastStepUsage) return { lastStepUsage, lastStepIndex }
  }
  return { lastStepIndex }
}
