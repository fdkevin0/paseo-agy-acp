/** Session revert orchestration: token validation, DB/Brain conversation forking, and file rollback. */
import { randomUUID } from 'node:crypto'
import {
  copyFile,
  lstat,
  mkdir,
  readFile,
  readdir,
  realpath,
  rename,
  rm,
  writeFile,
} from 'node:fs/promises'
import { join, sep } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { z } from 'zod'
import { readTranscriptRows } from './brain.ts'
import { extractRowMessageSenders } from './brain-io.ts'
import {
  findCheckpointForUserStep,
  forkSessionCheckpoints,
  readSessionCheckpoints,
  removeSessionCheckpoints,
  restoreSessionFileCheckpoint,
  validateSessionFileCheckpoint,
  type TurnFileCheckpoint,
} from './checkpoints.ts'
import {
  assertSafeNativeId,
  isMissingError,
  isNativeToolName,
  num,
  record,
  str,
} from './common.ts'
import { normalizeUserText, type HistoryEntry } from './history.ts'
import { removeSessionArtifacts } from './preflight.ts'

export type RevertScope = 'files' | 'conversation' | 'both'

const AgyRevertTokenSchema = z.object({
  version: z.literal(1),
  kind: z.literal('agy-acp-turn'),
  clientMessageId: z.string().min(1).optional(),
  afterStepIndex: z.number().int().min(-1).optional(),
  stepIndex: z.number().int().min(0).optional(),
  turnOrdinal: z.number().int().min(0).optional(),
  checkpointId: z.string().min(1).optional(),
  text: z.string().optional(),
})

type AgyRevertToken = z.infer<typeof AgyRevertTokenSchema>

const TRANSCRIPT_CHUNK_BYTES = 102_400

const TRANSCRIPT_CHUNK_TARGET_BY_REL: Record<
  string,
  { flag: 'hasTranscriptChunks' | 'hasTranscriptFullChunks'; chunkDir: string }
> = {
  '.system_generated/logs/transcript.jsonl': {
    flag: 'hasTranscriptChunks',
    chunkDir: '.system_generated/logs/chunks/transcript',
  },
  '.system_generated/logs/transcript_full.jsonl': {
    flag: 'hasTranscriptFullChunks',
    chunkDir: '.system_generated/logs/chunks/transcript_full',
  },
}

interface ProtoField {
  fieldNumber: number
  wireType: number
  raw: Buffer
  varint?: bigint
  bytes?: Buffer
}

function readVarint(
  buf: Uint8Array,
  offset: number,
): { value: bigint; nextOffset: number } | null {
  let value = 0n
  let shift = 0n
  let pos = offset
  while (pos < buf.length && shift < 70n) {
    const byte = BigInt(buf[pos++])
    value |= (byte & 0x7fn) << shift
    if ((byte & 0x80n) === 0n) return { value, nextOffset: pos }
    shift += 7n
  }
  return null
}

function encodeVarint(value: number | bigint): Buffer {
  let v = typeof value === 'bigint' ? value : BigInt(Math.max(0, value))
  const out: number[] = []
  do {
    let byte = Number(v & 0x7fn)
    v >>= 7n
    if (v > 0n) byte |= 0x80
    out.push(byte)
  } while (v > 0n)
  return Buffer.from(out)
}

function encodeVarintField(
  fieldNumber: number,
  value: number | bigint,
): Buffer {
  return Buffer.concat([
    encodeVarint((fieldNumber << 3) | 0),
    encodeVarint(value),
  ])
}

function encodeBytesField(fieldNumber: number, payload: Uint8Array): Buffer {
  return Buffer.concat([
    encodeVarint((fieldNumber << 3) | 2),
    encodeVarint(payload.byteLength),
    Buffer.from(payload),
  ])
}

function parseProtoFields(input: Uint8Array): ProtoField[] | null {
  const buf = Buffer.from(input.buffer, input.byteOffset, input.byteLength)
  const fields: ProtoField[] = []
  let offset = 0
  while (offset < buf.length) {
    const start = offset
    const tag = readVarint(buf, offset)
    if (!tag) return null
    offset = tag.nextOffset
    const fieldNumber = Number(tag.value >> 3n)
    const wireType = Number(tag.value & 7n)
    if (fieldNumber <= 0) return null
    if (wireType === 0) {
      const v = readVarint(buf, offset)
      if (!v) return null
      offset = v.nextOffset
      fields.push({
        fieldNumber,
        wireType,
        raw: buf.subarray(start, offset),
        varint: v.value,
      })
    } else if (wireType === 1) {
      if (offset + 8 > buf.length) return null
      offset += 8
      fields.push({ fieldNumber, wireType, raw: buf.subarray(start, offset) })
    } else if (wireType === 2) {
      const len = readVarint(buf, offset)
      if (!len) return null
      const byteLen = Number(len.value)
      if (
        !Number.isSafeInteger(byteLen) ||
        len.nextOffset + byteLen > buf.length
      )
        return null
      const bytes = buf.subarray(len.nextOffset, len.nextOffset + byteLen)
      offset = len.nextOffset + byteLen
      fields.push({
        fieldNumber,
        wireType,
        raw: buf.subarray(start, offset),
        bytes,
      })
    } else if (wireType === 5) {
      if (offset + 4 > buf.length) return null
      offset += 4
      fields.push({ fieldNumber, wireType, raw: buf.subarray(start, offset) })
    } else {
      return null
    }
  }
  return fields
}

function protoVarint(
  fields: readonly ProtoField[],
  fieldNumber: number,
): number {
  for (let i = fields.length - 1; i >= 0; i--) {
    const f = fields[i]
    if (
      f.fieldNumber === fieldNumber &&
      f.wireType === 0 &&
      f.varint !== undefined
    ) {
      return Number(f.varint)
    }
  }
  return 0
}

function protoString(
  fields: readonly ProtoField[],
  fieldNumber: number,
): string {
  for (let i = fields.length - 1; i >= 0; i--) {
    const f = fields[i]
    if (f.fieldNumber === fieldNumber && f.wireType === 2 && f.bytes) {
      return f.bytes.toString('utf8')
    }
  }
  return ''
}

function decodeGenStepIndices(fields: readonly ProtoField[]): number[] {
  const indices: number[] = []
  for (const f of fields) {
    if (f.fieldNumber !== 2) continue
    if (f.wireType === 0 && f.varint !== undefined) {
      indices.push(Number(f.varint))
    } else if (f.wireType === 2 && f.bytes) {
      let off = 0
      while (off < f.bytes.length) {
        const v = readVarint(f.bytes, off)
        if (!v) break
        indices.push(Number(v.value))
        off = v.nextOffset
      }
    }
  }
  return indices
}

function replaceProtoFields(
  fields: readonly ProtoField[],
  replacements: ReadonlyMap<number, Buffer | null>,
): Buffer {
  const chunks: Buffer[] = []
  const handled = new Set<number>()
  for (const f of fields) {
    if (replacements.has(f.fieldNumber)) {
      if (!handled.has(f.fieldNumber)) {
        handled.add(f.fieldNumber)
        const next = replacements.get(f.fieldNumber)
        if (next && next.byteLength > 0) chunks.push(next)
      }
      continue
    }
    chunks.push(f.raw)
  }
  for (const [fn, next] of replacements) {
    if (handled.has(fn) || !next || next.byteLength === 0) continue
    chunks.push(next)
  }
  return Buffer.concat(chunks)
}

interface UserStepRow {
  stepIndex: number
  text: string
}

async function assertWithinDir(rootDir: string, targetPath: string) {
  const base = await realpath(rootDir)
  const resolved = await realpath(targetPath)
  const prefix = base === sep ? sep : base + sep
  if (!resolved.startsWith(prefix)) {
    throw new Error(`Path escapes directory: ${targetPath}`)
  }
  return resolved
}

export function makeRevertToken(fields: {
  clientMessageId?: string
  afterStepIndex?: number
  stepIndex?: number
  turnOrdinal?: number
  checkpointId?: string
  text?: string
}): AgyRevertToken {
  const token: AgyRevertToken = {
    version: 1,
    kind: 'agy-acp-turn',
  }
  if (fields.clientMessageId !== undefined)
    token.clientMessageId = fields.clientMessageId
  if (fields.afterStepIndex !== undefined)
    token.afterStepIndex = fields.afterStepIndex
  if (fields.stepIndex !== undefined) token.stepIndex = fields.stepIndex
  if (fields.turnOrdinal !== undefined) token.turnOrdinal = fields.turnOrdinal
  if (fields.checkpointId !== undefined)
    token.checkpointId = fields.checkpointId
  if (fields.text !== undefined) token.text = fields.text
  return token
}

function parseRevertToken(token: unknown): AgyRevertToken {
  const parsed = AgyRevertTokenSchema.safeParse(token)
  if (!parsed.success) {
    throw new Error('Invalid or missing revert token for this message')
  }
  return parsed.data
}

export function extractUserStepRows(
  rows: readonly Record<string, unknown>[],
): UserStepRow[] {
  const result: UserStepRow[] = []
  for (const row of rows) {
    if (row.type !== 'USER_INPUT' || typeof row.content !== 'string') continue
    if (!row.content) continue
    const stepIndex = Number(row.step_index)
    if (!Number.isInteger(stepIndex) || stepIndex < 0) continue
    result.push({
      stepIndex,
      text: normalizeUserText(row.content),
    })
  }
  return result
}

/** Resolve the native step_index at which the target turn starts (exclusive upper bound to keep). */
async function resolveCutoffStepIndex(
  brainRoot: string,
  nativeId: string,
  token: AgyRevertToken,
): Promise<number> {
  assertSafeNativeId(nativeId)
  const rows = await readTranscriptRows(brainRoot, nativeId).catch((error) => {
    if (isMissingError(error)) return [] as Record<string, unknown>[]
    throw error
  })
  const userSteps = extractUserStepRows(rows)
  const stepIndices = rows
    .map((r) => num(r.step_index))
    .filter(
      (n): n is number => n !== undefined && Number.isInteger(n) && n >= 0,
    )
  const maxStepIndex = stepIndices.length > 0 ? Math.max(...stepIndices) : -1
  const normalizeCutoff = (stepIndex: number): number =>
    userSteps.length === 0 || stepIndex <= userSteps[0].stepIndex
      ? 0
      : stepIndex

  if (token.stepIndex !== undefined) {
    if (userSteps.length === 0) return token.stepIndex
    const exact = userSteps.find((s) => s.stepIndex === token.stepIndex)
    if (exact) return normalizeCutoff(exact.stepIndex)
  }

  if (token.afterStepIndex !== undefined) {
    const postSteps = stepIndices.filter((idx) => idx > token.afterStepIndex!)
    if (postSteps.length > 0) {
      const minPostStep = Math.min(...postSteps)
      const afterMatches = userSteps.filter(
        (s) => s.stepIndex > token.afterStepIndex!,
      )
      const exactText =
        token.text !== undefined
          ? afterMatches.find((s) => s.text === token.text)
          : undefined
      if (exactText) {
        const hasEarlierUserAfter = afterMatches.some(
          (s) => s.stepIndex < exactText.stepIndex,
        )
        return normalizeCutoff(
          token.afterStepIndex >= 0 && !hasEarlierUserAfter
            ? Math.min(minPostStep, exactText.stepIndex)
            : exactText.stepIndex,
        )
      }
      return normalizeCutoff(minPostStep)
    }
    if (token.afterStepIndex === -1) return 0
    if (token.afterStepIndex >= 0 && token.afterStepIndex <= maxStepIndex) {
      return normalizeCutoff(token.afterStepIndex + 1)
    }
  }

  if (userSteps.length === 0) {
    if (token.turnOrdinal === 0) return 0
    throw new Error('Unable to locate target turn in session transcript')
  }

  let matched: UserStepRow | undefined

  if (token.turnOrdinal !== undefined && token.turnOrdinal < userSteps.length) {
    const candidate = userSteps[token.turnOrdinal]
    if (token.text === undefined || candidate.text === token.text) {
      matched = candidate
    }
  }

  if (!matched && token.text !== undefined) {
    const byText = userSteps.filter((s) => s.text === token.text)
    if (byText.length === 1) matched = byText[0]
  }

  if (!matched) {
    throw new Error('Unable to locate target turn in session transcript')
  }

  // Rewinding the very first user turn leaves no conversation history; start clean.
  return normalizeCutoff(matched.stepIndex)
}

interface BrainFilterContext {
  oldNativeId: string
  newNativeId: string
  cutoffStepIndex: number
  droppedSubagentIds: ReadonlySet<string>
  readMessageIds: ReadonlySet<string>
  keptWakeupDeliveriesBySender: Map<string, number>
  cutoffTimestampMs?: number
  hasTranscriptChunks: boolean
  hasTranscriptFullChunks: boolean
}

async function directoryExists(dirPath: string): Promise<boolean> {
  const st = await lstat(dirPath).catch(() => null)
  return Boolean(st?.isDirectory() && !st.isSymbolicLink())
}

async function readConsumedMessageIds(
  readJsonPath: string,
): Promise<Set<string>> {
  const ids = new Set<string>()
  try {
    const info = await lstat(readJsonPath)
    if (info.isSymbolicLink() || !info.isFile()) return ids
    const parsed = record(JSON.parse(await readFile(readJsonPath, 'utf8')))
    for (const [id, value] of Object.entries(parsed)) {
      if (value === true) ids.add(id)
    }
  } catch {
    // Missing or invalid read.json leaves the consumed set empty.
  }
  return ids
}

function visitTranscriptCutoff(
  raw: string,
  cutoffStepIndex: number,
  handlers: {
    onKept: (line: string, parsed: Record<string, unknown>) => void
    onCut?: (parsed: Record<string, unknown>) => void
    onUnparsedKept?: (line: string) => void
  },
): void {
  let truncated = false
  for (const line of raw.split(/\r?\n/)) {
    if (!line.trim()) continue
    try {
      const parsed = record(JSON.parse(line))
      const stepIndex = num(parsed.step_index)
      if (
        stepIndex !== undefined &&
        Number.isInteger(stepIndex) &&
        stepIndex >= cutoffStepIndex
      ) {
        truncated = true
        handlers.onCut?.(parsed)
        continue
      }
      if (
        truncated &&
        (stepIndex === undefined || !Number.isInteger(stepIndex))
      ) {
        continue
      }
      handlers.onKept(line, parsed)
    } catch {
      if (!truncated) handlers.onUnparsedKept?.(line)
    }
  }
}

async function scanTranscriptState(
  brainDir: string,
  cutoffStepIndex: number,
): Promise<{
  cutoffTimestampMs?: number
  keptWakeupDeliveriesBySender: Map<string, number>
}> {
  for (const rel of [
    '.system_generated/logs/transcript_full.jsonl',
    '.system_generated/logs/transcript.jsonl',
  ]) {
    const raw = await readFile(join(brainDir, rel), 'utf8').catch(() => '')
    if (!raw) continue
    let earliest: number | undefined
    const keptWakeupDeliveriesBySender = new Map<string, number>()
    visitTranscriptCutoff(raw, cutoffStepIndex, {
      onCut(parsed) {
        const ts =
          typeof parsed.created_at === 'string'
            ? Date.parse(parsed.created_at)
            : Number.NaN
        if (Number.isFinite(ts) && (earliest === undefined || ts < earliest)) {
          earliest = ts
        }
      },
      onKept(_line, parsed) {
        for (const sender of extractRowMessageSenders(parsed)) {
          keptWakeupDeliveriesBySender.set(
            sender,
            (keptWakeupDeliveriesBySender.get(sender) ?? 0) + 1,
          )
        }
      },
    })
    return { cutoffTimestampMs: earliest, keptWakeupDeliveriesBySender }
  }
  return {
    cutoffTimestampMs: undefined,
    keptWakeupDeliveriesBySender: new Map(),
  }
}

async function collectDroppedSubagentIds(
  subagentsDir: string,
  cutoffStepIndex: number,
): Promise<Set<string>> {
  const dropped = new Set<string>()
  const entries = await readdir(subagentsDir, { withFileTypes: true }).catch(
    () => [],
  )
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith('.json')) continue
    try {
      const raw = record(
        JSON.parse(await readFile(join(subagentsDir, entry.name), 'utf8')),
      )
      const spawnStepIndex = num(raw.spawnStepIndex)
      if (
        spawnStepIndex !== undefined &&
        Number.isInteger(spawnStepIndex) &&
        spawnStepIndex >= cutoffStepIndex
      ) {
        const conversationId =
          str(raw.conversationId) ?? entry.name.slice(0, -5)
        dropped.add(conversationId)
        dropped.add(entry.name.slice(0, -5))
      }
    } catch {
      // Unreadable descriptors are retained by filterBrainSystemSubagents.
    }
  }
  return dropped
}

async function writeTranscriptChunks(
  chunkDir: string,
  payload: Buffer,
): Promise<void> {
  await mkdir(chunkDir, { recursive: true })
  for (
    let offset = 0, index = 0;
    offset < payload.byteLength;
    offset += TRANSCRIPT_CHUNK_BYTES, index++
  ) {
    const chunkName = `${String(index).padStart(8, '0')}.jsonl`
    await writeFile(
      join(chunkDir, chunkName),
      payload.subarray(offset, offset + TRANSCRIPT_CHUNK_BYTES),
    )
  }
}

async function filterJsonlTranscript(
  srcPath: string,
  destPath: string,
  cutoffStepIndex: number,
): Promise<Buffer> {
  const raw = await readFile(srcPath, 'utf8')
  const kept: string[] = []
  visitTranscriptCutoff(raw, cutoffStepIndex, {
    onKept(line) {
      kept.push(line)
    },
    onUnparsedKept(line) {
      kept.push(line)
    },
  })
  const output = Buffer.from(
    kept.length > 0 ? `${kept.join('\n')}\n` : '',
    'utf8',
  )
  await writeFile(destPath, output)
  return output
}

async function filterBrainSystemSubagents(
  srcDir: string,
  destDir: string,
  cutoffStepIndex: number,
) {
  await mkdir(destDir, { recursive: true })
  const entries = await readdir(srcDir, { withFileTypes: true })
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith('.json')) continue
    const srcFile = join(srcDir, entry.name)
    try {
      const raw = record(JSON.parse(await readFile(srcFile, 'utf8')))
      const spawnStepIndex = num(raw.spawnStepIndex)
      if (
        spawnStepIndex !== undefined &&
        Number.isInteger(spawnStepIndex) &&
        spawnStepIndex >= cutoffStepIndex
      ) {
        continue
      }
    } catch {
      // Keep unreadable descriptor files as best effort.
    }
    await copyFile(srcFile, join(destDir, entry.name))
  }
}

function shouldDropMessage(
  raw: Record<string, unknown>,
  ctx: BrainFilterContext,
): boolean {
  const sourceTool = record(record(raw.sourceMetadata).tool)
  const legacyTool = record(raw.tool)
  const convId =
    str(sourceTool.conversationId) ?? str(legacyTool.conversationId)
  const stepIndex = num(sourceTool.stepIndex) ?? num(legacyTool.stepIndex)
  if (
    (!convId || convId === ctx.oldNativeId) &&
    stepIndex !== undefined &&
    Number.isInteger(stepIndex) &&
    stepIndex >= ctx.cutoffStepIndex
  ) {
    return true
  }
  if (
    typeof raw.sender === 'string' &&
    ctx.droppedSubagentIds.has(raw.sender)
  ) {
    return true
  }
  if (
    ctx.cutoffTimestampMs !== undefined &&
    typeof raw.timestamp === 'string'
  ) {
    const ts = Date.parse(raw.timestamp)
    if (Number.isFinite(ts) && ts >= ctx.cutoffTimestampMs) return true
  }
  return false
}

function rewriteMessageIdentity(
  raw: Record<string, unknown>,
  oldNativeId: string,
  newNativeId: string,
): void {
  if (raw.recipient === oldNativeId) raw.recipient = newNativeId
  if (
    typeof raw.sender === 'string' &&
    raw.sender.startsWith(`${oldNativeId}/`)
  ) {
    raw.sender = `${newNativeId}/${raw.sender.slice(oldNativeId.length + 1)}`
  }
  if (raw.sourceMetadata && typeof raw.sourceMetadata === 'object') {
    const sm = record(raw.sourceMetadata)
    if (sm.tool && typeof sm.tool === 'object') {
      const tool = record(sm.tool)
      if (tool.conversationId === oldNativeId) tool.conversationId = newNativeId
    }
  }
  if (raw.tool && typeof raw.tool === 'object') {
    const tool = record(raw.tool)
    if (tool.conversationId === oldNativeId) tool.conversationId = newNativeId
  }
}

function isWakeupCandidateMessage(raw: Record<string, unknown>): boolean {
  const sender = typeof raw.sender === 'string' ? raw.sender : ''
  const sourceTool = record(record(raw.sourceMetadata).tool)
  const toolCall = record(sourceTool.toolCall)
  const hasToolRecord =
    typeof sourceTool.conversationId === 'string' ||
    (typeof sourceTool.stepIndex === 'number' &&
      Number.isInteger(sourceTool.stepIndex)) ||
    (typeof toolCall.id === 'string' && toolCall.id.length > 0)
  const toolName =
    hasToolRecord && typeof toolCall.name === 'string' ? toolCall.name : ''
  return (
    sender !== 'user' &&
    raw.hideFromUser !== true &&
    raw.priority !== 'MESSAGE_PRIORITY_LOW' &&
    !isNativeToolName(toolName, 'invoke_subagent')
  )
}

interface MessageCandidate {
  entryName: string
  fallbackId: string
  msgId: string
  raw: Record<string, unknown>
  timestampMs: number
}

async function filterBrainMessages(
  srcDir: string,
  destDir: string,
  ctx: BrainFilterContext,
) {
  await mkdir(destDir, { recursive: true })
  const entries = await readdir(srcDir, { withFileTypes: true })
  const keptIds = new Set<string>()
  const candidates: MessageCandidate[] = []
  let hasReadJson = false

  for (const entry of entries) {
    if (!entry.isFile()) continue
    if (entry.name === 'read.json') {
      hasReadJson = true
      continue
    }
    const srcFile = join(srcDir, entry.name)
    if (!entry.name.endsWith('.json')) {
      await copyFile(srcFile, join(destDir, entry.name))
      continue
    }
    try {
      const raw = record(JSON.parse(await readFile(srcFile, 'utf8')))
      const fallbackId = entry.name.slice(0, -5)
      const msgId = typeof raw.id === 'string' && raw.id ? raw.id : fallbackId
      const timestampMs =
        typeof raw.timestamp === 'string'
          ? Date.parse(raw.timestamp)
          : Number.NaN
      candidates.push({
        entryName: entry.name,
        fallbackId,
        msgId,
        raw,
        timestampMs,
      })
    } catch {
      // Keep unreadable message files as best effort.
      keptIds.add(entry.name.slice(0, -5))
      await copyFile(srcFile, join(destDir, entry.name))
    }
  }

  candidates.sort((a, b) => {
    const aFinite = Number.isFinite(a.timestampMs)
    const bFinite = Number.isFinite(b.timestampMs)
    if (aFinite !== bFinite) return aFinite ? -1 : 1
    if (aFinite && bFinite && a.timestampMs !== b.timestampMs) {
      return a.timestampMs - b.timestampMs
    }
    return a.entryName.localeCompare(b.entryName)
  })

  for (const candidate of candidates) {
    if (shouldDropMessage(candidate.raw, ctx)) continue
    if (isWakeupCandidateMessage(candidate.raw)) {
      if (!ctx.readMessageIds.has(candidate.msgId)) continue
      const sender =
        typeof candidate.raw.sender === 'string' ? candidate.raw.sender : ''
      const remaining = ctx.keptWakeupDeliveriesBySender.get(sender) ?? 0
      if (remaining <= 0) continue
      ctx.keptWakeupDeliveriesBySender.set(sender, remaining - 1)
    }
    rewriteMessageIdentity(candidate.raw, ctx.oldNativeId, ctx.newNativeId)
    keptIds.add(candidate.fallbackId)
    keptIds.add(candidate.msgId)
    await writeFile(
      join(destDir, candidate.entryName),
      JSON.stringify(candidate.raw),
      'utf8',
    )
  }

  if (hasReadJson) {
    try {
      const readMap = record(
        JSON.parse(await readFile(join(srcDir, 'read.json'), 'utf8')),
      )
      const filtered: Record<string, unknown> = {}
      for (const [id, value] of Object.entries(readMap)) {
        if (keptIds.has(id)) filtered[id] = value
      }
      await writeFile(
        join(destDir, 'read.json'),
        JSON.stringify(filtered),
        'utf8',
      )
    } catch {
      await copyFile(join(srcDir, 'read.json'), join(destDir, 'read.json'))
    }
  }
}

async function createBrainFilterContext(
  oldBrainDir: string,
  oldNativeId: string,
  newNativeId: string,
  cutoffStepIndex: number,
): Promise<BrainFilterContext> {
  const [
    droppedSubagentIds,
    readMessageIds,
    transcriptState,
    hasTranscriptChunks,
    hasTranscriptFullChunks,
  ] = await Promise.all([
    collectDroppedSubagentIds(
      join(oldBrainDir, '.system_generated/subagents'),
      cutoffStepIndex,
    ),
    readConsumedMessageIds(
      join(oldBrainDir, '.system_generated/messages/read.json'),
    ),
    scanTranscriptState(oldBrainDir, cutoffStepIndex),
    directoryExists(
      join(oldBrainDir, '.system_generated/logs/chunks/transcript'),
    ),
    directoryExists(
      join(oldBrainDir, '.system_generated/logs/chunks/transcript_full'),
    ),
  ])
  return {
    oldNativeId,
    newNativeId,
    cutoffStepIndex,
    droppedSubagentIds,
    readMessageIds,
    keptWakeupDeliveriesBySender: transcriptState.keptWakeupDeliveriesBySender,
    cutoffTimestampMs: transcriptState.cutoffTimestampMs,
    hasTranscriptChunks,
    hasTranscriptFullChunks,
  }
}

async function copyBrainDirectoryFiltered(
  srcDir: string,
  destDir: string,
  ctx: BrainFilterContext,
  destBrainRoot = destDir,
  relParts: readonly string[] = [],
): Promise<void> {
  const entries = await readdir(srcDir, { withFileTypes: true })
  await mkdir(destDir, { recursive: true })
  for (const entry of entries) {
    if (entry.isSymbolicLink()) continue
    const srcEntry = join(srcDir, entry.name)
    const destEntry = join(destDir, entry.name)
    const nextParts = [...relParts, entry.name]
    const relPath = nextParts.join('/')

    if (
      relPath === '.system_generated/logs/chunks/transcript' ||
      relPath === '.system_generated/logs/chunks/transcript_full' ||
      relPath === '.system_generated/messages/undelivered'
    ) {
      continue
    }
    if (relPath === '.system_generated/subagents' && entry.isDirectory()) {
      await filterBrainSystemSubagents(srcEntry, destEntry, ctx.cutoffStepIndex)
      continue
    }
    if (relPath === '.system_generated/messages' && entry.isDirectory()) {
      await filterBrainMessages(srcEntry, destEntry, ctx)
      continue
    }
    if (
      relParts.length === 2 &&
      relParts[0] === '.system_generated' &&
      relParts[1] === 'steps'
    ) {
      const numericName = Number.parseInt(
        entry.name.replace(/\.[^.]+$/, ''),
        10,
      )
      if (
        Number.isInteger(numericName) &&
        String(numericName) === entry.name.replace(/\.[^.]+$/, '') &&
        numericName >= ctx.cutoffStepIndex
      ) {
        continue
      }
    }

    if (entry.isDirectory()) {
      await copyBrainDirectoryFiltered(
        srcEntry,
        destEntry,
        ctx,
        destBrainRoot,
        nextParts,
      )
    } else if (entry.isFile()) {
      const target = TRANSCRIPT_CHUNK_TARGET_BY_REL[relPath]
      if (target) {
        const filtered = await filterJsonlTranscript(
          srcEntry,
          destEntry,
          ctx.cutoffStepIndex,
        )
        if (ctx[target.flag]) {
          await writeTranscriptChunks(
            join(destBrainRoot, target.chunkDir),
            filtered,
          )
        }
      } else {
        await copyFile(srcEntry, destEntry)
      }
    }
  }
}

function tableColumnNames(db: DatabaseSync, table: string): Set<string> {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{
    name?: unknown
  }>
  return new Set(
    rows.map((r) => r.name).filter((n): n is string => typeof n === 'string'),
  )
}

function rewriteIndexedBlobRows(
  db: DatabaseSync,
  table: string,
  rows: ReadonlyArray<{ data: Buffer }>,
  hasSizeColumn = false,
): void {
  db.prepare(`DELETE FROM ${table}`).run()
  const stmt = hasSizeColumn
    ? db.prepare(`INSERT INTO ${table} (idx, data, size) VALUES (?, ?, ?)`)
    : db.prepare(`INSERT INTO ${table} (idx, data) VALUES (?, ?)`)
  rows.forEach((row, idx) => {
    if (hasSizeColumn) stmt.run(idx, row.data, row.data.byteLength)
    else stmt.run(idx, row.data)
  })
}

function branchIndexedProtoTable(
  db: DatabaseSync,
  tableName: string,
  cols: ReadonlySet<string>,
  cutoffStepIndex: number,
  filterRow: (fields: ProtoField[], raw: Buffer) => Buffer | undefined,
  options?: { trackSize?: boolean; extraRow?: Buffer },
): void {
  if (!cols.has('data')) {
    db.prepare(`DELETE FROM ${tableName} WHERE idx >= ?`).run(cutoffStepIndex)
    return
  }
  const rows = db
    .prepare(`SELECT idx, data FROM ${tableName} ORDER BY idx ASC`)
    .all() as Array<{ idx: number; data: unknown }>
  const kept: Array<{ data: Buffer }> = []
  for (const row of rows) {
    if (!(row.data instanceof Uint8Array)) continue
    const raw = Buffer.from(row.data)
    const fields = parseProtoFields(row.data)
    if (!fields) {
      if (row.idx < cutoffStepIndex) kept.push({ data: raw })
      continue
    }
    const transformed = filterRow(fields, raw)
    if (transformed) kept.push({ data: transformed })
  }
  if (options?.extraRow) kept.push({ data: options.extraRow })
  rewriteIndexedBlobRows(db, tableName, kept, options?.trackSize)
}

function branchGenMetadata(
  db: DatabaseSync,
  cols: ReadonlySet<string>,
  cutoffStepIndex: number,
): void {
  branchIndexedProtoTable(
    db,
    'gen_metadata',
    cols,
    cutoffStepIndex,
    (fields, raw) => {
      const stepIndices = decodeGenStepIndices(fields)
      const filteredSteps = stepIndices.filter((s) => s < cutoffStepIndex)
      if (filteredSteps.length === 0) return undefined
      if (filteredSteps.length === stepIndices.length) return raw
      const packed = Buffer.concat(filteredSteps.map((s) => encodeVarint(s)))
      return replaceProtoFields(
        fields,
        new Map([[2, encodeBytesField(2, packed)]]),
      )
    },
    { trackSize: cols.has('size') },
  )
}

function branchExecutorMetadata(
  db: DatabaseSync,
  cols: ReadonlySet<string>,
  cutoffStepIndex: number,
): void {
  branchIndexedProtoTable(
    db,
    'executor_metadata',
    cols,
    cutoffStepIndex,
    (fields, raw) =>
      protoVarint(fields, 3) < cutoffStepIndex ? raw : undefined,
  )
}

function branchBattleModeInfos(
  db: DatabaseSync,
  cols: ReadonlySet<string>,
  cutoffStepIndex: number,
): void {
  branchIndexedProtoTable(
    db,
    'battle_mode_infos',
    cols,
    cutoffStepIndex,
    (fields, raw) =>
      protoVarint(fields, 3) < cutoffStepIndex &&
      protoString(fields, 2).length > 0
        ? raw
        : undefined,
  )
}

function branchParentReferences(
  db: DatabaseSync,
  cols: ReadonlySet<string>,
  cutoffStepIndex: number,
  oldMeta: { trajectoryId: string; source: number; lastKeptStepType: number },
  oldNativeId: string,
): void {
  const branchParts: Buffer[] = []
  if (oldMeta.trajectoryId) {
    branchParts.push(
      encodeBytesField(1, Buffer.from(oldMeta.trajectoryId, 'utf8')),
    )
  }
  const parentStepIndex = cutoffStepIndex - 1
  if (parentStepIndex > 0)
    branchParts.push(encodeVarintField(2, parentStepIndex))
  if (oldMeta.source !== 0)
    branchParts.push(encodeVarintField(3, oldMeta.source))
  if (oldMeta.lastKeptStepType !== 0) {
    branchParts.push(encodeVarintField(4, oldMeta.lastKeptStepType))
  }
  branchParts.push(encodeVarintField(5, 2))
  branchParts.push(encodeBytesField(6, Buffer.from(oldNativeId, 'utf8')))

  branchIndexedProtoTable(
    db,
    'parent_references',
    cols,
    cutoffStepIndex,
    (fields, raw) =>
      protoVarint(fields, 2) < cutoffStepIndex ? raw : undefined,
    { extraRow: Buffer.concat(branchParts) },
  )
}

function updateTrajectoryMetadataBlob(
  db: DatabaseSync,
  oldNativeId: string,
  newNativeId: string,
): void {
  const row = db
    .prepare("SELECT data FROM trajectory_metadata_blob WHERE id = 'main'")
    .get() as { data?: unknown } | undefined
  if (!row || !(row.data instanceof Uint8Array)) return
  const fields = parseProtoFields(row.data)
  if (!fields) return
  if (protoString(fields, 6) !== oldNativeId) return
  const updated = replaceProtoFields(
    fields,
    new Map([[6, encodeBytesField(6, Buffer.from(newNativeId, 'utf8'))]]),
  )
  db.prepare(
    "UPDATE trajectory_metadata_blob SET data = ? WHERE id = 'main'",
  ).run(updated)
}

function patchRawSummaryBlob(
  raw: Uint8Array,
  remainingSteps: number,
  newTrajectoryId: string,
  lastUserStepIndex: number,
  oldNativeId: string,
  newNativeId: string,
): Buffer {
  const fields = parseProtoFields(raw)
  if (!fields) return Buffer.from(raw)
  const replacements = new Map<number, Buffer | null>([
    [2, remainingSteps > 0 ? encodeVarintField(2, remainingSteps) : null],
    [4, encodeBytesField(4, Buffer.from(newTrajectoryId, 'utf8'))],
    [
      16,
      lastUserStepIndex > 0 ? encodeVarintField(16, lastUserStepIndex) : null,
    ],
  ])
  if (protoString(fields, 19) === oldNativeId) {
    replacements.set(19, encodeBytesField(19, Buffer.from(newNativeId, 'utf8')))
  }
  return replaceProtoFields(fields, replacements)
}

async function openSummariesTable(
  appDataDir: string,
): Promise<{ db: DatabaseSync; cols: Set<string> } | undefined> {
  const summariesPath = join(appDataDir, 'conversation_summaries.db')
  const st = await lstat(summariesPath).catch(() => null)
  if (!st || st.isSymbolicLink() || !st.isFile()) return undefined
  await assertWithinDir(appDataDir, summariesPath)

  const db = new DatabaseSync(summariesPath)
  try {
    db.exec('PRAGMA busy_timeout = 3000')
    const tableRow = db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'conversation_summaries'",
      )
      .get()
    if (!tableRow) {
      db.close()
      return undefined
    }
    const cols = tableColumnNames(db, 'conversation_summaries')
    if (!cols.has('conversation_id')) {
      db.close()
      return undefined
    }
    return { db, cols }
  } catch (error) {
    db.close()
    throw error
  }
}

/** Remove on-disk artifacts and summary row created for a forked or disposable conversation. */
export async function removeForkedConversation(
  appDataDir: string,
  nativeId: string,
): Promise<void> {
  assertSafeNativeId(nativeId)
  await Promise.all([
    removeSessionArtifacts(appDataDir, nativeId).catch(() => undefined),
    (async () => {
      const opened = await openSummariesTable(appDataDir).catch(() => undefined)
      if (!opened) return
      try {
        opened.db
          .prepare(
            'DELETE FROM conversation_summaries WHERE conversation_id = ?',
          )
          .run(nativeId)
      } finally {
        opened.db.close()
      }
    })().catch(() => undefined),
  ])
}

async function syncConversationSummary(
  appDataDir: string,
  oldNativeId: string,
  newNativeId: string,
  newTrajectoryId: string,
  remainingSteps: number,
  lastUserStepIndex: number,
): Promise<void> {
  const opened = await openSummariesTable(appDataDir)
  if (!opened) return
  const { db, cols } = opened
  try {
    const oldRow = db
      .prepare('SELECT * FROM conversation_summaries WHERE conversation_id = ?')
      .get(oldNativeId) as Record<string, unknown> | undefined
    if (!oldRow) return

    const cloned: Record<string, unknown> = { ...oldRow }
    cloned.conversation_id = newNativeId
    if (cols.has('step_count') && remainingSteps >= 0) {
      cloned.step_count = remainingSteps
    }
    if (cols.has('last_modified_time')) {
      cloned.last_modified_time = new Date()
        .toISOString()
        .replace('T', ' ')
        .replace('Z', '+00:00')
    }
    if (cols.has('last_user_input_step_index') && lastUserStepIndex >= -1) {
      cloned.last_user_input_step_index = lastUserStepIndex
    }
    if (cols.has('not_fully_idle')) cloned.not_fully_idle = 0
    if (cols.has('killed')) cloned.killed = 0
    if (cols.has('raw_summary') && oldRow.raw_summary instanceof Uint8Array) {
      cloned.raw_summary = patchRawSummaryBlob(
        oldRow.raw_summary,
        Math.max(0, remainingSteps),
        newTrajectoryId,
        lastUserStepIndex,
        oldNativeId,
        newNativeId,
      )
    }
    const colList = [...cols]
    const placeholders = colList.map(() => '?').join(', ')
    const values = colList.map(
      (c) =>
        (cloned[c] as string | number | bigint | Uint8Array | null) ?? null,
    )
    db.prepare(
      `INSERT OR REPLACE INTO conversation_summaries (${colList.join(', ')}) VALUES (${placeholders})`,
    ).run(...values)
  } finally {
    db.close()
  }
}

async function conversationForkNeedsReload(
  appDataDir: string,
  nativeId: string,
  cutoffStepIndex: number,
): Promise<boolean> {
  assertSafeNativeId(nativeId)
  if (cutoffStepIndex <= 0) return false
  const conversationsDir = join(appDataDir, 'conversations')
  const oldDbPath = join(conversationsDir, `${nativeId}.db`)
  const oldDbStat = await lstat(oldDbPath).catch((error) => {
    if (isMissingError(error)) return undefined
    throw error
  })
  if (!oldDbStat) return false
  await assertWithinDir(conversationsDir, oldDbPath)
  const db = new DatabaseSync(oldDbPath, { readOnly: true })
  try {
    db.exec('PRAGMA busy_timeout = 3000')
    const hasSteps = db
      .prepare(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'steps'",
      )
      .get()
    if (!hasSteps) return true
    const countRow = db
      .prepare('SELECT COUNT(*) AS cnt FROM steps WHERE idx < ?')
      .get(cutoffStepIndex) as { cnt?: number | bigint } | undefined
    return Number(countRow?.cnt ?? 0) > 0
  } finally {
    db.close()
  }
}

/**
 * Fork the native Agy SQLite DB + Brain directory up to `cutoffStepIndex` (keeping `idx < cutoffStepIndex`),
 * or allocate a brand-new empty native session when `cutoffStepIndex <= 0`.
 */
async function forkNativeConversation(
  appDataDir: string,
  oldNativeId: string,
  cutoffStepIndex: number,
  createFreshSession: () => Promise<string>,
): Promise<string> {
  assertSafeNativeId(oldNativeId)
  if (cutoffStepIndex <= 0) {
    return await createFreshSession()
  }

  const conversationsDir = join(appDataDir, 'conversations')
  const oldDbPath = join(conversationsDir, `${oldNativeId}.db`)
  const oldDbStat = await lstat(oldDbPath).catch((error) => {
    if (isMissingError(error)) return undefined
    throw error
  })

  if (!oldDbStat) {
    // No native SQLite DB yet; only create a fresh session if there's nothing to fork.
    return await createFreshSession()
  }
  await assertWithinDir(conversationsDir, oldDbPath)

  const newNativeId = randomUUID()
  const newTrajectoryId = randomUUID()
  const targetDbPath = join(conversationsDir, `${newNativeId}.db`)
  const tempDbPath = join(
    conversationsDir,
    `${newNativeId}.db.tmp-${process.pid}`,
  )
  const newBrainDir = join(appDataDir, 'brain', newNativeId)
  let remainingSteps = -1
  let lastDbUserStepIndex = -1

  try {
    await mkdir(conversationsDir, { recursive: true })
    const sourceDb = new DatabaseSync(oldDbPath, { readOnly: true })
    try {
      sourceDb.exec('PRAGMA busy_timeout = 3000')
      const escapedTemp = tempDbPath.replaceAll("'", "''")
      sourceDb.exec(`VACUUM INTO '${escapedTemp}'`)
    } finally {
      sourceDb.close()
    }

    const tempDb = new DatabaseSync(tempDbPath)
    try {
      tempDb.exec('PRAGMA busy_timeout = 3000')
      const tableRows = tempDb
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .all() as Array<{ name: unknown }>
      const tables = new Set(
        tableRows
          .map((r) => r.name)
          .filter((n): n is string => typeof n === 'string'),
      )

      if (tables.has('steps')) {
        const countRow = tempDb
          .prepare('SELECT COUNT(*) AS cnt FROM steps WHERE idx < ?')
          .get(cutoffStepIndex) as { cnt?: number | bigint } | undefined
        remainingSteps = Number(countRow?.cnt ?? 0)
      }

      if (remainingSteps === 0) {
        tempDb.close()
        await rm(tempDbPath, { force: true })
        return await createFreshSession()
      }

      let oldTrajectoryId = ''
      let oldSource = 0
      if (tables.has('trajectory_meta')) {
        const metaCols = tableColumnNames(tempDb, 'trajectory_meta')
        const metaRow = tempDb
          .prepare('SELECT * FROM trajectory_meta LIMIT 1')
          .get() as Record<string, unknown> | undefined
        if (metaCols.has('trajectory_id')) {
          oldTrajectoryId = str(metaRow?.trajectory_id) ?? ''
        }
        if (metaCols.has('source')) {
          oldSource = num(metaRow?.source) ?? 0
        }
      }

      let lastKeptStepType = 0
      if (tables.has('steps')) {
        const stepCols = tableColumnNames(tempDb, 'steps')
        if (stepCols.has('step_type')) {
          const lastStepRow = tempDb
            .prepare(
              'SELECT step_type FROM steps WHERE idx < ? ORDER BY idx DESC LIMIT 1',
            )
            .get(cutoffStepIndex) as { step_type?: unknown } | undefined
          lastKeptStepType = num(lastStepRow?.step_type) ?? 0
          const lastUserRow = tempDb
            .prepare(
              'SELECT idx FROM steps WHERE idx < ? AND step_type = 14 ORDER BY idx DESC LIMIT 1',
            )
            .get(cutoffStepIndex) as { idx?: unknown } | undefined
          lastDbUserStepIndex = num(lastUserRow?.idx) ?? -1
        }
      }

      tempDb.exec('BEGIN IMMEDIATE')
      try {
        if (tables.has('steps')) {
          tempDb
            .prepare('DELETE FROM steps WHERE idx >= ?')
            .run(cutoffStepIndex)
        }
        if (tables.has('gen_metadata')) {
          branchGenMetadata(
            tempDb,
            tableColumnNames(tempDb, 'gen_metadata'),
            cutoffStepIndex,
          )
        }
        if (tables.has('executor_metadata')) {
          branchExecutorMetadata(
            tempDb,
            tableColumnNames(tempDb, 'executor_metadata'),
            cutoffStepIndex,
          )
        }
        if (tables.has('battle_mode_infos')) {
          branchBattleModeInfos(
            tempDb,
            tableColumnNames(tempDb, 'battle_mode_infos'),
            cutoffStepIndex,
          )
        }
        if (tables.has('parent_references')) {
          branchParentReferences(
            tempDb,
            tableColumnNames(tempDb, 'parent_references'),
            cutoffStepIndex,
            {
              trajectoryId: oldTrajectoryId,
              source: oldSource,
              lastKeptStepType,
            },
            oldNativeId,
          )
        }
        if (tables.has('trajectory_metadata_blob')) {
          updateTrajectoryMetadataBlob(tempDb, oldNativeId, newNativeId)
        }
        if (tables.has('trajectory_meta')) {
          const colNames = tableColumnNames(tempDb, 'trajectory_meta')
          if (colNames.has('cascade_id') && colNames.has('trajectory_id')) {
            tempDb
              .prepare(
                'UPDATE trajectory_meta SET cascade_id = ?, trajectory_id = ?',
              )
              .run(newNativeId, newTrajectoryId)
          } else if (colNames.has('cascade_id')) {
            tempDb
              .prepare('UPDATE trajectory_meta SET cascade_id = ?')
              .run(newNativeId)
          }
        }
        tempDb.exec('COMMIT')
      } catch (error) {
        tempDb.exec('ROLLBACK')
        throw error
      }
      tempDb.exec('PRAGMA journal_mode = DELETE')
    } finally {
      if (tempDb.isOpen) tempDb.close()
    }

    await rename(tempDbPath, targetDbPath)

    const brainDir = join(appDataDir, 'brain')
    const oldBrainDir = join(brainDir, oldNativeId)
    const oldBrainStat = await lstat(oldBrainDir).catch((error) => {
      if (isMissingError(error)) return undefined
      throw error
    })
    let retainedCutoff = cutoffStepIndex
    let lastUserStepIndex = lastDbUserStepIndex
    if (oldBrainStat?.isDirectory()) {
      await assertWithinDir(brainDir, oldBrainDir)
      const brainCtx = await createBrainFilterContext(
        oldBrainDir,
        oldNativeId,
        newNativeId,
        cutoffStepIndex,
      )
      await copyBrainDirectoryFiltered(oldBrainDir, newBrainDir, brainCtx)
      const keptRows = await readTranscriptRows(brainDir, newNativeId).catch(
        () => [] as Record<string, unknown>[],
      )
      const keptUserSteps = extractUserStepRows(keptRows)
      if (keptUserSteps.length > 0) {
        lastUserStepIndex = keptUserSteps[keptUserSteps.length - 1].stepIndex
        retainedCutoff = lastUserStepIndex
      }
    }

    await forkSessionCheckpoints(
      appDataDir,
      oldNativeId,
      newNativeId,
      retainedCutoff,
    )
    await syncConversationSummary(
      appDataDir,
      oldNativeId,
      newNativeId,
      newTrajectoryId,
      remainingSteps,
      lastUserStepIndex,
    ).catch(() => undefined)
    return newNativeId
  } catch (error) {
    await Promise.all([
      rm(tempDbPath, { force: true }).catch(() => undefined),
      rm(`${tempDbPath}-wal`, { force: true }).catch(() => undefined),
      rm(`${tempDbPath}-shm`, { force: true }).catch(() => undefined),
      rm(targetDbPath, { force: true }).catch(() => undefined),
      rm(newBrainDir, { recursive: true, force: true }).catch(() => undefined),
      removeSessionCheckpoints(appDataDir, newNativeId).catch(() => undefined),
    ])
    throw error
  }
}

export async function revertSession(options: {
  appDataDir: string
  nativeId: string
  scope: RevertScope
  token: unknown
  checkpoints?: readonly TurnFileCheckpoint[]
  supportsReload?: boolean
  deferFileRestore?: boolean
  createFreshSession: () => Promise<string>
}): Promise<{
  newNativeId?: string
  cutoffStepIndex?: number
  checkpointId?: string
}> {
  const { appDataDir, nativeId, scope, createFreshSession } = options
  const parsed = parseRevertToken(options.token)
  const brainRoot = join(appDataDir, 'brain')

  let checkpointId = parsed.checkpointId
  let cutoffStepIndex: number | undefined

  if (scope !== 'files' || checkpointId === undefined) {
    cutoffStepIndex = await resolveCutoffStepIndex(brainRoot, nativeId, parsed)
  }

  if (
    scope !== 'files' &&
    options.supportsReload === false &&
    (await conversationForkNeedsReload(appDataDir, nativeId, cutoffStepIndex!))
  ) {
    throw new Error('agy does not support session/load')
  }

  if (scope === 'files' || scope === 'both') {
    if (!checkpointId && cutoffStepIndex !== undefined) {
      const [checkpoints, rows] = await Promise.all([
        options.checkpoints ?? readSessionCheckpoints(appDataDir, nativeId),
        readTranscriptRows(brainRoot, nativeId).catch(
          () => [] as Record<string, unknown>[],
        ),
      ])
      const userSteps = extractUserStepRows(rows)
      const prevUserStep =
        userSteps.filter((u) => u.stepIndex < cutoffStepIndex!).at(-1)
          ?.stepIndex ?? -1
      const targetUserStep = userSteps.find(
        (u) => u.stepIndex >= cutoffStepIndex!,
      )?.stepIndex
      const targetStep = targetUserStep ?? parsed.stepIndex ?? cutoffStepIndex
      checkpointId = findCheckpointForUserStep(
        checkpoints,
        prevUserStep,
        targetStep,
      )?.id
    }
    if (!checkpointId) {
      throw new Error('No workspace file checkpoint recorded for this message')
    }
    // Validate checkpoint readability before forking so missing backups leave history and files intact.
    await validateSessionFileCheckpoint(appDataDir, nativeId, checkpointId)
  }

  if (scope === 'files') {
    await restoreSessionFileCheckpoint(appDataDir, nativeId, checkpointId!)
    return {}
  }

  const newNativeId = await forkNativeConversation(
    appDataDir,
    nativeId,
    cutoffStepIndex!,
    createFreshSession,
  )
  if (scope === 'both' && !options.deferFileRestore) {
    try {
      await restoreSessionFileCheckpoint(appDataDir, nativeId, checkpointId!)
    } catch (error) {
      if (newNativeId !== nativeId) {
        await removeForkedConversation(appDataDir, newNativeId).catch(
          () => undefined,
        )
      }
      throw error
    }
  }
  return {
    newNativeId,
    cutoffStepIndex: cutoffStepIndex!,
    ...(scope === 'both' && checkpointId ? { checkpointId } : {}),
  }
}

/** Attach `revertToken` to replayed `user_message` entries aligned against Brain user steps and saved checkpoints. */
export function attachReplayRevertTokens(
  entries: HistoryEntry[],
  rows: readonly Record<string, unknown>[],
  checkpoints: readonly TurnFileCheckpoint[],
): HistoryEntry[] {
  const userSteps = extractUserStepRows(rows)
  const byStep = new Map<
    number,
    { ordinal: number; stepIndex: number; text: string }
  >()
  userSteps.forEach((step, ordinal) => {
    byStep.set(step.stepIndex, { ...step, ordinal })
  })

  let fallbackCursor = 0
  return entries.map((entry) => {
    if (entry.item.type !== 'user_message') return entry
    const normalizedText = normalizeUserText(entry.item.text)
    let matchedOrdinal: number | undefined
    let matchedStepIndex: number | undefined

    const stepMatch = entry.item.id.match(/:step:(\d+)$/)
    if (stepMatch) {
      const stepNum = Number.parseInt(stepMatch[1], 10)
      const found = byStep.get(stepNum)
      if (found) {
        matchedOrdinal = found.ordinal
        matchedStepIndex = found.stepIndex
        if (found.ordinal >= fallbackCursor) fallbackCursor = found.ordinal + 1
      } else {
        matchedStepIndex = stepNum
      }
    }

    if (matchedOrdinal === undefined) {
      for (let i = fallbackCursor; i < userSteps.length; i++) {
        if (userSteps[i].text === normalizedText) {
          matchedOrdinal = i
          matchedStepIndex = userSteps[i].stepIndex
          fallbackCursor = i + 1
          break
        }
      }
    }

    if (matchedOrdinal === undefined && fallbackCursor < userSteps.length) {
      const fallback = userSteps[fallbackCursor]
      if (fallback) {
        matchedOrdinal = fallbackCursor
        matchedStepIndex = fallback.stepIndex
        fallbackCursor++
      }
    }

    let checkpointId: string | undefined
    if (matchedStepIndex !== undefined) {
      const prevUserStep =
        matchedOrdinal !== undefined && matchedOrdinal > 0
          ? userSteps[matchedOrdinal - 1].stepIndex
          : -1
      checkpointId = findCheckpointForUserStep(
        checkpoints,
        prevUserStep,
        matchedStepIndex,
      )?.id
    }

    const revertToken = makeRevertToken({
      stepIndex: matchedStepIndex,
      turnOrdinal: matchedOrdinal,
      checkpointId,
      text: normalizedText,
    })

    return {
      ...entry,
      item: {
        ...entry.item,
        revertToken,
      },
    }
  })
}
