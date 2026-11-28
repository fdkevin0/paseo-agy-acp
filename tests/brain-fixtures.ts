import { randomUUID } from 'node:crypto'
import { mkdtemp, mkdir, open, readFile, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { writeFileAtomic } from './helpers.ts'

export const parent = '11111111-1111-4111-8111-111111111111'
export const child = '22222222-2222-4222-8222-222222222222'
export const grandchild = '33333333-3333-4333-8333-333333333333'
export type Row = Record<string, unknown>

export const userRow = (step_index: number, content = 'work'): Row => ({
  step_index,
  type: 'USER_INPUT',
  content,
})

export const modelRow = (
  step_index: number,
  input_tokens: number,
  content = 'model text',
): Row => ({
  step_index,
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  input_tokens,
  output_tokens: 1,
  content,
})

export const checkpointRow = (step_index: number, extra: Row = {}): Row => ({
  step_index,
  type: 'CHECKPOINT',
  status: 'DONE',
  ...extra,
})

export const done = (step_index: number, content = 'finished'): Row => ({
  step_index,
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  content,
})
export const message = (
  step_index: number,
  sender: string,
  content: string,
): Row => ({
  step_index,
  type: 'SYSTEM_MESSAGE',
  content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-08T17:00:00Z sender=${sender} priority=MESSAGE_PRIORITY_HIGH content=${content}\n</SYSTEM_MESSAGE>`,
})
export const invoke = (id: string): Row[] => [
  { step_index: 1, type: 'USER_INPUT', content: 'work' },
  {
    step_index: 2,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [{ id: 'invoke', name: 'invoke_subagent', args: {} }],
  },
  {
    step_index: 3,
    type: 'GENERIC',
    status: 'DONE',
    content: JSON.stringify({ conversationId: id }),
  },
  done(4, 'waiting'),
]
export const report = (recipient: string, text: string, start = 1): Row[] => [
  {
    step_index: start,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [
      {
        id: `report-${start}`,
        name: 'send_message',
        args: {
          Recipient: recipient,
          Message: text,
        },
      },
    ],
  },
  {
    step_index: start + 1,
    type: 'GENERIC',
    status: 'DONE',
    content: 'Message sent',
  },
  done(start + 2, 'I sent the report.'),
]
export const encode = (rows: readonly Row[]) =>
  rows.map((row) => JSON.stringify(row) + '\n').join('')

function encodeVarint(value: number): Buffer {
  const bytes: number[] = []
  let v = BigInt(value)
  while (v >= 0x80n) {
    bytes.push(Number(v & 0x7fn) | 0x80)
    v >>= 7n
  }
  bytes.push(Number(v))
  return Buffer.from(bytes)
}

export function encodeProtoField(
  fieldNumber: number,
  payload: number | Uint8Array,
): Buffer {
  if (typeof payload === 'number') {
    return Buffer.concat([
      encodeVarint(fieldNumber << 3),
      encodeVarint(payload),
    ])
  }
  return Buffer.concat([
    encodeVarint((fieldNumber << 3) | 2),
    encodeVarint(payload.length),
    payload,
  ])
}

/** Encode a protobuf `gen_metadata` payload containing `context_window_usage` fields. */
export function encodeGenMetadataContextUsage(fields: {
  maxTokens?: number
  estimatedTokens?: number
}): Buffer {
  const inner: Buffer[] = []
  if (fields.estimatedTokens !== undefined)
    inner.push(encodeProtoField(1, fields.estimatedTokens))
  if (fields.maxTokens !== undefined)
    inner.push(encodeProtoField(4, fields.maxTokens))
  return encodeProtoField(
    1,
    encodeProtoField(9, encodeProtoField(10, Buffer.concat(inner))),
  )
}

/** Encode a protobuf `gen_metadata` payload with only `max_context_tokens`. */
export function encodeGenMetadataMaxContext(maxTokens: number): Buffer {
  return encodeGenMetadataContextUsage({ maxTokens })
}

/** Encode a protobuf `CortexStep` `step_payload` for `step_type = 17` (`[24, 3, ...]`). */
export function encodeStepErrorPayload(fields: {
  userError?: string
  shortError?: string
  fullError?: string
  errorId?: string
  retryInfoJson?: string
  terminalFlag?: boolean
}): Buffer {
  const errFields: Buffer[] = []
  if (fields.userError !== undefined)
    errFields.push(encodeProtoField(1, Buffer.from(fields.userError, 'utf8')))
  if (fields.shortError !== undefined)
    errFields.push(encodeProtoField(2, Buffer.from(fields.shortError, 'utf8')))
  if (fields.fullError !== undefined)
    errFields.push(encodeProtoField(3, Buffer.from(fields.fullError, 'utf8')))
  if (fields.errorId !== undefined)
    errFields.push(encodeProtoField(6, Buffer.from(fields.errorId, 'utf8')))
  if (fields.retryInfoJson !== undefined)
    errFields.push(
      encodeProtoField(10, Buffer.from(fields.retryInfoJson, 'utf8')),
    )
  const stepErrorFields: Buffer[] = [
    encodeProtoField(3, Buffer.concat(errFields)),
  ]
  if (fields.terminalFlag) stepErrorFields.push(encodeProtoField(5, 1))
  // Include leading step metadata envelope bytes matching real agy 1.5.18 SQLite rows.
  return Buffer.concat([
    encodeProtoField(
      1,
      Buffer.from('e6b81e75-87d0-4ef7-9d3c-3174267c5a46', 'utf8'),
    ),
    encodeProtoField(24, Buffer.concat(stepErrorFields)),
  ])
}

/** A `brain` directory holding per-session transcripts, subagent records and inbox files. */
export async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'agy-brain-test-'))
  const path = (id: string) =>
    join(root, id, '.system_generated/logs/transcript_full.jsonl')
  async function put(id: string, rows: Row[]) {
    await mkdir(join(root, id, '.system_generated/logs'), { recursive: true })
    await writeFile(path(id), encode(rows))
  }
  /** Rewrite like agy's WriteFullTranscript: O_TRUNC on the same inode, optionally pausing while empty. */
  async function rewrite(
    id: string,
    rows: Row[],
    whileEmpty?: () => Promise<void>,
  ) {
    const file = await open(path(id), 'r+')
    try {
      await file.truncate(0)
      await whileEmpty?.()
      await file.write(encode(rows), 0)
    } finally {
      await file.close()
    }
  }
  async function subagent(
    parentId: string,
    childId: string,
    state = 'SUBAGENT_STATE_ALIVE',
    spawnStepIndex = 3,
    spawnCallId = 'invoke',
  ) {
    const dir = join(root, parentId, '.system_generated/subagents')
    await writeFileAtomic(
      join(dir, `${childId}.json`),
      JSON.stringify({
        conversationId: childId,
        subagentDescriptor: {
          typeName: 'research',
          role: 'Researcher',
        },
        spawnStepIndex,
        state,
      }),
    )
    await deliver(childId, parentId, 'work', true, 'init', {
      conversationId: parentId,
      stepIndex: spawnStepIndex,
      callId: spawnCallId,
      name: 'invoke_subagent',
    })
  }
  async function deliver(
    recipientId: string,
    sender: string,
    content: string,
    read = true,
    msgId: string = randomUUID(),
    tool?: {
      conversationId: string
      stepIndex: number
      callId: string
      name: string
    },
  ) {
    const dir = join(root, recipientId, '.system_generated/messages')
    await writeFileAtomic(
      join(dir, `${msgId}.json`),
      JSON.stringify({
        id: msgId,
        sender,
        recipient: recipientId,
        priority: 'MESSAGE_PRIORITY_HIGH',
        content,
        timestamp: new Date().toISOString(),
        ...(tool
          ? {
              sourceMetadata: {
                tool: {
                  conversationId: tool.conversationId,
                  stepIndex: tool.stepIndex,
                  toolCall: {
                    id: tool.callId,
                    name: tool.name,
                    argumentsJson: '{}',
                  },
                },
              },
            }
          : {}),
      }),
    )
    const readPath = join(dir, 'read.json')
    const current = await readFile(readPath, 'utf8').then(
      (raw) => JSON.parse(raw) as Record<string, boolean>,
      () => ({}) as Record<string, boolean>,
    )
    if (read) current[msgId] = true
    else delete current[msgId]
    await writeFileAtomic(readPath, JSON.stringify(current))
    return msgId
  }
  return {
    root,
    path,
    put,
    rewrite,
    subagent,
    deliver,
    close: () => rm(root, { recursive: true, force: true }),
  }
}
