import { randomUUID } from 'node:crypto'
import { mkdtemp, mkdir, open, readFile, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { writeFileAtomic } from './helpers.ts'

export const parent = '11111111-1111-4111-8111-111111111111'
export const child = '22222222-2222-4222-8222-222222222222'
export const grandchild = '33333333-3333-4333-8333-333333333333'
export type Row = Record<string, unknown>

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
export const encode = (rows: Row[]) =>
  rows.map((row) => JSON.stringify(row)).join('\n') + '\n'

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
