import { AgentSideConnection, ndJsonStream } from '@agentclientprotocol/sdk'
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { Readable, Writable } from 'node:stream'

const nativeId = '11111111-1111-4111-8111-111111111111'
const childId = '22222222-2222-4222-8222-222222222222'
const scenario = process.env.AGY_COMPAT_SCENARIO!
type Row = Record<string, unknown>
const user = (step_index: number): Row => ({
  step_index,
  type: 'USER_INPUT',
  content: 'test',
})
const childInit = (parentId: string, prompt = 'work'): Row => ({
  step_index: 0,
  type: 'SYSTEM_MESSAGE',
  content: `<SYSTEM_MESSAGE>\n[Message] timestamp=${new Date().toISOString()} sender=${parentId} priority=MESSAGE_PRIORITY_HIGH content=${prompt}\n</SYSTEM_MESSAGE>`,
})
const done = (step_index: number, content = 'done'): Row => ({
  step_index,
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  content,
})
const call = (step_index: number, name: string, args: object): Row => ({
  step_index,
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  tool_calls: [{ name, args }],
})
const result = (step_index: number, content: string, status = 'DONE'): Row => ({
  step_index,
  type: 'GENERIC',
  status,
  content,
})
function sysDir(id: string, sub: string) {
  const dir = join(
    process.env.HOME!,
    '.gemini/antigravity-cli/brain',
    id,
    '.system_generated',
    sub,
  )
  mkdirSync(dir, { recursive: true })
  return dir
}
function transcript(id: string, rows: Row[], append = false) {
  const directory = sysDir(id, 'logs')
  const text = rows.map((row) => JSON.stringify(row) + '\n').join('')
  const path = join(directory, 'transcript_full.jsonl')
  if (append) appendFileSync(path, text)
  else writeFileSync(path, text)
}
function writeSubagent(
  parentId: string,
  subId: string,
  state:
    'SUBAGENT_STATE_ALIVE' | 'SUBAGENT_STATE_KILLED' = 'SUBAGENT_STATE_ALIVE',
  spawnStepIndex = 2,
  callId = 'call_234567890',
) {
  writeFileSync(
    join(sysDir(parentId, 'subagents'), `${subId}.json`),
    JSON.stringify({
      conversationId: subId,
      subagentDescriptor: {
        typeName: 'research',
        role: 'Researcher',
      },
      spawnStepIndex,
      state,
    }),
  )
  writeFileSync(
    join(sysDir(subId, 'messages'), 'init.json'),
    JSON.stringify({
      id: 'init',
      sender: parentId,
      recipient: subId,
      priority: 'MESSAGE_PRIORITY_HIGH',
      content: 'work',
      timestamp: new Date().toISOString(),
      sourceMetadata: {
        tool: {
          conversationId: parentId,
          stepIndex: spawnStepIndex,
          toolCall: {
            id: callId,
            name: 'invoke_subagent',
            argumentsJson: '{}',
          },
        },
      },
    }),
  )
  writeFileSync(
    join(sysDir(subId, 'messages'), 'read.json'),
    JSON.stringify({ init: true }),
  )
}
function writeInboxMessage(
  recipientId: string,
  msgId: string,
  sender: string,
  content: string,
  read = true,
  toolMeta?: { stepIndex: number; id: string; name: string },
) {
  const dir = sysDir(recipientId, 'messages')
  writeFileSync(
    join(dir, `${msgId}.json`),
    JSON.stringify({
      id: msgId,
      sender,
      recipient: recipientId,
      priority: 'MESSAGE_PRIORITY_HIGH',
      content,
      timestamp: new Date().toISOString(),
      ...(toolMeta
        ? {
            sourceMetadata: {
              tool: {
                conversationId: recipientId,
                stepIndex: toolMeta.stepIndex,
                toolCall: {
                  id: toolMeta.id,
                  name: toolMeta.name,
                  argumentsJson: '{}',
                },
              },
            },
          }
        : {}),
    }),
  )
  const readPath = join(dir, 'read.json')
  let current: Record<string, boolean> = {}
  try {
    current = JSON.parse(readFileSync(readPath, 'utf8')) as Record<
      string,
      boolean
    >
  } catch {
    current = {}
  }
  if (read) current[msgId] = true
  else delete current[msgId]
  writeFileSync(readPath, JSON.stringify(current))
}
new AgentSideConnection(
  (client) => {
    const update = (
      update: Parameters<typeof client.sessionUpdate>[0]['update'],
    ) => client.sessionUpdate({ sessionId: nativeId, update })
    async function tool(toolCallId: string, title: string, rawInput: object) {
      await update({
        sessionUpdate: 'tool_call',
        toolCallId,
        title,
        rawInput,
        status: 'in_progress',
      })
      await update({
        sessionUpdate: 'tool_call_update',
        toolCallId,
        status: 'completed',
      })
    }
    function commands() {
      setTimeout(
        () =>
          void update({
            sessionUpdate: 'available_commands_update',
            availableCommands: [],
          }),
        0,
      )
    }
    async function invoke() {
      const input = { Prompt: 'work', Role: 'Child' }
      transcript(childId, [childInit(nativeId), done(1)])
      writeSubagent(nativeId, childId, 'SUBAGENT_STATE_ALIVE', 2)
      transcript(nativeId, [
        user(0),
        call(1, 'invoke_subagent', input),
        result(
          2,
          `Created the following subagent:\n{"conversationId":"${childId}"}`,
        ),
        done(3),
      ])
      await tool('call_234567890', 'Run invoke_subagent', input)
    }
    return {
      async initialize() {
        return { protocolVersion: 1, agentCapabilities: { loadSession: true } }
      },
      async authenticate() {
        return {}
      },
      async newSession() {
        // Existing ownership lets the cancel scenarios exercise send_message on an old child.
        if (scenario.startsWith('kill')) {
          writeSubagent(nativeId, childId, 'SUBAGENT_STATE_ALIVE', 2)
          transcript(nativeId, [
            user(0),
            call(1, 'invoke_subagent', { Prompt: 'work' }),
            result(
              2,
              `Created the following subagent:\n{"conversationId":"${childId}"}`,
            ),
            done(3),
          ])
        }
        commands()
        return { sessionId: nativeId }
      },
      async loadSession() {
        await update({
          sessionUpdate: 'user_message_chunk',
          content: { type: 'text', text: 'old question' },
        })
        await invoke()
        if (scenario === 'history-kill') {
          transcript(childId, [
            childInit(nativeId),
            done(1, 'Saved child reply'),
            call(2, 'run_command', { CommandLine: 'sleep 60' }),
          ])
          writeSubagent(nativeId, childId, 'SUBAGENT_STATE_KILLED', 2)
          const input = { Action: 'kill_all' }
          transcript(
            nativeId,
            [
              user(4),
              call(5, 'manage_subagents', input),
              result(6, 'Killed subagent'),
              done(7),
            ],
            true,
          )
          await tool('call_345678901', 'Run manage_subagents', input)
        }
        commands()
        return {}
      },
      async cancel() {},
      async closeSession() {
        return {}
      },
      async prompt() {
        await update({
          sessionUpdate: 'user_message_chunk',
          content: { type: 'text', text: 'test' },
        })
        if (scenario === 'background' || scenario === 'parallel') {
          const reply = 'waiting'
          const input = { CommandLine: 'sleep 1', WaitMsBeforeAsync: 1000 }
          const calls =
            scenario === 'parallel'
              ? [
                  { id: 'call_123456789', output: 'first' },
                  { id: 'call_987654321', output: 'second' },
                ]
              : [{ id: 'call_123456789', output: 'output' }]
          transcript(nativeId, [
            user(0),
            {
              ...call(1, 'run_command', input),
              tool_calls: calls.map(() => ({
                name: 'run_command',
                args: input,
              })),
            },
            ...calls.map((_, index) =>
              result(
                index + 2,
                `Tool is running as a background task with task id: ${nativeId}/task-${index + 2}`,
                'RUNNING',
              ),
            ),
            done(calls.length + 2, reply),
          ])
          for (const { id } of calls)
            await update({
              sessionUpdate: 'tool_call',
              toolCallId: id,
              title: 'sleep 1',
              status: 'in_progress',
              rawInput: input,
            })
          await update({
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: reply },
          })
          setTimeout(() => {
            calls.forEach(({ id, output }, index) => {
              const stepIdx = index + 2
              const taskId = `${nativeId}/task-${stepIdx}`
              writeInboxMessage(
                nativeId,
                `msg-task-${index}`,
                taskId,
                `Task id "${taskId}" finished with result:\n${output}`,
                true,
                { stepIndex: stepIdx, id, name: 'run_command' },
              )
            })
            transcript(
              nativeId,
              [
                ...calls
                  .map(({ output }, index) => {
                    const taskId = `${nativeId}/task-${index + 2}`
                    return {
                      step_index: calls.length * 2 + 2 - index,
                      type: 'SYSTEM_MESSAGE',
                      content: `<SYSTEM_MESSAGE>\n[Message] timestamp=${new Date().toISOString()} sender=${taskId} priority=MESSAGE_PRIORITY_HIGH content=Task id "${taskId}" finished with result:\n${output}\n</SYSTEM_MESSAGE>`,
                    }
                  })
                  .reverse(),
                done(calls.length * 2 + 3, 'done'),
              ],
              true,
            )
          }, 100)
        } else if (scenario === 'child') {
          await invoke()
        } else {
          const input = { Recipient: childId, Message: 'continue' }
          const action =
            scenario === 'kill-all'
              ? { Action: 'kill_all' }
              : { Action: 'kill', ConversationIds: [childId] }
          writeSubagent(nativeId, childId, 'SUBAGENT_STATE_KILLED', 2)
          transcript(childId, [
            childInit(nativeId),
            call(1, 'run_command', { CommandLine: 'work' }),
          ])
          transcript(
            nativeId,
            [
              user(4),
              call(5, 'send_message', input),
              result(6, 'Message sent'),
              call(7, 'manage_subagents', action),
              result(8, 'Killed subagents'),
              done(9),
            ],
            true,
          )
          await tool('call_345678901', 'Run send_message', input)
          await tool('call_456789012', 'Run manage_subagents', action)
        }
        return { stopReason: 'end_turn' }
      },
    }
  },
  ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin)),
)
