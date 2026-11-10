import { AgentSideConnection, ndJsonStream } from '@agentclientprotocol/sdk'
import { randomUUID } from 'node:crypto'
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { WritableStream } from 'node:stream/web'
import { Readable, Writable } from 'node:stream'
let nativeId = ''
function log(method: string) {
  if (process.env.AGY_TEST_LOG)
    appendFileSync(process.env.AGY_TEST_LOG, method + '\n')
}
const configOptions = [
  {
    id: 'model',
    name: 'Model',
    type: 'select' as const,
    currentValue: 'test',
    options: [{ value: 'test', name: 'Test' }],
  },
  {
    id: 'thought_level',
    name: 'Thinking',
    type: 'select' as const,
    currentValue: 'low',
    options: ['low', 'high'].map((value) => ({ name: value, value })),
  },
  {
    id: 'mode',
    name: 'Mode',
    category: 'mode' as const,
    type: 'select' as const,
    currentValue: 'default',
    options: [
      { value: 'default', name: 'Default' },
      { value: 'accept-edits', name: 'Accept edits' },
      { value: 'plan', name: 'Plan' },
    ],
  },
]
const canceled = new Set<string>()
const cancels = new Map<string, () => void>()
let wokenStepBase = 0
/** Sessions waiting on a woken ask_question, keyed to their transcript step base. */
const wokenQuestions = new Map<string, number>()
const readWokenMsgIds: Record<string, boolean> = {}
if (process.argv.includes('--dangerously-skip-permissions'))
  log('flag:--dangerously-skip-permissions')
const stream = ndJsonStream(
  Writable.toWeb(process.stdout),
  Readable.toWeb(process.stdin),
)
const output = stream.writable.getWriter()
stream.writable = new WritableStream({
  write(message) {
    // Emulate agy's legacy field, which the fixture SDK would otherwise strip.
    if (
      'result' in message &&
      message.result &&
      typeof message.result === 'object' &&
      'sessionId' in message.result
    ) {
      message = {
        ...message,
        result: {
          ...message.result,
          models: { availableModels: [{ modelId: 'test-low' }] },
          _meta: {
            antigravity: {
              tier: { id: 'g1-ultra-tier', name: 'Google AI Ultra' },
            },
          },
        },
      }
    }
    if (
      process.env.AGY_TEST_BAD_PERMISSION &&
      'method' in message &&
      message.method === 'session/request_permission'
    )
      message = { ...message, params: { sessionId: nativeId, options: null } }
    return output.write(message)
  },
})
new AgentSideConnection((client) => {
  // agy's question forms (array items with oneOf, no `mode`) are not SDK-typed elicitation requests.
  const elicit = (params: Record<string, unknown>) => {
    if (process.env.AGY_TEST_LOG_ELICITATION_CAP) log('elicit')
    return client.createElicitation(
      params as unknown as Parameters<typeof client.createElicitation>[0],
    )
  }
  async function staleEvents(sessionId: string) {
    for (const update of [
      {
        sessionUpdate: 'agent_message_chunk' as const,
        content: { type: 'text' as const, text: 'stale' },
      },
      {
        sessionUpdate: 'available_commands_update' as const,
        availableCommands: [{ name: 'stale', description: 'stale' }],
      },
      { sessionUpdate: 'usage_update' as const, used: 999, size: 1000 },
      { sessionUpdate: 'config_option_update' as const, configOptions: [] },
    ])
      await client.sessionUpdate({ sessionId, update })
    const result = await client.requestPermission({
      sessionId,
      toolCall: { toolCallId: 'stale', title: 'stale' },
      options: [{ optionId: 'yes', name: 'Allow', kind: 'allow_once' }],
    })
    log(`stale-permission:${result.outcome.outcome}`)
  }
  function sendCommands(sessionId: string) {
    if (process.env.AGY_TEST_NO_COMMANDS) return
    setTimeout(async () => {
      if (process.env.AGY_TEST_STALE_EVENTS) await staleEvents(randomUUID())
      await client.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: 'available_commands_update',
          availableCommands: [
            { name: 'compact', description: 'Compact context' },
          ],
        },
      })
    }, 20)
  }
  return {
    async initialize(params) {
      if (
        process.env.AGY_TEST_LOG_ELICITATION_CAP &&
        params.clientCapabilities?.elicitation?.form
      )
        log('cap:elicitation.form')
      return {
        protocolVersion: 1,
        agentCapabilities: {
          loadSession: true,
          sessionCapabilities: {
            list: {},
            ...(process.env.AGY_TEST_RESUME_CAP ? { resume: {} } : {}),
          },
        },
      }
    },
    async authenticate() {
      return {}
    },
    async newSession(params) {
      const previousId = nativeId || randomUUID()
      nativeId = randomUUID()
      const append = (
        params._meta?.antigravity as { systemPrompt?: { append?: string } }
      )?.systemPrompt?.append
      log(append ? `new:${append}` : 'new')
      if (process.env.AGY_TEST_STALE_EVENTS) await staleEvents(previousId)
      sendCommands(nativeId)
      return { sessionId: nativeId, configOptions }
    },
    async closeSession({ sessionId }) {
      log('close')
      if (process.env.AGY_TEST_STALE_EVENTS) await staleEvents(sessionId)
      if (process.env.AGY_TEST_CLOSE_ERROR) throw new Error('close failed')
      return {}
    },
    async loadSession({ sessionId, _meta }) {
      const append = (
        _meta?.antigravity as { systemPrompt?: { append?: string } }
      )?.systemPrompt?.append
      if (append) log(`load:${append}`)
      else if (process.env.AGY_TEST_LOG_LOAD) log('load')
      sendCommands(sessionId)
      await client.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: 'user_message_chunk',
          content: { type: 'text', text: 'old question' },
        },
      })
      await client.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: 'agent_message_chunk',
          content: { type: 'text', text: 'old answer' },
        },
      })
      await client.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: 'tool_call',
          toolCallId: 'child',
          title: 'invoke_subagent',
          status: 'completed',
          rawOutput: { conversationId: '22222222-2222-4222-8222-222222222222' },
        },
      })
      return { configOptions }
    },
    async resumeSession({ sessionId, _meta }) {
      const append = (
        _meta?.antigravity as { systemPrompt?: { append?: string } }
      )?.systemPrompt?.append
      log(append ? `resume:${append}` : 'resume')
      sendCommands(sessionId)
      return { configOptions }
    },
    async listSessions({ cwd }) {
      return {
        sessions: [
          {
            sessionId: 'native-test',
            cwd: cwd ?? process.cwd(),
            title: 'Test session',
            _meta: { description: 'Last prompt preview' },
          },
        ],
      }
    },
    async setSessionConfigOption({ configId, value }) {
      log(`configure:${configId}=${value}`)
      return {
        configOptions: configOptions.map((o) =>
          o.id === configId ? { ...o, currentValue: String(value) } : o,
        ),
      }
    },
    async cancel({ sessionId }) {
      if (process.env.AGY_TEST_LOG_LOAD) log('cancel')
      canceled.add(sessionId)
      cancels.get(sessionId)?.()
    },
    async prompt({ sessionId, prompt }) {
      const text = prompt.map((c) => (c.type === 'text' ? c.text : '')).join('')
      if (process.env.AGY_TEST_LOG_ELICITATION_CAP) log(`prompt:${text}`)
      if (text === 'message-refresh-boundary') {
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'tool_call',
            toolCallId: 'message-refresh',
            title: 'send_message',
            status: 'in_progress',
            rawInput: {
              Recipient: '22222222-2222-4222-8222-222222222222',
              Message: 'continue',
            },
          },
        })
        return { stopReason: 'end_turn' }
      }
      if (text === 'must-not-run-after-interrupt') {
        log('unexpected-prompt')
        return { stopReason: 'end_turn' as const }
      }
      if (text === 'after-background') {
        const logs = join(
          process.env.HOME ?? '/tmp',
          '.gemini/antigravity-cli/brain',
          sessionId,
          '.system_generated/logs',
        )
        const file = join(logs, 'transcript_full.jsonl')
        appendFileSync(
          file,
          [
            {
              step_index: 7,
              type: 'USER_INPUT',
              content: '<USER_REQUEST>after-background</USER_REQUEST>',
            },
            {
              step_index: 8,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Next turn reply.',
              input_tokens: 20,
              output_tokens: 3,
            },
          ]
            .map((r) => JSON.stringify(r) + '\n')
            .join(''),
        )
        // agy replays post-RPC Reactive Wakeup steps from Turn 1 before emitting Turn 2's user_message_chunk.
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_thought_chunk',
            content: { type: 'text', text: 'Analyzing scan output' },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'tool_call',
            toolCallId: 'stale-wakeup-tool',
            title: 'manage_subagents',
            status: 'completed',
            rawInput: { Action: 'list' },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: 'Scan complete: 42G.' },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'user_message_chunk',
            content: { type: 'text', text },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: 'Next turn reply.' },
          },
        })
        return {
          stopReason: 'end_turn' as const,
          usage: {
            inputTokens: 60,
            outputTokens: 8,
            totalTokens: 78,
            cachedReadTokens: 10,
          },
        }
      }
      await client.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: 'user_message_chunk',
          content: { type: 'text', text },
        },
      })
      if (
        text !== 'background' &&
        text !== 'woken-question' &&
        !wokenQuestions.has(sessionId)
      ) {
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'tool_call',
            toolCallId: 'unfinished',
            title: 'run_command',
            status: 'in_progress',
            rawInput: { CommandLine: 'echo unfinished' },
          },
        })
      }
      if (text === 'disconnect') {
        setInterval(() => {}, 1000)
        process.stdout.end()
        await new Promise(() => {})
      }
      if (text === 'crash') {
        process.exit(7)
      }
      if (text === 'background') {
        const sys = join(
          process.env.HOME ?? '/tmp',
          '.gemini/antigravity-cli/brain',
          sessionId,
          '.system_generated',
        )
        const logs = join(sys, 'logs')
        const messagesDir = join(sys, 'messages')
        mkdirSync(logs, { recursive: true })
        mkdirSync(messagesDir, { recursive: true })
        const file = join(logs, 'transcript_full.jsonl')
        const taskId = `${sessionId}/task-3`
        writeFileSync(
          file,
          [
            {
              step_index: 1,
              type: 'USER_INPUT',
              content: '<USER_REQUEST>background</USER_REQUEST>',
            },
            {
              step_index: 2,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Scanning disk...',
              tool_calls: [
                {
                  id: 'bg-scan',
                  name: 'run_command',
                  args: { CommandLine: 'du -sh /home' },
                },
              ],
            },
            {
              step_index: 3,
              type: 'GENERIC',
              status: 'RUNNING',
              content: `Tool is running as a background task with task id: ${taskId}\n`,
            },
            {
              step_index: 4,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Waiting for background scan to finish...',
            },
          ]
            .map((r) => JSON.stringify(r) + '\n')
            .join(''),
        )
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'tool_call',
            toolCallId: 'bg-scan',
            title: 'run_command',
            status: 'in_progress',
            rawInput: { CommandLine: 'du -sh /home' },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: {
              type: 'text',
              text: 'Waiting for background scan to finish...',
            },
          },
        })
        setTimeout(() => {
          const now = new Date().toISOString()
          writeFileSync(
            join(messagesDir, 'msg-scan.json'),
            JSON.stringify({
              id: 'msg-scan',
              sender: taskId,
              recipient: sessionId,
              priority: 'MESSAGE_PRIORITY_HIGH',
              content: `Task id "${taskId}" finished with result:\nCommand exited with code 0.\n\nCommand output:\n42G\t/home\nLog: file:///tmp/task-scan.log`,
              timestamp: now,
              sourceMetadata: {
                tool: {
                  conversationId: sessionId,
                  stepIndex: 3,
                  toolCall: {
                    id: 'bg-scan',
                    name: 'run_command',
                    argumentsJson: '{"CommandLine":"du -sh /home"}',
                  },
                },
              },
            }),
          )
          writeFileSync(
            join(messagesDir, 'read.json'),
            JSON.stringify({ 'msg-scan': true }),
          )
          appendFileSync(
            file,
            [
              {
                step_index: 5,
                type: 'SYSTEM_MESSAGE',
                content: `<SYSTEM_MESSAGE>\n[Message] timestamp=${now} sender=${taskId} priority=MESSAGE_PRIORITY_HIGH content=Task id "${taskId}" finished with result:\nCommand exited with code 0.\n\nCommand output:\n42G\t/home\nLog: file:///tmp/task-scan.log\n</SYSTEM_MESSAGE>`,
              },
              {
                step_index: 6,
                type: 'PLANNER_RESPONSE',
                status: 'DONE',
                thinking: 'Analyzing scan output',
                content: 'Scan complete: 42G.',
                input_tokens: 40,
                cache_read_tokens: 10,
                output_tokens: 5,
              },
            ]
              .map((r) => JSON.stringify(r) + '\n')
              .join(''),
          )
        }, 80)
        return {
          stopReason: 'end_turn' as const,
          usage: {
            inputTokens: 10,
            outputTokens: 2,
            totalTokens: 12,
            cachedReadTokens: 0,
          },
        }
      }
      if (text === 'mcp-preapproved') {
        const lazy = await client.requestPermission({
          sessionId,
          toolCall: {
            toolCallId: 'mcp-lazy',
            title: 'call_mcp_tool',
            rawInput: { ServerName: 'rea', ToolName: 'open_binary' },
          },
          options: [
            { optionId: 'always', name: 'Always', kind: 'allow_always' },
            { optionId: 'yes', name: 'Allow', kind: 'allow_once' },
            { optionId: 'no', name: 'Deny', kind: 'reject_once' },
          ],
        })
        const direct = await client.requestPermission({
          sessionId,
          toolCall: {
            toolCallId: 'mcp-direct',
            title: 'mcp__rea__open_binary',
            rawInput: { path: '/bin/ls' },
          },
          options: [
            { optionId: 'always', name: 'Always', kind: 'allow_always' },
            { optionId: 'yes', name: 'Allow', kind: 'allow_once' },
            { optionId: 'no', name: 'Deny', kind: 'reject_once' },
          ],
        })
        const fmt = (r: typeof lazy) =>
          r.outcome.outcome === 'selected'
            ? r.outcome.optionId
            : r.outcome.outcome
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: {
              type: 'text',
              text: `mcp:${fmt(lazy)},${fmt(direct)}`,
            },
          },
        })
        return { stopReason: 'end_turn' as const }
      }
      if (text === 'elicitation') {
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'tool_call',
            toolCallId: 'ask-fg',
            title: 'default_api:ask_question',
            status: 'in_progress',
            rawInput: {
              questions: [
                {
                  question: 'Which approach?',
                  options: ['(Recommended) Option 1', 'Option 2'],
                  is_multi_select: false,
                },
                {
                  question: 'Which platforms?',
                  options: ['Linux, x86_64', 'macOS'],
                  is_multi_select: true,
                },
              ],
            },
          },
        })
        const response = await elicit({
          message: 'The agent has a question.',
          requestedSchema: {
            type: 'object',
            properties: {
              q1: {
                type: 'string',
                title: 'Which approach?',
                oneOf: [
                  { const: '1', title: '(Recommended) Option 1' },
                  { const: '2', title: 'Option 2' },
                ],
              },
              q1_other: {
                type: 'string',
                title: 'Other',
                description: 'Optional free-form answer to: Which approach?',
              },
              q2: {
                type: 'array',
                title: 'Which platforms?',
                items: {
                  anyOf: [
                    { const: '1', title: 'Linux, x86_64' },
                    { const: '2', title: 'macOS' },
                  ],
                },
              },
              q2_other: {
                type: 'string',
                title: 'Other',
                description: 'Optional free-form answer to: Which platforms?',
              },
            },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'tool_call_update',
            toolCallId: 'ask-fg',
            status: 'completed',
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: {
              type: 'text',
              text: `elicited:${JSON.stringify(response)}`,
            },
          },
        })
        return { stopReason: 'end_turn' as const }
      }
      if (text === 'double-elicitation') {
        const base = wokenStepBase
        wokenStepBase += 20
        const logs = join(
          process.env.HOME ?? '/tmp',
          '.gemini/antigravity-cli/brain',
          sessionId,
          '.system_generated/logs',
        )
        mkdirSync(logs, { recursive: true })
        const file = join(logs, 'transcript_full.jsonl')
        for (const [idx, title] of [
          'First question?',
          'Second question?',
        ].entries()) {
          await client.sessionUpdate({
            sessionId,
            update: {
              sessionUpdate: 'tool_call',
              toolCallId: `ask-dbl-${base}-${idx}`,
              title: 'default_api:ask_question',
              status: 'in_progress',
              rawInput: {
                questions: [
                  {
                    question: title,
                    options: ['Yes', 'No'],
                    is_multi_select: false,
                  },
                ],
              },
            },
          })
        }
        const r1 = await elicit({
          message: 'Question 1',
          requestedSchema: {
            type: 'object',
            properties: {
              q1: {
                type: 'string',
                title: 'First question?',
                oneOf: [
                  { const: '0', title: 'Yes' },
                  { const: '1', title: 'No' },
                ],
              },
            },
          },
        })
        const r2 = await elicit({
          message: 'Question 2',
          requestedSchema: {
            type: 'object',
            properties: {
              q1: {
                type: 'string',
                title: 'Second question?',
                oneOf: [
                  { const: '0', title: 'Yes' },
                  { const: '1', title: 'No' },
                ],
              },
            },
          },
        })
        // Intentionally omit tool_call_update(completed) and GENERIC rows to verify
        // handledElicitations matches both questions without re-prompting and completes both running cards.
        appendFileSync(
          file,
          [
            {
              step_index: base + 1,
              type: 'USER_INPUT',
              content: '<USER_REQUEST>double-elicitation</USER_REQUEST>',
            },
            {
              step_index: base + 2,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: `double:${JSON.stringify([r1, r2])}`,
              tool_calls: [
                {
                  id: `ask-dbl-${base}-0`,
                  name: 'default_api:ask_question',
                  args: {
                    questions: [
                      {
                        question: 'First question?',
                        options: ['Yes', 'No'],
                        is_multi_select: false,
                      },
                    ],
                  },
                },
                {
                  id: `ask-dbl-${base}-1`,
                  name: 'default_api:ask_question',
                  args: {
                    questions: [
                      {
                        question: 'Second question?',
                        options: ['Yes', 'No'],
                        is_multi_select: false,
                      },
                    ],
                  },
                },
              ],
            },
          ]
            .map((r) => JSON.stringify(r) + '\n')
            .join(''),
        )
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: {
              type: 'text',
              text: `double:${JSON.stringify([r1, r2])}`,
            },
          },
        })
        return { stopReason: 'end_turn' as const }
      }
      if (text === 'ask-permission-binary') {
        const decision = await client.requestPermission({
          sessionId,
          toolCall: {
            toolCallId: 'ask-bin',
            title: 'default_api:ask_question',
            rawInput: {
              questions: [{ question: 'Proceed?', options: ['Yes', 'No'] }],
            },
          },
          options: [
            { optionId: 'allow_once', name: 'Yes', kind: 'allow_once' },
            { optionId: 'reject_once', name: 'No', kind: 'reject_once' },
          ],
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: {
              type: 'text',
              text: `ask-bin:${JSON.stringify(decision.outcome)}`,
            },
          },
        })
        return { stopReason: 'end_turn' as const }
      }
      if (text === 'woken-native-elicitation') {
        const base = wokenStepBase
        wokenStepBase += 20
        const sys = join(
          process.env.HOME ?? '/tmp',
          '.gemini/antigravity-cli/brain',
          sessionId,
          '.system_generated',
        )
        const logs = join(sys, 'logs')
        const messagesDir = join(sys, 'messages')
        mkdirSync(logs, { recursive: true })
        mkdirSync(messagesDir, { recursive: true })
        const file = join(logs, 'transcript_full.jsonl')
        const taskId = `${sessionId}/task-woken-native-${base}`
        const msgId = `msg-native-${base}`
        const now = new Date().toISOString()
        writeFileSync(
          join(messagesDir, `${msgId}.json`),
          JSON.stringify({
            id: msgId,
            sender: taskId,
            recipient: sessionId,
            priority: 'MESSAGE_PRIORITY_HIGH',
            content: `Task id "${taskId}" finished with result:\nCommand exited with code 0.\n\nCommand output:\nready\nLog: file:///tmp/task-native.log`,
            timestamp: now,
          }),
        )
        readWokenMsgIds[msgId] = true
        writeFileSync(
          join(messagesDir, 'read.json'),
          JSON.stringify(readWokenMsgIds),
        )
        // Write PLANNER_RESPONSE (ask_question without GENERIC) first so Brain's onQuestion pops the card before elicitation arrives.
        appendFileSync(
          file,
          [
            {
              step_index: base + 1,
              type: 'USER_INPUT',
              content: '<USER_REQUEST>woken-native-elicitation</USER_REQUEST>',
            },
            {
              step_index: base + 2,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Starting background check...',
              tool_calls: [
                {
                  id: `bg-native-${base}`,
                  name: 'run_command',
                  args: { CommandLine: 'check-env' },
                },
              ],
            },
            {
              step_index: base + 3,
              type: 'GENERIC',
              status: 'RUNNING',
              content: `Tool is running as a background task with task id: ${taskId}\n`,
            },
            {
              step_index: base + 4,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Waiting for background check...',
            },
            {
              step_index: base + 5,
              type: 'SYSTEM_MESSAGE',
              content: `<SYSTEM_MESSAGE>\n[Message] timestamp=${now} sender=${taskId} priority=MESSAGE_PRIORITY_HIGH content=Task id "${taskId}" finished with result:\nCommand exited with code 0.\n\nCommand output:\nready\nLog: file:///tmp/task-native.log\n</SYSTEM_MESSAGE>`,
            },
            {
              step_index: base + 6,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Background check finished. Please select:',
              tool_calls: [
                {
                  id: `woken-native-ask-${base}`,
                  name: 'default_api:ask_question',
                  args: {
                    questions: [
                      {
                        question: 'Which option?',
                        options: ['(Recommended) Option 1', 'Option 2'],
                        is_multi_select: false,
                      },
                    ],
                  },
                },
              ],
            },
          ]
            .map((r) => JSON.stringify(r) + '\n')
            .join(''),
        )
        setTimeout(() => {
          void (async () => {
            // Send elicitation/create after Brain's onQuestion has already popped the card.
            const response = await elicit({
              message: 'The agent has a question.',
              requestedSchema: {
                type: 'object',
                properties: {
                  q1: {
                    type: 'string',
                    title: 'Which option?',
                    oneOf: [
                      { const: '1', title: '(Recommended) Option 1' },
                      { const: '2', title: 'Option 2' },
                    ],
                  },
                },
              },
            })
            appendFileSync(
              file,
              [
                {
                  step_index: base + 7,
                  type: 'GENERIC',
                  status: 'DONE',
                  content:
                    'Created At: 2026-11-10T18:59:59+08:00\nCompleted At: 2026-11-10T19:00:05+08:00\nA1: (Recommended) Option 1',
                },
                {
                  step_index: base + 8,
                  type: 'PLANNER_RESPONSE',
                  status: 'DONE',
                  content: `Native woken:${JSON.stringify(response)}`,
                },
              ]
                .map((r) => JSON.stringify(r) + '\n')
                .join(''),
            )
          })()
        }, 30)
        return { stopReason: 'end_turn' as const }
      }
      if (text === 'woken-question') {
        const base = wokenStepBase
        wokenStepBase += 20
        wokenQuestions.set(sessionId, base)
        const sys = join(
          process.env.HOME ?? '/tmp',
          '.gemini/antigravity-cli/brain',
          sessionId,
          '.system_generated',
        )
        const logs = join(sys, 'logs')
        const messagesDir = join(sys, 'messages')
        mkdirSync(logs, { recursive: true })
        mkdirSync(messagesDir, { recursive: true })
        const file = join(logs, 'transcript_full.jsonl')
        const taskId = `${sessionId}/task-woken-q-${base}`
        const msgId = `msg-q-${base}`
        appendFileSync(
          file,
          [
            {
              step_index: base + 1,
              type: 'USER_INPUT',
              content: '<USER_REQUEST>woken-question</USER_REQUEST>',
            },
            {
              step_index: base + 2,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Starting background check...',
              tool_calls: [
                {
                  id: `bg-q-${base}`,
                  name: 'run_command',
                  args: { CommandLine: 'check-env' },
                },
              ],
            },
            {
              step_index: base + 3,
              type: 'GENERIC',
              status: 'RUNNING',
              content: `Tool is running as a background task with task id: ${taskId}\n`,
            },
            {
              step_index: base + 4,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Waiting for background check...',
            },
          ]
            .map((r) => JSON.stringify(r) + '\n')
            .join(''),
        )
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'tool_call',
            toolCallId: `bg-q-${base}`,
            title: 'run_command',
            status: 'in_progress',
            rawInput: { CommandLine: 'check-env' },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: {
              type: 'text',
              text: 'Waiting for background check...',
            },
          },
        })
        setTimeout(() => {
          const now = new Date().toISOString()
          writeFileSync(
            join(messagesDir, `${msgId}.json`),
            JSON.stringify({
              id: msgId,
              sender: taskId,
              recipient: sessionId,
              priority: 'MESSAGE_PRIORITY_HIGH',
              content: `Task id "${taskId}" finished with result:\nCommand exited with code 0.\n\nCommand output:\nready\nLog: file:///tmp/task-q.log`,
              timestamp: now,
            }),
          )
          readWokenMsgIds[msgId] = true
          writeFileSync(
            join(messagesDir, 'read.json'),
            JSON.stringify(readWokenMsgIds),
          )
          appendFileSync(
            file,
            [
              {
                step_index: base + 5,
                type: 'SYSTEM_MESSAGE',
                content: `<SYSTEM_MESSAGE>\n[Message] timestamp=${now} sender=${taskId} priority=MESSAGE_PRIORITY_HIGH content=Task id "${taskId}" finished with result:\nCommand exited with code 0.\n\nCommand output:\nready\nLog: file:///tmp/task-q.log\n</SYSTEM_MESSAGE>`,
              },
              {
                step_index: base + 6,
                type: 'PLANNER_RESPONSE',
                status: 'DONE',
                content: 'Background check finished. Please select:',
                tool_calls: [
                  {
                    id: `woken-ask-${base}`,
                    name: 'default_api:ask_question',
                    args: {
                      questions: [
                        {
                          question: 'Which option?',
                          options: ['(Recommended) Option 1', 'Option 2'],
                          is_multi_select: false,
                        },
                      ],
                    },
                  },
                ],
              },
            ]
              .map((r) => JSON.stringify(r) + '\n')
              .join(''),
          )
        }, 50)
        return { stopReason: 'end_turn' as const }
      }
      const wokenBase = wokenQuestions.get(sessionId)
      if (wokenBase !== undefined) {
        // The prompt is the answer to the woken ask_question.
        wokenQuestions.delete(sessionId)
        const base = wokenBase
        const logs = join(
          process.env.HOME ?? '/tmp',
          '.gemini/antigravity-cli/brain',
          sessionId,
          '.system_generated/logs',
        )
        const file = join(logs, 'transcript_full.jsonl')
        // For Option 1, replay the matching question (with real agy's 1-based const IDs);
        // for Option 2, ask a different question to verify preStagedQuestion does not mis-answer it.
        const replayed = await elicit({
          message: 'The agent has a question.',
          requestedSchema: {
            type: 'object',
            properties: {
              q1: {
                type: 'string',
                title:
                  text === 'Option 2'
                    ? 'Different follow-up question?'
                    : 'Which option?',
                oneOf: [
                  { const: '1', title: '(Recommended) Option 1' },
                  { const: '2', title: 'Option 2' },
                ],
              },
              q1_other: {
                type: 'string',
                title: 'Other',
                description: 'Optional free-form answer to: Which option?',
              },
            },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: {
              type: 'text',
              text: 'Background check finished. Please select:',
            },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'user_message_chunk',
            content: { type: 'text', text },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: {
              type: 'text',
              text: `Completed woken question:${JSON.stringify(replayed)}`,
            },
          },
        })
        appendFileSync(
          file,
          [
            {
              step_index: base + 7,
              type: 'GENERIC',
              status: 'DONE',
              content: `Created At: 2026-11-10T18:59:59+08:00\nCompleted At: 2026-11-10T19:00:05+08:00\nA1: ${text}`,
            },
            {
              step_index: base + 8,
              type: 'USER_INPUT',
              content: `<USER_REQUEST>${text}</USER_REQUEST>`,
            },
            ...(text === 'Option 2'
              ? [
                  {
                    step_index: base + 9,
                    type: 'PLANNER_RESPONSE',
                    status: 'DONE',
                    content: `Completed woken question:${JSON.stringify(replayed)}`,
                    tool_calls: [
                      {
                        id: `woken-followup-${base}`,
                        name: 'default_api:ask_question',
                        args: {
                          questions: [
                            {
                              question: 'Different follow-up question?',
                              options: ['(Recommended) Option 1', 'Option 2'],
                              is_multi_select: false,
                            },
                          ],
                        },
                      },
                    ],
                  },
                ]
              : [
                  {
                    step_index: base + 9,
                    type: 'PLANNER_RESPONSE',
                    status: 'DONE',
                    content: `Completed woken question:${JSON.stringify(replayed)}`,
                  },
                ]),
          ]
            .map((r) => JSON.stringify(r) + '\n')
            .join(''),
        )
        return { stopReason: 'end_turn' as const }
      }
      if (text === 'chooser') {
        const choice = await client.requestPermission({
          sessionId,
          toolCall: {
            toolCallId: 'ask',
            title: 'ask_question',
            rawInput: { question: 'Which environment?' },
          },
          options: [
            { optionId: 'staging', name: 'Staging', kind: 'allow_once' },
            { optionId: 'prod', name: 'Production', kind: 'allow_once' },
            { optionId: 'cancel', name: 'Cancel', kind: 'reject_once' },
          ],
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: {
              type: 'text',
              text:
                choice.outcome.outcome === 'selected'
                  ? `choice:${choice.outcome.optionId}`
                  : choice.outcome.outcome,
            },
          },
        })
        return { stopReason: 'end_turn' as const }
      }
      if (text === 'wait') {
        if (process.env.AGY_TEST_CANCEL_DELAY)
          await new Promise((resolve) => setTimeout(resolve, 100))
        await new Promise<void>((resolve) => {
          if (canceled.has(sessionId)) resolve()
          else cancels.set(sessionId, resolve)
        })
        cancels.delete(sessionId)
        canceled.delete(sessionId)
        return { stopReason: 'cancelled' as const }
      }
      const permission = await client.requestPermission({
        sessionId,
        toolCall: {
          toolCallId: 'shell',
          title: 'run_command',
          rawInput: { CommandLine: 'echo test' },
        },
        options: [
          { optionId: 'always', name: 'Always', kind: 'allow_always' },
          { optionId: 'yes', name: 'Allow', kind: 'allow_once' },
          { optionId: 'no', name: 'Deny', kind: 'reject_once' },
        ],
      })
      await client.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: 'tool_call',
          toolCallId: 'shell',
          title: 'run_command',
          kind: 'execute',
          status: 'in_progress',
          rawInput: { CommandLine: 'echo test' },
        },
      })
      await client.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: 'tool_call_update',
          toolCallId: 'shell',
          status: 'completed',
          rawOutput: 'test',
        },
      })
      await client.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: 'agent_message_chunk',
          content: {
            type: 'text',
            text:
              permission.outcome.outcome === 'selected'
                ? `selected:${permission.outcome.optionId}`
                : permission.outcome.outcome,
          },
        },
      })
      if (!process.env.AGY_TEST_NO_CONTEXT)
        void client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'usage_update',
            used: 10,
            size: 1000,
            cost: { amount: 0.01, currency: 'USD' },
          },
        })
      return {
        stopReason: 'end_turn' as const,
        usage: {
          inputTokens: 12,
          outputTokens: 3,
          totalTokens: 15,
          cachedReadTokens: 4,
        },
      }
    },
  }
}, stream)
