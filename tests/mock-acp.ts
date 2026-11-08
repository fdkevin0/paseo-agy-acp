import { AgentSideConnection, ndJsonStream } from '@agentclientprotocol/sdk'
import { appendFileSync } from 'node:fs'
import { WritableStream } from 'node:stream/web'
import { Readable, Writable } from 'node:stream'
const nativeId = '11111111-1111-4111-8111-111111111111'
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
]
let cancel: (() => void) | undefined
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
  let sessions = 0
  function sendCommands(sessionId: string) {
    if (process.env.AGY_TEST_NO_COMMANDS) return
    const extra =
      process.env.AGY_TEST_WARM_COMMANDS && sessions > 1
        ? [{ name: 'plan', description: 'Plan task' }]
        : []
    setTimeout(() => {
      void client.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: 'available_commands_update',
          availableCommands: [
            { name: 'compact', description: 'Compact context' },
            ...extra,
          ],
        },
      })
    }, 20)
  }
  return {
    async initialize() {
      return {
        protocolVersion: 1,
        agentCapabilities: {
          loadSession: true,
          sessionCapabilities: { list: {} },
        },
      }
    },
    async authenticate() {
      return {}
    },
    async newSession() {
      sessions += 1
      log('new')
      sendCommands(nativeId)
      return { sessionId: nativeId, configOptions }
    },
    async closeSession() {
      log('close')
      if (process.env.AGY_TEST_CLOSE_ERROR) throw new Error('close failed')
      return {}
    },
    async loadSession({ sessionId }) {
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
    async listSessions() {
      return {
        sessions: [
          {
            sessionId: 'native-test',
            cwd: process.cwd(),
            title: 'Test session',
          },
        ],
      }
    },
    async setSessionConfigOption() {
      log('configure')
      return { configOptions }
    },
    async cancel() {
      cancel?.()
    },
    async prompt({ sessionId, prompt }) {
      const text = prompt.map((c) => (c.type === 'text' ? c.text : '')).join('')
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
          sessionUpdate: 'tool_call',
          toolCallId: 'unfinished',
          title: 'run_command',
          status: 'in_progress',
          rawInput: { CommandLine: 'echo unfinished' },
        },
      })
      if (text === 'crash') {
        process.exit(7)
      }
      if (text === 'wait') {
        await new Promise<void>((resolve) => {
          cancel = resolve
        })
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
          content: { type: 'text', text: permission.outcome.outcome },
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
