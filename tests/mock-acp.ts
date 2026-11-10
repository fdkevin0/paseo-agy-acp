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
    async initialize() {
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
      if (text !== 'background') {
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
