import { AgentSideConnection, ndJsonStream } from '@agentclientprotocol/sdk'
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { Readable, Writable } from 'node:stream'

const nativeId = '33333333-3333-4333-8333-333333333333'
const home = process.env.HOME!
const fullAccess = process.argv.includes('--dangerously-skip-permissions')
const log = (event: string, data: object = {}) =>
  appendFileSync(
    join(home, 'runtime.jsonl'),
    JSON.stringify({ event, pid: process.pid, ...data }) + '\n',
  )
const configOptions = [
  ['model', 'small', ['small', 'large']],
  ['thought_level', 'low', ['low', 'high']],
  ['mode', 'default', ['default', 'accept-edits', 'plan']],
].map(([id, currentValue, options]) => ({
  id: id as string,
  name: id as string,
  type: 'select' as const,
  currentValue: currentValue as string,
  options: (options as string[]).map((value) => ({ value, name: value })),
}))
const directory = join(
  home,
  '.gemini/antigravity-cli/brain',
  nativeId,
  '.system_generated/logs',
)
mkdirSync(directory, { recursive: true })
const transcript = join(directory, 'transcript_full.jsonl')
const rows = (value: object[], append = true) => {
  const text = value.map((row) => JSON.stringify(row) + '\n').join('')
  if (append) appendFileSync(transcript, text)
  else writeFileSync(transcript, text)
}
log('start', { fullAccess })
process.stdin.on('end', () => {
  log('exit')
  process.exit(0)
})
new AgentSideConnection(
  (client) => {
    let probeLatePermissionOnCancel = false
    const update = (
      update: Parameters<typeof client.sessionUpdate>[0]['update'],
    ) => client.sessionUpdate({ sessionId: nativeId, update })
    const commands = () =>
      setTimeout(
        () =>
          void update({
            sessionUpdate: 'available_commands_update',
            availableCommands: [],
          }),
        0,
      )
    return {
      async initialize() {
        return { protocolVersion: 1, agentCapabilities: { loadSession: true } }
      },
      async authenticate() {
        return {}
      },
      async newSession(params) {
        log('new', { params, nativeId })
        commands()
        return { sessionId: nativeId, configOptions }
      },
      async loadSession(params) {
        log('load', { params, nativeId: params.sessionId })
        await update({
          sessionUpdate: 'user_message_chunk',
          content: { type: 'text', text: 'old prompt' },
        })
        await update({
          sessionUpdate: 'agent_message_chunk',
          content: { type: 'text', text: 'HISTORY_REPLAY' },
        })
        await new Promise((resolve) => setTimeout(resolve, 40))
        commands()
        return { configOptions }
      },
      async setSessionConfigOption(params) {
        if (typeof params.value !== 'string')
          throw new Error('Expected select option')
        configOptions.find(
          (option) => option.id === params.configId,
        )!.currentValue = params.value
        if (
          probeLatePermissionOnCancel &&
          params.configId === 'thought_level'
        ) {
          probeLatePermissionOnCancel = false
          const permission = await client.requestPermission({
            sessionId: nativeId,
            toolCall: {
              toolCallId: 'late-permission',
              title: 'late-probe',
              rawInput: {},
            },
            options: [{ optionId: 'allow', name: 'Allow', kind: 'allow_once' }],
          })
          log('late-permission', permission)
        }
        return { configOptions }
      },
      async closeSession() {
        return {}
      },
      // Like native agy, cancel ends the model turn but leaves background work alive.
      async cancel() {
        log('cancel')
      },
      async prompt(params) {
        const text = params.prompt
          .flatMap((block) => (block.type === 'text' ? [block.text] : []))
          .join('')
        log('prompt', {
          text,
          nativeId: params.sessionId,
          config: Object.fromEntries(
            configOptions.map((option) => [option.id, option.currentValue]),
          ),
          fullAccess,
        })
        await update({
          sessionUpdate: 'user_message_chunk',
          content: { type: 'text', text },
        })
        if (text === 'retry-foreground-active') {
          rows(
            [
              { step_index: 0, type: 'USER_INPUT', content: text },
              {
                step_index: 1,
                type: 'ERROR_MESSAGE',
                status: 'DONE',
                source: 'SYSTEM',
                created_at: '2020-01-01T00:00:00Z',
                error:
                  'API error (attempt 1): request failed: synthetic timeout',
              },
            ],
            false,
          )
          log('held-foreground')
          await new Promise((resolve) => setTimeout(resolve, 150))
          rows([
            {
              step_index: 2,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              thinking: 'Recovered silently',
            },
          ])
        } else if (text === 'background' || text === 'retry-background') {
          rows(
            [
              { step_index: 0, type: 'USER_INPUT', content: text },
              {
                step_index: 1,
                type: 'PLANNER_RESPONSE',
                status: 'DONE',
                tool_calls: [
                  {
                    name: 'run_command',
                    args: { CommandLine: 'background work' },
                  },
                ],
              },
              {
                step_index: 2,
                type: 'GENERIC',
                status: 'RUNNING',
                content:
                  'Tool is running as a background task with task id: task-1',
              },
              {
                step_index: 3,
                type: 'PLANNER_RESPONSE',
                status: 'DONE',
                content: 'WAITING',
              },
            ],
            false,
          )
          if (text === 'retry-background')
            rows([
              {
                step_index: 4,
                type: 'ERROR_MESSAGE',
                status: 'DONE',
                source: 'SYSTEM',
                created_at: '2020-01-01T00:00:00Z',
                error:
                  'API error (attempt 8): request failed: synthetic timeout',
              },
            ])
          await update({
            sessionUpdate: 'tool_call',
            toolCallId: 'call-background',
            title: 'background work',
            rawInput: { CommandLine: 'background work' },
            status: 'in_progress',
          })
          setTimeout(() => {
            log('leaked-background')
            writeFileSync(join(home, 'old-result'), 'OLD_RESULT')
            void update({
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: 'OLD_RESULT' },
            })
          }, 700)
          await update({
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: 'WAITING' },
          })
        } else if (text === 'error-step') {
          rows(
            [
              { step_index: 0, type: 'USER_INPUT', content: text },
              {
                step_index: 1,
                type: 'ERROR_MESSAGE',
                content: 'Model quota exceeded',
              },
            ],
            false,
          )
        } else if (text === 'stall') {
          probeLatePermissionOnCancel = true
          rows(
            [
              { step_index: 2, type: 'USER_INPUT', content: text },
              {
                step_index: 3,
                type: 'PLANNER_RESPONSE',
                status: 'RUNNING',
              },
            ],
            true,
          )
        } else {
          if (fullAccess) {
            const permission = await client.requestPermission({
              sessionId: nativeId,
              toolCall: {
                toolCallId: 'permission',
                title: 'probe',
                rawInput: {},
              },
              options: [
                { optionId: 'allow', name: 'Allow', kind: 'allow_once' },
              ],
            })
            log('permission', permission)
          }
          rows([
            { step_index: 4, type: 'USER_INPUT', content: text },
            {
              step_index: 5,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'NEW_REPLY',
            },
          ])
          await update({
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: 'NEW_REPLY' },
          })
        }
        return { stopReason: 'end_turn' as const }
      },
    }
  },
  ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin)),
)
