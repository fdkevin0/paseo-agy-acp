import { AgentSideConnection, ndJsonStream } from '@agentclientprotocol/sdk'
import {
  appendFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  watch,
  writeFileSync,
} from 'node:fs'
import { join } from 'node:path'
import { Readable, Writable } from 'node:stream'

const nativeId = '33333333-3333-4333-8333-333333333333'
const home = process.env.HOME!
const fullAccess = process.argv.includes('--dangerously-skip-permissions')
const resumeCap = process.env.AGY_RESUME_CAP === '1'
const gateDir = process.env.AGY_RELOAD_GATE
const awaitReloadGate = async (kind: 'load' | 'resume') => {
  if (!gateDir) return
  mkdirSync(gateDir, { recursive: true })
  writeFileSync(join(gateDir, 'entered'), kind)
  if (existsSync(join(gateDir, 'release'))) return
  await new Promise<void>((resolve) => {
    const watcher = watch(gateDir, () => {
      if (existsSync(join(gateDir, 'release'))) {
        watcher.close()
        resolve()
      }
    })
    if (existsSync(join(gateDir, 'release'))) {
      watcher.close()
      resolve()
    }
  })
}
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
const nextStepIndex = (floor = 4) => {
  if (!existsSync(transcript)) return floor
  let max = floor - 1
  for (const line of readFileSync(transcript, 'utf8').split('\n')) {
    if (!line.trim()) continue
    try {
      const row = JSON.parse(line) as { step_index?: unknown }
      if (Number.isInteger(row.step_index) && Number(row.step_index) > max)
        max = Number(row.step_index)
    } catch {}
  }
  return max + 1
}
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
    let cancelHang: (() => void) | undefined
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
        return {
          protocolVersion: 1,
          agentCapabilities: {
            loadSession: true,
            ...(resumeCap ? { sessionCapabilities: { resume: {} } } : {}),
          },
        }
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
        await awaitReloadGate('load')
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
      async resumeSession(params) {
        log('resume', { params, nativeId: params.sessionId })
        await awaitReloadGate('resume')
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
        cancelHang?.()
        cancelHang = undefined
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
        } else if (text === 'stale-everything') {
          const staleChild = '44444444-4444-4444-8444-444444444444'
          const brainBase = join(
            home,
            '.gemini/antigravity-cli/brain',
            nativeId,
          )
          const subagentsDir = join(brainBase, '.system_generated/subagents')
          mkdirSync(subagentsDir, { recursive: true })
          writeFileSync(
            join(subagentsDir, `${staleChild}.json`),
            JSON.stringify({
              conversationId: staleChild,
              subagentDescriptor: { typeName: 'research', role: 'Researcher' },
              spawnStepIndex: 1,
              state: 'SUBAGENT_STATE_ALIVE',
            }),
          )
          const childLogs = join(
            home,
            '.gemini/antigravity-cli/brain',
            staleChild,
            '.system_generated/logs',
          )
          mkdirSync(childLogs, { recursive: true })
          writeFileSync(
            join(childLogs, 'transcript_full.jsonl'),
            [
              {
                step_index: 0,
                type: 'SYSTEM_MESSAGE',
                content: `<SYSTEM_MESSAGE>\n1. [Message] sender=${nativeId} priority=MESSAGE_PRIORITY_HIGH content=stale work\n</SYSTEM_MESSAGE>`,
              },
              { step_index: 1, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
            ]
              .map((r) => JSON.stringify(r) + '\n')
              .join(''),
          )
          const msgDir = join(brainBase, '.system_generated/messages')
          mkdirSync(msgDir, { recursive: true })
          writeFileSync(
            join(msgDir, 'msg-unread.json'),
            JSON.stringify({
              id: 'msg-unread',
              sender: staleChild,
              recipient: nativeId,
              priority: 'MESSAGE_PRIORITY_HIGH',
              content: 'stale unread',
              timestamp: new Date().toISOString(),
            }),
          )
          writeFileSync(
            join(msgDir, 'msg-deficit.json'),
            JSON.stringify({
              id: 'msg-deficit',
              sender: staleChild,
              recipient: nativeId,
              priority: 'MESSAGE_PRIORITY_HIGH',
              content: 'stale deficit',
              timestamp: new Date().toISOString(),
            }),
          )
          writeFileSync(
            join(msgDir, 'read.json'),
            JSON.stringify({ 'msg-deficit': true }),
          )
          rows(
            [
              { step_index: 0, type: 'USER_INPUT', content: text },
              {
                step_index: 1,
                type: 'PLANNER_RESPONSE',
                status: 'DONE',
                tool_calls: [
                  {
                    id: 'bg-stale',
                    name: 'run_command',
                    args: { CommandLine: 'bg stale' },
                  },
                  {
                    id: 'cron-stale',
                    name: 'schedule',
                    args: {
                      CronExpression: '0 * * * *',
                      IsDaemon: true,
                      Prompt: 'tick',
                    },
                  },
                  {
                    id: 'inv-stale',
                    name: 'invoke_subagent',
                    args: { Role: 'Researcher' },
                  },
                ],
              },
              {
                step_index: 2,
                type: 'GENERIC',
                status: 'RUNNING',
                content: `Tool is running as a background task with task id: ${nativeId}/task-stale\n`,
              },
              {
                step_index: 3,
                type: 'GENERIC',
                status: 'RUNNING',
                content: `Tool is running as a background task with task id: ${nativeId}/cron-stale\n`,
              },
              {
                step_index: 4,
                type: 'GENERIC',
                status: 'DONE',
                content: `Launched subagent ${staleChild}`,
              },
              {
                step_index: 5,
                type: 'PLANNER_RESPONSE',
                status: 'DONE',
                content: 'WAITING_STALE',
              },
            ],
            false,
          )
          await update({
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: 'WAITING_STALE' },
          })
        } else if (text === 'hang') {
          const step = nextStepIndex(4)
          rows([{ step_index: step, type: 'USER_INPUT', content: text }])
          await update({
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: 'HANGING' },
          })
          await new Promise<void>((resolve) => {
            cancelHang = resolve
          })
          return { stopReason: 'cancelled' as const }
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
          const step = nextStepIndex(4)
          rows([
            { step_index: step, type: 'USER_INPUT', content: text },
            {
              step_index: step + 1,
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
