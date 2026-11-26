import {
  agent,
  methods,
  ndJsonStream,
  type AgentConnection,
  type CreateElicitationResponse,
  type RequestPermissionRequest,
  type SessionNotification,
} from '@agentclientprotocol/sdk/unstable'
import { randomUUID } from 'node:crypto'
import {
  appendFileSync,
  existsSync,
  mkdirSync,
  unlinkSync,
  writeFileSync,
  readFileSync,
} from 'node:fs'
import { basename, join } from 'node:path'
import { WritableStream } from 'node:stream/web'
import { Readable, Writable } from 'node:stream'
import { MessageChannel } from 'node:worker_threads'
import recordedWakeup from './fixtures/agy-1.5.12-wakeup.json' with { type: 'json' }

let nativeId = ''
let lastSessionCwd = ''
let afterPromptResponse: (() => Promise<void>) | undefined
let startRecordedWakeup: (() => Promise<void>) | undefined
let writeRecordedWakeupRows: (() => void) | undefined
let finishRecordedReads: (() => Promise<void>) | undefined
if (process.env.AGY_TEST_PID_FILE)
  writeFileSync(process.env.AGY_TEST_PID_FILE, String(process.pid))
if (process.env.AGY_TEST_PID_LOG)
  appendFileSync(process.env.AGY_TEST_PID_LOG, `${process.pid}\n`)
function logRpcPid(method: string) {
  if (process.env.AGY_TEST_RPC_PID_LOG)
    appendFileSync(
      process.env.AGY_TEST_RPC_PID_LOG,
      `${process.pid}:${method}\n`,
    )
}
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
    options: [
      { value: 'test', name: 'Test' },
      { value: 'gpt-oss-120b-medium', name: 'GPT-OSS 120B' },
    ],
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
const earlyCancelGates = new Map<string, () => void>()
let wokenStepBase = 0
let holdSeq = 0
function writeSessionTranscript(
  sessionId: string,
  body: string | object[],
  mode: 'write' | 'append' = 'append',
) {
  const dir = join(
    process.env.HOME!,
    '.gemini/antigravity-cli/brain',
    sessionId,
    '.system_generated/logs',
  )
  mkdirSync(dir, { recursive: true })
  const target = join(dir, 'transcript_full.jsonl')
  const encoded =
    typeof body === 'string'
      ? body
      : body.map((row) => JSON.stringify(row) + '\n').join('')
  if (mode === 'write') writeFileSync(target, encoded)
  else appendFileSync(target, encoded)
}
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
      const cwdTag =
        process.env.AGY_TEST_CATALOG_BY_CWD && lastSessionCwd
          ? basename(lastSessionCwd)
          : undefined
      message = {
        ...message,
        result: {
          ...message.result,
          models: {
            availableModels: [{ modelId: `${cwdTag ?? 'test'}-low` }],
          },
          _meta: {
            antigravity: {
              tier: {
                id: cwdTag ? `tier-${cwdTag}` : 'g1-ultra-tier',
                name: cwdTag ? `Tier ${cwdTag}` : 'Google AI Ultra',
              },
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
    const afterResponse =
      'result' in message &&
      typeof message.result === 'object' &&
      message.result !== null &&
      'stopReason' in message.result
        ? afterPromptResponse
        : undefined
    if (afterResponse) afterPromptResponse = undefined
    return output.write(message).then(() => {
      void afterResponse?.().catch((error) => {
        console.error(error)
        process.exitCode = 1
        process.stdin.destroy()
      })
    })
  },
})
let connection!: AgentConnection
const client = {
  sessionUpdate: (params: SessionNotification) =>
    connection.client.notify(methods.client.session.update, params),
  requestPermission: (params: RequestPermissionRequest) =>
    connection.client.request(methods.client.session.requestPermission, params),
  createElicitation: (params: Record<string, unknown>) =>
    connection.client.request<CreateElicitationResponse>(
      methods.client.elicitation.create,
      params,
    ),
}
// agy's question forms (array items with oneOf, no `mode`) are not SDK-typed elicitation requests.
const elicit = (params: Record<string, unknown>) => {
  if (process.env.AGY_TEST_LOG_ELICITATION_CAP) log('elicit')
  return client.createElicitation(params)
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
  setImmediate(async () => {
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
  })
}
connection = agent()
  .onRequest(methods.agent.initialize, async ({ params }) => {
    logRpcPid('initialize')
    if (
      process.env.AGY_TEST_LOG_ELICITATION_CAP &&
      params.clientCapabilities?.elicitation?.form
    )
      log('cap:elicitation.form')
    const version = process.argv.includes('--dangerously-skip-permissions')
      ? (process.env.AGY_TEST_RELAUNCH_VERSION ?? '1.5.16')
      : (process.env.AGY_TEST_VERSION ?? '1.5.16')
    return {
      protocolVersion: 1,
      ...(version === 'missing'
        ? {}
        : { agentInfo: { name: 'Antigravity ACP Server', version } }),
      agentCapabilities: {
        loadSession: true,
        sessionCapabilities: {
          list: {},
          ...(process.env.AGY_TEST_RESUME_CAP ? { resume: {} } : {}),
        },
      },
    }
  })
  .onRequest(methods.agent.authenticate, async () => ({}))
  .onRequest(methods.agent.session.new, async ({ params }) => {
    logRpcPid('new')
    if (
      process.env.AGY_TEST_FAIL_NEW_SESSION_FLAG &&
      existsSync(process.env.AGY_TEST_FAIL_NEW_SESSION_FLAG)
    )
      throw new Error('probe unavailable')
    const previousId = nativeId || randomUUID()
    nativeId = randomUUID()
    lastSessionCwd = params.cwd
    if (process.env.AGY_TEST_ARTIFACTS_DIR) {
      const conversationsDir = join(
        process.env.AGY_TEST_ARTIFACTS_DIR,
        'conversations',
      )
      const brainDir = join(
        process.env.AGY_TEST_ARTIFACTS_DIR,
        'brain',
        nativeId,
      )
      mkdirSync(conversationsDir, { recursive: true })
      mkdirSync(brainDir, { recursive: true })
      for (const ext of ['.db', '.db-wal', '.db-shm'])
        writeFileSync(join(conversationsDir, nativeId + ext), 'temp')
    }
    const append = (
      params._meta?.antigravity as { systemPrompt?: { append?: string } }
    )?.systemPrompt?.append
    log(append ? `new:${append}` : 'new')
    if (process.env.AGY_TEST_STALE_EVENTS) await staleEvents(previousId)
    sendCommands(nativeId)
    if (process.env.AGY_TEST_CATALOG_BY_CWD) {
      const cwdTag = basename(params.cwd)
      return {
        sessionId: nativeId,
        configOptions: configOptions.map((option) =>
          option.id === 'model'
            ? {
                ...option,
                currentValue: cwdTag,
                options: [{ value: cwdTag, name: `Model ${cwdTag}` }],
              }
            : option,
        ),
      }
    }
    return { sessionId: nativeId, configOptions }
  })
  .onRequest(methods.agent.session.close, async ({ params: { sessionId } }) => {
    logRpcPid('close')
    log('close')
    if (process.env.AGY_TEST_STALE_EVENTS) await staleEvents(sessionId)
    if (
      process.env.AGY_TEST_CLOSE_ERROR_FLAG &&
      existsSync(process.env.AGY_TEST_CLOSE_ERROR_FLAG)
    ) {
      unlinkSync(process.env.AGY_TEST_CLOSE_ERROR_FLAG)
      throw new Error('close failed')
    }
    if (process.env.AGY_TEST_CLOSE_ERROR) throw new Error('close failed')
    return {}
  })
  .onRequest(
    methods.agent.session.load,
    async ({ params: { sessionId, cwd, _meta } }) => {
      logRpcPid('load')
      if (process.env.AGY_TEST_LOG_CWD)
        log(`spawn-cwd:${process.cwd()}|load-cwd:${cwd}`)
      const append = (
        _meta?.antigravity as { systemPrompt?: { append?: string } }
      )?.systemPrompt?.append
      if (append) log(`load:${append}`)
      else if (process.env.AGY_TEST_LOG_LOAD) log('load')
      sendCommands(sessionId)
      if (process.env.AGY_TEST_CHECKPOINT_HISTORY) {
        const rows = readFileSync(
          join(
            process.env.HOME!,
            '.gemini/antigravity-cli/brain',
            sessionId,
            '.system_generated/logs/transcript_full.jsonl',
          ),
          'utf8',
        )
          .split('\n')
          .flatMap((line) => {
            const trimmed = line.trim()
            if (!trimmed) return []
            try {
              return [JSON.parse(trimmed)]
            } catch {
              return []
            }
          })
        for (const row of rows) {
          if (
            (row.type === 'USER_INPUT' || row.type === 'PLANNER_RESPONSE') &&
            typeof row.content === 'string' &&
            row.content
          ) {
            await client.sessionUpdate({
              sessionId,
              update: {
                sessionUpdate:
                  row.type === 'USER_INPUT'
                    ? 'user_message_chunk'
                    : 'agent_message_chunk',
                content: { type: 'text', text: row.content },
              },
            })
          }
        }
        return { configOptions }
      }
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
      if (process.env.AGY_TEST_LOAD_USAGE) {
        await client.sessionUpdate({
          sessionId,
          update: { sessionUpdate: 'usage_update', used: 300, size: 256_000 },
        })
        await client.sessionUpdate({
          sessionId,
          update: { sessionUpdate: 'usage_update', used: 900, size: 256_000 },
        })
      }
      return { configOptions }
    },
  )
  .onRequest(
    methods.agent.session.resume,
    async ({ params: { sessionId, _meta } }) => {
      logRpcPid('resume')
      const append = (
        _meta?.antigravity as { systemPrompt?: { append?: string } }
      )?.systemPrompt?.append
      log(append ? `resume:${append}` : 'resume')
      sendCommands(sessionId)
      return { configOptions }
    },
  )
  .onRequest(methods.agent.session.list, async ({ params: { cwd } }) => {
    logRpcPid('list')
    if (
      process.env.AGY_TEST_CRASH_ON_LIST_FLAG &&
      existsSync(process.env.AGY_TEST_CRASH_ON_LIST_FLAG)
    ) {
      const target = readFileSync(
        process.env.AGY_TEST_CRASH_ON_LIST_FLAG,
        'utf8',
      ).trim()
      if (!target || target === '1' || target === cwd) {
        unlinkSync(process.env.AGY_TEST_CRASH_ON_LIST_FLAG)
        process.exit(1)
      }
    }
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
  })
  .onRequest(
    methods.agent.session.setConfigOption,
    async ({ params: { configId, value } }) => {
      if (configId === 'fixture-start-wakeup') {
        if (!startRecordedWakeup)
          throw new Error('Recorded wakeup is not waiting to start')
        await startRecordedWakeup()
        startRecordedWakeup = undefined
        return { configOptions }
      }
      if (configId === 'fixture-write-wakeup-rows') {
        if (!writeRecordedWakeupRows)
          throw new Error('Recorded wakeup rows are not waiting')
        writeRecordedWakeupRows()
        writeRecordedWakeupRows = undefined
        return { configOptions }
      }
      if (configId === 'fixture-release-reads') {
        if (!finishRecordedReads)
          throw new Error('Recorded reads are not waiting')
        await finishRecordedReads()
        finishRecordedReads = undefined
        return { configOptions }
      }
      if (
        process.env.AGY_TEST_STRICT_MODEL &&
        configId === 'model' &&
        !configOptions[0].options.some((o) => o.value === String(value))
      )
        throw new Error(`Unknown model ID: ${value}`)
      log(`configure:${configId}=${value}`)
      return {
        configOptions: configOptions.map((o) =>
          o.id === configId ? { ...o, currentValue: String(value) } : o,
        ),
      }
    },
  )
  .onNotification(
    methods.agent.session.cancel,
    async ({ params: { sessionId } }) => {
      if (process.env.AGY_TEST_LOG_LOAD) log('cancel')
      canceled.add(sessionId)
      earlyCancelGates.get(sessionId)?.()
      cancels.get(sessionId)?.()
    },
  )
  .onRequest(
    methods.agent.session.prompt,
    async ({ params: { sessionId, prompt } }) => {
      const text = prompt.map((c) => (c.type === 'text' ? c.text : '')).join('')
      if (process.env.AGY_TEST_LOG_ELICITATION_CAP) log(`prompt:${text}`)
      if (
        [
          'recorded-wakeup',
          'recorded-wakeup-repeat',
          'recorded-wakeup-delayed-reads',
          'recorded-wakeup-denied-tool',
          'recorded-next',
        ].includes(text)
      ) {
        const sendUpdates = async (
          updates:
            | typeof recordedWakeup.foreground.updates
            | typeof recordedWakeup.wakeup.updates
            | typeof recordedWakeup.answer.updates,
        ) => {
          for (const update of updates)
            await output.write({
              jsonrpc: '2.0',
              method: 'session/update',
              params: { sessionId, update },
            })
        }
        const phase =
          text === 'recorded-next'
            ? recordedWakeup.next
            : recordedWakeup.foreground
        writeSessionTranscript(sessionId, phase.rows)
        await sendUpdates(phase.updates)
        if (text !== 'recorded-next') {
          let wakeupGate: Promise<void> | undefined
          let markWakeupReady: (() => void) | undefined
          if (text === 'recorded-wakeup-delayed-reads') {
            let releaseWakeup!: () => void
            wakeupGate = new Promise<void>((resolve) => {
              releaseWakeup = resolve
            })
            const wakeupReady = new Promise<void>((resolve) => {
              markWakeupReady = resolve
            })
            startRecordedWakeup = async () => {
              releaseWakeup()
              await wakeupReady
            }
          }
          afterPromptResponse = async () => {
            // This notification batch starts only after the sole prompt RPC returns.
            if (text === 'recorded-wakeup-denied-tool') {
              // Insert a rejected transcript-only slot before the two recorded reads.
              const [message, reads, , beta] = recordedWakeup.wakeup.rows
              writeSessionTranscript(sessionId, [
                message,
                {
                  ...reads,
                  tool_calls: [
                    { name: 'read_file', args: { Path: '/fixture/invalid' } },
                    ...reads.tool_calls!,
                  ],
                },
              ])
              await sendUpdates(
                recordedWakeup.wakeup.updates
                  .filter(
                    (update) =>
                      update.sessionUpdate === 'agent_thought_chunk' ||
                      (update.sessionUpdate === 'tool_call' &&
                        (update.toolCallId === 'call_2' ||
                          update.toolCallId === 'call_3')),
                  )
                  .slice(0, 3),
              )
              const decision = await client.requestPermission({
                sessionId,
                toolCall: {
                  toolCallId: 'call_2',
                  title: 'Run read_file',
                  rawInput: { Path: '/fixture/a.txt' },
                },
                options: [
                  { optionId: 'allow', name: 'Allow', kind: 'allow_once' },
                  { optionId: 'reject', name: 'Reject', kind: 'reject_once' },
                ],
              })
              if (
                decision.outcome.outcome !== 'selected' ||
                decision.outcome.optionId !== 'reject'
              )
                throw new Error('Expected the tool denial')
              await client.sessionUpdate({
                sessionId,
                update: {
                  sessionUpdate: 'tool_call_update',
                  toolCallId: 'call_2',
                  status: 'failed',
                },
              })
              for (const chunk of ['\n', '\nWakeup ', 'finished.']) {
                await client.sessionUpdate({
                  sessionId,
                  update: {
                    sessionUpdate: 'agent_message_chunk',
                    content: { type: 'text', text: chunk },
                  },
                })
              }
              writeSessionTranscript(sessionId, [
                { step_index: 6, type: 'GENERIC', status: 'INVALID' },
                {
                  step_index: 7,
                  type: 'GENERIC',
                  status: 'ERROR',
                  content: 'denied',
                },
                { ...beta, step_index: 8 },
                {
                  step_index: 9,
                  type: 'PLANNER_RESPONSE',
                  status: 'DONE',
                  content: 'Wakeup finished.',
                  input_tokens: 40,
                  output_tokens: 1,
                },
              ])
              return
            }
            if (text === 'recorded-wakeup-delayed-reads') {
              await wakeupGate
              const readCompleted = (
                u: (typeof recordedWakeup.wakeup.updates)[number],
              ) =>
                u.sessionUpdate === 'tool_call_update' &&
                (u.toolCallId === 'call_2' || u.toolCallId === 'call_3')
              writeRecordedWakeupRows = () =>
                writeSessionTranscript(sessionId, recordedWakeup.wakeup.rows)
              finishRecordedReads = () =>
                sendUpdates(recordedWakeup.wakeup.updates.filter(readCompleted))
              await sendUpdates(
                recordedWakeup.wakeup.updates.filter((u) => !readCompleted(u)),
              )
              markWakeupReady?.()
            } else {
              writeSessionTranscript(sessionId, recordedWakeup.wakeup.rows)
              await sendUpdates(recordedWakeup.wakeup.updates)
            }
            const answer = await client.createElicitation({
              ...recordedWakeup.wakeup.question,
              sessionId,
            })
            if (answer.action !== 'accept') return
            if (
              typeof answer.content !== 'object' ||
              answer.content === null ||
              !('q1' in answer.content) ||
              answer.content.q1 !== '1'
            )
              throw new Error('Expected the recorded answer')
            if (text === 'recorded-wakeup-repeat') {
              await client.createElicitation({
                ...recordedWakeup.wakeup.question,
                sessionId,
                toolCallId: 'call_5',
              })
              return
            }
            writeSessionTranscript(sessionId, recordedWakeup.answer.rows)
            await sendUpdates(recordedWakeup.answer.updates)
          }
        }
        return phase.response as {
          stopReason: 'end_turn'
          usage: typeof phase.response.usage
        }
      }
      if (
        text === 'hold:cancel' ||
        text === 'hold:cancel-throw' ||
        text === 'hold:cancel-usage' ||
        text === 'hold:cancel-throw-usage' ||
        text === 'hold:never'
      ) {
        if (
          text === 'hold:cancel-usage' ||
          text === 'hold:cancel-throw-usage'
        ) {
          writeSessionTranscript(
            sessionId,
            [
              { step_index: 1, type: 'USER_INPUT', content: text },
              {
                step_index: 2,
                type: 'PLANNER_RESPONSE',
                input_tokens: 10,
                output_tokens: 1,
              },
            ],
            'write',
          )
        }
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'available_commands_update',
            availableCommands: [{ name: `hold-${++holdSeq}`, description: '' }],
          },
        })
        if (text === 'hold:never') return await new Promise<never>(() => {})
        await new Promise<void>((resolve) => {
          if (canceled.has(sessionId)) resolve()
          else cancels.set(sessionId, resolve)
        })
        cancels.delete(sessionId)
        canceled.delete(sessionId)
        if (text === 'hold:cancel-throw' || text === 'hold:cancel-throw-usage')
          throw new Error('Synthetic RPC failure')
        if (text === 'hold:cancel-usage')
          return {
            stopReason: 'cancelled' as const,
            usage: {
              inputTokens: 600000,
              outputTokens: 4405,
              totalTokens: 604405,
            },
          }
        return { stopReason: 'cancelled' as const }
      }
      if (text === 'flushed-cancel') {
        writeSessionTranscript(
          sessionId,
          [
            { step_index: 1, type: 'USER_INPUT', content: 'hold:cancel-usage' },
            {
              step_index: 2,
              type: 'PLANNER_RESPONSE',
              input_tokens: 70207,
              output_tokens: 256,
            },
            { step_index: 3, type: 'GENERIC', status: 'CANCELED' },
          ],
          'write',
        )
        return { stopReason: 'cancelled' as const }
      }
      if (text === 'snapshot-only-cancel') {
        const step = ++wokenStepBase * 10
        writeSessionTranscript(sessionId, [
          { step_index: step, type: 'USER_INPUT', content: text },
          {
            step_index: step + 1,
            type: 'PLANNER_RESPONSE',
            input_tokens: 20,
            output_tokens: 2,
          },
        ])
        return { stopReason: 'cancelled' as const }
      }
      if (text === 'cancelled-positive-usage') {
        return {
          stopReason: 'cancelled' as const,
          usage: { inputTokens: 100, outputTokens: 10, totalTokens: 110 },
        }
      }
      if (text === 'native-context-then-wakeup') {
        await client.sessionUpdate({
          sessionId,
          update: { sessionUpdate: 'usage_update', used: 1234, size: 128000 },
        })
        writeSessionTranscript(
          sessionId,
          [
            { step_index: 0, type: 'USER_INPUT', content: 'work' },
            {
              step_index: 1,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Waiting',
              input_tokens: 15000,
              output_tokens: 1,
            },
            {
              step_index: 2,
              type: 'SYSTEM_MESSAGE',
              content:
                '[Message] sender=worker priority=MESSAGE_PRIORITY_HIGH content=ready',
            },
            {
              step_index: 3,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Finished',
              input_tokens: 89744,
              output_tokens: 256,
            },
          ],
          'write',
        )
        return {
          stopReason: 'end_turn' as const,
          usage: { inputTokens: 15000, outputTokens: 1, totalTokens: 15001 },
        }
      }
      if (text === 'late-history-initial') {
        writeSessionTranscript(
          sessionId,
          [
            {
              step_index: 1,
              type: 'PLANNER_RESPONSE',
              input_tokens: 70,
              output_tokens: 3,
            },
            { step_index: 2, type: 'GENERIC', status: 'CANCELED' },
          ],
          'write',
        )
        return { stopReason: 'cancelled' as const }
      }
      if (text === 'no-lf-snapshot-cancel' || text === 'no-lf-snapshot-error') {
        writeSessionTranscript(
          sessionId,
          JSON.stringify({ step_index: 1, type: 'USER_INPUT' }) +
            '\n' +
            JSON.stringify({
              step_index: 2,
              type: 'PLANNER_RESPONSE',
              input_tokens: 70207,
              output_tokens: 256,
            }),
          'write',
        )
        if (text === 'no-lf-snapshot-error')
          throw new Error('Synthetic RPC failure')
        return { stopReason: 'cancelled' as const }
      }
      if (
        text === 'checkpoint-turn' ||
        text === '/compact' ||
        text === 'checkpoint-cancel'
      ) {
        const dir = join(
          process.env.HOME!,
          '.gemini/antigravity-cli/brain',
          sessionId,
          '.system_generated/logs',
        )
        mkdirSync(dir, { recursive: true })
        const step = ++wokenStepBase * 10
        const checkpoint = JSON.parse(
          readFileSync(
            new URL('./fixtures/checkpoint.sanitized.jsonl', import.meta.url),
            'utf8',
          ),
        )
        const rows: object[] =
          text === '/compact'
            ? []
            : [{ step_index: step, type: 'USER_INPUT', content: text }]
        rows.push({
          ...checkpoint,
          step_index: step + 1,
          ...(text === 'checkpoint-cancel' ? { status: 'RUNNING' } : {}),
        })
        if (text === 'checkpoint-turn')
          rows.push({
            step_index: step + 2,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'After checkpoint',
            input_tokens: 10,
            output_tokens: 2,
          })
        appendFileSync(
          join(dir, 'transcript_full.jsonl'),
          rows.map((row) => JSON.stringify(row) + '\n').join(''),
        )
        return {
          stopReason:
            text === 'checkpoint-cancel'
              ? ('cancelled' as const)
              : ('end_turn' as const),
          usage: { inputTokens: 500, outputTokens: 50, totalTokens: 550 },
        }
      }
      if (text.startsWith('usage-')) {
        const dir = join(
          process.env.HOME!,
          '.gemini/antigravity-cli/brain',
          sessionId,
          '.system_generated/logs',
        )
        mkdirSync(dir, { recursive: true })
        const step = ++wokenStepBase * 10
        const rows: object[] = [
          { step_index: step, type: 'USER_INPUT', content: text },
        ]
        if (text === 'usage-empty' || text.startsWith('usage-stream'))
          rows.push({
            step_index: step + 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'No counters',
          })
        else
          rows.push({
            step_index: step + 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Synthetic usage regression',
            input_tokens: 70207,
            output_tokens: 256,
          })
        appendFileSync(
          join(dir, 'transcript_full.jsonl'),
          rows.map((row) => JSON.stringify(row) + '\n').join(''),
        )
        if (text === 'usage-native')
          await client.sessionUpdate({
            sessionId,
            update: { sessionUpdate: 'usage_update', used: 1234, size: 128000 },
          })
        if (text.startsWith('usage-stream:')) {
          const [, rawUsed, rawSize] = text.split(':')
          await client.sessionUpdate({
            sessionId,
            update: {
              sessionUpdate: 'usage_update',
              used: Number(rawUsed),
              size: Number(rawSize),
            },
          })
        }
        if (text.startsWith('usage-stream-wait:')) {
          const [, rawUsed, rawSize] = text.split(':')
          await client.sessionUpdate({
            sessionId,
            update: {
              sessionUpdate: 'usage_update',
              used: Number(rawUsed),
              size: Number(rawSize),
            },
          })
          await client.requestPermission({
            sessionId,
            toolCall: { toolCallId: 'usage-barrier', title: 'usage-barrier' },
            options: [{ optionId: 'yes', name: 'Allow', kind: 'allow_once' }],
          })
        }
        if (text.startsWith('usage-switch-model:')) {
          const [, nextModel] = text.split(':')
          configOptions[0].currentValue = nextModel
          await client.sessionUpdate({
            sessionId,
            update: {
              sessionUpdate: 'config_option_update',
              configOptions,
            },
          })
        }
        if (text === 'usage-error') throw new Error('Synthetic prompt failure')
        return {
          stopReason:
            text === 'usage-cancel'
              ? ('cancelled' as const)
              : ('end_turn' as const),
          usage: {
            inputTokens: 600000,
            outputTokens: 4405,
            totalTokens: 604405,
          },
        }
      }

      if (text === 'dangling-tool') {
        writeSessionTranscript(
          sessionId,
          [
            { step_index: 1, type: 'USER_INPUT', content: text },
            { step_index: 2, type: 'PLANNER_RESPONSE', status: 'DONE' },
          ],
          'write',
        )
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'tool_call',
            toolCallId: 'dangling-tool',
            title: 'run_command',
            status: 'in_progress',
            rawInput: { CommandLine: 'echo dangling' },
          },
        })
        return { stopReason: 'end_turn' as const }
      }
      if (text === 'cancel-only') {
        return { stopReason: 'cancelled' as const }
      }
      if (text === 'dangling-permission') {
        void client.requestPermission({
          sessionId,
          toolCall: {
            toolCallId: 'dangling-tool',
            title: 'run_command',
            rawInput: { CommandLine: 'echo dangling' },
          },
          options: [
            { optionId: 'yes', name: 'Allow', kind: 'allow_once' },
            { optionId: 'no', name: 'Deny', kind: 'reject_once' },
          ],
        })
        return { stopReason: 'end_turn' as const }
      }
      if (text === 'seed-wakeup-usage') {
        const step = ++wokenStepBase * 10
        writeSessionTranscript(sessionId, [
          { step_index: step, type: 'USER_INPUT', content: text },
          {
            step_index: step + 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Foreground',
            input_tokens: 100,
            output_tokens: 10,
          },
          {
            step_index: step + 2,
            type: 'SYSTEM_MESSAGE',
            content:
              '<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-10T18:59:59+08:00 sender=bg-1 priority=MESSAGE_PRIORITY_HIGH content=ready\n</SYSTEM_MESSAGE>',
          },
          {
            step_index: step + 3,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Woken done',
            input_tokens: 40,
            output_tokens: 4,
          },
        ])
        return {
          stopReason: 'end_turn' as const,
          usage: { inputTokens: 100, outputTokens: 10, totalTokens: 110 },
        }
      }
      if (text === 'corrupt-transcript-positive-usage') {
        writeSessionTranscript(sessionId, '{corrupt\n')
        return {
          stopReason: 'end_turn' as const,
          usage: { inputTokens: 120, outputTokens: 12, totalTokens: 132 },
        }
      }
      if (text === 'clean-positive-usage') {
        const step = ++wokenStepBase * 10
        writeSessionTranscript(sessionId, [
          { step_index: step, type: 'USER_INPUT', content: text },
          {
            step_index: step + 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Clean done',
            input_tokens: 50,
            output_tokens: 5,
          },
        ])
        return {
          stopReason: 'end_turn' as const,
          usage: { inputTokens: 50, outputTokens: 5, totalTokens: 55 },
        }
      }
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
      if (text === 'in-rpc-continuation') {
        const sys = join(
          process.env.HOME!,
          '.gemini/antigravity-cli/brain',
          sessionId,
          '.system_generated',
        )
        const logs = join(sys, 'logs')
        const messagesDir = join(sys, 'messages')
        mkdirSync(logs, { recursive: true })
        mkdirSync(messagesDir, { recursive: true })
        const file = join(logs, 'transcript_full.jsonl')
        const taskId = `${sessionId}/task-inrpc`
        const rows = [
          {
            step_index: 1,
            type: 'USER_INPUT',
            content: '<USER_REQUEST>in-rpc-continuation</USER_REQUEST>',
          },
          {
            step_index: 2,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Starting background check...',
            tool_calls: [
              {
                id: 'inrpc-bg',
                name: 'run_command',
                args: { CommandLine: 'sleep 1' },
              },
            ],
            input_tokens: 10,
            cache_read_tokens: 0,
            output_tokens: 2,
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
            content: 'Waiting for background check...',
            input_tokens: 15,
            cache_read_tokens: 5,
            output_tokens: 3,
          },
          {
            step_index: 5,
            type: 'SYSTEM_MESSAGE',
            content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-13T05:00:00Z sender=${taskId} priority=MESSAGE_PRIORITY_HIGH content=Task id "${taskId}" finished with result:\nCommand exited with code 0.\n\nCommand output:\ninrpc-ok\n</SYSTEM_MESSAGE>`,
          },
          {
            step_index: 6,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            thinking: 'In-RPC reasoning after HIGH wakeup',
            content: 'Final in-RPC reply.',
            input_tokens: 30,
            cache_read_tokens: 10,
            output_tokens: 8,
          },
        ]
        writeFileSync(
          join(messagesDir, 'msg-inrpc.json'),
          JSON.stringify({
            id: 'msg-inrpc',
            sender: taskId,
            recipient: sessionId,
            priority: 'MESSAGE_PRIORITY_HIGH',
            content: `Task id "${taskId}" finished with result:\nCommand exited with code 0.\n\nCommand output:\ninrpc-ok`,
            timestamp: '2026-11-13T05:00:00Z',
            sourceMetadata: {
              tool: {
                conversationId: sessionId,
                stepIndex: 3,
                toolCall: {
                  id: 'inrpc-bg',
                  name: 'run_command',
                  argumentsJson: '{"CommandLine":"sleep 1"}',
                },
              },
            },
          }),
        )
        writeFileSync(
          join(messagesDir, 'read.json'),
          JSON.stringify({ 'msg-inrpc': true }),
        )
        writeFileSync(file, rows.map((r) => JSON.stringify(r) + '\n').join(''))
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'user_message_chunk',
            content: { type: 'text', text: 'in-rpc-continuation' },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: 'Starting background check...' },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'tool_call',
            toolCallId: 'inrpc-bg',
            title: 'run_command',
            status: 'in_progress',
            rawInput: { CommandLine: 'sleep 1' },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: 'Waiting for background check...' },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_thought_chunk',
            content: {
              type: 'text',
              text: 'In-RPC reasoning after HIGH wakeup',
            },
          },
        })
        await client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: 'Final in-RPC reply.' },
          },
        })
        return {
          stopReason: 'end_turn' as const,
          usage: {
            inputTokens: 55,
            outputTokens: 13,
            totalTokens: 83,
            cachedReadTokens: 15,
          },
        }
      }
      if (text === 'after-in-rpc-continuation') {
        const logs = join(
          process.env.HOME!,
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
              content: '<USER_REQUEST>after-in-rpc-continuation</USER_REQUEST>',
            },
            {
              step_index: 8,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Turn 2 reply.',
              input_tokens: 40,
              output_tokens: 10,
            },
          ]
            .map((r) => JSON.stringify(r) + '\n')
            .join(''),
        )
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
            content: { type: 'text', text: 'Turn 2 reply.' },
          },
        })
        return {
          stopReason: 'end_turn' as const,
          usage: {
            inputTokens: 40,
            outputTokens: 10,
            totalTokens: 50,
            cachedReadTokens: 0,
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
      if (text === 'disconnect') {
        const keepAlive = new MessageChannel()
        keepAlive.port1.on('message', () => {})
        process.stdout.end()
        await new Promise(() => {})
      }
      if (text === 'crash') {
        process.exit(7)
      }
      if (text === 'mcp-preapproved') {
        const lazy = await client.requestPermission({
          sessionId,
          toolCall: {
            toolCallId: 'mcp-lazy',
            title: 'Run default_api:call_mcp_tool',
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
            name: 'default_api:mcp_rea_open_binary',
            title: 'Open binary',
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
          process.env.HOME!,
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
      if (text === 'permission-deny') {
        await client.requestPermission({
          sessionId,
          toolCall: {
            toolCallId: 'unfinished',
            title: 'run_command',
            rawInput: { CommandLine: 'echo unfinished' },
          },
          options: [
            { optionId: 'yes', name: 'Allow', kind: 'allow_once' },
            { optionId: 'no', name: 'Deny', kind: 'reject_once' },
          ],
        })
        return { stopReason: 'end_turn' as const }
      }
      if (text === 'wait') {
        if (
          process.env.AGY_TEST_CANCEL_BEFORE_REGISTER &&
          !canceled.has(sessionId)
        ) {
          await new Promise<void>((resolve) => {
            earlyCancelGates.set(sessionId, resolve)
          })
          earlyCancelGates.delete(sessionId)
        }
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
  )
  .connect(stream)
