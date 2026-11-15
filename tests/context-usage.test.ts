import { test, type TestContext } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  extractStepUsage,
  readLastStepIndex,
  readLatestStepUsage,
} from '../server/brain-io.ts'
import { promptUsage } from '../server/mapping.ts'
import { SessionManager } from '../server/session.ts'
import { AgyUsageSource } from '../server/usage.ts'
import { createEventCollector } from './helpers.ts'

test('latest usage respects the turn boundary, incomplete rows and cache-only counters', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-usage-reader-'))
  const id = '11111111-1111-4111-8111-111111111111'
  try {
    assert.deepEqual(await readLatestStepUsage(root, id), { lastStepIndex: -1 })
    const dir = join(root, id, '.system_generated/logs')
    await mkdir(dir, { recursive: true })
    const rows = [
      {
        step_index: 28,
        type: 'PLANNER_RESPONSE',
        input_tokens: 70207,
        output_tokens: 256,
      },
      { step_index: 29, type: 'USER_INPUT' },
      { step_index: 30, type: 'PLANNER_RESPONSE', thinking: 'No usage yet' },
    ]
    await writeFile(
      join(dir, 'transcript_full.jsonl'),
      rows.map((row) => JSON.stringify(row) + '\n').join(''),
    )
    const { lastStepUsage: lastStep } = await readLatestStepUsage(root, id)
    assert.equal(
      promptUsage(
        { inputTokens: 600000, outputTokens: 4405, totalTokens: 604405 },
        { lastStep },
      ).contextWindowUsedTokens,
      70463,
    )
    assert.deepEqual(await readLatestStepUsage(root, id, 28), {
      lastStepIndex: 30,
    })
    assert.deepEqual(await readLatestStepUsage(root, id, 29), {
      lastStepIndex: 30,
    })
    const cache = extractStepUsage({
      type: 'PLANNER_RESPONSE',
      cache_read_tokens: 90,
      cache_write_tokens: 10,
    })
    assert.equal(
      promptUsage(null, { lastStep: cache }).contextWindowUsedTokens,
      100,
    )
    assert.equal(
      extractStepUsage({
        type: 'PLANNER_RESPONSE',
        input_tokens: -1,
        output_tokens: NaN,
      }),
      undefined,
    )
    assert.equal(
      extractStepUsage({ type: 'GENERIC', input_tokens: 10 }),
      undefined,
    )
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('tail snapshots accept complete JSON without LF but skip torn records and UTF-8', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'agy-usage-eof-'))
  t.after(() => rm(root, { recursive: true, force: true }))
  const id = '11111111-1111-4111-8111-111111111111'
  const dir = join(root, id, '.system_generated/logs')
  await mkdir(dir, { recursive: true })
  const previous = JSON.stringify({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    input_tokens: 10,
  })
  const latest = JSON.stringify({
    step_index: 2,
    type: 'PLANNER_RESPONSE',
    input_tokens: 70,
    output_tokens: 3,
    content: '中'.repeat(30000),
  })
  const tornUtf8 = Buffer.from(latest).subarray(
    0,
    Buffer.from(latest).indexOf(Buffer.from('中')) + 1,
  )
  const checkTail = async (
    name: string,
    bytes: Buffer,
    expectedStep: number,
    expectedInputTokens: number | undefined,
  ) =>
    t.test(name, async () => {
      await writeFile(join(dir, 'transcript_full.jsonl'), bytes)
      assert.equal(await readLastStepIndex(root, id), expectedStep)
      const result = await readLatestStepUsage(root, id)
      assert.equal(result.lastStepIndex, expectedStep)
      assert.equal(result.lastStepUsage?.inputTokens, expectedInputTokens)
      assert.deepEqual(await readLatestStepUsage(root, id, expectedStep), {
        lastStepIndex: expectedStep,
      })
    })
  await checkTail('LF', Buffer.from(`${previous}\n${latest}\n`), 2, 70)
  await checkTail('no LF', Buffer.from(`${previous}\n${latest}`), 2, 70)
  await checkTail('single row', Buffer.from(latest), 2, 70)
  await checkTail(
    'partial JSON',
    Buffer.from(`${previous}\n{"step_index":2`),
    1,
    10,
  )
  await checkTail(
    'torn UTF-8',
    Buffer.concat([Buffer.from(`${previous}\n`), tornUtf8]),
    1,
    10,
  )
  await checkTail('only partial', Buffer.from('{"step_index":2'), -1, undefined)
})

function setupUsageManager(
  t: TestContext,
  home: string,
  options?: {
    hasActiveBackgroundWork?: ConstructorParameters<
      typeof SessionManager
    >[0]['hasActiveBackgroundWork']
  },
) {
  const { events, emit, wait } = createEventCollector()
  const manager = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: { HOME: home, PATH: process.env.PATH ?? '' },
    },
    usage: new AgyUsageSource(),
    capabilities: ['prompt.message', 'session.configure'],
    emit,
    hasActiveBackgroundWork: options?.hasActiveBackgroundWork,
  })
  t.after(async () => {
    await manager.close()
    await rm(home, { recursive: true, force: true })
  })
  const latestUsage = () => {
    const event = events.findLast((e) => e.type === 'session.usage')
    assert.ok(event?.type === 'session.usage')
    return event.usage
  }
  return { events, wait, manager, latestUsage }
}

test('restoration publishes context only after applying the selected model', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-restore-'))
  const native = '11111111-1111-4111-8111-111111111111'
  const dir = join(
    home,
    '.gemini/antigravity-cli/brain',
    native,
    '.system_generated/logs',
  )
  await mkdir(dir, { recursive: true })
  await writeFile(
    join(dir, 'transcript_full.jsonl'),
    JSON.stringify({
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      input_tokens: 70,
      output_tokens: 3,
    }) + '\n',
  )
  const { wait, manager, latestUsage } = setupUsageManager(t, home)
  await manager.handle({
    type: 'session.open',
    requestId: 'open',
    sessionId: 's',
    history: 'skip',
    config: {
      cwd: home,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
      model: 'gpt-oss-120b-medium',
    },
    persistence: { version: 1, data: { sessionId: native } },
  })
  await wait((e) => e.type === 'session.usage')
  assert.equal(latestUsage().contextWindowUsedTokens, 73)
  assert.equal(latestUsage().contextWindowMaxTokens, 131072)
  assert.equal(latestUsage().inputTokens, undefined)
  assert.equal(latestUsage().outputTokens, undefined)
})

test('native capacity survives a later same-model transcript fallback', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-cap-'))
  const { manager, latestUsage } = setupUsageManager(t, home)
  await manager.handle({
    type: 'session.open',
    requestId: 'open',
    sessionId: 's',
    history: 'skip',
    config: {
      cwd: home,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
    },
  })
  const sendPrompt = (id: string, text: string) =>
    manager.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: id,
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text }] },
      },
    })
  await sendPrompt('p1', 'usage-native')
  await sendPrompt('p2', 'usage-cancel')
  assert.equal(latestUsage().contextWindowMaxTokens, 128000)
  await manager.handle({
    type: 'session.configure',
    requestId: 'model',
    sessionId: 's',
    changes: { model: 'gpt-oss-120b-medium' },
  })
  await sendPrompt('p3', 'usage-cancel')
  assert.equal(
    latestUsage().contextWindowMaxTokens,
    131072,
    'native capacity from another model must not leak',
  )
  await manager.handle({
    type: 'session.configure',
    requestId: 'model-back',
    sessionId: 's',
    changes: { model: 'test' },
  })
  await sendPrompt('p4', 'usage-cancel')
  assert.equal(
    latestUsage().contextWindowMaxTokens,
    128000,
    'the observed capacity is remembered by model',
  )
})

test('all-zero planner counters are not an occupancy measurement', () => {
  assert.equal(
    extractStepUsage({
      type: 'PLANNER_RESPONSE',
      input_tokens: 0,
      output_tokens: 0,
      cache_read_tokens: 0,
      cache_write_tokens: 0,
    }),
    undefined,
  )
})

test('latest snapshot tail-read avoids unrelated large history and preserves complete UTF-8 rows', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-tail-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const native = '11111111-1111-4111-8111-111111111111'
  const dir = join(home, native, '.system_generated/logs')
  await mkdir(dir, { recursive: true })
  const snapshot = {
    step_index: 21,
    type: 'PLANNER_RESPONSE',
    input_tokens: 70,
    output_tokens: 3,
    content: '中'.repeat(30000),
  }
  await writeFile(
    join(dir, 'transcript_full.jsonl'),
    'unrelated old history'.repeat(100000) +
      '\n' +
      JSON.stringify(snapshot) +
      '\n' +
      JSON.stringify({ step_index: 22, type: 'GENERIC' }) +
      '\n' +
      '{"step_index":23',
  )
  const result = await readLatestStepUsage(home, native, 20)
  assert.deepEqual(result, {
    lastStepUsage: { inputTokens: 70, outputTokens: 3, cachedReadTokens: 0 },
    lastStepIndex: 22,
  })
})

function openFreshUsageSession(
  manager: SessionManager,
  home: string,
  sessionId = 's',
) {
  return manager.handle({
    type: 'session.open',
    requestId: `open-${sessionId}`,
    sessionId,
    history: 'skip',
    config: {
      cwd: home,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
    },
  })
}

function sendUsagePrompt(
  manager: SessionManager,
  clientMessageId: string,
  text: string,
  sessionId = 's',
) {
  return manager.handle({
    type: 'session.prompt',
    sessionId,
    prompt: {
      clientMessageId,
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text }] },
    },
  })
}

async function runCancelSnapshotFollowsInterruptTest(
  t: TestContext,
  promptText: 'hold:cancel-usage' | 'hold:cancel-throw-usage',
  expectedInputTokens: number | undefined,
) {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-interrupt-'))
  const flushed = [
    { step_index: 1, type: 'USER_INPUT', content: promptText },
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      input_tokens: 70207,
      output_tokens: 256,
    },
    { step_index: 3, type: 'GENERIC', status: 'CANCELED' },
  ]
  const { events, wait, manager, latestUsage } = setupUsageManager(t, home, {
    hasActiveBackgroundWork: async (brainRoot, nativeId) => {
      await writeFile(
        join(
          brainRoot,
          nativeId,
          '.system_generated/logs/transcript_full.jsonl',
        ),
        flushed.map((row) => JSON.stringify(row) + '\n').join(''),
      )
      return false
    },
  })
  await openFreshUsageSession(manager, home)
  const opened = events.find((e) => e.type === 'session.opened')
  assert.ok(opened?.type === 'session.opened' && opened.persistence)
  const nativeId = (opened.persistence.data as { sessionId: string }).sessionId
  const transcriptPath = join(
    home,
    '.gemini/antigravity-cli/brain',
    nativeId,
    '.system_generated/logs/transcript_full.jsonl',
  )
  const commandsBefore = events.filter(
    (e) => e.type === 'session.commands',
  ).length
  const running = sendUsagePrompt(manager, 'p1', promptText)
  await wait(
    () =>
      events.filter((e) => e.type === 'session.commands').length >
      commandsBefore,
  )
  await manager.handle({
    type: 'session.interrupt',
    requestId: 'interrupt',
    sessionId: 's',
  })
  await running
  assert.equal(latestUsage().contextWindowUsedTokens, 70463)
  assert.equal(latestUsage().inputTokens, expectedInputTokens)
  await writeFile(transcriptPath, '')
  const usageCount = events.filter((e) => e.type === 'session.usage').length
  await sendUsagePrompt(manager, 'p2', 'flushed-cancel')
  assert.equal(
    events.filter((e) => e.type === 'session.usage').length,
    usageCount,
    'an empty next-turn baseline cannot republish the canceled turn as new usage',
  )
}

test('cancel snapshot follows interrupt work and advances cross-turn boundary (cancelled)', async (t) => {
  await runCancelSnapshotFollowsInterruptTest(t, 'hold:cancel-usage', 600000)
})

test('cancel snapshot follows interrupt work and advances cross-turn boundary (throw)', async (t) => {
  await runCancelSnapshotFollowsInterruptTest(
    t,
    'hold:cancel-throw-usage',
    undefined,
  )
})

test('snapshot-only cancellation preserves consumption and wakeup debt for the next bill', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-woken-debt-'))
  const { manager, latestUsage } = setupUsageManager(t, home)
  await openFreshUsageSession(manager, home)
  await sendUsagePrompt(manager, 'p1', 'seed-woken-carry')
  assert.equal(latestUsage().inputTokens, 140)
  assert.equal(latestUsage().outputTokens, 14)

  await sendUsagePrompt(manager, 'p2', 'snapshot-only-cancel')
  assert.equal(latestUsage().contextWindowUsedTokens, 22)
  assert.equal(latestUsage().inputTokens, 140)
  assert.equal(latestUsage().outputTokens, 14)

  await sendUsagePrompt(manager, 'p3', 'cancelled-positive-usage')
  assert.equal(latestUsage().inputTokens, 60)
  assert.equal(latestUsage().outputTokens, 6)
})

test('foreground native context does not freeze later wakeup occupancy or lose its size', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-fg-wakeup-'))
  const { manager, latestUsage } = setupUsageManager(t, home)
  await openFreshUsageSession(manager, home)
  await sendUsagePrompt(manager, 'p1', 'native-context-then-wakeup')
  assert.equal(latestUsage().contextWindowUsedTokens, 90000)
  assert.equal(latestUsage().contextWindowMaxTokens, 128000)
})

test('late historical counters stay outside the next turn but remain recoverable on restore', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-late-history-'))
  const { events, wait, manager, latestUsage } = setupUsageManager(t, home)
  await openFreshUsageSession(manager, home)
  const opened = events.find((e) => e.type === 'session.opened')
  assert.ok(opened?.type === 'session.opened' && opened.persistence)
  const nativeId = (opened.persistence.data as { sessionId: string }).sessionId
  const transcriptPath = join(
    home,
    '.gemini/antigravity-cli/brain',
    nativeId,
    '.system_generated/logs/transcript_full.jsonl',
  )
  await sendUsagePrompt(manager, 'p1', 'late-history-initial')
  assert.equal(latestUsage().contextWindowUsedTokens, 73)
  await writeFile(
    transcriptPath,
    [
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        input_tokens: 700,
        output_tokens: 30,
      },
      { step_index: 2, type: 'GENERIC', status: 'CANCELED' },
    ]
      .map((row) => JSON.stringify(row) + '\n')
      .join(''),
  )
  const usageCount = events.filter((e) => e.type === 'session.usage').length
  await sendUsagePrompt(manager, 'p2', 'cancel-only')
  assert.equal(
    events.filter((e) => e.type === 'session.usage').length,
    usageCount,
    'late previous-turn data must not become a new-turn bill or snapshot',
  )
  await manager.handle({
    type: 'session.close',
    requestId: 'close',
    sessionId: 's',
  })
  await manager.handle({
    type: 'session.open',
    requestId: 'restore',
    sessionId: 'restored',
    history: 'skip',
    persistence: opened.persistence,
    config: {
      cwd: home,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
    },
  })
  await wait((e) => e.type === 'session.usage' && e.sessionId === 'restored')
  assert.equal(latestUsage().contextWindowUsedTokens, 730)
  assert.equal(
    latestUsage().inputTokens,
    undefined,
    'restoring late history does not create a consumption bill',
  )
})

async function runNoLfSnapshotSurvivesTest(
  t: TestContext,
  promptText: 'no-lf-snapshot-cancel' | 'no-lf-snapshot-error',
) {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-nolf-restore-'))
  const { events, wait, manager, latestUsage } = setupUsageManager(t, home)
  await openFreshUsageSession(manager, home)
  const opened = events.find((e) => e.type === 'session.opened')
  assert.ok(opened?.type === 'session.opened' && opened.persistence)
  await sendUsagePrompt(manager, 'p1', promptText)
  assert.equal(latestUsage().contextWindowUsedTokens, 70463)
  const usageCount = events.filter((e) => e.type === 'session.usage').length
  await sendUsagePrompt(manager, 'p2', 'cancel-only')
  assert.equal(
    events.filter((e) => e.type === 'session.usage').length,
    usageCount,
    'the complete previous-turn EOF record is not republished as this turn',
  )
  await manager.handle({
    type: 'session.close',
    requestId: 'close',
    sessionId: 's',
  })
  await manager.handle({
    type: 'session.open',
    requestId: 'restore',
    sessionId: 'restored',
    history: 'skip',
    persistence: opened.persistence,
    config: {
      cwd: home,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
    },
  })
  await wait((e) => e.type === 'session.usage' && e.sessionId === 'restored')
  assert.equal(latestUsage().contextWindowUsedTokens, 70463)
  assert.equal(latestUsage().inputTokens, undefined)
}

test('newline-less snapshot survives cancel, next-turn baseline and restore', async (t) => {
  await runNoLfSnapshotSurvivesTest(t, 'no-lf-snapshot-cancel')
})

test('newline-less snapshot survives error, next-turn baseline and restore', async (t) => {
  await runNoLfSnapshotSurvivesTest(t, 'no-lf-snapshot-error')
})
