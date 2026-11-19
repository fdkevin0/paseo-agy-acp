import { test, type TestContext } from 'node:test'
import assert from 'node:assert/strict'
import { mkdirSync } from 'node:fs'
import { mkdtemp, mkdir, writeFile, rm, symlink } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { fileURLToPath } from 'node:url'
import {
  extractStepUsage,
  readConversationCompactionWindow,
  readLastStepIndex,
  readLatestStepUsage,
} from '../server/brain-io.ts'
import { promptUsage } from '../server/mapping.ts'
import { SessionManager } from '../server/session.ts'
import { AgyUsageSource } from '../server/usage.ts'
import { createEventCollector, isolatedEnv } from './helpers.ts'

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

function encodeProtoField(
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

function encodeGenMetadataMaxContext(maxTokens: number): Buffer {
  return encodeProtoField(
    1,
    encodeProtoField(9, encodeProtoField(10, encodeProtoField(4, maxTokens))),
  )
}

function encodeExecutorMetadataMaxTokens(maxTokens: number): Buffer {
  return encodeProtoField(
    10,
    encodeProtoField(2, encodeProtoField(5, maxTokens)),
  )
}

function writeConversationDb(
  home: string,
  nativeId: string,
  entries: {
    gen?: Array<{ idx: number; data: Uint8Array }>
    exe?: Array<{ idx: number; data: Uint8Array }>
  },
) {
  const convDir = join(home, '.gemini/antigravity-cli/conversations')
  mkdirSync(convDir, { recursive: true })
  const db = new DatabaseSync(join(convDir, `${nativeId}.db`))
  try {
    db.exec(
      'CREATE TABLE IF NOT EXISTS gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)',
    )
    db.exec(
      'CREATE TABLE IF NOT EXISTS executor_metadata (idx INTEGER PRIMARY KEY, data BLOB)',
    )
    const insGen = db.prepare(
      'INSERT OR REPLACE INTO gen_metadata (idx, data) VALUES (?, ?)',
    )
    ;(entries.gen ?? []).forEach((row) => insGen.run(row.idx, row.data))
    const insExe = db.prepare(
      'INSERT OR REPLACE INTO executor_metadata (idx, data) VALUES (?, ?)',
    )
    ;(entries.exe ?? []).forEach((row) => insExe.run(row.idx, row.data))
  } finally {
    db.close()
  }
}

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
    backgroundProbeTimeoutMs?: number
  },
) {
  const { events, emit, wait } = createEventCollector()
  const env = isolatedEnv(home)
  const manager = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env,
    },
    usage: new AgyUsageSource({ env }),
    capabilities: ['prompt.message', 'session.configure'],
    emit,
    hasActiveBackgroundWork: options?.hasActiveBackgroundWork,
    backgroundProbeTimeoutMs: options?.backgroundProbeTimeoutMs,
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
  assert.equal(latestUsage().contextWindowMaxTokens, undefined)
  assert.equal(latestUsage().inputTokens, undefined)
  assert.equal(latestUsage().outputTokens, undefined)
})

test('persisted session restoration reads SQLite compaction window for persisted model without leaking to a switched model', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-restore-db-'))
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
  writeConversationDb(home, native, {
    gen: [{ idx: 0, data: encodeGenMetadataMaxContext(512_000) }],
  })
  const { events, wait, manager, latestUsage } = setupUsageManager(t, home)
  await manager.handle({
    type: 'session.open',
    requestId: 'open-same-model',
    sessionId: 's1',
    history: 'skip',
    config: {
      cwd: home,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
      model: 'test',
    },
    persistence: { version: 1, data: { sessionId: native } },
  })
  await wait((e) => e.type === 'session.usage' && e.sessionId === 's1')
  assert.equal(latestUsage().contextWindowUsedTokens, 73)
  assert.equal(latestUsage().contextWindowMaxTokens, 512_000)

  await manager.handle({
    type: 'session.open',
    requestId: 'open-switched-model',
    sessionId: 's2',
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
  await wait((e) => e.type === 'session.usage' && e.sessionId === 's2')
  const s2Usage = events.findLast(
    (e) => e.type === 'session.usage' && e.sessionId === 's2',
  )
  assert.ok(s2Usage?.type === 'session.usage')
  assert.equal(s2Usage.usage.contextWindowUsedTokens, 73)
  assert.equal(
    s2Usage.usage.contextWindowMaxTokens,
    undefined,
    'persisted model compaction window must not leak to a different requested model',
  )
})

test('two-stage transition locks SQLite compaction window after Turn 1 and isolates model switches', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-two-stage-'))
  const { events, manager, latestUsage } = setupUsageManager(t, home)
  await openFreshUsageSession(manager, home)
  const opened = events.find((e) => e.type === 'session.opened')
  assert.ok(opened?.type === 'session.opened' && opened.persistence)
  const nativeId = (opened.persistence.data as { sessionId: string }).sessionId

  writeConversationDb(home, nativeId, {
    gen: [{ idx: 0, data: encodeGenMetadataMaxContext(512_000) }],
  })

  // Turn 1: streaming uses native interface size (1,048,576); turn settlement locks SQLite limit (512,000).
  const turn1Start = events.length
  await sendUsagePrompt(manager, 'p1', 'usage-stream:15000:1048576')
  const turn1Windows = events
    .slice(turn1Start)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(turn1Windows, [1_048_576, 512_000])
  assert.equal(latestUsage().contextWindowUsedTokens, 15_000)

  // Turn 2: same model uses learned 512,000 from streaming through settlement despite native size staying 1,048,576.
  const turn2Start = events.length
  await sendUsagePrompt(manager, 'p2', 'usage-stream:28000:1048576')
  const turn2Windows = events
    .slice(turn2Start)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(turn2Windows, [512_000, 512_000])
  assert.equal(latestUsage().contextWindowUsedTokens, 28_000)

  // Switch to model-b before any new SQLite row is written: must show model-b's interface size, never model-a's 512,000.
  await manager.handle({
    type: 'session.configure',
    requestId: 'model-b',
    sessionId: 's',
    changes: { model: 'model-b' },
  })
  const turn3Start = events.length
  await sendUsagePrompt(manager, 'p3', 'usage-stream:30000:1000000')
  const turn3Windows = events
    .slice(turn3Start)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(turn3Windows, [1_000_000, 1_000_000])

  // New SQLite row from model-b (idx=1 > cursor 0) updates model-b to 256,000.
  writeConversationDb(home, nativeId, {
    gen: [{ idx: 1, data: encodeGenMetadataMaxContext(256_000) }],
  })
  const turn4Start = events.length
  await sendUsagePrompt(manager, 'p4', 'usage-stream:35000:1000000')
  const turn4Windows = events
    .slice(turn4Start)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(turn4Windows, [1_000_000, 256_000])

  // Switching back to test retains test's 512,000 immediately on streaming.
  await manager.handle({
    type: 'session.configure',
    requestId: 'model-test-back',
    sessionId: 's',
    changes: { model: 'test' },
  })
  const turn5Start = events.length
  await sendUsagePrompt(manager, 'p5', 'usage-stream:40000:1048576')
  const turn5Windows = events
    .slice(turn5Start)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(turn5Windows, [512_000, 512_000])
})

test('SQLite compaction reader falls back to executor_metadata and rejects symlink escapes', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-sqlite-reader-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  await mkdir(brainRoot, { recursive: true })
  const idExe = '22222222-2222-4222-8222-222222222222'
  writeConversationDb(home, idExe, {
    gen: [{ idx: 0, data: encodeProtoField(1, encodeProtoField(2, 1)) }],
    exe: [{ idx: 0, data: encodeExecutorMetadataMaxTokens(256_000) }],
  })
  assert.deepEqual(await readConversationCompactionWindow(brainRoot, idExe), {
    maxTokens: 256_000,
    genIdx: 0,
    exeIdx: 0,
  })

  const idLargeGen = '33333333-3333-4333-8333-333333333333'
  writeConversationDb(home, idLargeGen, {
    gen: [
      {
        idx: 0,
        data: Buffer.concat([
          encodeProtoField(99, Buffer.alloc(40_000, 0x41)),
          encodeGenMetadataMaxContext(512_000),
        ]),
      },
    ],
  })
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idLargeGen),
    { maxTokens: 512_000, genIdx: 0, exeIdx: -1 },
  )

  const escapedTarget = join(home, 'outside.db')
  await writeFile(escapedTarget, 'not-a-session-db')
  const idSymlinkDb = '44444444-4444-4444-8444-444444444444'
  const convDir = join(home, '.gemini/antigravity-cli/conversations')
  await symlink(escapedTarget, join(convDir, `${idSymlinkDb}.db`))
  await assert.rejects(
    readConversationCompactionWindow(brainRoot, idSymlinkDb),
    { name: 'BrainEscapeError' },
  )

  const idSymlinkWal = '55555555-5555-4555-8555-555555555555'
  writeConversationDb(home, idSymlinkWal, {
    gen: [{ idx: 0, data: encodeGenMetadataMaxContext(512_000) }],
  })
  await symlink(escapedTarget, join(convDir, `${idSymlinkWal}.db-wal`))
  await assert.rejects(
    readConversationCompactionWindow(brainRoot, idSymlinkWal),
    { name: 'BrainEscapeError' },
  )

  const idSymlinkShm = '66666666-6666-4666-8666-666666666666'
  writeConversationDb(home, idSymlinkShm, {
    gen: [{ idx: 0, data: encodeGenMetadataMaxContext(512_000) }],
  })
  await symlink(escapedTarget, join(convDir, `${idSymlinkShm}.db-shm`))
  await assert.rejects(
    readConversationCompactionWindow(brainRoot, idSymlinkShm),
    { name: 'BrainEscapeError' },
  )

  const escapedConvHome = await mkdtemp(join(tmpdir(), 'agy-sqlite-dir-esc-'))
  t.after(() => rm(escapedConvHome, { recursive: true, force: true }))
  const escapedConvBrain = join(
    escapedConvHome,
    '.gemini/antigravity-cli/brain',
  )
  await mkdir(escapedConvBrain, { recursive: true })
  const outsideConvDir = join(escapedConvHome, 'outside-conversations')
  await mkdir(outsideConvDir, { recursive: true })
  await symlink(
    outsideConvDir,
    join(escapedConvHome, '.gemini/antigravity-cli/conversations'),
  )
  await assert.rejects(
    readConversationCompactionWindow(escapedConvBrain, idExe),
    { name: 'BrainEscapeError' },
  )
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
    undefined,
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
    backgroundProbeTimeoutMs: 2000,
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

test('snapshot-only cancellation preserves consumption without modifying the next bill', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-snapshot-cancel-'))
  const { manager, latestUsage } = setupUsageManager(t, home)
  await openFreshUsageSession(manager, home)
  await sendUsagePrompt(manager, 'p1', 'seed-wakeup-usage')
  assert.equal(latestUsage().inputTokens, 140)
  assert.equal(latestUsage().outputTokens, 14)

  await sendUsagePrompt(manager, 'p2', 'snapshot-only-cancel')
  assert.equal(latestUsage().contextWindowUsedTokens, 22)
  assert.equal(latestUsage().inputTokens, 140)
  assert.equal(latestUsage().outputTokens, 14)

  await sendUsagePrompt(manager, 'p3', 'cancelled-positive-usage')
  assert.equal(latestUsage().inputTokens, 100)
  assert.equal(latestUsage().outputTokens, 10)
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
