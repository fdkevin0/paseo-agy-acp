import { test, type TestContext } from 'node:test'
import assert from 'node:assert/strict'
import { mkdirSync, utimesSync } from 'node:fs'
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

function encodeGenMetadataContextUsage(fields: {
  maxTokens?: number
  estimatedTokens?: number
}): Buffer {
  const inner: Buffer[] = []
  if (fields.estimatedTokens !== undefined)
    inner.push(encodeProtoField(1, fields.estimatedTokens))
  if (fields.maxTokens !== undefined)
    inner.push(encodeProtoField(4, fields.maxTokens))
  return encodeProtoField(
    1,
    encodeProtoField(9, encodeProtoField(10, Buffer.concat(inner))),
  )
}

function encodeGenMetadataMaxContext(maxTokens: number): Buffer {
  return encodeGenMetadataContextUsage({ maxTokens })
}

let dbWriteEpochSeconds = Math.floor(Date.now() / 1000) + 1

function writeConversationDb(
  home: string,
  nativeId: string,
  entries: {
    gen?: Array<{ idx: number; data: Uint8Array }>
  },
) {
  const convDir = join(home, '.gemini/antigravity-cli/conversations')
  mkdirSync(convDir, { recursive: true })
  const dbPath = join(convDir, `${nativeId}.db`)
  const db = new DatabaseSync(dbPath)
  try {
    db.exec(
      'CREATE TABLE IF NOT EXISTS gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)',
    )
    const insGen = db.prepare(
      'INSERT OR REPLACE INTO gen_metadata (idx, data) VALUES (?, ?)',
    )
    ;(entries.gen ?? []).forEach((row) => insGen.run(row.idx, row.data))
  } finally {
    db.close()
  }
  const tick = ++dbWriteEpochSeconds
  utimesSync(dbPath, tick, tick)
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
    capabilities: ['prompt.message', 'session.configure', 'permission'],
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

test('SQLite compaction window resolves live on usage_update, syncs on model switch, and isolates models', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-two-stage-'))
  const { events, manager, latestUsage } = setupUsageManager(t, home)
  await openFreshUsageSession(manager, home)
  const opened = events.find((e) => e.type === 'session.opened')
  assert.ok(opened?.type === 'session.opened' && opened.persistence)
  const nativeId = (opened.persistence.data as { sessionId: string }).sessionId

  writeConversationDb(home, nativeId, {
    gen: [{ idx: 0, data: encodeGenMetadataMaxContext(512_000) }],
  })

  // Turn 1: live usage_update resolves SQLite max_context_tokens (512,000) immediately instead of raw interface size (1,048,576).
  const turn1Start = events.length
  await sendUsagePrompt(manager, 'p1', 'usage-stream:15000:1048576')
  const turn1Windows = events
    .slice(turn1Start)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(turn1Windows, [512_000, 512_000])
  assert.equal(latestUsage().contextWindowUsedTokens, 15_000)

  // Turn 2: same model keeps learned 512,000 from streaming through settlement despite native size staying 1,048,576.
  const turn2Start = events.length
  await sendUsagePrompt(manager, 'p2', 'usage-stream:28000:1048576')
  const turn2Windows = events
    .slice(turn2Start)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(turn2Windows, [512_000, 512_000])
  assert.equal(latestUsage().contextWindowUsedTokens, 28_000)

  // Switch to model-b before any new SQLite row is written: clears test's 512,000 immediately and falls back to model-b's interface size.
  const switchToBStart = events.length
  await manager.handle({
    type: 'session.configure',
    requestId: 'model-b',
    sessionId: 's',
    changes: { model: 'model-b' },
  })
  const switchToBWindows = events
    .slice(switchToBStart)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(switchToBWindows, [undefined])

  const turn3Start = events.length
  await sendUsagePrompt(manager, 'p3', 'usage-stream:30000:1000000')
  const turn3Windows = events
    .slice(turn3Start)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(turn3Windows, [1_000_000, 1_000_000])

  // New SQLite row from model-b (idx=1 > cursor 0) updates model-b immediately on streaming usage_update to 256,000.
  writeConversationDb(home, nativeId, {
    gen: [{ idx: 1, data: encodeGenMetadataMaxContext(256_000) }],
  })
  const turn4Start = events.length
  await sendUsagePrompt(manager, 'p4', 'usage-stream:35000:1000000')
  const turn4Windows = events
    .slice(turn4Start)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(turn4Windows, [256_000, 256_000])

  // Switching back to test publishes test's 512,000 immediately and retains it on streaming.
  const switchBackStart = events.length
  await manager.handle({
    type: 'session.configure',
    requestId: 'model-test-back',
    sessionId: 's',
    changes: { model: 'test' },
  })
  const switchBackWindows = events
    .slice(switchBackStart)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(switchBackWindows, [512_000])

  const turn5Start = events.length
  await sendUsagePrompt(manager, 'p5', 'usage-stream:40000:1048576')
  const turn5Windows = events
    .slice(turn5Start)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(turn5Windows, [512_000, 512_000])
})

test('turn settlement upgrades compaction window when SQLite flushes after streaming usage_update', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-settle-upgrade-'))
  const { events, wait, manager, latestUsage } = setupUsageManager(t, home)
  await openFreshUsageSession(manager, home)
  const opened = events.find((e) => e.type === 'session.opened')
  assert.ok(opened?.type === 'session.opened' && opened.persistence)
  const nativeId = (opened.persistence.data as { sessionId: string }).sessionId

  const turnStart = events.length
  const running = sendUsagePrompt(
    manager,
    'p1',
    'usage-stream-wait:15000:1048576',
  )
  await wait((e) => e.type === 'session.usage')
  const permission = await wait((e) => e.type === 'session.permission')
  assert.equal(permission.type, 'session.permission')
  assert.equal(latestUsage().contextWindowMaxTokens, 1_048_576)

  // SQLite flushes only after streaming usage_update already published native interface size.
  writeConversationDb(home, nativeId, {
    gen: [{ idx: 0, data: encodeGenMetadataMaxContext(512_000) }],
  })
  await manager.handle({
    type: 'session.permission',
    sessionId: 's',
    permissionId: permission.request.id,
    response: { behavior: 'allow', selectedActionId: 'yes' },
  })
  await running

  const turnWindows = events
    .slice(turnStart)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(turnWindows, [1_048_576, 512_000])
  assert.equal(latestUsage().contextWindowUsedTokens, 15_000)
})

test('persisted history replay clears prior-model compaction window when opened with a different model', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-replay-switch-'))
  const archivedNative = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
  writeConversationDb(home, archivedNative, {
    gen: [{ idx: 0, data: encodeGenMetadataMaxContext(200_000) }],
  })
  const { events, manager } = setupUsageManager(t, home)
  await manager.handle({
    type: 'session.open',
    requestId: 'open-replay-switched',
    sessionId: 'replayed-switched',
    history: 'replay',
    config: {
      cwd: home,
      env: { AGY_TEST_LOAD_USAGE: '1' },
      mcpServers: {},
      settings: {},
      persist: true,
      model: 'gpt-oss-120b-medium',
    },
    persistence: { version: 1, data: { sessionId: archivedNative } },
  })
  const switchedUsage = events
    .filter((e) => e.type === 'session.usage')
    .filter((e) => e.sessionId === 'replayed-switched')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(switchedUsage, [200_000, 200_000, undefined])
})

test('unsolicited config_option_update model switch advances SQLite cursor and syncs live capacity', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-native-switch-'))
  const { events, manager } = setupUsageManager(t, home)
  await openFreshUsageSession(manager, home)
  const opened = events.find((e) => e.type === 'session.opened')
  assert.ok(opened?.type === 'session.opened' && opened.persistence)
  const nativeId = (opened.persistence.data as { sessionId: string }).sessionId

  // Run a turn on default model `test` with native size 1,048,576 (no DB row yet).
  await sendUsagePrompt(manager, 'p1', 'usage-stream:12000:1048576')
  // Now DB row idx=0 is written for `test` before an unsolicited native config_option_update switches to `model-b`.
  writeConversationDb(home, nativeId, {
    gen: [{ idx: 0, data: encodeGenMetadataMaxContext(512_000) }],
  })
  const switchStart = events.length
  await sendUsagePrompt(manager, 'p2', 'usage-switch-model:model-b')
  const switchWindows = events
    .slice(switchStart)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.equal(switchWindows.at(-1), undefined)

  // Subsequent turn on `model-b` must not inherit `test`'s idx=0 row (512,000); falls back to `model-b`'s native size.
  const modelBStart = events.length
  await sendUsagePrompt(manager, 'p3', 'usage-stream:20000:1000000')
  const modelBWindows = events
    .slice(modelBStart)
    .filter((e) => e.type === 'session.usage')
    .map((e) => e.usage.contextWindowMaxTokens)
  assert.deepEqual(modelBWindows, [1_000_000, 1_000_000])
})

test('SQLite compaction reader orders strictly by idx DESC across large payloads and trailing incomplete rows', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-sqlite-order-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  await mkdir(brainRoot, { recursive: true })
  const idOrder = '77777777-7777-4777-8777-777777777777'
  writeConversationDb(home, idOrder, {
    gen: [
      { idx: 0, data: encodeGenMetadataMaxContext(1_000_000) },
      {
        idx: 1,
        data: Buffer.concat([
          encodeProtoField(99, Buffer.alloc(40_000, 0x41)),
          encodeGenMetadataMaxContext(512_000),
        ]),
      },
      { idx: 2, data: encodeProtoField(1, encodeProtoField(2, 1)) },
      { idx: 3, data: encodeProtoField(1, encodeProtoField(2, 2)) },
    ],
  })
  assert.deepEqual(await readConversationCompactionWindow(brainRoot, idOrder), {
    maxTokens: 512_000,
    genIdx: 3,
  })
})

test('SQLite compaction reader handles missing/corrupt DBs and rejects symlink escapes', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-sqlite-reader-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  await mkdir(brainRoot, { recursive: true })
  const idMissing = '22222222-2222-4222-8222-222222222222'
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idMissing),
    { genIdx: -1 },
  )

  const convDir = join(home, '.gemini/antigravity-cli/conversations')
  await mkdir(convDir, { recursive: true })
  const idCorrupt = '99999999-9999-4999-8999-999999999999'
  await writeFile(join(convDir, `${idCorrupt}.db`), 'not-sqlite')
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idCorrupt),
    { genIdx: -1 },
  )

  const escapedTarget = join(home, 'outside.db')
  await writeFile(escapedTarget, 'not-a-session-db')
  const idSymlinkDb = '44444444-4444-4444-8444-444444444444'
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
    readConversationCompactionWindow(escapedConvBrain, idMissing),
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

test('SQLite gen_metadata reader extracts estimated_tokens_used alongside max_context_tokens without stale leakage', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-sqlite-estimated-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  await mkdir(brainRoot, { recursive: true })

  const idBoth = '88888888-8888-4888-8888-888888888881'
  writeConversationDb(home, idBoth, {
    gen: [
      {
        idx: 0,
        data: encodeGenMetadataContextUsage({
          maxTokens: 1_000_000,
          estimatedTokens: 5_000,
        }),
      },
      {
        idx: 1,
        data: encodeGenMetadataContextUsage({
          maxTokens: 512_000,
          estimatedTokens: 18_500,
        }),
      },
    ],
  })
  assert.deepEqual(await readConversationCompactionWindow(brainRoot, idBoth), {
    maxTokens: 512_000,
    estimatedTokens: 18_500,
    genIdx: 1,
  })

  // Zero estimated_tokens_used is ignored and older row estimated_tokens_used is not leaked when newer row has maxTokens.
  const idNoLeak = '88888888-8888-4888-8888-888888888882'
  writeConversationDb(home, idNoLeak, {
    gen: [
      {
        idx: 0,
        data: encodeGenMetadataContextUsage({
          maxTokens: 1_000_000,
          estimatedTokens: 9_000,
        }),
      },
      {
        idx: 1,
        data: encodeGenMetadataContextUsage({
          maxTokens: 512_000,
          estimatedTokens: 0,
        }),
      },
    ],
  })
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idNoLeak),
    {
      maxTokens: 512_000,
      genIdx: 1,
    },
  )

  // Newer estimated-only row pairs with older maxTokens row, and estimated-only DB returns estimatedTokens without maxTokens.
  const idSplit = '88888888-8888-4888-8888-888888888883'
  writeConversationDb(home, idSplit, {
    gen: [
      {
        idx: 0,
        data: encodeGenMetadataContextUsage({
          maxTokens: 512_000,
          estimatedTokens: 10_000,
        }),
      },
      {
        idx: 1,
        data: encodeGenMetadataContextUsage({ estimatedTokens: 22_000 }),
      },
    ],
  })
  assert.deepEqual(await readConversationCompactionWindow(brainRoot, idSplit), {
    maxTokens: 512_000,
    estimatedTokens: 22_000,
    genIdx: 1,
  })
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idSplit, 0),
    {
      estimatedTokens: 22_000,
      genIdx: 1,
    },
  )
})

test('SQLite estimated_tokens_used acts as last-resort context fallback without overriding usage_update or transcript counts', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-estimated-fallback-'))
  const { events, wait, manager, latestUsage } = setupUsageManager(t, home)
  await openFreshUsageSession(manager, home)
  const opened = events.find((e) => e.type === 'session.opened')
  assert.ok(opened?.type === 'session.opened' && opened.persistence)
  const nativeId = (opened.persistence.data as { sessionId: string }).sessionId

  // 1. No usage_update and no transcript step tokens -> falls back to SQLite estimated_tokens_used + max_context_tokens.
  writeConversationDb(home, nativeId, {
    gen: [
      {
        idx: 0,
        data: encodeGenMetadataContextUsage({
          maxTokens: 512_000,
          estimatedTokens: 18_500,
        }),
      },
    ],
  })
  await sendUsagePrompt(manager, 'p1', 'usage-empty')
  assert.equal(latestUsage().contextWindowUsedTokens, 18_500)
  assert.equal(latestUsage().contextWindowMaxTokens, 512_000)
  assert.equal(latestUsage().inputTokens, 600_000)

  // 2. Empty subsequent turn with no new SQLite row does not republish stale estimatedTokens.
  const usageCountAfterP1 = events.filter(
    (e) => e.type === 'session.usage',
  ).length
  await sendUsagePrompt(manager, 'p2', 'cancel-only')
  assert.equal(
    events.filter((e) => e.type === 'session.usage').length,
    usageCountAfterP1,
  )

  // 3. Cancelled turn without ACP usage or transcript recovers new SQLite estimatedTokens without fabricating turn billing.
  writeConversationDb(home, nativeId, {
    gen: [
      {
        idx: 1,
        data: encodeGenMetadataContextUsage({
          maxTokens: 512_000,
          estimatedTokens: 24_000,
        }),
      },
    ],
  })
  await sendUsagePrompt(manager, 'p3', 'cancel-only')
  assert.equal(latestUsage().contextWindowUsedTokens, 24_000)
  assert.equal(latestUsage().contextWindowMaxTokens, 512_000)
  assert.equal(latestUsage().inputTokens, 600_000)

  // 4. Live usage_update.used takes precedence over SQLite estimated_tokens_used.
  writeConversationDb(home, nativeId, {
    gen: [
      {
        idx: 2,
        data: encodeGenMetadataContextUsage({
          maxTokens: 512_000,
          estimatedTokens: 99_999,
        }),
      },
    ],
  })
  await sendUsagePrompt(manager, 'p4', 'usage-stream:15000:1048576')
  assert.equal(latestUsage().contextWindowUsedTokens, 15_000)
  assert.equal(latestUsage().contextWindowMaxTokens, 512_000)

  // 5. Exact transcript_full.jsonl step tokens take precedence over SQLite estimated_tokens_used.
  writeConversationDb(home, nativeId, {
    gen: [
      {
        idx: 3,
        data: encodeGenMetadataContextUsage({
          maxTokens: 512_000,
          estimatedTokens: 88_888,
        }),
      },
    ],
  })
  await sendUsagePrompt(manager, 'p5', 'usage-cancel')
  assert.equal(latestUsage().contextWindowUsedTokens, 70_463)
  assert.equal(latestUsage().contextWindowMaxTokens, 512_000)

  // 6. Persisted session restore uses SQLite estimatedTokens only when transcript has no step counts.
  const restoreNoTranscriptId = '88888888-8888-4888-8888-888888888889'
  writeConversationDb(home, restoreNoTranscriptId, {
    gen: [
      {
        idx: 0,
        data: encodeGenMetadataContextUsage({
          maxTokens: 256_000,
          estimatedTokens: 31_400,
        }),
      },
    ],
  })
  await manager.handle({
    type: 'session.open',
    requestId: 'open-restore-estimated',
    sessionId: 'restored-est',
    history: 'skip',
    config: {
      cwd: home,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
      model: 'test',
    },
    persistence: { version: 1, data: { sessionId: restoreNoTranscriptId } },
  })
  await wait(
    (e) => e.type === 'session.usage' && e.sessionId === 'restored-est',
  )
  const restoredEstUsage = events.findLast(
    (e) => e.type === 'session.usage' && e.sessionId === 'restored-est',
  )
  assert.ok(restoredEstUsage?.type === 'session.usage')
  assert.equal(restoredEstUsage.usage.contextWindowUsedTokens, 31_400)
  assert.equal(restoredEstUsage.usage.contextWindowMaxTokens, 256_000)
  assert.equal(restoredEstUsage.usage.inputTokens, undefined)

  // 7. Persisted session restore prefers exact transcript_full.jsonl counts over SQLite estimatedTokens.
  const restoreWithTranscriptId = '88888888-8888-4888-8888-888888888890'
  const transcriptDir = join(
    home,
    '.gemini/antigravity-cli/brain',
    restoreWithTranscriptId,
    '.system_generated/logs',
  )
  await mkdir(transcriptDir, { recursive: true })
  await writeFile(
    join(transcriptDir, 'transcript_full.jsonl'),
    JSON.stringify({
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      input_tokens: 70,
      output_tokens: 3,
    }) + '\n',
  )
  writeConversationDb(home, restoreWithTranscriptId, {
    gen: [
      {
        idx: 0,
        data: encodeGenMetadataContextUsage({
          maxTokens: 256_000,
          estimatedTokens: 99_999,
        }),
      },
    ],
  })
  await manager.handle({
    type: 'session.open',
    requestId: 'open-restore-transcript',
    sessionId: 'restored-tx',
    history: 'skip',
    config: {
      cwd: home,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
      model: 'test',
    },
    persistence: { version: 1, data: { sessionId: restoreWithTranscriptId } },
  })
  await wait((e) => e.type === 'session.usage' && e.sessionId === 'restored-tx')
  const restoredTxUsage = events.findLast(
    (e) => e.type === 'session.usage' && e.sessionId === 'restored-tx',
  )
  assert.ok(restoredTxUsage?.type === 'session.usage')
  assert.equal(restoredTxUsage.usage.contextWindowUsedTokens, 73)
  assert.equal(restoredTxUsage.usage.contextWindowMaxTokens, 256_000)
})

test('SQLite compaction reader caches by stat fingerprint, invalidates on .db/.db-wal edits, and reopens on lower minGenIdx', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-sqlite-cache-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  await mkdir(brainRoot, { recursive: true })
  const id = '33333333-3333-4333-8333-333333333331'

  let openCount = 0
  const openDb = (path: string) => {
    openCount += 1
    return new DatabaseSync(path, { readOnly: true, timeout: 50 })
  }

  writeConversationDb(home, id, {
    gen: [
      {
        idx: 0,
        data: encodeGenMetadataContextUsage({
          maxTokens: 512_000,
          estimatedTokens: 12_000,
        }),
      },
      {
        idx: 1,
        data: encodeGenMetadataContextUsage({
          maxTokens: 512_000,
          estimatedTokens: 19_000,
        }),
      },
    ],
  })

  // First read opens SQLite once and populates cache.
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, id, -1, { openDb }),
    { maxTokens: 512_000, estimatedTokens: 19_000, genIdx: 1 },
  )
  assert.equal(openCount, 1)

  // Repeated read with identical cursor reuses cache without reopening SQLite.
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, id, -1, { openDb }),
    { maxTokens: 512_000, estimatedTokens: 19_000, genIdx: 1 },
  )
  assert.equal(openCount, 1)

  // Advancing minGenIdx to >= cached genIdx short-circuits without reopening SQLite.
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, id, 1, { openDb }),
    { genIdx: 1 },
  )
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, id, 4, { openDb }),
    { genIdx: 4 },
  )
  assert.equal(openCount, 1)

  // Modifying .db invalidates the fingerprint and reopens SQLite for the new row.
  writeConversationDb(home, id, {
    gen: [
      {
        idx: 2,
        data: encodeGenMetadataContextUsage({
          maxTokens: 256_000,
          estimatedTokens: 25_000,
        }),
      },
    ],
  })
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, id, 1, { openDb }),
    { maxTokens: 256_000, estimatedTokens: 25_000, genIdx: 2 },
  )
  assert.equal(openCount, 2)

  // Asking for a lower minGenIdx than the cached cursor reopens SQLite to scan full history.
  const idLowerCursor = '33333333-3333-4333-8333-333333333332'
  writeConversationDb(home, idLowerCursor, {
    gen: [
      {
        idx: 0,
        data: encodeGenMetadataContextUsage({
          maxTokens: 1_000_000,
          estimatedTokens: 42_000,
        }),
      },
    ],
  })
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idLowerCursor, 0, {
      openDb,
    }),
    { genIdx: 0 },
  )
  assert.equal(openCount, 3)
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idLowerCursor, -1, {
      openDb,
    }),
    { maxTokens: 1_000_000, estimatedTokens: 42_000, genIdx: 0 },
  )
  assert.equal(openCount, 4)

  // Modifying .db-wal (while .db itself stays unchanged) invalidates the fingerprint and reopens SQLite.
  const idWal = '33333333-3333-4333-8333-333333333333'
  const convDir = join(home, '.gemini/antigravity-cli/conversations')
  const walWriter = new DatabaseSync(join(convDir, `${idWal}.db`))
  t.after(() => walWriter.close())
  walWriter.exec('PRAGMA journal_mode = WAL')
  walWriter.exec(
    'CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)',
  )
  const insWal = walWriter.prepare(
    'INSERT INTO gen_metadata (idx, data) VALUES (?, ?)',
  )
  insWal.run(
    0,
    encodeGenMetadataContextUsage({
      maxTokens: 512_000,
      estimatedTokens: 10_000,
    }),
  )
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idWal, -1, { openDb }),
    { maxTokens: 512_000, estimatedTokens: 10_000, genIdx: 0 },
  )
  assert.equal(openCount, 5)
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idWal, -1, { openDb }),
    { maxTokens: 512_000, estimatedTokens: 10_000, genIdx: 0 },
  )
  assert.equal(openCount, 5)
  insWal.run(
    1,
    encodeGenMetadataContextUsage({
      maxTokens: 256_000,
      estimatedTokens: 33_000,
    }),
  )
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idWal, 0, { openDb }),
    { maxTokens: 256_000, estimatedTokens: 33_000, genIdx: 1 },
  )
  assert.equal(openCount, 6)
})

test('warm SQLite compaction cache never bypasses symlink validation on .db or .db-wal', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-sqlite-cache-symlink-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  const convDir = join(home, '.gemini/antigravity-cli/conversations')
  await mkdir(brainRoot, { recursive: true })
  const outsideTarget = join(home, 'outside.db')
  await writeFile(outsideTarget, 'outside')

  const idReplaceDb = '33333333-3333-4333-8333-333333333334'
  writeConversationDb(home, idReplaceDb, {
    gen: [{ idx: 0, data: encodeGenMetadataMaxContext(512_000) }],
  })
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idReplaceDb),
    { maxTokens: 512_000, genIdx: 0 },
  )
  await rm(join(convDir, `${idReplaceDb}.db`))
  await symlink(outsideTarget, join(convDir, `${idReplaceDb}.db`))
  await assert.rejects(
    readConversationCompactionWindow(brainRoot, idReplaceDb),
    { name: 'BrainEscapeError' },
  )

  const idAddWalSymlink = '33333333-3333-4333-8333-333333333335'
  writeConversationDb(home, idAddWalSymlink, {
    gen: [{ idx: 0, data: encodeGenMetadataMaxContext(512_000) }],
  })
  assert.deepEqual(
    await readConversationCompactionWindow(brainRoot, idAddWalSymlink),
    { maxTokens: 512_000, genIdx: 0 },
  )
  await symlink(outsideTarget, join(convDir, `${idAddWalSymlink}.db-wal`))
  await assert.rejects(
    readConversationCompactionWindow(brainRoot, idAddWalSymlink),
    { name: 'BrainEscapeError' },
  )
})

test('mid-turn usage_update advancing sqlGenIdx preserves turnEstimatedTokens for wakeup end-of-turn fallback', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-sqlgen-est-cache-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const nativeId = '44444444-4444-4444-8444-444444444444'
  const sender = '55555555-5555-4555-8555-555555555555'
  const brainDir = join(
    home,
    '.gemini/antigravity-cli/brain',
    nativeId,
    '.system_generated/logs',
  )
  await mkdir(brainDir, { recursive: true })
  const events: Array<
    import('@getpaseo/plugin/server/provider').ProviderEvent
  > = []
  const env = isolatedEnv(home)
  const manager = new SessionManager({
    launch: { command: 'agy', args: ['acp'], env },
    usage: new AgyUsageSource({ env }),
    capabilities: [
      'prompt.message',
      'prompt.command',
      'prompt.image',
      'session.configure',
      'session.list',
      'session.persistence',
      'session.can_stop_runtime',
      'permission',
      'permission.tool_policy',
      'session.subsession',
    ],
    emit: (event) => events.push(event),
    commandWaitTimeoutMs: 50,
    connectAcp: (_l, _cwd, _e, client) => {
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => ({
          protocolVersion: 1,
          agentInfo: { name: 'Antigravity ACP Server', version: '1.5.12' },
          agentCapabilities: { loadSession: true },
        }),
        newSession: async () => {
          void client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'available_commands_update',
              availableCommands: [],
            },
          })
          return {
            sessionId: nativeId,
            configOptions: [
              {
                id: 'model',
                name: 'Model',
                category: 'model',
                type: 'select',
                currentValue: 'model-a',
                options: [{ value: 'model-a', name: 'Model A' }],
              },
            ],
          }
        },
        closeSession: async () => ({}),
        prompt: async () => {
          writeConversationDb(home, nativeId, {
            gen: [
              {
                idx: 0,
                data: encodeGenMetadataContextUsage({
                  maxTokens: 512_000,
                  estimatedTokens: 88_000,
                }),
              },
            ],
          })
          // Live usage_update advances sqlGenIdx to 0 while caching turnEstimatedTokens = 88_000.
          await client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'usage_update',
              used: 40_000,
              size: 1_000_000,
            },
          })
          // Post-RPC wakeup completes without per-step token counters in transcript_full.jsonl,
          // so end-of-turn settlement must fall back to the cached 88_000 estimatedTokens even
          // though sqlGenIdx is already at 0 and no new gen_metadata row was written.
          await writeFile(
            join(brainDir, 'transcript_full.jsonl'),
            [
              { step_index: 1, type: 'USER_INPUT', content: 'run' },
              {
                step_index: 2,
                type: 'PLANNER_RESPONSE',
                status: 'DONE',
                content: 'waiting',
              },
              {
                step_index: 3,
                type: 'SYSTEM_MESSAGE',
                content: `<SYSTEM_MESSAGE>\n[Message] sender=${sender} priority=MESSAGE_PRIORITY_HIGH content=done\n</SYSTEM_MESSAGE>`,
              },
              {
                step_index: 4,
                type: 'PLANNER_RESPONSE',
                status: 'DONE',
                content: 'wakeup reply without token fields',
              },
            ]
              .map((row) => JSON.stringify(row) + '\n')
              .join(''),
          )
          return { stopReason: 'end_turn' as const }
        },
        cancel: async () => {},
      } as unknown as ReturnType<
        typeof import('../server/transport.ts').connectAcp
      >['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {},
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return false
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => manager.close())
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
  await manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm1',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'go' }] },
    },
  })
  const usageEvents = events.filter(
    (
      e,
    ): e is Extract<
      import('@getpaseo/plugin/server/provider').ProviderEvent,
      { type: 'session.usage' }
    > => e.type === 'session.usage',
  )
  assert.equal(usageEvents.at(-1)?.usage.contextWindowMaxTokens, 512_000)
  assert.equal(usageEvents.at(-1)?.usage.contextWindowUsedTokens, 88_000)
})
