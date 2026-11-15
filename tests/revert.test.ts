import { test } from 'node:test'
import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { createHash } from 'node:crypto'
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  symlink,
  writeFile,
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import {
  PROVIDER_CAPABILITIES,
  type ProviderCapability,
} from '@getpaseo/plugin/server/provider'
import {
  captureTurnFileCheckpoint,
  forkSessionCheckpoints,
  readSessionCheckpoints,
  recordJournalToolEdit,
  removeSessionCheckpoints,
  restoreSessionFileCheckpoint,
} from '../server/checkpoints.ts'
import { dataDirectory } from '../server/preflight.ts'
import { createProvider } from '../server/provider.ts'
import { AgyUsageSource } from '../server/usage.ts'
import { collectProviderEvents, writeBrainTranscript } from './helpers.ts'

const execFileAsync = promisify(execFile)

async function runGit(cwd: string, args: string[]) {
  return await execFileAsync('git', args, { cwd })
}

function pbVarintVal(value: number): Buffer {
  return Buffer.from(
    [(value & 0x7f) | (Math.min(1, value >>> 7) << 7), value >>> 7].slice(
      0,
      1 + Math.min(1, value >>> 7),
    ),
  )
}

function pbVarint(fieldNum: number, value: number): Buffer {
  return Buffer.concat([pbVarintVal((fieldNum << 3) | 0), pbVarintVal(value)])
}

function pbBytes(fieldNum: number, payload: Buffer): Buffer {
  return Buffer.concat([
    pbVarintVal((fieldNum << 3) | 2),
    pbVarintVal(payload.byteLength),
    payload,
  ])
}

function pbStr(fieldNum: number, text: string): Buffer {
  return pbBytes(fieldNum, Buffer.from(text, 'utf8'))
}

function createNativeSessionDb(appDataDir: string, nativeId: string) {
  const dbPath = join(appDataDir, 'conversations', `${nativeId}.db`)
  const db = new DatabaseSync(dbPath)
  try {
    db.exec(`
      CREATE TABLE trajectory_meta (
        cascade_id TEXT PRIMARY KEY,
        trajectory_id TEXT,
        created_at INTEGER,
        updated_at INTEGER,
        source INTEGER
      );
      CREATE TABLE steps (
        idx INTEGER PRIMARY KEY,
        step_type INTEGER,
        data BLOB,
        size INTEGER
      );
      CREATE TABLE gen_metadata (
        idx INTEGER PRIMARY KEY,
        data BLOB,
        size INTEGER
      );
      CREATE TABLE executor_metadata (
        idx INTEGER PRIMARY KEY,
        data BLOB
      );
      CREATE TABLE parent_references (
        idx INTEGER PRIMARY KEY,
        data BLOB
      );
      CREATE TABLE battle_mode_infos (
        idx INTEGER PRIMARY KEY,
        data BLOB
      );
      CREATE TABLE trajectory_metadata_blob (
        id TEXT PRIMARY KEY,
        data BLOB
      );
    `)
    db.prepare(
      'INSERT INTO trajectory_meta (cascade_id, trajectory_id, created_at, updated_at, source) VALUES (?, ?, ?, ?, ?)',
    ).run(nativeId, 'traj-original', 1700000000, 1700000100, 1)

    const stepStmt = db.prepare(
      'INSERT INTO steps (idx, step_type, data, size) VALUES (?, ?, ?, ?)',
    )
    const stepDefs: ReadonlyArray<readonly [number, number, string]> = [
      [0, 14, 'first question'],
      [1, 15, 'first answer'],
      [2, 14, 'second question'],
      [3, 15, 'spawning subagent'],
      [4, 21, 'subagent output'],
      [5, 22, 'wakeup delivery'],
      [6, 15, 'second answer'],
    ]
    stepDefs.forEach(([idx, stepType, label]) => {
      const data = pbStr(1, label)
      stepStmt.run(idx, stepType, data, data.byteLength)
    })

    const genStmt = db.prepare(
      'INSERT INTO gen_metadata (idx, data, size) VALUES (?, ?, ?)',
    )
    const genRows: readonly Buffer[] = [
      Buffer.concat([pbStr(1, 'gen-0'), pbBytes(2, Buffer.from([0]))]),
      Buffer.concat([
        pbStr(1, 'gen-straddle'),
        pbBytes(2, Buffer.from([1, 2, 3])),
      ]),
      Buffer.concat([pbStr(1, 'gen-5'), pbBytes(2, Buffer.from([5]))]),
    ]
    genRows.forEach((data, idx) => {
      genStmt.run(idx, data, data.byteLength)
    })

    const execStmt = db.prepare(
      'INSERT INTO executor_metadata (idx, data) VALUES (?, ?)',
    )
    const execRows: readonly Buffer[] = [
      Buffer.concat([pbStr(1, 'exec-0'), pbVarint(3, 1)]),
      Buffer.concat([pbStr(1, 'exec-1'), pbVarint(3, 3)]),
    ]
    execRows.forEach((data, idx) => {
      execStmt.run(idx, data)
    })

    const parentStmt = db.prepare(
      'INSERT INTO parent_references (idx, data) VALUES (?, ?)',
    )
    const parentRows: readonly Buffer[] = [
      Buffer.concat([
        pbStr(1, 'traj-anc-0'),
        pbVarint(2, 1),
        pbVarint(5, 2),
        pbStr(6, '00000000-0000-4000-8000-000000000000'),
      ]),
      Buffer.concat([
        pbStr(1, 'traj-anc-1'),
        pbVarint(2, 3),
        pbVarint(5, 2),
        pbStr(6, '00000000-0000-4000-8000-000000000001'),
      ]),
    ]
    parentRows.forEach((data, idx) => {
      parentStmt.run(idx, data)
    })

    const battleStmt = db.prepare(
      'INSERT INTO battle_mode_infos (idx, data) VALUES (?, ?)',
    )
    const battleRows: readonly Buffer[] = [
      Buffer.concat([pbStr(2, 'winner-a'), pbVarint(3, 1)]),
      Buffer.concat([pbStr(2, ''), pbVarint(3, 0)]),
      Buffer.concat([pbStr(2, 'winner-b'), pbVarint(3, 3)]),
    ]
    battleRows.forEach((data, idx) => {
      battleStmt.run(idx, data)
    })

    const metaBlob = Buffer.concat([pbStr(1, 'meta-title'), pbStr(6, nativeId)])
    db.prepare(
      'INSERT INTO trajectory_metadata_blob (id, data) VALUES (?, ?)',
    ).run('main', metaBlob)
  } finally {
    db.close()
  }
}

function createSummariesDb(appDataDir: string, nativeId: string) {
  const dbPath = join(appDataDir, 'conversation_summaries.db')
  const db = new DatabaseSync(dbPath)
  try {
    db.exec(`
      CREATE TABLE conversation_summaries (
        conversation_id TEXT PRIMARY KEY,
        title TEXT,
        step_count INTEGER,
        last_modified_time TEXT,
        last_user_input_step_index INTEGER,
        not_fully_idle INTEGER,
        killed INTEGER,
        raw_summary BLOB
      );
    `)
    const rawSummary = Buffer.concat([
      pbStr(1, 'First question title'),
      pbVarint(2, 6),
      pbStr(4, 'traj-original'),
      pbVarint(16, 2),
      pbStr(19, nativeId),
    ])
    db.prepare(
      `INSERT INTO conversation_summaries (
        conversation_id, title, step_count, last_modified_time,
        last_user_input_step_index, not_fully_idle, killed, raw_summary
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      nativeId,
      'First question title',
      6,
      '2026-01-01 00:00:13.000+00:00',
      2,
      1,
      0,
      rawSummary,
    )
  } finally {
    db.close()
  }
}

async function seedNativeSessionArtifacts(
  appDataDir: string,
  nativeId: string,
) {
  const childKeepId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
  const childDropId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
  const brainDir = join(appDataDir, 'brain')

  await mkdir(join(appDataDir, 'conversations'), { recursive: true })
  createNativeSessionDb(appDataDir, nativeId)
  createSummariesDb(appDataDir, nativeId)

  const brainRows = [
    {
      step_index: 0,
      type: 'USER_INPUT',
      created_at: '2026-01-01T00:00:00.000Z',
      content: '<USER_REQUEST>\nfirst question\n</USER_REQUEST>',
    },
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      created_at: '2026-01-01T00:00:01.000Z',
      content: `first answer ${'x'.repeat(110_000)}`,
      input_tokens: 1200,
      output_tokens: 50,
    },
    {
      step_index: 2,
      type: 'USER_INPUT',
      created_at: '2026-01-01T00:00:10.000Z',
      content: '<USER_REQUEST>\nsecond question\n</USER_REQUEST>',
    },
    {
      step_index: 3,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      created_at: '2026-01-01T00:00:11.000Z',
      tool_calls: [{ id: 'spawn-bbb', name: 'invoke_subagent', args: {} }],
      input_tokens: 2000,
      output_tokens: 80,
    },
    {
      step_index: 4,
      type: 'GENERIC',
      status: 'DONE',
      created_at: '2026-01-01T00:00:12.000Z',
      content: JSON.stringify({ conversationId: childDropId }),
    },
    {
      step_index: 5,
      type: 'SYSTEM_MESSAGE',
      created_at: '2026-01-01T00:00:12.500Z',
      content: `[Message] timestamp=2026-01-01T00:00:01.500Z sender=${nativeId}/task-1 priority=MESSAGE_PRIORITY_HIGH\nbackground done`,
    },
    {
      step_index: 6,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      created_at: '2026-01-01T00:00:13.000Z',
      content: 'second answer',
      input_tokens: 2400,
      output_tokens: 100,
    },
  ]

  await writeBrainTranscript(brainDir, nativeId, brainRows)
  await writeBrainTranscript(brainDir, childDropId, [
    {
      step_index: 0,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      content: 'child turn done',
    },
  ])

  const logsDir = join(brainDir, nativeId, '.system_generated/logs')
  const jsonlPayload = brainRows.map((r) => JSON.stringify(r)).join('\n') + '\n'
  await writeFile(join(logsDir, 'transcript.jsonl'), jsonlPayload)
  await Promise.all(
    ['transcript', 'transcript_full'].map(async (kind) => {
      const chunkDir = join(logsDir, 'chunks', kind)
      await mkdir(chunkDir, { recursive: true })
      await writeFile(join(chunkDir, '00000000.jsonl'), 'stale-chunk-0')
      await writeFile(join(chunkDir, '00000002.jsonl'), 'stale-chunk-2')
    }),
  )

  const subDir = join(brainDir, nativeId, '.system_generated/subagents')
  await mkdir(subDir, { recursive: true })
  await writeFile(
    join(subDir, `${childKeepId}.json`),
    JSON.stringify({
      conversationId: childKeepId,
      spawnStepIndex: 1,
      state: 'completed',
    }),
  )
  await writeFile(
    join(subDir, `${childDropId}.json`),
    JSON.stringify({
      conversationId: childDropId,
      spawnStepIndex: 3,
      spawnCallId: 'spawn-bbb',
      state: 'completed',
    }),
  )

  const msgDir = join(brainDir, nativeId, '.system_generated/messages')
  await mkdir(join(msgDir, 'undelivered'), { recursive: true })
  await writeFile(
    join(msgDir, 'keep.json'),
    JSON.stringify({
      id: 'keep',
      recipient: nativeId,
      sender: `${nativeId}/main`,
      priority: 'MESSAGE_PRIORITY_LOW',
      timestamp: '2026-01-01T00:00:01.000Z',
      sourceMetadata: { tool: { conversationId: nativeId, stepIndex: 1 } },
    }),
  )
  await writeFile(
    join(msgDir, 'drop-orphan-wakeup.json'),
    JSON.stringify({
      id: 'drop-orphan-wakeup',
      recipient: nativeId,
      sender: `${nativeId}/task-1`,
      priority: 'MESSAGE_PRIORITY_HIGH',
      timestamp: '2026-01-01T00:00:01.500Z',
      sourceMetadata: { tool: { conversationId: nativeId, stepIndex: 1 } },
    }),
  )
  await writeFile(
    join(msgDir, 'drop-step.json'),
    JSON.stringify({
      id: 'drop-step',
      recipient: nativeId,
      sender: `${nativeId}/main`,
      priority: 'MESSAGE_PRIORITY_LOW',
      timestamp: '2026-01-01T00:00:01.000Z',
      sourceMetadata: { tool: { conversationId: nativeId, stepIndex: 2 } },
    }),
  )
  await writeFile(
    join(msgDir, 'drop-subagent.json'),
    JSON.stringify({
      id: 'drop-subagent',
      recipient: nativeId,
      sender: childDropId,
      priority: 'MESSAGE_PRIORITY_LOW',
      timestamp: '2026-01-01T00:00:05.000Z',
      sourceMetadata: { tool: { conversationId: childDropId, stepIndex: 0 } },
    }),
  )
  await writeFile(
    join(msgDir, 'drop-ts.json'),
    JSON.stringify({
      id: 'drop-ts',
      recipient: nativeId,
      sender: childKeepId,
      priority: 'MESSAGE_PRIORITY_LOW',
      timestamp: '2026-01-01T00:00:11.000Z',
      sourceMetadata: { tool: { conversationId: childKeepId, stepIndex: 0 } },
    }),
  )
  await writeFile(
    join(msgDir, 'read.json'),
    JSON.stringify({
      keep: true,
      'drop-orphan-wakeup': true,
      'drop-step': true,
      'drop-subagent': true,
      'drop-ts': true,
    }),
  )
  await writeFile(
    join(msgDir, 'undelivered', 'stale.json'),
    JSON.stringify({ id: 'stale' }),
  )

  const stepsDir = join(brainDir, nativeId, '.system_generated/steps')
  await mkdir(join(stepsDir, '1'), { recursive: true })
  await mkdir(join(stepsDir, '2'), { recursive: true })
  await writeFile(join(stepsDir, '1', 'output.txt'), 'keep')
  await writeFile(join(stepsDir, '2', 'output.txt'), 'drop')
}

async function openHarness(
  cwd: string,
  env: Record<string, string>,
  capabilities: readonly ProviderCapability[] = PROVIDER_CAPABILITIES,
) {
  const usage = new AgyUsageSource()
  const provider = await createProvider(usage).connect({
    versions: [1],
    capabilities,
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: { PATH: process.env.PATH ?? '', ...env },
    },
  })
  const { events, wait } = collectProviderEvents(provider)
  const config = {
    cwd,
    env: {},
    mcpServers: {},
    settings: {},
    persist: true,
  }
  return { provider, events, wait, config }
}

test('capabilities include revert scopes and reject invalid, active-turn, and concurrent reverts', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-guards-'))
  const workDir = join(home, 'ws')
  await mkdir(workDir, { recursive: true })
  const filteredCaps = PROVIDER_CAPABILITIES.filter(
    (c) => c !== 'session.revert.files' && c !== 'session.revert.both',
  )
  const h = await openHarness(workDir, { HOME: home }, filteredCaps)
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-1',
      sessionId: 's-guard',
      config: h.config,
      history: 'replay',
    })
    const opened = await h.wait((e) => e.type === 'session.opened')
    assert(opened.type === 'session.opened')
    assert.ok(opened.capabilities.includes('session.revert.conversation'))
    assert.ok(!opened.capabilities.includes('session.revert.files'))
    assert.ok(!opened.capabilities.includes('session.revert.both'))
    await h.wait((e) => e.type === 'session.ready')

    // Missing/filtered capability rejects.
    await assert.rejects(
      h.provider.send({
        type: 'session.revert',
        requestId: 'rev-cap',
        sessionId: 's-guard',
        scope: 'files',
        token: { version: 1, kind: 'agy-acp-turn', afterStepIndex: -1 },
      }),
      /Provider does not support session\.revert\.files/,
    )

    // Invalid token emits request.failed.
    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-bad-token',
      sessionId: 's-guard',
      scope: 'conversation',
      token: { bad: true },
    })
    const badTokenErr = await h.wait(
      (e) => e.type === 'request.failed' && e.requestId === 'rev-bad-token',
    )
    assert(badTokenErr.type === 'request.failed')
    assert.match(badTokenErr.error.message, /Invalid or missing revert token/)

    // Active turn rejects session.revert while waiting for permission.
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-guard',
      prompt: {
        clientMessageId: 'msg-active',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'wait-perm' }],
        },
      },
    })
    const perm = await h.wait((e) => e.type === 'session.permission')
    assert(perm.type === 'session.permission')
    const userItem = await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'msg-active',
    )
    assert(
      userItem.type === 'timeline.item' &&
        userItem.item.type === 'user_message',
    )
    const token = userItem.item.revertToken
    assert(token !== undefined)

    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-during-turn',
      sessionId: 's-guard',
      scope: 'conversation',
      token: { version: 1, kind: 'agy-acp-turn', afterStepIndex: -1 },
    })
    const activeErr = await h.wait(
      (e) => e.type === 'request.failed' && e.requestId === 'rev-during-turn',
    )
    assert(activeErr.type === 'request.failed')
    assert.match(
      activeErr.error.message,
      /Cannot revert while a turn is active/,
    )

    await h.provider.send({
      type: 'session.permission',
      sessionId: 's-guard',
      permissionId: perm.request.id,
      response: { behavior: 'allow', selectedActionId: 'yes' },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')

    // Concurrent session.revert on the same session must reject the second request.
    await Promise.all([
      h.provider.send({
        type: 'session.revert',
        requestId: 'rev-conc-1',
        sessionId: 's-guard',
        scope: 'conversation',
        token,
      }),
      h.provider.send({
        type: 'session.revert',
        requestId: 'rev-conc-2',
        sessionId: 's-guard',
        scope: 'conversation',
        token,
      }),
    ])
    const concErr = await h.wait(
      (e) => e.type === 'request.failed' && e.requestId === 'rev-conc-2',
    )
    assert(concErr.type === 'request.failed')
    assert.match(concErr.error.message, /Session revert is already in progress/)
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-conc-1',
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('conversation revert branches all 7 SQLite tables, Brain chunks/messages, summaries, closes pruned subagents, and continues live prompts', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-conv-'))
  const workDir = join(home, 'ws')
  await mkdir(workDir, { recursive: true })
  const oldNativeId = '11111111-1111-4111-8111-111111111111'
  const childDropId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
  const appDataDir = dataDirectory(undefined, { HOME: home }, workDir)
  await seedNativeSessionArtifacts(appDataDir, oldNativeId)

  const h = await openHarness(workDir, {
    HOME: home,
    AGY_TEST_CHECKPOINT_HISTORY: '1',
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-imported',
      sessionId: 's-conv',
      config: h.config,
      persistence: { version: 1, data: { sessionId: oldNativeId } },
      history: 'replay',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's-conv')
    await h.wait(
      (e) =>
        e.type === 'session.ready' &&
        e.sessionId === `s-conv:child:${childDropId}`,
    )

    const replayedUsers = h.events
      .map((e) => e)
      .filter(
        (
          e,
        ): e is Extract<
          (typeof h.events)[number],
          { type: 'timeline.item' }
        > & {
          item: Extract<
            Extract<
              (typeof h.events)[number],
              { type: 'timeline.item' }
            >['item'],
            { type: 'user_message' }
          >
        } =>
          e.type === 'timeline.item' &&
          e.sessionId === 's-conv' &&
          e.item.type === 'user_message',
      )
      .map((e) => e.item)
    assert.equal(replayedUsers.length, 2)
    const token1 = replayedUsers[0].revertToken
    const token2 = replayedUsers[1].revertToken
    assert(token1 !== undefined)
    assert(token2 !== undefined)

    // Imported session without checkpoints refuses `both` without forking DB.
    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-no-files',
      sessionId: 's-conv',
      scope: 'both',
      token: token2,
    })
    const noFilesErr = await h.wait(
      (e) => e.type === 'request.failed' && e.requestId === 'rev-no-files',
    )
    assert(noFilesErr.type === 'request.failed')
    assert.match(
      noFilesErr.error.message,
      /No workspace file checkpoint recorded/,
    )
    assert.equal(
      h.events.some((e) => e.type === 'session.persistence'),
      false,
    )

    // Revert conversation back before Turn 2 (cutoffStepIndex = 2).
    const beforeCount = h.events.length
    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-turn2',
      sessionId: 's-conv',
      scope: 'conversation',
      token: token2,
    })
    const persisted = await h.wait(
      (e) =>
        e.type === 'session.persistence' && h.events.indexOf(e) >= beforeCount,
    )
    assert(persisted.type === 'session.persistence')
    const forkedId = (persisted.persistence.data as { sessionId: string })
      .sessionId
    assert.notEqual(forkedId, oldNativeId)

    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-turn2',
    )
    await h.wait(
      (e) =>
        e.type === 'session.closed' &&
        e.sessionId === `s-conv:child:${childDropId}`,
    )

    const revSlice = h.events.slice(beforeCount)
    const revCompleted = revSlice.findIndex(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-turn2',
    )
    const revPersisted = revSlice.findIndex(
      (e) => e.type === 'session.persistence',
    )
    assert.ok(revPersisted >= 0 && revCompleted > revPersisted)
    assert.deepEqual(
      revSlice.filter(
        (e) => e.type === 'timeline.item' && e.sessionId === 's-conv',
      ),
      [],
      'conversation revert must reload silently without duplicate timeline.item events',
    )

    // Verify all 7 tables and protobuf payloads in the forked SQLite DB.
    const forkedDb = new DatabaseSync(
      join(appDataDir, 'conversations', `${forkedId}.db`),
    )
    let forkedTrajId = ''
    try {
      const meta = forkedDb
        .prepare('SELECT cascade_id, trajectory_id FROM trajectory_meta')
        .get() as { cascade_id: string; trajectory_id: string }
      assert.equal(meta.cascade_id, forkedId)
      assert.notEqual(meta.trajectory_id, 'traj-original')
      forkedTrajId = meta.trajectory_id

      const stepIdxs = (
        forkedDb.prepare('SELECT idx FROM steps ORDER BY idx').all() as Array<{
          idx: number
        }>
      ).map((r) => r.idx)
      assert.deepEqual(stepIdxs, [0, 1])

      const genRows = forkedDb
        .prepare('SELECT idx, data, size FROM gen_metadata ORDER BY idx')
        .all() as Array<{ idx: number; data: Uint8Array; size: number }>
      assert.equal(genRows.length, 2)
      assert.equal(genRows[1].idx, 1)
      const expectedGen1 = Buffer.concat([
        pbStr(1, 'gen-straddle'),
        pbBytes(2, Buffer.from([1])),
      ])
      assert.deepEqual(Buffer.from(genRows[1].data), expectedGen1)
      assert.equal(genRows[1].size, expectedGen1.byteLength)

      const execRows = forkedDb
        .prepare('SELECT idx, data FROM executor_metadata ORDER BY idx')
        .all() as Array<{ idx: number; data: Uint8Array }>
      assert.equal(execRows.length, 1)
      assert.deepEqual(
        Buffer.from(execRows[0].data),
        Buffer.concat([pbStr(1, 'exec-0'), pbVarint(3, 1)]),
      )

      const battleRows = forkedDb
        .prepare('SELECT idx, data FROM battle_mode_infos ORDER BY idx')
        .all() as Array<{ idx: number; data: Uint8Array }>
      assert.equal(battleRows.length, 1)
      assert.deepEqual(
        Buffer.from(battleRows[0].data),
        Buffer.concat([pbStr(2, 'winner-a'), pbVarint(3, 1)]),
      )

      const parentRows = forkedDb
        .prepare('SELECT idx, data FROM parent_references ORDER BY idx')
        .all() as Array<{ idx: number; data: Uint8Array }>
      assert.equal(parentRows.length, 2)
      assert.deepEqual(
        Buffer.from(parentRows[1].data),
        Buffer.concat([
          pbStr(1, 'traj-original'),
          pbVarint(2, 1),
          pbVarint(3, 1),
          pbVarint(4, 15),
          pbVarint(5, 2),
          pbStr(6, oldNativeId),
        ]),
      )

      const metaBlobRow = forkedDb
        .prepare("SELECT data FROM trajectory_metadata_blob WHERE id = 'main'")
        .get() as { data: Uint8Array }
      assert.deepEqual(
        Buffer.from(metaBlobRow.data),
        Buffer.concat([pbStr(1, 'meta-title'), pbStr(6, forkedId)]),
      )
    } finally {
      forkedDb.close()
    }

    // Verify conversation_summaries.db clone and raw_summary protobuf patching.
    const summariesDb = new DatabaseSync(
      join(appDataDir, 'conversation_summaries.db'),
    )
    try {
      const summaryRow = summariesDb
        .prepare(
          'SELECT step_count, last_user_input_step_index, not_fully_idle, raw_summary FROM conversation_summaries WHERE conversation_id = ?',
        )
        .get(forkedId) as {
        step_count: number
        last_user_input_step_index: number
        not_fully_idle: number
        raw_summary: Uint8Array
      }
      assert.equal(summaryRow.step_count, 2)
      assert.equal(summaryRow.last_user_input_step_index, 0)
      assert.equal(summaryRow.not_fully_idle, 0)
      assert.deepEqual(
        Buffer.from(summaryRow.raw_summary),
        Buffer.concat([
          pbStr(1, 'First question title'),
          pbVarint(2, 2),
          pbStr(4, forkedTrajId),
          pbStr(19, forkedId),
        ]),
      )
    } finally {
      summariesDb.close()
    }

    // Verify Brain transcript chunks, subagents, messages, read.json, and steps.
    const forkedBrain = join(appDataDir, 'brain', forkedId, '.system_generated')
    const forkedTranscriptBuf = await readFile(
      join(forkedBrain, 'logs/transcript_full.jsonl'),
    )
    const parsedSteps = forkedTranscriptBuf
      .toString('utf8')
      .trim()
      .split('\n')
      .map((line) => (JSON.parse(line) as { step_index: number }).step_index)
    assert.deepEqual(parsedSteps, [0, 1])

    const fullChunkNames = (
      await readdir(join(forkedBrain, 'logs/chunks/transcript_full'))
    ).sort()
    assert.deepEqual(fullChunkNames, ['00000000.jsonl', '00000001.jsonl'])
    const c0 = await readFile(
      join(forkedBrain, 'logs/chunks/transcript_full/00000000.jsonl'),
    )
    const c1 = await readFile(
      join(forkedBrain, 'logs/chunks/transcript_full/00000001.jsonl'),
    )
    assert.equal(c0.byteLength, 102_400)
    assert.equal(
      Buffer.concat([c0, c1]).equals(forkedTranscriptBuf),
      true,
      'rewritten byte chunks must concatenate back to exact filtered transcript',
    )

    await stat(
      join(forkedBrain, 'subagents/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.json'),
    )
    await assert.rejects(
      stat(join(forkedBrain, `subagents/${childDropId}.json`)),
    )

    const keptMsg = JSON.parse(
      await readFile(join(forkedBrain, 'messages/keep.json'), 'utf8'),
    ) as {
      recipient: string
      sender: string
      sourceMetadata: { tool: { conversationId: string } }
    }
    assert.equal(keptMsg.recipient, forkedId)
    assert.equal(keptMsg.sender, `${forkedId}/main`)
    assert.equal(keptMsg.sourceMetadata.tool.conversationId, forkedId)
    await assert.rejects(
      stat(join(forkedBrain, 'messages/drop-orphan-wakeup.json')),
    )
    await assert.rejects(stat(join(forkedBrain, 'messages/drop-step.json')))
    await assert.rejects(stat(join(forkedBrain, 'messages/drop-subagent.json')))
    await assert.rejects(stat(join(forkedBrain, 'messages/drop-ts.json')))
    await assert.rejects(stat(join(forkedBrain, 'messages/undelivered')))
    assert.deepEqual(
      JSON.parse(
        await readFile(join(forkedBrain, 'messages/read.json'), 'utf8'),
      ),
      { keep: true },
    )
    await stat(join(forkedBrain, 'steps/1/output.txt'))
    await assert.rejects(stat(join(forkedBrain, 'steps/2/output.txt')))

    // Post-revert live prompt on the SAME open session must succeed on forkedId.
    const postRevertStart = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-conv',
      prompt: {
        clientMessageId: 'post-revert-msg',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'usage-post-revert' }],
        },
      },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= postRevertStart &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )

    // Reverting Turn 1 creates a fresh empty session, resets context usage to 0, and continues accepting prompts.
    const turn1RevertStart = h.events.length
    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-turn1',
      sessionId: 's-conv',
      scope: 'conversation',
      token: token1,
    })
    const freshPersisted = await h.wait(
      (e) =>
        e.type === 'session.persistence' &&
        e.sessionId === 's-conv' &&
        h.events.indexOf(e) >= turn1RevertStart,
    )
    assert(freshPersisted.type === 'session.persistence')
    const freshId = (freshPersisted.persistence.data as { sessionId: string })
      .sessionId
    assert.notEqual(freshId, forkedId)
    assert.notEqual(freshId, oldNativeId)
    await h.wait(
      (e) =>
        e.type === 'session.usage' &&
        e.sessionId === 's-conv' &&
        e.usage.contextWindowUsedTokens === 0 &&
        h.events.indexOf(e) >= turn1RevertStart,
    )
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-turn1',
    )

    const freshPromptStart = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-conv',
      prompt: {
        clientMessageId: 'post-fresh-msg',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'usage-post-fresh' }],
        },
      },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= freshPromptStart &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('conversation revert uses session/resume when advertised and accepts subsequent prompts', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-resume-'))
  const workDir = join(home, 'ws')
  await mkdir(workDir, { recursive: true })
  const acpLog = join(home, 'acp.log')
  const oldNativeId = '33333333-3333-4333-8333-333333333333'
  const appDataDir = dataDirectory(undefined, { HOME: home }, workDir)
  await seedNativeSessionArtifacts(appDataDir, oldNativeId)

  const h = await openHarness(workDir, {
    HOME: home,
    AGY_TEST_RESUME_CAP: '1',
    AGY_TEST_CHECKPOINT_HISTORY: '1',
    AGY_TEST_LOG: acpLog,
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-resume-rev',
      sessionId: 's-resume-rev',
      config: h.config,
      persistence: { version: 1, data: { sessionId: oldNativeId } },
      history: 'replay',
    })
    await h.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 's-resume-rev',
    )

    const users = h.events
      .map((e) => e)
      .filter(
        (
          e,
        ): e is Extract<
          (typeof h.events)[number],
          { type: 'timeline.item' }
        > & {
          item: Extract<
            Extract<
              (typeof h.events)[number],
              { type: 'timeline.item' }
            >['item'],
            { type: 'user_message' }
          >
        } =>
          e.type === 'timeline.item' &&
          e.sessionId === 's-resume-rev' &&
          e.item.type === 'user_message',
      )
      .map((e) => e.item)
    const token2 = users[1].revertToken
    assert(token2 !== undefined)

    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-resume-t2',
      sessionId: 's-resume-rev',
      scope: 'conversation',
      token: token2,
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-resume-t2',
    )

    const logLines = (await readFile(acpLog, 'utf8')).trim().split('\n')
    assert.deepEqual(
      logLines.filter((l) => l.startsWith('resume')),
      ['resume'],
    )

    const promptStart = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-resume-rev',
      prompt: {
        clientMessageId: 'after-resume-revert',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'usage-after-resume-rev' }],
        },
      },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= promptStart &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('Git revert restores edits/deletions/symlinks/directories/unborn nested repos while preserving pre-turn staged state, plus scope=both', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-git-'))
  const repoDir = join(home, 'repo')
  const outsideVictim = join(home, 'outside-git-victim.txt')
  const outsideKeep = join(home, 'outside-git-keep.txt')
  await writeFile(outsideVictim, 'victim-untouched')
  await writeFile(outsideKeep, 'keep-untouched')

  await mkdir(repoDir, { recursive: true })
  await runGit(repoDir, ['init'])
  await runGit(repoDir, ['config', 'user.email', 'test@example.com'])
  await runGit(repoDir, ['config', 'user.name', 'Test User'])
  await writeFile(join(repoDir, 'tracked.txt'), 'v1-clean\n')
  await writeFile(join(repoDir, 'keep.txt'), 'keep-me\n')
  await writeFile(join(repoDir, 'dirty.txt'), 'dirty-committed\n')
  await writeFile(join(repoDir, 'staged.txt'), 'staged-committed\n')
  await runGit(repoDir, ['add', '.'])
  await runGit(repoDir, ['commit', '-m', 'init'])

  // User already has unstaged, staged, and a pre-existing unborn nested .git dir before Turn 1.
  await writeFile(join(repoDir, 'dirty.txt'), 'user-unstaged-edit\n')
  await writeFile(join(repoDir, 'staged.txt'), 'user-staged-edit\n')
  await runGit(repoDir, ['add', 'staged.txt'])
  await mkdir(join(repoDir, 'nested-pre'), { recursive: true })
  await runGit(join(repoDir, 'nested-pre'), ['init'])

  const appDataDir = dataDirectory(undefined, { HOME: home }, repoDir)
  const h = await openHarness(repoDir, { HOME: home })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-git',
      sessionId: 's-git',
      config: h.config,
      history: 'replay',
    })
    const opened = await h.wait((e) => e.type === 'session.opened')
    assert(opened.type === 'session.opened')
    const nativeId = (opened.persistence?.data as { sessionId: string })
      .sessionId
    await h.wait((e) => e.type === 'session.ready')

    // Turn 1 captures git checkpoint including unborn `nested-pre` and staged edits.
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-git',
      prompt: {
        clientMessageId: 'git-t1',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'usage-git1' }],
        },
      },
    })
    const userMsg1 = await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'git-t1',
    )
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')

    const cpsAfterT1 = await readSessionCheckpoints(appDataDir, nativeId)
    assert.equal(cpsAfterT1[0].kind, 'git')
    assert(cpsAfterT1[0].kind === 'git')
    assert.deepEqual(cpsAfterT1[0].unbornDirs, ['nested-pre'])

    // Mutate workspace including symlink replacement & directory swap attacks.
    await rm(join(repoDir, 'tracked.txt'))
    await symlink(outsideVictim, join(repoDir, 'tracked.txt'))
    await symlink(outsideKeep, join(repoDir, 'evil-added-link'))
    await writeFile(join(repoDir, 'dirty.txt'), 'clobbered-by-agent\n')
    await writeFile(join(repoDir, 'created.txt'), 'new-file-from-agent\n')
    await rm(join(repoDir, 'keep.txt'))
    await mkdir(join(repoDir, 'keep.txt', 'inner'), { recursive: true })
    await writeFile(join(repoDir, 'keep.txt', 'inner', 'f.txt'), 'dir-trap')

    // Seed native SQLite DB and run Turn 2, then test `scope: 'both'` + post-revert prompt.
    await mkdir(join(appDataDir, 'conversations'), { recursive: true })
    createNativeSessionDb(appDataDir, nativeId)
    await writeFile(join(repoDir, 'turn2-only.txt'), 'before-t2\n')
    const turn2EventStart = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-git',
      prompt: {
        clientMessageId: 'git-t2',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'usage-git2' }],
        },
      },
    })
    const userMsg2 = await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'git-t2',
    )
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= turn2EventStart &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )

    await writeFile(join(repoDir, 'turn2-only.txt'), 'mutated-in-t2\n')
    await writeFile(join(repoDir, 'extra-t2.txt'), 'unwanted-t2\n')
    await mkdir(join(repoDir, 'sub dir', 'nested-new'), { recursive: true })
    await runGit(join(repoDir, 'sub dir', 'nested-new'), ['init'])

    assert(
      userMsg1.type === 'timeline.item' &&
        userMsg1.item.type === 'user_message',
    )
    assert(
      userMsg2.type === 'timeline.item' &&
        userMsg2.item.type === 'user_message',
    )
    const token1 = userMsg1.item.revertToken
    const token2 = userMsg2.item.revertToken
    assert(token1 !== undefined)
    assert(token2 !== undefined)

    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-git-both',
      sessionId: 's-git',
      scope: 'both',
      token: token2,
    })
    const persistedBoth = await h.wait((e) => e.type === 'session.persistence')
    assert(persistedBoth.type === 'session.persistence')
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-git-both',
    )

    assert.equal(
      await readFile(join(repoDir, 'turn2-only.txt'), 'utf8'),
      'before-t2\n',
    )
    await assert.rejects(stat(join(repoDir, 'extra-t2.txt')))
    await assert.rejects(lstat(join(repoDir, 'sub dir', 'nested-new')))
    await stat(join(repoDir, 'nested-pre', '.git'))

    const newNativeId = (
      persistedBoth.persistence.data as { sessionId: string }
    ).sessionId
    const keptCheckpoints = await readSessionCheckpoints(
      appDataDir,
      newNativeId,
    )
    assert.equal(keptCheckpoints.length, 1)

    // Live post-revert prompt must succeed after `scope: 'both'`.
    const postBothPromptStart = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-git',
      prompt: {
        clientMessageId: 'git-post-both',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'usage-git-post' }],
        },
      },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= postBothPromptStart &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )

    // Revert Turn 1 files: defends against leaf symlink escape and EISDIR directory swap.
    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-git-files',
      sessionId: 's-git',
      scope: 'files',
      token: token1,
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-git-files',
    )

    assert.equal(
      (await lstat(join(repoDir, 'tracked.txt'))).isSymbolicLink(),
      false,
    )
    assert.equal(
      await readFile(join(repoDir, 'tracked.txt'), 'utf8'),
      'v1-clean\n',
    )
    assert.equal((await lstat(join(repoDir, 'keep.txt'))).isDirectory(), false)
    assert.equal(await readFile(join(repoDir, 'keep.txt'), 'utf8'), 'keep-me\n')
    assert.equal(
      await readFile(join(repoDir, 'dirty.txt'), 'utf8'),
      'user-unstaged-edit\n',
    )
    assert.equal(
      await readFile(join(repoDir, 'staged.txt'), 'utf8'),
      'user-staged-edit\n',
    )
    await assert.rejects(lstat(join(repoDir, 'created.txt')))
    await assert.rejects(lstat(join(repoDir, 'evil-added-link')))
    assert.equal(await readFile(outsideVictim, 'utf8'), 'victim-untouched')
    assert.equal(await readFile(outsideKeep, 'utf8'), 'keep-untouched')

    const { stdout: cachedDiff } = await runGit(repoDir, [
      'diff',
      '--cached',
      '--name-only',
    ])
    assert.equal(cachedDiff.trim(), 'staged.txt')
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('Unborn root Git repo (0 commits) checkpoints and reverts files cleanly', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-unborn-'))
  const repoDir = join(home, 'unborn-root')
  await mkdir(repoDir, { recursive: true })
  await runGit(repoDir, ['init'])
  await writeFile(join(repoDir, 'initial.txt'), 'v0')

  const appDataDir = dataDirectory(undefined, { HOME: home }, repoDir)
  const h = await openHarness(repoDir, { HOME: home })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-unborn',
      sessionId: 's-unborn',
      config: h.config,
      history: 'replay',
    })
    const opened = await h.wait((e) => e.type === 'session.opened')
    assert(opened.type === 'session.opened')
    const nativeId = (opened.persistence?.data as { sessionId: string })
      .sessionId
    await h.wait((e) => e.type === 'session.ready')

    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-unborn',
      prompt: {
        clientMessageId: 'unborn-t1',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'usage-unborn' }],
        },
      },
    })
    const userItem = await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'unborn-t1',
    )
    assert(
      userItem.type === 'timeline.item' &&
        userItem.item.type === 'user_message',
    )
    const token = userItem.item.revertToken
    assert(token !== undefined)
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')

    const cps = await readSessionCheckpoints(appDataDir, nativeId)
    assert.equal(cps[0].kind, 'git')

    await writeFile(join(repoDir, 'initial.txt'), 'mutated')
    await writeFile(join(repoDir, 'added.txt'), 'extra')

    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-unborn',
      sessionId: 's-unborn',
      scope: 'files',
      token,
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-unborn',
    )

    assert.equal(await readFile(join(repoDir, 'initial.txt'), 'utf8'), 'v0')
    await assert.rejects(lstat(join(repoDir, 'added.txt')))
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('Non-Git journal revert restores content, preserves 0o755 mode, and resists leaf symlink escape and EISDIR', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-nongit-'))
  const workDir = join(home, 'workspace')
  const outsideVictim = join(home, 'outside-journal-victim.txt')
  const outsideKeep = join(home, 'outside-journal-keep.txt')
  await writeFile(outsideVictim, 'secret-victim')
  await writeFile(outsideKeep, 'secret-keep')

  await mkdir(workDir, { recursive: true })
  await writeFile(join(workDir, 'existing.txt'), 'before-v1')
  await writeFile(join(workDir, 'oldtext-only.txt'), 'from-diff')
  await chmod(join(workDir, 'oldtext-only.txt'), 0o755)
  await writeFile(join(workDir, 'unchanged.txt'), 'stay')

  const h = await openHarness(workDir, {
    HOME: home,
    AGY_TEST_REVERT_DIR: workDir,
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-nongit',
      sessionId: 's-nongit',
      config: h.config,
      history: 'replay',
    })
    await h.wait((e) => e.type === 'session.ready')

    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-nongit',
      prompt: {
        clientMessageId: 'ng-1',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'revert-journal-edit' }],
        },
      },
    })
    const perm = await h.wait((e) => e.type === 'session.permission')
    assert(perm.type === 'session.permission')
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's-nongit',
      permissionId: perm.request.id,
      response: { behavior: 'allow', selectedActionId: 'allow' },
    })
    const userItem = await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'ng-1',
    )
    assert(
      userItem.type === 'timeline.item' &&
        userItem.item.type === 'user_message',
    )
    const token = userItem.item.revertToken
    assert(token !== undefined)
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')

    assert.equal(
      await readFile(join(workDir, 'existing.txt'), 'utf8'),
      'after-v2',
    )
    assert.equal(
      await readFile(join(workDir, 'new-file.txt'), 'utf8'),
      'created',
    )
    assert.equal(
      await readFile(join(workDir, 'oldtext-only.txt'), 'utf8'),
      'to-diff',
    )

    // Replace targets with symlink traps and an EISDIR directory before reverting.
    await rm(join(workDir, 'existing.txt'))
    await symlink(outsideVictim, join(workDir, 'existing.txt'))
    await rm(join(workDir, 'new-file.txt'))
    await symlink(outsideKeep, join(workDir, 'new-file.txt'))
    await rm(join(workDir, 'oldtext-only.txt'))
    await mkdir(join(workDir, 'oldtext-only.txt', 'nested'), {
      recursive: true,
    })
    await writeFile(
      join(workDir, 'oldtext-only.txt', 'nested', 'f.txt'),
      'dir-trap',
    )

    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-ng-files',
      sessionId: 's-nongit',
      scope: 'files',
      token,
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-ng-files',
    )

    assert.equal(
      (await lstat(join(workDir, 'existing.txt'))).isSymbolicLink(),
      false,
    )
    assert.equal(
      await readFile(join(workDir, 'existing.txt'), 'utf8'),
      'before-v1',
    )
    assert.equal(
      (await lstat(join(workDir, 'oldtext-only.txt'))).isDirectory(),
      false,
    )
    assert.equal(
      await readFile(join(workDir, 'oldtext-only.txt'), 'utf8'),
      'from-diff',
    )
    assert.equal(
      (await stat(join(workDir, 'oldtext-only.txt'))).mode & 0o777,
      0o755,
    )
    assert.equal(await readFile(join(workDir, 'unchanged.txt'), 'utf8'), 'stay')
    await assert.rejects(lstat(join(workDir, 'new-file.txt')))
    assert.equal(await readFile(outsideVictim, 'utf8'), 'secret-victim')
    assert.equal(await readFile(outsideKeep, 'utf8'), 'secret-keep')
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('Mixed journal to git workspace transition restores edits across both turns when reverting to the journal turn', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-mixed-'))
  const workDir = join(home, 'workspace')
  await mkdir(workDir, { recursive: true })
  await writeFile(join(workDir, 'existing.txt'), 'before-v1')
  await writeFile(join(workDir, 'oldtext-only.txt'), 'from-diff')

  const h = await openHarness(workDir, {
    HOME: home,
    AGY_TEST_REVERT_DIR: workDir,
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-mixed',
      sessionId: 's-mixed',
      config: h.config,
      history: 'replay',
    })
    await h.wait((e) => e.type === 'session.ready')

    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-mixed',
      prompt: {
        clientMessageId: 'mixed-t1',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'revert-journal-edit' }],
        },
      },
    })
    const perm1 = await h.wait((e) => e.type === 'session.permission')
    assert(perm1.type === 'session.permission')
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's-mixed',
      permissionId: perm1.request.id,
      response: { behavior: 'allow', selectedActionId: 'allow' },
    })
    const userItem1 = await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'mixed-t1',
    )
    assert(
      userItem1.type === 'timeline.item' &&
        userItem1.item.type === 'user_message',
    )
    const token1 = userItem1.item.revertToken
    assert(token1 !== undefined)
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')

    // Initialize git before Turn 2 so Turn 2 captures a `git` checkpoint after Turn 1's `journal` checkpoint.
    await runGit(workDir, ['init'])

    const perm2WaitStart = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-mixed',
      prompt: {
        clientMessageId: 'mixed-t2',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'wait-perm' }],
        },
      },
    })
    const perm2 = await h.wait(
      (e) =>
        e.type === 'session.permission' &&
        h.events.indexOf(e) >= perm2WaitStart,
    )
    assert(perm2.type === 'session.permission')
    await writeFile(join(workDir, 'existing.txt'), 'after-git-v3')
    await writeFile(join(workDir, 'git-turn-added.txt'), 'from-git-turn')
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's-mixed',
      permissionId: perm2.request.id,
      response: { behavior: 'allow', selectedActionId: 'yes' },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= perm2WaitStart &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )

    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-mixed',
      sessionId: 's-mixed',
      scope: 'files',
      token: token1,
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-mixed',
    )

    assert.equal(
      await readFile(join(workDir, 'existing.txt'), 'utf8'),
      'before-v1',
    )
    assert.equal(
      await readFile(join(workDir, 'oldtext-only.txt'), 'utf8'),
      'from-diff',
    )
    await assert.rejects(lstat(join(workDir, 'new-file.txt')))
    await assert.rejects(lstat(join(workDir, 'git-turn-added.txt')))
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('Failed conversation revert activation rolls back metadata, closes aborted fork, removes leaked artifacts, and keeps original session usable', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-rollback-'))
  const workDir = join(home, 'workspace')
  await mkdir(workDir, { recursive: true })
  const acpLog = join(home, 'acp.log')
  const oldNativeId = '44444444-4444-4444-8444-444444444444'
  const childDropId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
  const appDataDir = dataDirectory(undefined, { HOME: home }, workDir)
  await seedNativeSessionArtifacts(appDataDir, oldNativeId)

  const h = await openHarness(workDir, {
    HOME: home,
    AGY_TEST_CHECKPOINT_HISTORY: '1',
    AGY_TEST_FAIL_CONFIG_ON_REVERT: '1',
    AGY_TEST_LOG: acpLog,
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-rb',
      sessionId: 's-rb',
      config: h.config,
      persistence: { version: 1, data: { sessionId: oldNativeId } },
      history: 'replay',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's-rb')

    await h.provider.send({
      type: 'session.configure',
      requestId: 'cfg-high',
      sessionId: 's-rb',
      changes: { thinkingOption: 'high' },
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'cfg-high',
    )

    const users = h.events
      .map((e) => e)
      .filter(
        (
          e,
        ): e is Extract<
          (typeof h.events)[number],
          { type: 'timeline.item' }
        > & {
          item: Extract<
            Extract<
              (typeof h.events)[number],
              { type: 'timeline.item' }
            >['item'],
            { type: 'user_message' }
          >
        } =>
          e.type === 'timeline.item' &&
          e.sessionId === 's-rb' &&
          e.item.type === 'user_message',
      )
      .map((e) => e.item)
    const token2 = users[1].revertToken
    assert(token2 !== undefined)

    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-fail-cfg',
      sessionId: 's-rb',
      scope: 'conversation',
      token: token2,
    })
    const err = await h.wait(
      (e) => e.type === 'request.failed' && e.requestId === 'rev-fail-cfg',
    )
    assert(err.type === 'request.failed')
    assert.match(err.error.message, /simulated config failure/)

    // Forked on-disk DB, Brain folder, and summary row must be cleaned up.
    assert.deepEqual(await readdir(join(appDataDir, 'conversations')), [
      `${oldNativeId}.db`,
    ])
    assert.deepEqual(
      (await readdir(join(appDataDir, 'brain'))).sort(),
      [oldNativeId, childDropId].sort(),
    )
    const sumDb = new DatabaseSync(
      join(appDataDir, 'conversation_summaries.db'),
      { readOnly: true },
    )
    try {
      const summaryIds = (
        sumDb
          .prepare('SELECT conversation_id FROM conversation_summaries')
          .all() as Array<{ conversation_id: string }>
      ).map((r) => r.conversation_id)
      assert.deepEqual(summaryIds, [oldNativeId])
    } finally {
      sumDb.close()
    }

    // Aborted fork must be closed on ACP transport and original session remains usable.
    const logLines = (await readFile(acpLog, 'utf8')).trim().split('\n')
    assert.equal(logLines.includes('close'), true)
    assert.equal(
      h.events.some(
        (e) =>
          e.type === 'session.persistence' &&
          e.sessionId === 's-rb' &&
          (e.persistence.data as { sessionId: string }).sessionId !==
            oldNativeId,
      ),
      false,
    )

    const postFailStart = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-rb',
      prompt: {
        clientMessageId: 'after-failed-revert',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'usage-after-failed-revert' }],
        },
      },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= postFailStart &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('Interleaved journal -> git -> journal file checkpoints fail closed without mutating workspace', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-interleaved-'))
  const workDir = join(home, 'workspace')
  await mkdir(workDir, { recursive: true })
  const filePath = join(workDir, 'a.txt')
  await writeFile(filePath, 'v0')
  const appDataDir = dataDirectory(undefined, { HOME: home }, workDir)
  const nativeId = '55555555-5555-4555-8555-555555555555'

  try {
    const cp0 = await captureTurnFileCheckpoint(
      appDataDir,
      nativeId,
      workDir,
      -1,
    )
    await recordJournalToolEdit(appDataDir, nativeId, cp0.id, workDir, filePath)
    await writeFile(filePath, 'v1-journal')

    await runGit(workDir, ['init'])
    await runGit(workDir, ['config', 'user.name', 'Test'])
    await runGit(workDir, ['config', 'user.email', 'test@example.com'])
    await runGit(workDir, ['add', 'a.txt'])
    await runGit(workDir, ['commit', '-m', 'init'])
    await captureTurnFileCheckpoint(appDataDir, nativeId, workDir, 0)
    await writeFile(filePath, 'v2-git')

    await rm(join(workDir, '.git'), { recursive: true, force: true })
    const cp2 = await captureTurnFileCheckpoint(
      appDataDir,
      nativeId,
      workDir,
      1,
    )
    await recordJournalToolEdit(appDataDir, nativeId, cp2.id, workDir, filePath)
    await writeFile(filePath, 'v3-journal')

    await assert.rejects(
      restoreSessionFileCheckpoint(appDataDir, nativeId, cp0.id),
      /Cannot restore files across interleaved non-git and git turns/,
    )
    assert.equal(await readFile(filePath, 'utf8'), 'v3-journal')
  } finally {
    await rm(home, { recursive: true, force: true })
  }
})

test('removeSessionCheckpoints prunes orphaned blobs while preserving blobs shared with a forked session', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-blobs-'))
  const workDir = join(home, 'workspace')
  await mkdir(workDir, { recursive: true })
  const sharedPath = join(workDir, 'shared.txt')
  const onlyOldPath = join(workDir, 'only-old.txt')
  await writeFile(sharedPath, 'shared-v0')
  await writeFile(onlyOldPath, 'old-v0')
  const appDataDir = dataDirectory(undefined, { HOME: home }, workDir)
  const oldNativeId = '66666666-6666-4666-8666-666666666666'
  const newNativeId = '77777777-7777-4777-8777-777777777777'
  const sharedHash = createHash('sha256').update('shared-v0').digest('hex')
  const onlyOldHash = createHash('sha256').update('old-v0').digest('hex')
  const sessionsDir = join(appDataDir, 'paseo-checkpoints', 'sessions')
  const blobsDir = join(appDataDir, 'paseo-checkpoints', 'blobs')

  try {
    const cp0 = await captureTurnFileCheckpoint(
      appDataDir,
      oldNativeId,
      workDir,
      -1,
    )
    await recordJournalToolEdit(
      appDataDir,
      oldNativeId,
      cp0.id,
      workDir,
      sharedPath,
    )
    const cp1 = await captureTurnFileCheckpoint(
      appDataDir,
      oldNativeId,
      workDir,
      0,
    )
    await recordJournalToolEdit(
      appDataDir,
      oldNativeId,
      cp1.id,
      workDir,
      onlyOldPath,
    )
    await forkSessionCheckpoints(appDataDir, oldNativeId, newNativeId, 0)

    assert.deepEqual(
      (await readdir(blobsDir)).sort(),
      [onlyOldHash, sharedHash].sort(),
    )

    await removeSessionCheckpoints(appDataDir, oldNativeId)
    await assert.rejects(lstat(join(sessionsDir, `${oldNativeId}.json`)))
    await assert.rejects(lstat(join(blobsDir, onlyOldHash)))
    assert.equal(
      await readFile(join(blobsDir, sharedHash), 'utf8'),
      'shared-v0',
    )

    await removeSessionCheckpoints(appDataDir, newNativeId)
    await assert.rejects(lstat(join(sessionsDir, `${newNativeId}.json`)))
    assert.deepEqual(await readdir(blobsDir), [])
  } finally {
    await rm(home, { recursive: true, force: true })
  }
})

test('restoreGitTree rejects reverting across a directory replaced by an external symlink', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-symlink-dir-'))
  const workDir = join(home, 'workspace')
  const outsideDir = join(home, 'outside')
  await mkdir(join(workDir, 'nested', 'sub'), { recursive: true })
  await mkdir(outsideDir, { recursive: true })
  await writeFile(join(workDir, 'base.txt'), 'base')
  await writeFile(join(workDir, 'nested', 'sub', 'item.txt'), 'inside-v1')
  await writeFile(join(outsideDir, 'secret.txt'), 'keep-secret')
  const appDataDir = dataDirectory(undefined, { HOME: home }, workDir)
  const nativeId = '88888888-8888-4888-8888-888888888888'

  try {
    await runGit(workDir, ['init'])
    await runGit(workDir, ['config', 'user.name', 'Test'])
    await runGit(workDir, ['config', 'user.email', 'test@example.com'])
    await runGit(workDir, ['add', 'base.txt'])
    await runGit(workDir, ['commit', '-m', 'init'])

    const cp0 = await captureTurnFileCheckpoint(
      appDataDir,
      nativeId,
      workDir,
      -1,
    )
    await writeFile(join(workDir, '.gitignore'), 'nested\n')
    await rm(join(workDir, 'nested'), { recursive: true, force: true })
    await symlink(outsideDir, join(workDir, 'nested'))

    await assert.rejects(
      restoreSessionFileCheckpoint(appDataDir, nativeId, cp0.id),
      /Refusing to restore file across symlink: nested\/sub\/item\.txt/,
    )
    assert.equal(
      await readFile(join(outsideDir, 'secret.txt'), 'utf8'),
      'keep-secret',
    )
  } finally {
    await rm(home, { recursive: true, force: true })
  }
})

test('Failed non-USER_INPUT and zero-row turns revert files and prune error steps while keeping turnOrdinal aligned', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-nonuser-fail-'))
  const workDir = join(home, 'workspace')
  await mkdir(workDir, { recursive: true })
  await writeFile(join(workDir, 'existing.txt'), 'before-v1')
  const appDataDir = dataDirectory(undefined, { HOME: home }, workDir)

  const h = await openHarness(workDir, {
    HOME: home,
    AGY_TEST_REVERT_DIR: workDir,
    AGY_TEST_CHECKPOINT_HISTORY: '1',
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-nu',
      sessionId: 's-nu',
      config: h.config,
      history: 'replay',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's-nu')

    const t0Start = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-nu',
      prompt: {
        clientMessageId: 'nu-t0',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'revert-seed-turn0' }],
        },
      },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= t0Start &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )

    const t1Start = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-nu',
      prompt: {
        clientMessageId: 'nu-t1',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'revert-nonuser-fail' }],
        },
      },
    })
    const perm1 = await h.wait(
      (e) => h.events.indexOf(e) >= t1Start && e.type === 'session.permission',
    )
    assert(perm1.type === 'session.permission')
    const userItem1 = await h.wait(
      (e) =>
        h.events.indexOf(e) >= t1Start &&
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'nu-t1',
    )
    assert(
      userItem1.type === 'timeline.item' &&
        userItem1.item.type === 'user_message',
    )
    const token1 = userItem1.item.revertToken
    assert(token1 !== undefined)
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's-nu',
      permissionId: perm1.request.id,
      response: { behavior: 'allow', selectedActionId: 'allow' },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= t1Start &&
        e.type === 'session.turn' &&
        e.state === 'failed',
    )
    assert.equal(
      await readFile(join(workDir, 'existing.txt'), 'utf8'),
      'dirty-from-failed-turn',
    )

    const t2Start = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-nu',
      prompt: {
        clientMessageId: 'nu-t2',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'revert-post-turn' }],
        },
      },
    })
    const userItem2 = await h.wait(
      (e) =>
        h.events.indexOf(e) >= t2Start &&
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'nu-t2',
    )
    assert(
      userItem2.type === 'timeline.item' &&
        userItem2.item.type === 'user_message',
    )
    assert.equal(
      (userItem2.item.revertToken as { turnOrdinal?: number }).turnOrdinal,
      1,
    )
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= t2Start &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )

    const rev1Start = h.events.length
    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-nu-1',
      sessionId: 's-nu',
      scope: 'both',
      token: token1,
    })
    const persist1 = await h.wait(
      (e) =>
        h.events.indexOf(e) >= rev1Start && e.type === 'session.persistence',
    )
    assert(persist1.type === 'session.persistence')
    const fork1Id = (persist1.persistence.data as { sessionId: string })
      .sessionId
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-nu-1',
    )
    assert.equal(
      await readFile(join(workDir, 'existing.txt'), 'utf8'),
      'before-v1',
    )

    const db1 = new DatabaseSync(
      join(appDataDir, 'conversations', `${fork1Id}.db`),
      {
        readOnly: true,
      },
    )
    const stepIndices1 = (
      db1.prepare('SELECT idx FROM steps ORDER BY idx ASC').all() as Array<{
        idx: number
      }>
    ).map((r) => r.idx)
    db1.close()
    assert.deepEqual(stepIndices1, [0, 1])

    const brainSteps1 = (
      await readFile(
        join(
          appDataDir,
          'brain',
          fork1Id,
          '.system_generated/logs/transcript.jsonl',
        ),
        'utf8',
      )
    )
      .trim()
      .split('\n')
      .map((line) => (JSON.parse(line) as { step_index: number }).step_index)
    assert.deepEqual(brainSteps1, [0, 1])

    const t3Start = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-nu',
      prompt: {
        clientMessageId: 'nu-t3',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'revert-zerorow-fail' }],
        },
      },
    })
    const perm3 = await h.wait(
      (e) => h.events.indexOf(e) >= t3Start && e.type === 'session.permission',
    )
    assert(perm3.type === 'session.permission')
    const userItem3 = await h.wait(
      (e) =>
        h.events.indexOf(e) >= t3Start &&
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'nu-t3',
    )
    assert(
      userItem3.type === 'timeline.item' &&
        userItem3.item.type === 'user_message',
    )
    const token3 = userItem3.item.revertToken
    assert(token3 !== undefined)
    assert.equal((token3 as { turnOrdinal?: number }).turnOrdinal, 1)
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's-nu',
      permissionId: perm3.request.id,
      response: { behavior: 'allow', selectedActionId: 'allow' },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= t3Start &&
        e.type === 'session.turn' &&
        e.state === 'failed',
    )
    assert.equal(
      await readFile(join(workDir, 'existing.txt'), 'utf8'),
      'dirty-zero-row',
    )

    const rev3Start = h.events.length
    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-nu-3',
      sessionId: 's-nu',
      scope: 'both',
      token: token3,
    })
    const persist3 = await h.wait(
      (e) =>
        h.events.indexOf(e) >= rev3Start && e.type === 'session.persistence',
    )
    assert(persist3.type === 'session.persistence')
    const fork3Id = (persist3.persistence.data as { sessionId: string })
      .sessionId
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-nu-3',
    )
    assert.equal(
      await readFile(join(workDir, 'existing.txt'), 'utf8'),
      'before-v1',
    )

    const db3 = new DatabaseSync(
      join(appDataDir, 'conversations', `${fork3Id}.db`),
      {
        readOnly: true,
      },
    )
    const stepIndices3 = (
      db3.prepare('SELECT idx FROM steps ORDER BY idx ASC').all() as Array<{
        idx: number
      }>
    ).map((r) => r.idx)
    db3.close()
    assert.deepEqual(stepIndices3, [0, 1])
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('Revert refuses atomically before touching workspace files when runtime supports neither loadSession nor resume', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-no-reload-'))
  const workDir = join(home, 'workspace')
  await mkdir(workDir, { recursive: true })
  await writeFile(join(workDir, 'existing.txt'), 'before-v1')
  await writeFile(join(workDir, 'oldtext-only.txt'), 'from-diff')
  const appDataDir = dataDirectory(undefined, { HOME: home }, workDir)

  const h = await openHarness(workDir, {
    HOME: home,
    AGY_TEST_REVERT_DIR: workDir,
    AGY_TEST_CHECKPOINT_HISTORY: '1',
    AGY_TEST_NO_LOAD_CAP: '1',
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-noload',
      sessionId: 's-noload',
      config: h.config,
      history: 'replay',
    })
    const initOpened = await h.wait(
      (e) => e.type === 'session.opened' && e.sessionId === 's-noload',
    )
    assert(initOpened.type === 'session.opened')
    const origNativeId = (initOpened.persistence?.data as { sessionId: string })
      .sessionId
    await h.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 's-noload',
    )

    const t0Start = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-noload',
      prompt: {
        clientMessageId: 'nl-t0',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'revert-seed-turn0' }],
        },
      },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= t0Start &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )

    const t1Start = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-noload',
      prompt: {
        clientMessageId: 'nl-t1',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'revert-journal-edit' }],
        },
      },
    })
    const perm1 = await h.wait(
      (e) => h.events.indexOf(e) >= t1Start && e.type === 'session.permission',
    )
    assert(perm1.type === 'session.permission')
    const userItem1 = await h.wait(
      (e) =>
        h.events.indexOf(e) >= t1Start &&
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'nl-t1',
    )
    assert(
      userItem1.type === 'timeline.item' &&
        userItem1.item.type === 'user_message',
    )
    const token1 = userItem1.item.revertToken
    assert(token1 !== undefined)
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's-noload',
      permissionId: perm1.request.id,
      response: { behavior: 'allow', selectedActionId: 'allow' },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= t1Start &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )

    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-noload',
      sessionId: 's-noload',
      scope: 'both',
      token: token1,
    })
    const err = await h.wait(
      (e) => e.type === 'request.failed' && e.requestId === 'rev-noload',
    )
    assert(err.type === 'request.failed')
    assert.match(err.error.message, /does not support session\/load/)

    assert.equal(
      await readFile(join(workDir, 'existing.txt'), 'utf8'),
      'after-v2',
    )
    assert.equal(
      await readFile(join(workDir, 'new-file.txt'), 'utf8'),
      'created',
    )
    assert.deepEqual(await readdir(join(appDataDir, 'conversations')), [
      `${origNativeId}.db`,
    ])
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('config.persist === false revert omits session.persistence and cleans previous, forked, checkpoint, and summary artifacts on close', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-nopersist-'))
  const workDir = join(home, 'workspace')
  await mkdir(workDir, { recursive: true })
  await writeFile(join(workDir, 'existing.txt'), 'before-v1')
  await writeFile(join(workDir, 'oldtext-only.txt'), 'from-diff')
  const appDataDir = dataDirectory(undefined, { HOME: home }, workDir)

  const h = await openHarness(workDir, {
    HOME: home,
    AGY_TEST_REVERT_DIR: workDir,
    AGY_TEST_CHECKPOINT_HISTORY: '1',
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-np',
      sessionId: 's-np',
      config: { ...h.config, persist: false },
      history: 'replay',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's-np')

    const t0Start = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-np',
      prompt: {
        clientMessageId: 'np-t0',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'revert-seed-turn0' }],
        },
      },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= t0Start &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )
    const convBefore = await readdir(join(appDataDir, 'conversations'))
    assert.equal(convBefore.length, 1)
    const oldDbName = convBefore[0]
    const oldNativeId = oldDbName.replace(/\.db$/, '')

    const t1Start = h.events.length
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's-np',
      prompt: {
        clientMessageId: 'np-t1',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'revert-journal-edit' }],
        },
      },
    })
    const perm1 = await h.wait(
      (e) => h.events.indexOf(e) >= t1Start && e.type === 'session.permission',
    )
    assert(perm1.type === 'session.permission')
    const userItem1 = await h.wait(
      (e) =>
        h.events.indexOf(e) >= t1Start &&
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'np-t1',
    )
    assert(
      userItem1.type === 'timeline.item' &&
        userItem1.item.type === 'user_message',
    )
    const token1 = userItem1.item.revertToken
    assert(token1 !== undefined)
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's-np',
      permissionId: perm1.request.id,
      response: { behavior: 'allow', selectedActionId: 'allow' },
    })
    await h.wait(
      (e) =>
        h.events.indexOf(e) >= t1Start &&
        e.type === 'session.turn' &&
        e.state === 'completed',
    )

    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-np',
      sessionId: 's-np',
      scope: 'both',
      token: token1,
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-np',
    )

    assert.equal(
      h.events.some(
        (e) => e.type === 'session.persistence' && e.sessionId === 's-np',
      ),
      false,
    )
    assert.equal(
      await readFile(join(workDir, 'existing.txt'), 'utf8'),
      'before-v1',
    )
    await assert.rejects(lstat(join(workDir, 'new-file.txt')))

    const convAfterRevert = await readdir(join(appDataDir, 'conversations'))
    assert.equal(convAfterRevert.length, 1)
    assert.notEqual(convAfterRevert[0], oldDbName)
    const brainAfterRevert = await readdir(join(appDataDir, 'brain'))
    assert.equal(brainAfterRevert.length, 1)
    assert.notEqual(brainAfterRevert[0], oldNativeId)

    await h.provider.send({
      type: 'session.close',
      requestId: 'close-np',
      sessionId: 's-np',
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'close-np',
    )

    assert.deepEqual(await readdir(join(appDataDir, 'conversations')), [])
    assert.deepEqual(await readdir(join(appDataDir, 'brain')), [])
    assert.deepEqual(
      await readdir(join(appDataDir, 'paseo-checkpoints', 'sessions')),
      [],
    )
    assert.deepEqual(
      await readdir(join(appDataDir, 'paseo-checkpoints', 'blobs')),
      [],
    )
    const sumDb = new DatabaseSync(
      join(appDataDir, 'conversation_summaries.db'),
      { readOnly: true },
    )
    const sumRows = sumDb
      .prepare('SELECT conversation_id FROM conversation_summaries')
      .all()
    sumDb.close()
    assert.deepEqual(sumRows, [])
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})
