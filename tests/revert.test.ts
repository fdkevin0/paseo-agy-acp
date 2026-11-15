import { test } from 'node:test'
import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import {
  PROVIDER_CAPABILITIES,
  type ProviderCapability,
} from '@getpaseo/plugin/server/provider'
import { readSessionCheckpoints } from '../server/checkpoints.ts'
import { dataDirectory } from '../server/preflight.ts'
import { createProvider } from '../server/provider.ts'
import { AgyUsageSource } from '../server/usage.ts'
import { collectProviderEvents, writeBrainTranscript } from './helpers.ts'

const execFileAsync = promisify(execFile)

async function runGit(cwd: string, args: string[]) {
  return await execFileAsync('git', args, { cwd })
}

function createNativeSessionDb(appDataDir: string, nativeId: string) {
  const dbPath = join(appDataDir, 'conversations', `${nativeId}.db`)
  const db = new DatabaseSync(dbPath)
  try {
    db.exec(`
      CREATE TABLE trajectory_meta (
        cascade_id TEXT PRIMARY KEY,
        trajectory_id TEXT
      );
      CREATE TABLE steps (
        idx INTEGER PRIMARY KEY,
        step_type INTEGER,
        step_json TEXT
      );
      CREATE TABLE gen_metadata (
        idx INTEGER PRIMARY KEY,
        meta TEXT
      );
      CREATE TABLE executor_metadata (
        idx INTEGER PRIMARY KEY,
        meta TEXT
      );
      CREATE TABLE parent_references (
        idx INTEGER PRIMARY KEY,
        ref TEXT
      );
      CREATE TABLE battle_mode_infos (
        idx INTEGER PRIMARY KEY,
        info TEXT
      );
    `)
    db.prepare(
      'INSERT INTO trajectory_meta (cascade_id, trajectory_id) VALUES (?, ?)',
    ).run(nativeId, 'traj-original')
    for (const [idx, stepType, json] of [
      [0, 14, '{"type":"USER_INPUT","content":"first question"}'],
      [1, 15, '{"type":"PLANNER_RESPONSE","content":"first answer"}'],
      [2, 14, '{"type":"USER_INPUT","content":"second question"}'],
      [3, 15, '{"type":"PLANNER_RESPONSE","content":"second answer"}'],
      [4, 15, '{"type":"PLANNER_RESPONSE","content":"extra step"}'],
    ] as const) {
      db.prepare(
        'INSERT INTO steps (idx, step_type, step_json) VALUES (?, ?, ?)',
      ).run(idx, stepType, json)
      db.prepare('INSERT INTO gen_metadata (idx, meta) VALUES (?, ?)').run(
        idx,
        `gen-${idx}`,
      )
      db.prepare('INSERT INTO executor_metadata (idx, meta) VALUES (?, ?)').run(
        idx,
        `exec-${idx}`,
      )
      db.prepare('INSERT INTO parent_references (idx, ref) VALUES (?, ?)').run(
        idx,
        `ref-${idx}`,
      )
      db.prepare('INSERT INTO battle_mode_infos (idx, info) VALUES (?, ?)').run(
        idx,
        `battle-${idx}`,
      )
    }
  } finally {
    db.close()
  }
}

async function seedNativeSessionArtifacts(
  appDataDir: string,
  nativeId: string,
) {
  await mkdir(join(appDataDir, 'conversations'), { recursive: true })
  createNativeSessionDb(appDataDir, nativeId)
  const brainRows = [
    {
      step_index: 0,
      type: 'USER_INPUT',
      content: '<USER_REQUEST>\nfirst question\n</USER_REQUEST>',
    },
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      content: 'first answer',
    },
    {
      step_index: 2,
      type: 'USER_INPUT',
      content: '<USER_REQUEST>\nsecond question\n</USER_REQUEST>',
    },
    {
      step_index: 3,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      content: 'second answer',
    },
    {
      step_index: 4,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      content: 'extra step',
    },
  ]
  await writeBrainTranscript(join(appDataDir, 'brain'), nativeId, brainRows)
  const logsDir = join(appDataDir, 'brain', nativeId, '.system_generated/logs')
  await writeFile(
    join(logsDir, 'transcript.jsonl'),
    brainRows.map((r) => JSON.stringify(r)).join('\n') + '\n',
  )
  const subDir = join(
    appDataDir,
    'brain',
    nativeId,
    '.system_generated/subagents',
  )
  await mkdir(subDir, { recursive: true })
  await writeFile(
    join(subDir, 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.json'),
    JSON.stringify({
      conversationId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      spawnStepIndex: 1,
      state: 'completed',
    }),
  )
  await writeFile(
    join(subDir, 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb.json'),
    JSON.stringify({
      conversationId: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
      spawnStepIndex: 3,
      state: 'completed',
    }),
  )
  const msgDir = join(
    appDataDir,
    'brain',
    nativeId,
    '.system_generated/messages',
  )
  await mkdir(msgDir, { recursive: true })
  await writeFile(
    join(msgDir, 'keep.json'),
    JSON.stringify({ tool: { stepIndex: 1 } }),
  )
  await writeFile(
    join(msgDir, 'drop.json'),
    JSON.stringify({ tool: { stepIndex: 2 } }),
  )
  const stepsDir = join(
    appDataDir,
    'brain',
    nativeId,
    '.system_generated/steps',
  )
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

test('capabilities include revert scopes and respect host filtering + runtime guards', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-guards-'))
  const filteredCaps = PROVIDER_CAPABILITIES.filter(
    (c) => c !== 'session.revert.files' && c !== 'session.revert.both',
  )
  const h = await openHarness(process.cwd(), { HOME: home }, filteredCaps)
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-1',
      sessionId: 's-guard',
      config: h.config,
      history: 'replay',
    })
    const opened = await h.wait((e) => e.type === 'session.opened')
    assert.equal(opened.type, 'session.opened')
    if (opened.type !== 'session.opened') return
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
    assert.equal(badTokenErr.type, 'request.failed')
    if (badTokenErr.type === 'request.failed') {
      assert.match(badTokenErr.error.message, /Invalid or missing revert token/)
    }

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
    const userItem = await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'msg-active',
    )
    assert.equal(userItem.type, 'timeline.item')
    if (
      userItem.type === 'timeline.item' &&
      userItem.item.type === 'user_message'
    ) {
      assert.ok(userItem.item.revertToken)
    }

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
    assert.equal(activeErr.type, 'request.failed')
    if (activeErr.type === 'request.failed') {
      assert.match(
        activeErr.error.message,
        /Cannot revert while a turn is active/,
      )
    }

    if (perm.type === 'session.permission') {
      await h.provider.send({
        type: 'session.permission',
        sessionId: 's-guard',
        permissionId: perm.request.id,
        response: { behavior: 'allow', selectedActionId: 'yes' },
      })
    }
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('conversation revert forks SQLite DB + Brain at Turn 2 and reopens fresh on Turn 1', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-conv-'))
  const oldNativeId = '11111111-1111-4111-8111-111111111111'
  const appDataDir = dataDirectory(undefined, { HOME: home }, process.cwd())
  await seedNativeSessionArtifacts(appDataDir, oldNativeId)

  const h = await openHarness(process.cwd(), {
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
    await h.wait((e) => e.type === 'session.ready')

    const replayedUsers = h.events
      .filter(
        (e) => e.type === 'timeline.item' && e.item.type === 'user_message',
      )
      .map((e) =>
        e.type === 'timeline.item' && e.item.type === 'user_message'
          ? e.item
          : undefined,
      )
      .filter((x): x is NonNullable<typeof x> => Boolean(x))
    assert.equal(replayedUsers.length, 2)
    assert.ok(replayedUsers[0].revertToken)
    assert.ok(replayedUsers[1].revertToken)

    // Imported session without checkpoints refuses `files` and `both` without forking conversation.
    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-no-files',
      sessionId: 's-conv',
      scope: 'both',
      token: replayedUsers[1].revertToken!,
    })
    const noFilesErr = await h.wait(
      (e) => e.type === 'request.failed' && e.requestId === 'rev-no-files',
    )
    assert.equal(noFilesErr.type, 'request.failed')
    if (noFilesErr.type === 'request.failed') {
      assert.match(
        noFilesErr.error.message,
        /No workspace file checkpoint recorded/,
      )
    }
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
      token: replayedUsers[1].revertToken!,
    })
    const persisted = await h.wait((e) => e.type === 'session.persistence')
    assert.equal(persisted.type, 'session.persistence')
    if (persisted.type !== 'session.persistence') return
    const forkedId = (persisted.persistence.data as { sessionId: string })
      .sessionId
    assert.notEqual(forkedId, oldNativeId)

    const revCompleted = h.events
      .slice(beforeCount)
      .findIndex(
        (e) => e.type === 'request.completed' && e.requestId === 'rev-turn2',
      )
    const revPersisted = h.events
      .slice(beforeCount)
      .findIndex((e) => e.type === 'session.persistence')
    assert.ok(revPersisted >= 0 && revCompleted > revPersisted)

    // Verify forked SQLite DB rows & trajectory_meta.
    const forkedDb = new DatabaseSync(
      join(appDataDir, 'conversations', `${forkedId}.db`),
    )
    try {
      const meta = forkedDb
        .prepare('SELECT cascade_id, trajectory_id FROM trajectory_meta')
        .get() as { cascade_id: string; trajectory_id: string }
      assert.equal(meta.cascade_id, forkedId)
      assert.notEqual(meta.trajectory_id, 'traj-original')
      for (const table of [
        'steps',
        'gen_metadata',
        'executor_metadata',
        'parent_references',
        'battle_mode_infos',
      ]) {
        const rows = forkedDb
          .prepare(`SELECT idx FROM ${table} ORDER BY idx`)
          .all() as Array<{ idx: number }>
        assert.deepEqual(
          rows.map((r) => r.idx),
          [0, 1],
          `table ${table} should only retain idx < 2`,
        )
      }
    } finally {
      forkedDb.close()
    }

    // Verify forked Brain filtering (transcript, subagents, messages, steps).
    const forkedBrain = join(appDataDir, 'brain', forkedId, '.system_generated')
    const forkedTranscript = await readFile(
      join(forkedBrain, 'logs/transcript_full.jsonl'),
      'utf8',
    )
    const parsedSteps = forkedTranscript
      .trim()
      .split('\n')
      .map((line) => (JSON.parse(line) as { step_index: number }).step_index)
    assert.deepEqual(parsedSteps, [0, 1])
    await stat(
      join(forkedBrain, 'subagents/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.json'),
    )
    await assert.rejects(
      stat(
        join(
          forkedBrain,
          'subagents/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb.json',
        ),
      ),
    )
    await stat(join(forkedBrain, 'messages/keep.json'))
    await assert.rejects(stat(join(forkedBrain, 'messages/drop.json')))
    await stat(join(forkedBrain, 'steps/1/output.txt'))
    await assert.rejects(stat(join(forkedBrain, 'steps/2/output.txt')))

    // Close and reopen forked session with replay: only Turn 1 returns.
    await h.provider.send({
      type: 'session.close',
      requestId: 'close-1',
      sessionId: 's-conv',
    })
    await h.wait((e) => e.type === 'session.closed' && e.sessionId === 's-conv')

    const reopenStart = h.events.length
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-forked',
      sessionId: 's-conv-forked',
      config: h.config,
      persistence: { version: 1, data: { sessionId: forkedId } },
      history: 'replay',
    })
    await h.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 's-conv-forked',
    )

    const forkedUsers = h.events
      .slice(reopenStart)
      .filter(
        (e) => e.type === 'timeline.item' && e.item.type === 'user_message',
      )
      .map((e) =>
        e.type === 'timeline.item' && e.item.type === 'user_message'
          ? e.item
          : undefined,
      )
      .filter((x): x is NonNullable<typeof x> => Boolean(x))
    assert.equal(forkedUsers.length, 1)
    assert.match(forkedUsers[0].text, /first question/)
    assert.ok(forkedUsers[0].revertToken)

    // Reverting Turn 1 (first user step) creates a fresh empty session.
    const turn1RevertStart = h.events.length
    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-turn1',
      sessionId: 's-conv-forked',
      scope: 'conversation',
      token: forkedUsers[0].revertToken!,
    })
    const freshPersisted = await h.wait(
      (e) =>
        e.type === 'session.persistence' &&
        e.sessionId === 's-conv-forked' &&
        h.events.indexOf(e) >= turn1RevertStart,
    )
    assert.equal(freshPersisted.type, 'session.persistence')
    if (freshPersisted.type !== 'session.persistence') return
    const freshId = (freshPersisted.persistence.data as { sessionId: string })
      .sessionId
    assert.notEqual(freshId, forkedId)
    assert.notEqual(freshId, oldNativeId)
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('Git workspace file revert restores edits/deletions/untracked while preserving pre-turn dirty & staged index state, plus scope=both', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-git-'))
  const repoDir = join(home, 'repo')
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

  // User already has unstaged & staged local edits BEFORE the turn starts!
  await writeFile(join(repoDir, 'dirty.txt'), 'user-unstaged-edit\n')
  await writeFile(join(repoDir, 'staged.txt'), 'user-staged-edit\n')
  await runGit(repoDir, ['add', 'staged.txt'])

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
    assert.equal(opened.type, 'session.opened')
    if (opened.type !== 'session.opened') return
    const nativeId = (opened.persistence?.data as { sessionId: string })
      .sessionId
    await h.wait((e) => e.type === 'session.ready')

    // Turn 1 runs, capturing baseline snapshot (including dirty.txt & staged.txt edits!).
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

    // Simulate agent mutating workspace during Turn 1 / Turn 2.
    await writeFile(join(repoDir, 'tracked.txt'), 'mutated-by-agent\n')
    await writeFile(join(repoDir, 'dirty.txt'), 'clobbered-by-agent\n')
    await writeFile(join(repoDir, 'created.txt'), 'new-file-from-agent\n')
    await rm(join(repoDir, 'keep.txt'))

    // Also add Turn 2 and mutate another file so we test `both` on Turn 2 as well.
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

    const token1 =
      userMsg1.type === 'timeline.item' && userMsg1.item.type === 'user_message'
        ? userMsg1.item.revertToken
        : undefined
    const token2 =
      userMsg2.type === 'timeline.item' && userMsg2.item.type === 'user_message'
        ? userMsg2.item.revertToken
        : undefined
    assert.ok(token1)
    assert.ok(token2)

    // Revert Turn 2 with scope='both': restores workspace to Turn 2 baseline & forks DB.
    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-git-both',
      sessionId: 's-git',
      scope: 'both',
      token: token2!,
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-git-both',
    )
    assert.equal(
      await readFile(join(repoDir, 'turn2-only.txt'), 'utf8'),
      'before-t2\n',
    )
    await assert.rejects(stat(join(repoDir, 'extra-t2.txt')))

    // Verify forked checkpoints retained Turn 1 checkpoint for Turn 1 revert.
    const persistedBoth = h.events.find((e) => e.type === 'session.persistence')
    assert.ok(persistedBoth)
    if (persistedBoth?.type === 'session.persistence') {
      const newNativeId = (
        persistedBoth.persistence.data as { sessionId: string }
      ).sessionId
      const keptCheckpoints = await readSessionCheckpoints(
        appDataDir,
        newNativeId,
      )
      assert.equal(keptCheckpoints.length, 1)
    }

    // Revert Turn 1 files: restores tracked.txt, keep.txt, dirty.txt, deletes created.txt, preserves git staged.txt!
    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-git-files',
      sessionId: 's-git',
      scope: 'files',
      token: token1!,
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-git-files',
    )

    assert.equal(
      await readFile(join(repoDir, 'tracked.txt'), 'utf8'),
      'v1-clean\n',
    )
    assert.equal(await readFile(join(repoDir, 'keep.txt'), 'utf8'), 'keep-me\n')
    assert.equal(
      await readFile(join(repoDir, 'dirty.txt'), 'utf8'),
      'user-unstaged-edit\n',
    )
    assert.equal(
      await readFile(join(repoDir, 'staged.txt'), 'utf8'),
      'user-staged-edit\n',
    )
    await assert.rejects(stat(join(repoDir, 'created.txt')))

    // Real git index must still contain staged.txt change untouched by shadow repo.
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

test('Non-Git workspace file revert restores journal pre-edit snapshots, diff.oldText, and deletes newly created files', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-revert-nongit-'))
  const workDir = join(home, 'workspace')
  await mkdir(workDir, { recursive: true })
  await writeFile(join(workDir, 'existing.txt'), 'before-v1')
  await writeFile(join(workDir, 'oldtext-only.txt'), 'from-diff')
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
    assert.equal(perm.type, 'session.permission')
    if (perm.type === 'session.permission') {
      await h.provider.send({
        type: 'session.permission',
        sessionId: 's-nongit',
        permissionId: perm.request.id,
        response: { behavior: 'allow', selectedActionId: 'allow' },
      })
    }
    const userItem = await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'user_message' &&
        e.item.clientMessageId === 'ng-1',
    )
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

    const token =
      userItem.type === 'timeline.item' && userItem.item.type === 'user_message'
        ? userItem.item.revertToken
        : undefined
    assert.ok(token)

    await h.provider.send({
      type: 'session.revert',
      requestId: 'rev-ng-files',
      sessionId: 's-nongit',
      scope: 'files',
      token: token!,
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'rev-ng-files',
    )

    assert.equal(
      await readFile(join(workDir, 'existing.txt'), 'utf8'),
      'before-v1',
    )
    assert.equal(
      await readFile(join(workDir, 'oldtext-only.txt'), 'utf8'),
      'from-diff',
    )
    assert.equal(await readFile(join(workDir, 'unchanged.txt'), 'utf8'), 'stay')
    await assert.rejects(stat(join(workDir, 'new-file.txt')))
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})
