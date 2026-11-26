import assert from 'node:assert/strict'
import { mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { BrainEscapeError } from '../server/brain-io.ts'
import { BrainNode } from '../server/brain-projector.ts'
import {
  readTurnBoundaryBefore,
  resolveChildTurnState,
  type TurnBoundaryCache,
} from '../server/brain-turns.ts'
import {
  child,
  done,
  fixture,
  message,
  parent,
  report,
} from './brain-fixtures.ts'

test('resolveChildTurnState correlates tagged Turn 1 and Turn 2 wakeups', () => {
  const parentNode = new BrainNode(parent, parent, {
    allowInitialSpawnPrompt: false,
  })
  const childNode = new BrainNode(child, child, {
    allowInitialSpawnPrompt: true,
  })
  childNode.ingest(message(0, parent, 'init'))
  childNode.ingest(message(3, parent, 'tagged turn 1'))
  childNode.ingest(message(6, parent, 'tagged turn 2'))

  assert.deepEqual(
    resolveChildTurnState(
      parentNode,
      5,
      {
        conversationId: child,
        spawnStepIndex: 2,
        state: 'SUBAGENT_STATE_ALIVE',
      },
      childNode,
      {
        messages: [
          {
            id: 't1',
            sender: parent,
            priority: 'MESSAGE_PRIORITY_HIGH',
            signature: 'sig-t1',
            tool: {
              conversationId: parent,
              stepIndex: 4,
              name: 'send_message',
            },
          },
          {
            id: 't2',
            sender: parent,
            priority: 'MESSAGE_PRIORITY_HIGH',
            signature: 'sig-t2',
            tool: {
              conversationId: parent,
              stepIndex: 7,
              name: 'send_message',
            },
          },
        ],
        readIds: new Set(['t1', 't2']),
        unconsumedHighPriority: [],
        consumedNonUserCount: 2,
        stepCallIds: new Map(),
      },
    ),
    { status: 'active', minStepIndex: 5, awaitingTurnWakeup: false },
  )
})

test('readTurnBoundaryBefore is not ready until the transcript holds readable rows', async () => {
  const f = await fixture()
  try {
    const cache: TurnBoundaryCache = new Map()
    assert.deepEqual(await readTurnBoundaryBefore(f.root, parent, 5, cache), {
      boundary: -1,
      ready: false,
    })

    await mkdir(join(f.root, parent, '.system_generated/logs'), {
      recursive: true,
    })
    await writeFile(f.path(parent), '')
    assert.deepEqual(await readTurnBoundaryBefore(f.root, parent, 5, cache), {
      boundary: -1,
      ready: false,
    })

    await writeFile(f.path(parent), '\n')
    assert.deepEqual(await readTurnBoundaryBefore(f.root, parent, 5, cache), {
      boundary: -1,
      ready: false,
    })

    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      done(1, 't1'),
      { step_index: 2, type: 'USER_INPUT', content: 'turn 2' },
      done(3, 't2'),
    ])
    assert.deepEqual(await readTurnBoundaryBefore(f.root, parent, 2, cache), {
      boundary: 0,
      ready: true,
    })
    assert.deepEqual(await readTurnBoundaryBefore(f.root, parent, 4, cache), {
      boundary: 2,
      ready: true,
    })
  } finally {
    await f.close()
  }
})

test('readTurnBoundaryBefore re-reads when a message file completes in place', async () => {
  const f = await fixture()
  try {
    const cache: TurnBoundaryCache = new Map()
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      done(1, 't1'),
      { step_index: 2, type: 'USER_INPUT', content: 'turn 2' },
      done(3, 't2'),
    ])
    await f.put(child, [
      message(0, parent, 'init'),
      message(1, parent, 'wake t2'),
      done(2, 'child done'),
    ])
    const msgDir = join(f.root, child, '.system_generated/messages')
    await mkdir(msgDir, { recursive: true })
    await f.deliver(child, parent, 'wake t2', true, 'wake-c', {
      conversationId: parent,
      stepIndex: 3,
      callId: 'send-c',
      name: 'send_message',
    })
    // A torn write: the spawn record is present but unparsable, so its step is unknown.
    await writeFile(join(msgDir, 'init-c.json'), '{')

    assert.deepEqual(await readTurnBoundaryBefore(f.root, child, 2, cache), {
      boundary: -1,
      ready: true,
    })

    await f.deliver(child, parent, 'init child', true, 'init-c', {
      conversationId: parent,
      stepIndex: 0,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })

    assert.deepEqual(await readTurnBoundaryBefore(f.root, child, 2, cache), {
      boundary: 1,
      ready: true,
    })
  } finally {
    await f.close()
  }
})

test('readTurnBoundaryBefore resolves re-woken child boundary against tagged ancestor turns', async () => {
  const f = await fixture()
  const rootParent = '44444444-4444-4444-8444-444444444444'
  const reWokenChild = '55555555-5555-4555-8555-555555555555'
  try {
    const cache: TurnBoundaryCache = new Map()
    await f.put(rootParent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      done(1, 't1'),
      { step_index: 2, type: 'USER_INPUT', content: 'turn 2' },
      ...report(reWokenChild, 'tagged wake t2', 3),
      done(6, 't2'),
    ])
    await f.deliver(reWokenChild, rootParent, 'init child', true, 'init-c', {
      conversationId: rootParent,
      stepIndex: 0,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })
    await f.deliver(reWokenChild, rootParent, 'tagged wake t1', true, 't1-c', {
      conversationId: rootParent,
      stepIndex: 1,
      callId: 'send-1',
      name: 'send_message',
    })
    await f.deliver(reWokenChild, rootParent, 'tagged wake t2', true, 't2-c', {
      conversationId: rootParent,
      stepIndex: 4,
      callId: 'send-2',
      name: 'send_message',
    })
    await f.put(reWokenChild, [
      message(0, rootParent, 'init'),
      message(1, rootParent, 'tagged wake t1'),
      message(2, rootParent, 'tagged wake t2'),
      done(3, 'child done'),
    ])
    assert.deepEqual(
      await readTurnBoundaryBefore(f.root, reWokenChild, 3, cache),
      { boundary: 2, ready: true },
    )
  } finally {
    await f.close()
  }
})

test('readTurnBoundaryBefore fails closed on corrupt JSONL and symlink escapes without caching emptyRows', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  const outside = await mkdtemp(join(tmpdir(), 'agy-turn-escape-'))
  t.after(() => rm(outside, { recursive: true, force: true }))
  const cache: TurnBoundaryCache = new Map()

  const logsDir = join(f.root, parent, '.system_generated/logs')
  await mkdir(logsDir, { recursive: true })
  await writeFile(f.path(parent), '{"step_index":0,broken\n')

  await assert.rejects(
    readTurnBoundaryBefore(f.root, parent, 5, cache),
    SyntaxError,
  )
  assert.equal(cache.size, 0)

  await f.put(parent, [
    { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
    done(1, 't1'),
  ])
  assert.deepEqual(await readTurnBoundaryBefore(f.root, parent, 5, cache), {
    boundary: 0,
    ready: true,
  })

  const escapedFile = join(outside, 'transcript.jsonl')
  await writeFile(
    escapedFile,
    `${JSON.stringify({ step_index: 0, type: 'USER_INPUT', content: 'x' })}\n`,
  )
  const childLogs = join(f.root, child, '.system_generated/logs')
  await mkdir(childLogs, { recursive: true })
  await symlink(escapedFile, f.path(child))

  await assert.rejects(
    readTurnBoundaryBefore(f.root, child, 5, cache),
    BrainEscapeError,
  )
})
