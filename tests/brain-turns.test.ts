import assert from 'node:assert/strict'
import { mkdir, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { test } from 'node:test'
import { BrainNode } from '../server/brain.ts'
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

test('resolveChildTurnState correlates mixed untagged Turn 1 and tagged Turn 2 wakeups', () => {
  const parentNode = new BrainNode(parent, parent, {
    allowInitialSpawnPrompt: false,
  })
  const childNode = new BrainNode(child, child, {
    allowInitialSpawnPrompt: true,
  })
  childNode.ingest(message(0, parent, 'init'))
  childNode.ingest(message(3, parent, 'untagged turn 1'))
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
            id: 'u1',
            sender: parent,
            priority: 'MESSAGE_PRIORITY_HIGH',
            signature: 'sig-u1',
            tool: { conversationId: parent, stepIndex: undefined },
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
        readIds: new Set(['u1', 't2']),
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

test('readTurnBoundaryBefore places untagged wakeups by walking ancestor turns', async () => {
  const f = await fixture()
  const mixedParent = '44444444-4444-4444-8444-444444444444'
  const mixedChild = '55555555-5555-4555-8555-555555555555'
  const orderedParent = '66666666-6666-4666-8666-666666666666'
  const orderedChild = '77777777-7777-4777-8777-777777777777'
  const untaggedParent = '88888888-8888-4888-8888-888888888888'
  const untaggedChild = '99999999-9999-4999-8999-999999999999'
  try {
    const cache: TurnBoundaryCache = new Map()

    // Untagged Turn 1 wakeup followed by a tagged Turn 2 wakeup: the boundary sits at the untagged one.
    await f.put(mixedParent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      done(1, 't1'),
      { step_index: 2, type: 'USER_INPUT', content: 'turn 2' },
      ...report(mixedChild, 'tagged wake t2', 3),
      done(6, 't2'),
    ])
    await f.deliver(mixedChild, mixedParent, 'init child', true, 'init-c', {
      conversationId: mixedParent,
      stepIndex: 0,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })
    await f.deliver(mixedChild, mixedParent, 'untagged wake', true, 'u1-c')
    await f.deliver(mixedChild, mixedParent, 'tagged wake t2', true, 't2-c', {
      conversationId: mixedParent,
      stepIndex: 4,
      callId: 'send-c',
      name: 'send_message',
    })
    await f.put(mixedChild, [
      message(0, mixedParent, 'init'),
      message(1, mixedParent, 'untagged wake'),
      message(2, mixedParent, 'tagged wake t2'),
      done(3, 'child done'),
    ])
    assert.deepEqual(
      await readTurnBoundaryBefore(f.root, mixedChild, 3, cache),
      { boundary: 2, ready: true },
    )

    // A tagged Turn 2 wakeup that lands before its untagged twin anchors the turn at its own step.
    await f.put(orderedParent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      done(1, 't1'),
      { step_index: 2, type: 'USER_INPUT', content: 'turn 2' },
      ...report(orderedChild, 'tagged t2', 3),
      ...report(orderedChild, 'untagged t2', 6),
      done(9, 't2'),
    ])
    await f.deliver(orderedChild, orderedParent, 'init child', true, 'init-c', {
      conversationId: orderedParent,
      stepIndex: 0,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })
    await f.deliver(orderedChild, orderedParent, 'tagged t2', true, 't2-1', {
      conversationId: orderedParent,
      stepIndex: 4,
      callId: 'send-1',
      name: 'send_message',
    })
    await f.deliver(orderedChild, orderedParent, 'untagged t2', true, 'u2-2')
    await f.put(orderedChild, [
      message(0, orderedParent, 'init'),
      message(1, orderedParent, 'tagged t2'),
      done(2, 'work in t2'),
      message(3, orderedParent, 'untagged t2'),
      done(4, 'child done'),
    ])
    assert.deepEqual(
      await readTurnBoundaryBefore(f.root, orderedChild, 4, cache),
      { boundary: 1, ready: true },
    )

    // Every wakeup untagged on both turns.
    await f.put(untaggedParent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      ...report(untaggedChild, 'untagged t1', 1),
      done(4, 't1'),
      { step_index: 5, type: 'USER_INPUT', content: 'turn 2' },
      ...report(untaggedChild, 'untagged t2', 6),
      done(9, 't2'),
    ])
    await f.deliver(
      untaggedChild,
      untaggedParent,
      'init child',
      true,
      'init-c',
      {
        conversationId: untaggedParent,
        stepIndex: 0,
        callId: 'spawn-c',
        name: 'invoke_subagent',
      },
    )
    await f.deliver(untaggedChild, untaggedParent, 'untagged t1', true, 'u1')
    await f.deliver(untaggedChild, untaggedParent, 'untagged t2', true, 'u2')
    await f.put(untaggedChild, [
      message(0, untaggedParent, 'init'),
      message(1, untaggedParent, 'untagged t1'),
      message(2, untaggedParent, 'untagged t2'),
      done(3, 'child done'),
    ])
    assert.deepEqual(
      await readTurnBoundaryBefore(f.root, untaggedChild, 3, cache),
      { boundary: 2, ready: true },
    )
  } finally {
    await f.close()
  }
})
