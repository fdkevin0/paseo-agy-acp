import assert from 'node:assert/strict'
import { test } from 'node:test'
import { BrainTranscriptProjector } from '../server/brain-projector.ts'
import { mergeCompactionHistory } from '../server/history.ts'
import {
  BrainTurnObservation,
  type BrainRead,
} from '../server/brain-observer.ts'

const planner = (step_index: number, id?: string) => ({
  step_index,
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  tool_calls: [
    { ...(id ? { id } : {}), name: 'read_file', args: { Path: '/file' } },
  ],
})
const metadata = (step: number, id: string) => ({
  messages: [],
  readIds: new Set<string>(),
  unconsumedHighPriority: [],
  consumedNonUserCount: 0,
  stepCallIds: new Map([[step, id]]),
})

test('raw checkpoint observation reconciles only confirmed snapshot deletion before step reuse', async (t) => {
  const items: unknown[] = []
  const observer = new BrainTurnObservation('', '', 's', -1, {
    isCurrent: () => true,
    onUsage: () => {},
    onItem: (item) => items.push(item),
  })
  const cp = { step_index: 2, type: 'CHECKPOINT', status: 'DONE' }
  let read: BrainRead = {
    rows: [cp],
    reset: true,
    snapshotStepIndices: new Set([2]),
  }
  t.mock.method(observer.reader, 'read', async () => read)
  await observer.poll()
  read = { rows: [], reset: true }
  await observer.poll()
  read = { rows: [cp], reset: false }
  await observer.poll()
  assert.equal(
    items.length,
    1,
    'unconfirmed empty/torn replay retains identity',
  )
  read = { rows: [], reset: false, snapshotStepIndices: new Set([1]) }
  await observer.poll()
  read = { rows: [cp], reset: false }
  await observer.poll()
  assert.equal(
    items.length,
    2,
    'confirmed deletion permits a later checkpoint occurrence',
  )
  await observer.dispose()
})

for (const source of ['tool_id', 'step_metadata', 'acp', 'fallback'] as const) {
  test(`deferred ${source} emissions retain their provenance through compaction history`, () => {
    const p = new BrainTranscriptProjector('s')
    const ids: string[] = []
    p.bindToolCallIds(ids)
    p.resolveToolIdentities(true)
    const items = p.project(
      planner(1, source === 'tool_id' ? 'native' : undefined),
    )
    items.push(
      ...p.project({
        step_index: 2,
        type: 'GENERIC',
        status: 'DONE',
        content: 'result',
      }),
    )
    if (source === 'step_metadata') p.syncMetadata(metadata(2, 'native'), [])
    if (source === 'acp') ids.push('native')
    items.push(...p.resolveToolIdentities(source !== 'fallback'))
    const tools = items.filter((item) => item.type === 'tool_call')
    assert.deepEqual(
      tools.map((item) => item.status),
      ['running', 'completed'],
    )
    const trusted = source === 'tool_id' || source === 'step_metadata'
    for (const item of tools)
      assert.equal(
        p.nativeToolIdentitySource(item),
        trusted ? source : undefined,
      )
    items.push(
      ...p.project({ step_index: 3, type: 'CHECKPOINT', status: 'DONE' }),
    )
    // A later genuinely native occurrence of the same literal cannot bless an earlier positional ID.
    if (!trusted) items.push(...p.project(planner(4, tools[0].callId)))
    const merged = mergeCompactionHistory(
      [{ item: tools[1], resultProvided: true }],
      items.map((item) => ({
        item,
        nativeToolIdentitySource: p.nativeToolIdentitySource(item),
      })),
    )
    assert.equal(
      merged.filter(({ item }) => item.type === 'compaction').length,
      trusted ? 1 : 0,
    )
    assert.equal(
      merged.filter(({ item }) => item.type === 'notification').length,
      trusted ? 0 : 1,
    )
  })
}

test('superseded terminal emissions inherit each pending identity source, including delayed slots', () => {
  for (const source of [
    'tool_id',
    'step_metadata',
    'acp',
    'fallback',
  ] as const) {
    const p = new BrainTranscriptProjector('s')
    const ids: string[] = []
    p.bindToolCallIds(ids)
    p.resolveToolIdentities(true)
    const emitted = p.project(
      planner(1, source === 'tool_id' ? 'native' : undefined),
    )
    emitted.push(...p.project(planner(3, 'next')))
    if (source === 'step_metadata') p.syncMetadata(metadata(2, 'native'), [])
    if (source === 'acp') ids.push('native')
    emitted.push(...p.resolveToolIdentities(source !== 'fallback'))
    const canceled = emitted.filter(
      (item) => item.type === 'tool_call' && item.status === 'canceled',
    )
    assert.equal(canceled.length, 1)
    assert.equal(
      p.nativeToolIdentitySource(canceled[0]),
      source === 'tool_id' || source === 'step_metadata' ? source : undefined,
    )
  }
})
