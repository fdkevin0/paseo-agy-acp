import assert from 'node:assert/strict'
import { test } from 'node:test'
import { BrainTranscriptProjector } from '../server/brain-projector.ts'
import { mergeCompactionHistory } from '../server/history.ts'
import {
  BrainTurnObservation,
  type BrainRead,
} from '../server/brain-observer.ts'
import { TranscriptReader } from '../server/brain-io.ts'
import { sequenceDouble } from './helpers.ts'

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

class ScriptedTranscriptReader extends TranscriptReader {
  private readonly next: () => Promise<BrainRead>
  constructor(reads: readonly BrainRead[]) {
    super()
    this.next = sequenceDouble(reads.map((read) => async () => read))
  }
  override read(): ReturnType<TranscriptReader['read']> {
    return this.next()
  }
}

test('raw checkpoint observation reconciles only confirmed snapshot deletion before step reuse', async () => {
  const items: unknown[] = []
  const cp = { step_index: 2, type: 'CHECKPOINT', status: 'DONE' }
  const reader = new ScriptedTranscriptReader([
    { rows: [cp], reset: true, snapshotStepIndices: new Set([2]) },
    { rows: [], reset: true },
    { rows: [cp], reset: false },
    { rows: [], reset: false, snapshotStepIndices: new Set([1]) },
    { rows: [cp], reset: false },
  ])
  const observer = new BrainTurnObservation(
    '',
    '',
    's',
    -1,
    {
      isCurrent: () => true,
      onUsage: () => {},
      onItem: (item) => items.push(item),
    },
    200,
    reader,
  )
  await observer.poll()
  await observer.poll()
  await observer.poll()
  assert.equal(
    items.length,
    1,
    'unconfirmed empty/torn replay retains identity',
  )
  await observer.poll()
  await observer.poll()
  assert.equal(
    items.length,
    2,
    'confirmed deletion permits a later checkpoint occurrence',
  )
  await observer.dispose()
})

function assertDeferredProvenance(spec: {
  toolId?: string
  bindIdentity: (p: BrainTranscriptProjector, ids: string[]) => void
  holdForLateIds: boolean
  expectedSource: 'tool_id' | 'step_metadata' | undefined
  afterCheckpoint: (
    p: BrainTranscriptProjector,
    items: ReturnType<BrainTranscriptProjector['project']>,
    callId: string,
  ) => void
  expectedCompactions: number
  expectedNotifications: number
}) {
  const p = new BrainTranscriptProjector('s')
  const ids: string[] = []
  p.bindToolCallIds(ids)
  p.resolveToolIdentities(true)
  const items = p.project(planner(1, spec.toolId))
  items.push(
    ...p.project({
      step_index: 2,
      type: 'GENERIC',
      status: 'DONE',
      content: 'result',
    }),
  )
  spec.bindIdentity(p, ids)
  items.push(...p.resolveToolIdentities(spec.holdForLateIds))
  const tools = items.filter((item) => item.type === 'tool_call')
  assert.deepEqual(
    tools.map((item) => item.status),
    ['running', 'completed'],
  )
  assert.deepEqual(
    tools.map((item) => p.nativeToolIdentitySource(item)),
    [spec.expectedSource, spec.expectedSource],
  )
  items.push(
    ...p.project({ step_index: 3, type: 'CHECKPOINT', status: 'DONE' }),
  )
  spec.afterCheckpoint(p, items, tools[0].callId)
  const merged = mergeCompactionHistory(
    [{ item: tools[1], resultProvided: true }],
    items.map((item) => ({
      item,
      nativeToolIdentitySource: p.nativeToolIdentitySource(item),
    })),
  )
  assert.equal(
    merged.filter(({ item }) => item.type === 'compaction').length,
    spec.expectedCompactions,
  )
  assert.equal(
    merged.filter(({ item }) => item.type === 'notification').length,
    spec.expectedNotifications,
  )
}

test('deferred tool_id emissions retain their provenance through compaction history', () => {
  assertDeferredProvenance({
    toolId: 'native',
    bindIdentity: () => {},
    holdForLateIds: true,
    expectedSource: 'tool_id',
    afterCheckpoint: () => {},
    expectedCompactions: 1,
    expectedNotifications: 0,
  })
})

test('deferred step_metadata emissions retain their provenance through compaction history', () => {
  assertDeferredProvenance({
    bindIdentity: (p) => p.syncMetadata(metadata(2, 'native'), []),
    holdForLateIds: true,
    expectedSource: 'step_metadata',
    afterCheckpoint: () => {},
    expectedCompactions: 1,
    expectedNotifications: 0,
  })
})

test('deferred acp emissions retain their provenance through compaction history', () => {
  assertDeferredProvenance({
    bindIdentity: (_p, ids) => ids.push('native'),
    holdForLateIds: true,
    expectedSource: undefined,
    // A later genuinely native occurrence of the same literal cannot bless an earlier positional ID.
    afterCheckpoint: (p, items, callId) =>
      items.push(...p.project(planner(4, callId))),
    expectedCompactions: 0,
    expectedNotifications: 1,
  })
})

test('deferred fallback emissions retain their provenance through compaction history', () => {
  assertDeferredProvenance({
    bindIdentity: () => {},
    holdForLateIds: false,
    expectedSource: undefined,
    // A later genuinely native occurrence of the same literal cannot bless an earlier positional ID.
    afterCheckpoint: (p, items, callId) =>
      items.push(...p.project(planner(4, callId))),
    expectedCompactions: 0,
    expectedNotifications: 1,
  })
})

function assertSupersededSource(spec: {
  toolId?: string
  bindIdentity: (p: BrainTranscriptProjector, ids: string[]) => void
  holdForLateIds: boolean
  expectedSource: 'tool_id' | 'step_metadata' | undefined
}) {
  const p = new BrainTranscriptProjector('s')
  const ids: string[] = []
  p.bindToolCallIds(ids)
  p.resolveToolIdentities(true)
  const emitted = p.project(planner(1, spec.toolId))
  emitted.push(...p.project(planner(3, 'next')))
  spec.bindIdentity(p, ids)
  emitted.push(...p.resolveToolIdentities(spec.holdForLateIds))
  const canceled = emitted.filter(
    (item) => item.type === 'tool_call' && item.status === 'canceled',
  )
  assert.equal(canceled.length, 1)
  assert.equal(p.nativeToolIdentitySource(canceled[0]), spec.expectedSource)
}

test('superseded terminal emissions inherit each pending identity source, including delayed slots', () => {
  assertSupersededSource({
    toolId: 'native',
    bindIdentity: () => {},
    holdForLateIds: true,
    expectedSource: 'tool_id',
  })
  assertSupersededSource({
    bindIdentity: (p) => p.syncMetadata(metadata(2, 'native'), []),
    holdForLateIds: true,
    expectedSource: 'step_metadata',
  })
  assertSupersededSource({
    bindIdentity: (_p, ids) => ids.push('native'),
    holdForLateIds: true,
    expectedSource: undefined,
  })
  assertSupersededSource({
    bindIdentity: () => {},
    holdForLateIds: false,
    expectedSource: undefined,
  })
})

function assertDeferredBrainJson(output: unknown) {
  const p = new BrainTranscriptProjector('s')
  const ids: string[] = []
  p.bindToolCallIds(ids)
  p.resolveToolIdentities(true)
  assert.deepEqual(
    p.project({
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [{ name: 'Vendor.custom', args: { nested: null } }],
    }),
    [],
  )
  assert.deepEqual(
    p.project({
      step_index: 2,
      type: 'GENERIC',
      status: 'DONE',
      content: output,
    }),
    [],
  )
  ids.push('late-acp-id')
  const items = p.resolveToolIdentities(true)
  assert.equal(items.length, 2)
  const item = items.at(-1)
  assert.ok(item?.type === 'tool_call' && item.detail.type === 'unknown')
  assert.equal(item.callId, 'late-acp-id')
  assert.equal(item.status, 'completed')
  assert.deepEqual(item.detail.input, { nested: null })
  assert.deepEqual(item.detail.output, output)
  assert.equal(p.nativeToolIdentitySource(item), undefined)
  assert.deepEqual(p.resolveToolIdentities(false), [])
}

test('deferred Brain JSON null remains data across late ACP identity', () => {
  assertDeferredBrainJson(null)
})

test('deferred Brain JSON content remains data across late ACP identity', () => {
  assertDeferredBrainJson({
    content: [{ type: 'text', text: 'real tool content' }],
    nested: null,
  })
})
