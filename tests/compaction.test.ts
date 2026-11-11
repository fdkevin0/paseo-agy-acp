import assert from 'node:assert/strict'
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import type { ProviderEvent } from '@getpaseo/plugin/server/provider'
import { watchBrain } from '../server/brain.ts'
import { StepDedupe } from '../server/brain-io.ts'
import { waitUntil } from './helpers.ts'

const native = '11111111-1111-4111-8111-111111111111'
const checkpoint = (content: string) => ({
  step_index: 1,
  type: 'CHECKPOINT',
  status: 'DONE',
  content,
})

test('checkpoint completion is semantic, not a summary revision or regressed status', () => {
  const dedupe = new StepDedupe()
  assert.equal(dedupe.fresh(checkpoint('summary A')), true)
  assert.equal(dedupe.fresh(checkpoint('summary B')), false)
  assert.equal(
    dedupe.fresh({ ...checkpoint('summary B'), status: 'RUNNING' }),
    false,
  )
  assert.equal(dedupe.fresh(checkpoint('summary B')), false)
  dedupe.prune(new Set())
  assert.equal(dedupe.fresh(checkpoint('summary B')), false)
  dedupe.prune(new Set([0]))
  assert.equal(dedupe.fresh(checkpoint('summary B')), true)
  assert.equal(
    dedupe.fresh({
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      content: 'different event',
    }),
    true,
  )
  assert.equal(dedupe.fresh(checkpoint('summary B')), true)
})

test('watcher emits one completion per checkpoint and preserves proven deletion/index reuse', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-checkpoint-review-'))
  const dir = join(root, native, '.system_generated/logs')
  await mkdir(dir, { recursive: true })
  const path = join(dir, 'transcript_full.jsonl')
  const first = { step_index: 0, type: 'PLANNER_RESPONSE', thinking: 'working' }
  const marker = (thinking: string) => ({
    step_index: 2,
    type: 'PLANNER_RESPONSE',
    thinking,
  })
  const put = (rows: object[]) =>
    writeFile(path, rows.map((row) => JSON.stringify(row) + '\n').join(''))
  await put([first, checkpoint('summary A')])
  const events: ProviderEvent[] = []
  const watcher = watchBrain(
    root,
    native,
    's',
    (event) => events.push(event),
    5,
  )
  const compactions = () =>
    events.filter(
      (event) =>
        event.type === 'timeline.item' && event.item.type === 'compaction',
    )
  const saw = (text: string) =>
    events.some(
      (event) =>
        event.type === 'timeline.item' &&
        event.item.type === 'reasoning' &&
        event.item.text === text,
    )
  try {
    await watcher.ready
    assert.equal(compactions().length, 1)
    await put([first, checkpoint('summary B'), marker('revised')])
    await waitUntil(() => saw('revised'))
    assert.equal(
      compactions().length,
      1,
      'the host must not need to deduplicate provider ids',
    )
    await put([first, marker('deleted')])
    await waitUntil(() => saw('deleted'))
    await put([first, checkpoint('summary B'), marker('reused')])
    await waitUntil(() => saw('reused'))
    assert.equal(
      compactions().length,
      2,
      'a proven deletion permits a new checkpoint at that index',
    )
  } finally {
    await watcher.cancel()
    await rm(root, { recursive: true, force: true })
  }
})
