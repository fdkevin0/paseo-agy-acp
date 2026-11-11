import assert from 'node:assert/strict'
import { test } from 'node:test'
import { appendFile, mkdtemp, mkdir, open, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  StepDedupe,
  TranscriptReader,
  readTranscriptRows,
} from '../server/brain-io.ts'

const id = '11111111-1111-4111-8111-111111111111'
const row = (step_index: number, content: string) => ({
  step_index,
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  content,
})
const encode = (rows: object[]) =>
  rows.map((r) => JSON.stringify(r) + '\n').join('')

test('empty rewrite observations preserve delivered identities until rows return', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-dedupe-'))
  const dir = join(root, id, '.system_generated/logs')
  await mkdir(dir, { recursive: true })
  const path = join(dir, 'transcript_full.jsonl')
  const reader = new TranscriptReader()
  const dedupe = new StepDedupe()
  const read = async () => {
    const { rows, snapshotStepIndices } = await reader.read(root, id)
    if (snapshotStepIndices) dedupe.prune(snapshotStepIndices)
    return rows.filter((r) => dedupe.fresh(r))
  }
  const file = await open(path, 'w+')
  try {
    const original = [row(0, 'First'), row(1, 'Second')]
    await file.write(encode(original), 0)
    assert.deepEqual(await read(), original)
    await file.truncate(0)
    // Force the same observations as multiple watcher polls during O_TRUNC, with no timing assumption.
    assert.deepEqual(await read(), [])
    assert.deepEqual(await read(), [])
    // A torn first record is also not proof of deletion.
    const encoded = encode(original)
    await file.write(encoded.slice(0, 12), 0)
    assert.deepEqual(await read(), [])
    await appendFile(path, encoded.slice(12))
    assert.deepEqual(
      await read(),
      [],
      'restored history must not be delivered again',
    )
    const next = row(2, 'Appended')
    await appendFile(path, encode([next]))
    assert.deepEqual(await read(), [next])
    await file.truncate(0)
    assert.deepEqual(await read(), [])
    // The nonempty replacement still carries reset, so deleted trailing identities are pruned.
    await file.write(encode([original[0]]), 0)
    assert.deepEqual(await read(), [])
    await appendFile(path, encode([original[1]]))
    assert.deepEqual(
      await read(),
      [original[1]],
      'a removed row with identical bytes is new after reconciliation',
    )
    await file.truncate(0)
    const replacement = encode([original[0], row(1, 'Changed')])
    await file.write(replacement.slice(0, -5), 0)
    assert.deepEqual(
      await read(),
      [],
      'a torn rewrite must not prune a complete prefix',
    )
    await appendFile(path, replacement.slice(-5))
    assert.deepEqual(await read(), [row(1, 'Changed')])
    await file.truncate(0)
    assert.deepEqual(await read(), [])
    const reused = row(0, 'New content after clear')
    await file.write(encode([reused]), 0)
    assert.deepEqual(
      await read(),
      [reused],
      'clearing cannot permanently suppress new content at a reused index',
    )
    await appendFile(path, '{broken}\n')
    assert.deepEqual(
      await read(),
      [],
      'append parse errors are retried as possible rewrites',
    )
    await assert.rejects(
      read(),
      SyntaxError,
      'complete malformed data is still an error',
    )
  } finally {
    await file.close()
    await rm(root, { recursive: true, force: true })
  }
})

test('nonempty rewrites prune deleted indices and permit reuse without retaining old generations', () => {
  const dedupe = new StepDedupe()
  const kept = row(0, 'Kept')
  const removed = row(1, 'Removed')
  assert.ok(dedupe.fresh(kept))
  assert.ok(dedupe.fresh(removed))
  dedupe.prune(new Set([kept.step_index]))
  assert.equal(dedupe.fresh(kept), false)
  assert.equal(
    dedupe.fresh(removed),
    true,
    'a deleted index can be appended again',
  )
  assert.equal(
    dedupe.fresh(row(1, 'Replacement')),
    true,
    'changed payload at the same index is new',
  )
  for (let step = 2; step < 100; step++) {
    const current = row(step, 'Current generation')
    dedupe.prune(new Set([current.step_index]))
    assert.ok(dedupe.fresh(current))
    assert.equal(dedupe.fresh(current), false)
  }
  assert.ok(
    dedupe.fresh(kept),
    'old generations are evicted by nonempty reconciliation',
  )
})

test('empty observations are not a session reset; each watcher lifetime owns its identities', () => {
  const original = row(0, 'Same bytes')
  const old = new StepDedupe()
  assert.ok(old.fresh(original))
  old.prune(new Set())
  assert.equal(old.fresh(original), false)
  assert.ok(old.fresh(row(0, 'Reused after clear')))
  assert.ok(
    new StepDedupe().fresh(original),
    'a new watcher/session starts with no delivered rows',
  )
})

test('first read and one-shot history deliver complete rows before a partial tail', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-prefix-'))
  const dir = join(root, id, '.system_generated/logs')
  await mkdir(dir, { recursive: true })
  const path = join(dir, 'transcript_full.jsonl')
  const file = await open(path, 'w+')
  try {
    const complete = [row(0, 'First'), row(1, 'NEW COMPLETE ROW')]
    const tail = JSON.stringify(row(2, 'Next'))
    await file.write(encode(complete) + tail.slice(0, 12), 0)
    const reader = new TranscriptReader()
    assert.deepEqual((await reader.read(root, id)).rows, complete)
    assert.deepEqual(await readTranscriptRows(root, id), complete)
    await appendFile(path, tail.slice(12) + '\n')
    assert.deepEqual((await reader.read(root, id)).rows, [row(2, 'Next')])
    assert.deepEqual((await reader.read(root, id)).rows, [])
  } finally {
    await file.close()
    await rm(root, { recursive: true, force: true })
  }
})

test('snapshot reconciliation spans partial reads and never inherits a truncated generation', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-snapshot-'))
  const dir = join(root, id, '.system_generated/logs')
  await mkdir(dir, { recursive: true })
  const path = join(dir, 'transcript_full.jsonl')
  const reader = new TranscriptReader()
  const dedupe = new StepDedupe()
  const read = async () => {
    const result = await reader.read(root, id)
    if (result.snapshotStepIndices) dedupe.prune(result.snapshotStepIndices)
    return { ...result, delivered: result.rows.filter((r) => dedupe.fresh(r)) }
  }
  const file = await open(path, 'w+')
  try {
    const kept = row(0, 'Kept')
    const old = row(1, 'Old')
    const deleted = row(2, 'Deleted')
    await file.write(encode([kept, old, deleted]), 0)
    assert.deepEqual((await read()).delivered, [kept, old, deleted])
    await file.truncate(0)
    const empty = await read()
    assert.equal(empty.reset, true)
    assert.equal(empty.snapshotStepIndices, undefined)
    const changed = row(1, 'Changed')
    const last = row(3, 'Last')
    const changedLine = JSON.stringify(changed)
    const lastLine = JSON.stringify(last)
    await file.write(encode([kept]) + changedLine.slice(0, 12), 0)
    const prefix = await read()
    assert.equal(prefix.reset, false)
    assert.equal(prefix.snapshotStepIndices, undefined)
    assert.deepEqual(prefix.rows, [kept])
    assert.deepEqual(prefix.delivered, [])
    assert.equal(
      dedupe.fresh(deleted),
      false,
      'absence in an incomplete replacement does not authorize deletion',
    )
    await appendFile(path, changedLine.slice(12) + '\n' + lastLine.slice(0, 12))
    const middle = await read()
    assert.equal(middle.reset, false)
    assert.equal(middle.snapshotStepIndices, undefined)
    assert.deepEqual(middle.delivered, [changed])
    await appendFile(path, lastLine.slice(12) + '\n')
    const complete = await read()
    assert.equal(
      complete.reset,
      false,
      'finishing a partial snapshot must not reset the projector again',
    )
    assert.deepEqual(complete.snapshotStepIndices, new Set([0, 1, 3]))
    assert.deepEqual(complete.delivered, [last])
    assert.equal(
      dedupe.fresh(kept),
      false,
      'prefix identities survive reconciliation on a later read',
    )
    assert.equal(dedupe.fresh(changed), false)
    await appendFile(path, encode([deleted]))
    assert.deepEqual(
      (await read()).delivered,
      [deleted],
      'a confirmed deleted index can be reused with identical bytes',
    )
    // A replacement interrupted by another truncation must discard its candidate snapshot indices.
    await file.truncate(0)
    const abandoned = row(8, 'Abandoned generation')
    await file.write(encode([abandoned]) + '{"step_index":9', 0)
    const partial = await read()
    assert.equal(partial.reset, true)
    assert.equal(partial.snapshotStepIndices, undefined)
    assert.deepEqual(partial.delivered, [abandoned])
    await file.truncate(0)
    assert.equal((await read()).snapshotStepIndices, undefined)
    const replacement = row(10, 'New generation')
    await file.write(encode([replacement]), 0)
    const final = await read()
    assert.deepEqual(final.snapshotStepIndices, new Set([10]))
    assert.deepEqual(final.delivered, [replacement])
    assert.ok(
      dedupe.fresh(abandoned),
      'abandoned generation indices were pruned',
    )
    const freshOwner = await new TranscriptReader().read(root, id)
    assert.equal(freshOwner.reset, true)
    assert.deepEqual(freshOwner.snapshotStepIndices, new Set([10]))
    assert.deepEqual(freshOwner.rows, [replacement])
  } finally {
    await file.close()
    await rm(root, { recursive: true, force: true })
  }
})
