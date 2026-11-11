import assert from 'node:assert/strict'
import { test } from 'node:test'
import { appendFile, mkdtemp, mkdir, open, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { StepDedupe, TranscriptReader } from '../server/brain-io.ts'

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
    const { rows, reset } = await reader.read(root, id)
    if (reset) dedupe.prune(rows)
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
  dedupe.prune([kept])
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
    dedupe.prune([current])
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
  old.prune([])
  assert.equal(old.fresh(original), false)
  assert.ok(old.fresh(row(0, 'Reused after clear')))
  assert.ok(
    new StepDedupe().fresh(original),
    'a new watcher/session starts with no delivered rows',
  )
})
