import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  extractStepUsage,
  readLastStepIndex,
  readLatestStepUsage,
} from '../server/brain-io.ts'
import { promptUsage } from '../server/mapping.ts'

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
  for (const [name, bytes, expected] of [
    ['LF', Buffer.from(`${previous}\n${latest}\n`), 2],
    ['no LF', Buffer.from(`${previous}\n${latest}`), 2],
    ['single row', Buffer.from(latest), 2],
    ['partial JSON', Buffer.from(`${previous}\n{"step_index":2`), 1],
    ['torn UTF-8', Buffer.concat([Buffer.from(`${previous}\n`), tornUtf8]), 1],
    ['only partial', Buffer.from('{"step_index":2'), -1],
  ] as const) {
    await t.test(name, async () => {
      await writeFile(join(dir, 'transcript_full.jsonl'), bytes)
      assert.equal(await readLastStepIndex(root, id), expected)
      const result = await readLatestStepUsage(root, id)
      assert.equal(result.lastStepIndex, expected)
      assert.equal(
        result.lastStepUsage?.inputTokens,
        expected === 2 ? 70 : expected === 1 ? 10 : undefined,
      )
      assert.deepEqual(await readLatestStepUsage(root, id, expected), {
        lastStepIndex: expected,
      })
    })
  }
})
