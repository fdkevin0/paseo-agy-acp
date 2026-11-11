import assert from 'node:assert/strict'
import { test } from 'node:test'
import { BrainTurnObservation } from '../server/brain-observer.ts'

const busy = (message: string) =>
  Object.assign(new Error(message), { code: 'EBUSY' })
const observation = () =>
  new BrainTurnObservation('', '', 's', -1, {
    isCurrent: () => true,
    onUsage: () => {},
    onItem: () => {},
  })

test('a successful reader observation clears only the preceding I/O fault', async (t) => {
  const o = observation(),
    error = busy('temporary read fault')
  let reads = 0
  t.mock.method(o.reader, 'read', async () => {
    if (++reads === 1) throw error
    return { rows: [], reset: false }
  })
  await assert.rejects(o.poll(), (actual) => actual === error)
  assert.equal(o.error, error)
  await o.poll()
  assert.equal(o.error, undefined)
  assert.equal(reads, 2)
  await o.dispose()
})

test('persistent reader I/O exposes its latest diagnostic and stops reading on disposal', async (t) => {
  const o = observation(),
    first = busy('first read fault'),
    latest = busy('latest read fault')
  let reads = 0
  t.mock.method(o.reader, 'read', async () => {
    throw ++reads === 1 ? first : latest
  })
  await assert.rejects(o.poll(), (actual) => actual === first)
  await assert.rejects(o.poll(), (actual) => actual === latest)
  assert.equal(o.error, latest)
  await o.dispose()
  await o.poll()
  assert.equal(reads, 2)
  assert.equal(o.error, latest)
})

test('an errno-shaped publication failure remains fatal instead of being cleared by another read', async (t) => {
  const error = busy('publisher failed'),
    o = new BrainTurnObservation('', '', 's', -1, {
      isCurrent: () => true,
      onUsage: () => {},
      onItem: () => {
        throw error
      },
    })
  let reads = 0
  t.mock.method(o.reader, 'read', async () => {
    reads++
    return {
      rows: [{ step_index: 1, type: 'CHECKPOINT', status: 'DONE' }],
      reset: false,
    }
  })
  await assert.rejects(o.poll(), (actual) => actual === error)
  await assert.rejects(o.poll(), (actual) => actual === error)
  assert.equal(o.error, error)
  assert.equal(
    reads,
    1,
    'a partly applied callback cannot be retried with an empty cursor',
  )
  await o.dispose()
})

test('an errno-shaped handoff failure cannot become a recoverable reader failure', async (t) => {
  const o = observation(),
    error = busy('fold failed')
  let reads = 0
  t.mock.method(o.reader, 'read', async () => {
    reads++
    return { rows: [], reset: false }
  })
  await o.poll()
  await assert.rejects(
    o.attach(async () => {
      throw error
    }),
    (actual) => actual === error,
  )
  await assert.rejects(o.poll(), (actual) => actual === error)
  assert.equal(o.error, error)
  assert.equal(reads, 1)
  await o.dispose()
})

test('malformed committed data stays fatal even if a later cursor read would be empty', async (t) => {
  const o = observation(),
    error = new SyntaxError('invalid complete transcript record')
  let reads = 0
  t.mock.method(o.reader, 'read', async () => {
    if (++reads === 1) throw error
    return { rows: [], reset: false }
  })
  await assert.rejects(o.poll(), (actual) => actual === error)
  await assert.rejects(o.poll(), (actual) => actual === error)
  assert.equal(o.error, error)
  assert.equal(reads, 1)
  await o.dispose()
})
