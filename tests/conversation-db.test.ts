import assert from 'node:assert/strict'
import {
  mkdtemp,
  mkdir,
  rename,
  rm,
  symlink,
  writeFile,
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { test, type TestContext } from 'node:test'
import {
  captureConversationStepBaseline,
  classifyStepError,
  parseGoDurationMs,
  readConversationCompactionWindow,
  readConversationStepError,
} from '../server/conversation-db.ts'

const ID_A = '11111111-1111-4111-8111-111111111111'
const ID_B = '22222222-2222-4222-8222-222222222222'
const ID_C = '33333333-3333-4333-8333-333333333333'
const ID_D = '44444444-4444-4444-8444-444444444444'

async function createEnv(t: TestContext) {
  const home = await mkdtemp(join(tmpdir(), 'agy-conv-db-test-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const appRoot = join(home, '.gemini', 'antigravity-cli')
  const brainRoot = join(appRoot, 'brain')
  const convDir = join(appRoot, 'conversations')
  await mkdir(brainRoot, { recursive: true })
  await mkdir(convDir, { recursive: true })
  return { home, brainRoot, convDir }
}

function encodeVarint(value: number): number[] {
  return [
    (value & 0x7f) | 0x80,
    ((value >> 7) & 0x7f) | 0x80,
    (value >> 14) & 0x7f,
  ]
}

function encodeLengthDelimited(fieldNumber: number, payload: Uint8Array) {
  return new Uint8Array([(fieldNumber << 3) | 2, payload.length, ...payload])
}

function encodeCompactionMetadata(maxTokens: number, estimatedTokens: number) {
  const inner = new Uint8Array([
    (1 << 3) | 0,
    ...encodeVarint(estimatedTokens),
    (4 << 3) | 0,
    ...encodeVarint(maxTokens),
  ])
  return encodeLengthDelimited(
    1,
    encodeLengthDelimited(9, encodeLengthDelimited(10, inner)),
  )
}

test('readConversationCompactionWindow returns defensive copies on cache miss and cache hit', async (t) => {
  const { brainRoot, convDir } = await createEnv(t)
  const dbPath = join(convDir, `${ID_A}.db`)
  const db = new DatabaseSync(dbPath)
  try {
    db.exec('CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)')
    db.prepare('INSERT INTO gen_metadata (idx, data) VALUES (?, ?)').run(
      1,
      encodeCompactionMetadata(200_000, 30_000),
    )
  } finally {
    db.close()
  }

  let openCount = 0
  const openDb = (path: string) => {
    openCount += 1
    return new DatabaseSync(path, { readOnly: true })
  }

  const first = await readConversationCompactionWindow(brainRoot, ID_A, -1, {
    openDb,
  })
  assert.deepEqual(first, {
    maxTokens: 200_000,
    estimatedTokens: 30_000,
    genIdx: 1,
  })
  assert.equal(openCount, 1)

  first.maxTokens = 1
  first.estimatedTokens = 2
  first.genIdx = 999

  const second = await readConversationCompactionWindow(brainRoot, ID_A, -1, {
    openDb,
  })
  assert.equal(openCount, 1)
  assert.deepEqual(second, {
    maxTokens: 200_000,
    estimatedTokens: 30_000,
    genIdx: 1,
  })

  second.maxTokens = 3
  const third = await readConversationCompactionWindow(brainRoot, ID_A, -1, {
    openDb,
  })
  assert.equal(openCount, 1)
  assert.deepEqual(third, {
    maxTokens: 200_000,
    estimatedTokens: 30_000,
    genIdx: 1,
  })
})

test('parseGoDurationMs parses valid Go durations including both microsecond symbols and rejects invalid inputs', () => {
  assert.equal(parseGoDurationMs('1h2m3.5s'), 3_723_500)
  assert.equal(parseGoDurationMs('45s'), 45_000)
  assert.equal(parseGoDurationMs('250ms'), 250)
  assert.equal(parseGoDurationMs('500\u00b5s'), 0.5)
  assert.equal(parseGoDurationMs('500\u03bcs'), 0.5)
  assert.equal(parseGoDurationMs('100ns'), 0.0001)
  assert.equal(parseGoDurationMs('0s'), 0)
  assert.equal(parseGoDurationMs('1m500ms250\u00b5s'), 60_500.25)

  assert.equal(parseGoDurationMs(''), undefined)
  assert.equal(parseGoDurationMs('-5s'), undefined)
  assert.equal(parseGoDurationMs('1d'), undefined)
  assert.equal(parseGoDurationMs('123'), undefined)
  assert.equal(parseGoDurationMs('abc'), undefined)
  assert.equal(parseGoDurationMs('1.2.3s'), undefined)
  assert.equal(parseGoDurationMs(' 5s'), undefined)
  assert.equal(parseGoDurationMs('5s '), undefined)
  assert.equal(parseGoDurationMs(`${'9'.repeat(400)}h`), undefined)
})

test('classifyStepError prioritizes invalid thinking signature variants', () => {
  const fieldRequired = classifyStepError({
    stepType: 17,
    payloadText:
      'Error 400, Message: messages.4.content.0.thinking.signature: Field required, Status: INVALID_ARGUMENT',
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.equal(fieldRequired?.kind, 'invalid_thinking_signature')
  assert.match(fieldRequired?.message ?? '', /new session/i)
  assert.equal(fieldRequired?.statusCode, 400)
  assert.equal(fieldRequired?.statusName, 'INVALID_ARGUMENT')
  assert.equal(fieldRequired?.reachedModel, false)

  const invalidBlock = classifyStepError({
    stepType: 17,
    payloadText:
      'messages.12.content.1: Invalid `signature` in `thinking` block',
    stopReason: 'end_turn',
    reachedModel: true,
  })
  assert.equal(invalidBlock?.kind, 'invalid_thinking_signature')
  assert.match(invalidBlock?.message ?? '', /new session/i)
  assert.equal(invalidBlock?.reachedModel, true)
})

test('classifyStepError classifies quota exhaustion and parses reset timestamp and retry delays', () => {
  const withTimestampAndDelay = classifyStepError({
    stepType: 17,
    payloadText:
      'reason:QUOTA_EXHAUSTED quotaResetTimeStamp:"2026-11-23T06:00:00Z" quotaResetDelay:"1h2m3.5s"',
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.equal(withTimestampAndDelay?.kind, 'quota_exhausted')
  assert.equal(withTimestampAndDelay?.resetAt, '2026-11-23T06:00:00Z')
  assert.equal(withTimestampAndDelay?.retryAfterMs, 3_723_500)
  assert.match(withTimestampAndDelay?.message ?? '', /2026-11-23T06:00:00Z/)
  assert.match(withTimestampAndDelay?.message ?? '', /1h2m3\.5s/)
  assert.equal(withTimestampAndDelay?.reachedModel, false)

  const withMicroSign = classifyStepError({
    stepType: 17,
    payloadText: 'RESOURCE_EXHAUSTED retryDelay="500\u00b5s"',
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.equal(withMicroSign?.kind, 'quota_exhausted')
  assert.equal(withMicroSign?.retryAfterMs, 0.5)

  const withGreekMu = classifyStepError({
    stepType: 17,
    payloadText: 'RESOURCE_EXHAUSTED retryDelay="500\u03bcs"',
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.equal(withGreekMu?.kind, 'quota_exhausted')
  assert.equal(withGreekMu?.retryAfterMs, 0.5)

  const go429 = classifyStepError({
    stepType: 17,
    payloadText:
      'Error 429, Message: Too many requests, Status: RESOURCE_EXHAUSTED',
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.equal(go429?.kind, 'quota_exhausted')
  assert.equal(go429?.statusCode, 429)
  assert.equal(go429?.statusName, 'RESOURCE_EXHAUSTED')

  const agy429 = classifyStepError({
    stepType: 17,
    payloadText:
      'RESOURCE_EXHAUSTED (code 429): rate limit exceeded retryDelay=45s',
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.equal(agy429?.kind, 'quota_exhausted')
  assert.equal(agy429?.statusCode, 429)
  assert.equal(agy429?.statusName, 'RESOURCE_EXHAUSTED')
  assert.equal(agy429?.retryAfterMs, 45_000)

  const refusalQuotaCue = classifyStepError({
    stopReason: 'refusal',
    payloadText: 'HTTP 429 Too Many Requests retryDelay="250ms"',
    reachedModel: true,
  })
  assert.equal(refusalQuotaCue?.kind, 'quota_exhausted')
  assert.equal(refusalQuotaCue?.retryAfterMs, 250)
  assert.equal(refusalQuotaCue?.reachedModel, true)
})

test('classifyStepError parses Go and agy 1.5 error formats and caps message length', () => {
  const goError = classifyStepError({
    stepType: 17,
    payloadText:
      'Error 503, Message: The model is overloaded.\nPlease retry later., Status: UNAVAILABLE',
    stopReason: 'end_turn',
    reachedModel: true,
  })
  assert.equal(goError?.kind, 'api_error')
  assert.equal(goError?.statusCode, 503)
  assert.equal(goError?.statusName, 'UNAVAILABLE')
  assert.match(goError?.message ?? '', /The model is overloaded/)
  assert.equal(goError?.reachedModel, true)

  const agyError = classifyStepError({
    stepType: 17,
    payloadText:
      '\x0a\x10INTERNAL (code 500): upstream gateway failed\x00\x01binary-tail',
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.equal(agyError?.kind, 'api_error')
  assert.equal(agyError?.statusCode, 500)
  assert.equal(agyError?.statusName, 'INTERNAL')
  assert.match(agyError?.message ?? '', /upstream gateway failed/)
  assert.doesNotMatch(agyError?.message ?? '', /binary-tail/)

  const longGoError = classifyStepError({
    stepType: 17,
    payloadText: `Error 502, Message: ${'x'.repeat(1000)}, Status: BAD_GATEWAY`,
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.equal(longGoError?.kind, 'api_error')
  assert.ok([...(longGoError?.message ?? '')].length <= 512)

  const surrogateGoError = classifyStepError({
    stepType: 17,
    payloadText: `Error 502, Message: ${'🔥'.repeat(600)}, Status: BAD_GATEWAY`,
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.equal(surrogateGoError?.kind, 'api_error')
  assert.equal([...(surrogateGoError?.message ?? '')].length, 512)
  assert.equal(surrogateGoError?.message.endsWith('🔥…'), true)
})

test('classifyStepError handles refusal fallbacks, generic step 17 fallback, retry warnings, and non-17 steps', () => {
  assert.deepEqual(
    classifyStepError({ stopReason: 'refusal', reachedModel: false }),
    {
      kind: 'quota_exhausted',
      message:
        'Request was refused before reaching the model (quota exhausted or rate limited).',
      reachedModel: false,
    },
  )

  assert.deepEqual(
    classifyStepError({ stopReason: 'refusal', reachedModel: true }),
    {
      kind: 'refusal',
      message: 'Model refused to complete the turn.',
      reachedModel: true,
    },
  )

  const generic = classifyStepError({
    stepType: 17,
    payloadText: '\x00\x07  upstream timeout  \x1f',
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.deepEqual(generic, {
    kind: 'api_error',
    message: 'upstream timeout',
    reachedModel: false,
  })

  const emptyGeneric = classifyStepError({
    stepType: 17,
    payloadText: '\x00\x01\x02',
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.deepEqual(emptyGeneric, {
    kind: 'api_error',
    message: 'Upstream API error recorded by agy',
    reachedModel: false,
  })

  assert.equal(
    classifyStepError({
      stepType: 17,
      payloadText:
        'The stream was interrupted. Please continue the task you were working on.',
      stopReason: 'end_turn',
      reachedModel: true,
    }),
    undefined,
  )
  assert.equal(
    classifyStepError({
      stepType: 17,
      payloadText: 'API error (attempt 2): temporary 503',
      stopReason: 'end_turn',
      reachedModel: true,
    }),
    undefined,
  )
  assert.equal(
    classifyStepError({
      stepType: 17,
      payloadText:
        'There was a problem with the request.\nRetries remaining: 3',
      stopReason: 'end_turn',
      reachedModel: true,
    }),
    undefined,
  )

  const exhaustedRetry = classifyStepError({
    stepType: 17,
    payloadText: 'There was a problem with the request.\nRetries remaining: 0',
    stopReason: 'end_turn',
    reachedModel: false,
  })
  assert.equal(exhaustedRetry?.kind, 'api_error')

  assert.equal(
    classifyStepError({
      stepType: 15,
      payloadText: 'RESOURCE_EXHAUSTED',
      stopReason: 'end_turn',
      reachedModel: true,
    }),
    undefined,
  )
})

test('captureConversationStepBaseline and readConversationStepError read BLOB step_payload and error_details', async (t) => {
  const { brainRoot, convDir } = await createEnv(t)
  const dbPath = join(convDir, `${ID_A}.db`)
  const db = new DatabaseSync(dbPath)
  try {
    db.exec(
      'CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_payload BLOB, error_details BLOB)',
    )
    db.prepare(
      'INSERT INTO steps (idx, step_type, step_payload, error_details) VALUES (?, ?, ?, ?)',
    ).run(
      0,
      17,
      Buffer.from('Error 500, Message: stale, Status: INTERNAL'),
      null,
    )
  } finally {
    db.close()
  }

  const baseline = await captureConversationStepBaseline(brainRoot, ID_A)
  assert.equal(baseline.maxIdx, 0)
  assert.equal(typeof baseline.dev, 'bigint')
  assert.equal(typeof baseline.ino, 'bigint')

  assert.equal(
    await readConversationStepError(brainRoot, ID_A, baseline, 'end_turn'),
    undefined,
  )

  const dbWrite = new DatabaseSync(dbPath)
  try {
    dbWrite
      .prepare(
        'INSERT INTO steps (idx, step_type, step_payload, error_details) VALUES (?, ?, ?, ?)',
      )
      .run(1, 15, Buffer.from('planner step'), null)
    dbWrite
      .prepare(
        'INSERT INTO steps (idx, step_type, step_payload, error_details) VALUES (?, ?, ?, ?)',
      )
      .run(
        2,
        17,
        Buffer.from('\x0a\x20reason:QUOTA_EXHAUSTED'),
        Buffer.from(
          'quotaResetTimeStamp="2026-11-23T08:00:00Z" quotaResetDelay="2m30s"',
        ),
      )
  } finally {
    dbWrite.close()
  }

  const detected = await readConversationStepError(
    brainRoot,
    ID_A,
    baseline,
    'end_turn',
  )
  assert.equal(detected?.kind, 'quota_exhausted')
  assert.equal(detected?.resetAt, '2026-11-23T08:00:00Z')
  assert.equal(detected?.retryAfterMs, 150_000)
  assert.equal(detected?.reachedModel, true)
})

test('readConversationStepError reads newly created DB when baseline predated DB creation', async (t) => {
  const { brainRoot, convDir } = await createEnv(t)

  const baseline = await captureConversationStepBaseline(brainRoot, ID_B)
  assert.deepEqual(baseline, { maxIdx: -1 })

  const dbPath = join(convDir, `${ID_B}.db`)
  const db = new DatabaseSync(dbPath)
  try {
    db.exec(
      'CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_payload BLOB)',
    )
    db.prepare(
      'INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)',
    ).run(
      0,
      17,
      Buffer.from('INTERNAL (code 500): cold-start upstream failure'),
    )
  } finally {
    db.close()
  }

  const detected = await readConversationStepError(
    brainRoot,
    ID_B,
    baseline,
    'end_turn',
  )
  assert.equal(detected?.kind, 'api_error')
  assert.equal(detected?.statusCode, 500)
  assert.equal(detected?.statusName, 'INTERNAL')
  assert.match(detected?.message ?? '', /cold-start upstream failure/)
  assert.equal(detected?.reachedModel, false)
})

test('readConversationStepError handles TEXT payloads, refusal after model step, and retry warnings in SQLite', async (t) => {
  const { brainRoot, convDir } = await createEnv(t)
  const dbPath = join(convDir, `${ID_D}.db`)
  const db = new DatabaseSync(dbPath)
  try {
    db.exec(
      'CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_payload TEXT)',
    )
    db.prepare(
      'INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)',
    ).run(0, 15, 'assistant partial output')
  } finally {
    db.close()
  }

  const refusalAfterModel = await readConversationStepError(
    brainRoot,
    ID_D,
    { maxIdx: -1 },
    'refusal',
  )
  assert.deepEqual(refusalAfterModel, {
    kind: 'refusal',
    message: 'Model refused to complete the turn.',
    reachedModel: true,
  })

  const dbWrite = new DatabaseSync(dbPath)
  try {
    dbWrite
      .prepare(
        'INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)',
      )
      .run(1, 17, 'API error (attempt 1): transient 503')
  } finally {
    dbWrite.close()
  }

  assert.equal(
    await readConversationStepError(brainRoot, ID_D, { maxIdx: 0 }, 'end_turn'),
    undefined,
  )

  const dbWrite2 = new DatabaseSync(dbPath)
  try {
    dbWrite2
      .prepare(
        'INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)',
      )
      .run(
        2,
        17,
        'Error 503, Message: text payload overload, Status: UNAVAILABLE',
      )
  } finally {
    dbWrite2.close()
  }

  const textErr = await readConversationStepError(
    brainRoot,
    ID_D,
    { maxIdx: 1 },
    'end_turn',
  )
  assert.equal(textErr?.kind, 'api_error')
  assert.equal(textErr?.statusCode, 503)
  assert.equal(textErr?.statusName, 'UNAVAILABLE')
  assert.match(textErr?.message ?? '', /text payload overload/)
  assert.equal(textErr?.reachedModel, false)
})

test('readConversationStepError fails closed on (dev, ino) mismatch without opening SQLite', async (t) => {
  const { brainRoot, convDir } = await createEnv(t)
  const dbPath = join(convDir, `${ID_C}.db`)
  const db1 = new DatabaseSync(dbPath)
  try {
    db1.exec(
      'CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_payload BLOB)',
    )
    db1
      .prepare(
        'INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)',
      )
      .run(0, 15, Buffer.from('ok'))
  } finally {
    db1.close()
  }

  const baseline = await captureConversationStepBaseline(brainRoot, ID_C)
  assert.equal(baseline.maxIdx, 0)

  // Retain old inode under a backup name so the new DB is guaranteed a distinct inode on POSIX.
  await rename(dbPath, join(convDir, `${ID_C}.old.db`))
  const db2 = new DatabaseSync(dbPath)
  try {
    db2.exec(
      'CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_payload BLOB)',
    )
    db2
      .prepare(
        'INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)',
      )
      .run(1, 17, Buffer.from('Error 500, Message: replaced, Status: INTERNAL'))
  } finally {
    db2.close()
  }

  let openCalls = 0
  const openDb = (path: string) => {
    openCalls += 1
    return new DatabaseSync(path, { readOnly: true })
  }

  assert.equal(
    await readConversationStepError(brainRoot, ID_C, baseline, 'end_turn', {
      openDb,
    }),
    undefined,
  )
  assert.equal(openCalls, 0)

  assert.deepEqual(
    await readConversationStepError(brainRoot, ID_C, baseline, 'refusal', {
      openDb,
    }),
    {
      kind: 'quota_exhausted',
      message:
        'Request was refused before reaching the model (quota exhausted or rate limited).',
      reachedModel: false,
    },
  )
  assert.equal(openCalls, 0)
})

test('baseline capture and step error read fail closed on symlink escape, missing steps table, corrupt DB, and invalid UUID', async (t) => {
  const { home, brainRoot, convDir } = await createEnv(t)

  // Invalid UUID
  assert.deepEqual(
    await captureConversationStepBaseline(brainRoot, '../escape'),
    { maxIdx: -1 },
  )
  assert.equal(
    await readConversationStepError(
      brainRoot,
      '../escape',
      { maxIdx: -1 },
      'end_turn',
    ),
    undefined,
  )
  assert.equal(
    (
      await readConversationStepError(
        brainRoot,
        '../escape',
        { maxIdx: -1 },
        'refusal',
      )
    )?.kind,
    'quota_exhausted',
  )

  // Symlink escape
  const outsidePath = join(home, 'outside.db')
  const outsideDb = new DatabaseSync(outsidePath)
  try {
    outsideDb.exec(
      'CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_payload BLOB)',
    )
    outsideDb
      .prepare(
        'INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)',
      )
      .run(0, 17, Buffer.from('Error 500, Message: escaped, Status: INTERNAL'))
  } finally {
    outsideDb.close()
  }
  await symlink(outsidePath, join(convDir, `${ID_A}.db`))
  assert.deepEqual(await captureConversationStepBaseline(brainRoot, ID_A), {
    maxIdx: -1,
    unreadable: true,
  })
  assert.equal(
    await readConversationStepError(
      brainRoot,
      ID_A,
      { maxIdx: -1 },
      'end_turn',
    ),
    undefined,
  )
  assert.equal(
    (
      await readConversationStepError(
        brainRoot,
        ID_A,
        { maxIdx: -1 },
        'refusal',
      )
    )?.kind,
    'quota_exhausted',
  )

  // Missing steps table
  const noStepsDb = new DatabaseSync(join(convDir, `${ID_B}.db`))
  try {
    noStepsDb.exec('CREATE TABLE other (id INTEGER PRIMARY KEY)')
  } finally {
    noStepsDb.close()
  }
  const missingStepsBaseline = await captureConversationStepBaseline(
    brainRoot,
    ID_B,
  )
  assert.equal(missingStepsBaseline.maxIdx, -1)
  assert.equal(typeof missingStepsBaseline.dev, 'bigint')
  assert.equal(typeof missingStepsBaseline.ino, 'bigint')
  assert.equal(missingStepsBaseline.unreadable, undefined)
  assert.equal(
    await readConversationStepError(
      brainRoot,
      ID_B,
      missingStepsBaseline,
      'end_turn',
    ),
    undefined,
  )
  assert.equal(
    (
      await readConversationStepError(
        brainRoot,
        ID_B,
        missingStepsBaseline,
        'refusal',
      )
    )?.kind,
    'quota_exhausted',
  )

  // Corrupt DB
  await writeFile(join(convDir, `${ID_D}.db`), 'definitely-not-sqlite')
  const corruptBaseline = await captureConversationStepBaseline(brainRoot, ID_D)
  assert.equal(corruptBaseline.maxIdx, -1)
  assert.equal(corruptBaseline.unreadable, true)
  assert.equal(
    await readConversationStepError(
      brainRoot,
      ID_D,
      corruptBaseline,
      'end_turn',
    ),
    undefined,
  )
  assert.equal(
    (
      await readConversationStepError(
        brainRoot,
        ID_D,
        corruptBaseline,
        'refusal',
      )
    )?.kind,
    'quota_exhausted',
  )
})

test('readConversationStepError ignores non-17 step_payload prose on refusal', async (t) => {
  const { brainRoot, convDir } = await createEnv(t)
  const dbPath = join(convDir, `${ID_A}.db`)
  const db = new DatabaseSync(dbPath)
  try {
    db.exec(
      'CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_payload BLOB, error_details BLOB)',
    )
    db.prepare(
      'INSERT INTO steps (idx, step_type, step_payload, error_details) VALUES (?, ?, ?, ?)',
    ).run(
      0,
      14,
      Buffer.from(
        'User asked about Error 500, Message: fake boom, Status: INTERNAL and HTTP 429 rate limit retryDelay="10s"',
      ),
      null,
    )
  } finally {
    db.close()
  }

  const userOnlyRefusal = await readConversationStepError(
    brainRoot,
    ID_A,
    { maxIdx: -1 },
    'refusal',
  )
  assert.deepEqual(userOnlyRefusal, {
    kind: 'quota_exhausted',
    message:
      'Request was refused before reaching the model (quota exhausted or rate limited).',
    reachedModel: false,
  })

  const dbWrite = new DatabaseSync(dbPath)
  try {
    dbWrite
      .prepare(
        'INSERT INTO steps (idx, step_type, step_payload, error_details) VALUES (?, ?, ?, ?)',
      )
      .run(
        1,
        15,
        Buffer.from(
          'Assistant mentioned reason:QUOTA_EXHAUSTED and HTTP 429 rate limit in explanation before refusing',
        ),
        null,
      )
  } finally {
    dbWrite.close()
  }

  const plannerRefusal = await readConversationStepError(
    brainRoot,
    ID_A,
    { maxIdx: -1 },
    'refusal',
  )
  assert.deepEqual(plannerRefusal, {
    kind: 'refusal',
    message: 'Model refused to complete the turn.',
    reachedModel: true,
  })
})

test('readConversationStepError does not re-attribute stale errors when baseline capture hits transient SQLite failure', async (t) => {
  const { brainRoot, convDir } = await createEnv(t)
  const dbPath = join(convDir, `${ID_B}.db`)
  const db = new DatabaseSync(dbPath)
  try {
    db.exec(
      'CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_payload BLOB)',
    )
    db.prepare(
      'INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)',
    ).run(
      0,
      17,
      Buffer.from(
        'Error 500, Message: stale prior-turn error, Status: INTERNAL',
      ),
    )
  } finally {
    db.close()
  }

  const baseline = await captureConversationStepBaseline(brainRoot, ID_B, {
    openDb: () => {
      throw new Error('SQLITE_BUSY')
    },
  })
  assert.equal(baseline.unreadable, true)

  let postOpenCalls = 0
  const postOpenDb = (path: string) => {
    postOpenCalls += 1
    return new DatabaseSync(path, { readOnly: true })
  }

  assert.equal(
    await readConversationStepError(brainRoot, ID_B, baseline, 'end_turn', {
      openDb: postOpenDb,
    }),
    undefined,
  )
  assert.equal(postOpenCalls, 0)

  assert.deepEqual(
    await readConversationStepError(brainRoot, ID_B, baseline, 'refusal', {
      openDb: postOpenDb,
    }),
    {
      kind: 'quota_exhausted',
      message:
        'Request was refused before reaching the model (quota exhausted or rate limited).',
      reachedModel: false,
    },
  )
  assert.equal(postOpenCalls, 0)
})
