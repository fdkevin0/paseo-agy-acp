import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  mkdtemp,
  mkdir,
  writeFile,
  readFile,
  rm,
  utimes,
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { hashAccountKey } from '@getpaseo/plugin/server/usage'
import { AgyUsageSource, parseUsage, readLoggedEmail } from '../server/usage.ts'

const groups = ['Gemini Models', 'Claude and GPT models'].map((name) => ({
  name,
  buckets: [
    {
      window: '5h',
      remaining_fraction: 0.8,
      reset_time: '2026-11-08T10:00:00Z',
    },
    {
      window: 'weekly',
      remaining_fraction: 0.05,
      reset_time: '2026-11-15T10:00:00Z',
    },
  ],
}))
const envelope = (groups: unknown[]) =>
  JSON.stringify({ command: { data: { groups } } })

test('quota parser validates the CLI envelope and preserves unknown windows', () => {
  const parsed = parseUsage(envelope(groups))
  assert.equal(parsed[0].buckets[0].remainingFraction, 0.8)
  assert.equal(parsed[1].buckets[1].resetTime, '2026-11-15T10:00:00Z')
  assert.deepEqual(parseUsage(envelope([])), [])
  assert.throws(() => parseUsage('{}'))
  assert.throws(() =>
    parseUsage(
      envelope([
        { name: 'bad', buckets: [{ window: '5h', remaining_fraction: 'NaN' }] },
      ]),
    ),
  )
  assert.equal(
    parseUsage(
      envelope([{ name: 'unknown', buckets: [{ window: 'daily' }] }]),
    )[0].buckets[0].remainingFraction,
    undefined,
  )
})

test('email discovery uses recent logs, the latest match and bounded tails', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'agy-email-'))
  try {
    assert.equal(await readLoggedEmail(dir), undefined)
    await mkdir(join(dir, 'log'))
    await writeFile(
      join(dir, 'cli.log'),
      'applyAuthResult: email=outside@example.test\n' + 'x'.repeat(70_000),
    )
    assert.equal(await readLoggedEmail(dir), undefined)
    await writeFile(
      join(dir, 'log/cli-old.log'),
      'applyAuthResult: email=old@example.test, token=omitted\n',
    )
    await utimes(join(dir, 'log/cli-old.log'), 1, 1)
    await writeFile(
      join(dir, 'log/cli-new.log'),
      'applyAuthResult: email=first@example.test\nOAuth: authenticated successfully as latest@example.test\n',
    )
    assert.equal(await readLoggedEmail(dir), 'latest@example.test')
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test('usage discovers account scopes and maps windows, tier and failures through the CLI', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-usage-'))
  const home = join(root, 'home')
  const dir = join(home, '.gemini/antigravity-cli')
  const fixture = join(root, 'quota.json')
  const argsLog = join(root, 'args.json')
  const script = fileURLToPath(new URL('./mock-usage.ts', import.meta.url))
  const usage = new AgyUsageSource()
  const launch = {
    command: process.execPath,
    args: [
      script,
      'acp',
      '--stdio',
      '--gemini-dir',
      '.gemini',
      '--app-data-dir=antigravity-cli',
    ],
    env: { HOME: home, AGY_USAGE_FIXTURE: fixture, AGY_USAGE_ARGS: argsLog },
  }
  try {
    await mkdir(dir, { recursive: true })
    const scope = {
      kind: 'session' as const,
      provider: 'agy',
      env: { HOME: home },
    }
    assert.deepEqual(await usage.discover(scope), [])
    await writeFile(join(dir, 'antigravity-oauth-token'), '')
    assert.deepEqual(await usage.discover(scope), [])
    await writeFile(join(dir, 'antigravity-oauth-token'), 'fixture-token')
    assert.equal((await usage.discover(scope))[0].key, hashAccountKey(dir))
    await writeFile(
      join(dir, 'cli.log'),
      'applyAuthResult: email=example@example.test\n',
    )
    assert.equal(usage.remember(launch, {}, home), dir)
    usage.tiers.set(dir, 'Google AI Ultra')
    assert.deepEqual(await usage.discover({ ...scope, provider: 'codex' }), [])
    const [account] = await usage.discover(scope)
    assert.equal(account.label, 'example@example.test')
    assert.equal(account.harness, 'Antigravity')
    assert.equal(
      (await usage.discover({ kind: 'global' })).filter(
        (a) => a.key === account.key,
      ).length,
      1,
    )
    await writeFile(fixture, envelope(groups))
    const report = await usage.fetch(account.input)
    assert.equal(report.status, 'available')
    assert.ok(report.status === 'available')
    assert.equal(report.planLabel, 'Google AI Ultra')
    assert.deepEqual(
      report.windows.map((w) => w.id),
      [
        'gemini_models:five_hour',
        'gemini_models:weekly',
        'claude_and_gpt_models:five_hour',
        'claude_and_gpt_models:weekly',
      ],
    )
    assert.deepEqual(
      report.windows.map((w) => w.usedPct),
      [20, 95, 20, 95],
    )
    assert.deepEqual(
      report.windows.map((w) => w.tone),
      ['ok', 'danger', 'ok', 'danger'],
    )
    assert.ok(report.windows.every((w) => w.summary))
    assert.equal(report.windows[0].resetsAt, groups[0].buckets[0].reset_time)
    assert.deepEqual(JSON.parse(await readFile(argsLog, 'utf8')), [
      '--gemini-dir',
      '.gemini',
      '--app-data-dir=antigravity-cli',
      '-p',
      '/usage',
      '--output-format',
      'json',
    ])
    await writeFile(
      fixture,
      envelope([
        {
          name: 'Extra',
          buckets: [
            { window: 'daily' },
            { window: '5h', remaining_fraction: -1 },
            { window: 'weekly', remaining_fraction: 2 },
          ],
        },
      ]),
    )
    const clamped = await usage.fetch(account.input)
    assert.ok(clamped.status === 'available')
    assert.deepEqual(
      clamped.windows.map((w) => w.usedPct),
      [null, 100, 0],
    )
    assert.equal(clamped.windows[0].id, 'extra:daily')
    await writeFile(fixture, envelope([]))
    assert.deepEqual(await usage.fetch(account.input), {
      status: 'unavailable',
      problem: { kind: 'no_quota', detail: 'No usage quota reported' },
    })
    await writeFile(fixture, 'invalid JSON')
    assert.equal((await usage.fetch(account.input)).status, 'error')
    usage.remember(launch, { AGY_USAGE_FAIL: '1' }, home)
    const failure = await usage.fetch(account.input)
    assert.ok(
      failure.status === 'error' &&
        failure.error.includes('Authentication required'),
    )
    await assert.rejects(usage.fetch({ dataDir: dir, extra: true }))
    await rm(join(dir, 'antigravity-oauth-token'))
    await assert.rejects(
      usage.fetch(account.input),
      /login store no longer exists/,
    )
    // A known launch can use an OS-backed login store instead of the token file.
    const fresh = new AgyUsageSource()
    fresh.remember(launch, {}, home)
    const [known] = await fresh.discover(scope)
    await writeFile(fixture, envelope(groups))
    assert.equal((await fresh.fetch(known.input)).status, 'available')
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('default usage launch adds no ACP directory flags and keeps the session HOME', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-usage-default-'))
  const dir = join(home, '.gemini/antigravity-cli')
  const fixture = join(home, 'quota.json')
  const argsLog = join(home, 'args.json')
  const usage = new AgyUsageSource()
  try {
    await mkdir(dir, { recursive: true })
    await writeFile(join(dir, 'antigravity-oauth-token'), 'fixture-token')
    await writeFile(fixture, envelope(groups))
    usage.remember({
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-usage.ts', import.meta.url)), 'acp'],
      env: { HOME: home, AGY_USAGE_FIXTURE: fixture, AGY_USAGE_ARGS: argsLog },
    })
    const [account] = await usage.discover({
      kind: 'session',
      provider: 'agy',
      env: { HOME: home },
    })
    assert.equal((await usage.fetch(account.input)).status, 'available')
    assert.deepEqual(JSON.parse(await readFile(argsLog, 'utf8')), [
      '-p',
      '/usage',
      '--output-format',
      'json',
    ])
  } finally {
    await rm(home, { recursive: true, force: true })
  }
})
