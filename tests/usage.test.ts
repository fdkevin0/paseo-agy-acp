import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  appendFile,
  mkdtemp,
  mkdir,
  writeFile,
  readFile,
  realpath,
  rm,
  stat,
  symlink,
  utimes,
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { hashAccountKey } from '@getpaseo/plugin/server/usage'
import { AgyUsageSource, parseUsage, readLoggedEmail } from '../server/usage.ts'
import { isolatedEnv, waitUntil } from './helpers.ts'

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

test('email discovery reads bounded tail and startup head chunks and deduplicates cli.log', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-email-'))
  const dir = join(root, 'data')
  const outside = join(root, 'outside')
  try {
    await mkdir(dir)
    assert.equal(await readLoggedEmail(dir), undefined)
    await mkdir(join(dir, 'log'))
    await writeFile(
      join(dir, 'cli.log'),
      'x'.repeat(70_000) +
        '\napplyAuthResult: email=outside@example.test\n' +
        'x'.repeat(70_000),
    )
    assert.equal(await readLoggedEmail(dir), undefined)
    await writeFile(
      join(dir, 'cli.log'),
      'x'.repeat(65_505) +
        '\napplyAuthResult: email=truncated@example.test, token=omitted\n' +
        'y'.repeat(80_000),
    )
    assert.equal(await readLoggedEmail(dir), undefined)
    await writeFile(
      join(dir, 'cli.log'),
      'x'.repeat(10_000) +
        '\napplyAuthResult: email=complete@example.test\n' +
        'z'.repeat(55_460) +
        '\napplyAuthResult: email=truncated@example.test, token=omitted\n' +
        'y'.repeat(80_000),
    )
    assert.equal(await readLoggedEmail(dir), 'complete@example.test')
    await writeFile(
      join(dir, 'cli.log'),
      'x'.repeat(11_000) +
        '\napplyAuthResult: email=startup@example.test\n' +
        'y'.repeat(140_000),
    )
    assert.equal(await readLoggedEmail(dir), 'startup@example.test')
    await appendFile(
      join(dir, 'cli.log'),
      '\nOAuth: authenticated successfully as relogin@example.test\n',
    )
    await utimes(join(dir, 'cli.log'), 10, 10)
    assert.equal(await readLoggedEmail(dir), 'relogin@example.test')
    await writeFile(
      join(dir, 'log/cli-old.log'),
      'applyAuthResult: email=old@example.test, token=omitted\n',
    )
    await utimes(join(dir, 'log/cli-old.log'), 5, 5)
    await writeFile(
      join(dir, 'log/cli-new.log'),
      'applyAuthResult: email=first@example.test\nOAuth: authenticated successfully as latest@example.test\n',
    )
    await utimes(join(dir, 'log/cli-new.log'), 20, 20)
    assert.equal(await readLoggedEmail(dir), 'latest@example.test')
    await rm(join(dir, 'cli.log'))
    await symlink(join(dir, 'log/cli-new.log'), join(dir, 'cli.log'))
    await writeFile(join(dir, 'log/cli-new.log'), 'no auth banner here\n')
    await utimes(join(dir, 'log/cli-new.log'), 25, 25)
    assert.equal(await readLoggedEmail(dir), 'old@example.test')
    await writeFile(
      join(dir, 'log/acp-server-1234.log'),
      'applyAuthResult: email=acp@example.test, token=omitted\n',
    )
    await utimes(join(dir, 'log/acp-server-1234.log'), 30, 30)
    assert.equal(await readLoggedEmail(dir), 'acp@example.test')

    // 10 newer acp-server-*.log files without auth lines must not evict cli.log.
    await rm(join(dir, 'cli.log'))
    await writeFile(
      join(dir, 'cli.log'),
      'applyAuthResult: email=retained-cli@example.test\n',
    )
    await utimes(join(dir, 'cli.log'), 5, 5)
    await rm(join(dir, 'log'), { recursive: true, force: true })
    await mkdir(join(dir, 'log'))
    await Promise.all(
      Array.from({ length: 12 }, async (_, i) => {
        const logFile = join(dir, `log/acp-server-${i}.log`)
        await writeFile(logFile, 'ACP session started without auth banner\n')
        await utimes(logFile, 100 + i, 100 + i)
      }),
    )
    await mkdir(join(dir, 'log/cli-ignored-dir.log'))
    assert.equal(await readLoggedEmail(dir), 'retained-cli@example.test')

    // Symlinks escaping dataDir or a symlinked dataDir root must be rejected.
    await mkdir(outside)
    await writeFile(
      join(outside, 'secret.log'),
      'applyAuthResult: email=leaked@example.test\n',
    )
    await rm(join(dir, 'log'), { recursive: true, force: true })
    await mkdir(join(dir, 'log'))
    await rm(join(dir, 'cli.log'))
    await symlink(join(outside, 'secret.log'), join(dir, 'cli.log'))
    await symlink(join(outside, 'secret.log'), join(dir, 'log/cli-escape.log'))
    assert.equal(await readLoggedEmail(dir), undefined)
    await rm(join(dir, 'cli.log'))
    await writeFile(
      join(dir, 'cli.log'),
      'applyAuthResult: email=real@example.test\n',
    )
    await symlink(dir, join(root, 'linked-data'))
    assert.equal(await readLoggedEmail(join(root, 'linked-data')), undefined)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('keychain logins without a token file support cold global discovery, account isolation and fast auth abort', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-keychain-'))
  const sessionHome = join(home, 'session-home')
  const symlinkTokenHome = join(home, 'symlink-token-home')
  const unrememberedHome = join(home, 'unremembered-home')
  const customHome = join(home, 'custom-home')
  const binDir = join(home, 'bin')
  const dir = join(home, '.gemini/antigravity-cli')
  const sessionDir = join(sessionHome, '.gemini/antigravity-cli')
  const symlinkTokenDir = join(symlinkTokenHome, '.gemini/antigravity-cli')
  const fixture = join(home, 'quota.json')
  const argsLog = join(home, 'args.json')
  const pidFile = join(home, 'hung.pid')
  const mockPath = fileURLToPath(new URL('./mock-usage.ts', import.meta.url))
  try {
    await mkdir(dir, { recursive: true })
    await mkdir(sessionDir, { recursive: true })
    await mkdir(symlinkTokenDir, { recursive: true })
    await mkdir(binDir, { recursive: true })
    await writeFile(
      join(binDir, 'agy'),
      `#!/bin/sh\nexec "${process.execPath}" "${mockPath}" "$@"\n`,
      { mode: 0o755 },
    )
    await writeFile(
      join(binDir, 'agy.cmd'),
      `@"${process.execPath}" "${mockPath}" %*\r\n`,
    )
    await writeFile(
      join(dir, 'cli.log'),
      'x'.repeat(11_000) +
        '\napplyAuthResult: email=keychain@example.test\n' +
        'y'.repeat(100_000),
    )
    await writeFile(fixture, envelope(groups))
    await symlink(fixture, join(symlinkTokenDir, 'antigravity-oauth-token'))

    const baseEnv = isolatedEnv(home, {
      PATH: binDir,
      AGY_USAGE_FIXTURE: fixture,
      AGY_USAGE_ARGS: argsLog,
    })
    const usage = new AgyUsageSource({ env: baseEnv })

    // Symlinked legacy token files must never pass hasTokenFile().
    assert.deepEqual(
      await usage.discover({
        kind: 'session',
        provider: 'agy',
        env: { HOME: symlinkTokenHome },
      }),
      [],
    )

    // Cold global discovery must find keychain logins from startup log lines without remember().
    const discoveredGlobal = await usage.discover({ kind: 'global' })
    assert.equal(discoveredGlobal.length, 1)
    const [account] = discoveredGlobal
    assert.equal(account.key, hashAccountKey(dir))
    assert.equal(account.label, 'keychain@example.test')
    const globalReport = await usage.fetch(account.input)
    assert.equal(globalReport.status, 'available')
    assert.deepEqual(JSON.parse(await readFile(argsLog, 'utf8')), [
      '-p',
      '/usage',
      '--output-format',
      'json',
    ])

    // Unlaunched session discovery with broken env must not poison subsequent global fetches on defaultDir.
    await usage.discover({
      kind: 'session',
      provider: 'agy',
      env: { PATH: '/nonexistent-broken-path' },
    })
    await usage.discover({ kind: 'global' })
    assert.equal((await usage.fetch({ dataDir: dir })).status, 'available')

    // Removing the only login log must hide an unlaunched account on the same singleton.
    await rm(join(dir, 'cli.log'))
    assert.deepEqual(await usage.discover({ kind: 'global' }), [])

    // Unlaunched session-scoped discovery uses its own HOME without leaking to global discovery.
    await writeFile(
      join(sessionDir, 'cli.log'),
      'applyAuthResult: email=session@example.test\n',
    )
    const sessionScope = {
      kind: 'session' as const,
      provider: 'agy',
      env: isolatedEnv(sessionHome, {
        PATH: binDir,
        AGY_USAGE_FIXTURE: fixture,
        AGY_USAGE_ARGS: argsLog,
      }),
    }
    const [sessionAccount] = await usage.discover(sessionScope)
    assert.equal(sessionAccount.key, hashAccountKey(sessionDir))
    assert.equal(sessionAccount.label, 'session@example.test')
    assert.equal((await usage.fetch(sessionAccount.input)).status, 'available')
    await rm(join(sessionDir, 'cli.log'))
    assert.deepEqual(await usage.discover(sessionScope), [])
    assert.deepEqual(await usage.discover({ kind: 'global' }), [])

    // Over 100 unlaunched session discoveries must not evict remembered custom launches.
    const customUsage = new AgyUsageSource({ env: baseEnv })
    const customDir = customUsage.remember(
      {
        command: process.execPath,
        args: [mockPath, 'acp'],
        env: { ...baseEnv, HOME: customHome },
      },
      {},
      customHome,
    )
    await mkdir(customDir, { recursive: true })
    await Promise.all(
      Array.from({ length: 105 }, async (_, i) => {
        const sHome = join(home, `many-${i}`)
        const sDir = join(sHome, '.gemini/antigravity-cli')
        await mkdir(sDir, { recursive: true })
        await writeFile(
          join(sDir, 'cli.log'),
          `applyAuthResult: email=session-${i}@example.test\n`,
        )
        await customUsage.discover({
          kind: 'session',
          provider: 'agy',
          env: { ...baseEnv, HOME: sHome },
        })
      }),
    )
    const retainedGlobal = await customUsage.discover({ kind: 'global' })
    assert.equal(
      retainedGlobal.some((a) => a.key === hashAccountKey(customDir)),
      true,
    )
    assert.equal(
      (await customUsage.fetch({ dataDir: customDir })).status,
      'available',
    )

    // Non-default account directories without launch context must fail instead of querying default HOME.
    await rm(argsLog, { force: true })
    const isolated = await usage.fetch({
      dataDir: join(unrememberedHome, '.gemini/antigravity-cli'),
    })
    assert.ok(isolated.status === 'error')
    assert.match(isolated.error, /Account launch context is not available/)
    assert.equal(await stat(argsLog).catch(() => null), null)

    // Unauthenticated CLI OAuth prompts must abort fast, terminate the child process, and scrub PKCE URLs.
    await writeFile(
      join(dir, 'cli.log'),
      'applyAuthResult: email=keychain@example.test\n',
    )
    const hungUsage = new AgyUsageSource({
      env: {
        ...baseEnv,
        AGY_USAGE_AUTH_HANG: '1',
        AGY_USAGE_PID_FILE: pidFile,
      },
    })
    const [hungAccount] = await hungUsage.discover({ kind: 'global' })
    const hungReport = await hungUsage.fetch(hungAccount.input)
    assert.deepEqual(hungReport, {
      status: 'error',
      error: 'Authentication required; run agy to sign in',
    })
    const hungPid = Number(await readFile(pidFile, 'utf8'))
    assert.ok(Number.isInteger(hungPid) && hungPid > 0)
    await waitUntil(() => {
      try {
        process.kill(hungPid, 0)
        return false
      } catch (error) {
        return (error as NodeJS.ErrnoException).code === 'ESRCH'
      }
    })
  } finally {
    await rm(home, { recursive: true, force: true })
  }
})

test('usage discovers account scopes and maps windows, tier and failures through the CLI', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'agy-usage-')))
  const home = join(root, 'home')
  const dir = join(home, '.gemini/antigravity-cli')
  const fixture = join(root, 'quota.json')
  const argsLog = join(root, 'args.json')
  const script = fileURLToPath(new URL('./mock-usage.ts', import.meta.url))
  const baseEnv = isolatedEnv(home)
  const usage = new AgyUsageSource({ env: baseEnv })
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
    env: isolatedEnv(home, {
      AGY_USAGE_FIXTURE: fixture,
      AGY_USAGE_ARGS: argsLog,
    }),
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
    const [beforeRemember] = await usage.discover(scope)
    assert.equal(beforeRemember.key, hashAccountKey(dir))
    assert.deepEqual(beforeRemember.input, { dataDir: dir })
    await writeFile(
      join(dir, 'cli.log'),
      'applyAuthResult: email=example@example.test\n',
    )
    assert.equal(usage.remember(launch, {}, home), dir)
    usage.tiers.set(dir, 'Google AI Ultra')
    assert.deepEqual(await usage.discover({ ...scope, provider: 'codex' }), [])
    const [account] = await usage.discover(scope)
    assert.deepEqual(account.input, beforeRemember.input)
    assert.equal(
      (await usage.discover({ ...scope, provider: 'antigravity' }))[0]?.key,
      account.key,
    )
    assert.equal(account.label, 'example@example.test')
    assert.equal(account.harness, 'Antigravity')
    assert.deepEqual(await usage.discover({ kind: 'global' }), [account])
    await writeFile(
      fixture,
      envelope(
        groups.map((group) => ({
          ...group,
          buckets: [...group.buckets].reverse(),
        })),
      ),
    )
    const report = await usage.fetch(account.input)
    assert.ok(report.status === 'available')
    assert.equal(report.planLabel, 'Ultra')
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
    assert.deepEqual(
      report.windows.map((w) => w.label),
      [
        'Gemini · 5-hour',
        'Gemini · Weekly',
        'Claude / GPT · 5-hour',
        'Claude / GPT · Weekly',
      ],
    )
    assert.deepEqual(
      report.windows.map((w) => w.shortLabel),
      ['Gemini 5h', 'Gemini wk', 'Claude / GPT 5h', 'Claude / GPT wk'],
    )
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
            { id: 'future', name: 'Future allowance', window: 'future' },
          ],
        },
      ]),
    )
    const clamped = await usage.fetch(account.input)
    assert.ok(clamped.status === 'available')
    assert.deepEqual(
      clamped.windows.map((w) => w.usedPct),
      [100, 0, null, null],
    )
    assert.deepEqual(
      clamped.windows.map((w) => w.id),
      ['extra:five_hour', 'extra:weekly', 'extra:daily', 'extra:future'],
    )
    assert.equal(clamped.windows[2].label, 'Extra · daily')
    assert.equal(clamped.windows[3].label, 'Extra · Future allowance')
    assert.equal(clamped.windows[3].shortLabel, 'Extra future')
    const checkTier = async (tier: string, expected: string) => {
      usage.tiers.set(dir, tier)
      const result = await usage.fetch(account.input)
      assert.ok(result.status === 'available')
      assert.equal(result.planLabel, expected)
    }
    await checkTier('Google AI Pro', 'Pro')
    await checkTier('Google AI Ultra', 'Ultra')
    await checkTier('Google AI Ultra Business', 'Google AI Ultra Business')
    await checkTier('Google AI Future', 'Google AI Future')
    await checkTier('Other Pro', 'Other Pro')
    usage.tiers.delete(dir)
    const withoutTier = await usage.fetch(account.input)
    assert.ok(withoutTier.status === 'available')
    assert.equal(withoutTier.planLabel, undefined)
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
    assert.deepEqual(account.input, { dataDir: dir })
    await assert.rejects(usage.fetch({ dataDir: '' }))
    usage.remember(launch, {}, home)
    await writeFile(fixture, envelope(groups))
    assert.equal(
      (
        await usage.fetch({
          dataDir: dir,
          command: '/nonexistent/legacy-binary',
          args: ['--legacy'],
        })
      ).status,
      'available',
    )
    await rm(join(dir, 'antigravity-oauth-token'))
    // The CLI stays authoritative: removing the legacy token file must not hide an
    // account that still answers, because keychain logins never write that file.
    assert.equal((await usage.fetch(account.input)).status, 'available')
    // A known launch can use an OS-backed login store instead of the token file.
    const fresh = new AgyUsageSource({ env: baseEnv })
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
  const usage = new AgyUsageSource({ env: isolatedEnv(home) })
  try {
    await mkdir(dir, { recursive: true })
    await writeFile(join(dir, 'antigravity-oauth-token'), 'fixture-token')
    await writeFile(fixture, envelope(groups))
    usage.remember({
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-usage.ts', import.meta.url)), 'acp'],
      env: isolatedEnv(home, {
        AGY_USAGE_FIXTURE: fixture,
        AGY_USAGE_ARGS: argsLog,
      }),
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

test('deleted workspaces fall back only when quota account paths are cwd independent', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-deleted-worktree-'))
  const workspace = join(root, 'worktree')
  const fixture = join(root, 'quota.json')
  const argsLog = join(root, 'args.json')
  try {
    await writeFile(fixture, envelope(groups))
    const fetchFromDeletedWorkspace = async (args: string[]) => {
      await mkdir(workspace, { recursive: true })
      const usage = new AgyUsageSource({ env: isolatedEnv(root) })
      const dir = usage.remember(
        {
          command: process.execPath,
          args: [
            fileURLToPath(new URL('./mock-usage.ts', import.meta.url)),
            'acp',
            ...args,
          ],
          env: isolatedEnv(root, {
            AGY_USAGE_FIXTURE: fixture,
            AGY_USAGE_ARGS: argsLog,
          }),
        },
        {},
        workspace,
      )
      await rm(workspace, { recursive: true, force: true })
      const accounts = await usage.discover({ kind: 'global' })
      assert.equal(accounts.length, 1)
      assert.equal(accounts[0].key, hashAccountKey(dir))
      return usage.fetch(accounts[0].input)
    }

    assert.equal((await fetchFromDeletedWorkspace([])).status, 'available')
    assert.equal(
      (await fetchFromDeletedWorkspace(['--gemini-dir', root])).status,
      'available',
    )
    const relative = await fetchFromDeletedWorkspace([
      '--gemini-dir',
      '.gemini',
    ])
    assert.ok(relative.status === 'error')
    assert.match(relative.error, /workspace is missing/)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('in-flight discover never overwrites or deletes a concurrent remember()', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-discover-race-'))
  const homeA = join(root, 'home-a')
  const homeB = join(root, 'home-b')
  const dirA = join(homeA, '.gemini/antigravity-cli')
  const dirB = join(homeB, '.gemini/antigravity-cli')
  const fixture = join(root, 'quota.json')
  const argsLog = join(root, 'args.json')
  const script = fileURLToPath(new URL('./mock-usage.ts', import.meta.url))
  const usage = new AgyUsageSource({ env: isolatedEnv(root) })
  try {
    await mkdir(dirA, { recursive: true })
    await mkdir(dirB, { recursive: true })
    await writeFile(fixture, envelope(groups))

    // Case A: logged-in session directory remembered while discover() awaits log I/O.
    await writeFile(
      join(dirA, 'cli.log'),
      'applyAuthResult: email=race-a@example.test\n',
    )
    const scopeA = {
      kind: 'session' as const,
      provider: 'agy',
      env: { HOME: homeA },
    }
    const launchA = {
      command: process.execPath,
      args: [script, 'acp', '--flag-a'],
      env: { HOME: homeA, AGY_USAGE_FIXTURE: fixture, AGY_USAGE_ARGS: argsLog },
    }
    const pendingA = usage.discover(scopeA)
    assert.equal(usage.remember(launchA, {}, homeA), dirA)
    const [acctA] = await pendingA
    assert.equal(acctA.key, hashAccountKey(dirA))
    assert.equal((await usage.fetch(acctA.input)).status, 'available')
    assert.deepEqual(JSON.parse(await readFile(argsLog, 'utf8')), [
      '--flag-a',
      '-p',
      '/usage',
      '--output-format',
      'json',
    ])
    await rm(join(dirA, 'cli.log'))
    assert.equal((await usage.discover(scopeA)).length, 1)

    // Case B: unremembered session entry upgraded via remember() after cli.log removal.
    await writeFile(
      join(dirB, 'cli.log'),
      'applyAuthResult: email=race-b@example.test\n',
    )
    const scopeB = {
      kind: 'session' as const,
      provider: 'agy',
      env: { HOME: homeB },
    }
    await usage.discover(scopeB)
    await rm(join(dirB, 'cli.log'))
    const launchB = {
      command: process.execPath,
      args: [script, 'acp', '--flag-b'],
      env: { HOME: homeB, AGY_USAGE_FIXTURE: fixture, AGY_USAGE_ARGS: argsLog },
    }
    const pendingB = usage.discover(scopeB)
    assert.equal(usage.remember(launchB, {}, homeB), dirB)
    const [acctB] = await pendingB
    assert.equal(acctB.key, hashAccountKey(dirB))
    assert.equal((await usage.fetch(acctB.input)).status, 'available')
    assert.deepEqual(JSON.parse(await readFile(argsLog, 'utf8')), [
      '--flag-b',
      '-p',
      '/usage',
      '--output-format',
      'json',
    ])
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})
