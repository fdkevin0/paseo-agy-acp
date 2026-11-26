import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  mkdtemp,
  mkdir,
  writeFile,
  readFile,
  rename,
  chmod,
  access,
  symlink,
  rm,
} from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { mergeEnv } from '../server/common.ts'
import {
  binaryFingerprint,
  catalogCacheKey,
  dataDirectory,
  providerStatus,
  removeSessionArtifacts,
  resolveCatalogCacheKey,
  resolveSpawnCwd,
} from '../server/preflight.ts'

const id = '11111111-1111-4111-8111-111111111111'

test('catalog key is environment-order independent and includes launch differences', () => {
  const launch = { command: 'agy', args: ['acp'], env: { A: 'a', B: 'b' } }
  assert.equal(
    catalogCacheKey(launch),
    catalogCacheKey({ ...launch, env: { B: 'b', A: 'a' } }),
  )
  assert.notEqual(
    catalogCacheKey(launch),
    catalogCacheKey({ ...launch, args: ['acp', '--sandbox'] }),
  )
  assert.notEqual(
    catalogCacheKey(launch),
    catalogCacheKey({ ...launch, env: { A: 'changed', B: 'b' } }),
  )
  assert.notEqual(
    catalogCacheKey(launch, '1.5.16'),
    catalogCacheKey(launch, '1.5.17'),
  )
  assert.equal(
    dataDirectory({
      ...launch,
      args: [
        'acp',
        '--gemini-dir=/tmp/test-gemini',
        '--app-data-dir',
        'custom',
      ],
    }),
    '/tmp/test-gemini/custom',
  )
})

test('catalog cleanup removes only its closed session and refuses symlinks', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-cleanup-'))
  try {
    const data = join(root, 'app')
    await mkdir(join(data, 'conversations'), { recursive: true })
    await mkdir(join(data, 'brain', id), { recursive: true })
    await writeFile(join(data, 'conversations', `${id}.db`), 'temporary')
    await writeFile(join(data, 'conversations', 'keep.db'), 'keep')
    await removeSessionArtifacts(data, id)
    await assert.rejects(access(join(data, 'brain', id)))
    await assert.rejects(access(join(data, 'conversations', `${id}.db`)))
    assert.equal(
      await readFile(join(data, 'conversations', 'keep.db'), 'utf8'),
      'keep',
    )
    await assert.rejects(removeSessionArtifacts(data, '../escape'), /Unsafe/)
    await symlink(
      join(data, 'conversations', 'keep.db'),
      join(data, 'conversations', `${id}.db`),
    )
    await assert.rejects(removeSessionArtifacts(data, id), /symlink/)
    await rm(join(data, 'conversations', `${id}.db`))
    const outside = join(root, 'outside')
    await mkdir(outside)
    await writeFile(join(outside, `${id}.db`), 'outside')
    await rm(join(data, 'conversations'), { recursive: true })
    await symlink(outside, join(data, 'conversations'))
    await removeSessionArtifacts(data, id)
    assert.equal(await readFile(join(outside, `${id}.db`), 'utf8'), 'outside')

    // Symlinked appData root itself must never be traversed into external directories.
    const externalApp = join(root, 'external-app')
    await mkdir(join(externalApp, 'conversations'), { recursive: true })
    await writeFile(
      join(externalApp, 'conversations', `${id}.db`),
      'keep-external',
    )
    const linkedApp = join(root, 'linked-app')
    await symlink(externalApp, linkedApp)
    await removeSessionArtifacts(linkedApp, id)
    assert.equal(
      await readFile(join(externalApp, 'conversations', `${id}.db`), 'utf8'),
      'keep-external',
    )
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('status reports missing executable clearly', async () => {
  const status = await providerStatus({
    command: '/nonexistent/agy-test',
    args: [],
    env: {},
  })
  assert.equal(status.available, false)
  assert.match(status.diagnostic ?? '', /Cannot run agy/)
})

test('providerStatus accepts version >= 1.5.16, coalesces concurrent probes, caches unchanged binaries, and invalidates on removal', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-version-ok-'))
  const fakeBin = join(tmp, 'fake-agy')
  const log = join(tmp, 'calls')
  const launch = { command: fakeBin, args: [], env: {} }
  await writeFile(
    fakeBin,
    `#!/bin/sh\necho call >> "${log}"\necho "agy 1.5.16"\n`,
    { mode: 0o755 },
  )
  try {
    const [first, second] = await Promise.all([
      providerStatus(launch),
      providerStatus(launch),
    ])
    assert.equal(first.available, true)
    assert.equal(first.diagnostic, undefined)
    assert.equal(second.available, true)
    assert.equal(await readFile(log, 'utf8'), 'call\n')

    const cachedStatus = await providerStatus(launch)
    assert.equal(cachedStatus.available, true)
    assert.equal(await readFile(log, 'utf8'), 'call\n')

    await rm(fakeBin)

    const removed = await providerStatus(launch)
    assert.equal(removed.available, false)
    assert.match(removed.diagnostic ?? '', /Cannot run agy/)
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('providerStatus rejects version < 1.5.16', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-version-old-'))
  const fakeBin = join(tmp, 'fake-agy-old')
  await writeFile(fakeBin, '#!/bin/sh\necho "agy 1.5.15"\n', { mode: 0o755 })
  try {
    const status = await providerStatus({
      command: fakeBin,
      args: [],
      env: {},
    })
    assert.equal(status.available, false)
    assert.match(status.diagnostic ?? '', /too old; requires >=1\.5\.16/)
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('providerStatus reports diagnostic when version cannot be parsed', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-version-unparseable-'))
  const fakeBin = join(tmp, 'fake-agy-bad')
  await writeFile(fakeBin, '#!/bin/sh\necho "hello world"\n', { mode: 0o755 })
  try {
    const status = await providerStatus({
      command: fakeBin,
      args: [],
      env: {},
    })
    assert.equal(status.available, false)
    assert.match(status.diagnostic ?? '', /Could not determine agy version/)
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('providerStatus rejects 1.5.16 pre-release builds and dataDirectory ignores flag-like missing values', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-version-pre-'))
  const fakeBin = join(tmp, 'fake-agy-pre')
  await writeFile(fakeBin, '#!/bin/sh\necho "agy 1.5.16-rc.1" >&2\n', {
    mode: 0o755,
  })
  try {
    const status = await providerStatus({
      command: fakeBin,
      args: [],
      env: {},
    })
    assert.equal(status.available, false)
    assert.match(status.diagnostic ?? '', /too old; requires >=1\.5\.16/)
    assert.equal(
      dataDirectory(
        {
          command: 'agy',
          args: ['acp', '--gemini-dir', '--stdio'],
          env: { HOME: '/tmp/home-test' },
        },
        {},
        '/tmp',
      ),
      '/tmp/home-test/.gemini/antigravity-cli',
    )
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

async function writeExecutable(path: string, script: string): Promise<void> {
  const staged = `${path}.next`
  await writeFile(staged, script, { mode: 0o755 })
  await rename(staged, path)
}

test('resolveCatalogCacheKey fingerprints the executable without spawning it and tracks upgrades via path or symlink', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-catalog-key-'))
  const log = join(tmp, 'spawns')
  const v1 = join(tmp, 'agy-1.5.16')
  const v2 = join(tmp, 'agy-1.5.17')
  const binDir = join(tmp, 'bin')
  const dirShadow = join(tmp, 'shadow')
  const linked = join(binDir, 'agy')
  try {
    await mkdir(binDir)
    await mkdir(join(dirShadow, 'agy'), { recursive: true })
    await writeExecutable(v1, `#!/bin/sh\necho ran >> "${log}"\necho 1.5.16\n`)
    await writeExecutable(v2, `#!/bin/sh\necho ran >> "${log}"\necho 1.5.17\n`)
    await symlink(v1, linked)

    const launch = { command: linked, args: ['acp'], env: {} }
    const key1 = await resolveCatalogCacheKey(launch)
    assert.notEqual(key1, catalogCacheKey(launch))
    await assert.rejects(access(log))

    // Repointing a Homebrew-style symlink changes the fingerprint immediately.
    await rm(linked)
    await symlink(v2, linked)
    const key2 = await resolveCatalogCacheKey(launch)
    assert.notEqual(key2, key1)
    assert.notEqual(key2, catalogCacheKey(launch))

    // Replacing the target binary atomically changes the fingerprint immediately.
    await writeExecutable(v2, `#!/bin/sh\necho ran >> "${log}"\necho 1.5.18\n`)
    const key3 = await resolveCatalogCacheKey(launch)
    assert.notEqual(key3, key2)
    await assert.rejects(access(log))

    // Bare command lookup resolves through launch.env.PATH and skips directories.
    const pathLaunch = {
      command: 'agy',
      args: ['acp'],
      env: { PATH: `${dirShadow}:${binDir}` },
    }
    const pathKey1 = await resolveCatalogCacheKey(pathLaunch)
    assert.notEqual(pathKey1, catalogCacheKey(pathLaunch))
    await writeExecutable(v2, `#!/bin/sh\necho ran >> "${log}"\necho 1.5.19\n`)
    assert.notEqual(await resolveCatalogCacheKey(pathLaunch), pathKey1)

    // Missing or non-executable binaries fall back to the base cache key.
    await chmod(v2, 0o644)
    assert.equal(await resolveCatalogCacheKey(launch), catalogCacheKey(launch))

    const fallbackLaunch = {
      command: '/nonexistent/agy-catalog-key',
      args: ['acp'],
      env: {},
    }
    assert.equal(
      await resolveCatalogCacheKey(fallbackLaunch),
      catalogCacheKey(fallbackLaunch),
    )
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('providerStatus invalidates cached availability immediately on binary downgrade, corruption, chmod, or removal', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-status-invalidate-'))
  const fakeBin = join(tmp, 'fake-agy')
  const launch = { command: fakeBin, args: ['acp'], env: {} }
  try {
    await writeExecutable(fakeBin, '#!/bin/sh\necho "1.5.16"\n')
    assert.equal((await providerStatus(launch)).available, true)

    await writeExecutable(fakeBin, '#!/bin/sh\necho "1.5.15"\n')
    const downgraded = await providerStatus(launch)
    assert.equal(downgraded.available, false)
    assert.match(downgraded.diagnostic ?? '', /too old; requires >=1\.5\.16/)

    await writeExecutable(fakeBin, '#!/bin/sh\necho "1.5.16"\n')
    assert.equal((await providerStatus(launch)).available, true)
    await writeExecutable(fakeBin, '#!/bin/sh\necho "unparseable"\n')
    const unparseable = await providerStatus(launch)
    assert.equal(unparseable.available, false)
    assert.match(
      unparseable.diagnostic ?? '',
      /Could not determine agy version/,
    )

    await writeExecutable(fakeBin, '#!/bin/sh\necho "1.5.16"\n')
    assert.equal((await providerStatus(launch)).available, true)
    await writeExecutable(fakeBin, '#!/bin/sh\nexit 1\n')
    assert.equal((await providerStatus(launch)).available, false)

    await writeExecutable(fakeBin, '#!/bin/sh\necho "1.5.16"\n')
    assert.equal((await providerStatus(launch)).available, true)
    await chmod(fakeBin, 0o644)
    assert.equal((await providerStatus(launch)).available, false)
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('resolveSpawnCwd falls back only when allowed and launch paths are cwd-independent', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-spawn-cwd-'))
  const existing = join(tmp, 'workspace')
  const missing = join(tmp, 'deleted-worktree')
  await mkdir(existing)
  try {
    assert.equal(
      await resolveSpawnCwd(
        existing,
        {
          command: './agy',
          args: ['acp', '--gemini-dir', './custom'],
          env: { HOME: tmp },
        },
        { allowMissingCwd: true },
      ),
      existing,
    )
    assert.equal(
      await resolveSpawnCwd(
        missing,
        {
          command: 'agy',
          args: ['acp'],
          env: { HOME: tmp },
        },
        { allowMissingCwd: true },
      ),
      tmp,
    )
    await assert.rejects(
      resolveSpawnCwd(missing, {
        command: 'agy',
        args: ['acp'],
        env: { HOME: tmp },
      }),
      /Working directory does not exist/,
    )
    await assert.rejects(
      resolveSpawnCwd(
        missing,
        {
          command: './agy',
          args: ['acp'],
          env: { HOME: tmp },
        },
        { allowMissingCwd: true },
      ),
      /Working directory does not exist/,
    )
    await assert.rejects(
      resolveSpawnCwd(
        missing,
        {
          command: 'agy',
          args: ['acp', '--gemini-dir', './custom'],
          env: { HOME: tmp },
        },
        { allowMissingCwd: true },
      ),
      /Working directory does not exist/,
    )
    await assert.rejects(
      resolveSpawnCwd(
        missing,
        {
          command: 'agy',
          args: ['acp'],
          env: { HOME: join(tmp, 'missing-home') },
        },
        { allowMissingCwd: true },
      ),
      /Working directory does not exist/,
    )
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('resolveCatalogCacheKey handles case-insensitive Path, quoted entries, Windows PATHEXT, and default install fallbacks', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-cross-platform-key-'))
  try {
    // POSIX: case-insensitive Path key and quoted directory with spaces.
    const quotedBin = join(tmp, 'my bin')
    await mkdir(quotedBin, { recursive: true })
    const posixAgy = join(quotedBin, 'agy')
    await writeExecutable(posixAgy, '#!/bin/sh\necho 1.5.16\n')
    const quotedLaunch = {
      command: 'agy',
      args: ['acp'],
      env: { Path: `"${quotedBin}"`, HOME: join(tmp, 'empty-home') },
    }
    const quotedKey1 = await resolveCatalogCacheKey(quotedLaunch, {
      platform: 'linux',
      fallbackHome: join(tmp, 'empty-home'),
    })
    assert.notEqual(quotedKey1, catalogCacheKey(quotedLaunch))
    await writeExecutable(posixAgy, '#!/bin/sh\necho 1.5.17\n')
    assert.notEqual(
      await resolveCatalogCacheKey(quotedLaunch, {
        platform: 'linux',
        fallbackHome: join(tmp, 'empty-home'),
      }),
      quotedKey1,
    )

    // POSIX default install fallback (~/.local/bin/agy) when PATH misses for bare 'agy'.
    const posixHome = join(tmp, 'posix-home')
    const posixDefaultBin = join(posixHome, '.local', 'bin')
    await mkdir(posixDefaultBin, { recursive: true })
    const posixDefaultAgy = join(posixDefaultBin, 'agy')
    await writeExecutable(posixDefaultAgy, '#!/bin/sh\necho 1.5.16\n')
    const posixFallbackLaunch = {
      command: 'agy',
      args: ['acp'],
      env: { PATH: join(tmp, 'missing-dir'), HOME: posixHome },
    }
    const posixFallbackKey1 = await resolveCatalogCacheKey(
      posixFallbackLaunch,
      { platform: 'linux', fallbackHome: join(tmp, 'unused-home') },
    )
    assert.notEqual(posixFallbackKey1, catalogCacheKey(posixFallbackLaunch))
    await writeExecutable(posixDefaultAgy, '#!/bin/sh\necho 1.5.18\n')
    assert.notEqual(
      await resolveCatalogCacheKey(posixFallbackLaunch, {
        platform: 'linux',
        fallbackHome: join(tmp, 'unused-home'),
      }),
      posixFallbackKey1,
    )

    // Non-default bare commands never fall back to ~/.local/bin/agy.
    const customCmdLaunch = {
      command: 'custom-agy',
      args: ['acp'],
      env: { PATH: join(tmp, 'missing-dir'), HOME: posixHome },
    }
    assert.equal(
      await resolveCatalogCacheKey(customCmdLaunch, {
        platform: 'linux',
        fallbackHome: posixHome,
      }),
      catalogCacheKey(customCmdLaunch),
    )

    // Non-executable POSIX default install is ignored on POSIX.
    await chmod(posixDefaultAgy, 0o644)
    assert.equal(
      await resolveCatalogCacheKey(posixFallbackLaunch, {
        platform: 'linux',
        fallbackHome: posixHome,
      }),
      catalogCacheKey(posixFallbackLaunch),
    )

    // Windows: case-insensitive Path, semicolon delimiter, quoted dir, .exe via PATHEXT,
    // skips directory shadows, and uses stat().isFile() instead of POSIX X_OK.
    const winShadow = join(tmp, 'win-shadow')
    const winBin = join(tmp, 'win bin')
    await mkdir(join(winShadow, 'agy.exe'), { recursive: true })
    await mkdir(winBin, { recursive: true })
    const winExe = join(winBin, 'agy.exe')
    await writeFile(winExe, 'MZ-v1', { mode: 0o644 })
    const winPathLaunch = {
      command: 'agy',
      args: ['acp'],
      env: {
        Path: `${winShadow}; "${winBin}" ; ""`,
        PathExt: '.COM;.EXE;.BAT;.CMD',
        USERPROFILE: join(tmp, 'win-empty-home'),
      },
    }
    const winKey1 = await resolveCatalogCacheKey(winPathLaunch, {
      platform: 'win32',
      fallbackHome: join(tmp, 'win-empty-home'),
    })
    assert.notEqual(winKey1, catalogCacheKey(winPathLaunch))
    await writeFile(winExe, 'MZ-v2-longer', { mode: 0o644 })
    assert.notEqual(
      await resolveCatalogCacheKey(winPathLaunch, {
        platform: 'win32',
        fallbackHome: join(tmp, 'win-empty-home'),
      }),
      winKey1,
    )

    // Windows default install fallback: %LOCALAPPDATA%\agy\bin\agy.exe.
    const winLocalAppData = join(tmp, 'win-local-appdata')
    await mkdir(join(winLocalAppData, 'agy', 'bin'), { recursive: true })
    const winLocalExe = join(winLocalAppData, 'agy', 'bin', 'agy.exe')
    await writeFile(winLocalExe, 'MZ-local-1', { mode: 0o644 })
    const winLocalLaunch = {
      command: 'agy',
      args: ['acp'],
      env: {
        Path: join(tmp, 'missing-win-dir'),
        LocalAppData: winLocalAppData,
        USERPROFILE: join(tmp, 'win-empty-home'),
      },
    }
    const winLocalKey1 = await resolveCatalogCacheKey(winLocalLaunch, {
      platform: 'win32',
      fallbackHome: join(tmp, 'win-empty-home'),
    })
    assert.notEqual(winLocalKey1, catalogCacheKey(winLocalLaunch))
    await writeFile(winLocalExe, 'MZ-local-2-updated', { mode: 0o644 })
    assert.notEqual(
      await resolveCatalogCacheKey(winLocalLaunch, {
        platform: 'win32',
        fallbackHome: join(tmp, 'win-empty-home'),
      }),
      winLocalKey1,
    )

    // Windows secondary default install fallback: ~/AppData/Local/agy/bin/agy.exe.
    const winProfile = join(tmp, 'win-profile')
    await mkdir(join(winProfile, 'AppData', 'Local', 'agy', 'bin'), {
      recursive: true,
    })
    const winProfileExe = join(
      winProfile,
      'AppData',
      'Local',
      'agy',
      'bin',
      'agy.exe',
    )
    await writeFile(winProfileExe, 'MZ-profile-1', { mode: 0o644 })
    const winProfileLaunch = {
      command: 'agy',
      args: ['acp'],
      env: {
        Path: join(tmp, 'missing-win-dir'),
        USERPROFILE: winProfile,
      },
    }
    assert.notEqual(
      await resolveCatalogCacheKey(winProfileLaunch, {
        platform: 'win32',
        fallbackHome: join(tmp, 'win-empty-home'),
      }),
      catalogCacheKey(winProfileLaunch),
    )

    // Windows default install fallback also applies when launch.command is bare 'agy.exe'.
    const winExeFallbackLaunch = {
      command: 'agy.exe',
      args: ['acp'],
      env: {
        Path: join(tmp, 'missing-win-dir'),
        LOCALAPPDATA: winLocalAppData,
        USERPROFILE: join(tmp, 'win-empty-home'),
      },
    }
    assert.notEqual(
      await resolveCatalogCacheKey(winExeFallbackLaunch, {
        platform: 'win32',
        fallbackHome: join(tmp, 'win-empty-home'),
      }),
      catalogCacheKey(winExeFallbackLaunch),
    )

    // Windows non-default bare command does not fall back to default agy install paths.
    const winCustomLaunch = {
      command: 'other-agy',
      args: ['acp'],
      env: {
        Path: join(tmp, 'missing-win-dir'),
        LOCALAPPDATA: winLocalAppData,
        USERPROFILE: winProfile,
      },
    }
    assert.equal(
      await resolveCatalogCacheKey(winCustomLaunch, {
        platform: 'win32',
        fallbackHome: winProfile,
      }),
      catalogCacheKey(winCustomLaunch),
    )
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('dataDirectory and resolveSpawnCwd fall back through HOME, USERPROFILE, launch.env and fallbackHome while ignoring empty strings', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-home-fallback-'))
  try {
    const envHome = join(tmp, 'env-home')
    const envProfile = join(tmp, 'env-profile')
    const launchHome = join(tmp, 'launch-home')
    const launchProfile = join(tmp, 'launch-profile')
    const fallbackHome = join(tmp, 'fallback-home')
    await mkdir(envProfile)
    await mkdir(launchProfile)

    const launch = {
      command: 'agy',
      args: ['acp'],
      env: { HOME: launchHome, USERPROFILE: launchProfile },
    }

    assert.equal(
      dataDirectory(
        launch,
        { HOME: envHome, USERPROFILE: envProfile },
        tmp,
        fallbackHome,
      ),
      join(envHome, '.gemini', 'antigravity-cli'),
    )
    assert.equal(
      dataDirectory(
        launch,
        { HOME: '', USERPROFILE: envProfile },
        tmp,
        fallbackHome,
      ),
      join(envProfile, '.gemini', 'antigravity-cli'),
    )
    assert.equal(
      dataDirectory(launch, { HOME: '', USERPROFILE: '' }, tmp, fallbackHome),
      join(launchHome, '.gemini', 'antigravity-cli'),
    )
    assert.equal(
      dataDirectory(
        {
          command: 'agy',
          args: ['acp'],
          env: { HOME: '', USERPROFILE: launchProfile },
        },
        { HOME: '', USERPROFILE: '' },
        tmp,
        fallbackHome,
      ),
      join(launchProfile, '.gemini', 'antigravity-cli'),
    )
    assert.equal(
      dataDirectory(
        { command: 'agy', args: ['acp'], env: { HOME: '', USERPROFILE: '' } },
        { HOME: '', USERPROFILE: '' },
        tmp,
        fallbackHome,
      ),
      join(fallbackHome, '.gemini', 'antigravity-cli'),
    )
    assert.equal(
      await resolveSpawnCwd(
        join(tmp, 'deleted-cwd'),
        {
          command: 'agy',
          args: ['acp'],
          env: { HOME: '', USERPROFILE: launchProfile },
        },
        {
          env: { HOME: '', USERPROFILE: envProfile },
          fallbackHome,
          allowMissingCwd: true,
        },
      ),
      envProfile,
    )
    assert.equal(
      await resolveSpawnCwd(
        join(tmp, 'deleted-cwd'),
        {
          command: 'agy',
          args: ['acp'],
          env: { HOME: '', USERPROFILE: launchProfile },
        },
        {
          env: { HOME: '', USERPROFILE: '' },
          fallbackHome,
          allowMissingCwd: true,
        },
      ),
      launchProfile,
    )
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('mergeEnv deduplicates case-variant keys when caseInsensitive is true and preserves them when false', () => {
  assert.deepEqual(
    mergeEnv(
      { Path: 'C:\\old', KEEP: '1', DROP: undefined },
      { PATH: 'C:\\new', IGNORED: undefined },
      true,
    ),
    { KEEP: '1', PATH: 'C:\\new' },
  )
  assert.deepEqual(
    mergeEnv(
      { Path: '/old', KEEP: '1' },
      { PATH: '/new', IGNORED: undefined },
      false,
    ),
    { Path: '/old', KEEP: '1', PATH: '/new' },
  )
})

test('binaryFingerprint recognizes uppercase AGY/AGY.EXE on win32 and honors options.env PATH overrides', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-fingerprint-env-'))
  try {
    const emptyHome = join(tmp, 'empty-home')
    const missingDir = join(tmp, 'missing-dir')
    const winLocalAppData = join(tmp, 'win-local-appdata')
    await mkdir(join(winLocalAppData, 'agy', 'bin'), { recursive: true })
    const winLocalExe = join(winLocalAppData, 'agy', 'bin', 'agy.exe')
    await writeFile(winLocalExe, 'MZ-default-agy', { mode: 0o644 })

    const baseWinEnv = {
      Path: missingDir,
      LOCALAPPDATA: winLocalAppData,
      USERPROFILE: emptyHome,
    }
    const lowerFp = await binaryFingerprint(
      { command: 'agy', args: ['acp'], env: baseWinEnv },
      { platform: 'win32', fallbackHome: emptyHome },
    )
    const upperCmdFp = await binaryFingerprint(
      { command: 'AGY', args: ['acp'], env: baseWinEnv },
      { platform: 'win32', fallbackHome: emptyHome },
    )
    const upperExeFp = await binaryFingerprint(
      { command: 'AGY.EXE', args: ['acp'], env: baseWinEnv },
      { platform: 'win32', fallbackHome: emptyHome },
    )
    assert.ok(lowerFp)
    assert.equal(upperCmdFp, lowerFp)
    assert.equal(upperExeFp, lowerFp)

    // POSIX options.env PATH overrides launch.env PATH.
    const binA = join(tmp, 'bin-a')
    const binB = join(tmp, 'bin-b')
    await mkdir(binA)
    await mkdir(binB)
    const agyA = join(binA, 'agy')
    const agyB = join(binB, 'agy')
    await writeExecutable(agyA, '#!/bin/sh\necho 1.5.16\n')
    await writeExecutable(agyB, '#!/bin/sh\necho 1.5.17-override\n')

    const posixLaunch = {
      command: 'agy',
      args: ['acp'],
      env: { PATH: binA, HOME: emptyHome },
    }
    const fpFromLaunch = await binaryFingerprint(posixLaunch, {
      platform: 'linux',
      fallbackHome: emptyHome,
    })
    const fpFromOverride = await binaryFingerprint(posixLaunch, {
      platform: 'linux',
      fallbackHome: emptyHome,
      env: { PATH: binB },
    })
    const fpDirectB = await binaryFingerprint(
      { command: agyB, args: ['acp'], env: {} },
      { platform: 'linux', fallbackHome: emptyHome },
    )
    assert.ok(fpFromLaunch)
    assert.ok(fpFromOverride)
    assert.notEqual(fpFromOverride, fpFromLaunch)
    assert.equal(fpFromOverride, fpDirectB)

    // Windows options.env PATH overrides case-variant launch.env Path.
    const winBinA = join(tmp, 'win-bin-a')
    const winBinB = join(tmp, 'win-bin-b')
    await mkdir(winBinA)
    await mkdir(winBinB)
    const winA = join(winBinA, 'agy.exe')
    const winB = join(winBinB, 'agy.exe')
    await writeFile(winA, 'MZ-a', { mode: 0o644 })
    await writeFile(winB, 'MZ-b-override', { mode: 0o644 })

    const winLaunch = {
      command: 'agy',
      args: ['acp'],
      env: { Path: winBinA, USERPROFILE: emptyHome },
    }
    const winFpFromLaunch = await binaryFingerprint(winLaunch, {
      platform: 'win32',
      fallbackHome: emptyHome,
    })
    const winFpFromOverride = await binaryFingerprint(winLaunch, {
      platform: 'win32',
      fallbackHome: emptyHome,
      env: { PATH: winBinB },
    })
    const winFpDirectB = await binaryFingerprint(
      { command: winB, args: ['acp'], env: {} },
      { platform: 'win32', fallbackHome: emptyHome },
    )
    assert.ok(winFpFromLaunch)
    assert.ok(winFpFromOverride)
    assert.notEqual(winFpFromOverride, winFpFromLaunch)
    assert.equal(winFpFromOverride, winFpDirectB)
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})
