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
import {
  catalogCacheKey,
  dataDirectory,
  hasLocalSessionArtifacts,
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
    catalogCacheKey(launch, '1.5.11'),
    catalogCacheKey(launch, '1.5.12'),
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

test('hasLocalSessionArtifacts detects local db or brain dir and refuses invalid IDs and symlinks', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-artifacts-'))
  try {
    const data = join(root, 'app')
    const outside = join(root, 'outside')
    await mkdir(join(outside, 'brain', id), { recursive: true })
    await writeFile(join(outside, `${id}.db`), 'outside')

    assert.equal(await hasLocalSessionArtifacts(data, id), false)

    await mkdir(join(data, 'conversations'), { recursive: true })
    await mkdir(join(data, 'brain'), { recursive: true })
    assert.equal(await hasLocalSessionArtifacts(data, id), false)

    // Detects regular conversation .db file.
    await writeFile(join(data, 'conversations', `${id}.db`), 'db')
    assert.equal(await hasLocalSessionArtifacts(data, id), true)
    assert.equal(await hasLocalSessionArtifacts(data, 'not-a-uuid'), false)
    assert.equal(await hasLocalSessionArtifacts(data, '../escape'), false)
    await rm(join(data, 'conversations', `${id}.db`))
    assert.equal(await hasLocalSessionArtifacts(data, id), false)

    // Detects real brain/<id> directory.
    await mkdir(join(data, 'brain', id))
    assert.equal(await hasLocalSessionArtifacts(data, id), true)
    await rm(join(data, 'brain', id), { recursive: true })
    assert.equal(await hasLocalSessionArtifacts(data, id), false)

    // Rejects wrong entry types (directory at .db, regular file at brain/<id>).
    await mkdir(join(data, 'conversations', `${id}.db`))
    await writeFile(join(data, 'brain', id), 'file')
    assert.equal(await hasLocalSessionArtifacts(data, id), false)
    await rm(join(data, 'conversations', `${id}.db`), { recursive: true })
    await rm(join(data, 'brain', id))

    // Refuses symlinked leaf .db and symlinked leaf brain/<id>.
    await symlink(
      join(outside, `${id}.db`),
      join(data, 'conversations', `${id}.db`),
    )
    assert.equal(await hasLocalSessionArtifacts(data, id), false)
    await rm(join(data, 'conversations', `${id}.db`))

    await symlink(join(outside, 'brain', id), join(data, 'brain', id))
    assert.equal(await hasLocalSessionArtifacts(data, id), false)
    await rm(join(data, 'brain', id))

    // Refuses symlinked conversations/ and brain/ directories.
    await rm(join(data, 'conversations'), { recursive: true })
    await symlink(outside, join(data, 'conversations'))
    assert.equal(await hasLocalSessionArtifacts(data, id), false)
    await rm(join(data, 'conversations'))

    await rm(join(data, 'brain'), { recursive: true })
    await symlink(join(outside, 'brain'), join(data, 'brain'))
    assert.equal(await hasLocalSessionArtifacts(data, id), false)

    // Refuses symlinked appData root.
    const externalApp = join(root, 'external-app')
    await mkdir(join(externalApp, 'conversations'), { recursive: true })
    await writeFile(join(externalApp, 'conversations', `${id}.db`), 'db')
    const linkedApp = join(root, 'linked-app')
    await symlink(externalApp, linkedApp)
    assert.equal(await hasLocalSessionArtifacts(linkedApp, id), false)
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

test('providerStatus accepts version >= 1.5.10, coalesces concurrent probes, caches unchanged binaries, and invalidates on removal', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-version-ok-'))
  const fakeBin = join(tmp, 'fake-agy')
  const log = join(tmp, 'calls')
  const launch = { command: fakeBin, args: [], env: {} }
  await writeFile(
    fakeBin,
    `#!/bin/sh\necho call >> "${log}"\necho "agy 1.5.10"\n`,
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

test('providerStatus rejects version < 1.5.10', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-version-old-'))
  const fakeBin = join(tmp, 'fake-agy-old')
  await writeFile(fakeBin, '#!/bin/sh\necho "agy 1.5.9"\n', { mode: 0o755 })
  try {
    const status = await providerStatus({
      command: fakeBin,
      args: [],
      env: {},
    })
    assert.equal(status.available, false)
    assert.match(status.diagnostic ?? '', /too old; requires >=1\.5\.10/)
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

test('providerStatus rejects 1.5.10 pre-release builds and dataDirectory ignores flag-like missing values', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-version-pre-'))
  const fakeBin = join(tmp, 'fake-agy-pre')
  await writeFile(fakeBin, '#!/bin/sh\necho "agy 1.5.10-rc.1" >&2\n', {
    mode: 0o755,
  })
  try {
    const status = await providerStatus({
      command: fakeBin,
      args: [],
      env: {},
    })
    assert.equal(status.available, false)
    assert.match(status.diagnostic ?? '', /too old; requires >=1\.5\.10/)
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
  const v1 = join(tmp, 'agy-1.5.11')
  const v2 = join(tmp, 'agy-1.5.12')
  const binDir = join(tmp, 'bin')
  const dirShadow = join(tmp, 'shadow')
  const linked = join(binDir, 'agy')
  try {
    await mkdir(binDir)
    await mkdir(join(dirShadow, 'agy'), { recursive: true })
    await writeExecutable(v1, `#!/bin/sh\necho ran >> "${log}"\necho 1.5.11\n`)
    await writeExecutable(v2, `#!/bin/sh\necho ran >> "${log}"\necho 1.5.12\n`)
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
    await writeExecutable(v2, `#!/bin/sh\necho ran >> "${log}"\necho 1.5.13\n`)
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
    await writeExecutable(v2, `#!/bin/sh\necho ran >> "${log}"\necho 1.5.14\n`)
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
    await writeExecutable(fakeBin, '#!/bin/sh\necho "1.5.12"\n')
    assert.equal((await providerStatus(launch)).available, true)

    await writeExecutable(fakeBin, '#!/bin/sh\necho "1.5.9"\n')
    const downgraded = await providerStatus(launch)
    assert.equal(downgraded.available, false)
    assert.match(downgraded.diagnostic ?? '', /too old; requires >=1\.5\.10/)

    await writeExecutable(fakeBin, '#!/bin/sh\necho "1.5.12"\n')
    assert.equal((await providerStatus(launch)).available, true)
    await writeExecutable(fakeBin, '#!/bin/sh\necho "unparseable"\n')
    const unparseable = await providerStatus(launch)
    assert.equal(unparseable.available, false)
    assert.match(
      unparseable.diagnostic ?? '',
      /Could not determine agy version/,
    )

    await writeExecutable(fakeBin, '#!/bin/sh\necho "1.5.12"\n')
    assert.equal((await providerStatus(launch)).available, true)
    await writeExecutable(fakeBin, '#!/bin/sh\nexit 1\n')
    assert.equal((await providerStatus(launch)).available, false)

    await writeExecutable(fakeBin, '#!/bin/sh\necho "1.5.12"\n')
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
