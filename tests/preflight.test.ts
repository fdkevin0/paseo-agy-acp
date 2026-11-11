import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  mkdtemp,
  mkdir,
  writeFile,
  readFile,
  access,
  symlink,
  rm,
} from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import {
  catalogCacheKey,
  dataDirectory,
  providerStatus,
  removeSessionArtifacts,
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

test('providerStatus accepts version >= 1.5.3 and caches result', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-version-ok-'))
  const fakeBin = join(tmp, 'fake-agy')
  await writeFile(fakeBin, '#!/bin/sh\necho "agy 1.5.3"\n', { mode: 0o755 })
  try {
    const status = await providerStatus({
      command: fakeBin,
      args: [],
      env: {},
    })
    assert.equal(status.available, true)
    assert.equal(status.diagnostic, undefined)

    const cachedStatus = await providerStatus({
      command: fakeBin,
      args: [],
      env: {},
    })
    assert.equal(cachedStatus.available, true)
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('providerStatus rejects version < 1.5.3', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-version-old-'))
  const fakeBin = join(tmp, 'fake-agy-old')
  await writeFile(fakeBin, '#!/bin/sh\necho "agy 1.5.0"\n', { mode: 0o755 })
  try {
    const status = await providerStatus({
      command: fakeBin,
      args: [],
      env: {},
    })
    assert.equal(status.available, false)
    assert.match(status.diagnostic ?? '', /too old; requires >=1.5.3/)
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

test('providerStatus rejects 1.5.3 pre-release builds and dataDirectory ignores flag-like missing values', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'agy-version-pre-'))
  const fakeBin = join(tmp, 'fake-agy-pre')
  await writeFile(fakeBin, '#!/bin/sh\necho "agy 1.5.3-alpha.1" >&2\n', {
    mode: 0o755,
  })
  try {
    const status = await providerStatus({
      command: fakeBin,
      args: [],
      env: {},
    })
    assert.equal(status.available, false)
    assert.match(status.diagnostic ?? '', /too old; requires >=1\.5\.3/)
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
