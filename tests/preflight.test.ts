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
