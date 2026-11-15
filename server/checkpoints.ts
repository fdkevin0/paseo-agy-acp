/** Per-turn workspace file checkpoints using isolated shadow Git trees. */
import { execFile } from 'node:child_process'
import { createHash, randomUUID } from 'node:crypto'
import {
  chmod,
  copyFile,
  lstat,
  mkdir,
  readFile,
  readdir,
  realpath,
  rename,
  rm,
  stat,
  symlink,
  utimes,
  writeFile,
} from 'node:fs/promises'
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path'
import { promisify } from 'node:util'
import { z } from 'zod'
import {
  assertSafeNativeId,
  isMissingError,
  record,
  sessionIdPattern,
  str,
} from './common.ts'

const execFileAsync = promisify(execFile)
const GIT_TIMEOUT_MS = 5000
const GIT_DIFF_MAX_BUFFER = 32 * 1024 * 1024

function gitEnv(extra?: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  return {
    ...process.env,
    ...extra,
    LC_ALL: 'C',
    LANG: 'C',
    LANGUAGE: 'C',
  }
}

const TurnFileCheckpointSchema = z.object({
  id: z.string(),
  afterStepIndex: z.number().int().min(-1),
  kind: z.literal('git'),
  repoRoot: z.string(),
  treeHash: z.string().regex(/^[a-f0-9]{40,64}$/i),
  unbornDirs: z.array(z.string()).optional(),
  createdAt: z.string(),
})

export type TurnFileCheckpoint = z.infer<typeof TurnFileCheckpointSchema>

const SessionCheckpointsFileSchema = z.object({
  version: z.literal(1),
  checkpoints: z.array(TurnFileCheckpointSchema),
})

const sessionWriteLanes = new Map<string, Promise<void>>()

function queueSessionOp<T>(key: string, fn: () => Promise<T>): Promise<T> {
  const prev = sessionWriteLanes.get(key) ?? Promise.resolve()
  const next = prev.catch(() => undefined).then(fn)
  const settled = next.then(
    () => undefined,
    () => undefined,
  )
  sessionWriteLanes.set(key, settled)
  void settled.finally(() => {
    if (sessionWriteLanes.get(key) === settled) sessionWriteLanes.delete(key)
  })
  return next
}

async function removeExistingEntry(
  targetPath: string,
  statInfo: Awaited<ReturnType<typeof lstat>> | null,
): Promise<void> {
  if (!statInfo) return
  await rm(targetPath, {
    recursive: statInfo.isDirectory() && !statInfo.isSymbolicLink(),
    force: true,
  })
}

function checkpointsRoot(appDataDir: string): string {
  return join(appDataDir, 'paseo-checkpoints')
}

function sessionCheckpointsPath(appDataDir: string, nativeId: string): string {
  assertSafeNativeId(nativeId)
  return join(checkpointsRoot(appDataDir), 'sessions', `${nativeId}.json`)
}

function shadowGitDirForRepo(appDataDir: string, repoRoot: string): string {
  const hash = createHash('sha256').update(repoRoot).digest('hex').slice(0, 16)
  return join(checkpointsRoot(appDataDir), 'git', `${hash}.git`)
}

function resolveContainedPath(root: string, relOrAbs: string): string | null {
  const resolvedRoot = resolve(root)
  const candidate = isAbsolute(relOrAbs)
    ? resolve(relOrAbs)
    : resolve(resolvedRoot, relOrAbs)
  if (candidate === resolvedRoot) return null
  if (!candidate.startsWith(resolvedRoot + sep)) return null
  return candidate
}

async function assertNoSymlinkEscape(
  root: string,
  targetPath: string,
): Promise<boolean> {
  const rel = relative(root, targetPath)
  if (rel === '..' || rel.startsWith(`..${sep}`) || isAbsolute(rel))
    return false
  if (!rel) return true
  const parts = rel.split(sep).filter(Boolean)
  let current = root
  for (let i = 0; i < parts.length; i++) {
    current = join(current, parts[i])
    try {
      const info = await lstat(current)
      if (info.isSymbolicLink()) return false
      if (i < parts.length - 1 && !info.isDirectory()) return false
    } catch (error) {
      if (isMissingError(error)) return true
      return false
    }
  }
  return true
}

async function pruneEmptyParents(
  startDir: string,
  stopRoot: string,
): Promise<void> {
  const resolvedStop = resolve(stopRoot)
  let current = resolve(startDir)
  while (current !== resolvedStop && current.startsWith(resolvedStop + sep)) {
    const entries = await readdir(current).catch(() => null)
    if (!entries || entries.length > 0) break
    await rm(current, { recursive: false, force: true }).catch(() => undefined)
    current = dirname(current)
  }
}

async function writeAtomicJson(
  filePath: string,
  payload: unknown,
): Promise<void> {
  await mkdir(dirname(filePath), { recursive: true })
  const tmpPath = `${filePath}.tmp-${randomUUID()}`
  await writeFile(tmpPath, JSON.stringify(payload), 'utf8')
  await rename(tmpPath, filePath)
}

/** Read persisted turn file checkpoints for `nativeId`. */
export async function readSessionCheckpoints(
  appDataDir: string,
  nativeId: string,
): Promise<TurnFileCheckpoint[]> {
  if (!sessionIdPattern.test(nativeId)) return []
  const filePath = sessionCheckpointsPath(appDataDir, nativeId)
  try {
    const info = await lstat(filePath)
    if (info.isSymbolicLink()) return []
    const parsed = SessionCheckpointsFileSchema.safeParse(
      JSON.parse(await readFile(filePath, 'utf8')),
    )
    return parsed.success ? parsed.data.checkpoints : []
  } catch (error) {
    if (isMissingError(error)) return []
    throw error
  }
}

async function writeSessionCheckpoints(
  appDataDir: string,
  nativeId: string,
  checkpoints: readonly TurnFileCheckpoint[],
): Promise<void> {
  const filePath = sessionCheckpointsPath(appDataDir, nativeId)
  await writeAtomicJson(filePath, { version: 1, checkpoints })
}

async function resolveGitRepo(
  cwd: string,
): Promise<{ repoRoot: string; gitDir: string; commonDir: string } | null> {
  try {
    const env = gitEnv()
    const { stdout: topOut } = await execFileAsync(
      'git',
      ['-C', cwd, 'rev-parse', '--show-toplevel'],
      { encoding: 'utf8', env, timeout: 2000 },
    )
    const rawRoot = topOut.trim()
    if (!rawRoot) return null
    const repoRoot = await realpath(rawRoot)
    const [{ stdout: dirOut }, { stdout: commonOut }] = await Promise.all([
      execFileAsync(
        'git',
        ['-C', repoRoot, 'rev-parse', '--absolute-git-dir'],
        { encoding: 'utf8', env, timeout: 2000 },
      ),
      execFileAsync('git', ['-C', repoRoot, 'rev-parse', '--git-common-dir'], {
        encoding: 'utf8',
        env,
        timeout: 2000,
      }),
    ])
    const gitDir = dirOut.trim()
    const rawCommon = commonOut.trim()
    if (!gitDir || !rawCommon) return null
    const commonDir = await realpath(resolve(repoRoot, rawCommon)).catch(() =>
      resolve(repoRoot, rawCommon),
    )
    return { repoRoot, gitDir, commonDir }
  } catch {
    return null
  }
}

/** Whether `cwd` is inside a Git working tree eligible for shadow Git file checkpoints. */
export async function isGitWorkspace(cwd: string): Promise<boolean> {
  return Boolean(await resolveGitRepo(cwd))
}

async function ensureShadowGitRepo(
  appDataDir: string,
  repoRoot: string,
  realGitDir: string,
  commonGitDir: string,
): Promise<string> {
  const shadowDir = shadowGitDirForRepo(appDataDir, repoRoot)
  return await queueSessionOp(`shadow:${shadowDir}`, async () => {
    const env = gitEnv()
    try {
      await lstat(join(shadowDir, 'HEAD'))
    } catch (error) {
      if (!isMissingError(error)) throw error
      await mkdir(shadowDir, { recursive: true })
      await execFileAsync('git', ['init', '--bare', shadowDir], {
        encoding: 'utf8',
        env,
        timeout: GIT_TIMEOUT_MS,
      })
    }
    // Point alternates at the real repo's object store (read-only) so tracked unchanged blobs aren't re-hashed.
    const realObjectsDir = await realpath(join(commonGitDir, 'objects')).catch(
      () => join(commonGitDir, 'objects'),
    )
    const alternatesPath = join(shadowDir, 'objects', 'info', 'alternates')
    const expectedAlternates = `${realObjectsDir}\n`
    const currentAlternates = await readFile(alternatesPath, 'utf8').catch(
      () => null,
    )
    if (currentAlternates !== expectedAlternates) {
      await mkdir(dirname(alternatesPath), { recursive: true })
      const tmpAlternates = `${alternatesPath}.tmp-${randomUUID()}`
      await writeFile(tmpAlternates, expectedAlternates, 'utf8')
      await rename(tmpAlternates, alternatesPath).catch(async (error) => {
        await rm(tmpAlternates, { force: true }).catch(() => undefined)
        throw error
      })
    }
    const srcExclude = join(realGitDir, 'info', 'exclude')
    const dstExclude = join(shadowDir, 'info', 'exclude')
    try {
      const excludeInfo = await lstat(srcExclude)
      if (!excludeInfo.isSymbolicLink() && excludeInfo.isFile()) {
        await mkdir(dirname(dstExclude), { recursive: true })
        const tmpExclude = `${dstExclude}.tmp-${randomUUID()}`
        await copyFile(srcExclude, tmpExclude)
        await rename(tmpExclude, dstExclude).catch(async (error) => {
          await rm(tmpExclude, { force: true }).catch(() => undefined)
          throw error
        })
      }
    } catch (error) {
      if (!isMissingError(error)) throw error
    }
    return shadowDir
  })
}

function parseUnbornGitPaths(error: unknown): string[] {
  const errRecord = record(error)
  const stderr =
    str(errRecord.stderr) ?? (error instanceof Error ? error.message : '')
  const paths: string[] = []
  for (const match of stderr.matchAll(
    /error:\s+'([^']+?)\/?'\s+does not have a commit checked out/g,
  )) {
    const cleaned = match[1].replace(/\/+$/, '')
    if (cleaned) paths.push(cleaned)
  }
  return paths
}

async function captureGitTree(
  repoRoot: string,
  shadowDir: string,
  realGitDir: string,
): Promise<{ treeHash: string; unbornDirs: string[] }> {
  const tempIndex = join(shadowDir, `tmp-index-${randomUUID()}`)
  const env = gitEnv({
    GIT_DIR: shadowDir,
    GIT_WORK_TREE: repoRoot,
    GIT_INDEX_FILE: tempIndex,
  })
  const excluded: string[] = []
  const excludedSet = new Set<string>()
  let captured = false
  let lastError: unknown
  try {
    for (let attempt = 0; attempt < 128; attempt++) {
      // Seed tempIndex with the real git index while preserving its mtime so Git's racy-index check still re-hashes same-second edits.
      const srcIndex = join(realGitDir, 'index')
      const indexStat = await stat(srcIndex).catch(() => null)
      if (indexStat) {
        await copyFile(srcIndex, tempIndex)
          .then(() => utimes(tempIndex, indexStat.atime, indexStat.mtime))
          .catch(() => rm(tempIndex, { force: true }).catch(() => undefined))
      }
      const addArgs =
        excluded.length > 0
          ? [
              'add',
              '-A',
              '--',
              '.',
              ...excluded.map((dir) => `:(top,exclude,literal)${dir}`),
            ]
          : ['add', '-A', '--', '.']
      try {
        await execFileAsync('git', addArgs, {
          cwd: repoRoot,
          env,
          encoding: 'utf8',
          timeout: GIT_TIMEOUT_MS,
        })
        captured = true
        break
      } catch (error) {
        lastError = error
        await rm(tempIndex, { force: true }).catch(() => undefined)
        const discovered = parseUnbornGitPaths(error).filter(
          (rel) => !excludedSet.has(rel),
        )
        if (discovered.length === 0) throw error
        for (const rel of discovered) {
          excludedSet.add(rel)
          excluded.push(rel)
        }
      }
    }
    if (!captured) {
      throw lastError instanceof Error
        ? lastError
        : new Error(
            'Exceeded maximum unborn nested repository exclusions while capturing git tree',
          )
    }
    const { stdout } = await execFileAsync('git', ['write-tree'], {
      cwd: repoRoot,
      env,
      encoding: 'utf8',
      timeout: GIT_TIMEOUT_MS,
    })
    const treeHash = stdout.trim()
    if (!/^[a-f0-9]{40,64}$/i.test(treeHash)) {
      throw new Error(`Invalid git tree hash: ${treeHash}`)
    }
    return { treeHash, unbornDirs: excluded }
  } finally {
    await rm(tempIndex, { force: true }).catch(() => undefined)
  }
}

type DiffEntry = {
  modeOld: string
  modeNew: string
  hashOld: string
  status: string
  path: string
}

function parseNullDiffTree(buf: Buffer): DiffEntry[] {
  const entries: DiffEntry[] = []
  let offset = 0
  while (offset < buf.length) {
    const headerEnd = buf.indexOf(0, offset)
    if (headerEnd < 0) break
    const header = buf.subarray(offset, headerEnd).toString('utf8')
    offset = headerEnd + 1
    const pathEnd = buf.indexOf(0, offset)
    if (pathEnd < 0) break
    const filePath = buf.subarray(offset, pathEnd).toString('utf8')
    offset = pathEnd + 1
    if (!header.startsWith(':')) continue
    const parts = header.slice(1).split(' ')
    if (parts.length < 5) continue
    entries.push({
      modeOld: parts[0],
      modeNew: parts[1],
      hashOld: parts[2],
      status: parts[4].charAt(0),
      path: filePath,
    })
  }
  return entries
}

async function validateGitCheckpoint(
  appDataDir: string,
  checkpoint: TurnFileCheckpoint,
): Promise<{
  resolvedRoot: string
  git: { repoRoot: string; gitDir: string; commonDir: string }
  shadowDir: string
}> {
  const { repoRoot, treeHash } = checkpoint
  const resolvedRoot = await realpath(repoRoot).catch(() => null)
  const git = resolvedRoot ? await resolveGitRepo(resolvedRoot) : null
  if (!resolvedRoot || !git) {
    throw new Error(`Git repository is no longer available at ${repoRoot}`)
  }
  const shadowDir = await ensureShadowGitRepo(
    appDataDir,
    resolvedRoot,
    git.gitDir,
    git.commonDir,
  )
  await execFileAsync(
    'git',
    [`--git-dir=${shadowDir}`, 'cat-file', '-e', `${treeHash}^{tree}`],
    { cwd: resolvedRoot, env: gitEnv(), timeout: GIT_TIMEOUT_MS },
  )
  return { resolvedRoot, git, shadowDir }
}

async function restoreGitTree(
  appDataDir: string,
  checkpoint: TurnFileCheckpoint,
): Promise<void> {
  const { treeHash: targetTreeHash } = checkpoint
  const { resolvedRoot, git, shadowDir } = await validateGitCheckpoint(
    appDataDir,
    checkpoint,
  )
  const current = await captureGitTree(resolvedRoot, shadowDir, git.gitDir)
  const baselineUnborn = new Set(checkpoint.unbornDirs ?? [])
  for (const unbornDir of current.unbornDirs) {
    if (baselineUnborn.has(unbornDir)) continue
    const absPath = resolveContainedPath(resolvedRoot, unbornDir)
    if (!absPath) continue
    if (!(await assertNoSymlinkEscape(resolvedRoot, dirname(absPath)))) {
      throw new Error(
        `Refusing to remove directory across symlink: ${unbornDir}`,
      )
    }
    const existing = await lstat(absPath).catch(() => null)
    await removeExistingEntry(absPath, existing)
    await pruneEmptyParents(dirname(absPath), resolvedRoot)
  }
  if (current.treeHash === targetTreeHash) return
  const { stdout } = await execFileAsync(
    'git',
    [
      `--git-dir=${shadowDir}`,
      'diff-tree',
      '-r',
      '-z',
      '--no-renames',
      targetTreeHash,
      current.treeHash,
    ],
    {
      cwd: resolvedRoot,
      env: gitEnv(),
      encoding: 'buffer',
      maxBuffer: GIT_DIFF_MAX_BUFFER,
      timeout: GIT_TIMEOUT_MS,
    },
  )
  const diffEntries = parseNullDiffTree(stdout).filter(
    (entry) => entry.modeOld !== '160000' && entry.modeNew !== '160000',
  )
  // Remove added paths before restoring target entries so file/directory swaps cannot collide across phases.
  for (const entry of diffEntries) {
    if (entry.status !== 'A') continue
    const absPath = resolveContainedPath(resolvedRoot, entry.path)
    if (!absPath) continue
    if (!(await assertNoSymlinkEscape(resolvedRoot, dirname(absPath)))) {
      throw new Error(`Refusing to remove entry across symlink: ${entry.path}`)
    }
    const existing = await lstat(absPath).catch(() => null)
    if (!existing) continue
    await removeExistingEntry(absPath, existing)
    await pruneEmptyParents(dirname(absPath), resolvedRoot)
  }
  for (const entry of diffEntries) {
    if (entry.status === 'A') continue
    const absPath = resolveContainedPath(resolvedRoot, entry.path)
    if (!absPath) continue
    const { stdout: blob } = await execFileAsync(
      'git',
      [`--git-dir=${shadowDir}`, 'cat-file', 'blob', entry.hashOld],
      {
        cwd: resolvedRoot,
        env: gitEnv(),
        encoding: 'buffer',
        maxBuffer: GIT_DIFF_MAX_BUFFER,
        timeout: GIT_TIMEOUT_MS,
      },
    )
    const parent = dirname(absPath)
    if (!(await assertNoSymlinkEscape(resolvedRoot, parent))) {
      throw new Error(`Refusing to restore file across symlink: ${entry.path}`)
    }
    await mkdir(parent, { recursive: true })
    const currentStat = await lstat(absPath).catch(() => null)
    if (entry.modeOld === '120000') {
      await removeExistingEntry(absPath, currentStat)
      await symlink(blob.toString('utf8'), absPath)
      continue
    }
    await removeExistingEntry(absPath, currentStat)
    await writeFile(absPath, blob)
    await chmod(absPath, entry.modeOld === '100755' ? 0o755 : 0o644)
  }
}

async function loadTargetCheckpoint(
  appDataDir: string,
  nativeId: string,
  checkpointId: string,
): Promise<TurnFileCheckpoint> {
  const checkpoints = await readSessionCheckpoints(appDataDir, nativeId)
  const checkpoint = checkpoints.find((cp) => cp.id === checkpointId)
  if (!checkpoint) {
    throw new Error('No file checkpoint recorded for this message')
  }
  return checkpoint
}

/** Capture a pre-turn Git workspace checkpoint before dispatching `session/prompt`, or return `undefined` outside Git. */
export async function captureTurnFileCheckpoint(
  appDataDir: string,
  nativeId: string,
  cwd: string,
  afterStepIndex: number,
): Promise<TurnFileCheckpoint | undefined> {
  assertSafeNativeId(nativeId)
  const git = await resolveGitRepo(cwd)
  if (!git) return undefined
  const createdAt = new Date().toISOString()
  const shadowDir = await ensureShadowGitRepo(
    appDataDir,
    git.repoRoot,
    git.gitDir,
    git.commonDir,
  )
  const { treeHash, unbornDirs } = await captureGitTree(
    git.repoRoot,
    shadowDir,
    git.gitDir,
  )
  const checkpoint: TurnFileCheckpoint = {
    id: randomUUID(),
    afterStepIndex,
    kind: 'git',
    repoRoot: git.repoRoot,
    treeHash,
    ...(unbornDirs.length > 0 ? { unbornDirs } : {}),
    createdAt,
  }
  await queueSessionOp(nativeId, async () => {
    const existing = await readSessionCheckpoints(appDataDir, nativeId)
    existing.push(checkpoint)
    await writeSessionCheckpoints(appDataDir, nativeId, existing)
  })
  return checkpoint
}

/** Verify that `checkpointId` exists and its backing Git tree can be restored without mutating workspace files. */
export async function validateSessionFileCheckpoint(
  appDataDir: string,
  nativeId: string,
  checkpointId: string,
): Promise<void> {
  assertSafeNativeId(nativeId)
  await queueSessionOp(nativeId, async () => {
    const cp = await loadTargetCheckpoint(appDataDir, nativeId, checkpointId)
    await validateGitCheckpoint(appDataDir, cp)
  })
}

/** Restore the workspace to the Git snapshot recorded by `checkpointId`. */
export async function restoreSessionFileCheckpoint(
  appDataDir: string,
  nativeId: string,
  checkpointId: string,
): Promise<void> {
  assertSafeNativeId(nativeId)
  await queueSessionOp(nativeId, async () => {
    const cp = await loadTargetCheckpoint(appDataDir, nativeId, checkpointId)
    await restoreGitTree(appDataDir, cp)
  })
}

/** Copy checkpoints belonging to turns strictly before `cutoffStepIndex` to a forked session. */
export async function forkSessionCheckpoints(
  appDataDir: string,
  oldNativeId: string,
  newNativeId: string,
  cutoffStepIndex: number,
): Promise<void> {
  if (
    !sessionIdPattern.test(oldNativeId) ||
    !sessionIdPattern.test(newNativeId) ||
    oldNativeId === newNativeId
  ) {
    return
  }
  await queueSessionOp(oldNativeId, async () => {
    const existing = await readSessionCheckpoints(appDataDir, oldNativeId)
    const retained = existing.filter(
      (cp) => cp.afterStepIndex < cutoffStepIndex,
    )
    if (!retained.length) return
    await queueSessionOp(newNativeId, () =>
      writeSessionCheckpoints(appDataDir, newNativeId, retained),
    )
  })
}

/** Remove the persisted checkpoint manifest for a disposable or closed session. */
export async function removeSessionCheckpoints(
  appDataDir: string,
  nativeId: string,
): Promise<void> {
  if (!sessionIdPattern.test(nativeId)) return
  await queueSessionOp(nativeId, async () => {
    const filePath = sessionCheckpointsPath(appDataDir, nativeId)
    const info = await lstat(filePath).catch((error: unknown) => {
      if (isMissingError(error)) return null
      throw error
    })
    if (!info) return
    if (info.isSymbolicLink()) {
      throw new Error(`Refusing to remove symlink: ${filePath}`)
    }
    await rm(filePath, { force: true })
  })
}

/** Find the checkpoint captured immediately before `userStepIndex` (bounded by `prevUserStepIndex`). */
export function findCheckpointForUserStep(
  checkpoints: readonly TurnFileCheckpoint[],
  prevUserStepIndex: number,
  userStepIndex: number,
): TurnFileCheckpoint | undefined {
  let match: TurnFileCheckpoint | undefined
  for (const cp of checkpoints) {
    if (
      cp.afterStepIndex >= prevUserStepIndex &&
      cp.afterStepIndex < userStepIndex
    ) {
      match = cp
    }
  }
  return match
}
