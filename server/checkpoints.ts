/** Per-turn file checkpoints using isolated shadow Git trees or non-Git pre-edit journals. */
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
  symlink,
  writeFile,
} from 'node:fs/promises'
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path'
import { promisify } from 'node:util'
import { z } from 'zod'
import { record, sessionIdPattern, str } from './common.ts'

const execFileAsync = promisify(execFile)
const GIT_TIMEOUT_MS = 5000
const GIT_DIFF_MAX_BUFFER = 32 * 1024 * 1024

export const JournalFileSnapshotSchema = z.discriminatedUnion('existed', [
  z.object({ existed: z.literal(false) }),
  z.object({
    existed: z.literal(true),
    blobSha256: z.string(),
    mode: z.number().int().optional(),
  }),
])

export type JournalFileSnapshot = z.infer<typeof JournalFileSnapshotSchema>

export const TurnFileCheckpointSchema = z.discriminatedUnion('kind', [
  z.object({
    id: z.string(),
    afterStepIndex: z.number().int().min(-1),
    kind: z.literal('git'),
    repoRoot: z.string(),
    treeHash: z.string(),
    createdAt: z.string(),
  }),
  z.object({
    id: z.string(),
    afterStepIndex: z.number().int().min(-1),
    kind: z.literal('journal'),
    cwd: z.string(),
    edits: z.record(z.string(), JournalFileSnapshotSchema),
    createdAt: z.string(),
  }),
])

export type TurnFileCheckpoint = z.infer<typeof TurnFileCheckpointSchema>

const SessionCheckpointsFileSchema = z.object({
  version: z.literal(1),
  checkpoints: z.array(TurnFileCheckpointSchema),
})

const sessionWriteLanes = new Map<string, Promise<void>>()
const checkpointFirstTools = new Map<string, Map<string, string>>()

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

function isMissingError(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | undefined)?.code
  return code === 'ENOENT' || code === 'ENOTDIR'
}

function assertSafeNativeId(nativeId: string) {
  if (!sessionIdPattern.test(nativeId)) {
    throw new Error(`Invalid session ID: ${nativeId}`)
  }
}

function checkpointsRoot(appDataDir: string): string {
  return join(appDataDir, 'paseo-checkpoints')
}

function sessionCheckpointsPath(appDataDir: string, nativeId: string): string {
  assertSafeNativeId(nativeId)
  return join(checkpointsRoot(appDataDir), 'sessions', `${nativeId}.json`)
}

function blobsDir(appDataDir: string): string {
  return join(checkpointsRoot(appDataDir), 'blobs')
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

async function resolveSafeJournalTarget(
  cwd: string,
  filePath: string,
): Promise<string | null> {
  const rawRoot = resolve(cwd)
  const realRoot = await realpath(rawRoot).catch(() => rawRoot)
  const candidate = isAbsolute(filePath)
    ? resolve(filePath)
    : resolve(rawRoot, filePath)
  let rel: string | null = null
  if (candidate.startsWith(rawRoot + sep)) {
    rel = relative(rawRoot, candidate)
  } else if (candidate.startsWith(realRoot + sep)) {
    rel = relative(realRoot, candidate)
  }
  if (!rel) return null
  const absPath = join(realRoot, rel)
  if (!(await assertNoSymlinkEscape(realRoot, absPath))) return null
  return absPath
}

async function assertNoSymlinkEscape(
  root: string,
  targetPath: string,
): Promise<boolean> {
  const rel = relative(root, targetPath)
  if (rel.startsWith('..') || isAbsolute(rel)) return false
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
    const { stdout: topOut } = await execFileAsync(
      'git',
      ['-C', cwd, 'rev-parse', '--show-toplevel'],
      { encoding: 'utf8', timeout: 2000 },
    )
    const rawRoot = topOut.trim()
    if (!rawRoot) return null
    const repoRoot = await realpath(rawRoot)
    const [{ stdout: dirOut }, { stdout: commonOut }] = await Promise.all([
      execFileAsync(
        'git',
        ['-C', repoRoot, 'rev-parse', '--absolute-git-dir'],
        {
          encoding: 'utf8',
          timeout: 2000,
        },
      ),
      execFileAsync('git', ['-C', repoRoot, 'rev-parse', '--git-common-dir'], {
        encoding: 'utf8',
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

async function ensureShadowGitRepo(
  appDataDir: string,
  repoRoot: string,
  realGitDir: string,
  commonGitDir: string,
): Promise<string> {
  const shadowDir = shadowGitDirForRepo(appDataDir, repoRoot)
  try {
    await lstat(join(shadowDir, 'HEAD'))
  } catch (error) {
    if (!isMissingError(error)) throw error
    await mkdir(shadowDir, { recursive: true })
    await execFileAsync('git', ['init', '--bare', shadowDir], {
      encoding: 'utf8',
      timeout: GIT_TIMEOUT_MS,
    })
  }
  // Point alternates at the real repo's object store (read-only) so tracked unchanged blobs aren't re-hashed.
  const realObjectsDir = await realpath(join(commonGitDir, 'objects')).catch(
    () => join(commonGitDir, 'objects'),
  )
  const alternatesPath = join(shadowDir, 'objects', 'info', 'alternates')
  await mkdir(dirname(alternatesPath), { recursive: true })
  await writeFile(alternatesPath, `${realObjectsDir}\n`, 'utf8')
  const srcExclude = join(realGitDir, 'info', 'exclude')
  const dstExclude = join(shadowDir, 'info', 'exclude')
  try {
    const excludeInfo = await lstat(srcExclude)
    if (!excludeInfo.isSymbolicLink() && excludeInfo.isFile()) {
      await mkdir(dirname(dstExclude), { recursive: true })
      await copyFile(srcExclude, dstExclude)
    }
  } catch (error) {
    if (!isMissingError(error)) throw error
  }
  return shadowDir
}

async function captureGitTree(
  repoRoot: string,
  shadowDir: string,
  realGitDir: string,
): Promise<string> {
  const tempIndex = join(shadowDir, `tmp-index-${randomUUID()}`)
  const env = {
    ...process.env,
    GIT_DIR: shadowDir,
    GIT_WORK_TREE: repoRoot,
    GIT_INDEX_FILE: tempIndex,
  }
  try {
    // Seed tempIndex with a copy of the real git index so `git add -A` skips clean tracked files via stat cache.
    await copyFile(join(realGitDir, 'index'), tempIndex).catch(() => undefined)
    await execFileAsync('git', ['add', '-A'], {
      cwd: repoRoot,
      env,
      encoding: 'utf8',
      timeout: GIT_TIMEOUT_MS,
    })
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
    return treeHash
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

async function restoreGitTree(
  appDataDir: string,
  repoRoot: string,
  targetTreeHash: string,
): Promise<void> {
  const resolvedRoot = await realpath(repoRoot)
  const git = await resolveGitRepo(resolvedRoot)
  if (!git) {
    throw new Error(`Git repository is no longer available at ${repoRoot}`)
  }
  const shadowDir = await ensureShadowGitRepo(
    appDataDir,
    resolvedRoot,
    git.gitDir,
    git.commonDir,
  )
  const currentTreeHash = await captureGitTree(
    resolvedRoot,
    shadowDir,
    git.gitDir,
  )
  if (currentTreeHash === targetTreeHash) return
  const { stdout } = await execFileAsync(
    'git',
    [
      `--git-dir=${shadowDir}`,
      'diff-tree',
      '-r',
      '-z',
      '--no-renames',
      targetTreeHash,
      currentTreeHash,
    ],
    {
      cwd: resolvedRoot,
      encoding: 'buffer',
      maxBuffer: GIT_DIFF_MAX_BUFFER,
      timeout: GIT_TIMEOUT_MS,
    },
  )
  for (const entry of parseNullDiffTree(stdout)) {
    if (entry.modeOld === '160000' || entry.modeNew === '160000') continue
    const absPath = resolveContainedPath(resolvedRoot, entry.path)
    if (!absPath) continue
    if (entry.status === 'A') {
      const existing = await lstat(absPath).catch(() => null)
      if (!existing) continue
      await rm(absPath, { recursive: false, force: true })
      await pruneEmptyParents(dirname(absPath), resolvedRoot)
      continue
    }
    const { stdout: blob } = await execFileAsync(
      'git',
      [`--git-dir=${shadowDir}`, 'cat-file', 'blob', entry.hashOld],
      {
        cwd: resolvedRoot,
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
    if (currentStat?.isSymbolicLink() || currentStat?.isDirectory()) {
      await rm(absPath, { recursive: currentStat.isDirectory(), force: true })
    }
    if (entry.modeOld === '120000') {
      await symlink(blob.toString('utf8'), absPath)
      continue
    }
    await writeFile(absPath, blob)
    await chmod(absPath, entry.modeOld === '100755' ? 0o755 : 0o644)
  }
}

async function storeBlob(appDataDir: string, content: Buffer): Promise<string> {
  const hash = createHash('sha256').update(content).digest('hex')
  const dir = blobsDir(appDataDir)
  const blobPath = join(dir, hash)
  try {
    await lstat(blobPath)
  } catch (error) {
    if (!isMissingError(error)) throw error
    await mkdir(dir, { recursive: true })
    const tmpPath = `${blobPath}.tmp-${randomUUID()}`
    await writeFile(tmpPath, content)
    await rename(tmpPath, blobPath)
  }
  return hash
}

/** Capture a pre-turn workspace checkpoint before dispatching `session/prompt`. */
export async function captureTurnFileCheckpoint(
  appDataDir: string,
  nativeId: string,
  cwd: string,
  afterStepIndex: number,
): Promise<TurnFileCheckpoint> {
  assertSafeNativeId(nativeId)
  const createdAt = new Date().toISOString()
  const git = await resolveGitRepo(cwd)
  let checkpoint: TurnFileCheckpoint
  if (git) {
    try {
      const shadowDir = await ensureShadowGitRepo(
        appDataDir,
        git.repoRoot,
        git.gitDir,
        git.commonDir,
      )
      const treeHash = await captureGitTree(git.repoRoot, shadowDir, git.gitDir)
      checkpoint = {
        id: randomUUID(),
        afterStepIndex,
        kind: 'git',
        repoRoot: git.repoRoot,
        treeHash,
        createdAt,
      }
    } catch {
      checkpoint = {
        id: randomUUID(),
        afterStepIndex,
        kind: 'journal',
        cwd: await realpath(cwd).catch(() => resolve(cwd)),
        edits: {},
        createdAt,
      }
    }
  } else {
    checkpoint = {
      id: randomUUID(),
      afterStepIndex,
      kind: 'journal',
      cwd: await realpath(cwd).catch(() => resolve(cwd)),
      edits: {},
      createdAt,
    }
  }
  await queueSessionOp(nativeId, async () => {
    const existing = await readSessionCheckpoints(appDataDir, nativeId)
    existing.push(checkpoint)
    await writeSessionCheckpoints(appDataDir, nativeId, existing)
  })
  return checkpoint
}

/** Record pre-edit state of a file touched during a non-Git turn checkpoint. */
export async function recordJournalToolEdit(
  appDataDir: string,
  nativeId: string,
  checkpointId: string,
  cwd: string,
  filePath: string,
  options?: {
    oldText?: string | null
    toolCompleted?: boolean
    toolCallId?: string
  },
): Promise<void> {
  if (!sessionIdPattern.test(nativeId)) return
  const absPath = await resolveSafeJournalTarget(cwd, filePath)
  if (!absPath) return

  let snapshot: JournalFileSnapshot | undefined
  if (options?.toolCompleted && options.oldText !== undefined) {
    if (options.oldText === null) snapshot = { existed: false }
    else {
      const blobSha256 = await storeBlob(
        appDataDir,
        Buffer.from(options.oldText, 'utf8'),
      )
      snapshot = { existed: true, blobSha256 }
    }
  } else {
    try {
      const info = await lstat(absPath)
      if (info.isSymbolicLink() || !info.isFile()) return
      const data = await readFile(absPath)
      const blobSha256 = await storeBlob(appDataDir, data)
      snapshot = { existed: true, blobSha256, mode: info.mode & 0o777 }
    } catch (error) {
      if (!isMissingError(error)) return
      snapshot = { existed: false }
    }
  }

  if (!snapshot) return
  const finalSnapshot = snapshot
  await queueSessionOp(nativeId, async () => {
    const existing = await readSessionCheckpoints(appDataDir, nativeId)
    const target = existing.find((cp) => cp.id === checkpointId)
    if (!target || target.kind !== 'journal') return
    let toolOwners = checkpointFirstTools.get(checkpointId)
    if (!toolOwners) {
      toolOwners = new Map<string, string>()
      checkpointFirstTools.set(checkpointId, toolOwners)
    }
    const alreadyRecorded = Object.hasOwn(target.edits, absPath)
    if (alreadyRecorded) {
      // Allow the same first tool call to refine an racy pre-edit lstat once its diff oldText completes.
      const owner = toolOwners.get(absPath)
      const canRefine =
        Boolean(options?.toolCompleted) &&
        options?.oldText !== undefined &&
        Boolean(options?.toolCallId) &&
        owner === options?.toolCallId
      if (!canRefine) return
    } else if (options?.toolCallId) {
      toolOwners.set(absPath, options.toolCallId)
    }
    target.edits[absPath] = finalSnapshot
    await writeSessionCheckpoints(appDataDir, nativeId, existing)
  })
}

/** Restore the workspace to the state recorded by `checkpointId`. */
export async function restoreSessionFileCheckpoint(
  appDataDir: string,
  nativeId: string,
  checkpointId: string,
): Promise<void> {
  assertSafeNativeId(nativeId)
  await queueSessionOp(nativeId, async () => {
    const checkpoints = await readSessionCheckpoints(appDataDir, nativeId)
    const idx = checkpoints.findIndex((cp) => cp.id === checkpointId)
    if (idx < 0) {
      throw new Error('No file checkpoint recorded for this message')
    }
    const target = checkpoints[idx]
    if (target.kind === 'git') {
      await restoreGitTree(appDataDir, target.repoRoot, target.treeHash)
      return
    }
    const resolvedRoot = await realpath(target.cwd).catch(() =>
      resolve(target.cwd),
    )
    for (let i = checkpoints.length - 1; i >= idx; i--) {
      const cp = checkpoints[i]
      if (cp.kind !== 'journal') continue
      for (const [absPath, snap] of Object.entries(cp.edits)) {
        const contained = resolveContainedPath(resolvedRoot, absPath)
        if (!contained) continue
        if (!(await assertNoSymlinkEscape(resolvedRoot, dirname(contained)))) {
          throw new Error(`Refusing to restore file across symlink: ${absPath}`)
        }
        if (!snap.existed) {
          await rm(contained, { force: true })
          await pruneEmptyParents(dirname(contained), resolvedRoot)
          continue
        }
        const blobPath = join(blobsDir(appDataDir), snap.blobSha256)
        const data = await readFile(blobPath)
        await mkdir(dirname(contained), { recursive: true })
        await writeFile(contained, data)
        if (snap.mode !== undefined) await chmod(contained, snap.mode)
      }
    }
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
    !sessionIdPattern.test(newNativeId)
  ) {
    return
  }
  const existing = await queueSessionOp(oldNativeId, () =>
    readSessionCheckpoints(appDataDir, oldNativeId),
  )
  const retained = existing.filter((cp) => cp.afterStepIndex < cutoffStepIndex)
  if (!retained.length) return
  await queueSessionOp(newNativeId, () =>
    writeSessionCheckpoints(appDataDir, newNativeId, retained),
  )
}

/** Remove the persisted checkpoint manifest for a disposable or closed session. */
export async function removeSessionCheckpoints(
  appDataDir: string,
  nativeId: string,
): Promise<void> {
  if (!sessionIdPattern.test(nativeId)) return
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

/** Extract candidate edited file paths and pre-edit text from an ACP tool update. */
export function extractToolEditTargets(tool: Record<string, unknown>): Array<{
  filePath: string
  oldText?: string | null
}> {
  const results: Array<{ filePath: string; oldText?: string | null }> = []
  const seen = new Set<string>()
  const add = (filePath: string | undefined, oldText?: string | null) => {
    if (!filePath || seen.has(filePath)) return
    seen.add(filePath)
    results.push({
      filePath,
      ...(oldText !== undefined ? { oldText } : {}),
    })
  }
  const content = Array.isArray(tool.content) ? tool.content : []
  for (const block of content) {
    const c = record(block)
    if (c.type === 'diff' && typeof c.path === 'string' && c.path) {
      const oldText =
        typeof c.oldText === 'string'
          ? c.oldText
          : c.oldText === null
            ? null
            : undefined
      add(c.path, oldText)
    }
  }
  const input = record(tool.rawInput)
  const inputPath =
    str(input.TargetFile) ??
    str(input.file_path) ??
    str(input.filePath) ??
    str(input.AbsolutePath) ??
    str(input.Path) ??
    str(input.path)
  const kind = str(tool.kind)
  const title = str(tool.title) ?? ''
  const isEditLike =
    kind === 'edit' ||
    kind === 'delete' ||
    /(?:write_file|write_to_file|create_file|fs\/write_text_file|write_text_file|edit_file|replace_file_content|multi_replace_file_content|delete_directory)/i.test(
      title,
    )
  if (isEditLike && inputPath) add(inputPath)
  const locations = Array.isArray(tool.locations) ? tool.locations : []
  if (isEditLike) {
    for (const loc of locations) {
      const path = str(record(loc).path)
      if (path) add(path)
    }
  }
  return results
}
