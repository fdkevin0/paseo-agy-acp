/** Subscription quota obtained through agy's local /usage command, without a model turn. */
import {
  lstat,
  open,
  readdir,
  realpath,
  stat,
  type FileHandle,
} from 'node:fs/promises'
import { join, sep } from 'node:path'
import { z } from 'zod'
import { spawnProcess, terminateProcess } from '@getpaseo/plugin/server'
import type { ProviderLaunch } from '@getpaseo/plugin/server/provider'
import {
  hashAccountKey,
  unavailable,
  windowFromReportedDuration,
  toneFromUsedPct,
  type UsageAccount,
  type UsageReport,
  type UsageScope,
  type UsageSourceRegistration,
} from '@getpaseo/plugin/server/usage'
import {
  definedProcessEnv,
  detachString,
  formatErrorMessage,
  mergeEnv,
  setBounded,
} from './common.ts'
import {
  dataDirectory,
  isCwdIndependentLaunch,
  resolveHome,
} from './preflight.ts'

const FIVE_HOURS_SECONDS = 5 * 3600
const WEEKLY_SECONDS = 7 * 86_400
const USAGE_FETCH_TIMEOUT_MS = 15_000
const LOG_CHUNK_SIZE = 64 * 1024
const ACCOUNT_CACHE_LIMIT = 100
const MAX_USAGE_STDOUT_BYTES = 1024 * 1024
const MAX_USAGE_STDERR_BYTES = 4096
const AUTH_LOG_PATTERN =
  /(?:applyAuthResult:\s*email=|OAuth:\s*authenticated successfully as\s+)([^\s,]+)(?=[,\s])/g

function isContainedIn(target: string, base: string): boolean {
  const prefix = base === sep ? sep : base + sep
  return target.startsWith(prefix)
}

function windowOrder(window: string): number {
  return window === '5h' ? 0 : window === 'weekly' ? 1 : 2
}

const usageInputSchema = z.object({
  dataDir: z.string().min(1),
})

const groupsSchema = z.object({
  status: z.string().nullish(),
  response: z.string().nullish(),
  command: z
    .object({
      data: z.object({
        groups: z.array(
          z.object({
            name: z.string().min(1),
            buckets: z.array(
              z.object({
                id: z.string().nullish(),
                name: z.string().nullish(),
                window: z.string().nullish(),
                disabled: z.boolean().nullish(),
                remaining_fraction: z.number().finite().nullish(),
                reset_time: z.string().nullish(),
              }),
            ),
          }),
        ),
      }),
    })
    .nullish(),
})

/** Validate the CLI envelope before translating its quota groups. */
export function parseUsage(stdout: string) {
  let parsed: z.infer<typeof groupsSchema>
  try {
    parsed = groupsSchema.parse(JSON.parse(stdout))
  } catch (error) {
    throw new Error('Unexpected /usage JSON output from agy', { cause: error })
  }
  if (parsed.status != null && parsed.status !== 'SUCCESS') {
    const detail = parsed.response?.trim()
    throw new Error(
      detail
        ? `agy /usage returned status ${parsed.status}: ${detail}`
        : `agy /usage returned status ${parsed.status}`,
    )
  }
  if (!parsed.command) {
    throw new Error('Unexpected /usage JSON output from agy')
  }
  return parsed.command.data.groups.map((group) => ({
    name: group.name,
    buckets: group.buckets.flatMap((bucket) => {
      if (bucket.disabled === true) return []
      const window = bucket.window?.trim() ?? ''
      const id = bucket.id?.trim() || window
      // Drop unidentifiable non-standard buckets (e.g. amount-only entries without id/window)
      // instead of failing or blocking valid sibling buckets in the same group.
      if (!id) return []
      return [
        {
          id,
          name: bucket.name?.trim() || window || id,
          window,
          remainingFraction: bucket.remaining_fraction ?? undefined,
          resetTime: bucket.reset_time?.trim() ?? '',
        },
      ]
    }),
  }))
}

/** Translate validated `/usage` groups into Paseo quota windows without producing NaN. */
export function usageWindows(stdout: string) {
  return parseUsage(stdout).flatMap((group) =>
    group.buckets
      .toSorted((a, b) => windowOrder(a.window) - windowOrder(b.window))
      .map((bucket) => {
        const utilizationPct =
          bucket.remainingFraction === undefined
            ? null
            : Math.round(
                Math.max(0, Math.min(1, 1 - bucket.remainingFraction)) * 100,
              )
        return windowFromReportedDuration({
          durationSeconds:
            bucket.window === '5h'
              ? FIVE_HOURS_SECONDS
              : bucket.window === 'weekly'
                ? WEEKLY_SECONDS
                : null,
          scope: {
            id: group.name
              .toLowerCase()
              .replace(/[^a-z0-9]+/g, '_')
              .replace(/^_|_$/g, ''),
            label:
              group.name === 'Gemini Models'
                ? 'Gemini'
                : group.name === 'Claude and GPT models'
                  ? 'Claude / GPT'
                  : group.name,
          },
          unknown: {
            id: bucket.id,
            label: bucket.name,
            shortLabel: bucket.window || bucket.id,
          },
          utilizationPct,
          resetsAt: bucket.resetTime || null,
          summary: true,
          tone: toneFromUsedPct(utilizationPct),
        })
      }),
  )
}

async function readChunkEmail(
  file: FileHandle,
  offset: number,
  length: number,
): Promise<string | undefined> {
  const bytes = Buffer.alloc(length)
  const { bytesRead } = await file.read(bytes, 0, length, offset)
  const matches = [
    ...bytes.toString('utf8', 0, bytesRead).matchAll(AUTH_LOG_PATTERN),
  ]
  const email = matches.at(-1)?.[1]
  return email === undefined ? undefined : detachString(email)
}

// shortcut: agy >=1.5 exposes no structured account email or lightweight keychain auth check in /usage JSON or ACP; drop log scanning once upstream adds one.
/** Read bounded log tail/head chunks, preferring the most recently modified deduplicated login log within dataDir. */
export async function readLoggedEmail(
  dataDir: string,
): Promise<string | undefined> {
  let base: string
  try {
    if (!(await lstat(dataDir)).isDirectory()) return undefined
    base = await realpath(dataDir)
  } catch {
    return undefined
  }
  const logDir = join(base, 'log')
  let names: string[] = []
  try {
    if ((await lstat(logDir)).isDirectory()) {
      names = (await readdir(logDir)).filter((name) =>
        /^(?:cli|acp-server)-.*\.log$/.test(name),
      )
    }
  } catch {
    /* Logs are optional. */
  }
  const candidates = (
    await Promise.all(
      names.map(async (name) => {
        const path = join(logDir, name)
        return {
          path,
          modified: await lstat(path).then(
            (s) => (s.isFile() ? s.mtimeMs : 0),
            () => 0,
          ),
        }
      }),
    )
  )
    .filter((entry) => entry.modified > 0)
    .sort((a, b) => b.modified - a.modified)
    .slice(0, 10)
  const main = join(base, 'cli.log')
  const mainModified = await stat(main).then(
    (s) => (s.isFile() ? s.mtimeMs : 0),
    () => 0,
  )
  if (mainModified > 0) {
    candidates.push({ path: main, modified: mainModified })
  }
  const resolved = (
    await Promise.all(
      candidates.map(async (entry) => ({
        path: await realpath(entry.path).then(
          (p) => (isContainedIn(p, base) ? p : undefined),
          () => undefined,
        ),
        modified: entry.modified,
      })),
    )
  ).filter(
    (entry): entry is { path: string; modified: number } =>
      entry.path !== undefined,
  )
  const deduped = new Map<string, number>()
  for (const entry of resolved) {
    const prev = deduped.get(entry.path) ?? 0
    if (entry.modified > prev) deduped.set(entry.path, entry.modified)
  }
  const ordered = [...deduped.entries()]
    .map(([path, modified]) => ({ path, modified }))
    .sort((a, b) => b.modified - a.modified)
  for (const { path } of ordered) {
    try {
      const file = await open(path, 'r')
      try {
        const info = await file.stat()
        if (!info.isFile() || info.size <= 0) continue
        const tailLen = Math.min(info.size, LOG_CHUNK_SIZE)
        const tailHit = await readChunkEmail(file, info.size - tailLen, tailLen)
        if (tailHit) return tailHit
        if (info.size > LOG_CHUNK_SIZE) {
          const headHit = await readChunkEmail(file, 0, LOG_CHUNK_SIZE)
          if (headHit) return headHit
        }
      } finally {
        await file.close()
      }
    } catch {
      /* Missing or unreadable logs do not prevent quota checks. */
    }
  }
}

/** agy >=1.5 keeps credentials in the OS keychain; the token file is the legacy store. */
async function hasTokenFile(dataDir: string): Promise<boolean> {
  try {
    if (!(await lstat(dataDir)).isDirectory()) return false
    const base = await realpath(dataDir)
    const s = await lstat(join(base, 'antigravity-oauth-token'))
    return s.isFile() && s.size > 0
  } catch {
    return false
  }
}

const VALUE_FLAGS = new Set([
  '--gemini-dir',
  '--gemini_dir',
  '--app-data-dir',
  '--app_data_dir',
])

const STRIPPED_ACP_FLAGS = new Set([
  '--stdio',
  '--worker',
  '--process-per-session',
  '--dangerously-skip-permissions',
])

function usageArgs(args: readonly string[]): string[] {
  const result: string[] = []
  let removedAcp = false
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    if (VALUE_FLAGS.has(arg)) {
      result.push(arg)
      if (i + 1 < args.length) result.push(args[++i])
      continue
    }
    if (!removedAcp && arg === 'acp') {
      removedAcp = true
      continue
    }
    if (STRIPPED_ACP_FLAGS.has(arg)) continue
    result.push(arg)
  }
  return result
}

async function runUsageCommand(
  cmd: string,
  args: string[],
  options: { cwd?: string; env: Record<string, string>; timeoutMs: number },
): Promise<string> {
  const child = spawnProcess(cmd, args, {
    cwd: options.cwd,
    env: options.env,
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: process.platform !== 'win32',
  })
  return new Promise<string>((resolve, reject) => {
    let settled = false
    let aborting = false
    let stdout = ''
    let stderr = ''
    let timer: NodeJS.Timeout | undefined

    child.stdout?.on('error', () => undefined)
    child.stderr?.on('error', () => undefined)

    const exited =
      child.exitCode !== null || child.signalCode !== null
        ? Promise.resolve()
        : new Promise<void>((r) => {
            child.once('exit', () => r())
            child.once('error', () => r())
          })

    const finish = (error?: Error, value?: string) => {
      if (settled) return
      settled = true
      if (timer) clearTimeout(timer)
      if (error) reject(error)
      else resolve(value ?? '')
    }

    const abort = (error: Error) => {
      if (settled || aborting) return
      aborting = true
      if (timer) clearTimeout(timer)
      void (async () => {
        if (process.platform !== 'win32' && child.pid) {
          try {
            process.kill(-child.pid, 'SIGKILL')
          } catch {
            try {
              child.kill('SIGKILL')
            } catch {
              /* Already exited. */
            }
          }
        } else {
          await terminateProcess(child).catch(() => undefined)
        }
        child.stdout?.destroy()
        child.stderr?.destroy()
        let exitTimer: NodeJS.Timeout | undefined
        await Promise.race([
          exited,
          new Promise<void>((r) => {
            exitTimer = setTimeout(r, 250)
            exitTimer.unref()
          }),
        ])
        if (exitTimer) clearTimeout(exitTimer)
        finish(error)
      })()
    }

    timer = setTimeout(() => {
      abort(new Error(`${cmd} /usage timed out after ${options.timeoutMs}ms`))
    }, options.timeoutMs)

    child.stdout?.setEncoding('utf8')
    child.stderr?.setEncoding('utf8')

    child.stdout?.on('data', (chunk: string) => {
      if (settled || aborting) return
      stdout += chunk
      if (stdout.length > MAX_USAGE_STDOUT_BYTES) {
        abort(new Error('Unexpectedly large /usage output from agy'))
      }
    })

    child.stderr?.on('data', (chunk: string) => {
      if (settled || aborting) return
      const next = stderr + chunk
      stderr = detachString(next.slice(-MAX_USAGE_STDERR_BYTES))
      if (/Authentication required\b/i.test(next)) {
        abort(new Error('Authentication required; run agy to sign in'))
      }
    })

    child.once('error', (error) => {
      if (settled || aborting) return
      child.stdout?.destroy()
      child.stderr?.destroy()
      finish(error instanceof Error ? error : new Error(String(error)))
    })

    child.once('close', (code, signal) => {
      if (settled || aborting) return
      if (code === 0 && !signal) {
        finish(undefined, stdout)
        return
      }
      const status =
        code !== null ? `code ${code}` : `signal ${signal ?? 'unknown'}`
      const detail = stderr.trim()
      finish(
        new Error(
          detail
            ? `${cmd} exited (${status}): ${detail}`
            : `${cmd} exited (${status})`,
        ),
      )
    })
  })
}

export interface AgyUsageOptions {
  env?: Readonly<Record<string, string>>
}

/** Share native launch/account observations with Paseo's subscription usage surface. */
export class AgyUsageSource {
  readonly tiers = new Map<string, string>()
  private readonly envOverride?: Readonly<Record<string, string>>
  private launches = new Map<
    string,
    {
      launch?: ProviderLaunch
      sessionEnv: Record<string, string>
      cwd?: string
    }
  >()
  private sessionEnvs = new Map<string, Record<string, string>>()

  constructor(options?: AgyUsageOptions) {
    this.envOverride = options?.env
  }

  private baseEnv(): Record<string, string> {
    return this.envOverride ? { ...this.envOverride } : definedProcessEnv()
  }

  remember(
    launch?: ProviderLaunch,
    env: Readonly<Record<string, string>> = {},
    cwd?: string,
  ): string {
    const defaultHome = resolveHome(this.baseEnv())
    const dir = dataDirectory(launch, env, cwd, defaultHome)
    this.sessionEnvs.delete(dir)
    setBounded(
      this.launches,
      dir,
      {
        launch,
        sessionEnv: { ...env },
        cwd,
      },
      ACCOUNT_CACHE_LIMIT,
    )
    return dir
  }

  async discover(scope: UsageScope): Promise<UsageAccount[]> {
    if (
      scope.kind === 'session' &&
      scope.provider !== 'agy' &&
      scope.provider !== 'antigravity'
    )
      return []
    const base = this.baseEnv()
    const defaultHome = resolveHome(base)
    let dirs: string[]
    if (scope.kind === 'global') {
      dirs = [
        ...this.launches.keys(),
        dataDirectory(undefined, {}, undefined, defaultHome),
      ]
    } else {
      // shortcut: scope omits launch commands; use the latest match until Paseo exposes a launch identity.
      const observed = [...this.launches.entries()]
        .reverse()
        .find(
          ([dir, value]) =>
            dataDirectory(value.launch, scope.env, value.cwd, defaultHome) ===
            dir,
        )
      dirs = [
        observed?.[0] ??
          dataDirectory(undefined, scope.env, undefined, defaultHome),
      ]
    }
    const accounts: UsageAccount[] = []
    for (const dir of new Set(dirs)) {
      const email = await readLoggedEmail(dir)
      const loggedIn = email !== undefined || (await hasTokenFile(dir))
      const remembered = this.launches.has(dir)
      if (!loggedIn && !remembered) {
        this.sessionEnvs.delete(dir)
        continue
      }
      if (scope.kind === 'global') {
        this.sessionEnvs.delete(dir)
      } else if (!remembered) {
        setBounded(this.sessionEnvs, dir, { ...scope.env }, ACCOUNT_CACHE_LIMIT)
      }
      accounts.push({
        key: hashAccountKey(dir),
        label: email ?? 'Antigravity',
        harness: 'Antigravity',
        input: {
          dataDir: dir,
        },
      })
    }
    return accounts
  }

  async fetch(rawInput: unknown): Promise<UsageReport> {
    const { dataDir } = usageInputSchema.parse(rawInput)
    const base = this.baseEnv()
    const defaultHome = resolveHome(base)
    const defaultDir = dataDirectory(undefined, {}, undefined, defaultHome)
    const known = this.launches.get(dataDir)
    const sessionEnv = known ? undefined : this.sessionEnvs.get(dataDir)
    if (!known && !sessionEnv && dataDir !== defaultDir) {
      return {
        status: 'error',
        error:
          'Account launch context is not available; open a session or refresh discovery first',
      }
    }
    const cmd = known?.launch?.command ?? 'agy'
    const rawArgs = known?.launch?.args ?? []
    const effectiveEnv =
      known?.launch?.env !== undefined
        ? mergeEnv(known.launch.env, known.sessionEnv)
        : mergeEnv(base, known?.sessionEnv ?? sessionEnv)
    // The CLI is the only reliable auth oracle: keychain logins leave no token file,
    // and a failed read surfaces as an error report instead of a disappearing account.
    try {
      let cwd = known?.cwd
      if (
        cwd &&
        !(await stat(cwd).then(
          (s) => s.isDirectory(),
          () => false,
        ))
      ) {
        const launch = {
          command: cmd,
          args: rawArgs,
          env: effectiveEnv,
        }
        if (!isCwdIndependentLaunch(cwd, defaultHome, launch, {}, defaultHome))
          throw new Error(
            'Remembered workspace is missing and the usage launch depends on relative paths',
          )
        cwd = defaultHome
      }
      const stdout = await runUsageCommand(
        cmd,
        [...usageArgs(rawArgs), '-p', '/usage', '--output-format', 'json'],
        {
          timeoutMs: USAGE_FETCH_TIMEOUT_MS,
          env: effectiveEnv,
          cwd,
        },
      )
      const windows = usageWindows(stdout)
      if (!windows.length)
        return unavailable({
          kind: 'no_quota',
          detail: 'No usage quota reported',
        })
      return {
        status: 'available',
        planLabel: this.tiers
          .get(dataDir)
          ?.replace(/^Google AI (Pro|Ultra)$/, '$1'),
        windows,
      }
    } catch (error) {
      return {
        status: 'error',
        error: formatErrorMessage(error),
      }
    }
  }

  registration(): UsageSourceRegistration {
    return {
      id: 'agy',
      label: 'Antigravity',
      icon: 'icon.svg',
      input: usageInputSchema,
      discover: (scope) => this.discover(scope),
      fetch: (input) => this.fetch(input),
    }
  }
}
