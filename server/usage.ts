/** Subscription quota obtained through agy's local /usage command, without a model turn. */
import { open, readdir, stat } from 'node:fs/promises'
import { join, isAbsolute } from 'node:path'
import { homedir } from 'node:os'
import { z } from 'zod'
import { execCommand } from '@getpaseo/plugin/server'
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
import { formatErrorMessage, setBounded } from './common.ts'
import { dataDirectory } from './preflight.ts'

const FIVE_HOURS_SECONDS = 5 * 3600
const WEEKLY_SECONDS = 7 * 86_400
const USAGE_FETCH_TIMEOUT_MS = 15_000
const LOG_CHUNK_SIZE = 64 * 1024
const ACCOUNT_CACHE_LIMIT = 100

function windowOrder(window: string): number {
  return window === '5h' ? 0 : window === 'weekly' ? 1 : 2
}

const usageInputSchema = z.object({
  dataDir: z.string().min(1),
})

const groupsSchema = z.object({
  command: z.object({
    data: z.object({
      groups: z.array(
        z.object({
          name: z.string().min(1),
          buckets: z.array(
            z.object({
              id: z.string().optional(),
              name: z.string().optional(),
              window: z.string(),
              remaining_fraction: z.number().finite().nullish(),
              reset_time: z.string().nullish(),
            }),
          ),
        }),
      ),
    }),
  }),
})

/** Validate the CLI envelope before translating its quota groups. */
export function parseUsage(stdout: string) {
  let parsed: z.infer<typeof groupsSchema>
  try {
    parsed = groupsSchema.parse(JSON.parse(stdout))
  } catch (error) {
    throw new Error('Unexpected /usage JSON output from agy', { cause: error })
  }
  return parsed.command.data.groups.map((group) => ({
    name: group.name,
    buckets: group.buckets.map((bucket) => ({
      id: bucket.id ?? bucket.window,
      name: bucket.name ?? bucket.window,
      window: bucket.window,
      remainingFraction: bucket.remaining_fraction ?? undefined,
      resetTime: bucket.reset_time ?? '',
    })),
  }))
}

/** Read only bounded log tails, preferring the most recently modified login log while retaining cli.log. */
export async function readLoggedEmail(
  dataDir: string,
): Promise<string | undefined> {
  let names: string[] = []
  try {
    names = (await readdir(join(dataDir, 'log'))).filter((name) =>
      /^(?:cli|acp-server)-.*\.log$/.test(name),
    )
  } catch {
    /* Logs are optional. */
  }
  const recent = (
    await Promise.all(
      names.map(async (name) => {
        const path = join(dataDir, 'log', name)
        return {
          path,
          modified: await stat(path).then(
            (s) => s.mtimeMs,
            () => 0,
          ),
        }
      }),
    )
  )
    .filter((entry) => entry.modified > 0)
    .sort((a, b) => b.modified - a.modified)
    .slice(0, 10)
  const main = join(dataDir, 'cli.log')
  const mainModified = await stat(main).then(
    (s) => s.mtimeMs,
    () => 0,
  )
  if (mainModified > 0) {
    recent.push({ path: main, modified: mainModified })
    recent.sort((a, b) => b.modified - a.modified)
  }
  for (const { path } of recent) {
    try {
      const file = await open(path, 'r')
      try {
        const size = (await file.stat()).size
        const bytes = Buffer.alloc(Math.min(size, LOG_CHUNK_SIZE))
        const { bytesRead } = await file.read(
          bytes,
          0,
          bytes.length,
          size - bytes.length,
        )
        const matches = [
          ...bytes
            .toString('utf8', 0, bytesRead)
            .matchAll(
              /(?:applyAuthResult:\s*email=|OAuth:\s*authenticated successfully as\s+)([^\s,]+)/g,
            ),
        ]
        if (matches.length) return matches.at(-1)![1]
      } finally {
        await file.close()
      }
    } catch {
      /* Missing or unreadable logs do not prevent quota checks. */
    }
  }
}

async function hasLogin(dataDir: string): Promise<boolean> {
  return stat(join(dataDir, 'antigravity-oauth-token')).then(
    (s) => s.isFile() && s.size > 0,
    () => false,
  )
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

/** Share native launch/account observations with Paseo's subscription usage surface. */
export class AgyUsageSource {
  readonly tiers = new Map<string, string>()
  private launches = new Map<
    string,
    {
      launch?: ProviderLaunch
      env: Record<string, string>
      cwd?: string
      hadLogin: boolean
    }
  >()

  remember(
    launch?: ProviderLaunch,
    env: Readonly<Record<string, string>> = {},
    cwd?: string,
  ): string {
    const dir = dataDirectory(launch, env, cwd)
    const hadLogin = this.launches.get(dir)?.hadLogin ?? false
    setBounded(
      this.launches,
      dir,
      {
        launch,
        env: { ...launch?.env, ...env },
        cwd,
        hadLogin,
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
    let dirs: string[]
    if (scope.kind === 'global')
      dirs = [...this.launches.keys(), dataDirectory()]
    else {
      // shortcut: scope omits launch commands; use the latest match until Paseo exposes a launch identity.
      const observed = [...this.launches.entries()]
        .reverse()
        .find(
          ([dir, value]) =>
            dataDirectory(value.launch, scope.env, value.cwd) === dir,
        )
      dirs = [observed?.[0] ?? dataDirectory(undefined, scope.env)]
    }
    const accounts: UsageAccount[] = []
    for (const dir of new Set(dirs)) {
      let known = this.launches.get(dir)
      const loggedIn = await hasLogin(dir)
      if (!loggedIn && (!known || known.hadLogin)) continue
      if (!known) {
        this.remember(undefined, scope.kind === 'session' ? scope.env : {})
        known = this.launches.get(dir)
      }
      if (known && loggedIn) known.hadLogin = true
      accounts.push({
        key: hashAccountKey(dir),
        label: (await readLoggedEmail(dir)) ?? 'Antigravity',
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
    const known = this.launches.get(dataDir)
    const cmd = known?.launch?.command ?? 'agy'
    const rawArgs = known?.launch?.args ?? []
    const loggedIn = await hasLogin(dataDir)
    if (!loggedIn && (!known || known.hadLogin))
      throw new Error(
        'Antigravity login store no longer exists; run agy to sign in again',
      )
    if (known && loggedIn) known.hadLogin = true
    try {
      let cwd = known?.cwd
      if (
        cwd &&
        !(await stat(cwd).then(
          (s) => s.isDirectory(),
          () => false,
        ))
      ) {
        cwd = homedir()
        const launch = {
          command: cmd,
          args: rawArgs,
          env: known!.env,
        }
        if (
          dataDirectory(launch, {}, cwd) !== dataDir ||
          (!isAbsolute(launch.command) && /[/\\]/.test(launch.command))
        )
          throw new Error(
            'Remembered workspace is missing and the usage launch depends on relative paths',
          )
      }
      const { stdout } = await execCommand(
        cmd,
        [...usageArgs(rawArgs), '-p', '/usage', '--output-format', 'json'],
        {
          timeout: USAGE_FETCH_TIMEOUT_MS,
          env: known?.launch
            ? { ...known.env }
            : { ...process.env, ...known?.env },
          cwd,
        },
      )
      const windows = parseUsage(stdout).flatMap((group) =>
        group.buckets
          .toSorted((a, b) => windowOrder(a.window) - windowOrder(b.window))
          .map((bucket) => {
            const utilizationPct =
              bucket.remainingFraction === undefined
                ? null
                : Math.round(
                    Math.max(0, Math.min(1, 1 - bucket.remainingFraction)) *
                      100,
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
                shortLabel: bucket.window,
              },
              utilizationPct,
              resetsAt: bucket.resetTime || null,
              summary: true,
              tone: toneFromUsedPct(utilizationPct),
            })
          }),
      )
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
