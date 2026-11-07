import { open, realpath } from 'node:fs/promises'
import { join, sep } from 'node:path'
import { record, toolDetail, sessionIdPattern } from './mapping.ts'
import type {
  ProviderEvent,
  ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'

type PendingTool = {
  callId: string
  name: string
  args: Record<string, unknown>
}

/** Match positional GENERIC results to preceding tool requests across transcript rows. */
export class BrainTranscriptProjector {
  private pending: PendingTool[] = []
  private sessionId: string
  constructor(sessionId: string) {
    this.sessionId = sessionId
  }

  project(value: unknown): ProviderTimelineItem[] {
    const row = record(value)
    if (!Number.isInteger(row.step_index)) return []
    const id = `${this.sessionId}:step:${row.step_index}`
    const text = typeof row.content === 'string' ? row.content : ''
    switch (row.type) {
      case 'USER_INPUT':
        return text ? [{ type: 'user_message', id, text }] : []
      case 'SYSTEM_MESSAGE':
      case 'CHECKPOINT':
        return []
      case 'ERROR_MESSAGE':
        return [
          {
            type: 'error',
            id: `${id}:error`,
            message:
              text ||
              (typeof row.error === 'string' ? row.error : 'Subagent error'),
          },
        ]
      case 'GENERIC': {
        const pending = this.pending.shift()
        if (!pending) return []
        const output = row.content ?? row.error ?? null
        const base = {
          type: 'tool_call' as const,
          id: pending.callId,
          callId: pending.callId,
          name: pending.name,
          detail: toolDetail({
            title: pending.name,
            rawInput: pending.args,
            rawOutput: output,
          }),
        }
        return [
          row.status === 'ERROR'
            ? {
                ...base,
                status: 'failed',
                error: { message: text || 'Tool execution failed' },
              }
            : { ...base, status: 'completed', error: null },
        ]
      }
      case 'PLANNER_RESPONSE': {
        const items: ProviderTimelineItem[] = []
        if (typeof row.thinking === 'string' && row.thinking)
          items.push({
            type: 'reasoning',
            id: `${id}:thought`,
            text: row.thinking,
          })
        if (text) items.push({ type: 'assistant_message', id, text })
        if (Array.isArray(row.tool_calls))
          row.tool_calls.forEach((value, index) => {
            const tool = record(value)
            const args = Object.fromEntries(
              Object.entries(record(tool.args)).map(([key, value]) => {
                if (typeof value === 'string') {
                  try {
                    return [key, JSON.parse(value)]
                  } catch {
                    /* Native args may be plain strings. */
                  }
                }
                return [key, value]
              }),
            )
            const pending = {
              callId: `${id}:tool:${typeof tool.id === 'string' ? tool.id : index}`,
              name: String(tool.name ?? 'Tool'),
              args,
            }
            this.pending.push(pending)
            items.push({
              type: 'tool_call',
              id: pending.callId,
              callId: pending.callId,
              name: pending.name,
              detail: toolDetail({
                title: pending.name,
                rawInput: args,
                rawOutput: null,
              }),
              status: 'running',
              error: null,
            })
          })
        return items
      }
      default:
        return []
    }
  }
}

/** Tail complete JSONL records; the returned cleanup stops future emissions. */
// shortcut: JSONL is polled every second; use filesystem notifications if many simultaneous child sessions make polling costly.
export function watchBrain(
  root: string,
  nativeId: string,
  sessionId: string,
  emit: (event: ProviderEvent) => void,
  pollIntervalMs = 1000,
) {
  if (!sessionIdPattern.test(nativeId))
    throw new Error('Invalid Brain session ID')
  let projector = new BrainTranscriptProjector(sessionId)
  let offset = 0
  let remainder = ''
  let stopped = false
  let busy = false
  let warned = false
  async function poll() {
    if (stopped || busy) return
    busy = true
    try {
      const base = await realpath(root)
      const path = await realpath(
        join(base, nativeId, '.system_generated/logs/transcript.jsonl'),
      )
      if (!path.startsWith(base + sep))
        throw new Error('Brain transcript escapes its directory')
      const file = await open(path, 'r')
      try {
        const stat = await file.stat()
        if (stat.size < offset) {
          offset = 0
          remainder = ''
          projector = new BrainTranscriptProjector(sessionId)
        }
        const buffer = Buffer.alloc(Math.min(stat.size - offset, 1024 * 1024))
        if (!buffer.length) return
        const { bytesRead } = await file.read(buffer, 0, buffer.length, offset)
        offset += bytesRead
        // Preserve UTF-8 bytes across chunk boundaries along with incomplete JSON lines.
        remainder += buffer.subarray(0, bytesRead).toString('latin1')
        const lines = remainder.split('\n')
        remainder = lines.pop()!
        if (remainder.length > 8 * 1024 * 1024)
          throw new Error('Brain JSONL line exceeds 8 MiB')
        for (const line of lines) {
          if (!line.trim() || stopped) continue
          const row = JSON.parse(Buffer.from(line, 'latin1').toString('utf8'))
          for (const item of projector.project(row))
            emit({
              type: 'timeline.item',
              sessionId,
              item,
              timestamp:
                typeof row.created_at === 'string' ? row.created_at : undefined,
            })
        }
      } finally {
        await file.close()
      }
    } catch (error) {
      if (
        (error as NodeJS.ErrnoException).code !== 'ENOENT' &&
        !warned &&
        !stopped
      ) {
        warned = true
        emit({
          type: 'timeline.item',
          sessionId,
          item: {
            type: 'error',
            id: `${sessionId}:brain-error`,
            message: `Cannot read child transcript: ${String(error)}`,
          },
        })
      }
    } finally {
      busy = false
    }
  }
  const timer = setInterval(() => {
    void poll()
  }, pollIntervalMs)
  timer.unref()
  void poll()
  return () => {
    stopped = true
    clearInterval(timer)
  }
}
