import type { ProviderTimelineItem } from '@getpaseo/plugin/server/provider'
import {
  TranscriptReader,
  StepDedupe,
  countRowMessages,
  extractStepUsage,
  isMissing,
} from './brain-io.ts'
import type { StepUsage } from './mapping.ts'

type Row = Record<string, unknown>
export type BrainRead = Awaited<ReturnType<TranscriptReader['read']>>

/** Numeric observations are independent of text/tool dedupe and never accumulate consumption. */
export class ModelUsageObserver {
  private candidate?: { stepIndex: number; usage: StepUsage }
  private published?: { stepIndex: number; signature: string }
  private replacing = false
  latest?: StepUsage
  latestStepIndex = -1
  private minStepIndex: number
  private publish: (usage: StepUsage) => boolean | void
  constructor(
    minStepIndex: number,
    publish: (usage: StepUsage) => boolean | void,
  ) {
    this.minStepIndex = minStepIndex
    this.publish = publish
  }
  observe({ rows, reset, snapshotStepIndices }: BrainRead) {
    if (reset) {
      this.candidate = undefined
      this.replacing = true
    }
    for (const row of rows) {
      const stepIndex = Number(row.step_index)
      if (
        !Number.isInteger(stepIndex) ||
        stepIndex <= this.minStepIndex ||
        stepIndex < (this.candidate?.stepIndex ?? -1)
      )
        continue
      const usage = extractStepUsage(row)
      if (usage) this.candidate = { stepIndex, usage }
    }
    if (snapshotStepIndices) {
      this.replacing = false
      if (!this.candidate) {
        this.latest = undefined
        this.latestStepIndex = -1
        this.published = undefined
      }
    }
    const candidate = this.candidate
    if (
      !candidate ||
      (this.replacing && candidate.stepIndex < this.latestStepIndex)
    )
      return
    this.latest = candidate.usage
    this.latestStepIndex = candidate.stepIndex
    const signature = JSON.stringify(candidate.usage)
    if (
      signature !== this.published?.signature &&
      this.publish(candidate.usage) !== false
    )
      this.published = { stepIndex: candidate.stepIndex, signature }
  }
}

/** One turn-scoped byte cursor. The pump never performs settlement or question actions. */
export class BrainTurnObservation {
  readonly reader: TranscriptReader
  readonly usage: ModelUsageObserver
  phase: 'foreground' | 'settling' | 'continuation' | 'draining' | 'disposed' =
    'foreground'
  wokeUp = false
  missing = false
  private readError?: unknown
  private failure?: unknown
  get error(): unknown {
    return this.failure ?? this.readError
  }
  lastStepIndex = -1
  historicalMessages = 0
  revision = 0
  private waiters = new Set<() => void>()
  private notify() {
    this.revision++
    for (const wake of this.waiters) wake()
    this.waiters.clear()
  }
  wait(after: number, signal?: AbortSignal): Promise<void> {
    if (this.revision !== after || signal?.aborted || this.phase === 'disposed')
      return Promise.resolve()
    return new Promise((resolve) => {
      const wake = () => {
        this.waiters.delete(wake)
        signal?.removeEventListener('abort', wake)
        resolve()
      }
      this.waiters.add(wake)
      signal?.addEventListener('abort', wake, { once: true })
    })
  }
  private timer?: ReturnType<typeof setInterval>
  private inflight?: Promise<void>
  private consumer?: (read: BrainRead) => Promise<void>
  private buffered = new Map<number, Row>()
  private bufferReset = false
  private bufferSnapshot?: Set<number>
  private bufferMtimeMs?: number
  private checkpoints = new StepDedupe()
  private drainingRead = false
  private mayPublish() {
    return (
      this.callbacks.isCurrent() &&
      (this.phase !== 'draining' || this.drainingRead) &&
      !this.isDisposed()
    )
  }
  readonly root: string
  readonly nativeId: string
  readonly sessionId: string
  readonly minStepIndex: number
  readonly pollIntervalMs: number
  private callbacks: {
    isCurrent: () => boolean
    onUsage: (usage: StepUsage) => boolean | void
    onItem: (item: ProviderTimelineItem, timestamp?: string) => void
    onBoundary?: (index: number, reconciled: boolean) => void
  }
  acceptsRows() {
    return this.phase !== 'draining' && this.phase !== 'disposed'
  }
  isDisposed() {
    return this.phase === 'disposed'
  }
  constructor(
    root: string,
    nativeId: string,
    sessionId: string,
    minStepIndex: number,
    callbacks: BrainTurnObservation['callbacks'],
    pollIntervalMs = 200,
    reader = new TranscriptReader(),
  ) {
    this.reader = reader
    this.root = root
    this.nativeId = nativeId
    this.sessionId = sessionId
    this.minStepIndex = minStepIndex
    this.callbacks = callbacks
    this.pollIntervalMs = pollIntervalMs
    this.usage = new ModelUsageObserver(minStepIndex, (usage) =>
      this.mayPublish() ? callbacks.onUsage(usage) : false,
    )
  }
  start() {
    this.timer = setInterval(() => {
      void this.poll().catch(() => undefined)
    }, this.pollIntervalMs)
    this.timer.unref()
    void this.poll().catch(() => undefined)
  }
  /** Calls share a pass in flight; no concurrent reads or folds. */
  poll(): Promise<void> {
    if (this.phase === 'disposed') return Promise.resolve()
    if (this.failure !== undefined) return Promise.reject(this.failure)
    this.inflight ??= this.read()
      .catch((error) => {
        if (error !== this.readError) this.failure = error
        throw error
      })
      .finally(() => {
        this.inflight = undefined
        this.notify()
      })
    return this.inflight
  }
  private async read() {
    let read: BrainRead
    try {
      read = await this.reader.read(this.root, this.nativeId)
      this.missing = false
    } catch (error) {
      if (!isMissing(error)) {
        // Only reader I/O can recover; a partially applied fold/callback cannot be replayed safely.
        if (
          error &&
          typeof error === 'object' &&
          'code' in error &&
          typeof error.code === 'string'
        )
          this.readError = error
        throw error
      }
      this.missing = true
      read = { rows: [], reset: false }
    }
    this.readError = undefined
    if (this.isDisposed()) return
    if (read.reset) {
      this.buffered.clear()
      this.bufferReset = true
      this.bufferSnapshot = undefined
      this.bufferMtimeMs = undefined
      this.lastStepIndex = -1
      this.historicalMessages = 0
    }
    for (const row of read.rows) {
      const index = Number(row.step_index)
      if (Number.isInteger(index))
        this.lastStepIndex = Math.max(this.lastStepIndex, index)
      if (index <= this.minStepIndex)
        this.historicalMessages += countRowMessages(row, false)
    }
    if (this.mayPublish())
      this.callbacks.onBoundary?.(
        this.lastStepIndex,
        Boolean(read.snapshotStepIndices),
      )
    const eligible = read.rows.filter(
      (row) => Number(row.step_index) > this.minStepIndex,
    )
    const next = { ...read, rows: eligible }
    if (this.consumer && this.acceptsRows() && this.callbacks.isCurrent())
      await this.consumer(next)
    else {
      for (const row of eligible) this.buffered.set(Number(row.step_index), row)
      if (
        eligible.length > 0 &&
        read.mtimeMs !== undefined &&
        (read.reset || this.bufferMtimeMs === undefined)
      )
        this.bufferMtimeMs = read.mtimeMs
      if (read.snapshotStepIndices)
        this.bufferSnapshot = new Set(read.snapshotStepIndices)
      if (this.bufferSnapshot)
        for (const row of read.rows)
          this.bufferSnapshot.add(Number(row.step_index))
    }
    if (this.isDisposed()) return
    const observed = this.drainingRead
      ? {
          rows: [...this.buffered.values()].sort(
            (a, b) => Number(a.step_index) - Number(b.step_index),
          ),
          reset: this.bufferReset,
          snapshotStepIndices: this.bufferSnapshot,
        }
      : next
    this.usage.observe(observed)
    if (read.snapshotStepIndices)
      this.checkpoints.prune(read.snapshotStepIndices)
    if (!this.mayPublish()) return
    if (this.drainingRead)
      this.callbacks.onBoundary?.(
        this.lastStepIndex,
        Boolean(this.bufferSnapshot),
      )
    // Completed checkpoints have immutable identities, even when their summaries are rewritten.
    for (const row of observed.rows)
      if (
        row.type === 'CHECKPOINT' &&
        row.status === 'DONE' &&
        this.checkpoints.fresh(row)
      )
        this.callbacks.onItem(
          {
            type: 'compaction',
            id: `${this.sessionId}:step:${row.step_index}:compaction`,
            status: 'completed',
          },
          typeof row.created_at === 'string' ? row.created_at : undefined,
        )
  }
  async attach(consumer: (read: BrainRead) => Promise<void>) {
    await this.inflight
    this.consumer = consumer
    this.inflight = (async () => {
      const rows = [...this.buffered.values()].sort(
        (a, b) => Number(a.step_index) - Number(b.step_index),
      )
      this.buffered.clear()
      await consumer({
        rows,
        reset: this.bufferReset,
        snapshotStepIndices: this.bufferSnapshot,
        mtimeMs: this.bufferMtimeMs,
      })
      this.bufferReset = false
      this.bufferSnapshot = undefined
      this.bufferMtimeMs = undefined
    })()
      .catch((error) => {
        this.failure = error
        throw error
      })
      .finally(() => {
        this.inflight = undefined
        this.notify()
      })
    await this.inflight
  }
  /** Stop actions immediately; final drain happens only after the driver's observable barriers. */
  stop() {
    clearInterval(this.timer)
    this.phase = 'draining'
    this.consumer = undefined
  }
  async drain() {
    this.stop()
    await this.inflight?.catch(() => undefined)
    this.drainingRead = true
    try {
      await this.poll()
    } finally {
      this.drainingRead = false
      this.buffered.clear()
    }
  }
  async dispose() {
    this.stop()
    this.phase = 'disposed'
    this.notify()
    await this.inflight?.catch(() => undefined)
    this.buffered.clear()
  }
}
