import type {
  ProviderEvent,
  ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'
import { isNativeToolName, sessionIdPattern } from './common.ts'
import {
  readSubagents,
  watchBrain,
  type BrainWatchHandle,
  type SubagentRecord,
} from './brain.ts'

export const isSubagentMessage = (name: string) =>
  isNativeToolName(name, 'send_message')

export interface SubagentRegistryOptions {
  sessionId: string
  cwd: string
  brainRoot: string
  capabilities: readonly string[]
  emit: (event: ProviderEvent) => void
  getNativeId: () => string
  isParentTurnActive: () => boolean
  isLoading: () => boolean
  isSuppressingReplay?: () => boolean
  isUnavailable: () => boolean
  retryTimeoutMs?: number
}

/** Own child sub-session watchers, parent-child ownership, and resumption for one root session. */
export class SubagentRegistry {
  private children = new Map<string, BrainWatchHandle>()
  private childParents = new Map<string, string>()
  private killedChildren = new Set<string>()
  private resuming = new Map<string, Promise<void>>()
  private cancelingAll = false
  private generation = 0
  private opts: SubagentRegistryOptions

  constructor(opts: SubagentRegistryOptions) {
    this.opts = opts
  }

  private childKey(nativeId: string): string {
    return `${this.opts.sessionId}:child:${nativeId}`
  }

  hasChild(nativeId: string): boolean {
    return this.children.has(this.childKey(nativeId))
  }

  /** A child stopped by interruption (not killed) can be woken by a message. */
  isChildResumable(nativeId: string): boolean {
    return this.isResumableKey(this.childKey(nativeId))
  }

  private isResumableKey(childId: string): boolean {
    return (
      !this.killedChildren.has(childId) &&
      this.children.get(childId)?.isStopping() === true
    )
  }

  hasRunningChildren(): boolean {
    return (
      this.resuming.size > 0 ||
      [...this.children.values()].some((child) => child.isRunning())
    )
  }

  cancelRunning(): Promise<void>[] {
    this.generation++
    const tasks = [...this.resuming.values()]
    for (const child of this.children.values()) {
      if (child.isRunning()) tasks.push(child.cancel())
    }
    return tasks
  }

  async cancelAll(): Promise<void> {
    this.cancelingAll = true
    const seen = new Set<BrainWatchHandle>()
    try {
      while (true) {
        this.generation++
        const batch: Promise<void>[] = [...this.resuming.values()]
        for (const child of this.children.values()) {
          if (!seen.has(child)) {
            seen.add(child)
            batch.push(child.cancel())
          }
        }
        if (batch.length === 0) break
        await Promise.all(batch)
      }
    } finally {
      this.cancelingAll = false
    }
  }

  /** Synchronously stop every active child watcher when bounded drain expires. */
  forceStopAll(): void {
    this.generation++
    this.resuming.clear()
    for (const child of this.children.values()) child.forceCancel()
  }

  async waitCurrentReady(): Promise<void> {
    await Promise.all([...this.children.values()].map((child) => child.ready))
  }

  /** Wait until all currently discovered children (including nested ones opened during replay) finish their initial read. */
  async waitNestedReady(): Promise<void> {
    let known = 0
    while (known !== this.children.size) {
      known = this.children.size
      await this.waitCurrentReady()
    }
  }

  rememberSubagents = (
    subagents: readonly SubagentRecord[],
    parentNativeId = this.opts.getNativeId(),
  ): void => {
    const rootNativeId = this.opts.getNativeId()
    for (const sub of subagents) {
      if (
        sub.conversationId !== rootNativeId &&
        sub.conversationId !== parentNativeId &&
        !this.childParents.has(sub.conversationId)
      ) {
        this.childParents.set(sub.conversationId, parentNativeId)
      }
      if (sub.state === 'SUBAGENT_STATE_KILLED') {
        const childId = this.childKey(sub.conversationId)
        this.killedChildren.add(childId)
        if (!this.opts.isLoading()) void this.children.get(childId)?.cancel()
      }
    }
  }

  syncKilledChildren = async (
    subagents?: readonly SubagentRecord[],
    parentNativeId = this.opts.getNativeId(),
  ): Promise<void> => {
    this.rememberSubagents(
      subagents ??
        (await readSubagents(this.opts.brainRoot, parentNativeId).catch(
          () => [],
        )),
      parentNativeId,
    )
    // Include descendants discovered by their own watchers, not just this parent's descriptors.
    await Promise.all(
      [...this.killedChildren].map((childId) =>
        this.children.get(childId)?.cancel(),
      ),
    )
  }

  refreshSubagents = async (): Promise<void> => {
    const rootNativeId = this.opts.getNativeId()
    // Optional ownership metadata must not poison the serialized ACP notification lane.
    const subagents = await readSubagents(
      this.opts.brainRoot,
      rootNativeId,
    ).catch(() => [])
    if (this.opts.isLoading()) this.rememberSubagents(subagents)
    else await this.syncKilledChildren(subagents)
  }

  private watchChild(
    nativeId: string,
    childId: string,
    resume?: ReturnType<BrainWatchHandle['resumePoint']>,
  ) {
    const child = watchBrain(
      this.opts.brainRoot,
      nativeId,
      childId,
      (event) => {
        if (this.opts.isUnavailable()) return
        if (event.type === 'timeline.item')
          this.linkSubsession(childId, nativeId, event.item)
        this.opts.emit(event)
      },
      1000,
      this.opts.isParentTurnActive,
      resume,
      (subagents) => this.rememberSubagents(subagents, nativeId),
      this.opts.retryTimeoutMs,
    )
    this.children.set(childId, child)
    void child.ready.then(() => {
      if (this.killedChildren.has(childId)) return child.cancel()
    })
  }

  /** Resume watching the same child session once its previous watcher has drained. */
  private resumeChild(
    nativeId: string,
    childId: string,
    existing: BrainWatchHandle,
  ) {
    if (this.cancelingAll || this.resuming.has(childId)) return
    const generation = this.generation
    const task = existing
      .cancel()
      .then(() => {
        if (
          this.cancelingAll ||
          this.generation !== generation ||
          this.opts.isUnavailable() ||
          this.killedChildren.has(childId) ||
          this.children.get(childId) !== existing
        )
          return
        this.watchChild(nativeId, childId, existing.resumePoint())
      })
      .finally(() => {
        if (this.resuming.get(childId) === task) this.resuming.delete(childId)
      })
    this.resuming.set(childId, task)
  }

  canLinkChild = (
    nativeId: string,
    parentNativeId: string,
    name: string,
  ): boolean => {
    const childId = this.childKey(nativeId)
    const spawn = isNativeToolName(name, 'invoke_subagent')
    // Killed children are only linked for history, their own spawn call, or an already open session.
    const killedAllowed =
      this.opts.isLoading() ||
      spawn ||
      !this.killedChildren.has(childId) ||
      this.children.has(childId)
    // Only a recorded parent, or the spawning call of an unseen child, owns it.
    const owned =
      this.childParents.get(nativeId) === parentNativeId ||
      (spawn && !this.childParents.has(nativeId))
    return (
      !this.opts.isUnavailable() &&
      (spawn || isSubagentMessage(name)) &&
      this.opts.capabilities.includes('session.subsession') &&
      sessionIdPattern.test(nativeId) &&
      nativeId !== this.opts.getNativeId() &&
      killedAllowed &&
      owned
    )
  }

  /** Open or resume direct child sub-sessions for root or nested subagents. */
  linkSubsession(
    parentSessionId: string,
    parentNativeId: string,
    item: ProviderTimelineItem,
  ): void {
    if (this.cancelingAll || this.opts.isSuppressingReplay?.()) {
      if (item.type === 'tool_call' && item.detail.type === 'sub_agent') {
        delete item.detail.childSessionId
      }
      return
    }
    if (item.type === 'tool_call' && item.detail.type === 'sub_agent') {
      const nativeId = item.detail.childSessionId
      delete item.detail.childSessionId
      if (!nativeId || !this.canLinkChild(nativeId, parentNativeId, item.name))
        return
      this.childParents.set(nativeId, parentNativeId)
      const childId = this.childKey(nativeId)
      const existing = this.children.get(childId)
      if (
        existing &&
        this.isResumableKey(childId) &&
        !this.opts.isLoading() &&
        isSubagentMessage(item.name)
      ) {
        this.resumeChild(nativeId, childId, existing)
      } else if (!existing) {
        this.opts.emit({
          type: 'session.opened',
          sessionId: childId,
          parentSessionId,
          toolCallId: item.callId,
          cwd: this.opts.cwd,
          capabilities: ['session.subsession'],
          restoration: 'parent',
          title: item.detail.subAgentType ?? 'Antigravity subagent',
        })
        this.opts.emit({ type: 'session.ready', sessionId: childId })
        this.watchChild(nativeId, childId)
      }
      item.detail.childSessionId = childId
    }
    if (
      item.type === 'tool_call' &&
      item.status === 'completed' &&
      isNativeToolName(item.name, 'manage_subagents')
    ) {
      void this.syncKilledChildren(undefined, parentNativeId)
    }
  }

  closeAll(): void {
    this.generation++
    this.resuming.clear()
    for (const [childId, stop] of this.children) {
      stop()
      this.opts.emit({ type: 'session.closed', sessionId: childId })
    }
    this.children.clear()
  }
}
