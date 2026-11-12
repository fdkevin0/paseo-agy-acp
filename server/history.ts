import { isRestartNotice, isRetryWarning } from './brain-io.ts'
import {
  shiftPendingPastCheckpoint,
  takePendingForGeneric,
  type NativeToolIdentitySource,
} from './brain-projector.ts'
import { cleanToolName, isAskQuestionName, record } from './common.ts'
import type { ProviderTimelineItem } from '@getpaseo/plugin/server/provider'

export interface HistoryEntry {
  item: ProviderTimelineItem
  timestamp?: string
  resultProvided?: boolean
  nativeToolIdentitySource?: NativeToolIdentitySource
  backgroundGeneric?: boolean
}

const userRequestPattern =
  /<USER_REQUEST>\r?\n([\s\S]*)\r?\n<\/USER_REQUEST>(?:\r?\n<ADDITIONAL_METADATA>[\s\S]*<\/ADDITIONAL_METADATA>)?\s*$/

function normalizeUserText(text: string): string {
  return text.match(userRequestPattern)?.[1] ?? text
}

function stableJson(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${stableJson(v)}`).join(',')}}`
}

/** Align Brain transcript tool calls with ACP replay tool call IDs and verify matching inputs. */
export function alignReplayTools(
  rows: readonly Record<string, unknown>[],
  stepCallIds: ReadonlyMap<number, string>,
  subagents: readonly { spawnStepIndex: number; spawnCallId?: string }[],
  acpToolCallIds: readonly string[],
  acpToolSnapshots: ReadonlyMap<string, { rawInput?: unknown }>,
): {
  alignedIds: readonly (string | undefined)[]
  verifiedAcpToolIds: ReadonlySet<string>
} {
  const claimedIds = new Set<string>(stepCallIds.values())
  for (const sub of subagents)
    if (sub.spawnCallId) claimedIds.add(sub.spawnCallId)
  type Slot = {
    slot: number
    stepIndex: number
    args: unknown
    isQuestion: boolean
    invalid: boolean
  }
  const slots: Slot[] = []
  let pending: Slot[] = []
  let firstStepIndex: number | undefined
  for (const raw of rows) {
    const row = record(raw)
    if (!Number.isInteger(row.step_index)) continue
    const stepIndex = Number(row.step_index)
    firstStepIndex ??= stepIndex
    switch (row.type) {
      case 'USER_INPUT':
        pending = []
        break
      case 'SYSTEM_MESSAGE':
        if (isRestartNotice(row)) pending = []
        break
      case 'ERROR_MESSAGE':
        if (!isRetryWarning(row)) pending = []
        break
      case 'CHECKPOINT':
        shiftPendingPastCheckpoint(pending, stepIndex)
        break
      case 'GENERIC': {
        const matched = takePendingForGeneric(pending, stepIndex)
        if (matched && row.status === 'INVALID') matched.invalid = true
        break
      }
      case 'PLANNER_RESPONSE':
        pending = []
        if (Array.isArray(row.tool_calls))
          row.tool_calls.forEach((value, index) => {
            const tool = record(value)
            if (typeof tool.id === 'string' && tool.id) claimedIds.add(tool.id)
            const cleanName = cleanToolName(String(tool.name ?? 'Tool'))
            const s: Slot = {
              slot: slots.length,
              stepIndex: stepIndex + index + 1,
              args: tool.args,
              isQuestion: isAskQuestionName(cleanName),
              invalid: false,
            }
            slots.push(s)
            pending.push(s)
          })
        break
    }
  }
  const valid = slots.filter((s) => !s.invalid)
  const M = valid.length
  const N = acpToolCallIds.length
  const inputMatches = (args: unknown, callId: string) => {
    const snap = acpToolSnapshots.get(callId)
    return (
      snap?.rawInput != null && stableJson(args) === stableJson(snap.rawInput)
    )
  }
  let vOffset = 0
  let aOffset = 0
  let count = 0
  if (M > 0 && M === N) {
    count = M
  } else if (M > 0 && M < N && (firstStepIndex ?? 0) > 0) {
    const matches: number[] = []
    for (let k = 0; k <= N - M; k++) {
      let ok = true
      for (let j = 0; j < M; j++) {
        if (!inputMatches(valid[j].args, acpToolCallIds[k + j])) {
          ok = false
          break
        }
      }
      if (ok) matches.push(k)
    }
    // Forked transcripts start at a non-zero step and retain a suffix of the parent trajectory (N - M);
    // verifiedAcpToolIds still requires per-slot inputMatches below.
    aOffset = matches.length === 1 ? matches[0] : N - M
    count = M
  } else if (M > N && N > 0) {
    const matches: number[] = []
    for (let k = 0; k <= M - N; k++) {
      let ok = true
      for (let j = 0; j < N; j++) {
        if (!inputMatches(valid[k + j].args, acpToolCallIds[j])) {
          ok = false
          break
        }
      }
      if (ok) matches.push(k)
    }
    if (matches.length === 1) {
      vOffset = matches[0]
      count = N
    }
  }
  if (count === 0)
    return { alignedIds: acpToolCallIds, verifiedAcpToolIds: new Set() }
  const alignedIds = new Array<string | undefined>(slots.length).fill(undefined)
  const verifiedAcpToolIds = new Set<string>()
  for (let j = 0; j < count; j++) {
    const s = valid[vOffset + j]
    const callId = acpToolCallIds[aOffset + j]
    alignedIds[s.slot] = callId
    if (!claimedIds.has(callId) && inputMatches(s.args, callId))
      verifiedAcpToolIds.add(callId)
  }
  return { alignedIds, verifiedAcpToolIds }
}

/** Place checkpoints only in gaps proved by native call IDs or unique complete text. */
export function mergeCompactionHistory(
  acp: readonly HistoryEntry[],
  brain: readonly HistoryEntry[],
): HistoryEntry[] {
  if (!brain.some(({ item }) => item.type === 'compaction'))
    return [
      ...acp,
      ...brain.filter(
        ({ item }) => item.type === 'tool_call' && item.status !== 'running',
      ),
    ]
  const textIds = new Map<string, number>()
  for (const { item } of acp)
    if ('text' in item) textIds.set(item.id, (textIds.get(item.id) ?? 0) + 1)
  function key(
    { item, nativeToolIdentitySource, backgroundGeneric }: HistoryEntry,
    source: 'acp' | 'brain',
  ): string | undefined {
    if (item.type === 'tool_call')
      return source === 'acp' || nativeToolIdentitySource !== undefined
        ? JSON.stringify([
            'tool',
            item.callId,
            item.status === 'running' && !backgroundGeneric
              ? 'running'
              : 'terminal',
          ])
        : undefined
    if (
      item.type === 'user_message' ||
      item.type === 'assistant_message' ||
      item.type === 'reasoning'
    ) {
      const text =
        item.type === 'user_message' ? normalizeUserText(item.text) : item.text
      return text ? JSON.stringify([item.type, text]) : undefined
    }
    return undefined
  }
  function positions(
    entries: readonly HistoryEntry[],
    source: 'acp' | 'brain',
  ) {
    const result = new Map<string, number | null>()
    entries.forEach((entry, index) => {
      const { item } = entry
      const identity = key(entry, source)
      if (identity === undefined) return
      if (source === 'acp' && 'text' in item && (textIds.get(item.id) ?? 0) > 1)
        result.set(identity, null)
      else result.set(identity, result.has(identity) ? null : index)
    })
    return result
  }
  const acpPositions = positions(acp, 'acp')
  const brainPositions = positions(brain, 'brain')
  const candidates = brain.flatMap((entry, index) => {
    const { item } = entry
    const identity = key(entry, 'brain')
    const position =
      identity === undefined ? undefined : acpPositions.get(identity)
    return position != null && brainPositions.get(identity!) === index
      ? [{ brain: index, acp: position, tool: item.type === 'tool_call' }]
      : []
  })
  const ordered = (anchors: typeof candidates) =>
    anchors.every((anchor, i) => i === 0 || anchors[i - 1].acp < anchor.acp)
  const tools = candidates.filter((anchor) => anchor.tool)
  // Text cannot override conflicting native call IDs. Conflicting call IDs prove no gap.
  const anchors = ordered(candidates) ? candidates : ordered(tools) ? tools : []
  const slots = new Map<number, HistoryEntry[]>()
  const trailing: HistoryEntry[] = []
  const completed = new Set<string>()
  const acpTools = new Map(
    acp.flatMap(({ item, resultProvided }, index) =>
      item.type === 'tool_call'
        ? [[item.callId, { item, index, resultProvided }] as const]
        : [],
    ),
  )
  let cursor = 0
  let localCursor = -1
  let localAnchors: typeof candidates = []
  const insert = (index: number, entry: HistoryEntry) => {
    const entries = slots.get(index) ?? []
    entries.push(entry)
    slots.set(index, entries)
  }
  brain.forEach((entry, index) => {
    while (cursor < anchors.length && anchors[cursor].brain < index) cursor++
    let left = cursor > 0 ? anchors[cursor - 1].acp + 1 : 0
    let right = cursor < anchors.length ? anchors[cursor].acp : acp.length
    if (left < right && cursor > 0 && cursor < anchors.length) {
      if (localCursor !== cursor) {
        localCursor = cursor
        const bLo = anchors[cursor - 1].brain + 1
        const bHi = anchors[cursor].brain
        const localAcp = positions(acp.slice(left, right), 'acp')
        const localBrain = positions(brain.slice(bLo, bHi), 'brain')
        const found: typeof candidates = []
        for (let i = 0; i < bHi - bLo; i++) {
          const sub = brain[bLo + i]
          const id = key(sub, 'brain')
          const pos = id === undefined ? undefined : localAcp.get(id)
          if (pos != null && localBrain.get(id!) === i)
            found.push({
              brain: bLo + i,
              acp: left + pos,
              tool: sub.item.type === 'tool_call',
            })
        }
        localAnchors =
          ordered(found) && found.length === right - left ? found : []
      }
      for (const a of localAnchors) {
        if (a.brain < index) left = a.acp + 1
        else if (a.brain > index) {
          right = a.acp
          break
        }
      }
    }
    const { item } = entry
    if (item.type === 'compaction') {
      if (completed.has(item.id)) return
      completed.add(item.id)
      if (anchors.length > 0 && left === right) insert(left, entry)
      else
        trailing.push({
          item: {
            id: `${item.id}:unlocated`,
            type: 'notification',
            level: 'info',
            message:
              'History contains a context compaction whose exact position is unknown.',
          },
        })
    } else if (item.type === 'tool_call' && item.status !== 'running') {
      const existing = acpTools.get(item.callId)
      if (existing) {
        if (existing.item.status === 'running')
          insert(existing.index + 1, entry)
        else if (!existing.resultProvided)
          insert(existing.index + 1, {
            ...entry,
            item: { ...existing.item, detail: item.detail },
          })
        else if (
          existing.item.detail.type === 'sub_agent' &&
          item.detail.type === 'sub_agent' &&
          !existing.item.detail.childSessionId &&
          item.detail.childSessionId
        )
          insert(existing.index + 1, {
            item: {
              ...existing.item,
              detail: {
                ...existing.item.detail,
                childSessionId: item.detail.childSessionId,
              },
            },
          })
        return
      }
      // Retain Brain-only tool recovery even when ACP provides no placement anchor.
      if (left === right) insert(left, entry)
      else trailing.push(entry)
    }
  })
  return acp
    .flatMap((entry, index) => [...(slots.get(index) ?? []), entry])
    .concat(slots.get(acp.length) ?? [], trailing)
}
