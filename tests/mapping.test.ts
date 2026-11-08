import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, appendFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Timeline, toolDetail, configState } from '../server/mapping.ts'
import { BrainTranscriptProjector, watchBrain } from '../server/brain.ts'
import type { ProviderEvent } from '@getpaseo/plugin/server/provider'

test('stream snapshots retain identity and split across tools/turns', () => {
  const t = new Timeline('s')
  const first = t.update({
    sessionUpdate: 'agent_message_chunk',
    content: { type: 'text', text: 'a' },
  })
  const second = t.update({
    sessionUpdate: 'agent_message_chunk',
    content: { type: 'text', text: 'b' },
  })
  assert.equal(first?.id, second?.id)
  assert.ok(second?.type === 'assistant_message' && second.text === 'ab')
  t.boundary()
  assert.notEqual(
    t.update({
      sessionUpdate: 'agent_message_chunk',
      content: { type: 'text', text: 'c' },
    })?.id,
    first?.id,
  )
})

test('subagent and diff details, grouped model choices', () => {
  const id = 'b3bbd901-0d20-4a21-aa53-99f4072fb378'
  assert.deepEqual(
    toolDetail({
      title: 'invoke_subagent',
      rawInput: { Role: 'review' },
      rawOutput: `Created: {"conversationId": "${id}"}`,
    }),
    {
      type: 'sub_agent',
      subAgentType: 'review',
      description: undefined,
      childSessionId: id,
      log: `Created: {"conversationId": "${id}"}`,
    },
  )
  assert.equal(
    toolDetail({
      content: [{ type: 'diff', path: '/test', oldText: 'a', newText: 'b' }],
    }).type,
    'edit',
  )
  assert.equal(
    configState({
      configOptions: [
        {
          id: 'model',
          name: 'Model',
          type: 'select',
          currentValue: 'm',
          options: [
            { group: 'g', name: 'G', options: [{ name: 'M', value: 'm' }] },
          ],
        },
      ],
    }).models[0]?.id,
    'm',
  )
})

test('Brain handles split UTF-8 JSONL and does not repeat rows', async () => {
  const root = await mkdtemp(join(tmpdir(), 'paseo-agy-test-'))
  const id = 'b3bbd901-0d20-4a21-aa53-99f4072fb378'
  const dir = join(root, id, '.system_generated/logs')
  await mkdir(dir, { recursive: true })
  const path = join(dir, 'transcript.jsonl')
  const bytes = Buffer.from(
    JSON.stringify({
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      content: '你好',
    }) + '\n',
  )
  await appendFile(path, bytes.subarray(0, bytes.length - 4))
  const events: ProviderEvent[] = []
  const stop = watchBrain(root, id, 'child', (e) => events.push(e), 20)
  try {
    await new Promise((resolve) => setTimeout(resolve, 30))
    assert.equal(events.length, 0)
    await appendFile(path, bytes.subarray(bytes.length - 4))
    await new Promise((resolve) => setTimeout(resolve, 80))
    assert.equal(events.length, 1)
    assert.ok(
      events[0].type === 'timeline.item' &&
        events[0].item.type === 'assistant_message' &&
        events[0].item.text === '你好',
    )
    assert.throws(() => watchBrain(root, '../escape', 'child', () => {}))
    assert.equal(
      new BrainTranscriptProjector('child').project({ step_index: 'bad' })
        .length,
      0,
    )
  } finally {
    stop()
    await rm(root, { recursive: true, force: true })
  }
})

test('unfinished tools terminalize once on every terminal state', () => {
  for (const status of ['completed', 'canceled', 'failed'] as const) {
    const timeline = new Timeline('s')
    timeline.update({
      sessionUpdate: 'tool_call',
      toolCallId: 'pending',
      title: 'run_command',
      status: 'in_progress',
      rawInput: { CommandLine: 'echo test' },
    })
    const items = timeline.terminalize(status, 'crashed')
    assert.equal(items.length, 1)
    assert.ok(items[0].type === 'tool_call' && items[0].status === status)
    if (items[0].type === 'tool_call')
      assert.equal(items[0].error, status === 'failed' ? 'crashed' : null)
    assert.deepEqual(timeline.terminalize(status), [])
  }
})

test('usage rejects invalid context and ignores foreign currency', async () => {
  const { contextUsage, promptUsage } = await import('../server/mapping.ts')
  assert.equal(contextUsage({ size: 0, used: 10 }), undefined)
  assert.equal(contextUsage({ size: 100, used: -1 }), undefined)
  assert.equal(contextUsage({ size: Infinity, used: 1 }), undefined)
  assert.deepEqual(
    contextUsage({ size: 100, used: 10, cost: { amount: 3, currency: 'EUR' } }),
    { contextWindowUsedTokens: 10, contextWindowMaxTokens: 100 },
  )
  assert.deepEqual(
    promptUsage({
      totalTokens: 6,
      inputTokens: 4,
      outputTokens: 2,
      cachedReadTokens: 3,
    }),
    {
      inputTokens: 4,
      outputTokens: 2,
      cachedInputTokens: 3,
      contextWindowUsedTokens: 9,
      contextWindowMaxTokens: 256_000,
    },
  )
})

test('model metadata restricts effort variants and hides fixed-effort selectors', () => {
  const state = configState({
    configOptions: [
      {
        type: 'select',
        id: 'model',
        name: 'Model',
        currentValue: 'gpt-oss-120b-medium',
        options: [
          { value: 'gemini', name: 'Gemini', description: 'Model description' },
          { value: 'gpt-oss-120b-medium', name: 'GPT-OSS' },
        ],
      },
      {
        type: 'select',
        id: 'thought_level',
        name: 'Thinking',
        currentValue: 'medium',
        options: ['low', 'medium', 'high'].map((value) => ({
          value,
          name: value,
        })),
      },
    ],
    models: {
      availableModels: [
        { modelId: 'gemini-low' },
        { modelId: 'gemini-high' },
        { modelId: 'gpt-oss-120b-medium' },
      ],
    },
  })
  assert.deepEqual(
    state.models[0].thinkingOptions?.map((o) => o.id),
    ['low', 'high'],
  )
  assert.equal(state.models[0].contextWindowMaxTokens, 256000)
  assert.equal(state.models[0].description, 'Model description')
  assert.equal(state.models[1].contextWindowMaxTokens, 131072)
  assert.deepEqual(state.thinkingOptions, [])
  assert.equal(state.thinkingOption, undefined)
  assert.deepEqual(state.modes, [
    {
      id: 'full-access',
      label: 'Full Access',
      description:
        'Approve file edits, commands and tool requests without asking.',
      icon: 'ShieldOff',
      colorTier: 'dangerous',
      isUnattended: true,
    },
  ])
})

test('attachments preserve file paths, issue context and review comments', async () => {
  const { promptContent } = await import('../server/mapping.ts')
  const file = promptContent({
    type: 'uploaded_file',
    id: 'f',
    fileName: 'file.txt',
    mimeType: 'text/plain',
    size: 3,
    path: '/tmp/file.txt',
  })
  assert.ok(file.type === 'text' && file.text.includes('/tmp/file.txt'))
  const issue = promptContent({
    type: 'github_issue',
    mimeType: 'application/github-issue',
    number: 12,
    title: 'Bug',
    body: 'Details',
    url: 'https://example.com/12',
  })
  assert.ok(
    issue.type === 'text' &&
      issue.text.includes('Details') &&
      issue.text.includes('#12'),
  )
  const review = promptContent({
    type: 'review',
    mimeType: 'application/paseo-review',
    cwd: '/repo',
    mode: 'uncommitted',
    comments: [
      {
        filePath: 'test.ts',
        side: 'new',
        lineNumber: 3,
        body: 'Fix here',
        context: {
          hunkHeader: '@@ -1 +1 @@',
          targetLine: {
            oldLineNumber: null,
            newLineNumber: 3,
            type: 'add',
            content: 'code',
          },
          lines: [],
        },
      },
    ],
  })
  assert.ok(
    review.type === 'text' &&
      review.text.includes('test.ts:3') &&
      review.text.includes('Fix here'),
  )
})

test('Brain tool cards decode JSON-encoded native arguments', () => {
  const items = new BrainTranscriptProjector('child').project({
    step_index: 2,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ name: 'run_command', args: { CommandLine: '"echo test"' } }],
  })
  assert.ok(
    items[0].type === 'tool_call' &&
      items[0].status === 'running' &&
      items[0].detail.type === 'shell' &&
      items[0].detail.command === 'echo test',
  )
})

test('native PascalCase tools retain paths, content, ranges and recipients', () => {
  assert.deepEqual(
    toolDetail({
      title: 'read_file',
      rawInput: { Path: '/a', StartLine: 3, EndLine: 7 },
    }),
    {
      type: 'read',
      filePath: '/a',
      content: undefined,
      offset: 3,
      limit: 5,
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'write_file',
      rawInput: { Path: '/a', Content: 'new' },
    }),
    {
      type: 'write',
      filePath: '/a',
      content: 'new',
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'edit_file',
      rawInput: { Path: '/a', Target: 'old', Replacement: 'new' },
    }),
    {
      type: 'edit',
      filePath: '/a',
      oldString: 'old',
      newString: 'new',
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'read_url_content',
      rawInput: { Url: 'https://example.com' },
      rawOutput: 'page',
    }),
    {
      type: 'fetch',
      url: 'https://example.com',
      result: 'page',
    },
  )
  const recipient = '22222222-2222-4222-8222-222222222222'
  assert.deepEqual(
    toolDetail({
      title: 'send_message',
      rawInput: { Recipient: recipient, Message: 'Continue' },
    }),
    {
      type: 'sub_agent',
      subAgentType: recipient,
      description: 'Continue',
      childSessionId: recipient,
      log: '',
    },
  )
  const invalid = toolDetail({
    title: 'send_message',
    rawInput: { Recipient: '../outside' },
  })
  assert.ok(
    invalid.type === 'sub_agent' && invalid.childSessionId === undefined,
  )
  const read = toolDetail({
    title: 'view_file',
    rawInput: { AbsolutePath: '/a', ContentOffset: 8 },
  })
  assert.ok(
    read.type === 'read' && read.offset === 8 && read.limit === undefined,
  )
})

test('Brain matches FIFO tool results without fake assistant bubbles', () => {
  const projector = new BrainTranscriptProjector('child')
  const started = projector.project({
    type: 'PLANNER_RESPONSE',
    source: 'MODEL',
    step_index: 1,
    thinking: 'Thinking',
    content: 'Checking',
    tool_calls: [
      { id: 'one', name: 'run_command', args: { CommandLine: '"echo hello"' } },
      { id: 'two', name: 'read_file', args: { Path: '"/missing"' } },
    ],
  })
  assert.deepEqual(
    started.map((i) => i.type),
    ['reasoning', 'assistant_message', 'tool_call', 'tool_call'],
  )
  assert.ok(started[2].type === 'tool_call' && started[2].status === 'running')
  for (const type of ['CHECKPOINT', 'SYSTEM_MESSAGE', 'UNKNOWN'])
    assert.deepEqual(
      projector.project({
        type,
        step_index: 2,
        content: 'metadata',
        thinking: 'hidden',
      }),
      [],
    )
  const done = projector.project({
    type: 'GENERIC',
    source: 'MODEL',
    step_index: 3,
    status: 'DONE',
    content: 'Created At: today\nhello',
  })
  assert.equal(done.length, 1)
  assert.ok(
    done[0].type === 'tool_call' &&
      done[0].status === 'completed' &&
      done[0].detail.type === 'shell',
  )
  assert.equal(done[0].id, started[2].id)
  assert.equal(done[0].callId, started[2].callId)
  assert.equal(done[0].detail.output, 'Created At: today\nhello')
  const failed = projector.project({
    type: 'GENERIC',
    source: 'MODEL',
    step_index: 4,
    status: 'ERROR',
    error: { message: 'not found' },
  })
  assert.ok(
    failed[0].type === 'tool_call' &&
      failed[0].status === 'failed' &&
      failed[0].error,
  )
  assert.equal(failed[0].id, started[3].id)
  assert.ok(
    failed[0].detail.type === 'read' &&
      failed[0].detail.content?.includes('not found'),
  )
  assert.deepEqual(
    projector.project({
      type: 'GENERIC',
      step_index: 5,
      content: 'orphan result',
    }),
    [],
  )
  assert.deepEqual(
    projector.project({
      type: 'ERROR_MESSAGE',
      source: 'SYSTEM',
      step_index: 6,
      content: 'Model error',
    }),
    [{ type: 'error', id: 'child:step:6:error', message: 'Model error' }],
  )
  assert.equal(
    projector.project({
      type: 'USER_INPUT',
      step_index: 7,
      content: 'hello',
    })[0].type,
    'user_message',
  )
})

test('prompt usage normalizes model steps, counts caches and preserves native context', async () => {
  const { promptUsage, contextWindowForModel } =
    await import('../server/mapping.ts')
  const usage = {
    inputTokens: 120,
    cachedReadTokens: 40,
    cachedWriteTokens: 20,
    outputTokens: 9,
    totalTokens: 189,
  }
  assert.deepEqual(promptUsage(usage, { model: 'gpt-oss-120b', steps: 3 }), {
    inputTokens: 120,
    outputTokens: 9,
    cachedInputTokens: 40,
    contextWindowUsedTokens: 69,
    contextWindowMaxTokens: 131_072,
  })
  assert.deepEqual(promptUsage(usage, { preserveContext: true }), {
    inputTokens: 120,
    outputTokens: 9,
    cachedInputTokens: 40,
  })
  assert.equal(
    promptUsage(
      { inputTokens: -1, outputTokens: NaN, totalTokens: 30 },
      { steps: 3 },
    ).contextWindowUsedTokens,
    10,
  )
  assert.equal(promptUsage(usage, { steps: 0 }).contextWindowUsedTokens, 189)
  assert.deepEqual(promptUsage(null), {})
  assert.equal(contextWindowForModel('gemini'), 256_000)
})
