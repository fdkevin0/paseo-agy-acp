import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  buildElicitationResponse,
  formatQuestionAnswersText,
  isPreapprovedMcpTool,
  normalizeAskQuestionInput,
  normalizeElicitationQuestions,
  permissionKind,
  pickPermissionOption,
  sameNormalizedQuestions,
  toQuestionPermissionInput,
} from '../server/permissions.ts'

const options = [
  { optionId: 'always', kind: 'allow_always' },
  { optionId: 'yes', kind: 'allow_once' },
  { optionId: 'never', kind: 'reject_always' },
  { optionId: 'no', kind: 'reject_once' },
]

test('pickPermissionOption prefers one-shot options and honors selected actions', () => {
  assert.equal(pickPermissionOption(options, 'allow')?.optionId, 'yes')
  assert.equal(pickPermissionOption(options, 'deny')?.optionId, 'no')
  assert.equal(
    pickPermissionOption(options, 'allow', 'always')?.optionId,
    'always',
  )
  // A selected action must match the allow/deny intent.
  assert.equal(pickPermissionOption(options, 'deny', 'yes'), undefined)
  assert.equal(
    pickPermissionOption([{ optionId: 'a', kind: 'allow_always' }], 'allow')
      ?.optionId,
    'a',
  )
})

test('permissionKind classifies questions, mode switches, plans and tools', () => {
  const binary = [{ kind: 'allow_once' }, { kind: 'reject_once' }]
  const chooser = [{ kind: 'allow_once' }, { kind: 'allow_once' }]
  const kind = (
    title: string,
    detailType: 'plan' | 'plain_text',
    toolKind?: string,
    options = binary,
  ) =>
    permissionKind(
      { options, toolCall: { title, kind: toolKind } },
      detailType === 'plan'
        ? { type: 'plan', text: 'p' }
        : { type: 'plain_text', label: title },
    )
  assert.equal(
    kind('run_command', 'plain_text', 'execute', chooser),
    'question',
  )
  assert.equal(kind('ask_user', 'plain_text'), 'question')
  assert.equal(kind('default_api:ask_question', 'plain_text'), 'question')
  assert.equal(kind('exit_plan_mode', 'plain_text'), 'mode')
  assert.equal(kind('x', 'plain_text', 'switch_mode'), 'mode')
  assert.equal(kind('write_to_file', 'plan', 'edit'), 'plan')
  assert.equal(kind('run_command', 'plain_text', 'execute'), 'tool')
})

test('isPreapprovedMcpTool matches exact direct and lazy MCP grants only', () => {
  const policy = {
    preapproved: [{ kind: 'mcp' as const, server: 'rea', tool: 'open' }],
  }
  assert.equal(isPreapprovedMcpTool('mcp__rea__open', {}, policy), true)
  assert.equal(isPreapprovedMcpTool('rea:open', {}, policy), true)
  assert.equal(
    isPreapprovedMcpTool(
      'call_mcp_tool',
      { ServerName: 'rea', ToolName: 'open' },
      policy,
    ),
    true,
  )
  assert.equal(
    isPreapprovedMcpTool(
      'call_mcp_tool',
      { ServerName: 'rea', ToolName: 'close' },
      policy,
    ),
    false,
  )
  assert.equal(isPreapprovedMcpTool('mcp__rea__close', {}, policy), false)
  assert.equal(isPreapprovedMcpTool('mcp_rea_open', {}, policy), false)
  assert.equal(isPreapprovedMcpTool('mcp__rea__open', {}, undefined), false)
})

// Shaped like agy's ask_question form: `qN` choices plus an optional free-form `qN_other`.
const agyForm = {
  message: 'The agent has a question.',
  requestedSchema: {
    type: 'object',
    properties: {
      q1: {
        type: 'string',
        title: 'Which implementation path?',
        oneOf: [
          { const: '0', title: '(Recommended) Option A: Direct fix' },
          { const: '1', title: 'Option B: Full refactor' },
        ],
      },
      q1_other: {
        type: 'string',
        title: 'Other',
        description: 'Optional free-form answer to: Which implementation path?',
      },
      q2: {
        type: 'array',
        title: 'Select target platforms',
        items: {
          type: 'string',
          oneOf: [
            { const: 'linux', title: 'Linux, x86_64' },
            { const: 'mac', title: 'macOS' },
          ],
        },
      },
      q2_other: { type: 'string', title: 'Other' },
      q3_other: { type: 'string', title: 'Any extra notes?' },
    },
  },
}

test('elicitation forms become Paseo QuestionCard input', () => {
  assert.deepEqual(
    toQuestionPermissionInput(normalizeElicitationQuestions(agyForm)),
    {
      questions: [
        {
          header: 'Question 1',
          question: 'Which implementation path?',
          options: [
            { label: '(Recommended) Option A: Direct fix' },
            { label: 'Option B: Full refactor' },
          ],
          multiSelect: false,
          allowOther: true,
        },
        {
          header: 'Question 2',
          question: 'Select target platforms',
          options: [{ label: 'Linux, x86_64' }, { label: 'macOS' }],
          multiSelect: true,
          allowOther: true,
        },
        {
          header: 'Question 3',
          question: 'Any extra notes?',
          options: [],
          multiSelect: false,
          allowOther: true,
        },
      ],
    },
  )
  assert.deepEqual(
    normalizeElicitationQuestions({ message: '  Continue?  ' }).map(
      (q) => q.question,
    ),
    ['Continue?'],
  )
  assert.deepEqual(normalizeElicitationQuestions({}), [])
})

test('QuestionCard answers map back to schema values and Other fields', () => {
  const questions = normalizeElicitationQuestions(agyForm)
  // Paseo answers by header; multi-select joins labels and custom text with ", ".
  assert.deepEqual(
    buildElicitationResponse(questions, {
      behavior: 'allow',
      updatedInput: {
        answers: {
          'Question 1': '(Recommended) Option A: Direct fix',
          'Question 2': 'Linux, x86_64, Windows ARM64',
          'Question 3': 'Keep tests fast',
        },
      },
    }),
    {
      action: 'accept',
      content: {
        q1: '0',
        q2: ['linux'],
        q2_other: 'Windows ARM64',
        q3_other: 'Keep tests fast',
      },
    },
  )
  assert.deepEqual(
    buildElicitationResponse(questions, {
      behavior: 'allow',
      updatedInput: { answers: { 'Question 1': 'Custom hybrid approach' } },
    }),
    { action: 'accept', content: { q1_other: 'Custom hybrid approach' } },
  )
  assert.deepEqual(buildElicitationResponse(questions, { behavior: 'deny' }), {
    action: 'decline',
  })
})

test('answer text keeps labels and prefixes questions only when there are several', () => {
  const questions = normalizeElicitationQuestions(agyForm)
  const response = {
    behavior: 'allow' as const,
    updatedInput: {
      answers: {
        'Question 1': 'Option B: Full refactor',
        'Question 2': 'Linux, x86_64, macOS',
      },
    },
  }
  assert.equal(
    formatQuestionAnswersText(questions.slice(0, 1), response),
    'Option B: Full refactor',
  )
  assert.equal(
    formatQuestionAnswersText(questions, response),
    'Which implementation path?\nAnswer: Option B: Full refactor\n\nSelect target platforms\nAnswer: Linux, x86_64, macOS',
  )
  assert.equal(
    formatQuestionAnswersText(questions, { behavior: 'deny' }),
    'Declined',
  )
})

test('raw ask_question arguments normalize like agy writes them', () => {
  const [question] = normalizeAskQuestionInput({
    questions: [
      {
        question: 'Proceed with deployment?',
        options: ['(Recommended) Yes', 'No'],
        is_multi_select: false,
      },
    ],
  })
  assert.deepEqual(question, {
    id: 'q1',
    header: 'Question 1',
    question: 'Proceed with deployment?',
    options: [
      { id: '0', label: '(Recommended) Yes' },
      { id: '1', label: 'No' },
    ],
    multiSelect: false,
    allowOther: true,
  })
  assert.deepEqual(normalizeAskQuestionInput({ question: 'Name?' }), [
    {
      id: 'q1',
      header: 'Question 1',
      question: 'Name?',
      options: [],
      multiSelect: false,
      allowOther: true,
    },
  ])
})

test('a replayed schema is matched by question and labels, and answered from the card that asked it', () => {
  const asked = normalizeAskQuestionInput({
    questions: [
      {
        header: 'Environment',
        question: 'Select environment',
        options: ['Production', 'Staging'],
      },
      {
        header: 'Region',
        question: 'Select regions',
        is_multi_select: true,
        options: ['US East', 'EU West'],
      },
    ],
  })
  const replay = (title: string, labels: string[]) =>
    normalizeElicitationQuestions({
      requestedSchema: {
        type: 'object',
        properties: {
          q1: {
            type: 'string',
            title,
            oneOf: labels.map((label, i) => ({
              const: `env_${i}`,
              title: label,
            })),
          },
          q2: {
            type: 'array',
            title: 'Select regions',
            items: {
              type: 'string',
              oneOf: [
                { const: 'us', title: 'US East' },
                { const: 'eu', title: 'EU West' },
              ],
            },
          },
        },
      },
    })
  const replayed = replay('Select environment', ['Production', 'Staging'])
  assert.equal(sameNormalizedQuestions(asked, replayed), true)
  assert.equal(
    sameNormalizedQuestions(
      asked,
      replay('Different?', ['Production', 'Staging']),
    ),
    false,
  )
  assert.equal(
    sameNormalizedQuestions(
      asked,
      replay('Select environment', ['Production', 'Dev']),
    ),
    false,
  )
  assert.equal(sameNormalizedQuestions([], []), false)
  // The card used the raw headers, so answers are read by them and mapped to the replayed schema's values.
  assert.deepEqual(
    buildElicitationResponse(
      replayed,
      {
        behavior: 'allow',
        updatedInput: {
          answers: { Environment: 'Staging', Region: 'US East, EU West' },
        },
      },
      asked,
    ),
    { action: 'accept', content: { q1: 'env_1', q2: ['us', 'eu'] } },
  )
})
