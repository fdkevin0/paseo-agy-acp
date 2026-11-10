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

test('normalizeElicitationQuestions and buildElicitationResponse translate single-select, multi-select, custom Other, freeform and decline', () => {
  const questions = normalizeElicitationQuestions({
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
          description:
            'Optional free-form answer to: Which implementation path?',
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
        q2_other: {
          type: 'string',
          title: 'Other',
        },
        q3_other: {
          type: 'string',
          title: 'Any extra notes?',
        },
      },
    },
  })
  assert.equal(questions.length, 3)
  assert.deepEqual(toQuestionPermissionInput(questions), {
    questions: [
      {
        id: 'q1',
        header: 'Question 1',
        question: 'Which implementation path?',
        options: [
          { label: '(Recommended) Option A: Direct fix' },
          { label: 'Option B: Full refactor' },
        ],
        multiSelect: false,
        allowOther: true,
        isOther: true,
      },
      {
        id: 'q2',
        header: 'Question 2',
        question: 'Select target platforms',
        options: [{ label: 'Linux, x86_64' }, { label: 'macOS' }],
        multiSelect: true,
        allowOther: true,
        isOther: true,
      },
      {
        id: 'q3',
        header: 'Question 3',
        question: 'Any extra notes?',
        options: [],
        multiSelect: false,
        allowOther: true,
        isOther: true,
      },
    ],
  })

  // Selected option on q1 + multi-select with comma in label and custom text on q2 + freeform on q3
  const accepted = buildElicitationResponse(questions, {
    behavior: 'allow',
    updatedInput: {
      answers: {
        'Question 1': '(Recommended) Option A: Direct fix',
        'Question 2': 'Linux, x86_64, Windows ARM64',
        'Question 3': 'Keep tests fast',
      },
    },
  })
  assert.deepEqual(accepted, {
    action: 'accept',
    content: {
      q1: '0',
      q2: ['linux'],
      q2_other: 'Windows ARM64',
      q3_other: 'Keep tests fast',
    },
  })

  // Custom "Other" text on single-select q1 must go to q1_other (never q1)
  const customSingle = buildElicitationResponse(questions.slice(0, 1), {
    behavior: 'allow',
    updatedInput: {
      answers: {
        'Question 1': 'Custom hybrid approach',
      },
    },
  })
  assert.deepEqual(customSingle, {
    action: 'accept',
    content: {
      q1_other: 'Custom hybrid approach',
    },
  })
  assert.equal(
    formatQuestionAnswersText(questions.slice(0, 1), {
      behavior: 'allow',
      updatedInput: { answers: { 'Question 1': '0' } },
    }),
    '(Recommended) Option A: Direct fix',
  )

  // Decline maps to { action: 'decline' }
  assert.deepEqual(buildElicitationResponse(questions, { behavior: 'deny' }), {
    action: 'decline',
  })

  // Raw ask_question tool input normalization
  const fromRaw = normalizeAskQuestionInput({
    questions: [
      {
        question: 'Proceed with deployment?',
        options: ['(Recommended) Yes', 'No'],
        is_multi_select: false,
      },
    ],
  })
  assert.equal(fromRaw.length, 1)
  assert.equal(fromRaw[0].id, 'q1')
  assert.deepEqual(fromRaw[0].options, [
    { id: '0', label: '(Recommended) Yes' },
    { id: '1', label: 'No' },
  ])
  const schemaSame = normalizeElicitationQuestions({
    requestedSchema: {
      type: 'object',
      properties: {
        q1: {
          type: 'string',
          title: 'Proceed with deployment?',
          oneOf: [
            { const: 'yes_id', title: '(Recommended) Yes' },
            { const: 'no_id', title: 'No' },
          ],
        },
      },
    },
  })
  const schemaDiff = normalizeElicitationQuestions({
    requestedSchema: {
      type: 'object',
      properties: {
        q1: {
          type: 'string',
          title: 'Different question?',
          oneOf: [
            { const: 'yes_id', title: '(Recommended) Yes' },
            { const: 'no_id', title: 'No' },
          ],
        },
      },
    },
  })
  assert.equal(sameNormalizedQuestions(fromRaw, schemaSame), true)
  assert.equal(sameNormalizedQuestions(fromRaw, schemaDiff), false)

  // Multi-question conversion when raw tool questions have custom header/id and elicitation uses generic Question 1/q1
  const rawMulti = normalizeAskQuestionInput({
    questions: [
      {
        id: 'env_id',
        header: 'Environment',
        question: 'Select environment',
        options: [
          { id: 'opt_prod', label: 'Production' },
          { id: 'opt_stage', label: 'Staging' },
        ],
      },
      {
        id: 'region_id',
        header: 'Region',
        question: 'Select regions',
        is_multi_select: true,
        options: [
          { id: 'opt_us', label: 'US East' },
          { id: 'opt_eu', label: 'EU West' },
        ],
      },
    ],
  })
  const schemaMulti = normalizeElicitationQuestions({
    requestedSchema: {
      type: 'object',
      properties: {
        q1: {
          type: 'string',
          title: 'Select environment',
          oneOf: [
            { const: '0', title: 'Production' },
            { const: '1', title: 'Staging' },
          ],
        },
        q2: {
          type: 'array',
          title: 'Select regions',
          items: {
            type: 'string',
            oneOf: [
              { const: 'us_const', title: 'US East' },
              { const: 'eu_const', title: 'EU West' },
            ],
          },
        },
      },
    },
  })
  assert.equal(sameNormalizedQuestions(rawMulti, schemaMulti), true)
  assert.deepEqual(
    buildElicitationResponse(
      schemaMulti,
      {
        behavior: 'allow',
        updatedInput: {
          answers: {
            Environment: 'Production',
            Region: 'US East, EU West',
          },
        },
      },
      rawMulti,
    ),
    {
      action: 'accept',
      content: {
        q1: '0',
        q2: ['us_const', 'eu_const'],
      },
    },
  )
  assert.deepEqual(
    buildElicitationResponse(
      schemaMulti,
      {
        behavior: 'allow',
        updatedInput: {
          answers: {
            env_id: 'opt_stage',
            region_id: ['opt_eu'],
          },
        },
      },
      rawMulti,
    ),
    {
      action: 'accept',
      content: {
        q1: '1',
        q2: ['eu_const'],
      },
    },
  )
  assert.equal(
    formatQuestionAnswersText(rawMulti, {
      behavior: 'allow',
      updatedInput: {
        answers: {
          env_id: 'opt_prod',
          region_id: ['opt_us', 'opt_eu'],
        },
      },
    }),
    'Select environment\nAnswer: Production\n\nSelect regions\nAnswer: US East, EU West',
  )
  assert.equal(
    formatQuestionAnswersText(rawMulti.slice(1), {
      behavior: 'allow',
      updatedInput: {
        answers: {
          region_id: 'opt_us, opt_eu',
        },
      },
    }),
    'US East, EU West',
  )
})
