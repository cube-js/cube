import { BinaryOperator, UnaryOperator, TimeDimensionGranularity } from '@cubejs-client/core';

type Operator = BinaryOperator | UnaryOperator;

export const DATA_RANGES = [
  'custom',
  // 'all time',
  'today',
  'yesterday',
  'this week',
  'this month',
  'this quarter',
  'this year',
  'last 7 days',
  'last 30 days',
  'last week',
  'last month',
  'last quarter',
  'last year',
  'last 12 months',
];

export const UNARY_OPERATORS: Operator[] = ['set', 'notSet'];
export const BASE_BINARY_OPERATORS: Operator[] = ['equals', 'notEquals'];
export const STRING_BINARY_OPERATORS: Operator[] = [
  'contains',
  'notContains',
  'startsWith',
  'notStartsWith',
  'endsWith',
  'notEndsWith',
];
export const NUMBER_BINARY_OPERATORS: Operator[] = ['gt', 'gte', 'lt', 'lte'];
export const TIME_OPERATORS: Operator[] = [
  'inDateRange',
  'notInDateRange',
  'beforeDate',
  'afterDate',
  'beforeOrOnDate',
  'afterOrOnDate',
];

export const BINARY_OPERATORS: Operator[] = [
  ...BASE_BINARY_OPERATORS,
  ...NUMBER_BINARY_OPERATORS,
  ...STRING_BINARY_OPERATORS,
  ...TIME_OPERATORS,
];

export const OPERATOR_LABELS: Record<Operator, string> = {
  set: 'is set',
  notSet: 'is not set',

  equals: 'equals',
  notEquals: 'not equals',

  contains: 'contains',
  notContains: 'not contains',
  startsWith: 'starts with',
  notStartsWith: 'not starts with',
  endsWith: 'ends with',
  notEndsWith: 'not ends with',

  gt: '>',
  gte: '>=',
  lt: '<',
  lte: '<=',

  inDateRange: 'in date range',
  notInDateRange: 'not in date range',
  beforeDate: 'before date',
  afterDate: 'after date',

  beforeOrOnDate: 'before or on date',
  afterOrOnDate: 'after or on date',
};

export const OPERATORS_BY_TYPE = {
  all: [
    ...UNARY_OPERATORS,
    ...BASE_BINARY_OPERATORS,
    ...STRING_BINARY_OPERATORS,
    ...NUMBER_BINARY_OPERATORS,
    ...TIME_OPERATORS,
  ],
  string: [...UNARY_OPERATORS, ...BASE_BINARY_OPERATORS, ...STRING_BINARY_OPERATORS],
  number: [...UNARY_OPERATORS, ...BASE_BINARY_OPERATORS, ...NUMBER_BINARY_OPERATORS],
  boolean: [...UNARY_OPERATORS, ...BASE_BINARY_OPERATORS],
  time: [...UNARY_OPERATORS, ...BASE_BINARY_OPERATORS, ...TIME_OPERATORS],
};

export const OPERATORS: Operator[] = [...UNARY_OPERATORS, ...BINARY_OPERATORS];

export const PREDEFINED_GRANULARITIES: TimeDimensionGranularity[] = [
  'second',
  'minute',
  'hour',
  'day',
  'week',
  'month',
  'quarter',
  'year',
];
