import {
  Query,
  Filter,
  UnaryFilter,
  BinaryFilter,
  LogicalAndFilter,
  LogicalOrFilter,
  TimeDimension,
  BinaryOperator,
} from '@cubejs-client/core';

const VALID_BINARY_OPERATORS: BinaryOperator[] = [
  'equals',
  'notEquals',
  'contains',
  'notContains',
  'gt',
  'gte',
  'lt',
  'lte',
  'startsWith',
  'notStartsWith',
  'endsWith',
  'notEndsWith',
  'inDateRange',
  'notInDateRange',
  'beforeDate',
  'afterDate',
  'beforeOrOnDate',
  'afterOrOnDate',
];

const isArrayOfStrings = (value: any): value is string[] =>
  Array.isArray(value) && value.every((item) => typeof item === 'string');

const isValidUnaryFilter = (filter: any): filter is UnaryFilter =>
  typeof filter === 'object' &&
  'member' in filter &&
  'operator' in filter &&
  (filter.operator === 'set' || filter.operator === 'notSet') &&
  filter.values === undefined;

const isValidBinaryFilter = (filter: any): filter is BinaryFilter =>
  typeof filter === 'object' &&
  'member' in filter &&
  'operator' in filter &&
  VALID_BINARY_OPERATORS.includes(filter.operator) &&
  'values' in filter &&
  isArrayOfStrings(filter.values);

const sanitizeLogicalFilter = (filter: any): LogicalAndFilter | LogicalOrFilter | null => {
  if (typeof filter !== 'object' || (!('and' in filter) && !('or' in filter))) {
    return null; // Not a valid logical filter
  }

  const key = 'and' in filter ? 'and' : 'or';
  const sanitizedSubFilters = filter[key]
    .map((subFilter: any) => sanitizeFilter(subFilter))
    .filter(Boolean); // Remove invalid subfilters

  // Empty logical filters are valid
  return { [key]: sanitizedSubFilters } as LogicalAndFilter | LogicalOrFilter;
};

const sanitizeFilter = (filter: any): Filter | null => {
  if (isValidUnaryFilter(filter)) {
    return filter;
  }

  if (isValidBinaryFilter(filter)) {
    return filter;
  }

  return sanitizeLogicalFilter(filter);
};

const sanitizeFilters = (filters: any): Filter[] =>
  Array.isArray(filters) ? (filters.map(sanitizeFilter).filter(Boolean) as Filter[]) : [];

const isValidTimeDimension = (td: any): td is TimeDimension =>
  typeof td === 'object' &&
  'dimension' in td &&
  typeof td.dimension === 'string' &&
  (!td.granularity || typeof td.granularity === 'string') &&
  (!td.dateRange ||
    typeof td.dateRange === 'string' ||
    (Array.isArray(td.dateRange) &&
      td.dateRange.length === 2 &&
      td.dateRange.every((date: any) => typeof date === 'string')));

const sanitizeTimeDimensions = (timeDimensions: any): TimeDimension[] =>
  Array.isArray(timeDimensions) ? timeDimensions.filter(isValidTimeDimension) : [];

export function validateQuery(query: Record<string, any>): Query {
  const sanitizedQuery: Partial<Query> = {};

  if (isArrayOfStrings(query.measures) && query.measures.length > 0) {
    sanitizedQuery.measures = query.measures;
  }

  if (isArrayOfStrings(query.dimensions) && query.dimensions.length > 0) {
    sanitizedQuery.dimensions = query.dimensions;
  }

  const sanitizedFilters = sanitizeFilters(query.filters);

  if (sanitizedFilters.length > 0) {
    sanitizedQuery.filters = sanitizedFilters;
  }

  const sanitizedTimeDimensions = sanitizeTimeDimensions(query.timeDimensions);

  if (sanitizedTimeDimensions.length > 0) {
    sanitizedQuery.timeDimensions = sanitizedTimeDimensions;
  }

  if (isArrayOfStrings(query.segments) && query.segments.length > 0) {
    sanitizedQuery.segments = query.segments;
  }

  if (typeof query.limit === 'number' && query.limit > 0) {
    sanitizedQuery.limit = query.limit;
  }

  if (typeof query.offset === 'number' && query.offset !== 0) {
    sanitizedQuery.offset = query.offset;
  }

  if (
    typeof query.order === 'object' &&
    !Array.isArray(query.order) &&
    Object.keys(query.order).length > 0
  ) {
    sanitizedQuery.order = query.order;
  }

  if (typeof query.timezone === 'string') {
    sanitizedQuery.timezone = query.timezone;
  }

  // It's not supported yet
  // if (query.renewQuery === true) {
  //   sanitizedQuery.renewQuery = query.renewQuery;
  // }

  if (query.ungrouped === true) {
    sanitizedQuery.ungrouped = query.ungrouped;
  }

  if (query.total === true) {
    sanitizedQuery.total = query.total;
  }

  return sanitizedQuery as Query;
}
