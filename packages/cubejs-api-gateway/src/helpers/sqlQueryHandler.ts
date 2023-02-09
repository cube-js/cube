import { UserError } from '../UserError';

export enum SQlRunnerQueryType {
  Select = 'select',
  With = 'with',
  Create = 'create',
  Alter = 'alter',
  Drop = 'drop',
  Truncate = 'truncate',
  Insert = 'insert',
  Update = 'update',
  Delete = 'delete',
  Explain = 'explain',
}

const INVALID_QUERY_ERROR = 'Invalid SQL query';

// Determine SQL query type and add LIMIT clause if needed
export const shouldAddLimit = (sql: string): Boolean => {
  // select statements
  const select = `^(${SQlRunnerQueryType.Select}|${SQlRunnerQueryType.With})\\b`;
  if (new RegExp(select, 'i').test(sql)) {
    return true;
  }

  // the rest statements
  const statements = Object.values(SQlRunnerQueryType).filter(
    (s) => s !== SQlRunnerQueryType.Select && s !== SQlRunnerQueryType.With
  );

  if (new RegExp(`^(${statements.join('|')})\\b`, 'i').test(sql)) {
    return false;
  }

  throw new UserError(INVALID_QUERY_ERROR);
};

// Determine particular SQL query type
const getStatementType = (sql: string): string => {
  for (const key of Object.keys(SQlRunnerQueryType)) {
    if (new RegExp(`^(${key})\\b`, 'i').test(sql)) {
      return SQlRunnerQueryType[key];
    }
  }

  throw new UserError(INVALID_QUERY_ERROR);
};

// check query string for scope permissions
// an example scope with permissions: ['sql-runner-permissions:create,update,delete,select']
export const isQueryAllowed = (query: string, scope: string[]): Boolean => {
  const statementType = getStatementType(query);
  const permissionsString = scope.find((s: string) => s.startsWith('sql-runner-permissions:'));

  if (!permissionsString) {
    return true;
  }

  const [, p] = permissionsString.split(':');
  if (!p.length) {
    return false;
  }

  const permissions = p.split(',');

  if (permissions.includes(statementType)) {
    return true;
  }

  return false;
};
