export enum SQLQueryType {
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

interface SQLRunnerQueryType {
  type: SQLQueryType;
  regex?: RegExp;
  shouldAddLimit?: boolean;
}

function createSQLRunnerQueryType({
  type,
}: SQLRunnerQueryType): SQLRunnerQueryType {
  const regex: RegExp = new RegExp(`^(${type})\\b`, 'i');
  let shouldAddLimit: boolean = false;

  if (type === SQLQueryType.Select || type === SQLQueryType.With) {
    shouldAddLimit = true;
  }

  return {
    type,
    regex,
    shouldAddLimit,
  };
}

export function getSQLRunnerQueryType(sql: string): SQLRunnerQueryType | null {
  const sqlRunnerQueryTypes: SQLRunnerQueryType[] = [];

  for (const type of Object.values(SQLQueryType)) {
    const option = createSQLRunnerQueryType({ type });
    sqlRunnerQueryTypes.push(option);
  }

  for (const queryType of sqlRunnerQueryTypes) {
    if (queryType?.regex?.test(sql)) {
      return queryType;
    }
  }

  return null;
}

/**
 * Checks query string for scope permissions.
 * Example scope with permissions: ['sql-runner-permissions:insert,update,delete,select']
 */
export const isQueryAllowed = (
  queryType: SQLRunnerQueryType,
  scope: string[]
): Boolean => {
  const permissionsString = scope.find((s: string) => s.startsWith('sql-runner-permissions:'));

  if (!permissionsString) {
    return true;
  }

  const [, p] = permissionsString.split(':');
  if (!p.length) {
    return false;
  }

  const permissions = p.split(',');

  if (permissions.includes(queryType.type)) {
    return true;
  }

  return false;
};
