import { UserError } from '../UserError';

// Determine SQL query type and add LIMIT clause if needed
export const shouldAddLimit = (sql: string): Boolean => {
  // TODO: Enhance the way we determine query type
  const ddlRegex = /^(CREATE|ALTER|DROP|TRUNCATE)\b/i;
  const dmlRegex = /^(INSERT|UPDATE|DELETE)\b/i;
  const selectRegex = /^(SELECT|WITH)\b/i;

  if (ddlRegex.test(sql) || dmlRegex.test(sql)) {
    return false;
  } else if (selectRegex.test(sql)) {
    return true;
  }

  throw new UserError('Invalid SQL query');
};
