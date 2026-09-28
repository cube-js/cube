import { formatMySql } from '@cubejs-backend/shared';

export function formatError(e: unknown): string {
  if (e instanceof AggregateError) {
    // Node leaves `message` empty for the errors it raises itself
    const prefix = e.message ? `Aggregate error: ${e.message}` : 'Aggregate error';
    return `${prefix}; errors: ${e.errors.map((inner) => formatError(inner)).join('; ')}`;
  }

  return `${e}`;
}

export function paramToken(index: number | string): string {
  return `___ClickHouseParam_${index}___`;
}

const PARAM_TOKEN_RE = new RegExp(paramToken('(\\d+)'), 'g');

// Placeholders are explicit tokens instead of `?`: a `?` also appears in string literals
// (e.g. regexes) and in the ternary operator, so it cannot be substituted positionally.
export function formatParams(sql: string, values: unknown[] = []): string {
  return sql.replace(PARAM_TOKEN_RE, (_, idx: string) => {
    const index = Number(idx);
    if (index >= values.length) {
      throw new Error(`Missing value for ClickHouse query parameter ${index} (${values.length} provided)`);
    }

    return formatMySql('?', [values[index]]);
  });
}
