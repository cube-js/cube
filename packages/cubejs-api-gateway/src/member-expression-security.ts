import { UserError } from './user-error';

const IDENTIFIER_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

// Property names that expose the prototype chain / Function constructor.
const FORBIDDEN_SEGMENTS = new Set(['constructor', '__proto__', 'prototype']);

// `[^}]*` stops at the first `}`: a legitimate reference never contains a brace,
// so anything that does fails closed below.
const INTERPOLATION_RE = /\$\{([^}]*)\}/g;

// An unescaped backtick (even number of preceding backslashes) closes the
// `return \`...\`` template literal; a trailing odd run of backslashes escapes
// its closing backtick. Either one breaks out into arbitrary JS.
function hasTemplateBreakout(sql: string): boolean {
  let backslashes = 0;

  for (const ch of sql) {
    if (ch === '\\') {
      backslashes += 1;
    } else {
      if (ch === '`' && backslashes % 2 === 0) {
        return true;
      }
      backslashes = 0;
    }
  }
  return backslashes % 2 === 1;
}

/**
 * Member expressions from the SQL API are spliced into `return \`${sql}\`` and
 * evaluated with `Function.constructor` (see `evalMemberExpression`). Reject any
 * input that could execute arbitrary JS there: a `cubeParams` entry that is not
 * a plain identifier (it becomes raw `Function` parameter source, e.g.
 * `[a = <expr>] = []`), a template breakout in `sql` (backtick / trailing
 * backslash), or a `${...}` that is not a reference rooted at one of `cubeParams`.
 */
export function assertSafeMemberExpressionSql(sql: string, cubeParams: string[]): void {
  for (const param of cubeParams) {
    if (!IDENTIFIER_RE.test(param) || FORBIDDEN_SEGMENTS.has(param)) {
      throw new UserError('Unsafe member expression: cube param is not a valid identifier');
    }
  }

  if (hasTemplateBreakout(sql)) {
    throw new UserError('Unsafe member expression: unescaped backtick or trailing backslash in SQL');
  }

  const allowedRoots = new Set(cubeParams);

  for (const [, rawBody] of sql.matchAll(INTERPOLATION_RE)) {
    const segments = rawBody.trim().split('.');
    const isSafeReference =
      rawBody.trim().length > 0 &&
      allowedRoots.has(segments[0]) &&
      segments.every((segment) => IDENTIFIER_RE.test(segment) && !FORBIDDEN_SEGMENTS.has(segment));

    if (!isSafeReference) {
      throw new UserError(
        'Unsafe member expression: only cube member references are allowed in SQL interpolations'
      );
    }
  }
}
