import { UserError } from './user-error';

const IDENTIFIER_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

// Property names that expose the prototype chain / Function constructor.
const FORBIDDEN_SEGMENTS = new Set(['constructor', '__proto__', 'prototype']);

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
 * `cubeParams` and `sql` are spliced into `Function(...cubeParams, \`return \`${sql}\`\`)`
 * (see `evalMemberExpression`); reject anything that could execute JS there.
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

  // Scan for `${...}` interpolations with a linear manual walk. A regex such as
  // /\$\{[^}]*\}/g backtracks per start position and is polynomial on hostile
  // input (e.g. `${` followed by many `{`); `indexOf` is linear.
  let searchFrom = 0;

  for (;;) {
    const open = sql.indexOf('${', searchFrom);

    if (open === -1) {
      break;
    }

    const close = sql.indexOf('}', open + 2);

    if (close === -1) {
      // A legitimate reference is always closed; an unterminated `${` is not one.
      throw new UserError('Unsafe member expression: unterminated interpolation in SQL');
    }

    const body = sql.slice(open + 2, close).trim();
    const segments = body.split('.');
    const isSafeReference =
      body.length > 0 &&
      allowedRoots.has(segments[0]) &&
      segments.every((segment) => IDENTIFIER_RE.test(segment) && !FORBIDDEN_SEGMENTS.has(segment));

    if (!isSafeReference) {
      throw new UserError(
        'Unsafe member expression: only cube member references are allowed in SQL interpolations'
      );
    }

    searchFrom = close + 1;
  }
}

// The exact body `parseMemberExpression` builds: `return \`<sql>\``.
const COMPILED_BODY_RE = /^return `([\s\S]*)`$/;

/**
 * Validates the compiled `[...cubeParams, body]` right before `Function.constructor`
 * (in `evalMemberExpression`), so object-form members are covered too.
 */
export function assertSafeCompiledMemberExpression(expression: string[]): void {
  if (!Array.isArray(expression) || expression.length < 1) {
    throw new UserError('Unsafe member expression: malformed compiled expression');
  }

  const params = expression.slice(0, -1);
  const body = expression[expression.length - 1];
  const match = typeof body === 'string' ? COMPILED_BODY_RE.exec(body) : null;

  if (!match) {
    throw new UserError('Unsafe member expression: unexpected compiled body');
  }

  assertSafeMemberExpressionSql(match[1], params);
}
