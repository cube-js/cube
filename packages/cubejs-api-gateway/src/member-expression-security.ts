import { UserError } from './user-error';

/**
 * Member expressions that arrive over the SQL API carry a SQL fragment
 * (`expr.sql`) which the gateway splices into a JavaScript template literal and
 * evaluates with `Function.constructor` (see `parseMemberExpression` /
 * `evalMemberExpression` in `gateway.ts`). The only interpolations the planner
 * ever emits into that fragment are references to the declared cube params,
 * e.g. `${Orders.amount}` or `${Orders.user.name}` — the root is always one of
 * the `cubeParams` the expression is compiled with.
 *
 * This guard rejects any `${...}` whose body is not such a reference, so a
 * fragment can never smuggle arbitrary JavaScript into the evaluated template.
 * It blocks, for example:
 *   - `${7*191}`                      (operators / literals)
 *   - `${process.env.SECRET}`         (root not a cube param => a global)
 *   - `${foo()}`                      (calls, any non-identifier char)
 *   - `${Orders.constructor.name}`    (prototype pivot)
 * while preserving every shape the planner actually produces.
 */

const IDENTIFIER_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

// Property names that expose the prototype chain / Function constructor and
// must never appear in an interpolation, even on a legitimate cube-param root.
const FORBIDDEN_SEGMENTS = new Set(['constructor', '__proto__', 'prototype']);

// `[^}]*` deliberately stops at the first `}`: a legitimate reference never
// contains a brace, so anything that does fails closed below.
const INTERPOLATION_RE = /\$\{([^}]*)\}/g;

/**
 * Throws a {@link UserError} if a member expression is not safe to splice into
 * the evaluated template:
 *   - `cubeParams` is spliced into `Function(...)` as raw parameter *source*
 *     (see `evalMemberExpression` in `gateway.ts`), so an entry that is not a
 *     plain identifier can carry executable code through a default-value or
 *     destructuring parameter, e.g. `[a = process.version] = []` or `x = <expr>`.
 *   - `sql` is spliced into a template literal, so every `${...}` must be a plain
 *     reference rooted at one of `cubeParams`.
 */
export function assertSafeMemberExpressionSql(sql: string, cubeParams: string[]): void {
  for (const param of cubeParams) {
    if (!IDENTIFIER_RE.test(param) || FORBIDDEN_SEGMENTS.has(param)) {
      throw new UserError(
        'Unsafe member expression: cube param is not a valid identifier'
      );
    }
  }

  const allowedRoots = new Set(cubeParams);
  const interpolationRe = new RegExp(INTERPOLATION_RE.source, 'g');

  let match: RegExpExecArray | null;
  // eslint-disable-next-line no-cond-assign
  while ((match = interpolationRe.exec(sql)) !== null) {
    const body = match[1].trim();
    const segments = body.split('.');
    const isSafeReference =
      body.length > 0 &&
      allowedRoots.has(segments[0]) &&
      segments.every(
        (segment) => IDENTIFIER_RE.test(segment) && !FORBIDDEN_SEGMENTS.has(segment)
      );

    if (!isSafeReference) {
      throw new UserError(
        'Unsafe member expression: only cube member references are allowed in SQL interpolations'
      );
    }
  }
}
