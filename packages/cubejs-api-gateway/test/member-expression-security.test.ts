// The cases below are raw SQL fragments that must contain a *literal* `${...}`
// (the member-expression interpolation syntax). They are intentionally plain
// strings, not template literals.
/* eslint-disable no-template-curly-in-string */
import {
  assertSafeMemberExpressionSql,
  assertSafeCompiledMemberExpression,
} from '../src/member-expression-security';

describe('assertSafeMemberExpressionSql', () => {
  describe('accepts the references the planner actually emits', () => {
    const cases: [string, string[]][] = [
      ['${KibanaSampleDataEcommerce.customer_gender}', ['KibanaSampleDataEcommerce']],
      ['${KibanaSampleDataEcommerce.sumPrice}', ['KibanaSampleDataEcommerce']],
      ['(LOWER(${KibanaSampleDataEcommerce.customer_gender}) = $0$)', ['KibanaSampleDataEcommerce']],
      [
        '(((CAST(TRUNC(EXTRACT(year FROM ${MultiTypeCube.dim_date0})) AS INTEGER) = 2024)) = TRUE)',
        ['MultiTypeCube'],
      ],
      // multiple references and multiple cube params
      ['(${A.x} + ${B.y})', ['A', 'B']],
      // nested member path
      ['${Orders.user.name}', ['Orders']],
      // no interpolation at all
      ['1 + 2', []],
      // bare param reference
      ['${Orders}', ['Orders']],
      // an escaped backtick (as CubeSQL emits) is literal text, not a breakout
      ['${Orders.col} = \\`x\\`', ['Orders']],
    ];

    test.each(cases)('%s', (sql, cubeParams) => {
      expect(() => assertSafeMemberExpressionSql(sql, cubeParams)).not.toThrow();
    });
  });

  describe('rejects arbitrary JavaScript in interpolations', () => {
    const cases: [string, string[]][] = [
      // arithmetic / literals
      ['${7*191}', ['Orders']],
      ['${1337}', ['Orders']],
      // access to globals (root is not a declared cube param)
      ['${process.version}', ['Orders']],
      ['${process.env.DB_PASSWORD}', ['Orders']],
      ['${global.process.exit}', ['Orders']],
      // the published PoC shape
      ['${process.mainModule.require("child_process").execSync("id")}', ['Orders']],
      // function calls / any non-identifier char
      ['${foo()}', ['foo']],
      ['${Orders.amount()}', ['Orders']],
      ['${`nested`}', ['Orders']],
      // prototype / Function-constructor pivot, even on a valid root
      ['${Orders.constructor.constructor("return process")()}', ['Orders']],
      ['${Orders.__proto__}', ['Orders']],
      ['${Orders.prototype}', ['Orders']],
      // empty interpolation
      ['${}', ['Orders']],
      // reference with no cube params available at all
      ['${Orders.amount}', []],
      // unterminated interpolation
      ['${Orders.amount', ['Orders']],
    ];

    test.each(cases)('%s', (sql, cubeParams) => {
      expect(() => assertSafeMemberExpressionSql(sql, cubeParams)).toThrow(
        /Unsafe member expression/
      );
    });
  });

  describe('rejects executable code smuggled through cubeParams', () => {
    // cubeParams entries are spliced into `Function(...)` as raw parameter
    // source, so a non-identifier entry can carry a default-value expression
    // that runs when the function is invoked, even when `sql` has no `${...}`.
    const cases: [string, string[]][] = [
      ['1', ['[a = process.mainModule.require("child_process").execSync("id")] = []']],
      ['1', ['[a = process.version] = []']],
      ['1', ['Orders', 'x = 1']],
      ['1', ['Orders', 'x = process.version']],
      ['${Orders.amount}', ['Orders', 'y = (() => {})()']],
      ['1', ['constructor']],
      ['1', ['a.b']],
      ['1', ['a b']],
    ];

    test.each(cases)('%s / %j', (sql, cubeParams) => {
      expect(() => assertSafeMemberExpressionSql(sql, cubeParams)).toThrow(
        /Unsafe member expression/
      );
    });
  });

  describe('rejects template-literal breakout in sql', () => {
    // sql is spliced into `return `<sql>``, so an unescaped backtick closes the
    // literal and a trailing odd backslash escapes its closing backtick.
    const cases: [string, string[]][] = [
      ['` + process.version + `', ['Orders']],
      ['${Orders.col}` + process.mainModule + `', ['Orders']],
      // escaped backtick is fine, but a further bare backtick is not
      ['\\`x` + process.version + `', ['Orders']],
      // trailing odd backslash escapes the closing backtick
      ['${Orders.col}\\', ['Orders']],
      ['abc\\\\\\', ['Orders']],
    ];

    test.each(cases)('%j', (sql, cubeParams) => {
      expect(() => assertSafeMemberExpressionSql(sql, cubeParams)).toThrow(
        /Unsafe member expression/
      );
    });
  });
});

describe('assertSafeCompiledMemberExpression', () => {
  describe('accepts the compiled arrays parseMemberExpression builds', () => {
    const cases: string[][] = [
      ['sales', 'return `${sales.amount}`'],
      ['A', 'B', 'return `${A.x} + ${B.y}`'],
      // constant expression, no params
      ['return `1 + 2`'],
    ];

    test.each(cases.map((c) => [c]))('%j', (expression) => {
      expect(() => assertSafeCompiledMemberExpression(expression)).not.toThrow();
    });
  });

  describe('rejects compiled arrays that could execute JS (object-form bypass)', () => {
    const cases: string[][] = [
      // body not wrapped in the `return \`...\`` template
      ['return process.version'],
      ['return process.mainModule.require("child_process").execSync("id")'],
      // non-identifier param (raw Function parameter source)
      ['[a = process.version] = []', 'return `1`'],
      ['sales', 'x = process.version', 'return `${sales.amount}`'],
      // arbitrary JS / globals inside the wrapped body
      ['sales', 'return `${process.version}`'],
      ['return `${7 * 191}`'],
      // backtick breakout inside the wrapped body
      ['sales', 'return `a` + process.version + `b`'],
      // malformed
      [],
    ];

    test.each(cases.map((c) => [c]))('%j', (expression) => {
      expect(() => assertSafeCompiledMemberExpression(expression)).toThrow(
        /Unsafe member expression/
      );
    });
  });
});
