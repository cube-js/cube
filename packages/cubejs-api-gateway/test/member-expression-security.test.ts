// The cases below are raw SQL fragments that must contain a *literal* `${...}`
// (the member-expression interpolation syntax). They are intentionally plain
// strings, not template literals.
/* eslint-disable no-template-curly-in-string */
import { assertSafeMemberExpressionSql } from '../src/member-expression-security';

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
    ];

    test.each(cases)('%s', (sql, cubeParams) => {
      expect(() => assertSafeMemberExpressionSql(sql, cubeParams)).toThrow(
        /Unsafe member expression/
      );
    });
  });
});
