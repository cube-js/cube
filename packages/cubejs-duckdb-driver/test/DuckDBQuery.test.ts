import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { DuckDBQuery } from '../src/DuckDBQuery';

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'main.js', content }]),
});

describe('DuckDBQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(
    `
cube(\`sales\`, {
  sql: \` select * from public.sales \`,

  measures: {
    count: {
      type: 'count'
    }
  },
  dimensions: {
    name: {
      type: 'string',
      sql: 'name'
    },
  }
});
`,
  );

  const buildFilter = (operator: string, values: string[]) => {
    const query = new DuckDBQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['sales.count'],
        filters: [
          {
            member: 'sales.name',
            operator,
            values,
          },
        ],
      }
    );

    return query.buildSqlAndParams();
  };

  // DuckDB does not treat backslash as a default LIKE escape character, so the
  // base `escapeWildcardChars` (which backslash-escapes `%`/`_` in user values)
  // only works when the LIKE/ILIKE is given an explicit `ESCAPE '\'` clause.
  // Regression test for the missing ESCAPE clause on the REST-API filter path.
  it('emits ESCAPE \'\\\' for `contains`', () => compiler.compile().then(() => {
    const [sql, params] = buildFilter('contains', ['50%']);

    expect(sql).toContain('ILIKE \'%\' || ? || \'%\' ESCAPE \'\\\'');
    // user value gets its wildcard char backslash-escaped
    expect(params).toContain('50\\%');
  }));

  it('emits ESCAPE \'\\\' for `notContains`', () => compiler.compile().then(() => {
    const [sql, params] = buildFilter('notContains', ['50%']);

    expect(sql).toContain('NOT ILIKE \'%\' || ? || \'%\' ESCAPE \'\\\'');
    expect(params).toContain('50\\%');
  }));

  it('emits ESCAPE \'\\\' for `startsWith`', () => compiler.compile().then(() => {
    const [sql, params] = buildFilter('startsWith', ['a_b']);

    expect(sql).toContain('ILIKE ? || \'%\' ESCAPE \'\\\'');
    expect(params).toContain('a\\_b');
  }));

  it('emits ESCAPE \'\\\' for `endsWith`', () => compiler.compile().then(() => {
    const [sql, params] = buildFilter('endsWith', ['a_b']);

    expect(sql).toContain('ILIKE \'%\' || ? ESCAPE \'\\\'');
    expect(params).toContain('a\\_b');
  }));
});
