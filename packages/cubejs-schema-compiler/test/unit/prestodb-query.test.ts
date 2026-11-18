/* eslint-disable no-restricted-syntax, quotes */
import { PrestodbQuery } from '../../src/adapter/PrestodbQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('PrestodbQuery', () => {
  const compilers = prepareJsCompiler(`
    cube('boolean_dimension', {
      sql: \`
        SELECT
          'true' AS dim,
          'name' AS name
      \`,
      dimensions: {
        dim: {
          sql: \`dim\`,
          type: 'boolean',
          primaryKey: true
        },
        name: {
          sql: \`name\`,
          type: 'string'
        }
      },
      measures: {
        count: {
          type: 'count',
        }
      }
    });
    `);

  it('bool param cast (PrestoQuery)', async () => {
    await compilers.compile();

    const query = compilers.createQuery(PrestodbQuery, {
      measures: [
        'boolean_dimension.count',
      ],
      timeDimensions: [{
        dimension: 'boolean_dimension.dim',
      }],
      filters: [
        {
          member: 'boolean_dimension.dim',
          operator: 'equals',
          values: ['true'],
        },
      ],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);

    expect(queryAndParams[0]).toContain('"boolean_dimension".dim = CAST(? AS BOOLEAN)');
  });

  it('escapes literal backslashes in LIKE filter parameters', async () => {
    await compilers.compile();

    const query = compilers.createQuery(PrestodbQuery, {
      useNativeSqlPlanner: false,
      dimensions: [
        'boolean_dimension.name',
      ],
      filters: [
        {
          member: 'boolean_dimension.name',
          operator: 'contains',
          values: ['folder\\name_%'],
        },
      ],
    });

    const [sql, params] = query.buildSqlAndParams();

    expect(sql).toContain("ESCAPE '\\'");
    expect(params).toEqual(['folder\\\\name\\_\\%']);
  });
});
