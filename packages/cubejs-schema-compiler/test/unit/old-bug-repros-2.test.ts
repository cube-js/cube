import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

// Repros for old open GitHub issues. Each test asserts the correct behavior,
// so it fails until its bug is fixed.
describe('old bug repros (unit)', () => {
  describe('issue #10371 cube name in join_path collides with a dimension name', () => {
    it('compiles a view that joins through `test.test2`', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: test
    sql_table: test
    dimensions:
      - name: id
        type: string
        sql: id
        primary_key: true
      - name: test2
        type: string
        sql: test2
    joins:
      - name: test2
        relationship: many_to_one
        sql: "{CUBE.test2} = {test2.id}"

  - name: test2
    sql_table: test2
    dimensions:
      - name: id
        type: string
        sql: id
        primary_key: true
      - name: name
        type: string
        sql: name

views:
  - name: v_test
    cubes:
      - join_path: test
        prefix: true
        includes:
          - id
      - join_path: test.test2
        prefix: true
        includes:
          - name
`);
      await compiler.compile();
      const view = metaTransformer.cubes.find((c: any) => c.config.name === 'v_test');
      expect(view?.config.dimensions.map((d: any) => d.name).sort()).toEqual(['v_test.test2_name', 'v_test.test_id']);
    });
  });

  describe('issue #3486 pre-aggregation with non-unique member references', () => {
    it('rejects a rollup that references the same dimension twice', async () => {
      const { compiler } = prepareJsCompiler(`
        cube('orders', {
          sql: 'SELECT 1 AS id, \\'a\\' AS status',
          measures: { count: { type: 'count' } },
          dimensions: {
            id: { sql: 'id', type: 'number', primaryKey: true },
            status: { sql: 'status', type: 'string' },
          },
          preAggregations: {
            main: {
              measures: [CUBE.count],
              dimensions: [CUBE.status, CUBE.status],
            },
          },
        });
      `);
      await expect(compiler.compile()).rejects.toThrow(/status/);
    });
  });

  // Does not reproduce with Tesseract when the join uses member references, as the
  // rollup_join docs require. Kept as a regression guard.
  describe('issue #11124 rollup_join with a measure from the secondary cube', () => {
    const model = `
      cube('SecondaryCube', {
        data_source: 'secondary',
        sql: \`
          select 1 as id, 'loc_1' as location_id, 500 as contract_value
          UNION ALL
          select 2 as id, 'loc_2' as location_id, 300 as contract_value
        \`,
        dimensions: {
          id: { sql: 'id', type: 'string', primary_key: true },
          location_id: { sql: 'location_id', type: 'string' },
        },
        measures: {
          total_contract_value: { sql: 'contract_value', type: 'sum' },
        },
        preAggregations: {
          main: {
            type: 'rollup',
            external: true,
            measures: [CUBE.total_contract_value],
            dimensions: [CUBE.id, CUBE.location_id],
          },
        },
      });

      cube('PrimaryCube', {
        sql: \`
          select 1 as id, 1 as secondary_id, 'SUCCESS' as action_status
          UNION ALL
          select 2 as id, 2 as secondary_id, 'SUCCESS' as action_status
          UNION ALL
          select 3 as id, 1 as secondary_id, 'FAILURE' as action_status
        \`,
        joins: {
          SecondaryCube: {
            sql: \`\${CUBE.secondary_id} = \${SecondaryCube.id}\`,
            relationship: 'many_to_one',
          },
        },
        dimensions: {
          id: { sql: 'id', type: 'string', primary_key: true },
          secondary_id: { sql: 'secondary_id', type: 'string' },
          action_status: { sql: 'action_status', type: 'string' },
        },
        measures: {
          total_count: {
            type: 'count',
            filters: [{ sql: \`\${CUBE}.action_status = 'SUCCESS'\` }],
          },
        },
        preAggregations: {
          primary_rollup: {
            type: 'rollup',
            external: true,
            measures: [CUBE.total_count],
            dimensions: [CUBE.secondary_id, CUBE.action_status],
          },
          joined_rollup: {
            type: 'rollup_join',
            rollups: [PrimaryCube.primary_rollup, SecondaryCube.main],
            measures: [PrimaryCube.total_count, SecondaryCube.total_contract_value],
            dimensions: [PrimaryCube.secondary_id, SecondaryCube.location_id],
          },
        },
      });
    `;

    const build = async (measures: string[]) => {
      const compilers = prepareJsCompiler(model);
      await compilers.compiler.compile();
      const query = new PostgresQuery(compilers, {
        timezone: 'UTC',
        measures,
        dimensions: ['SecondaryCube.location_id'],
      });
      const [sql] = query.buildSqlAndParams();
      return sql;
    };

    it('serves a query with only primary measures from the rollup_join', async () => {
      const sql = await build(['PrimaryCube.total_count']);
      expect(sql).toContain('primary_cube_primary_rollup');
      expect(sql).toContain('secondary_cube_main');
    });

    it('serves a query that adds a secondary measure from the rollup_join', async () => {
      const sql = await build(['PrimaryCube.total_count', 'SecondaryCube.total_contract_value']);
      expect(sql).toContain('primary_cube_primary_rollup');
      expect(sql).toContain('secondary_cube_main');
    });
  });
});
