/* eslint-disable no-restricted-syntax */
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('Multi-stage rank order_by template resolution through a view (issue #10856)', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`orders\`, {
      sql: \`SELECT 1 as id, 5 as parcels, '2024-01-01'::timestamp as created_at\`,

      dimensions: {
        id: {
          sql: \`id\`,
          type: \`number\`,
          primary_key: true,
          public: true
        },

        created_at: {
          sql: \`created_at\`,
          type: \`time\`
        }
      },

      measures: {
        num_parcels: {
          type: \`sum\`,
          sql: \`parcels\`
        },

        volume_by_day_rank: {
          multi_stage: true,
          type: \`rank\`,
          order_by: [{
            sql: \`\${num_parcels}\`,
            dir: 'desc'
          }],
          reduce_by: [created_at]
        }
      }
    });

    view(\`orders_view\`, {
      cubes: [{
        join_path: orders,
        includes: [\`created_at\`, \`volume_by_day_rank\`]
      }]
    });

    view(\`orders_aliased_view\`, {
      cubes: [{
        join_path: orders,
        includes: [
          \`created_at\`,
          { name: \`volume_by_day_rank\`, alias: \`day_rank\` }
        ]
      }]
    });

    view(\`orders_full_view\`, {
      cubes: [{
        join_path: orders,
        includes: [\`created_at\`, \`num_parcels\`, \`volume_by_day_rank\`]
      }]
    });
  `);

  function buildSql(measures: string[]) {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures,
      timeDimensions: [{
        dimension: `${measures[0].split('.')[0]}.created_at`,
        granularity: 'day',
      }],
      timezone: 'UTC',
    });
    return query.buildSqlAndParams()[0];
  }

  beforeAll(async () => {
    await compiler.compile();
  });

  // Before the fix this crashed with TypeError reading
  // '_objectWithResolvedProperties': the rank's order_by template
  // `${num_parcels}` was resolved against the view's symbol table, which
  // doesn't expose num_parcels.
  it('resolves rank order_by against the owning cube when the view does not expose the referenced measure', () => {
    const sql = buildSql(['orders_view.volume_by_day_rank']);
    expect(sql).toMatch(/ORDER BY[^)]*parcels/i);
  });

  it('resolves rank order_by when the rank itself is included under an alias', () => {
    const sql = buildSql(['orders_aliased_view.day_rank']);
    expect(sql).toMatch(/ORDER BY[^)]*parcels/i);
  });

  it('resolves rank order_by against the owning cube even when the view exposes the referenced measure', () => {
    const sql = buildSql(['orders_full_view.volume_by_day_rank']);
    expect(sql).toMatch(/ORDER BY[^)]*parcels/i);
  });

  it('keeps direct cube access unchanged', () => {
    const sql = buildSql(['orders.volume_by_day_rank']);
    expect(sql).toMatch(/ORDER BY[^)]*parcels/i);
  });
});
