import {
  getEnv,
} from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';
import type { BaseQueryOptions } from '../../../src/adapter/BaseQuery';

describe('Multi-fact join', () => {
  jest.setTimeout(200000);

  const compilers = prepareJsCompiler(`
cube(\`orders\`, {
  sql: \`
    SELECT 79 AS id, 1 AS amount, 1 AS city_id UNION ALL
    SELECT 80 AS id, 2 AS amount, 1 AS city_id UNION ALL
    SELECT 81 AS id, 3 AS amount, 1 AS city_id UNION ALL
    SELECT 82 AS id, 4 AS amount, 2 AS city_id UNION ALL
    SELECT 83 AS id, 5 AS amount, 2 AS city_id UNION ALL
    SELECT 84 AS id, 6 AS amount, 3 AS city_id
  \`,

  joins: {
    city: {
      relationship: \`many_to_one\`,
      sql: \`\${orders}.city_id = \${city}.id\`,
    },
  },

  measures: {
    amount: {
      sql: \`amount\`,
      type: 'sum'
    }
  },

  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true,
    },
  },
});

cube(\`shipments\`, {
  sql: \`
    SELECT 100 AS id, 1 AS foo_id, 1 AS city_id UNION ALL
    SELECT 101 AS id, 2 AS foo_id, 2 AS city_id UNION ALL
    SELECT 102 AS id, 3 AS foo_id, 2 AS city_id UNION ALL
    SELECT 103 AS id, 4 AS foo_id, 2 AS city_id UNION ALL
    SELECT 104 AS id, 5 AS foo_id, 4 AS city_id
  \`,

  joins: {
    city: {
      relationship: \`many_to_one\`,
      sql: \`\${shipments}.city_id = \${city}.id\`,
    },
  },

  measures: {
    count: {
      type: \`count\`
    },
  },

  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true,
      shown: true
    },
  }
});

cube(\`city\`, {
  sql: \`
    SELECT 1 AS id, 'San Francisco' AS name UNION ALL
    SELECT 2 AS id, 'New York City' AS name
  \`,

  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true,
    },

    name: {
      sql: \`\${CUBE}.name\`,
      type: \`string\`,
    },
  },
});
    `);

  async function runQueryTest(q: BaseQueryOptions, expectedResult: Record<string, unknown>[]) {
    if (!getEnv('nativeSqlPlanner')) {
      return;
    }
    await compilers.compile();
    const query = compilers.createQuery(PostgresQuery, q);

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      expectedResult
    );
  }

  it('two regular sub-queries', async () => runQueryTest({
    measures: ['orders.amount', 'shipments.count'],
    dimensions: [
      'city.name'
    ],
    order: [{ id: 'city.name' }]
  }, [{
    city__name: 'New York City',
    orders__amount: '9',
    shipments__count: '3',
  }, {
    city__name: 'San Francisco',
    orders__amount: '6',
    shipments__count: '1',
  }, {
    city__name: null,
    orders__amount: '6',
    shipments__count: '1',
  }]));

  // https://github.com/cube-js/cube/issues/12565
  // The per-fact CTEs are joined on every dimension null-safely. Postgres can't hash or merge
  // join on `IS NOT DISTINCT FROM`, so the join turns into a nested loop over all row pairs
  // and the query time grows quadratically with the number of keys.
  it('joins multi-fact CTEs with a hash or merge join', async () => {
    if (!getEnv('nativeSqlPlanner')) {
      return;
    }

    const nullSafeCompilers = prepareJsCompiler(`
cube(\`entities\`, {
  sql: \`
    SELECT g AS id, 'entity ' || g AS name,
      CASE WHEN g % 10 = 0 THEN NULL ELSE DATE '2026-01-01' + (g % 300) END AS valid_from
    FROM generate_series(1, 2000) g
  \`,
  joins: {
    fact_a: {
      relationship: \`one_to_many\`,
      sql: \`\${CUBE}.id = \${fact_a.entity_id}\`,
    },
    fact_b: {
      relationship: \`one_to_many\`,
      sql: \`\${CUBE}.id = \${fact_b.entity_id}\`,
    },
  },
  dimensions: {
    id: { sql: \`id\`, type: \`number\`, primaryKey: true },
    name: { sql: \`name\`, type: \`string\` },
    valid_from: { sql: \`valid_from\`, type: \`time\` },
  },
});

cube(\`fact_a\`, {
  sql: \`SELECT g AS id, (g % 2000) + 1 AS entity_id, g AS amount_a FROM generate_series(1, 6000) g\`,
  dimensions: {
    id: { sql: \`id\`, type: \`number\`, primaryKey: true },
    entity_id: { sql: \`entity_id\`, type: \`number\` },
  },
  measures: {
    amount_a: { sql: \`amount_a\`, type: \`sum\` },
  },
});

cube(\`fact_b\`, {
  sql: \`SELECT g AS id, (g % 2000) + 1 AS entity_id, g AS amount_b FROM generate_series(1, 4000) g\`,
  dimensions: {
    id: { sql: \`id\`, type: \`number\`, primaryKey: true },
    entity_id: { sql: \`entity_id\`, type: \`number\` },
  },
  measures: {
    amount_b: { sql: \`amount_b\`, type: \`sum\` },
  },
});
    `);

    await nullSafeCompilers.compile();
    const query = nullSafeCompilers.createQuery(PostgresQuery, {
      measures: ['fact_a.amount_a', 'fact_b.amount_b'],
      dimensions: ['entities.id', 'entities.name', 'entities.valid_from'],
      order: [{ id: 'entities.id' }],
    });
    const [sql, params] = query.buildSqlAndParams();

    // Same results as before: one row per entity, NULL `valid_from` keys still matched
    const rows = await dbRunner.testQuery([sql, params]);
    expect(rows.length).toEqual(2000);
    expect(rows[9]).toEqual({
      entities__id: 10,
      entities__name: 'entity 10',
      entities__valid_from: null,
      fact_a__amount_a: '6027',
      fact_b__amount_b: '2018',
    });

    const plan = await dbRunner.testQuery([`EXPLAIN ${sql}`, params]);
    const planText = plan.map((r: Record<string, string>) => r['QUERY PLAN']).join('\n');
    // A non-hashable null-safe key ends up in a nested loop's `Join Filter`
    expect(planText).not.toMatch(/Join Filter: .*IS DISTINCT FROM/);
  });
});
