import {
  getEnv,
} from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Multi-fact join', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
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

  async function runQueryTest(q, expectedResult) {
    if (!getEnv('nativeSqlPlanner')) {
      return;
    }
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      expectedResult
    );
  }

  if (getEnv('nativeSqlPlanner')) {
    it.skip('FIXME(tesseract): two regular sub-queries', () => {
      // TODO: Fix in tesseract
    });
  } else {
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
  }
});
