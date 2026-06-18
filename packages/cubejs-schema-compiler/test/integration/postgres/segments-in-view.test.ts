import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Segments in View with SubQuery Dimensions', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
cube(\`Accounts\`, {
  sql: \`
    SELECT 1 AS id, 'US' AS region UNION ALL
    SELECT 2 AS id, 'US' AS region UNION ALL
    SELECT 3 AS id, 'EU' AS region UNION ALL
    SELECT 4 AS id, 'EU' AS region UNION ALL
    SELECT 5 AS id, 'AP' AS region
  \`,

  joins: {
    Tickets: {
      relationship: \`one_to_many\`,
      sql: \`\${CUBE}.id = \${Tickets}.account_id\`,
    },
  },

  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true,
      public: true,
    },

    region: {
      sql: \`\${CUBE}.region\`,
      type: \`string\`,
    },

    ticketCount: {
      sql: \`\${Tickets.count}\`,
      type: \`number\`,
      subQuery: true,
    },
  },

  segments: {
    hasNoTickets: {
      sql: \`(\${ticketCount} = 0)\`,
    },
  },

  measures: {
    count: {
      type: \`count\`,
    },
  },
});

cube(\`Tickets\`, {
  sql: \`
    SELECT 1 AS id, 1 AS account_id UNION ALL
    SELECT 2 AS id, 1 AS account_id UNION ALL
    SELECT 3 AS id, 3 AS account_id UNION ALL
    SELECT 4 AS id, 5 AS account_id
  \`,

  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true,
    },

    accountId: {
      sql: \`\${CUBE}.account_id\`,
      type: \`number\`,
    },
  },

  measures: {
    count: {
      type: \`count\`,
    },
  },
});

view(\`accountOverview\`, {
  cubes: [
    {
      join_path: Accounts,
      includes: [
        \`hasNoTickets\`,
        \`count\`,
        \`region\`,
      ],
    },
  ],
});
  `);

  async function runQueryTest(q, expectedResult) {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      expectedResult
    );
  }

  it('segment with subquery dimension in view', async () => runQueryTest({
    measures: ['accountOverview.count'],
    segments: ['accountOverview.hasNoTickets'],
  }, [{
    account_overview__count: '2',
  }]));

  it('segment with subquery dimension in view with dimension', async () => runQueryTest({
    measures: ['accountOverview.count'],
    segments: ['accountOverview.hasNoTickets'],
    dimensions: ['accountOverview.region'],
    order: [{ id: 'accountOverview.region' }],
  }, [{
    account_overview__region: 'EU',
    account_overview__count: '1',
  }, {
    account_overview__region: 'US',
    account_overview__count: '1',
  }]));
});
