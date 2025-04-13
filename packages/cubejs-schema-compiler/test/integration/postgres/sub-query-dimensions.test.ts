import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Sub Query Dimensions', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
cube(\`A\`, {
  sql: \`
    SELECT 79 AS id, 1 AS foo_id UNION ALL
    SELECT 80 AS id, 2 AS foo_id UNION ALL
    SELECT 81 AS id, 3 AS foo_id UNION ALL
    SELECT 82 AS id, 4 AS foo_id UNION ALL
    SELECT 83 AS id, 5 AS foo_id UNION ALL
    SELECT 84 AS id, 6 AS foo_id
  \`,

  measures: {
    maxFooId: {
      sql: \`foo_id\`,
      type: 'max'
    }
  },

  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true,
    },

    fooId: {
      sql: \`\${CUBE}.foo_id\`,
      type: \`number\`,
    },
  },
});

cube(\`B\`, {
  sql: \`
    SELECT 100 AS id, 1 AS foo_id, 450 AS bar_id UNION ALL
    SELECT 101 AS id, 2 AS foo_id, 450 AS bar_id UNION ALL
    SELECT 102 AS id, 3 AS foo_id, 452 AS bar_id UNION ALL
    SELECT 103 AS id, 4 AS foo_id, 452 AS bar_id UNION ALL
    SELECT 104 AS id, 5 AS foo_id, 478 AS bar_id
  \`,

  joins: {
    A: {
      relationship: \`hasOne\`,
      sql: \`\${A}.foo_id = \${B}.foo_id\`,
    },
    C: {
      relationship: \`hasMany\`,
      sql: \`\${B}.bar_id = \${C}.bar_id AND \${B.fooId} > 3\`,
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

    fooId: {
      sql: \`\${A.maxFooId}\`,
      type: \`number\`,
      subQuery: true
    }
  }
});

cube(\`C\`, {
  sql: \`
    SELECT 789 AS id, 450 AS bar_id, 0.2 AS important_value UNION ALL
    SELECT 790 AS id, 450 AS bar_id, 0.3 AS important_value UNION ALL
    SELECT 791 AS id, 452 AS bar_id, 5.6 AS important_value UNION ALL
    SELECT 792 AS id, 452 AS bar_id, 5.6 AS important_value UNION ALL
    SELECT 793 AS id, 478 AS bar_id, 38.0 AS important_value UNION ALL
    SELECT 794 AS id, 478 AS bar_id, 43.5 AS important_value
  \`,

  measures: {
    importantValue: {
      sql: \`important_value\`,
      type: \`sum\`,
    },
  },

  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true,
    },

    barId: {
      sql: \`\${CUBE}.bar_id\`,
      type: \`number\`,
    },
  },
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

  it('inserted at the right place of a join', async () => runQueryTest({
    measures: ['C.importantValue'],
    dimensions: [
      'B.id'
    ],
    order: [{ id: 'B.id' }]
  }, [{
    b__id: 100,
    c__important_value: null,
  }, {
    b__id: 101,
    c__important_value: null,
  }, {
    b__id: 102,
    c__important_value: null,
  }, {
    b__id: 103,
    c__important_value: '11.2',
  }, {
    b__id: 104,
    c__important_value: '81.5',
  }]));
});
