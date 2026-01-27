import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

/**
 * This tests the cube join correctness for cases, when there are
 * multiple equal-cost paths between few cubes via transitive joins.
 */

describe('Views Join Order 3', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(
    // language=JavaScript
    `
cube(\`D\`, {
  sql: \`SELECT 1 id, 125 as balance\`,
  dimensions: {
    Balance: {
      sql: \`balance\`,
      type: \`number\`
    }
  }
});

cube(\`A\`, {
  sql: \`SELECT 1 id, 250 as balance\`,
  joins: {
    E: {
      sql: \`\${CUBE}.id = \${E}.id\`,
      relationship: \`many_to_one\`
    },
    B: {
      sql: \`\${CUBE}.id = \${B}.id\`,
      relationship: \`many_to_one\`
    },
    C: {
      sql: \`\${CUBE}.id = \${C}.id\`,
      relationship: \`many_to_one\`
    }
  },
  dimensions: {
    Balance: {
      sql: \`balance\`,
      type: \`number\`
    }
  }
});

cube('B', {
  sql: \`SELECT 1 id\`,
  joins: {
    D: {
      sql: \`\${CUBE}.id = \${D}.id\`,
      relationship: \`many_to_one\`
    },
    E: {
      sql: \`\${CUBE}.id = \${E}.id\`,
      relationship: \`many_to_one\`
    }
  },
  dimensions: {
    ActivityBalance: {
      sql: \`\${D.Balance}\`,
      type: \`number\`
    }
  }
});

cube(\`E\`, {
  sql: \`SELECT 1 id, 1 as plan_id, 1 as party_id\`,
  joins: {
    D: {
      sql: \`\${CUBE}.id = \${D}.id\`,
      relationship: \`many_to_one\`
    },
    F: {
      sql: \`\${CUBE}.plan_id = \${F}.plan_id\`,
      relationship: \`many_to_one\`
    },
    C: {
      sql: \`\${CUBE}.party_id = \${C}.party_id\`,
      relationship: \`many_to_one\`
    }
  }
});

cube('C', {
  sql: \`SELECT 1 id, 1 as plan_id, 1 as party_id\`,
  joins: {
    F: {
      sql: \`\${CUBE}.plan_id = \${F}.plan_id\`,
      relationship: \`many_to_one\`
    }
  }
});

cube(\`F\`, {
  sql: \`SELECT 1 id, 1 as plan_id, 'PLAN_CODE'::text as plan_code\`,
  dimensions: {
    PlanCode: {
      sql: \`plan_code\`,
      type: \`string\`
    }
  }
});

view(\`V\`, {
  cubes: [
    {
      join_path: A.B,
      includes: [\`ActivityBalance\`]
    },
    {
      join_path: A.C.F,
      includes: [\`PlanCode\`]
    }
  ]
});
    `
  );

  it('correct join for simple cube B dimension', async () => {
    const [sql, _params] = await dbRunner.runQueryTest({
      dimensions: ['B.ActivityBalance'],
      timeDimensions: [],
      segments: [],
      filters: [],
    }, [{
      b___activity_balance: 125,
    }], { compiler, joinGraph, cubeEvaluator });

    expect(sql).toMatch(/AS "b"/);
    expect(sql).toMatch(/AS "d"/);
    expect(sql).toMatch(/ON "b".id = "d".id/);
    expect(sql).not.toMatch(/AS "a"/);
    expect(sql).not.toMatch(/AS "e"/);
    expect(sql).not.toMatch(/AS "c"/);
  });

  it('correct join for simple view B-dimension', async () => dbRunner.runQueryTest({
    dimensions: ['V.ActivityBalance'],
    timeDimensions: [],
    segments: [],
    filters: [],
  }, [{
    v___activity_balance: 125,
  }], { compiler, joinGraph, cubeEvaluator }));

  it('correct join for simple view F-dimension', async () => dbRunner.runQueryTest({
    dimensions: ['V.PlanCode'],
    timeDimensions: [],
    segments: [],
    filters: [],
  }, [{
    v___plan_code: 'PLAN_CODE',
  }], { compiler, joinGraph, cubeEvaluator }));

  it('correct join for view F-dimension + B-dimension', async () => dbRunner.runQueryTest({
    dimensions: ['V.PlanCode', 'V.ActivityBalance'],
    timeDimensions: [],
    segments: [],
    filters: [],
  }, [{
    v___plan_code: 'PLAN_CODE',
    v___activity_balance: 125,
  }], { compiler, joinGraph, cubeEvaluator }));

  it('correct join for view B-dimension + F-dimension', async () => dbRunner.runQueryTest({
    dimensions: ['V.ActivityBalance', 'V.PlanCode'],
    timeDimensions: [],
    segments: [],
    filters: [],
  }, [{
    v___activity_balance: 125,
    v___plan_code: 'PLAN_CODE',
  }], { compiler, joinGraph, cubeEvaluator }));
});
