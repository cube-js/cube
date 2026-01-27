import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Views Join Order using join maps', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(
    // language=JavaScript
    `
view(\`View\`, {
  description: 'A view',
  cubes: [
    {
      join_path: A,
      includes: ['id', 'name', 'c_name', 'd_name'],
      prefix: true,
    },
    {
      join_path: A.B.C.D,
      includes: ['id', 'name'],
      prefix: true,
    },
  ],
});

cube('A', {
  sql: \`
    SELECT 1 id, 'a'::text as "name"\`,
  joins: {
    B: {
      relationship: \`one_to_many\`,
      sql: \`\${CUBE.id} = \${B}.fk\`,
    },
    C: {
      relationship: \`one_to_many\`,
      sql: \`\${CUBE.id} = \${C}.fk_a\`,
    },
    D: {
      relationship: \`one_to_many\`,
      sql: \`\${CUBE.id} = \${D}.fk\`,
    },
  },
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`string\`,
      primaryKey: true,
    },
    name: {
      sql: \`\${CUBE}."name"\`,
      type: \`string\`,
    },
    c_name: {
      sql: \`\${C.name}\`,
      type: \`string\`,
    },
    d_name: {
      sql: \`\${D.name}\`,
      type: \`string\`,
    },
  },
});

cube('B', {
  sql: \`
    SELECT 2 id, 1 as fk, 'b'::text as "name"\`,
  joins: {
    C: {
      relationship: \`many_to_one\`,
      sql: \`\${CUBE.id} = \${C}.fk\`,
    },
  },

  dimensions: {
    id: {
      sql: \`id\`,
      type: \`string\`,
      primaryKey: true,
    },
    name: {
      sql: \`\${CUBE}."name"\`,
      type: \`string\`,
    },
  },
});

cube('C', {
  sql: \`
    SELECT 3 id, 2 as fk, 2 as fk_a, 'c1'::text as "name"
    UNION ALL
    SELECT 4 id, 3 as fk, 1 as fk_a, 'c2'::text as "name"\`,
  joins: {
    D: {
      relationship: \`many_to_one\`,
      sql: \`\${CUBE.id} = \${D}.fk_d\`,
    },
  },
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`string\`,
      primaryKey: true,
    },
    name: {
      sql: \`\${CUBE}."name"\`,
      type: \`string\`,
    },
  },
});

cube('D', {
  sql: \`
    SELECT 4 id, 1 as fk, 1 fk_d, 'd1'::text as "name"
    UNION ALL
    SELECT 5 id, 3 as fk, 3 fk_d, 'd3'::text as "name"\`,
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`string\`,
      primaryKey: true,
    },
    name: {
      sql: \`\${CUBE}."name"\`,
      type: \`string\`,
    },
  },
});
    `
  );

  it('querying A member proxied to leaf D', async () => {
    const [sql, _params] = await dbRunner.runQueryTest({
      dimensions: [
        'View.A_id',
        'View.A_name',
        'View.A_d_name',
      ],
      timeDimensions: [],
      segments: [],
      filters: [],
      total: true,
    }, [{
      view___a_id: 1,
      view___a_name: 'a',
      view___a_d_name: 'd3',
    }], { compiler, joinGraph, cubeEvaluator });

    expect(sql).toMatch(/AS "b"/);
    expect(sql).toMatch(/AS "c"/);
    expect(sql).toMatch(/AS "d"/);
    expect(sql).toMatch(/ON "a".id = "b".fk/);
    expect(sql).toMatch(/ON "b".id = "c".fk/);
    expect(sql).toMatch(/ON "c".id = "d".fk_d/);
    expect(sql).not.toMatch(/ON "a".id = "d".fk/);
  });

  it('querying A member proxied to non-leaf C', async () => {
    const [sql, _params] = await dbRunner.runQueryTest({
      dimensions: [
        'View.A_id',
        'View.A_name',
        'View.A_c_name',
      ],
      timeDimensions: [],
      segments: [],
      filters: [],
      total: true,
    }, [{
      view___a_id: 1,
      view___a_name: 'a',
      view___a_c_name: 'c1',
    }], { compiler, joinGraph, cubeEvaluator });

    expect(sql).toMatch(/AS "b"/);
    expect(sql).toMatch(/AS "c"/);
    expect(sql).toMatch(/ON "a".id = "b".fk/);
    expect(sql).toMatch(/ON "b".id = "c".fk/);
    expect(sql).not.toMatch(/ON "c".id = "d".fk_d/);
    expect(sql).not.toMatch(/AS "d"/);
    expect(sql).not.toMatch(/ON "a".id = "c".fk_a/);
  });

  it('querying A member proxied to non-leaf C', async () => {
    const [sql, _params] = await dbRunner.runQueryTest({
      dimensions: [
        'View.A_id',
        'View.A_name',
        'View.A_c_name',
        'View.A_d_name',
      ],
      timeDimensions: [],
      segments: [],
      filters: [],
      total: true,
    }, [{
      view___a_id: 1,
      view___a_name: 'a',
      view___a_c_name: 'c1',
      view___a_d_name: 'd3',
    }], { compiler, joinGraph, cubeEvaluator });

    expect(sql).toMatch(/AS "b"/);
    expect(sql).toMatch(/AS "c"/);
    expect(sql).toMatch(/AS "d"/);
    expect(sql).toMatch(/ON "a".id = "b".fk/);
    expect(sql).toMatch(/ON "b".id = "c".fk/);
    expect(sql).toMatch(/ON "c".id = "d".fk_d/);
    expect(sql).not.toMatch(/ON "a".id = "c".fk_a/);
    expect(sql).not.toMatch(/ON "a".id = "d".fk/);
  });
});
