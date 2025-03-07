import { getEnv } from '@cubejs-backend/shared';
import { prepareCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Views Join Order 2', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
view(\`View\`, {
  description: 'A view',
  cubes: [
    {
      join_path: A,
      includes: ['id', 'name'],
      prefix: true,
    },
    {
      join_path: A.D,
      includes: ['id', 'name'],
      prefix: true,
    },
    {
      join_path: A.D.B,
      includes: ['id', 'name'],
      prefix: true,
    },
    {
      join_path: A.D.E,
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
  },
});

cube('B', {
  sql: \`
    SELECT 2 id, 'b'::text as "name"\`,
  joins: {
    A: {
      relationship: \`many_to_one\`,
      sql: \`\${CUBE.fk} = \${A.id}\`,
    },
    E: {
      sql: \`\${CUBE.name} = \${E.id}\`,
      relationship: \`many_to_one\`,
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
    fk: {
      sql: \`fk\`,
      type: \`string\`,
    },
  },
});

cube('E', {
  sql: \`
    SELECT 4 id, 'e'::text as "name"\`,
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
    SELECT 3 id, 'd'::text as "name", 1 fk, 2 b_fk, 4 e_fk\`,
  joins: {
    B: {
      relationship: \`one_to_one\`,
      sql: \`\${CUBE}.b_fk = \${B}.id\`,
    },
    A: {
      relationship: \`many_to_one\`,
      sql: \`\${CUBE}.fk = \${A}.id\`,
    },
    E: {
      relationship: \`many_to_one\`,
      sql: \`\${CUBE}.e_fk = \${E}.id\`,
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
    fk: {
      sql: \`fk\`,
      type: \`string\`,
    },
    bFk: {
      sql: \`b_fk\`,
      type: \`string\`,
    },
  },
});
    `);

  if (getEnv('nativeSqlPlanner')) {
    it('join order', async () => dbRunner.runQueryTest({
      dimensions: [
        'View.A_id',
        'View.A_name',
        'View.B_id',
        'View.B_name',
        'View.D_id',
        'View.D_name',
        'View.E_id',
        'View.E_name'
      ],
      timeDimensions: [],
      segments: [],
      filters: [],
      total: true,
      renewQuery: false,
      limit: 1
    }, [{
      view__a_id: 1,
      view__a_name: 'a',
      view__b_id: 2,
      view__b_name: 'b',
      view__d_id: 3,
      view__d_name: 'd',
      view__e_id: 4,
      view__e_name: 'e',
    }], { compiler, joinGraph, cubeEvaluator }));
  } else {
    it('join order', async () => dbRunner.runQueryTest({
      dimensions: [
        'View.A_id',
        'View.A_name',
        'View.B_id',
        'View.B_name',
        'View.D_id',
        'View.D_name',
        'View.E_id',
        'View.E_name'
      ],
      timeDimensions: [],
      segments: [],
      filters: [],
      total: true,
      renewQuery: false,
      limit: 1
    }, [{
      view___a_id: 1,
      view___a_name: 'a',
      view___b_id: 2,
      view___b_name: 'b',
      view___d_id: 3,
      view___d_name: 'd',
      view___e_id: 4,
      view___e_name: 'e',
    }], { compiler, joinGraph, cubeEvaluator }));
  }
});
