import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

describe('Circular member references', () => {
  const cube = (dimensions: string) => `
    cube('visitors', {
      sql: 'select * from visitors',

      measures: {
        count: { type: 'count' },
        half: { sql: \`\${count} / 2\`, type: 'number' },
      },

      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        createdAt: {
          sql: 'created_at',
          type: 'time',
          granularities: { fiscal_year: { interval: '1 year', offset: '1 month' } },
        },
        label: { sql: \`\${CUBE}.name || \${CUBE.id} || \${createdAt.fiscal_year}\`, type: 'string' },
        ${dimensions}
      },

      segments: {
        recent: { sql: \`\${CUBE.id} > 1\` },
      },
    });
  `;

  it('compiles members that reference other members without a cycle', async () => {
    const { compiler } = prepareJsCompiler(cube(`name: { sql: 'name', type: 'string' },`));
    await expect(compiler.compile()).resolves.not.toThrow();
  });

  it('reports a member that references itself', async () => {
    const { compiler } = prepareJsCompiler(cube(`foo: { sql: \`\${foo}\`, type: 'string' },`));
    await expect(compiler.compile()).rejects.toThrow(
      'Member \'visitors.foo\' references itself in its sql. A member can\'t reference itself.'
    );
  });

  it('reports a member that references itself through CUBE', async () => {
    const { compiler } = prepareJsCompiler(cube(`foo: { sql: \`\${CUBE.foo}\`, type: 'string' },`));
    await expect(compiler.compile()).rejects.toThrow('Member \'visitors.foo\' references itself in its sql.');
  });

  it('reports a cycle through other members with its full path', async () => {
    const { compiler } = prepareJsCompiler(cube(`
      a: { sql: \`\${b} || 'x'\`, type: 'string' },
      b: { sql: \`\${c}\`, type: 'string' },
      c: { sql: \`upper(\${a})\`, type: 'string' },
    `));
    await expect(compiler.compile()).rejects.toThrow(
      'Member \'visitors.a\' has a circular reference: visitors.a -> visitors.b -> visitors.c -> visitors.a.'
    );
  });

  it('reports a cycle between measures', async () => {
    const { compiler } = prepareJsCompiler(`
      cube('orders', {
        sql: 'select * from orders',
        measures: {
          x: { sql: \`\${y} + 1\`, type: 'number' },
          y: { sql: \`\${x} - 1\`, type: 'number' },
        },
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
        },
      });
    `);
    await expect(compiler.compile()).rejects.toThrow(
      'Member \'orders.x\' has a circular reference: orders.x -> orders.y -> orders.x.'
    );
  });

  it('reports a self reference in a YAML model', async () => {
    const { compiler } = prepareYamlCompiler(`
cubes:
  - name: visitors
    sql_table: visitors
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: foo
        sql: "{foo}"
        type: string
`);
    await expect(compiler.compile()).rejects.toThrow('Member \'visitors.foo\' references itself in its sql.');
  });
});
