import { prepareCompiler } from './PrepareCompiler';

describe('Repro #11260 - view extends order dependency', () => {
  const testCubeContent = `
cube(\`test\`, {
    public: false,
    sql: \`select 1 as id, 'test' as name\`,

    dimensions: {
        id: {
            sql: \`id\`,
            type: \`number\`
        },
        name: {
            sql: \`name\`,
            type: \`string\`
        }
    }
});
`;

  const viewAContent = `
view(\`a_view\`, {
    public: true,
    cubes: [
        {
            join_path: \`test\`,
            includes: [
                "id",
                "name"
            ]
        }
    ]
});
`;

  const viewBContent = `
view(\`b_view\`, {
    public: true,
    extends: a_view,
    cubes: []
});
`;

  it('compiles when base view (a_view) file comes before subview (b_view) file', async () => {
    const { compiler } = prepareCompiler([
      { content: testCubeContent, fileName: 'test.js' },
      { content: viewAContent, fileName: 'view_a.js' },
      { content: viewBContent, fileName: 'view_b.js' },
    ]);

    await compiler.compile();
  });

  it('compiles when subview (b_view) file comes before base view (a_view) file', async () => {
    const { compiler } = prepareCompiler([
      { content: testCubeContent, fileName: 'test.js' },
      { content: viewBContent, fileName: 'view_b.js' },
      { content: viewAContent, fileName: 'view_a.js' },
    ]);

    await compiler.compile();
  });
});
