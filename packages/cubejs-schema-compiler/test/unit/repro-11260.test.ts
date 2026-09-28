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

  const expectBViewInheritsDimensions = (metaTransformer: any) => {
    const bViewMeta = metaTransformer.cubes.map((def: any) => def.config).find((def: any) => def.name === 'b_view');
    const dimensionNames = (bViewMeta.dimensions || []).map((d: any) => d.name).sort();
    expect(dimensionNames).toEqual(['b_view.id', 'b_view.name']);
  };

  it('compiles when base view (a_view) file comes before subview (b_view) file', async () => {
    const { compiler, metaTransformer } = prepareCompiler([
      { content: testCubeContent, fileName: 'test.js' },
      { content: viewAContent, fileName: 'view_a.js' },
      { content: viewBContent, fileName: 'view_b.js' },
    ]);

    await compiler.compile();
    compiler.throwIfAnyErrors();

    expectBViewInheritsDimensions(metaTransformer);
  });

  it('compiles when subview (b_view) file comes before base view (a_view) file', async () => {
    const { compiler, metaTransformer } = prepareCompiler([
      { content: testCubeContent, fileName: 'test.js' },
      { content: viewBContent, fileName: 'view_b.js' },
      { content: viewAContent, fileName: 'view_a.js' },
    ]);

    await compiler.compile();
    compiler.throwIfAnyErrors();

    expectBViewInheritsDimensions(metaTransformer);
  });
});
