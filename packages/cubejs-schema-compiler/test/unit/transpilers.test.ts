import { prepareCompiler } from './PrepareCompiler';

describe('Transpilers', () => {
  it('CubeCheckDuplicatePropTranspiler', async () => {
    try {
      const { compiler } = prepareCompiler(`
        cube(\`Test\`, {
          sql: 'select * from test',
          dimensions: {
            test1: {
              type: 'number'
            },
            'test1': {
              type: 'number'
            },
            test2: {
              type: 'number'
            },
          }
        })
      `);

      await compiler.compile();

      throw new Error('Compile should thrown an error');
    } catch (e) {
      expect(e.message).toMatch(/Duplicate property parsing test1 in main.js/);
    }
  });

  it('ValidationTranspiler', async () => {
    const warnings: string[] = [];

    const { compiler } = prepareCompiler(`
        cube(\`Test\`, {
          sql: \`select * from test \${USER_CONTEXT.test1.filter('test1')}\`,
          dimensions: {
            test1: {
              type: 'number'
            },
          }
        });
      `, {
      errorReport: {
        logger: (msg) => {
          warnings.push(msg);
        }
      }
    });

    await compiler.compile();

    expect(warnings[0]).toMatch(/Warning: USER_CONTEXT was deprecated in favor of SECURITY_CONTEXT. in main.js/);
  });
});
