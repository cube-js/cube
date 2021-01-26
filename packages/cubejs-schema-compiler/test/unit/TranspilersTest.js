/* globals it,describe */
/* eslint-disable quote-props */
import { prepareCompiler } from './PrepareCompiler';

require('should');

describe('Transpilers', async () => {
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
      e.should.be.match(/Duplicate property parsing test1 in main.js/);
    }
  });

  it('ValidationTranspiler', async () => {
    const warnings = [];

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

    warnings[0].should.startWith('Warning: USER_CONTEXT was deprecated in flavour of SECURITY_CONTEXT. in main.js');
  });
});
