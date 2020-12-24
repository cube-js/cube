/* globals it,describe */
/* eslint-disable quote-props */
const PrepareCompiler = require('./PrepareCompiler');
require('should');

const { prepareCompiler } = PrepareCompiler;

describe('Transpilers', () => {
  it('CubeCheckDuplicatePropTranspiler', async () => {
    const { compiler } = prepareCompiler(`
      cube(\`Test\`, {
        extends: VisitorsFunnel,
  
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

    return compiler.compile()
      .then(() => {
        throw new Error('CubeCheckDuplicatePropTranspiler not working');
      })
      .catch((e) => e.should.be.match(/Duplicate property parsing test1/));
  });
});
