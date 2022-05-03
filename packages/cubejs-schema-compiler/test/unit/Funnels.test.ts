import R from 'ramda';
import { Funnels } from '../../src';
import { prepareCompiler } from './PrepareCompiler';

describe('Funnels', () => {
  it('schema', async () => {
    const compilers = prepareCompiler(
      `
        const Funnels = require('Funnels');
        cube('funnels', {
          extends: Funnels.eventFunnel({
            userId: { sql: 'userid' },
            time: { sql: 'timestamp' },
            steps: [
               { name: 'A', eventsView: { sql: 'A_SQL' } },
               { name: 'B', eventsView: { sql: 'B_SQL' } },
            ],
          })
        })
      `,
      {}
    );
    await compilers.compiler.compile();
    const funnelCube = compilers.cubeEvaluator.cubeFromPath('funnels');
    expect(R.map(m => m.type, funnelCube.measures)).toEqual(
      {
        conversions: 'count',
        firstStepConversions: 'countDistinct',
        conversionsPercent: 'number',
      }
    );
    expect(R.map(d => d.type, funnelCube.dimensions)).toEqual(
      {
        id: 'string',
        userId: 'string',
        firstStepUserId: 'string',
        time: 'time',
        step: 'string',
      }
    );
  });

  it('useApprox', async () => {
    const compilers = prepareCompiler(
      `
        const Funnels = require('Funnels');
        cube('funnels', {
          extends: Funnels.eventFunnel({
            useApprox: true,
            userId: { sql: 'userid' },
            time: { sql: 'timestamp' },
            steps: [
               { name: 'A', eventsView: { sql: 'A_SQL' } },
               { name: 'B', eventsView: { sql: 'B_SQL' } },
            ],
          })
        })
      `,
      {}
    );
    await compilers.compiler.compile();
    const funnelCube = compilers.cubeEvaluator.cubeFromPath('funnels');
    expect(R.map(m => m.type, funnelCube.measures)).toEqual(
      {
        conversions: 'count',
        firstStepConversions: 'countDistinctApprox',
        conversionsPercent: 'number',
      }
    );
  });
});
