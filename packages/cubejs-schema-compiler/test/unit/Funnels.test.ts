import R from 'ramda';
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
               { name: 'A', eventsView: { sql: 'SQL_A' } },
               { name: 'B', eventsView: { sql: 'SQL_B' } },
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
               { name: 'A', eventsView: { sql: 'SQL_A' } },
               { name: 'B', eventsView: { sql: 'SQL_B' } },
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
