import { prepareCompiler } from './PrepareCompiler';

describe('Schema Testing', () => {
  it('valid schemas', async () => {
    const { compiler, cubeEvaluator } = prepareCompiler(` 
      cube('CubeA', {
        sql: \`select * from test\`,
   
        measures: {
          count: {
            type: 'count'
          }
        },
  
        dimensions: {
          id: {
            type: 'number',
            sql: 'id',
            primaryKey: true
          },
          createdAt: {
            type: 'time',
            sql: 'created_at'
          },
        },

        preAggregations: {
            // Pre-aggregation without type, rollup is used by default
            countCreatedAt: {
                external: true,
                measureReferences: [count],
                timeDimensionReference: createdAt,
                granularity: \`day\`,
                partitionGranularity: \`month\`
            }
        },
      }) 
    `);
    await compiler.compile();

    expect(cubeEvaluator.preAggregationsForCube('CubeA')).toEqual({
      countCreatedAt: {
        external: true,
        granularity: 'day',
        measureReferences: expect.any(Function),
        timeDimensionReference: expect.any(Function),
        partitionGranularity: 'month',
        type: 'rollup',
      }
    });
  });
});
