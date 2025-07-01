import { prepareJsCompiler } from './PrepareCompiler';

describe('Test Speed', () => {
  it('100 cube', async () => {
    return; // uncomment this to run the test

    const measures: any = {};

    const dimensions: any = {
      created: {
        sql: 'created',
        type: 'time'
      }
    };
    const preAggregations: any = {};
    const mTypes = ['count', 'countDistinct', 'sum'];
    const dTypes = ['number', 'string', 'time'];

    for (let i = 0; i < 100; i++) {
      measures[`m${i}`] = {
        type: mTypes[i % mTypes.length],
        sql: `m${i}`
      };
      dimensions[`d${i}`] = {
        type: dTypes[i % dTypes.length],
        sql: `d${i}`
      };
      preAggregations[`pa${i}`] = {
        sqlAlias: `pa${i}`,
        type: 'rollup',
        measures: [`m${i}`],
        dimensions: [`d${i}`],
        timeDimension: 'created',
        granularity: 'day',
        partitionGranularity: 'month',
        refreshKey: {
          every: '1 day',
          incremental: true
        },
        buildRangeStart: { sql: 'SELECT DATE(\'2020-01-01\')' },
        buildRangeEnd: { sql: 'SELECT NOW()' }
      };
    }

    const cube = {
      sql: 'select * from table',
      dataSource: 'default',
      measures,
      dimensions,
      preAggregations,
    };

    const cubeString = `cube('cube100', ${JSON.stringify(cube, null, 2)});`
      .replace(/"([^"]+)":/g, '$1:');

    const startTime = +new Date();
    for (let i = 0; i < 10; i++) {
      const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(cubeString);
      const result = await compiler.compile();
    }
    const endTime = +new Date();

    console.log('Cube Compile Time:', (endTime - startTime) / 1000.0);
  });
});
