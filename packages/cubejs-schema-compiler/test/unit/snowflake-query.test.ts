import moment from 'moment-timezone';
import { SnowflakeQuery } from '../../src/adapter/SnowflakeQuery';
import { BaseQuery } from '../../src/adapter/BaseQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('SnowflakeQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`visitors\`, {
      sql: \`select * from visitors\`,

      measures: {
        count: {
          type: 'count'
        },
      },

      dimensions: {
        id: {
          sql: 'id',
          type: 'number',
          primaryKey: true,
        },

        createdAt: {
          type: 'time',
          sql: 'created_at'
        },
      }
    })
    `);

  it('uses Snowflake DATE_PART EPOCH_SECOND for unix timestamps', () => compiler.compile().then(() => {
    const query = new SnowflakeQuery(
      { joinGraph, cubeEvaluator, compiler },
      { measures: ['visitors.count'] }
    );

    expect(query.unixTimestampSql()).toEqual('DATE_PART(\'EPOCH_SECOND\', CURRENT_TIMESTAMP)');
  }));

  it('uses the Snowflake unix timestamp in everyRefreshKeySql', () => compiler.compile().then(() => {
    const timezone = 'America/Los_Angeles';
    const query = new SnowflakeQuery(
      { joinGraph, cubeEvaluator, compiler },
      { measures: ['visitors.count'], timezone }
    );

    const utcOffset = moment.tz(timezone).utcOffset() * 60;

    expect(query.everyRefreshKeySql({ every: '1 hour', timezone }))
      .toEqual([`FLOOR((${utcOffset} + DATE_PART('EPOCH_SECOND', CURRENT_TIMESTAMP)) / 3600)`, false, expect.any(BaseQuery)]);

    expect(query.everyRefreshKeySql({ every: '0 10 * * *', timezone }))
      .toEqual([`FLOOR((${utcOffset} + DATE_PART('EPOCH_SECOND', CURRENT_TIMESTAMP) - 36000) / 86400)`, false, expect.any(BaseQuery)]);
  }));
});
