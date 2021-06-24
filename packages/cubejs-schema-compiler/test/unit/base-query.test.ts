import moment from 'moment-timezone';
import { UserError } from '../../src/compiler/UserError';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareCompiler } from './PrepareCompiler';

describe('SQL Generation', () => {
  // this.timeout(90000);

  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(` 
    cube('cards', {
      sql: \`
      select * from cards
      \`,
 
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
      }
    }) 
    `);

  it('Test time series with different granularity', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'cards.count'
      ],
      timeDimensions: [],
      filters: [],
    });

    {
      const timeDimension = query.newTimeDimension({
        dimension: 'cards.createdAt',
        granularity: 'day',
        dateRange: ['2021-01-01', '2021-01-02']
      });
      expect(timeDimension.timeSeries()).toEqual([
        ['2021-01-01T00:00:00.000', '2021-01-01T23:59:59.999'],
        ['2021-01-02T00:00:00.000', '2021-01-02T23:59:59.999']
      ]);
    }

    {
      const timeDimension = query.newTimeDimension({
        dimension: 'cards.createdAt',
        granularity: 'day',
        dateRange: ['2021-01-01', '2021-01-02']
      });
      expect(timeDimension.timeSeries()).toEqual([
        ['2021-01-01T00:00:00.000', '2021-01-01T23:59:59.999'],
        ['2021-01-02T00:00:00.000', '2021-01-02T23:59:59.999']
      ]);
    }

    {
      const timeDimension = query.newTimeDimension({
        dimension: 'cards.createdAt',
        granularity: 'hour',
        dateRange: ['2021-01-01', '2021-01-01']
      });
      expect(timeDimension.timeSeries()).toEqual(
        new Array(24).fill(null).map((v, index) => [
          `2021-01-01T${index.toString().padStart(2, '0')}:00:00.000`,
          `2021-01-01T${index.toString().padStart(2, '0')}:59:59.999`
        ])
      );
    }

    {
      const timeDimension = query.newTimeDimension({
        dimension: 'cards.createdAt',
        granularity: 'minute',
        // for 1 hour only
        dateRange: ['2021-01-01T00:00:00.000', '2021-01-01T00:59:59.999']
      });
      expect(timeDimension.timeSeries()).toEqual(
        new Array(60).fill(null).map((v, index) => [
          `2021-01-01T00:${index.toString().padStart(2, '0')}:00.000`,
          `2021-01-01T00:${index.toString().padStart(2, '0')}:59.999`
        ])
      );
    }

    {
      const timeDimension = query.newTimeDimension({
        dimension: 'cards.createdAt',
        granularity: 'second',
        // for 1 minute only
        dateRange: ['2021-01-01T00:00:00.000', '2021-01-01T00:00:59.000']
      });
      expect(timeDimension.timeSeries()).toEqual(
        new Array(60).fill(null).map((v, index) => [
          `2021-01-01T00:00:${index.toString().padStart(2, '0')}.000`,
          `2021-01-01T00:00:${index.toString().padStart(2, '0')}.999`
        ])
      );
    }
  });

  it('Test for everyRefreshKeySql', async () => {
    await compiler.compile();

    const timezone = 'America/Los_Angeles';
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'cards.count'
      ],
      timeDimensions: [],
      filters: [],
      timezone,
    });
    //
    const utcOffset = moment.tz('America/Los_Angeles').utcOffset() * 60;
    expect(query.everyRefreshKeySql({
      every: '1 hour'
    })).toEqual('FLOOR((EXTRACT(EPOCH FROM NOW())) / 3600)');

    // Standard syntax (minutes hours day month dow)
    expect(query.everyRefreshKeySql({ every: '0 * * * *', timezone }))
      .toEqual(`FLOOR((${utcOffset} + 0 + EXTRACT(EPOCH FROM NOW())) / 3600)`);

    expect(query.everyRefreshKeySql({ every: '0 10 * * *', timezone }))
      .toEqual(`FLOOR((${utcOffset} + 36000 + EXTRACT(EPOCH FROM NOW())) / 86400)`);

    // Additional syntax with seconds (seconds minutes hours day month dow)
    expect(query.everyRefreshKeySql({ every: '0 * * * * *', timezone, }))
      .toEqual(`FLOOR((${utcOffset} + 0 + EXTRACT(EPOCH FROM NOW())) / 60)`);

    expect(query.everyRefreshKeySql({ every: '0 * * * *', timezone }))
      .toEqual(`FLOOR((${utcOffset} + 0 + EXTRACT(EPOCH FROM NOW())) / 3600)`);

    expect(query.everyRefreshKeySql({ every: '30 * * * *', timezone }))
      .toEqual(`FLOOR((${utcOffset} + 1800 + EXTRACT(EPOCH FROM NOW())) / 3600)`);

    expect(query.everyRefreshKeySql({ every: '30 5 * * 5', timezone }))
      .toEqual(`FLOOR((${utcOffset} + 365400 + EXTRACT(EPOCH FROM NOW())) / 604800)`);

    for (let i = 1; i < 59; i++) {
      expect(query.everyRefreshKeySql({ every: `${i} * * * *`, timezone }))
        .toEqual(`FLOOR((${utcOffset} + ${i * 60} + EXTRACT(EPOCH FROM NOW())) / ${1 * 60 * 60})`);
    }

    try {
      query.everyRefreshKeySql({
        every: '*/9 */7 * * *',
        timezone: 'America/Los_Angeles'
      });

      throw new Error();
    } catch (error) {
      expect(error).toBeInstanceOf(UserError);
    }
  });
});
