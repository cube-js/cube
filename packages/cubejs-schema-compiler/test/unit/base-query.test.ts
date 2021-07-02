import moment from 'moment-timezone';
import { UserError } from '../../src/compiler/UserError';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareCompiler } from './PrepareCompiler';
import { MssqlQuery } from '../../src/adapter/MssqlQuery';

describe('SQL Generation', () => {
  // this.timeout(90000);

  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(` 
    cube('cards', {
      sql: \`
      select * from cards
      \`,
      
      refreshKey: {
        every: '10 minute',
      },
 
      measures: {
        count: {
          type: 'count'
        },
        sum: {
          sql: \`amount\`,
          type: \`sum\`
        },
        max: {
          sql: \`amount\`,
          type: \`max\`
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
          countCreatedAt: {
              external: true,
              measureReferences: [count],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              refreshKey: {
                every: '1 hour',
              },
              scheduledRefresh: true,
          },
          maxCreatedAt: {
              external: true,
              measureReferences: [max],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              refreshKey: {
                sql: 'SELECT MAX(created_at) FROM cards',
              },
              scheduledRefresh: true,
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
    })).toEqual(['FLOOR((EXTRACT(EPOCH FROM NOW())) / 3600)', false]);

    // Standard syntax (minutes hours day month dow)
    expect(query.everyRefreshKeySql({ every: '0 * * * *', timezone }))
      .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 0) / 3600)`, false]);

    expect(query.everyRefreshKeySql({ every: '0 10 * * *', timezone }))
      .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 36000) / 86400)`, false]);

    // Additional syntax with seconds (seconds minutes hours day month dow)
    expect(query.everyRefreshKeySql({ every: '0 * * * * *', timezone, }))
      .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 0) / 60)`, false]);

    expect(query.everyRefreshKeySql({ every: '0 * * * *', timezone }))
      .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 0) / 3600)`, false]);

    expect(query.everyRefreshKeySql({ every: '30 * * * *', timezone }))
      .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 1800) / 3600)`, false]);

    expect(query.everyRefreshKeySql({ every: '30 5 * * 5', timezone }))
      .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 365400) / 604800)`, false]);

    for (let i = 1; i < 59; i++) {
      expect(query.everyRefreshKeySql({ every: `${i} * * * *`, timezone }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - ${i * 60}) / ${1 * 60 * 60})`, false]);
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

  it('cacheKeyQueries for cube with refreshKey.every (source)', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'cards.sum'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'America/Los_Angeles',
    });

    // Query should not match any pre-aggregation!
    expect(query.cacheKeyQueries()).toEqual([
      [
        // Postgres dialect
        "SELECT FLOOR((EXTRACT(EPOCH FROM NOW())) / 600)",
        [],
        {
          // false, because there is no externalQueryClass
          external: false,
          renewalThreshold: 60,
        }
      ]
    ]);
  });

  it('cacheKeyQueries for cube with refreshKey.every (external)', async () => {
    await compiler.compile();

    // Query should not match any pre-aggregation!
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'cards.sum'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'America/Los_Angeles',
      externalQueryClass: MssqlQuery
    });

    // Query should not match any pre-aggregation!
    expect(query.cacheKeyQueries()).toEqual([
      [
        // MSSQL dialect, because externalQueryClass
        "SELECT FLOOR((DATEDIFF(SECOND,'1970-01-01', GETUTCDATE())) / 600)",
        [],
        {
          // true, because externalQueryClass
          external: true,
          renewalThreshold: 60,
        }
      ]
    ]);
  });

  /**
   * Testing: pre-aggregation which use refreshKey.every & external database defined, should be executed in
   * external database
   */
  it('preAggregationsDescription for query - refreshKey every (external)', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'cards.count'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'America/Los_Angeles',
      externalQueryClass: MssqlQuery
    });

    const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
    expect(preAggregations.length).toEqual(1);
    expect(preAggregations[0].invalidateKeyQueries).toEqual([
      [
        // MSSQL dialect
        "SELECT FLOOR((DATEDIFF(SECOND,'1970-01-01', GETUTCDATE())) / 3600)",
        [],
        {
          external: true,
          renewalThreshold: 300,
        }
      ]
    ]);
  });

  /**
   * Testing: preAggregation which has refresh.sql, should be executed in source db
   */
  it('preAggregationsDescription for query - refreshKey manually (external)', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'cards.max'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'America/Los_Angeles',
      externalQueryClass: MssqlQuery
    });

    const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
    expect(preAggregations.length).toEqual(1);
    expect(preAggregations[0].invalidateKeyQueries).toEqual([
      [
        "SELECT MAX(created_at) FROM cards",
        [],
        {
          external: false,
          renewalThreshold: 10,
        }
      ]
    ]);
  });
});
