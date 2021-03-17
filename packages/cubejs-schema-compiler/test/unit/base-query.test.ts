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
        }
      }
    }) 
    `);

  it('Test for parseIntervalToPairs', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'cards.count'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'America/Los_Angeles'
    });

    expect(query.parseIntervalToPairs('1')).toEqual([['1', 'SECOND']]);
    expect(query.parseIntervalToPairs('1 minute 12')).toEqual([['1', 'MINUTE'], ['12', 'SECOND']]);
    expect(query.parseIntervalToPairs('1 HOUR')).toEqual([['1', 'HOUR']]);
    expect(query.parseIntervalToPairs('1 hour')).toEqual([['1', 'hour']]);
    expect(query.parseIntervalToPairs('1 DAY 12 HOUR')).toEqual([['1', 'DAY'], ['12', 'HOUR']]);
    expect(query.parseIntervalToPairs('1 day 12 hour')).toEqual([['1', 'DAY'], ['12', 'HOUR']]);

    expect(() => query.parseIntervalToPairs('1 HEH')).toThrow(
      `Unsupported granularity in interval: 1 HEH`
    );
  });

  it('Test for everyRefreshKeySql', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'cards.count'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'America/Los_Angeles'
    });

    const utcOffset = moment.tz('America/Los_Angeles').utcOffset() * 60;
    expect(query.everyRefreshKeySql({
      every: '1 hour'
    })).toEqual('FLOOR((EXTRACT(EPOCH FROM NOW())) / 3600)');

    expect(query.everyRefreshKeySql({
      every: '0 * * * * *',
      timezone: 'America/Los_Angeles'
    })).toEqual(`FLOOR((${utcOffset} + 0 + EXTRACT(EPOCH FROM NOW())) / 60)`);

    expect(query.everyRefreshKeySql({
      every: '0 * * * *',
      timezone: 'America/Los_Angeles'
    })).toEqual(`FLOOR((${utcOffset} + 0 + EXTRACT(EPOCH FROM NOW())) / 3600)`);

    expect(query.everyRefreshKeySql({
      every: '30 * * * *',
      timezone: 'America/Los_Angeles'
    })).toEqual(`FLOOR((${utcOffset} + 1800 + EXTRACT(EPOCH FROM NOW())) / 3600)`);

    expect(query.everyRefreshKeySql({
      every: '30 5 * * 5',
      timezone: 'America/Los_Angeles'
    })).toEqual(`FLOOR((${utcOffset} + 365400 + EXTRACT(EPOCH FROM NOW())) / 604800)`);

    for (let i = 1; i < 59; i++) {
      expect(query.everyRefreshKeySql({
        every: `${i} * * * *`,
        timezone: 'America/Los_Angeles'
      })).toEqual(`FLOOR((${utcOffset} + ${i * 60} + EXTRACT(EPOCH FROM NOW())) / ${1 * 60 * 60})`);
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
