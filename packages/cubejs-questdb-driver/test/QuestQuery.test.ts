import { QuestQuery } from '../src/QuestQuery';
import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

const prepareCompiler = (content: string, options: any[]) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'postgres', ...options });

describe('QuestQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        },

        unboundedCount: {
          type: 'count',
          rollingWindow: {
            trailing: 'unbounded'
          }
        }
      },

      dimensions: {
        createdAt: {
          type: 'time',
          sql: 'created_at'
        },
        name: {
          type: 'string',
          sql: 'name'
        }
      }
    });
    `, []);

  it('test equal filters', async () => {
    await compiler.compile();

    const filterValuesVariants = [
      [[true], 'WHERE ("visitors".name = $1)'],
      [[false], 'WHERE ("visitors".name = $1)'],
      [[''], 'WHERE ("visitors".name = $1)'],
      [[null], 'WHERE ("visitors".name = NULL)'],
    ];

    for (const [values, expected] of filterValuesVariants) {
      const query = new QuestQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [],
        filters: [{
          member: 'visitors.name',
          operator: 'equals',
          values
        }],
        timezone: 'America/Los_Angeles'
      });

      const queryAndParams = query.buildSqlAndParams();

      expect(queryAndParams[0]).toContain(expected);
    }
  });

  it('test non-positional order by',
    () => compiler.compile().then(() => {
      const query = new QuestQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-02'],
          },
        ],
        timezone: 'America/Los_Angeles',
        order: [
          {
            id: 'visitors.createdAt',
          },
        ],
      });

      const queryAndParams = query.buildSqlAndParams();

      expect(queryAndParams[0]).toContain('ORDER BY "visitors__created_at_day"');
    }));

  it('test non-positional group by',
    () => compiler.compile().then(() => {
      const query = new QuestQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-02'],
          },
        ],
        timezone: 'America/Los_Angeles',
        order: [
          {
            id: 'visitors.createdAt',
          },
        ],
      });

      const queryAndParams = query.buildSqlAndParams();

      expect(queryAndParams[0]).toContain('GROUP BY "visitors__created_at_day"');
    }));

  it('test query like',
    () => compiler.compile().then(() => {
      const query = new QuestQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [],
        filters: [
          {
            member: 'visitors.name',
            operator: 'contains',
            values: [
              'demo',
            ],
          },
        ],
      });

      const queryAndParams = query.buildSqlAndParams();

      expect(queryAndParams[0]).toContain('ILIKE \'%\' || $1 || \'%\'');
    }));

  it('test having filter',
    () => compiler.compile().then(() => {
      const query = new QuestQuery({ joinGraph, cubeEvaluator, compiler }, {
        dimensions: ['visitors.name'],
        measures: ['visitors.count'],
        filters: [
          {
            member: 'visitors.count',
            operator: 'gt',
            values: ['42']
          },
        ],
      });

      const queryAndParams = query.buildSqlAndParams();

      const expected = 'SELECT * FROM (SELECT\n' +
          '      "visitors".name "visitors__name", count(*) "visitors__count"\n' +
          '    FROM\n' +
          '      visitors AS "visitors"  GROUP BY "visitors__name") WHERE ("visitors__count" > $1) ORDER BY "visitors__count" DESC';
      expect(queryAndParams[0]).toEqual(expected);
      const expectedParams = ['42'];
      expect(queryAndParams[1]).toEqual(expectedParams);
    }));

  describe('dateBin (custom granularities)', () => {
    const buildQuery = () => new QuestQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
    });

    beforeAll(() => compiler.compile());

    it('generates timestamp_floor with the origin shifted back whole strides', () => {
      const query = buildQuery();

      // 2024-01-01 is 12288 months after the 1000-01-01 anchor, already a multiple
      // of 6, so the origin is shifted back exactly 12288 months (phase preserved).
      expect(query.dateBin('6 months', 't', '2024-01-01T00:00:00.000')).toEqual(
        "timestamp_floor('6M', t, dateadd('M', -12288, cast('2024-01-01T00:00:00.000' as timestamp)))"
      );

      // The shift is rounded up to a whole number of strides (12288 is a multiple of 2 too).
      expect(query.dateBin('2 months', 't', '2024-01-01T00:00:00.000')).toEqual(
        "timestamp_floor('2M', t, dateadd('M', -12288, cast('2024-01-01T00:00:00.000' as timestamp)))"
      );

      // Quarters are expressed as a 3-month stride.
      expect(query.dateBin('1 quarter', 't', '2024-01-01T00:00:00.000')).toEqual(
        "timestamp_floor('3M', t, dateadd('M', -12288, cast('2024-01-01T00:00:00.000' as timestamp)))"
      );

      // Year strides shift by whole years (2024 is 1024 years after the anchor).
      expect(query.dateBin('2 years', 't', '2024-01-01T00:00:00.000')).toEqual(
        "timestamp_floor('2y', t, dateadd('y', -1024, cast('2024-01-01T00:00:00.000' as timestamp)))"
      );

      // An origin already before the anchor needs no shift.
      expect(query.dateBin('6 months', 't', '0900-06-15T00:00:00.000')).toEqual(
        "timestamp_floor('6M', t, cast('0900-06-15T00:00:00.000' as timestamp))"
      );
    });

    it('throws for granularities it cannot express', () => {
      const query = buildQuery();

      // Compound intervals have no single-unit QuestDB timestamp_floor stride
      // (parseInterval only accepts a single unit).
      expect(() => query.dateBin('3 month 3 days 3 hours', 't', '2024-01-01T00:00:00.000'))
        .toThrow(/Invalid interval/);

      // The origin must be a parseable timestamp.
      expect(() => query.dateBin('6 months', 't', 'not-a-timestamp'))
        .toThrow(/unparseable origin/);

      // A sub-hour stride over ~1000 years needs a shift beyond dateadd()'s int32 offset.
      expect(() => query.dateBin('1 second', 't', '2024-01-01T00:00:00.000'))
        .toThrow(/32-bit range/);
    });
  });
});
