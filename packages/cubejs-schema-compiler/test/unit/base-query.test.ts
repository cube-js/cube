import moment from 'moment-timezone';
import { BaseQuery, PostgresQuery, MssqlQuery, UserError, CubeStoreQuery } from '../../src';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';
import {
  createCubeSchema,
  createCubeSchemaWithCustomGranularitiesAndTimeShift,
  createCubeSchemaYaml,
  createECommerceSchema,
  createJoinedCubesSchema,
  createSchemaYaml,
  createSchemaYamlForGroupFilterParamsTests
} from './utils';
import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';

describe('SQL Generation', () => {
  describe('Common - Yaml - syntax sugar', () => {
    const compilers = /** @type Compilers */ prepareYamlCompiler(
      createCubeSchemaYaml({ name: 'cards', sqlTable: 'card_tbl' })
    );

    it('Simple query', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        timeDimensions: [],
        filters: [],
      });
      const queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain('card_tbl');
    });
  });

  describe('Common - JS - syntax sugar', () => {
    const compilers = /** @type Compilers */ prepareJsCompiler(
      createCubeSchema({
        name: 'cards',
        sqlTable: 'card_tbl'
      })
    );

    it('Simple query - count measure', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        timeDimensions: [],
        filters: [],
      });
      const queryAndParams = query.buildSqlAndParams();
      const expected = 'SELECT\n' +
          '      count("cards".id) "cards__count"\n' +
          '    FROM\n' +
          '      card_tbl AS "cards" ';
      expect(queryAndParams[0]).toContain('card_tbl');
      expect(queryAndParams[0]).toEqual(expected);
    });

    it('Simple query - sum measure', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.sum'
        ],
        timeDimensions: [],
        filters: [],
      });
      const queryAndParams = query.buildSqlAndParams();
      const expected = 'SELECT\n' +
          '      sum("cards".amount) "cards__sum"\n' +
          '    FROM\n' +
          '      card_tbl AS "cards" ';
      expect(queryAndParams[0]).toContain('card_tbl');
      expect(queryAndParams[0]).toEqual(expected);
    });

    it('Simple query - dimension', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        dimensions: [
          'cards.type'
        ],
        timeDimensions: [],
        filters: [],
      });
      const queryAndParams = query.buildSqlAndParams();
      const expected = 'SELECT\n' +
          '      "cards".type "cards__type", count("cards".id) "cards__count"\n' +
          '    FROM\n' +
          '      card_tbl AS "cards"  GROUP BY 1 ORDER BY 2 DESC';
      expect(queryAndParams[0]).toEqual(expected);
    });
    it('Simple query - time dimension', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        dimensions: [
          'cards.type'
        ],
        timeDimensions: [
          {
            dimension: 'cards.createdAt',
            granularity: 'day',
            dateRange: ['2021-01-01', '2021-01-02']
          }
        ],
        timezone: 'America/Los_Angeles',
        filters: [],
      });

      const queryAndParams = query.buildSqlAndParams();

      expect(queryAndParams[0]).toContain('"cards".type "cards__type", date_trunc(\'day\', ("cards".created_at::timestamptz AT TIME ZONE \'America/Los_Angeles\')) "cards__created_at_day"');
      expect(queryAndParams[0]).toContain('GROUP BY 1, 2');
      expect(queryAndParams[0]).toContain('ORDER BY 2');
    });
    it('Simple query - complex measure', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.diff'
        ],
        filters: [],
      });

      const queryAndParams = query.buildSqlAndParams();
      const expected = 'SELECT\n' +
          '      max("cards".amount) - min("cards".amount) "cards__diff"\n' +
          '    FROM\n' +
          '      card_tbl AS "cards" ';
      expect(queryAndParams[0]).toEqual(expected);
    });
    it('Simple query - complex dimension', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        dimensions: [
          'cards.type_complex'
        ],
        measures: [
          'cards.diff'
        ],
        filters: [],
      });

      const queryAndParams = query.buildSqlAndParams();
      const expected = 'SELECT\n' +
          '      CONCAT("cards".type, \' \', "cards".location) "cards__type_complex", max("cards".amount) - min("cards".amount) "cards__diff"\n' +
          '    FROM\n' +
          '      card_tbl AS "cards"  GROUP BY 1 ORDER BY 2 DESC';
      expect(queryAndParams[0]).toEqual(expected);
    });
    it('Simple query - CUBE dimension', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        dimensions: [
          'cards.type_with_cube'
        ],
        measures: [
          'cards.diff'
        ],
        filters: [],
      });

      const queryAndParams = query.buildSqlAndParams();
      const expected = 'SELECT\n' +
          '      "cards".type "cards__type_with_cube", max("cards".amount) - min("cards".amount) "cards__diff"\n' +
          '    FROM\n' +
          '      card_tbl AS "cards"  GROUP BY 1 ORDER BY 2 DESC';
      expect(queryAndParams[0]).toEqual(expected);
    });
    it('Simple query - CUBE id', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        dimensions: [
          'cards.id_cube'
        ],
        measures: [
          'cards.diff'
        ],
        filters: [],
      });

      const queryAndParams = query.buildSqlAndParams();
      const expected = 'SELECT\n' +
          '      "cards".id "cards__id_cube", max("cards".amount) - min("cards".amount) "cards__diff"\n' +
          '    FROM\n' +
          '      card_tbl AS "cards"  GROUP BY 1 ORDER BY 2 DESC';
      expect(queryAndParams[0]).toEqual(expected);
    });
    it('Simple query - simple filter', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        dimensions: [
          'cards.type'
        ],
        measures: [
          'cards.count'
        ],
        filters: [
          {
            or: [
              {
                member: 'cards.type',
                operator: 'equals',
                values: ['type_value']
              },
              {
                member: 'cards.type',
                operator: 'notEquals',
                values: ['not_type_value']
              },

            ]

          },
          {
            member: 'cards.type',
            operator: 'equals',
            values: ['type_value']
          }],
      });

      const queryAndParams = query.buildSqlAndParams();
      const expected = 'SELECT\n' +
          '      "cards".type "cards__type", count("cards".id) "cards__count"\n' +
          '    FROM\n' +
          '      card_tbl AS "cards"  WHERE (("cards".type = $1) OR ("cards".type <> $2 OR "cards".type IS NULL)) AND ("cards".type = $3) GROUP BY 1 ORDER BY 2 DESC';
      expect(queryAndParams[0]).toEqual(expected);
      const expectedParams = ['type_value', 'not_type_value', 'type_value'];
      expect(queryAndParams[1]).toEqual(expectedParams);
    });
    it('Simple query - null and many equals filter', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        dimensions: [
          'cards.type'
        ],
        measures: [
          'cards.count'
        ],
        filters: [
          {
            or: [
              {
                member: 'cards.type',
                operator: 'equals',
                values: [null]
              },
              {
                member: 'cards.type',
                operator: 'notEquals',
                values: [null]
              },

            ]

          },
          {
            or: [
              {
                member: 'cards.type',
                operator: 'equals',
                values: ['t1', 't2']
              },
              {
                member: 'cards.type',
                operator: 'notEquals',
                values: ['t1', 't2']
              },

            ]

          },
          {
            or: [
              {
                member: 'cards.type',
                operator: 'equals',
                values: ['t1', null, 't2']
              },
              {
                member: 'cards.type',
                operator: 'notEquals',
                values: ['t1', null, 't2']
              },

            ]

          },
        ],
      });

      const queryAndParams = query.buildSqlAndParams();
      const expected = 'SELECT\n' +
          '      "cards".type "cards__type", count("cards".id) "cards__count"\n' +
          '    FROM\n' +
          '      card_tbl AS "cards"  WHERE (("cards".type IS NULL) OR ("cards".type IS NOT NULL)) AND (("cards".type IN ($1, $2)) OR ("cards".type NOT IN ($3, $4) OR "cards".type IS NULL)) AND (("cards".type IN ($5, $6) OR "cards".type IS NULL) OR ("cards".type NOT IN ($7, $8))) GROUP BY 1 ORDER BY 2 DESC';
      expect(queryAndParams[0]).toEqual(expected);
      // let expectedParams = [ 'type_value', 'not_type_value', 'type_value' ];
      // expect(queryAndParams[1]).toEqual(expectedParams);
    });

    it('Simple query - dimension and measure filter', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        dimensions: [
          'cards.type'
        ],
        measures: [
          'cards.count'
        ],
        filters: [
          {
            or: [
              {
                member: 'cards.type',
                operator: 'equals',
                values: ['type_value']
              },
              {
                member: 'cards.type',
                operator: 'notEquals',
                values: ['not_type_value']
              },

            ]

          },
          {
            member: 'cards.count',
            operator: 'equals',
            values: ['3']
          }],
      });

      const queryAndParams = query.buildSqlAndParams();
      const expected = 'SELECT\n' +
          '      "cards".type "cards__type", count("cards".id) "cards__count"\n' +
          '    FROM\n' +
          '      card_tbl AS "cards"  WHERE (("cards".type = $1) OR ("cards".type <> $2 OR "cards".type IS NULL)) GROUP BY 1 HAVING (count("cards".id) = $3) ORDER BY 2 DESC';
      expect(queryAndParams[0]).toEqual(expected);
      const expectedParams = ['type_value', 'not_type_value', '3'];
      expect(queryAndParams[1]).toEqual(expectedParams);
    });

    it('Simple query - order by for query with filtered timeDimension', async () => {
      const compilersLocal = prepareYamlCompiler(
        createSchemaYaml(createECommerceSchema())
      );

      await compilersLocal.compiler.compile();

      let query = new PostgresQuery(compilersLocal, {
        measures: [
          'orders.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.updated_at',
            granularity: 'week'
          },
          {
            dimension: 'orders.created_at',
            dateRange: [
              '2016-01-01',
              '2018-01-01'
            ]
          },
        ],
        order: [{ id: 'orders.updated_at', desc: false }],
      });

      let queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0].includes('ORDER BY 1')).toBeTruthy();

      // The order of time dimensions should have no effect on the `ORDER BY` clause

      query = new PostgresQuery(compilersLocal, {
        measures: [
          'orders.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.created_at',
            dateRange: [
              '2016-01-01',
              '2018-01-01'
            ]
          },
          {
            dimension: 'orders.updated_at',
            granularity: 'week'
          }
        ],
        order: [{ id: 'orders.updated_at', desc: false }],
      });

      queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0].includes('ORDER BY 1')).toBeTruthy();
    });
  });

  describe('Custom granularities', () => {
    const compilers = /** @type Compilers */ prepareJsCompiler(
      createCubeSchemaWithCustomGranularitiesAndTimeShift('orders')
    );

    const granularityQueries = [
      {
        measures: [
          'orders.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            granularity: 'half_year',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            granularity: 'half_year_by_1st_april',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            granularity: 'half_year_by_1st_march',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            granularity: 'half_year_by_1st_june',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.rollingCountByUnbounded'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            granularity: 'half_year',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders.status'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.rollingCountByUnbounded'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            granularity: 'half_year_by_1st_april',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders.status'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.rollingCountByTrailing2Day'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            granularity: 'half_year',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders.status'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.rollingCountByTrailing2Day'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            granularity: 'half_year_by_1st_april',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders.status'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.rollingCountByLeading2Day'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            granularity: 'half_year',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders.status'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.rollingCountByLeading2Day'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            granularity: 'half_year_by_1st_april',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders.status'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      // requesting via view
      {
        measures: [
          'orders_view.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders_view.createdAt',
            granularity: 'half_year_by_1st_june',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders_view.rollingCountByUnbounded'
        ],
        timeDimensions: [
          {
            dimension: 'orders_view.createdAt',
            granularity: 'half_year',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders_view.status'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
    ];

    const proxiedGranularitiesQueries = [
      {
        measures: [
          'orders.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders.createdAtHalfYear'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders.createdAtHalfYearBy1stJune'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            granularity: 'half_year_by_1st_june',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders.createdAtHalfYearBy1stMarch'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders.createdAtPredefinedYear'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders.createdAtPredefinedQuarter'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders_users.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders_users.proxyCreatedAtPredefinedYear'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders_users.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders.createdAt',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders_users.proxyCreatedAtHalfYear'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      // requesting via views
      {
        measures: [
          'orders_view.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders_view.createdAt',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders_view.createdAtHalfYear'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
      {
        measures: [
          'orders_view.count'
        ],
        timeDimensions: [
          {
            dimension: 'orders_view.createdAt',
            dateRange: [
              '2020-01-01',
              '2021-12-31'
            ]
          }
        ],
        dimensions: [
          'orders_users.proxyCreatedAtHalfYear'
        ],
        filters: [],
        timezone: 'Europe/Kyiv'
      },
    ];

    it('Test time series with different granularities', async () => {
      await compilers.compiler.compile();

      const query = new BaseQuery(compilers, granularityQueries[0]);

      {
        const timeDimension = query.newTimeDimension({
          dimension: 'orders.createdAt',
          granularity: 'half_year',
          dateRange: ['2021-01-01', '2021-12-31']
        });
        expect(timeDimension.timeSeries()).toEqual([
          ['2021-01-01T00:00:00.000', '2021-06-30T23:59:59.999'],
          ['2021-07-01T00:00:00.000', '2021-12-31T23:59:59.999']
        ]);
      }

      {
        const timeDimension = query.newTimeDimension({
          dimension: 'orders.createdAt',
          granularity: 'half_year_by_1st_april',
          dateRange: ['2021-01-01', '2021-12-31']
        });
        expect(timeDimension.timeSeries()).toEqual([
          ['2020-10-01T00:00:00.000', '2021-03-31T23:59:59.999'],
          ['2021-04-01T00:00:00.000', '2021-09-30T23:59:59.999'],
          ['2021-10-01T00:00:00.000', '2022-03-31T23:59:59.999']
        ]);
      }
    });

    describe('via PostgresQuery', () => {
      beforeAll(async () => {
        await compilers.compiler.compile();
      });

      granularityQueries.forEach(q => {
        it(`measure "${q.measures[0]}" + granularity "${q.timeDimensions[0].granularity}"`, () => {
          const query = new PostgresQuery(compilers, q);
          const queryAndParams = query.buildSqlAndParams();
          const queryString = queryAndParams[0];

          expect(queryString.includes('undefined')).toBeFalsy();
          if (q.measures[0].includes('count')) {
            expect(queryString.includes('INTERVAL \'6 months\'')).toBeTruthy();
          } else if (q.measures[0].includes('rollingCountByTrailing2Day')) {
            expect(queryString.includes('- interval \'2 day\'')).toBeTruthy();
          } else if (q.measures[0].includes('rollingCountByLeading2Day')) {
            expect(queryString.includes('+ interval \'3 day\'')).toBeTruthy();
          }
        });
      });

      proxiedGranularitiesQueries.forEach(q => {
        it(`proxy granularity reference "${q.dimensions[0]}"`, () => {
          const query = new PostgresQuery(compilers, q);
          const queryAndParams = query.buildSqlAndParams();
          const queryString = queryAndParams[0];
          console.log('Generated query: ', queryString);

          expect(queryString.includes('undefined')).toBeFalsy();
          if (q.dimensions[0].includes('PredefinedYear')) {
            expect(queryString.includes('date_trunc(\'year\'')).toBeTruthy();
          } else if (q.dimensions[0].includes('PredefinedQuarter')) {
            expect(queryString.includes('date_trunc(\'quarter\'')).toBeTruthy();
          } else {
            expect(queryString.includes('INTERVAL \'6 months\'')).toBeTruthy();
          }
        });
      });
    });

    describe('via CubeStoreQuery', () => {
      beforeAll(async () => {
        await compilers.compiler.compile();
      });

      granularityQueries.forEach(q => {
        it(`measure "${q.measures[0]}" + granularity "${q.timeDimensions[0].granularity}"`, () => {
          const query = new CubeStoreQuery(compilers, q);
          const queryAndParams = query.buildSqlAndParams();
          const queryString = queryAndParams[0];

          if (q.measures[0].includes('count')) {
            expect(queryString.includes('DATE_BIN(INTERVAL')).toBeTruthy();
            expect(queryString.includes('INTERVAL \'6 MONTH\'')).toBeTruthy();
          } else if (q.measures[0].includes('rollingCountByTrailing2Day')) {
            expect(queryString.includes('date_trunc(\'day\'')).toBeTruthy();
            expect(queryString.includes('INTERVAL \'2 DAY\'')).toBeTruthy();
          } else if (q.measures[0].includes('rollingCountByLeading2Day')) {
            expect(queryString.includes('date_trunc(\'day\'')).toBeTruthy();
            expect(queryString.includes('INTERVAL \'3 DAY\'')).toBeTruthy();
          }
        });
      });
    });
  });

  describe('Base joins', () => {
    const compilers = /** @type Compilers */ prepareJsCompiler([
      createCubeSchema({
        name: 'cardsA',
        sqlTable: 'card_tbl',
        joins: `{
          cardsB: {
            sql: \`\${CUBE}.other_id = \${cardsB}.id\`,
            relationship: 'one_to_one'
          },
        }`
      }),
      createCubeSchema({
        name: 'cardsB',
        sqlTable: 'card2_tbl',
        joins: `{
          cardsC: {
            sql: \`\${CUBE}.other_id = \${cardsC}.id\`,
            relationship: 'hasMany'
          },
        }`
      }),
      createCubeSchema({
        name: 'cardsC',
        sqlTable: 'card3_tbl',
      }),

    ]);

    it('Base joins - one-one join', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        dimensions: [
          'cardsA.type',
          'cardsB.type'
        ],
        measures: [
          'cardsC.count',
        ],
      });

      const queryAndParams = query.buildSqlAndParams();

      expect(queryAndParams[0]).toContain('LEFT JOIN card2_tbl AS "cards_b" ON "cards_a".other_id = "cards_b".id');
      expect(queryAndParams[0]).toContain('LEFT JOIN card3_tbl AS "cards_c" ON "cards_b".other_id = "cards_c".id');
    });

    it('Base joins - multiplied join', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        dimensions: [
          'cardsB.type',
        ],
        measures: [
          'cardsB.sum',
          'cardsC.count',
        ],
        timezone: 'America/Los_Angeles',
      });

      const queryAndParams = query.buildSqlAndParams();

      /* expect(queryAndParams[0]).toContain('LEFT JOIN card2_tbl AS "cards_b" ON "cards_a".other_id = "cards_b".id');
      expect(queryAndParams[0]).toContain('LEFT JOIN card3_tbl AS "cards_c" ON "cards_b".other_id = "cards_c".id'); */
    });
  });
  describe('Common - JS', () => {
    const compilers = /** @type Compilers */ prepareJsCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: `
          refreshKey: {
            every: '10 minute',
          },
        `,
      })
    );

    it('Test time series with 6 digits timestamp precision - bigquery', async () => {
      await compilers.compiler.compile();

      const query = new BigqueryQuery(compilers, {
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
          ['2021-01-01T00:00:00.000000', '2021-01-01T23:59:59.999999'],
          ['2021-01-02T00:00:00.000000', '2021-01-02T23:59:59.999999']
        ]);
      }

      const timeDimension = query.newTimeDimension({
        dimension: 'cards.createdAt',
        granularity: 'day',
        dateRange: ['2021-01-01', '2021-01-02']
      });

      expect(timeDimension.formatFromDate('2021-01-01T00:00:00.000')).toEqual(
        '2021-01-01T00:00:00.000000'
      );
      expect(timeDimension.formatFromDate('2021-01-01T00:00:00.000000')).toEqual(
        '2021-01-01T00:00:00.000000'
      );

      expect(timeDimension.formatToDate('2021-01-01T23:59:59.998')).toEqual(
        '2021-01-01T23:59:59.998000'
      );
      expect(timeDimension.formatToDate('2021-01-01T23:59:59.999')).toEqual(
        '2021-01-01T23:59:59.999999'
      );
      expect(timeDimension.formatToDate('2021-01-01T23:59:59.999999')).toEqual(
        '2021-01-01T23:59:59.999999'
      );
    });

    it('Test time series with different granularity - postgres', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
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

    it('Test same dimension with different granularities - postgres', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        timeDimensions: [
          {
            dimension: 'cards.createdAt',
            granularity: 'quarter',
          },
          {
            dimension: 'cards.createdAt',
            granularity: 'month',
          }
        ],
        filters: [],
      });

      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];
      expect(queryString.includes('date_trunc(\'quarter\'')).toBeTruthy();
      expect(queryString.includes('cards__created_at_quarter')).toBeTruthy();
      expect(queryString.includes('date_trunc(\'month\'')).toBeTruthy();
      expect(queryString.includes('cards__created_at_month')).toBeTruthy();
    });

    it('Test for everyRefreshKeySql', async () => {
      await compilers.compiler.compile();

      const timezone = 'America/Los_Angeles';
      const query = new PostgresQuery(compilers, {
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
      })).toEqual(['FLOOR((-25200 + EXTRACT(EPOCH FROM NOW())) / 3600)', false, expect.any(BaseQuery)]);

      // Standard syntax (minutes hours day month dow)
      expect(query.everyRefreshKeySql({ every: '0 * * * *', timezone }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 0) / 3600)`, false, expect.any(BaseQuery)]);

      expect(query.everyRefreshKeySql({ every: '0 10 * * *', timezone }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 36000) / 86400)`, false, expect.any(BaseQuery)]);

      // Additional syntax with seconds (seconds minutes hours day month dow)
      expect(query.everyRefreshKeySql({ every: '0 * * * * *', timezone, }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 0) / 60)`, false, expect.any(BaseQuery)]);

      expect(query.everyRefreshKeySql({ every: '0 * * * *', timezone }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 0) / 3600)`, false, expect.any(BaseQuery)]);

      expect(query.everyRefreshKeySql({ every: '30 * * * *', timezone }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 1800) / 3600)`, false, expect.any(BaseQuery)]);

      expect(query.everyRefreshKeySql({ every: '30 5 * * 5', timezone }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 365400) / 604800)`, false, expect.any(BaseQuery)]);

      for (let i = 1; i < 59; i++) {
        expect(query.everyRefreshKeySql({ every: `${i} * * * *`, timezone }))
          .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - ${i * 60}) / ${1 * 60 * 60})`, false, expect.any(BaseQuery)]);
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

  describe('refreshKey from schema', () => {
    const compilers = /** @type Compilers */ prepareJsCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: `
        refreshKey: {
          every: '10 minute',
        },
      `,
        preAggregations: `
        countCreatedAt: {
            type: 'rollup',
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
            type: 'rollup',
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
        minCreatedAt: {
            type: 'rollup',
            external: false,
            measureReferences: [min],
            timeDimensionReference: createdAt,
            granularity: \`day\`,
            partitionGranularity: \`month\`,
            refreshKey: {
              every: '1 hour',
              incremental: true,
            },
            scheduledRefresh: true,
        },
      `
      })
    );

    it('cacheKeyQueries for cube with refreshKey.every (source)', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
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
          'SELECT FLOOR((-25200 + EXTRACT(EPOCH FROM NOW())) / 600) as refresh_key',
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
      await compilers.compiler.compile();

      // Query should not match any pre-aggregation!
      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.sum'
        ],
        timeDimensions: [],
        filters: [],
        timezone: 'Europe/London',
        externalQueryClass: MssqlQuery
      });

      // Query should not match any pre-aggregation!
      expect(query.cacheKeyQueries()).toEqual([
        [
          // MSSQL dialect, because externalQueryClass
          'SELECT FLOOR((3600 + DATEDIFF(SECOND,\'1970-01-01\', GETUTCDATE())) / 600) as refresh_key',
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
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
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
          'SELECT FLOOR((-25200 + DATEDIFF(SECOND,\'1970-01-01\', GETUTCDATE())) / 3600) as refresh_key',
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
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
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
          'SELECT MAX(created_at) FROM cards',
          [],
          {
            external: false,
            renewalThreshold: 10,
          }
        ]
      ]);
    });

    it('preAggregationsDescription for query - refreshKey incremental (timeDimensions range)', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.min'
        ],
        timeDimensions: [{
          dimension: 'cards.createdAt',
          granularity: 'day',
          dateRange: ['2016-12-30', '2017-01-05']
        }],
        filters: [],
        timezone: 'Asia/Tokyo',
        externalQueryClass: MssqlQuery
      });

      const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
      expect(preAggregations.length).toEqual(1);
      expect(preAggregations[0].invalidateKeyQueries).toEqual([
        [
          'SELECT CASE\n    WHEN CURRENT_TIMESTAMP < CAST(@_1 AS DATETIMEOFFSET) THEN FLOOR((32400 + DATEDIFF(SECOND,\'1970-01-01\', GETUTCDATE())) / 3600) END as refresh_key',
          [
            '__TO_PARTITION_RANGE',
          ],
          {
            external: true,
            incremental: true,
            renewalThreshold: 300,
            renewalThresholdOutsideUpdateWindow: 86400,
            updateWindowSeconds: undefined
          }
        ]
      ]);
    });
  });

  describe('refreshKey only cube (immutable)', () => {
    /** @type Compilers */ prepareJsCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: `
        refreshKey: {
          immutable: true,
        },
      `,
        preAggregations: `
          countCreatedAt: {
              type: 'rollup',
              external: true,
              measureReferences: [count],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              scheduledRefresh: true,
          },
        `
      })
    );
  });

  describe('refreshKey only cube (every)', () => {
    const compilers = /** @type Compilers */ prepareJsCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: `
          refreshKey: {
            every: '10 minute',
          },
        `,
        preAggregations: `
          countCreatedAt: {
              type: 'rollup',
              external: true,
              measureReferences: [count],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              scheduledRefresh: true,
          },
        `
      })
    );

    it('refreshKey from cube (source)', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        timeDimensions: [{
          dimension: 'cards.createdAt',
          granularity: 'day',
          dateRange: ['2016-12-30', '2017-01-05']
        }],
        filters: [],
        timezone: 'America/Los_Angeles',
      });

      const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
      expect(preAggregations.length).toEqual(1);
      expect(preAggregations[0].invalidateKeyQueries).toEqual([
        [
          'SELECT FLOOR((-25200 + EXTRACT(EPOCH FROM NOW())) / 600) as refresh_key',
          [],
          {
            external: false,
            renewalThreshold: 60,
          }
        ]
      ]);
    });

    it('refreshKey from cube (external)', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        timeDimensions: [{
          dimension: 'cards.createdAt',
          granularity: 'day',
          dateRange: ['2016-12-30', '2017-01-05']
        }],
        filters: [],
        timezone: 'America/Los_Angeles',
        externalQueryClass: MssqlQuery
      });

      const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
      expect(preAggregations.length).toEqual(1);
      expect(preAggregations[0].invalidateKeyQueries).toEqual([
        [
          'SELECT FLOOR((-25200 + DATEDIFF(SECOND,\'1970-01-01\', GETUTCDATE())) / 600) as refresh_key',
          [],
          {
            external: true,
            renewalThreshold: 60,
          }
        ]
      ]);
    });
  });

  it('refreshKey (sql + every) in cube', async () => {
    const compilers = /** @type Compilers */ prepareJsCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: `
          refreshKey: {
            sql: 'SELECT MAX(created) FROM cards',
            every: '2 hours'
          },
        `,
        preAggregations: `
          countCreatedAt: {
              type: 'rollup',
              external: true,
              measureReferences: [count],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              scheduledRefresh: true,
          },
        `
      })
    );
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
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
        'SELECT MAX(created) FROM cards',
        [],
        {
          external: false,
          renewalThreshold: 7200,
        }
      ]
    ]);
  });

  it('refreshKey (sql + every) in preAggregation', async () => {
    const compilers = /** @type Compilers */ prepareJsCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: '',
        preAggregations: `
          countCreatedAt: {
              type: 'rollup',
              external: true,
              measureReferences: [count],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              scheduledRefresh: true,
              refreshKey: {
                sql: 'SELECT MAX(created) FROM cards',
                every: '2 hour'
              },
          },
        `
      })
    );
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
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
        'SELECT MAX(created) FROM cards',
        [],
        {
          external: false,
          // 60 * 60 *2
          renewalThreshold: 7200,
        }
      ]
    ]);
  });

  describe('FILTER_PARAMS', () => {
    /** @type {Compilers} */
    const compilers = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [{
          name: 'Order',
          sql: 'select * from order where {FILTER_PARAMS.Order.type.filter(\'type\')}',
          measures: [
            {
              name: 'count',
              type: 'count',
            },
            {
              name: 'avg_filtered',
              sql: 'product_id',
              type: 'avg',
              filters: [
                { sql: '{FILTER_PARAMS.Order.category.filter(\'category\')}' }
              ]
            }
          ],
          dimensions: [
            {
              name: 'type',
              sql: 'type',
              type: 'string'
            },
            {
              name: 'category',
              sql: 'category',
              type: 'string'
            },
            {
              name: 'proxied',
              sql: '{FILTER_PARAMS.Order.type.filter("x => type = \'online\'")}',
              type: 'boolean',
            }
          ]
        }],
        views: [{
          name: 'orders_view',
          cubes: [{
            join_path: 'Order',
            prefix: true,
            includes: [
              'type',
              'count',
            ]
          }]
        }]
      })
    );

    it('inserts filter params into query', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            member: 'Order.type',
            operator: 'equals',
            values: ['online'],
          },
        ],
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toContain('where ((type = $0$))');
    });

    it('inserts "or" filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            or: [
              {
                member: 'Order.type',
                operator: 'equals',
                values: ['online'],
              },
              {
                member: 'Order.type',
                operator: 'equals',
                values: ['in-store'],
              },
            ]
          }
        ]
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toContain('where (((type = $0$) OR (type = $1$)))');
    });

    it('inserts "and" filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            and: [
              {
                member: 'Order.type',
                operator: 'equals',
                values: ['online'],
              },
              {
                member: 'Order.type',
                operator: 'equals',
                values: ['in-store'],
              },
            ]
          }
        ]
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toContain('where (((type = $0$) AND (type = $1$)))');
    });

    it('inserts "or + and" filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            or: [
              {
                and: [
                  {
                    member: 'Order.type',
                    operator: 'equals',
                    values: ['value1'],
                  },
                  {
                    member: 'Order.type',
                    operator: 'equals',
                    values: ['value2'],
                  }
                ]
              },
              {
                and: [
                  {
                    member: 'Order.type',
                    operator: 'equals',
                    values: ['value3'],
                  },
                  {
                    member: 'Order.type',
                    operator: 'equals',
                    values: ['value4'],
                  }
                ]
              }
            ]
          }
        ]
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toContain('where ((((type = $0$) AND (type = $1$)) OR ((type = $2$) AND (type = $3$))))');
    });

    it('inserts "and + or" filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            and: [
              {
                or: [
                  {
                    member: 'Order.type',
                    operator: 'equals',
                    values: ['value1'],
                  },
                  {
                    member: 'Order.type',
                    operator: 'equals',
                    values: ['value2'],
                  }
                ]
              },
              {
                or: [
                  {
                    member: 'Order.type',
                    operator: 'equals',
                    values: ['value3'],
                  },
                  {
                    member: 'Order.type',
                    operator: 'equals',
                    values: ['value4'],
                  }
                ]
              }
            ]
          }
        ]
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toMatch(/\(\s*\(.*type\s*=\s*\$\d\$.*OR.*type\s*=\s*\$\d\$.*\)\s*AND\s*\(.*type\s*=\s*\$\d\$.*OR.*type\s*=\s*\$\d\$.*\)\s*\)/);
    });

    it('equals NULL filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            and: [
              {
                member: 'Order.type',
                operator: 'equals',
                values: [null],
              },
            ]
          }
        ],
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toContain('where (((type IS NULL)))');
    });

    it('notSet(IS NULL) filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            and: [
              {
                member: 'Order.type',
                operator: 'notSet',
              },
            ]
          }
        ],
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toContain('where (((type IS NULL)))');
    });

    it('notEquals NULL filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            and: [
              {
                member: 'Order.type',
                operator: 'notEquals',
                values: [null],
              },
            ]
          }
        ],
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toContain('where (((type IS NOT NULL)))');
    });

    it('set(IS NOT NULL) filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            and: [
              {
                member: 'Order.type',
                operator: 'set',
              },
            ]
          }
        ],
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toContain('where (((type IS NOT NULL)))');
    });

    it('propagate filter params from view into cube\'s query', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['orders_view.Order_count'],
        filters: [
          {
            member: 'orders_view.Order_type',
            operator: 'equals',
            values: ['online'],
          },
        ],
      });
      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];
      expect(queryString).toContain('select * from order where ((type = ?))');
    });

    it('propagate filter params within cte from view into cube\'s query', async () => {
      /** @type {Compilers} */
      const compiler = prepareYamlCompiler(
        createSchemaYaml({
          cubes: [{
            name: 'Order',
            sql: `WITH cte as (select *
                               from order
                               where {FILTER_PARAMS.Order.type.filter('type')}
                    )
                    select * from cte`,
            measures: [{
              name: 'count',
              type: 'count',
            }],
            dimensions: [{
              name: 'type',
              sql: 'type',
              type: 'string'
            }]
          }],
          views: [{
            name: 'orders_view',
            cubes: [{
              join_path: 'Order',
              prefix: true,
              includes: [
                'type',
                'count',
              ]
            }]
          }]
        })
      );

      await compiler.compiler.compile();
      const query = new BaseQuery(compiler, {
        measures: ['orders_view.Order_count'],
        filters: [
          {
            member: 'orders_view.Order_type',
            operator: 'equals',
            values: ['online'],
          },
        ],
      });
      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];
      expect(/select\s+\*\s+from\s+order\s+where\s+\(\(type\s=\s\?\)\)/.test(queryString)).toBeTruthy();
    });

    it('correctly substitute filter params in cube\'s query dimension used in filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        dimensions: ['Order.proxied'],
        filters: [
          {
            member: 'Order.proxied',
            operator: 'equals',
            values: [true],
          },
        ],
      });
      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];
      expect(queryString).toContain(`SELECT
      (1 = 1) "order__proxied", count(*) "order__count"
    FROM
      (select * from order where (1 = 1)) AS "order"  WHERE ((1 = 1) = ?)`);
    });

    it('correctly substitute filter params in cube\'s query measure used in filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.avg_filtered'],
        dimensions: ['Order.type'],
        filters: [
          {
            member: 'Order.type',
            operator: 'equals',
            values: ['online'],
          },
          {
            member: 'Order.category',
            operator: 'equals',
            values: ['category'],
          },
        ],
      });
      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];
      expect(queryString).toContain(`SELECT
      "order".type "order__type", avg(CASE WHEN ((category = ?)) THEN "order".product_id END) "order__avg_filtered"
    FROM
      (select * from order where (type = ?)) AS "order"  WHERE ("order".type = ?) AND ("order".category = ?)`);
    });

    it('view referencing cube with FILTER_PARAMS - multiple filters and complex query', async () => {
      /** @type {Compilers} */
      const viewCompiler = prepareYamlCompiler(
        createSchemaYaml({
          cubes: [{
            name: 'Product',
            sql: 'select * from products where {FILTER_PARAMS.Product.category.filter(\'category\')} and {FILTER_PARAMS.Product.status.filter(\'status\')}',
            measures: [
              {
                name: 'count',
                type: 'count',
              },
              {
                name: 'revenue',
                sql: 'price',
                type: 'sum',
              }
            ],
            dimensions: [
              {
                name: 'category',
                sql: 'category',
                type: 'string'
              },
              {
                name: 'status',
                sql: 'status',
                type: 'string'
              },
              {
                name: 'name',
                sql: 'name',
                type: 'string'
              }
            ]
          }],
          views: [{
            name: 'product_analytics',
            cubes: [{
              join_path: 'Product',
              prefix: true,
              includes: [
                'category',
                'status',
                'name',
                'count',
                'revenue'
              ]
            }]
          }]
        })
      );

      await viewCompiler.compiler.compile();
      const query = new PostgresQuery(viewCompiler, {
        measures: ['product_analytics.Product_count', 'product_analytics.Product_revenue'],
        dimensions: ['product_analytics.Product_name'],
        filters: [
          {
            member: 'product_analytics.Product_category',
            operator: 'equals',
            values: ['electronics'],
          },
          {
            member: 'product_analytics.Product_status',
            operator: 'equals',
            values: ['active'],
          },
        ],
      });
      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];

      expect(queryString).toContain('select * from products where (category = $1) and (status = $2)');
      expect(queryString).toMatch(/SELECT\s+"product"\.name/);
      expect(queryString).toMatch(/count\(\*\)/);
      expect(queryString).toMatch(/sum\("product"\.price\)/);
      expect(queryString).toContain('WHERE ("product".category = $3) AND ("product".status = $4)');
      expect(queryAndParams[1]).toEqual(['electronics', 'active', 'electronics', 'active']);
    });

    it('cube with FILTER_PARAMS in measure filters - triggers backAlias collection', async () => {
      /** @type {Compilers} */
      const filterParamsCompiler = prepareYamlCompiler(
        createSchemaYaml({
          cubes: [{
            name: 'Sales',
            sql: 'select * from sales',
            measures: [
              {
                name: 'count',
                type: 'count',
              },
              {
                name: 'filtered_revenue',
                sql: 'amount',
                type: 'sum',
                // This measure filter with FILTER_PARAMS should trigger backAlias collection
                // when evaluating symbols
                filters: [
                  { sql: '{FILTER_PARAMS.Sales.category.filter(\'category\')}' }
                ]
              }
            ],
            dimensions: [
              {
                name: 'id',
                sql: 'id',
                type: 'number',
                primaryKey: true
              },
              {
                name: 'category',
                sql: 'category',
                type: 'string'
              },
              {
                name: 'region',
                sql: 'region',
                type: 'string'
              }
            ]
          }],
          views: [{
            name: 'sales_analytics',
            cubes: [{
              join_path: 'Sales',
              prefix: true,
              includes: [
                'count',
                'filtered_revenue',
                'category',
                'region'
              ]
            }]
          }]
        })
      );

      await filterParamsCompiler.compiler.compile();

      const query = new PostgresQuery(filterParamsCompiler, {
        measures: ['sales_analytics.Sales_filtered_revenue'],
        dimensions: ['sales_analytics.Sales_region'],
        filters: [
          {
            member: 'sales_analytics.Sales_category',
            operator: 'equals',
            values: ['electronics'],
          },
        ],
      });

      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];

      expect(queryString).toContain('CASE WHEN (((category = $1)))');
      expect(queryString).toMatch(/sum.*CASE WHEN/);
      expect(queryString).toContain('WHERE ("sales".category = $2)');
      expect(queryAndParams[1]).toEqual(['electronics', 'electronics']);
    });
  });

  describe('FILTER_GROUP', () => {
    /** @type {Compilers} */
    const compilers = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [
          {
            name: 'Order',
            sql: `select * from order where {FILTER_GROUP(
              FILTER_PARAMS.Order.dim0.filter('dim0'),
              FILTER_PARAMS.Order.dim1.filter('dim1')
            )}`,
            measures: [{
              name: 'count',
              type: 'count',
            }],
            dimensions: [
              {
                name: 'dim0',
                sql: 'dim0',
                type: 'string'
              },
              {
                name: 'dim1',
                sql: 'dim1',
                type: 'string'
              }
            ]
          },
        ]
      })
    );

    it('inserts "or" filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            or: [
              {
                member: 'Order.dim0',
                operator: 'equals',
                values: ['val0'],
              },
              {
                member: 'Order.dim1',
                operator: 'equals',
                values: ['val1'],
              },
            ]
          }
        ],
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toContain('where (((dim0 = $0$) OR (dim1 = $1$)))');
    });

    it('inserts "and" filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            and: [
              {
                member: 'Order.dim0',
                operator: 'equals',
                values: ['val0'],
              },
              {
                member: 'Order.dim1',
                operator: 'equals',
                values: ['val1'],
              },
            ]
          }
        ],
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toContain('where (((dim0 = $0$) AND (dim1 = $1$)))');
    });

    it('inserts "or + and" filter', async () => {
      await compilers.compiler.compile();
      const query = new BaseQuery(compilers, {
        measures: ['Order.count'],
        filters: [
          {
            or: [
              {
                and: [
                  {
                    member: 'Order.dim0',
                    operator: 'equals',
                    values: ['val0'],
                  },
                  {
                    member: 'Order.dim1',
                    operator: 'equals',
                    values: ['val1'],
                  }
                ]
              },
              {
                and: [
                  {
                    member: 'Order.dim0',
                    operator: 'equals',
                    values: ['another_val0'],
                  },
                  {
                    member: 'Order.dim1',
                    operator: 'equals',
                    values: ['another_val1'],
                  }
                ]
              }
            ]
          }
        ]
      });
      const cubeSQL = query.cubeSql('Order');
      expect(cubeSQL).toContain('where ((((dim0 = $0$) AND (dim1 = $1$)) OR ((dim0 = $2$) AND (dim1 = $3$))))');
    });

    it('propagate 1 filter param from view into cube\'s query', async () => {
      /** @type {Compilers} */
      const compiler = prepareYamlCompiler(
        createSchemaYamlForGroupFilterParamsTests(
          `select *
           from order
           where {FILTER_GROUP(
             FILTER_PARAMS.Order.dim0.filter('dim0')
               , FILTER_PARAMS.Order.dim1.filter('dim1')
             )}`
        )
      );

      await compiler.compiler.compile();
      const query = new PostgresQuery(compiler, {
        measures: ['orders_view.Order_count'],
        filters: [
          {
            member: 'orders_view.Order_dim0',
            operator: 'equals',
            values: ['online'],
          },
        ],
      });
      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];
      expect(/select\s+\*\s+from\s+order\s+where\s+\(\(dim0\s=\s\$1\)\)/.test(queryString)).toBeTruthy();
    });

    it('propagate 2 filter params from view into cube\'s query', async () => {
      /** @type {Compilers} */
      const compiler = prepareYamlCompiler(
        createSchemaYamlForGroupFilterParamsTests(
          `select *
                    from order
                    where {FILTER_GROUP(
                            FILTER_PARAMS.Order.dim0.filter('dim0'),
                            FILTER_PARAMS.Order.dim1.filter('dim1')
                      )}`
        )
      );

      await compiler.compiler.compile();
      const query = new PostgresQuery(compiler, {
        measures: ['orders_view.Order_count'],
        filters: [
          {
            member: 'orders_view.Order_dim0',
            operator: 'equals',
            values: ['online'],
          },
          {
            member: 'orders_view.Order_dim1',
            operator: 'equals',
            values: ['online'],
          },
        ],
      });
      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];
      expect(/select\s+\*\s+from\s+order\s+where\s+\(\(dim0\s=\s\$1\)\s+AND\s+\(dim1\s+=\s+\$2\)\)/.test(queryString)).toBeTruthy();
    });

    it('propagate 1 filter param within cte from view into cube\'s query', async () => {
      /** @type {Compilers} */
      const compiler = prepareYamlCompiler(
        createSchemaYamlForGroupFilterParamsTests(
          `with cte as (
                        select *
                        from order
                        where
                           {FILTER_GROUP(
                             FILTER_PARAMS.Order.dim0.filter('dim0'),
                             FILTER_PARAMS.Order.dim1.filter('dim1')
                           )}
                    )
                    select * from cte`
        )
      );

      await compiler.compiler.compile();
      const query = new PostgresQuery(compiler, {
        measures: ['orders_view.Order_count'],
        filters: [
          {
            member: 'orders_view.Order_dim0',
            operator: 'equals',
            values: ['online'],
          },
        ],
      });
      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];
      expect(/select\s+\*\s+from\s+order\s+where\s+\(\(dim0\s=\s\$1\)\)/.test(queryString)).toBeTruthy();
    });

    it('propagate 2 filter params within cte from view into cube\'s query', async () => {
      /** @type {Compilers} */
      const compiler = prepareYamlCompiler(
        createSchemaYamlForGroupFilterParamsTests(
          `with cte as (
                        select *
                        from order
                        where
                           {FILTER_GROUP(
                             FILTER_PARAMS.Order.dim0.filter('dim0'),
                             FILTER_PARAMS.Order.dim1.filter('dim1')
                           )}
                    )
                    select * from cte`
        )
      );

      await compiler.compiler.compile();
      const query = new PostgresQuery(compiler, {
        measures: ['orders_view.Order_count'],
        filters: [
          {
            member: 'orders_view.Order_dim0',
            operator: 'equals',
            values: ['online'],
          },
          {
            member: 'orders_view.Order_dim1',
            operator: 'equals',
            values: ['online'],
          },
        ],
      });
      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];
      expect(/select\s+\*\s+from\s+order\s+where\s+\(\(dim0\s=\s\$1\)\s+AND\s+\(dim1\s+=\s+\$2\)\)/.test(queryString)).toBeTruthy();
    });

    it('propagate 1 filter param within cte (ref as cube and view dimensions)', async () => {
      /** @type {Compilers} */
      const compiler = prepareYamlCompiler(
        createSchemaYamlForGroupFilterParamsTests(
          `with cte as (
                        select *
                        from order
                        where
                           {FILTER_GROUP(
                             FILTER_PARAMS.Order.dim0.filter('dim0'),
                             FILTER_PARAMS.orders_view.dim0.filter('dim0')
                           )}
                    )
                    select * from cte`
        )
      );

      await compiler.compiler.compile();
      const query = new PostgresQuery(compiler, {
        measures: ['orders_view.Order_count'],
        filters: [
          {
            member: 'orders_view.Order_dim0',
            operator: 'equals',
            values: ['online'],
          },
        ],
      });
      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];
      expect(/select\s+\*\s+from\s+order\s+where\s+\(\(dim0\s=\s\$1\)\)/.test(queryString)).toBeTruthy();
    });
  });
});

describe('Class unit tests', () => {
  it('Test BaseQuery with unaliased cube', async () => {
    const set = /** @type Compilers */ prepareJsCompiler(`
      cube('CamelCaseCube', {
        sql: 'SELECT * FROM TABLE_NAME',
        measures: {
          grant_total: {
            format: 'currency',
            sql: 'grant_field',
            type: 'sum'
          },
        },
        dimensions: {
          id: {
            format: 'id',
            primaryKey: true,
            shown: true,
            sql: 'id',
            type: 'number'
          },
          description: {
            sql: 'description_field',
            type: 'string'
          },
        }
      })
    `);
    await set.compiler.compile();
    const baseQuery = new BaseQuery(set, {});
    // aliasName
    expect(baseQuery.aliasName('CamelCaseCube', false)).toEqual('camel_case_cube');
    expect(baseQuery.aliasName('CamelCaseCube.id', false)).toEqual('camel_case_cube__id');
    expect(baseQuery.aliasName('CamelCaseCube.description', false)).toEqual('camel_case_cube__description');
    expect(baseQuery.aliasName('CamelCaseCube.grant_total', false)).toEqual('camel_case_cube__grant_total');

    // aliasName for pre-agg
    expect(baseQuery.aliasName('CamelCaseCube', true)).toEqual('camel_case_cube');
    expect(baseQuery.aliasName('CamelCaseCube.id', true)).toEqual('camel_case_cube_id');
    expect(baseQuery.aliasName('CamelCaseCube.description', true)).toEqual('camel_case_cube_description');
    expect(baseQuery.aliasName('CamelCaseCube.grant_total', true)).toEqual('camel_case_cube_grant_total');

    // cubeAlias
    expect(baseQuery.cubeAlias('CamelCaseCube')).toEqual('"camel_case_cube"');
    expect(baseQuery.cubeAlias('CamelCaseCube.id')).toEqual('"camel_case_cube__id"');
    expect(baseQuery.cubeAlias('CamelCaseCube.description')).toEqual('"camel_case_cube__description"');
    expect(baseQuery.cubeAlias('CamelCaseCube.grant_total')).toEqual('"camel_case_cube__grant_total"');
  });

  it('Test BaseQuery with aliased cube', async () => {
    const set = /** @type Compilers */ prepareJsCompiler(`
      cube('CamelCaseCube', {
        sql: 'SELECT * FROM TABLE_NAME',
        sqlAlias: 'T1',
        measures: {
          grant_total: {
            format: 'currency',
            sql: 'grant_field',
            type: 'sum'
          },
        },
        dimensions: {
          id: {
            format: 'id',
            primaryKey: true,
            shown: true,
            sql: 'id',
            type: 'number'
          },
          description: {
            sql: 'description_field',
            type: 'string'
          },
        }
      })
    `);
    await set.compiler.compile();
    const baseQuery = new BaseQuery(set, {});

    // aliasName
    expect(baseQuery.aliasName('CamelCaseCube', false)).toEqual('t1');
    expect(baseQuery.aliasName('CamelCaseCube.id', false)).toEqual('t1__id');
    expect(baseQuery.aliasName('CamelCaseCube.description', false)).toEqual('t1__description');
    expect(baseQuery.aliasName('CamelCaseCube.grant_total', false)).toEqual('t1__grant_total');

    // aliasName for pre-agg
    expect(baseQuery.aliasName('CamelCaseCube', true)).toEqual('t1');
    expect(baseQuery.aliasName('CamelCaseCube.id', true)).toEqual('t1_id');
    expect(baseQuery.aliasName('CamelCaseCube.description', true)).toEqual('t1_description');
    expect(baseQuery.aliasName('CamelCaseCube.grant_total', true)).toEqual('t1_grant_total');

    // cubeAlias
    expect(baseQuery.cubeAlias('CamelCaseCube')).toEqual('"t1"');
    expect(baseQuery.cubeAlias('CamelCaseCube.id')).toEqual('"t1__id"');
    expect(baseQuery.cubeAlias('CamelCaseCube.description')).toEqual('"t1__description"');
    expect(baseQuery.cubeAlias('CamelCaseCube.grant_total')).toEqual('"t1__grant_total"');
  });

  it('Test BaseQuery columns order for the query with the sub-query', async () => {
    const joinedSchemaCompilers = prepareJsCompiler(createJoinedCubesSchema());
    await joinedSchemaCompilers.compiler.compile();
    await joinedSchemaCompilers.compiler.compile();
    const query = new BaseQuery({
      joinGraph: joinedSchemaCompilers.joinGraph,
      cubeEvaluator: joinedSchemaCompilers.cubeEvaluator,
      compiler: joinedSchemaCompilers.compiler,
    },
    {
      measures: ['B.bval_sum', 'B.count'],
      dimensions: ['B.aid'],
      filters: [{
        member: 'C.did',
        operator: 'lt',
        values: ['10']
      }],
      order: [['B.bval_sum', 'desc']]
    });
    const sql = query.buildSqlAndParams();
    const re = new RegExp('(b__aid).*(b__bval_sum).*(b__count).*');
    expect(re.test(sql[0])).toBeTruthy();
  });
});
