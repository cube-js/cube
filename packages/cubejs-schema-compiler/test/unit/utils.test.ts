import { camelizeCube } from '../../src/compiler/utils';

describe('Test Utils', () => {
  it('camelizeObject', () => {
    const res = camelizeCube({
      sql_table: 'tbl',
      measures: {
        // we should not camelize measure names
        test_measure: {
          drill_members: ['pkey', 'createdAt'],
          rolling_window: {
            trailing: '1 month',
          }
        }
      },
      joins: {

      },
      pre_aggregations: {
        // we should not camelize pre aggregation names
        count_by: {

        }
      }
    });

    expect(res).toEqual({
      sqlTable: 'tbl',
      measures: {
        // we should not camelize measure names
        test_measure: {
          drillMembers: ['pkey', 'createdAt'],
          rollingWindow: {
            trailing: '1 month',
          }
        }
      },
      joins: {

      },
      preAggregations: {
        // we should not camelize pre aggregation names
        count_by: {

        }
      }
    });
  });
});
