import { camelizeCube } from '../../src/compiler/utils';
import { toSnakeCase } from '../../src/scaffolding/utils';

describe('Test Utils', () => {
  it('toSnakeCase', () => {
    expect(toSnakeCase('customerkey')).toEqual('customerkey');
    expect(toSnakeCase('customerKey')).toEqual('customer_key');
    expect(toSnakeCase('customer_key')).toEqual('customer_key');
  });

  it('camelizeObject (js)', () => {
    const res = camelizeCube({
      sql_table: 'tbl',
      measures: {
        // we should not camelize measure names
        test_measure: {
          drill_members: ['pkey', 'createdAt'],
          rolling_window: {
            trailing: '1 month',
          },
          meta: {
            dont_camelize_field: true,
          }
        },
        // meta as name
        meta: {
          drill_members: ['pkey', 'createdAt'],
        }
      },
      dimensions: {
        my_dim: {
          meta: {
            dont_camelize_field: true,
          }
        },
        // meta as name
        meta: {
          meta: {
            dont_camelize_field: true,
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
          },
          meta: {
            dont_camelize_field: true,
          }
        },
        // meta as name
        meta: {
          drillMembers: ['pkey', 'createdAt'],
        }
      },
      dimensions: {
        my_dim: {
          meta: {
            dont_camelize_field: true,
          }
        },
        // meta as name
        meta: {
          meta: {
            dont_camelize_field: true,
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

  it('camelizeObject (yaml)', () => {
    const res = camelizeCube({
      sql_table: 'tbl',
      measures: [{
        // we should not camelize measure names
        name: 'my_measure_name',
        drill_members: ['pkey', 'createdAt'],
        rolling_window: {
          trailing: '1 month',
        },
        meta: {
          dont_camelize_field: true,
        }
      }, {
        // meta as name
        name: 'meta',
        drill_members: ['pkey', 'createdAt'],
      }],
      joins: {

      },
    });

    expect(res).toEqual({
      sqlTable: 'tbl',
      measures: [{
        // we should not camelize measure names
        name: 'my_measure_name',
        drillMembers: ['pkey', 'createdAt'],
        rollingWindow: {
          trailing: '1 month',
        },
        meta: {
          dont_camelize_field: true,
        }
      }, {
        // meta as name
        name: 'meta',
        drillMembers: ['pkey', 'createdAt'],
      }],
      joins: {

      },
    });
  });
});
