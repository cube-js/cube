/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview transformData related helpers unit tests.
 */

/* globals describe,test,expect */
/* eslint-disable import/no-duplicates */
/* eslint-disable @typescript-eslint/no-duplicate-imports */

import {
  QueryTimeDimension,
  NormalizedQuery,
} from '../../src/types/query';
import {
  QueryType as QueryTypeEnum,
  ResultType as ResultTypeEnum,
} from '../../src/types/enums';
import {
  DBResponseValue,
} from '../../src/helpers/transformValue';
import {
  ConfigItem,
} from '../../src/helpers/prepareAnnotation';
import transformDataDefault
  from '../../src/helpers/transformData';
import {
  COMPARE_DATE_RANGE_FIELD,
  COMPARE_DATE_RANGE_SEPARATOR,
  BLENDING_QUERY_KEY_PREFIX,
  BLENDING_QUERY_RES_SEPARATOR,
  MEMBER_SEPARATOR,
  getDateRangeValue,
  getBlendingQueryKey,
  getBlendingResponseKey,
  getMembers,
  getCompactRow,
  getVanilaRow,
  transformData,
} from '../../src/helpers/transformData';
import {
  QueryType,
} from '../../src/types/strings';

const mockData = {
  regular_discount_by_city: {
    query: {
      dimensions: [
        'ECommerceRecordsUs2021.city'
      ],
      measures: [
        'ECommerceRecordsUs2021.avg_discount'
      ],
      limit: 2
    },
    data: {
      aliasToMemberNameMap: {
        e_commerce_records_us2021__avg_discount: 'ECommerceRecordsUs2021.avg_discount',
        e_commerce_records_us2021__city: 'ECommerceRecordsUs2021.city'
      },
      annotation: {
        'ECommerceRecordsUs2021.avg_discount': {
          title: 'E Commerce Records Us2021 Avg Discount',
          shortTitle: 'Avg Discount',
          type: 'number',
          drillMembers: [],
          drillMembersGrouped: {
            measures: [],
            dimensions: []
          }
        },
        'ECommerceRecordsUs2021.city': {
          title: 'E Commerce Records Us2021 City',
          shortTitle: 'City',
          type: 'string'
        }
      },
      data: [
        {
          e_commerce_records_us2021__city: 'Missouri City',
          e_commerce_records_us2021__avg_discount: '0.80000000000000000000'
        },
        {
          e_commerce_records_us2021__city: 'Abilene',
          e_commerce_records_us2021__avg_discount: '0.80000000000000000000'
        }
      ],
      query: {
        dimensions: [
          'ECommerceRecordsUs2021.city'
        ],
        measures: [
          'ECommerceRecordsUs2021.avg_discount'
        ],
        limit: 2,
        rowLimit: 2,
        timezone: 'UTC',
        order: [],
        filters: [],
        timeDimensions: []
      },
      queryType: 'regularQuery',
      result_default: [
        {
          'ECommerceRecordsUs2021.city': 'Missouri City',
          'ECommerceRecordsUs2021.avg_discount': '0.80000000000000000000'
        },
        {
          'ECommerceRecordsUs2021.city': 'Abilene',
          'ECommerceRecordsUs2021.avg_discount': '0.80000000000000000000'
        }
      ],
      result_compact: {
        members: ['ECommerceRecordsUs2021.city', 'ECommerceRecordsUs2021.avg_discount'],
        dataset: [['Missouri City', '0.80000000000000000000'], ['Abilene', '0.80000000000000000000']],
      }
    }
  },
  regular_profit_by_postal_code: {
    query: {
      dimensions: [
        'ECommerceRecordsUs2021.postalCode'
      ],
      measures: [
        'ECommerceRecordsUs2021.avg_profit'
      ],
      limit: 2
    },
    data: {
      aliasToMemberNameMap: {
        e_commerce_records_us2021__avg_profit: 'ECommerceRecordsUs2021.avg_profit',
        e_commerce_records_us2021__postal_code: 'ECommerceRecordsUs2021.postalCode'
      },
      annotation: {
        'ECommerceRecordsUs2021.avg_profit': {
          title: 'E Commerce Records Us2021 Avg Profit',
          shortTitle: 'Avg Profit',
          type: 'number',
          drillMembers: [],
          drillMembersGrouped: {
            measures: [],
            dimensions: []
          }
        },
        'ECommerceRecordsUs2021.postalCode': {
          title: 'E Commerce Records Us2021 Postal Code',
          shortTitle: 'Postal Code',
          type: 'string'
        }
      },
      data: [
        {
          e_commerce_records_us2021__postal_code: '95823',
          e_commerce_records_us2021__avg_profit: '646.1258666666666667'
        },
        {
          e_commerce_records_us2021__postal_code: '64055',
          e_commerce_records_us2021__avg_profit: '487.8315000000000000'
        }
      ],
      query: {
        dimensions: [
          'ECommerceRecordsUs2021.postalCode'
        ],
        measures: [
          'ECommerceRecordsUs2021.avg_profit'
        ],
        limit: 2,
        rowLimit: 2,
        timezone: 'UTC',
        order: [],
        filters: [],
        timeDimensions: []
      },
      queryType: 'regularQuery',
      result_default: [
        {
          'ECommerceRecordsUs2021.postalCode': '95823',
          'ECommerceRecordsUs2021.avg_profit': '646.1258666666666667'
        },
        {
          'ECommerceRecordsUs2021.postalCode': '64055',
          'ECommerceRecordsUs2021.avg_profit': '487.8315000000000000'
        }
      ],
      result_compact: {
        members: [
          'ECommerceRecordsUs2021.postalCode',
          'ECommerceRecordsUs2021.avg_profit',
        ],
        dataset: [
          ['95823', '646.1258666666666667'],
          ['64055', '487.8315000000000000']
        ],
      }
    }
  },
  compare_date_range_count_by_order_date: {
    http_params: {
      queryType: 'whatever value or nothing, \'multi\' to apply pivot transformation'
    },
    query: {
      measures: ['ECommerceRecordsUs2021.count'],
      timeDimensions: [
        {
          dimension: 'ECommerceRecordsUs2021.orderDate',
          granularity: 'day',
          compareDateRange: [
            ['2020-01-01', '2020-01-31'],
            ['2020-03-01', '2020-03-31']
          ]
        }
      ],
      limit: 2
    },
    data: [{
      aliasToMemberNameMap: {
        e_commerce_records_us2021__count: 'ECommerceRecordsUs2021.count',
        e_commerce_records_us2021__order_date_day: 'ECommerceRecordsUs2021.orderDate.day'
      },
      annotation: {
        'ECommerceRecordsUs2021.count': {
          title: 'E Commerce Records Us2021 Count',
          shortTitle: 'Count',
          type: 'number',
          drillMembers: [
            'ECommerceRecordsUs2021.city',
            'ECommerceRecordsUs2021.country',
            'ECommerceRecordsUs2021.customerId',
            'ECommerceRecordsUs2021.orderId',
            'ECommerceRecordsUs2021.productId',
            'ECommerceRecordsUs2021.productName',
            'ECommerceRecordsUs2021.orderDate'
          ],
          drillMembersGrouped: {
            measures: [],
            dimensions: [
              'ECommerceRecordsUs2021.city',
              'ECommerceRecordsUs2021.country',
              'ECommerceRecordsUs2021.customerId',
              'ECommerceRecordsUs2021.orderId',
              'ECommerceRecordsUs2021.productId',
              'ECommerceRecordsUs2021.productName',
              'ECommerceRecordsUs2021.orderDate'
            ]
          }
        },
        'ECommerceRecordsUs2021.orderDate.day': {
          title: 'E Commerce Records Us2021 Order Date',
          shortTitle: 'Order Date',
          type: 'time'
        },
        'ECommerceRecordsUs2021.orderDate': {
          title: 'E Commerce Records Us2021 Order Date',
          shortTitle: 'Order Date',
          type: 'time'
        }
      },
      data: [
        {
          e_commerce_records_us2021__order_date_day: '2020-01-01T00:00:00.000',
          e_commerce_records_us2021__count: '10'
        },
        {
          e_commerce_records_us2021__order_date_day: '2020-01-02T00:00:00.000',
          e_commerce_records_us2021__count: '8'
        }
      ],
      query: {
        measures: [
          'ECommerceRecordsUs2021.count'
        ],
        timeDimensions: [
          {
            dimension: 'ECommerceRecordsUs2021.orderDate',
            granularity: 'day',
            dateRange: [
              '2020-01-01T00:00:00.000',
              '2020-01-31T23:59:59.999'
            ]
          }
        ],
        limit: 2,
        rowLimit: 2,
        timezone: 'UTC',
        order: [],
        filters: [],
        dimensions: []
      },
      queryType: 'compareDateRangeQuery',
      result_default: [
        {
          'ECommerceRecordsUs2021.orderDate.day': '2020-01-01T00:00:00.000',
          'ECommerceRecordsUs2021.orderDate': '2020-01-01T00:00:00.000',
          'ECommerceRecordsUs2021.count': '10',
          compareDateRange: '2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999'
        },
        {
          'ECommerceRecordsUs2021.orderDate.day': '2020-01-02T00:00:00.000',
          'ECommerceRecordsUs2021.orderDate': '2020-01-02T00:00:00.000',
          'ECommerceRecordsUs2021.count': '8',
          compareDateRange: '2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999'
        }
      ],
      result_compact: {
        members: [
          'ECommerceRecordsUs2021.orderDate.day',
          'ECommerceRecordsUs2021.orderDate',
          'ECommerceRecordsUs2021.count',
          'compareDateRange',
        ],
        dataset: [
          [
            '2020-01-01T00:00:00.000',
            '2020-01-01T00:00:00.000',
            '10',
            '2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999',
          ],
          [
            '2020-01-02T00:00:00.000',
            '2020-01-02T00:00:00.000',
            '8',
            '2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999'
          ],
        ],
      },
    }, {
      aliasToMemberNameMap: {
        e_commerce_records_us2021__count: 'ECommerceRecordsUs2021.count',
        e_commerce_records_us2021__order_date_day: 'ECommerceRecordsUs2021.orderDate.day'
      },
      annotation: {
        'ECommerceRecordsUs2021.count': {
          title: 'E Commerce Records Us2021 Count',
          shortTitle: 'Count',
          type: 'number',
          drillMembers: [
            'ECommerceRecordsUs2021.city',
            'ECommerceRecordsUs2021.country',
            'ECommerceRecordsUs2021.customerId',
            'ECommerceRecordsUs2021.orderId',
            'ECommerceRecordsUs2021.productId',
            'ECommerceRecordsUs2021.productName',
            'ECommerceRecordsUs2021.orderDate'
          ],
          drillMembersGrouped: {
            measures: [],
            dimensions: [
              'ECommerceRecordsUs2021.city',
              'ECommerceRecordsUs2021.country',
              'ECommerceRecordsUs2021.customerId',
              'ECommerceRecordsUs2021.orderId',
              'ECommerceRecordsUs2021.productId',
              'ECommerceRecordsUs2021.productName',
              'ECommerceRecordsUs2021.orderDate'
            ]
          }
        },
        'ECommerceRecordsUs2021.orderDate.day': {
          title: 'E Commerce Records Us2021 Order Date',
          shortTitle: 'Order Date',
          type: 'time'
        },
        'ECommerceRecordsUs2021.orderDate': {
          title: 'E Commerce Records Us2021 Order Date',
          shortTitle: 'Order Date',
          type: 'time'
        }
      },
      data: [
        {
          e_commerce_records_us2021__order_date_day: '2020-03-02T00:00:00.000',
          e_commerce_records_us2021__count: '11'
        },
        {
          e_commerce_records_us2021__order_date_day: '2020-03-03T00:00:00.000',
          e_commerce_records_us2021__count: '7'
        }
      ],
      query: {
        measures: [
          'ECommerceRecordsUs2021.count'
        ],
        timeDimensions: [
          {
            dimension: 'ECommerceRecordsUs2021.orderDate',
            granularity: 'day',
            dateRange: [
              '2020-03-01T00:00:00.000',
              '2020-03-31T23:59:59.999'
            ]
          }
        ],
        limit: 2,
        rowLimit: 2,
        timezone: 'UTC',
        order: [],
        filters: [],
        dimensions: []
      },
      queryType: 'compareDateRangeQuery',
      result_default: [
        {
          'ECommerceRecordsUs2021.orderDate.day': '2020-03-02T00:00:00.000',
          'ECommerceRecordsUs2021.orderDate': '2020-03-02T00:00:00.000',
          'ECommerceRecordsUs2021.count': '11',
          compareDateRange: '2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999'
        },
        {
          'ECommerceRecordsUs2021.orderDate.day': '2020-03-03T00:00:00.000',
          'ECommerceRecordsUs2021.orderDate': '2020-03-03T00:00:00.000',
          'ECommerceRecordsUs2021.count': '7',
          compareDateRange: '2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999'
        }
      ],
      result_compact: {
        members: [
          'ECommerceRecordsUs2021.orderDate.day',
          'ECommerceRecordsUs2021.orderDate',
          'ECommerceRecordsUs2021.count',
          'compareDateRange',
        ],
        dataset: [
          [
            '2020-03-02T00:00:00.000',
            '2020-03-02T00:00:00.000',
            '11',
            '2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999',
          ],
          [
            '2020-03-03T00:00:00.000',
            '2020-03-03T00:00:00.000',
            '7',
            '2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999'
          ],
        ],
      },
    }]
  },
  blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode: {
    http_params: {
      queryType: 'whatever value or nothing, \'multi\' to apply pivot transformation'
    },
    query: [{
      measures: ['ECommerceRecordsUs2021.avg_discount'],
      timeDimensions: [
        {
          dimension: 'ECommerceRecordsUs2021.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-30']
        }
      ],
      filters: [{
        member: 'ECommerceRecordsUs2021.shipMode',
        operator: 'equals',
        values: ['Standard Class']
      }],
      limit: 2
    }, {
      measures: ['ECommerceRecordsUs2021.avg_discount'],
      timeDimensions: [
        {
          dimension: 'ECommerceRecordsUs2021.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-30']
        }
      ],
      filters: [{
        member: 'ECommerceRecordsUs2021.shipMode',
        operator: 'equals',
        values: ['First Class']
      }],
      limit: 2
    }],
    data: [{
      aliasToMemberNameMap: {
        e_commerce_records_us2021__avg_discount: 'ECommerceRecordsUs2021.avg_discount',
        e_commerce_records_us2021__order_date_month: 'ECommerceRecordsUs2021.orderDate.month'
      },
      annotation: {
        'ECommerceRecordsUs2021.avg_discount': {
          title: 'E Commerce Records Us2021 Avg Discount',
          shortTitle: 'Avg Discount',
          type: 'number',
          drillMembers: [],
          drillMembersGrouped: {
            measures: [],
            dimensions: []
          }
        },
        'ECommerceRecordsUs2021.orderDate.month': {
          title: 'E Commerce Records Us2021 Order Date',
          shortTitle: 'Order Date',
          type: 'time'
        },
        'ECommerceRecordsUs2021.orderDate': {
          title: 'E Commerce Records Us2021 Order Date',
          shortTitle: 'Order Date',
          type: 'time'
        }
      },
      data: [
        {
          e_commerce_records_us2021__order_date_month: '2020-01-01T00:00:00.000',
          e_commerce_records_us2021__avg_discount: '0.15638297872340425532'
        },
        {
          e_commerce_records_us2021__order_date_month: '2020-02-01T00:00:00.000',
          e_commerce_records_us2021__avg_discount: '0.17573529411764705882'
        }
      ],
      query: {
        measures: [
          'ECommerceRecordsUs2021.avg_discount'
        ],
        timeDimensions: [
          {
            dimension: 'ECommerceRecordsUs2021.orderDate',
            granularity: 'month',
            dateRange: [
              '2020-01-01T00:00:00.000',
              '2020-12-30T23:59:59.999'
            ]
          }
        ],
        filters: [
          {
            operator: 'equals',
            values: [
              'Standard Class'
            ],
            member: 'ECommerceRecordsUs2021.shipMode'
          }
        ],
        limit: 2,
        rowLimit: 2,
        timezone: 'UTC',
        order: [],
        dimensions: []
      },
      queryType: 'blendingQuery',
      result_default: [
        {
          'ECommerceRecordsUs2021.orderDate.month': '2020-01-01T00:00:00.000',
          'ECommerceRecordsUs2021.orderDate': '2020-01-01T00:00:00.000',
          'ECommerceRecordsUs2021.avg_discount': '0.15638297872340425532',
          'time.month': '2020-01-01T00:00:00.000'
        },
        {
          'ECommerceRecordsUs2021.orderDate.month': '2020-02-01T00:00:00.000',
          'ECommerceRecordsUs2021.orderDate': '2020-02-01T00:00:00.000',
          'ECommerceRecordsUs2021.avg_discount': '0.17573529411764705882',
          'time.month': '2020-02-01T00:00:00.000'
        }
      ],
      result_compact: {
        members: [
          'ECommerceRecordsUs2021.orderDate.month',
          'ECommerceRecordsUs2021.orderDate',
          'ECommerceRecordsUs2021.avg_discount',
          'time.month',
        ],
        dataset: [
          [
            '2020-01-01T00:00:00.000',
            '2020-01-01T00:00:00.000',
            '0.15638297872340425532',
            '2020-01-01T00:00:00.000',
          ],
          [
            '2020-02-01T00:00:00.000',
            '2020-02-01T00:00:00.000',
            '0.17573529411764705882',
            '2020-02-01T00:00:00.000',
          ],
        ],
      },
    }, {
      aliasToMemberNameMap: {
        e_commerce_records_us2021__avg_discount: 'ECommerceRecordsUs2021.avg_discount',
        e_commerce_records_us2021__order_date_month: 'ECommerceRecordsUs2021.orderDate.month'
      },
      annotation: {
        'ECommerceRecordsUs2021.avg_discount': {
          title: 'E Commerce Records Us2021 Avg Discount',
          shortTitle: 'Avg Discount',
          type: 'number',
          drillMembers: [],
          drillMembersGrouped: {
            measures: [],
            dimensions: []
          }
        },
        'ECommerceRecordsUs2021.orderDate.month': {
          title: 'E Commerce Records Us2021 Order Date',
          shortTitle: 'Order Date',
          type: 'time'
        },
        'ECommerceRecordsUs2021.orderDate': {
          title: 'E Commerce Records Us2021 Order Date',
          shortTitle: 'Order Date',
          type: 'time'
        }
      },
      data: [
        {
          e_commerce_records_us2021__order_date_month: '2020-01-01T00:00:00.000',
          e_commerce_records_us2021__avg_discount: '0.28571428571428571429'
        },
        {
          e_commerce_records_us2021__order_date_month: '2020-02-01T00:00:00.000',
          e_commerce_records_us2021__avg_discount: '0.21777777777777777778'
        }
      ],
      query: {
        measures: [
          'ECommerceRecordsUs2021.avg_discount'
        ],
        timeDimensions: [
          {
            dimension: 'ECommerceRecordsUs2021.orderDate',
            granularity: 'month',
            dateRange: [
              '2020-01-01T00:00:00.000',
              '2020-12-30T23:59:59.999'
            ]
          }
        ],
        filters: [
          {
            operator: 'equals',
            values: [
              'First Class'
            ],
            member: 'ECommerceRecordsUs2021.shipMode'
          }
        ],
        limit: 2,
        rowLimit: 2,
        timezone: 'UTC',
        order: [],
        dimensions: []
      },
      queryType: 'blendingQuery',
      result_default: [{
        'ECommerceRecordsUs2021.orderDate.month': '2020-01-01T00:00:00.000',
        'ECommerceRecordsUs2021.orderDate': '2020-01-01T00:00:00.000',
        'ECommerceRecordsUs2021.avg_discount': '0.28571428571428571429',
        'time.month': '2020-01-01T00:00:00.000'
      },
      {
        'ECommerceRecordsUs2021.orderDate.month': '2020-02-01T00:00:00.000',
        'ECommerceRecordsUs2021.orderDate': '2020-02-01T00:00:00.000',
        'ECommerceRecordsUs2021.avg_discount': '0.21777777777777777778',
        'time.month': '2020-02-01T00:00:00.000'
      }],
      result_compact: {
        members: [
          'ECommerceRecordsUs2021.orderDate.month',
          'ECommerceRecordsUs2021.orderDate',
          'ECommerceRecordsUs2021.avg_discount',
          'time.month',
        ],
        dataset: [
          [
            '2020-01-01T00:00:00.000',
            '2020-01-01T00:00:00.000',
            '0.28571428571428571429',
            '2020-01-01T00:00:00.000',
          ],
          [
            '2020-02-01T00:00:00.000',
            '2020-02-01T00:00:00.000',
            '0.21777777777777777778',
            '2020-02-01T00:00:00.000',
          ],
        ],
      },
    }]
  }
};

describe('transformData helpers', () => {
  test('export looks as expected', () => {
    expect(transformDataDefault).toBeDefined();
    expect(COMPARE_DATE_RANGE_FIELD).toBeDefined();
    expect(COMPARE_DATE_RANGE_SEPARATOR).toBeDefined();
    expect(BLENDING_QUERY_KEY_PREFIX).toBeDefined();
    expect(BLENDING_QUERY_RES_SEPARATOR).toBeDefined();
    expect(MEMBER_SEPARATOR).toBeDefined();
    expect(getDateRangeValue).toBeDefined();
    expect(getBlendingQueryKey).toBeDefined();
    expect(getBlendingResponseKey).toBeDefined();
    expect(getMembers).toBeDefined();
    expect(getCompactRow).toBeDefined();
    expect(getVanilaRow).toBeDefined();
    expect(transformData).toBeDefined();
    expect(transformData).toEqual(transformDataDefault);
  });

  test('getDateRangeValue helper', () => {
    const timeDimensions = JSON.parse(
      JSON.stringify(
        mockData
          .blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode
          .data[0]
          .query
          .timeDimensions
      )
    ) as QueryTimeDimension[];

    expect(() => { getDateRangeValue(); }).toThrow(
      'QueryTimeDimension should be specified ' +
      'for the compare date range query.'
    );

    expect(() => {
      getDateRangeValue([
        { prop: 'val' } as unknown as QueryTimeDimension
      ]);
    }).toThrow(
      `${'Inconsistent QueryTimeDimension configuration ' +
      'for the compare date range query, dateRange required: '}${
        ({ prop: 'val' }).toString()}`
    );

    expect(() => {
      getDateRangeValue([
        { dateRange: 'val' } as unknown as QueryTimeDimension
      ]);
    }).toThrow(
      'Inconsistent dateRange configuration for the ' +
      'compare date range query: val'
    );

    expect(getDateRangeValue(timeDimensions)).toEqual(
      `${
        // @ts-ignore
        timeDimensions[0].dateRange[0]
      }${
        COMPARE_DATE_RANGE_SEPARATOR
      }${
        // @ts-ignore
        timeDimensions[0].dateRange[1]
      }`
    );
  });

  test('getBlendingQueryKey helper', () => {
    const timeDimensions = JSON.parse(
      JSON.stringify(
        mockData
          .blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode
          .data[0]
          .query
          .timeDimensions
      )
    ) as QueryTimeDimension[];

    expect(() => {
      getBlendingQueryKey();
    }).toThrow(
      'QueryTimeDimension should be specified ' +
      'for the blending query.'
    );

    expect(() => {
      getBlendingQueryKey([
        { prop: 'val' } as unknown as QueryTimeDimension
      ]);
    }).toThrow(
      'Inconsistent QueryTimeDimension configuration ' +
      `for the blending query, granularity required: ${
        ({ prop: 'val' }).toString()}`
    );

    expect(getBlendingQueryKey(timeDimensions))
      .toEqual(`${
        BLENDING_QUERY_KEY_PREFIX
      }${
        timeDimensions[0].granularity
      }`);
  });

  test('getBlendingResponseKey helper', () => {
    const timeDimensions = JSON.parse(
      JSON.stringify(
        mockData
          .blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode
          .data[0]
          .query
          .timeDimensions
      )
    ) as QueryTimeDimension[];
    
    expect(() => {
      getBlendingResponseKey();
    }).toThrow(
      'QueryTimeDimension should be specified ' +
      'for the blending query.'
    );

    expect(() => {
      getBlendingResponseKey([
        { prop: 'val' } as unknown as QueryTimeDimension
      ]);
    }).toThrow(
      'Inconsistent QueryTimeDimension configuration ' +
      `for the blending query, granularity required: ${
        ({ prop: 'val' }).toString()}`
    );

    expect(() => {
      getBlendingResponseKey([
        { granularity: 'day' } as unknown as QueryTimeDimension
      ]);
    }).toThrow(
      'Inconsistent QueryTimeDimension configuration ' +
      `for the blending query, dimension required: ${
        ({ granularity: 'day' }).toString()}`
    );

    expect(getBlendingResponseKey(timeDimensions))
      .toEqual(`${
        timeDimensions[0].dimension
      }${
        BLENDING_QUERY_RES_SEPARATOR
      }${
        timeDimensions[0].granularity
      }`);
  });

  test('getMembers helper', () => {
    let data;

    // throw
    data = JSON.parse(
      JSON.stringify(mockData.regular_profit_by_postal_code.data)
    );
    data.aliasToMemberNameMap = {};
    expect(() => {
      getMembers(
        data.queryType as QueryTypeEnum,
        data.query as unknown as NormalizedQuery,
        data.data as { [sqlAlias: string]: DBResponseValue }[],
        data.aliasToMemberNameMap,
      );
    }).toThrow(
      'You requested hidden member: \'e_commerce_records_us2021__postal_code\'. ' +
      'Please make it visible using `shown: true`. Please note primaryKey fields are ' +
      '`shown: false` by default: ' +
      'https://cube.dev/docs/schema/reference/joins#setting-a-primary-key.'
    );

    // regular
    data = JSON.parse(
      JSON.stringify(mockData.regular_profit_by_postal_code.data)
    );
    data.data = [];
    expect(getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    )).toEqual({});

    data = JSON.parse(
      JSON.stringify(mockData.regular_profit_by_postal_code.data)
    );
    expect(getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    )).toEqual({
      'ECommerceRecordsUs2021.postalCode': 'e_commerce_records_us2021__postal_code',
      'ECommerceRecordsUs2021.avg_profit': 'e_commerce_records_us2021__avg_profit'
    });

    // compare date range
    data = JSON.parse(
      JSON.stringify(mockData.compare_date_range_count_by_order_date.data[0])
    );
    data.data = [];
    expect(getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    )).toEqual({});

    data = JSON.parse(
      JSON.stringify(mockData.compare_date_range_count_by_order_date.data[0])
    );
    expect(getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    )).toEqual({
      'ECommerceRecordsUs2021.orderDate.day': 'e_commerce_records_us2021__order_date_day',
      'ECommerceRecordsUs2021.orderDate': 'e_commerce_records_us2021__order_date_day',
      'ECommerceRecordsUs2021.count': 'e_commerce_records_us2021__count',
      compareDateRange: 'compareDateRangeQuery',
    });

    data = JSON.parse(
      JSON.stringify(mockData.compare_date_range_count_by_order_date.data[1])
    );
    expect(getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    )).toEqual({
      'ECommerceRecordsUs2021.orderDate.day': 'e_commerce_records_us2021__order_date_day',
      'ECommerceRecordsUs2021.orderDate': 'e_commerce_records_us2021__order_date_day',
      'ECommerceRecordsUs2021.count': 'e_commerce_records_us2021__count',
      compareDateRange: 'compareDateRangeQuery',
    });

    // blending
    data = JSON.parse(
      JSON.stringify(
        mockData
          .blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode
          .data[0]
      )
    );
    data.data = [];
    expect(getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    )).toEqual({});

    data = JSON.parse(
      JSON.stringify(
        mockData
          .blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode
          .data[0]
      )
    );
    expect(getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    )).toEqual({
      'ECommerceRecordsUs2021.orderDate.month': 'e_commerce_records_us2021__order_date_month',
      'ECommerceRecordsUs2021.orderDate': 'e_commerce_records_us2021__order_date_month',
      'ECommerceRecordsUs2021.avg_discount': 'e_commerce_records_us2021__avg_discount',
      'time.month': 'e_commerce_records_us2021__order_date_month',
    });

    data = JSON.parse(
      JSON.stringify(
        mockData
          .blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode
          .data[1]
      )
    );
    expect(getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    )).toEqual({
      'ECommerceRecordsUs2021.orderDate.month': 'e_commerce_records_us2021__order_date_month',
      'ECommerceRecordsUs2021.orderDate': 'e_commerce_records_us2021__order_date_month',
      'ECommerceRecordsUs2021.avg_discount': 'e_commerce_records_us2021__avg_discount',
      'time.month': 'e_commerce_records_us2021__order_date_month',
    });
  });

  test('getCompactRow helper', () => {
    let data;
    let membersMap;
    let members;

    // regular
    data = JSON.parse(
      JSON.stringify(mockData.regular_profit_by_postal_code.data)
    );
    membersMap = getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    );
    members = Object.keys(membersMap);
    expect(getCompactRow(
      membersMap,
      data.annotation as unknown as { [member: string]: ConfigItem },
      data.queryType as QueryType,
      members,
      data.query.timeDimensions as QueryTimeDimension[],
      data.data[0],
    )).toEqual(['95823', '646.1258666666666667']);

    data = JSON.parse(
      JSON.stringify(mockData.regular_discount_by_city.data)
    );
    membersMap = getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    );
    members = Object.keys(membersMap);
    expect(getCompactRow(
      membersMap,
      data.annotation as unknown as { [member: string]: ConfigItem },
      data.queryType as QueryType,
      members,
      data.query.timeDimensions as QueryTimeDimension[],
      data.data[0],
    )).toEqual(['Missouri City', '0.80000000000000000000']);

    // compare date range
    data = JSON.parse(
      JSON.stringify(mockData.compare_date_range_count_by_order_date.data[0])
    );
    membersMap = getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    );
    members = Object.keys(membersMap);
    expect(getCompactRow(
      membersMap,
      data.annotation as unknown as { [member: string]: ConfigItem },
      data.queryType as QueryType,
      members,
      data.query.timeDimensions as QueryTimeDimension[],
      data.data[0],
    )).toEqual([
      '2020-01-01T00:00:00.000',
      '2020-01-01T00:00:00.000',
      '10',
      '2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999'
    ]);

    data = JSON.parse(
      JSON.stringify(mockData.compare_date_range_count_by_order_date.data[0])
    );
    membersMap = getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    );
    members = Object.keys(membersMap);
    expect(getCompactRow(
      membersMap,
      data.annotation as unknown as { [member: string]: ConfigItem },
      data.queryType as QueryType,
      members,
      data.query.timeDimensions as QueryTimeDimension[],
      data.data[1],
    )).toEqual([
      '2020-01-02T00:00:00.000',
      '2020-01-02T00:00:00.000',
      '8',
      '2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999'
    ]);

    // blending
    data = JSON.parse(
      JSON.stringify(mockData.blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode.data[0])
    );
    membersMap = getMembers(
      data.queryType as QueryTypeEnum,
      data.query as unknown as NormalizedQuery,
      data.data as { [sqlAlias: string]: DBResponseValue }[],
      data.aliasToMemberNameMap,
    );
    members = Object.keys(membersMap);
    expect(getCompactRow(
      membersMap,
      data.annotation as unknown as { [member: string]: ConfigItem },
      data.queryType as QueryType,
      members,
      data.query.timeDimensions as QueryTimeDimension[],
      data.data[0],
    )).toEqual([
      '2020-01-01T00:00:00.000',
      '2020-01-01T00:00:00.000',
      '0.15638297872340425532',
      '2020-01-01T00:00:00.000',
    ]);
  });

  test('getVanilaRow helper', () => {
    const data = JSON.parse(
      JSON.stringify(mockData.regular_discount_by_city.data)
    );
    delete data.aliasToMemberNameMap.e_commerce_records_us2021__avg_discount;
    expect(() => getVanilaRow(
      data.aliasToMemberNameMap,
      data.annotation as unknown as { [member: string]: ConfigItem },
      data.queryType as QueryType,
      data.query as unknown as NormalizedQuery,
      data.data[0],
    )).toThrow(
      'You requested hidden member: \'e_commerce_records_us2021__avg_discount\'. ' +
      'Please make it visible using `shown: true`. Please note ' +
      'primaryKey fields are `shown: false` by default: ' +
      'https://cube.dev/docs/schema/reference/joins#setting-a-primary-key.'
    );
  });
});

describe('transformData default mode', () => {
  test('regular discount by city', () => {
    let data;

    data = JSON.parse(
      JSON.stringify(mockData.regular_discount_by_city.data)
    );
    delete data.aliasToMemberNameMap.e_commerce_records_us2021__avg_discount;
    expect(() => transformData(
      data.aliasToMemberNameMap,
      data.annotation as unknown as { [member: string]: ConfigItem },
      data.data,
      data.query as unknown as NormalizedQuery,
      data.queryType as QueryType,
    )).toThrow();

    data = JSON.parse(
      JSON.stringify(mockData.regular_discount_by_city.data)
    );
    expect(
      transformData(
        data.aliasToMemberNameMap,
        data.annotation as unknown as { [member: string]: ConfigItem },
        data.data,
        data.query as unknown as NormalizedQuery,
        data.queryType as QueryType,
      )
    ).toEqual(data.result_default);
  });

  test('regular profit by postal code', () => {
    const data = JSON.parse(
      JSON.stringify(mockData.regular_profit_by_postal_code.data)
    );
    expect(
      transformData(
        data.aliasToMemberNameMap,
        data.annotation as unknown as { [member: string]: ConfigItem },
        data.data,
        data.query as unknown as NormalizedQuery,
        data.queryType as QueryType,
      )
    ).toEqual(data.result_default);
  });

  test('compare date range count by order date', () => {
    const data = JSON.parse(
      JSON.stringify(mockData.compare_date_range_count_by_order_date.data)
    );

    expect(
      transformData(
        data[0].aliasToMemberNameMap,
        data[0].annotation as unknown as { [member: string]: ConfigItem },
        data[0].data,
        data[0].query as unknown as NormalizedQuery,
        data[0].queryType as QueryType,
      )
    ).toEqual(data[0].result_default);

    expect(
      transformData(
        data[1].aliasToMemberNameMap,
        data[1].annotation as unknown as { [member: string]: ConfigItem },
        data[1].data,
        data[1].query as unknown as NormalizedQuery,
        data[1].queryType as QueryType,
      )
    ).toEqual(data[1].result_default);
  });

  test('blending query avg discount by date range for the first and standard ship mode', () => {
    const data = JSON.parse(
      JSON.stringify(
        mockData
          .blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode
          .data
      )
    );

    expect(
      transformData(
        data[0].aliasToMemberNameMap,
        data[0].annotation as unknown as { [member: string]: ConfigItem },
        data[0].data,
        data[0].query as unknown as NormalizedQuery,
        data[0].queryType as QueryType,
      )
    ).toEqual(data[0].result_default);

    expect(
      transformData(
        data[1].aliasToMemberNameMap,
        data[1].annotation as unknown as { [member: string]: ConfigItem },
        data[1].data,
        data[1].query as unknown as NormalizedQuery,
        data[1].queryType as QueryType,
      )
    ).toEqual(data[1].result_default);
  });
});

describe('transformData compact mode', () => {
  test('regular discount by city', () => {
    let data;

    data = JSON.parse(
      JSON.stringify(mockData.regular_discount_by_city.data)
    );
    delete data.aliasToMemberNameMap.e_commerce_records_us2021__avg_discount;
    expect(() => transformData(
      data.aliasToMemberNameMap,
      data.annotation as unknown as { [member: string]: ConfigItem },
      data.data,
      data.query as unknown as NormalizedQuery,
      data.queryType as QueryType,
      ResultTypeEnum.COMPACT,
    )).toThrow();

    data = JSON.parse(
      JSON.stringify(mockData.regular_discount_by_city.data)
    );
    expect(
      transformData(
        data.aliasToMemberNameMap,
        data.annotation as unknown as { [member: string]: ConfigItem },
        data.data,
        data.query as unknown as NormalizedQuery,
        data.queryType as QueryType,
        ResultTypeEnum.COMPACT,
      )
    ).toEqual(data.result_compact);
  });

  test('regular profit by postal code', () => {
    const data = JSON.parse(
      JSON.stringify(mockData.regular_profit_by_postal_code.data)
    );
    expect(
      transformData(
        data.aliasToMemberNameMap,
        data.annotation as unknown as { [member: string]: ConfigItem },
        data.data,
        data.query as unknown as NormalizedQuery,
        data.queryType as QueryType,
        ResultTypeEnum.COMPACT,
      )
    ).toEqual(data.result_compact);
  });

  test('compare date range count by order date', () => {
    const data = JSON.parse(
      JSON.stringify(mockData.compare_date_range_count_by_order_date.data)
    );

    expect(
      transformData(
        data[0].aliasToMemberNameMap,
        data[0].annotation as unknown as { [member: string]: ConfigItem },
        data[0].data,
        data[0].query as unknown as NormalizedQuery,
        data[0].queryType as QueryType,
        ResultTypeEnum.COMPACT,
      )
    ).toEqual(data[0].result_compact);

    expect(
      transformData(
        data[1].aliasToMemberNameMap,
        data[1].annotation as unknown as { [member: string]: ConfigItem },
        data[1].data,
        data[1].query as unknown as NormalizedQuery,
        data[1].queryType as QueryType,
        ResultTypeEnum.COMPACT,
      )
    ).toEqual(data[1].result_compact);
  });

  test('blending query avg discount by date range for the first and standard ship mode', () => {
    const data = JSON.parse(
      JSON.stringify(
        mockData
          .blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode
          .data
      )
    );

    expect(
      transformData(
        data[0].aliasToMemberNameMap,
        data[0].annotation as unknown as { [member: string]: ConfigItem },
        data[0].data,
        data[0].query as unknown as NormalizedQuery,
        data[0].queryType as QueryType,
        ResultTypeEnum.COMPACT,
      )
    ).toEqual(data[0].result_compact);

    expect(
      transformData(
        data[1].aliasToMemberNameMap,
        data[1].annotation as unknown as { [member: string]: ConfigItem },
        data[1].data,
        data[1].query as unknown as NormalizedQuery,
        data[1].queryType as QueryType,
        ResultTypeEnum.COMPACT,
      )
    ).toEqual(data[1].result_compact);
  });
});
