export const DescriptiveQueryRequest = {
  timeDimensions: [
    {
      dimension: 'base_orders.created_at',
      granularity: 'month'
    },
    {
      dimension: 'base_orders.completed_at',
      dateRange: [
        '2023-05-16',
        '2025-05-16'
      ]
    }
  ],
  filters: [
    {
      member: 'base_orders.fiscal_event_date_label',
      operator: 'set'
    }
  ],
  dimensions: [
    'base_orders.status'
  ],
  measures: [
    'base_orders.count'
  ],
  segments: [
    'users.sf_users'
  ]
};

export const DescriptiveQueryRequestCompact = {
  timeDimensions: [
    {
      dimension: 'base_orders.created_at',
      granularity: 'month'
    },
    {
      dimension: 'base_orders.completed_at',
      dateRange: [
        '2023-05-16',
        '2025-05-16'
      ]
    }
  ],
  filters: [
    {
      member: 'base_orders.fiscal_event_date_label',
      operator: 'set'
    }
  ],
  dimensions: [
    'base_orders.status'
  ],
  measures: [
    'base_orders.count'
  ],
  segments: [
    'users.sf_users'
  ],
  responseFormat: 'compact',
};

export const DescriptiveQueryResponse = {
  queryType: 'regularQuery',
  results: [
    {
      query: {
        measures: [
          'base_orders.count'
        ],
        dimensions: [
          'base_orders.status'
        ],
        timeDimensions: [
          {
            dimension: 'base_orders.created_at',
            granularity: 'month'
          },
          {
            dimension: 'base_orders.completed_at',
            dateRange: [
              '2023-05-16T00:00:00.000',
              '2025-05-16T23:59:59.999'
            ]
          }
        ],
        segments: [
          'users.sf_users'
        ],
        limit: 10000,
        total: true,
        timezone: 'UTC',
        filters: [
          {
            member: 'base_orders.fiscal_event_date_label',
            operator: 'set'
          }
        ],
        rowLimit: 10000
      },
      lastRefreshTime: '2025-05-16T13:34:38.144Z',
      refreshKeyValues: [
        [
          {
            refresh_key: '174740245'
          }
        ],
        [
          {
            refresh_key: '174740245'
          }
        ]
      ],
      usedPreAggregations: {},
      transformedQuery: {
        sortedDimensions: [
          'base_orders.fiscal_event_date_label',
          'base_orders.status',
          'users.sf_users'
        ],
        sortedTimeDimensions: [
          [
            'base_orders.completed_at',
            'day'
          ],
          [
            'base_orders.created_at',
            'month'
          ]
        ],
        timeDimensions: [
          [
            'base_orders.completed_at',
            null
          ],
          [
            'base_orders.created_at',
            'month'
          ]
        ],
        measures: [
          'base_orders.count'
        ],
        leafMeasureAdditive: true,
        leafMeasures: [
          'base_orders.count'
        ],
        measureToLeafMeasures: {
          'base_orders.count': [
            {
              measure: 'base_orders.count',
              additive: true,
              type: 'count'
            }
          ]
        },
        hasNoTimeDimensionsWithoutGranularity: false,
        allFiltersWithinSelectedDimensions: false,
        isAdditive: true,
        granularityHierarchies: {
          'line_items_to_orders.created_at.year': [
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'line_items_to_orders.created_at.quarter': [
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'line_items_to_orders.created_at.month': [
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'line_items_to_orders.created_at.week': [
            'week',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'line_items_to_orders.created_at.day': [
            'day',
            'hour',
            'minute',
            'second'
          ],
          'line_items_to_orders.created_at.hour': [
            'hour',
            'minute',
            'second'
          ],
          'line_items_to_orders.created_at.minute': [
            'minute',
            'second'
          ],
          'line_items_to_orders.created_at.second': [
            'second'
          ],
          'orders_to_line_items.created_at.year': [
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.created_at.quarter': [
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.created_at.month': [
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.created_at.week': [
            'week',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.created_at.day': [
            'day',
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.created_at.hour': [
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.created_at.minute': [
            'minute',
            'second'
          ],
          'orders_to_line_items.created_at.second': [
            'second'
          ],
          'orders_to_line_items.completed_at.year': [
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.completed_at.quarter': [
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.completed_at.month': [
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.completed_at.week': [
            'week',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.completed_at.day': [
            'day',
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.completed_at.hour': [
            'hour',
            'minute',
            'second'
          ],
          'orders_to_line_items.completed_at.minute': [
            'minute',
            'second'
          ],
          'orders_to_line_items.completed_at.second': [
            'second'
          ],
          'products.created_at.year': [
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'products.created_at.quarter': [
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'products.created_at.month': [
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'products.created_at.week': [
            'week',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'products.created_at.day': [
            'day',
            'hour',
            'minute',
            'second'
          ],
          'products.created_at.hour': [
            'hour',
            'minute',
            'second'
          ],
          'products.created_at.minute': [
            'minute',
            'second'
          ],
          'products.created_at.second': [
            'second'
          ],
          'simple_orders.created_at.year': [
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'simple_orders.created_at.quarter': [
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'simple_orders.created_at.month': [
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'simple_orders.created_at.week': [
            'week',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'simple_orders.created_at.day': [
            'day',
            'hour',
            'minute',
            'second'
          ],
          'simple_orders.created_at.hour': [
            'hour',
            'minute',
            'second'
          ],
          'simple_orders.created_at.minute': [
            'minute',
            'second'
          ],
          'simple_orders.created_at.second': [
            'second'
          ],
          'simple_orders_sql_ext.created_at.year': [
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'simple_orders_sql_ext.created_at.quarter': [
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'simple_orders_sql_ext.created_at.month': [
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'simple_orders_sql_ext.created_at.week': [
            'week',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'simple_orders_sql_ext.created_at.day': [
            'day',
            'hour',
            'minute',
            'second'
          ],
          'simple_orders_sql_ext.created_at.hour': [
            'hour',
            'minute',
            'second'
          ],
          'simple_orders_sql_ext.created_at.minute': [
            'minute',
            'second'
          ],
          'simple_orders_sql_ext.created_at.second': [
            'second'
          ],
          'users.created_at.year': [
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'users.created_at.quarter': [
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'users.created_at.month': [
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'users.created_at.week': [
            'week',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'users.created_at.day': [
            'day',
            'hour',
            'minute',
            'second'
          ],
          'users.created_at.hour': [
            'hour',
            'minute',
            'second'
          ],
          'users.created_at.minute': [
            'minute',
            'second'
          ],
          'users.created_at.second': [
            'second'
          ],
          'base_orders.created_at.year': [
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.created_at.quarter': [
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.created_at.month': [
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.created_at.week': [
            'week',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.created_at.day': [
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.created_at.hour': [
            'hour',
            'minute',
            'second'
          ],
          'base_orders.created_at.minute': [
            'minute',
            'second'
          ],
          'base_orders.created_at.second': [
            'second'
          ],
          'base_orders.completed_at.year': [
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.completed_at.quarter': [
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.completed_at.month': [
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.completed_at.week': [
            'week',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.completed_at.day': [
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.completed_at.hour': [
            'hour',
            'minute',
            'second'
          ],
          'base_orders.completed_at.minute': [
            'minute',
            'second'
          ],
          'base_orders.completed_at.second': [
            'second'
          ],
          'base_orders.event_date.year': [
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.event_date.quarter': [
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.event_date.month': [
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.event_date.week': [
            'week',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.event_date.day': [
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.event_date.hour': [
            'hour',
            'minute',
            'second'
          ],
          'base_orders.event_date.minute': [
            'minute',
            'second'
          ],
          'base_orders.event_date.second': [
            'second'
          ],
          'base_orders.event_date.fiscal_year': [
            'fiscal_year',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'base_orders.event_date.fiscal_quarter': [
            'fiscal_quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'check_dup_names.created_at.year': [
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'check_dup_names.created_at.quarter': [
            'quarter',
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'check_dup_names.created_at.month': [
            'month',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'check_dup_names.created_at.week': [
            'week',
            'day',
            'hour',
            'minute',
            'second'
          ],
          'check_dup_names.created_at.day': [
            'day',
            'hour',
            'minute',
            'second'
          ],
          'check_dup_names.created_at.hour': [
            'hour',
            'minute',
            'second'
          ],
          'check_dup_names.created_at.minute': [
            'minute',
            'second'
          ],
          'check_dup_names.created_at.second': [
            'second'
          ]
        },
        hasMultipliedMeasures: false,
        hasCumulativeMeasures: false,
        windowGranularity: null,
        filterDimensionsSingleValueEqual: null,
        ownedDimensions: [
          'base_orders.event_date',
          'base_orders.status',
          'users.sf_users'
        ],
        ownedTimeDimensionsWithRollupGranularity: [
          [
            'base_orders.completed_at',
            'day'
          ],
          [
            'base_orders.created_at',
            'month'
          ]
        ],
        ownedTimeDimensionsAsIs: [
          [
            'base_orders.completed_at',
            null
          ],
          [
            'base_orders.created_at',
            'month'
          ]
        ],
        allBackAliasMembers: {},
        ungrouped: null,
        sortedUsedCubePrimaryKeys: null,
        sortedAllCubeNames: null,
        hasMultiStage: false
      },
      requestId: '2ac2a7b1-008b-41ec-be93-691f79a55348-span-1',
      annotation: {
        measures: {
          'base_orders.count': {
            title: 'Base Orders Count',
            shortTitle: 'Count',
            type: 'number',
            drillMembers: [
              'base_orders.id',
              'base_orders.status',
              'users.city',
              'users.gender'
            ],
            drillMembersGrouped: {
              measures: [],
              dimensions: [
                'base_orders.id',
                'base_orders.status',
                'users.city',
                'users.gender'
              ]
            }
          }
        },
        dimensions: {
          'base_orders.status': {
            title: 'Base Orders Status',
            shortTitle: 'Status',
            type: 'string',
            meta: {
              addDesc: 'The status of order',
              moreNum: 42
            }
          }
        },
        timeDimensions: {
          'base_orders.created_at': {
            title: 'Base Orders Created at',
            shortTitle: 'Created at',
            type: 'time'
          },
          'base_orders.created_at.month': {
            title: 'Base Orders Created at',
            shortTitle: 'Created at',
            type: 'time',
            granularity: {
              name: 'month',
              title: 'month',
              interval: '1 month'
            }
          }
        },
        segments: {
          'users.sf_users': {
            title: 'Users Sf Users',
            shortTitle: 'Sf Users'
          }
        }
      },
      dataSource: 'default',
      dbType: 'postgres',
      extDbType: 'cubestore',
      external: false,
      slowQuery: false,
      total: 19,
      data: [
        {
          'base_orders.created_at.month': '2023-04-01T00:00:00.000',
          'base_orders.created_at': '2023-04-01T00:00:00.000',
          'base_orders.count': '2',
          'base_orders.status': 'completed'
        },
        {
          'base_orders.count': '6',
          'base_orders.created_at': '2023-05-01T00:00:00.000',
          'base_orders.created_at.month': '2023-05-01T00:00:00.000',
          'base_orders.status': 'completed'
        },
        {
          'base_orders.count': '6',
          'base_orders.status': 'processing',
          'base_orders.created_at': '2023-05-01T00:00:00.000',
          'base_orders.created_at.month': '2023-05-01T00:00:00.000'
        },
        {
          'base_orders.count': '9',
          'base_orders.created_at.month': '2023-05-01T00:00:00.000',
          'base_orders.status': 'shipped',
          'base_orders.created_at': '2023-05-01T00:00:00.000'
        },
        {
          'base_orders.created_at': '2023-06-01T00:00:00.000',
          'base_orders.status': 'completed',
          'base_orders.created_at.month': '2023-06-01T00:00:00.000',
          'base_orders.count': '5'
        },
        {
          'base_orders.count': '5',
          'base_orders.status': 'processing',
          'base_orders.created_at': '2023-06-01T00:00:00.000',
          'base_orders.created_at.month': '2023-06-01T00:00:00.000'
        },
        {
          'base_orders.count': '13',
          'base_orders.created_at': '2023-06-01T00:00:00.000',
          'base_orders.status': 'shipped',
          'base_orders.created_at.month': '2023-06-01T00:00:00.000'
        },
        {
          'base_orders.status': 'completed',
          'base_orders.created_at.month': '2023-07-01T00:00:00.000',
          'base_orders.created_at': '2023-07-01T00:00:00.000',
          'base_orders.count': '5'
        },
        {
          'base_orders.created_at.month': '2023-07-01T00:00:00.000',
          'base_orders.status': 'processing',
          'base_orders.created_at': '2023-07-01T00:00:00.000',
          'base_orders.count': '7'
        },
        {
          'base_orders.count': '5',
          'base_orders.status': 'shipped',
          'base_orders.created_at': '2023-07-01T00:00:00.000',
          'base_orders.created_at.month': '2023-07-01T00:00:00.000'
        },
        {
          'base_orders.created_at': '2023-08-01T00:00:00.000',
          'base_orders.status': 'completed',
          'base_orders.count': '11',
          'base_orders.created_at.month': '2023-08-01T00:00:00.000'
        },
        {
          'base_orders.count': '3',
          'base_orders.created_at.month': '2023-08-01T00:00:00.000',
          'base_orders.created_at': '2023-08-01T00:00:00.000',
          'base_orders.status': 'processing'
        },
        {
          'base_orders.status': 'shipped',
          'base_orders.count': '4',
          'base_orders.created_at.month': '2023-08-01T00:00:00.000',
          'base_orders.created_at': '2023-08-01T00:00:00.000'
        },
        {
          'base_orders.created_at.month': '2023-09-01T00:00:00.000',
          'base_orders.status': 'completed',
          'base_orders.count': '5',
          'base_orders.created_at': '2023-09-01T00:00:00.000'
        },
        {
          'base_orders.count': '10',
          'base_orders.created_at.month': '2023-09-01T00:00:00.000',
          'base_orders.status': 'processing',
          'base_orders.created_at': '2023-09-01T00:00:00.000'
        },
        {
          'base_orders.created_at': '2023-09-01T00:00:00.000',
          'base_orders.count': '9',
          'base_orders.created_at.month': '2023-09-01T00:00:00.000',
          'base_orders.status': 'shipped'
        },
        {
          'base_orders.count': '4',
          'base_orders.created_at.month': '2023-10-01T00:00:00.000',
          'base_orders.created_at': '2023-10-01T00:00:00.000',
          'base_orders.status': 'completed'
        },
        {
          'base_orders.count': '5',
          'base_orders.created_at': '2023-10-01T00:00:00.000',
          'base_orders.status': 'processing',
          'base_orders.created_at.month': '2023-10-01T00:00:00.000'
        },
        {
          'base_orders.status': 'shipped',
          'base_orders.created_at.month': '2023-10-01T00:00:00.000',
          'base_orders.count': '9',
          'base_orders.created_at': '2023-10-01T00:00:00.000'
        }
      ]
    }
  ],
  pivotQuery: {
    measures: [
      'base_orders.count'
    ],
    dimensions: [
      'base_orders.status'
    ],
    timeDimensions: [
      {
        dimension: 'base_orders.created_at',
        granularity: 'month'
      },
      {
        dimension: 'base_orders.completed_at',
        dateRange: [
          '2023-05-16T00:00:00.000',
          '2025-05-16T23:59:59.999'
        ]
      }
    ],
    segments: [
      'users.sf_users'
    ],
    limit: 10000,
    total: true,
    timezone: 'UTC',
    filters: [
      {
        member: 'base_orders.fiscal_event_date_label',
        operator: 'set'
      }
    ],
    rowLimit: 10000,
    queryType: 'regularQuery'
  },
  slowQuery: false
};

export const NumericCastedData = DescriptiveQueryResponse.results[0].data.map(r => ({
  ...r,
  'base_orders.count': Number(r['base_orders.count'])
}));
