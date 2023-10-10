import 'jest';
import ResultSet from '../ResultSet';

jest.mock('moment-range', () => {
  const Moment = jest.requireActual('moment');
  const MomentRange = jest.requireActual('moment-range');
  const moment = MomentRange.extendMoment(Moment);
  return {
    extendMoment: () => moment,
  };
});

const loadResponse = (query = {}) => ({
  queryType: 'regularQuery',
  results: [
    {
      query: {
        measures: ['Orders.count'],
        timeDimensions: [
          {
            dimension: 'Orders.ts',
            granularity: 'day',
            dateRange: ['2020-08-01T00:00:00.000', '2020-08-07T23:59:59.999'],
          },
        ],
        filters: [],
        timezone: 'UTC',
        order: [],
        dimensions: [],
        ...query,
      },
      data: [
        {
          'Orders.ts.day': '2020-08-01T00:00:00.000',
          'Orders.ts': '2020-08-01T00:00:00.000',
          'Orders.count': 1,
        },
        {
          'Orders.ts.day': '2020-08-02T00:00:00.000',
          'Orders.ts': '2020-08-02T00:00:00.000',
          'Orders.count': 2,
        },
      ],
      annotation: {
        measures: {
          'Orders.count': {
            title: 'Orders Count',
            shortTitle: 'Count',
            type: 'number',
            drillMembers: ['Orders.id', 'Orders.title'],
            drillMembersGrouped: {
              measures: [],
              dimensions: ['Orders.id', 'Orders.title'],
            },
          },
        },
        dimensions: {},
        segments: {},
        timeDimensions: {
          'Orders.ts.day': {
            title: 'Orders Ts',
            shortTitle: 'Ts',
            type: 'time',
          },
          'Orders.ts': { title: 'Orders Ts', shortTitle: 'Ts', type: 'time' },
        },
      },
    },
  ],
  pivotQuery: {
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.ts',
        granularity: 'day',
        dateRange: ['2020-08-01T00:00:00.000', '2020-08-07T23:59:59.999'],
      },
    ],
    filters: [],
    timezone: 'UTC',
    order: [],
    dimensions: [],
    ...query,
  },
});

const loadResponse2 = {
  queryType: 'regularQuery',
  results: [
    {
      query: {
        measures: ['Orders.count'],
        timeDimensions: [
          {
            dimension: 'Orders.createdAt',
            granularity: 'week',
            dateRange: ['2023-05-10T00:00:00.000', '2023-05-14T23:59:59.999'],
          },
        ],
        limit: 10000,
        timezone: 'UTC',
        order: [],
        filters: [],
        dimensions: [],
        rowLimit: 10000,
        queryType: 'regularQuery',
      },
      data: [
        {
          'Orders.createdAt.week': '2023-05-08T00:00:00.000',
          'Orders.createdAt': '2023-05-08T00:00:00.000',
          'Orders.count': '21',
        },
      ],
      lastRefreshTime: '2023-05-22T14:46:45.000Z',
      usedPreAggregations: {
        'prod_pre_aggregations.orders_main': {
          targetTableName:
            'prod_pre_aggregations.orders_main20230508_instgodu_ehgypjtt_1i6n02l',
          refreshKeyValues: [[]],
          lastUpdatedAt: 1684766805000,
        },
      },
      transformedQuery: {
        sortedDimensions: [],
        sortedTimeDimensions: [['Orders.createdAt', 'day']],
        timeDimensions: [['Orders.createdAt', 'week']],
        measures: ['Orders.count'],
        leafMeasureAdditive: true,
        leafMeasures: ['Orders.count'],
        measureToLeafMeasures: {
          'Orders.count': [
            { measure: 'Orders.count', additive: true, type: 'count' },
          ],
        },
        hasNoTimeDimensionsWithoutGranularity: true,
        allFiltersWithinSelectedDimensions: true,
        isAdditive: true,
        granularityHierarchies: {
          year: [
            'year',
            'quarter',
            'month',
            'month',
            'day',
            'hour',
            'minute',
            'second',
          ],
          quarter: ['quarter', 'month', 'day', 'hour', 'minute', 'second'],
          month: ['month', 'day', 'hour', 'minute', 'second'],
          week: ['week', 'day', 'hour', 'minute', 'second'],
          day: ['day', 'hour', 'minute', 'second'],
          hour: ['hour', 'minute', 'second'],
          minute: ['minute', 'second'],
          second: ['second'],
        },
        hasMultipliedMeasures: false,
        hasCumulativeMeasures: false,
        windowGranularity: null,
        filterDimensionsSingleValueEqual: {},
        ownedDimensions: [],
        ownedTimeDimensionsWithRollupGranularity: [['Orders.createdAt', 'day']],
        ownedTimeDimensionsAsIs: [['Orders.createdAt', 'week']],
      },
      requestId: 'x',
      annotation: {
        measures: {
          'Orders.count': {
            title: 'Orders Count',
            shortTitle: 'Count',
            type: 'number',
            drillMembers: ['Orders.id', 'Orders.createdAt'],
            drillMembersGrouped: {
              measures: [],
              dimensions: ['Orders.id', 'Orders.createdAt'],
            },
          },
        },
        dimensions: {},
        segments: {},
        timeDimensions: {
          'Orders.createdAt.week': {
            title: 'Orders Created at',
            shortTitle: 'Created at',
            type: 'time',
          },
          'Orders.createdAt': {
            title: 'Orders Created at',
            shortTitle: 'Created at',
            type: 'time',
          },
        },
      },
      dataSource: 'default',
      dbType: 'postgres',
      extDbType: 'cubestore',
    },
  ],
  pivotQuery: {
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        granularity: 'week',
        dateRange: ['2023-05-10T00:00:00.000', '2023-05-14T23:59:59.999'],
      },
    ],
    limit: 10000,
    timezone: 'UTC',
    order: [],
    filters: [],
    dimensions: [],
    rowLimit: 10000,
    queryType: 'regularQuery',
  },
  slowQuery: false,
};

describe('drill down query', () => {
  const resultSet1 = new ResultSet(loadResponse());
  const resultSet2 = new ResultSet(
    loadResponse({
      timezone: 'America/Los_Angeles',
    })
  );
  const resultSet3 = new ResultSet(
    loadResponse({
      filters: [
        {
          member: 'Users.country',
          operator: 'equals',
          values: ['Los Angeles'],
        },
      ],
    })
  );
  const resultSet4 = new ResultSet(
    loadResponse({
      dimensions: ['Statuses.potential'],
      timeDimensions: [],
    })
  );
  const resultSet5 = new ResultSet(
    loadResponse({
      timeDimensions: [
        {
          dimension: 'Orders.ts',
          granularity: 'week',
        }
      ]
    })
  );

  it('handles a query with a time dimension', () => {
    expect(
      resultSet1.drillDown({
        xValues: ['2020-08-01T00:00:00.000'],
      })
    ).toEqual({
      measures: [],
      segments: [],
      dimensions: ['Orders.id', 'Orders.title'],
      filters: [
        {
          member: 'Orders.count',
          operator: 'measureFilter',
        },
      ],
      timeDimensions: [
        {
          dimension: 'Orders.ts',
          dateRange: ['2020-08-01T00:00:00.000', '2020-08-01T23:59:59.999'],
        },
      ],
      timezone: 'UTC',
    });
  });

  it('respects the time zone', () => {
    expect(
      resultSet2.drillDown({
        xValues: ['2020-08-01T00:00:00.000'],
      })
    ).toEqual({
      measures: [],
      segments: [],
      dimensions: ['Orders.id', 'Orders.title'],
      filters: [
        {
          member: 'Orders.count',
          operator: 'measureFilter',
        },
      ],
      timeDimensions: [
        {
          dimension: 'Orders.ts',
          dateRange: ['2020-08-01T00:00:00.000', '2020-08-01T23:59:59.999'],
        },
      ],
      timezone: 'America/Los_Angeles',
    });
  });

  it('propagates parent filters', () => {
    expect(
      resultSet3.drillDown({
        xValues: ['2020-08-01T00:00:00.000'],
      })
    ).toEqual({
      measures: [],
      segments: [],
      dimensions: ['Orders.id', 'Orders.title'],
      filters: [
        {
          member: 'Orders.count',
          operator: 'measureFilter',
        },
        {
          member: 'Users.country',
          operator: 'equals',
          values: ['Los Angeles'],
        },
      ],
      timeDimensions: [
        {
          dimension: 'Orders.ts',
          dateRange: ['2020-08-01T00:00:00.000', '2020-08-01T23:59:59.999'],
        },
      ],
      timezone: 'UTC',
    });
  });

  it('handles null values', () => {
    expect(resultSet4.drillDown({ xvalues: [null] })).toEqual({
      measures: [],
      segments: [],
      dimensions: ['Orders.id', 'Orders.title'],
      filters: [
        {
          member: 'Orders.count',
          operator: 'measureFilter',
        },
        {
          member: 'Statuses.potential',
          operator: 'notSet',
        },
      ],
      timeDimensions: [],
      timezone: 'UTC',
    });
  });

  it('respects the parent time dimension date range', () => {
    const resultSet = new ResultSet(loadResponse2);

    expect(
      resultSet.drillDown({ xValues: ['2023-05-08T00:00:00.000'] })
    ).toEqual({
      measures: [],
      segments: [],
      dimensions: ['Orders.id', 'Orders.createdAt'],
      filters: [
        {
          operator: 'measureFilter',
          member: 'Orders.count',
        },
      ],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          dateRange: ['2023-05-10T00:00:00.000', '2023-05-14T23:59:59.999'],
        },
      ],
      timezone: 'UTC'
    });
  });

  it('snap date range to granularity if the date range is not defined in the time dimension', () => {
    expect(
      resultSet5.drillDown({ xValues: ['2020-08-01T00:00:00.000'] })
    ).toEqual({
      measures: [],
      segments: [],
      dimensions: ['Orders.id', 'Orders.title'],
      filters: [
        {
          member: 'Orders.count',
          operator: 'measureFilter',
        },
      ],
      timeDimensions: [
        {
          dimension: 'Orders.ts',
          dateRange: ['2020-07-27T00:00:00.000', '2020-08-02T23:59:59.999'],
        },
      ],
      timezone: 'UTC',
    });
  });
});
