/* globals describe,test,expect */

import 'jest';
import dayjs from 'dayjs';
import ko from 'dayjs/locale/ko';
import ResultSet from '../src/ResultSet';

describe('ResultSet Granularity', () => {
  describe('chartPivot', () => {
    test('week granularity', () => {
      const result = new ResultSet({
        queryType: 'regularQuery',
        results: [
          {
            query: {
              measures: ['LineItems.count'],
              timeDimensions: [
                {
                  dimension: 'LineItems.createdAt',
                  granularity: 'week',
                  dateRange: ['2019-01-08T00:00:00.000', '2019-01-18T00:00:00.000'],
                },
              ],
              filters: [
                {
                  operator: 'equals',
                  values: ['us-ut'],
                  member: 'Users.state',
                },
              ],
              limit: 2,
              rowLimit: 2,
              timezone: 'UTC',
              order: [],
              dimensions: [],
            },
            data: [
              {
                'LineItems.createdAt.week': '2019-01-07T00:00:00.000',
                'LineItems.createdAt': '2019-01-07T00:00:00.000',
                'LineItems.count': '2',
              },
            ],
            lastRefreshTime: '2021-07-07T14:31:30.458Z',
            annotation: {
              measures: {
                'LineItems.count': {
                  title: 'Line Items Count',
                  shortTitle: 'Count',
                  type: 'number',
                  drillMembers: ['LineItems.id', 'LineItems.createdAt'],
                  drillMembersGrouped: {
                    measures: [],
                    dimensions: ['LineItems.id', 'LineItems.createdAt'],
                  },
                },
              },
              dimensions: {},
              segments: {},
              timeDimensions: {
                'LineItems.createdAt.week': {
                  title: 'Line Items Created at',
                  shortTitle: 'Created at',
                  type: 'time',
                },
                'LineItems.createdAt': {
                  title: 'Line Items Created at',
                  shortTitle: 'Created at',
                  type: 'time',
                },
              },
            },
            slowQuery: false,
          },
        ],
        pivotQuery: {
          measures: ['LineItems.count'],
          timeDimensions: [
            {
              dimension: 'LineItems.createdAt',
              granularity: 'week',
              dateRange: ['2019-01-08T00:00:00.000', '2019-01-18T00:00:00.000'],
            },
          ],
          filters: [
            {
              operator: 'equals',
              values: ['us-ut'],
              member: 'Users.state',
            },
          ],
          limit: 2,
          rowLimit: 2,
          timezone: 'UTC',
          order: [],
          dimensions: [],
          queryType: 'regularQuery',
        },
        slowQuery: false,
      } as any);

      expect(result.chartPivot()).toStrictEqual([
        {
          x: '2019-01-07T00:00:00.000',
          xValues: ['2019-01-07T00:00:00.000'],
          'LineItems.count': 2,
        },
        {
          x: '2019-01-14T00:00:00.000',
          xValues: ['2019-01-14T00:00:00.000'],
          'LineItems.count': 0,
        },
      ]);
    });

    test('week granularity in other locale', () => {
      dayjs.locale(ko);
      const result = new ResultSet({
        queryType: 'regularQuery',
        results: [
          {
            query: {
              measures: ['LineItems.count'],
              timeDimensions: [
                {
                  dimension: 'LineItems.createdAt',
                  granularity: 'week',
                  dateRange: ['2019-01-08T00:00:00.000', '2019-01-18T00:00:00.000'],
                },
              ],
              filters: [
                {
                  operator: 'equals',
                  values: ['us-ut'],
                  member: 'Users.state',
                },
              ],
              limit: 2,
              rowLimit: 2,
              timezone: 'UTC',
              order: [],
              dimensions: [],
            },
            data: [
              {
                'LineItems.createdAt.week': '2019-01-07T00:00:00.000',
                'LineItems.createdAt': '2019-01-07T00:00:00.000',
                'LineItems.count': '2',
              },
            ],
            lastRefreshTime: '2021-07-07T14:31:30.458Z',
            annotation: {
              measures: {
                'LineItems.count': {
                  title: 'Line Items Count',
                  shortTitle: 'Count',
                  type: 'number',
                  drillMembers: ['LineItems.id', 'LineItems.createdAt'],
                  drillMembersGrouped: {
                    measures: [],
                    dimensions: ['LineItems.id', 'LineItems.createdAt'],
                  },
                },
              },
              dimensions: {},
              segments: {},
              timeDimensions: {
                'LineItems.createdAt.week': {
                  title: 'Line Items Created at',
                  shortTitle: 'Created at',
                  type: 'time',
                },
                'LineItems.createdAt': {
                  title: 'Line Items Created at',
                  shortTitle: 'Created at',
                  type: 'time',
                },
              },
            },
            slowQuery: false,
          },
        ],
        pivotQuery: {
          measures: ['LineItems.count'],
          timeDimensions: [
            {
              dimension: 'LineItems.createdAt',
              granularity: 'week',
              dateRange: ['2019-01-08T00:00:00.000', '2019-01-18T00:00:00.000'],
            },
          ],
          filters: [
            {
              operator: 'equals',
              values: ['us-ut'],
              member: 'Users.state',
            },
          ],
          limit: 2,
          rowLimit: 2,
          timezone: 'UTC',
          order: [],
          dimensions: [],
          queryType: 'regularQuery',
        },
        slowQuery: false,
      } as any);

      expect(result.chartPivot()).toStrictEqual([
        {
          x: '2019-01-07T00:00:00.000',
          xValues: ['2019-01-07T00:00:00.000'],
          'LineItems.count': 2,
        },
        {
          x: '2019-01-14T00:00:00.000',
          xValues: ['2019-01-14T00:00:00.000'],
          'LineItems.count': 0,
        },
      ]);
    });

    test('hour granularity (end minutes > start minutes)', () => {
      const result = new ResultSet({
        queryType: 'regularQuery',
        results: [
          {
            query: {
              measures: ['LineItems.count'],
              timeDimensions: [
                {
                  dimension: 'LineItems.createdAt',
                  granularity: 'hour',
                  dateRange: ['2019-01-08T01:45:25.342', '2019-01-08T07:45:58.399'],
                },
              ],
              filters: [],
              timezone: 'UTC',
              order: [],
              dimensions: [],
            },
            data: [
              {
                'LineItems.createdAt.hour': '2019-01-08T01:00:00.000',
                'LineItems.createdAt': '2019-01-08T01:00:00.000',
                'LineItems.count': '2',
              },
              {
                'LineItems.createdAt.hour': '2019-01-08T02:00:00.000',
                'LineItems.createdAt': '2019-01-08T02:00:00.000',
                'LineItems.count': '3',
              },
              {
                'LineItems.createdAt.hour': '2019-01-08T03:00:00.000',
                'LineItems.createdAt': '2019-01-08T03:00:00.000',
                'LineItems.count': '4',
              },
              {
                'LineItems.createdAt.hour': '2019-01-08T04:00:00.000',
                'LineItems.createdAt': '2019-01-08T04:00:00.000',
                'LineItems.count': '5',
              },
              {
                'LineItems.createdAt.hour': '2019-01-08T05:00:00.000',
                'LineItems.createdAt': '2019-01-08T05:00:00.000',
                'LineItems.count': '6',
              },
              {
                'LineItems.createdAt.hour': '2019-01-08T06:00:00.000',
                'LineItems.createdAt': '2019-01-08T06:00:00.000',
                'LineItems.count': '7',
              },
            ],
            lastRefreshTime: '2021-07-07T14:31:30.458Z',
            annotation: {
              measures: {
                'LineItems.count': {
                  title: 'Line Items Count',
                  shortTitle: 'Count',
                  type: 'number',
                  drillMembers: ['LineItems.id', 'LineItems.createdAt'],
                  drillMembersGrouped: {
                    measures: [],
                    dimensions: ['LineItems.id', 'LineItems.createdAt'],
                  },
                },
              },
              dimensions: {},
              segments: {},
              timeDimensions: {
                'LineItems.createdAt.week': {
                  title: 'Line Items Created at',
                  shortTitle: 'Created at',
                  type: 'time',
                },
                'LineItems.createdAt': {
                  title: 'Line Items Created at',
                  shortTitle: 'Created at',
                  type: 'time',
                },
              },
            },
            slowQuery: false,
          },
        ],
        pivotQuery: {
          measures: ['LineItems.count'],
          timeDimensions: [
            {
              dimension: 'LineItems.createdAt',
              granularity: 'hour',
              dateRange: ['2019-01-08T01:45:25.342', '2019-01-08T07:45:58.399'],
            },
          ],
          filters: [],
          timezone: 'UTC',
          order: [],
          dimensions: [],
          queryType: 'regularQuery',
        },
        slowQuery: false,
      } as any);

      expect(result.chartPivot()).toStrictEqual([
        {
          x: '2019-01-08T01:00:00.000',
          xValues: ['2019-01-08T01:00:00.000'],
          'LineItems.count': 2,
        },
        {
          x: '2019-01-08T02:00:00.000',
          xValues: ['2019-01-08T02:00:00.000'],
          'LineItems.count': 3,
        },
        {
          x: '2019-01-08T03:00:00.000',
          xValues: ['2019-01-08T03:00:00.000'],
          'LineItems.count': 4,
        },
        {
          x: '2019-01-08T04:00:00.000',
          xValues: ['2019-01-08T04:00:00.000'],
          'LineItems.count': 5,
        },
        {
          x: '2019-01-08T05:00:00.000',
          xValues: ['2019-01-08T05:00:00.000'],
          'LineItems.count': 6,
        },
        {
          x: '2019-01-08T06:00:00.000',
          xValues: ['2019-01-08T06:00:00.000'],
          'LineItems.count': 7,
        },
        {
          x: '2019-01-08T07:00:00.000',
          xValues: ['2019-01-08T07:00:00.000'],
          'LineItems.count': 0,
        },
      ]);
    });

    test('hour granularity (end minutes < start minutes)', () => {
      const result = new ResultSet({
        queryType: 'regularQuery',
        results: [
          {
            query: {
              measures: ['LineItems.count'],
              timeDimensions: [
                {
                  dimension: 'LineItems.createdAt',
                  granularity: 'hour',
                  dateRange: ['2019-01-08T01:45:25.342', '2019-01-08T07:35:58.399'],
                },
              ],
              filters: [],
              timezone: 'UTC',
              order: [],
              dimensions: [],
            },
            data: [
              {
                'LineItems.createdAt.hour': '2019-01-08T01:00:00.000',
                'LineItems.createdAt': '2019-01-08T01:00:00.000',
                'LineItems.count': '2',
              },
              {
                'LineItems.createdAt.hour': '2019-01-08T02:00:00.000',
                'LineItems.createdAt': '2019-01-08T02:00:00.000',
                'LineItems.count': '3',
              },
              {
                'LineItems.createdAt.hour': '2019-01-08T03:00:00.000',
                'LineItems.createdAt': '2019-01-08T03:00:00.000',
                'LineItems.count': '4',
              },
              {
                'LineItems.createdAt.hour': '2019-01-08T04:00:00.000',
                'LineItems.createdAt': '2019-01-08T04:00:00.000',
                'LineItems.count': '5',
              },
              {
                'LineItems.createdAt.hour': '2019-01-08T05:00:00.000',
                'LineItems.createdAt': '2019-01-08T05:00:00.000',
                'LineItems.count': '6',
              },
              {
                'LineItems.createdAt.hour': '2019-01-08T06:00:00.000',
                'LineItems.createdAt': '2019-01-08T06:00:00.000',
                'LineItems.count': '7',
              },
            ],
            lastRefreshTime: '2021-07-07T14:31:30.458Z',
            annotation: {
              measures: {
                'LineItems.count': {
                  title: 'Line Items Count',
                  shortTitle: 'Count',
                  type: 'number',
                  drillMembers: ['LineItems.id', 'LineItems.createdAt'],
                  drillMembersGrouped: {
                    measures: [],
                    dimensions: ['LineItems.id', 'LineItems.createdAt'],
                  },
                },
              },
              dimensions: {},
              segments: {},
              timeDimensions: {
                'LineItems.createdAt.week': {
                  title: 'Line Items Created at',
                  shortTitle: 'Created at',
                  type: 'time',
                },
                'LineItems.createdAt': {
                  title: 'Line Items Created at',
                  shortTitle: 'Created at',
                  type: 'time',
                },
              },
            },
            slowQuery: false,
          },
        ],
        pivotQuery: {
          measures: ['LineItems.count'],
          timeDimensions: [
            {
              dimension: 'LineItems.createdAt',
              granularity: 'hour',
              dateRange: ['2019-01-08T01:45:25.342', '2019-01-08T07:35:58.399'],
            },
          ],
          filters: [],
          timezone: 'UTC',
          order: [],
          dimensions: [],
          queryType: 'regularQuery',
        },
        slowQuery: false,
      } as any);

      expect(result.chartPivot()).toStrictEqual([
        {
          x: '2019-01-08T01:00:00.000',
          xValues: ['2019-01-08T01:00:00.000'],
          'LineItems.count': 2,
        },
        {
          x: '2019-01-08T02:00:00.000',
          xValues: ['2019-01-08T02:00:00.000'],
          'LineItems.count': 3,
        },
        {
          x: '2019-01-08T03:00:00.000',
          xValues: ['2019-01-08T03:00:00.000'],
          'LineItems.count': 4,
        },
        {
          x: '2019-01-08T04:00:00.000',
          xValues: ['2019-01-08T04:00:00.000'],
          'LineItems.count': 5,
        },
        {
          x: '2019-01-08T05:00:00.000',
          xValues: ['2019-01-08T05:00:00.000'],
          'LineItems.count': 6,
        },
        {
          x: '2019-01-08T06:00:00.000',
          xValues: ['2019-01-08T06:00:00.000'],
          'LineItems.count': 7,
        },
        {
          x: '2019-01-08T07:00:00.000',
          xValues: ['2019-01-08T07:00:00.000'],
          'LineItems.count': 0,
        },
      ]);
    });
  });
});
