import { bench, describe } from 'vitest';

import ResultSet from '../src/ResultSet';

const STATUSES = ['completed', 'processing', 'shipped'];

function generateLoadResponse(rowCount: number) {
  const data: Record<string, string>[] = [];
  const startDate = new Date('2020-01-01T00:00:00.000Z');

  for (let i = 0; i < rowCount; i++) {
    const date = new Date(startDate.getTime() + i * 86400000);
    const dateStr = date.toISOString().replace('Z', '');

    data.push({
      'Orders.createdAt.day': dateStr,
      'Orders.createdAt': dateStr,
      'Orders.status': STATUSES[i % 3],
      'Orders.count': String(Math.floor(Math.random() * 1000)),
    });
  }

  return {
    query: {
      measures: ['Orders.count'],
      dimensions: ['Orders.status'],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          granularity: 'day',
        },
      ],
    },
    data,
    lastRefreshTime: '2024-01-01T00:00:00.000Z',
    usedPreAggregations: {},
    annotation: {
      measures: {
        'Orders.count': {
          title: 'Orders Count',
          shortTitle: 'Count',
          type: 'number',
        },
      },
      dimensions: {
        'Orders.status': {
          title: 'Orders Status',
          shortTitle: 'Status',
          type: 'string',
        },
      },
      segments: {},
      timeDimensions: {
        'Orders.createdAt': {
          title: 'Orders Created at',
          shortTitle: 'Created at',
          type: 'time',
        },
        'Orders.createdAt.day': {
          title: 'Orders Created at',
          shortTitle: 'Created at',
          type: 'time',
          granularity: {
            name: 'day',
            title: 'day',
            interval: '1 day',
          },
        },
      },
    },
  };
}

function computeHourRange(targetHours: number): [string, string] {
  const start = new Date('2020-01-01T00:00:00.000Z');
  const end = new Date(start.getTime() + (targetHours - 1) * 3600000);
  return [
    start.toISOString().replace('Z', ''),
    end.toISOString().replace('Z', ''),
  ];
}

const ROW_COUNTS = [5_000, 10_000, 25_000, 50_000, 100_000];

// Pre-build all ResultSets outside benchmarks to avoid measuring construction time
const pivotResultSets = new Map<number, ResultSet>();
for (const count of ROW_COUNTS) {
  pivotResultSets.set(count, new ResultSet(generateLoadResponse(count) as any));
}

describe('pivot', () => {
  for (const count of ROW_COUNTS) {
    const label = `${count / 1000}k rows`;
    const rs = pivotResultSets.get(count)!;

    bench(`pivot - ${label}`, () => {
      rs.pivot();
    });

    bench(`chartPivot - ${label}`, () => {
      rs.chartPivot();
    });

    bench(`tablePivot - ${label}`, () => {
      rs.tablePivot();
    });
  }
});

describe('timeSeries', () => {
  const rs = new ResultSet({} as any);

  for (const count of ROW_COUNTS) {
    const label = `${count / 1000}k hours`;
    const dateRange = computeHourRange(count);

    bench(`timeSeries - ${label}`, () => {
      rs.timeSeries(
        {
          dimension: 'Orders.createdAt',
          granularity: 'hour',
          dateRange,
        },
      );
    });
  }
});
