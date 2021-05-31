import 'jest';
import { defaultHeuristics } from '../utils';

jest.mock('moment-range', () => {
  const Moment = jest.requireActual('moment');
  const MomentRange = jest.requireActual('moment-range');
  const moment = MomentRange.extendMoment(Moment);
  return {
    extendMoment: () => moment,
  };
});

describe('default heuristics', () => {
  it('removes the time dimension when the measure is removed', () => {
    const newState = {
      query: {
        timeDimensions: [
          {
            dimension: 'Orders.ts',
            granularity: 'month',
            dateRange: 'this year',
          },
        ],
      },
    };
    const oldQuery = {
      measures: ['Orders.count'],
      timeDimensions: [
        {
          dimension: 'Orders.ts',
          granularity: 'month',
          dateRange: 'this year',
        },
      ],
    };
    expect(
      defaultHeuristics(newState, oldQuery, {
        meta: {
          defaultTimeDimensionNameFor() {
            return 'Orders.ts';
          },
        },
      })
    ).toStrictEqual({
      pivotConfig: null,
      query: {
        filters: [],
        timeDimensions: [],
      },
      sessionGranularity: null,
      shouldApplyHeuristicOrder: true,
    });
  });

  it('respects the granularity', () => {
    const meta = {
      defaultTimeDimensionNameFor() {
        return 'Orders.createdAt';
      },
    };

    const newState = {
      query: {
        measures: ['Orders.count'],
        timeDimensions: [
          {
            dimension: 'Orders.createdAt',
            granularity: 'month',
          },
        ],
      },
    };

    const oldQuery = {};

    expect(defaultHeuristics(newState, oldQuery, { meta })).toMatchObject({
      query: {
        timeDimensions: [
          {
            granularity: 'month',
          },
        ],
      },
    });
  });

  it('handles dateRange correctly', () => {
    const meta = {
      defaultTimeDimensionNameFor() {
        return 'Orders.createdAt';
      },
    };

    const newState = {
      query: {
        measures: ['Orders.count'],
        timeDimensions: [
          {
            dimension: 'Orders.createdAt',
            granularity: 'month',
            dateRange: 'This quarter',
          },
        ],
      },
    };

    expect(defaultHeuristics(newState, {}, { meta })).toMatchObject({
      query: {
        timeDimensions: [
          {
            granularity: 'month',
            dateRange: 'This quarter',
          },
        ],
      },
    });
  });
});
