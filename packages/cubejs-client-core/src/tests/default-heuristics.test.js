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
    const newQuery = {
      timeDimensions: [
        {
          dimension: 'Orders.ts',
          granularity: 'month',
          dateRange: 'this year',
        },
      ],
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
      defaultHeuristics(newQuery, oldQuery, {
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
});
