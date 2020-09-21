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
  test('it removes the time dimension when the measure is removed', () => {
    const newQuery = {
      measures: ['Orders.count'],
      dimensions: ['Users.gender'],
    };
    const oldQuery = {};
    expect(
      defaultHeuristics(newQuery, oldQuery, {
        meta: {
          defaultTimeDimensionNameFor() {
            return 'Orders.ts';
          },
        },
      })
    ).toStrictEqual({});
  });
  // test('it removes the time dimension when the measure is removed', () => {
  //   const newQuery = {
  //     dimensions: [
  //       {
  //         dimension: 'Orders.ts',
  //         granularity: 'month',
  //       },
  //     ],
  //   };
  //   const oldQuery = {
  //     measures: ['Orders.count'],
  //     dimensions: [
  //       {
  //         dimension: 'Orders.ts',
  //         granularity: 'month',
  //       },
  //     ],
  //   };
  //   expect(
  //     defaultHeuristics(newQuery, oldQuery, {
  //       meta: {
  //         defaultTimeDimensionNameFor() {
  //           return 'Orders.ts';
  //         },
  //       },
  //     })
  //   ).toStrictEqual({});
  // });
});
