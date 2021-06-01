import 'jest';
import moment from 'moment';

import { TIME_SERIES } from '../ResultSet';
import { defaultOrder } from '../utils';

jest.mock('moment-range', () => {
  const Moment = jest.requireActual('moment');
  const MomentRange = jest.requireActual('moment-range');
  const moment = MomentRange.extendMoment(Moment);
  return {
    extendMoment: () => moment,
  };
});

describe('utils', () => {
  test('default order', () => {
    const query = {
      measures: ['Orders.count'],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          granularity: 'day',
        },
      ],
    };
    expect(defaultOrder(query)).toStrictEqual({
      'Orders.createdAt': 'asc',
    });
  });

  test('time series', () => {
    const range = moment.range('2021-01-01 00:00:00.000', '2021-01-07 00:00:00.000');

    expect(TIME_SERIES.day(range)).toStrictEqual([
      '2021-01-01T00:00:00.000',
      '2021-01-02T00:00:00.000',
      '2021-01-03T00:00:00.000',
      '2021-01-04T00:00:00.000',
      '2021-01-05T00:00:00.000',
      '2021-01-06T00:00:00.000',
      '2021-01-07T00:00:00.000',
    ]);
  });
});
