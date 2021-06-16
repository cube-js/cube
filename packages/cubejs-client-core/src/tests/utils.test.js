import 'jest';

import { TIME_SERIES, dayRange } from '../ResultSet';
import { defaultOrder } from '../utils';

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
    const range = dayRange('2021-01-01 00:00:00.000', '2021-01-07 00:00:00.000');

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
