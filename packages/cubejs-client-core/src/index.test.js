/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview CubejsApi class unit tests.
 */

/* globals describe,test,expect,beforeEach,jest */

import ResultSet from './ResultSet';
import { CubejsApi } from './index';

jest.mock('./ResultSet');
beforeEach(() => {
  ResultSet.mockClear();
});

const mockData = {
  regular_1: {
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
  },
  regular_2: {
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
  },
  compare: [{
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
  }],
  blending: [{
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
  }],
};

describe('CubejsApi', () => {
  test('CubejsApi#loadResponseInternal should work with the "default" resType for regular query', () => {
    const api = new CubejsApi(undefined, {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });
    const income = {
      results: [{
        query: {},
        data: JSON.parse(
          JSON.stringify(
            mockData.regular_1.result_default
          )
        )
      }],
    };
    const outcome = {
      results: [{
        query: {},
        data: JSON.parse(
          JSON.stringify(
            mockData.regular_1.result_default
          )
        )
      }],
    };
    api.loadResponseInternal(income);
    expect(ResultSet).toHaveBeenCalled();
    expect(ResultSet).toHaveBeenCalledTimes(1);
    expect(ResultSet).toHaveBeenCalledWith(outcome, {
      parseDateMeasures: undefined
    });
  });

  test('CubejsApi#loadResponseInternal should work with the "default" resType for compare date range query', () => {
    const api = new CubejsApi(undefined, {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });
    const income = {
      results: [{
        query: {},
        data: JSON.parse(
          JSON.stringify(
            mockData.compare[0].result_default
          )
        )
      }, {
        query: {},
        data: JSON.parse(
          JSON.stringify(
            mockData.compare[1].result_default
          )
        )
      }],
    };
    const outcome = {
      results: [{
        query: {},
        data: JSON.parse(
          JSON.stringify(
            mockData.compare[0].result_default
          )
        )
      }, {
        query: {},
        data: JSON.parse(
          JSON.stringify(
            mockData.compare[1].result_default
          )
        )
      }],
    };
    api.loadResponseInternal(income);
    expect(ResultSet).toHaveBeenCalled();
    expect(ResultSet).toHaveBeenCalledTimes(1);
    expect(ResultSet).toHaveBeenCalledWith(outcome, {
      parseDateMeasures: undefined
    });
  });

  test('CubejsApi#loadResponseInternal should work with the "default" resType for blending query', () => {
    const api = new CubejsApi(undefined, {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });
    const income = {
      results: [{
        query: {},
        data: JSON.parse(
          JSON.stringify(
            mockData.blending[0].result_default
          )
        )
      }, {
        query: {},
        data: JSON.parse(
          JSON.stringify(
            mockData.blending[1].result_default
          )
        )
      }],
    };
    const outcome = {
      results: [{
        query: {},
        data: JSON.parse(
          JSON.stringify(
            mockData.blending[0].result_default
          )
        )
      }, {
        query: {},
        data: JSON.parse(
          JSON.stringify(
            mockData.blending[1].result_default
          )
        )
      }],
    };
    api.loadResponseInternal(income);
    expect(ResultSet).toHaveBeenCalled();
    expect(ResultSet).toHaveBeenCalledTimes(1);
    expect(ResultSet).toHaveBeenCalledWith(outcome, {
      parseDateMeasures: undefined
    });
  });

  test('CubejsApi#loadResponseInternal should work with the "compact" resType for regular query', () => {
    const api = new CubejsApi(undefined, {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });
    const income = {
      results: [{
        query: { responseFormat: 'compact' },
        data: JSON.parse(
          JSON.stringify(
            mockData.regular_1.result_compact
          )
        )
      }],
    };
    const outcome = {
      results: [{
        query: { responseFormat: 'compact' },
        data: JSON.parse(
          JSON.stringify(
            mockData.regular_1.result_default
          )
        )
      }],
    };
    api.loadResponseInternal(income);
    expect(ResultSet).toHaveBeenCalled();
    expect(ResultSet).toHaveBeenCalledTimes(1);
    expect(ResultSet).toHaveBeenCalledWith(outcome, {
      parseDateMeasures: undefined
    });
  });

  test('CubejsApi#loadResponseInternal should work with the "compact" resType for compare date range query', () => {
    const api = new CubejsApi(undefined, {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });
    const income = {
      results: [{
        query: { responseFormat: 'compact' },
        data: JSON.parse(
          JSON.stringify(
            mockData.compare[0].result_compact
          )
        )
      }, {
        query: { responseFormat: 'compact' },
        data: JSON.parse(
          JSON.stringify(
            mockData.compare[1].result_compact
          )
        )
      }],
    };
    const outcome = {
      results: [{
        query: { responseFormat: 'compact' },
        data: JSON.parse(
          JSON.stringify(
            mockData.compare[0].result_default
          )
        )
      }, {
        query: { responseFormat: 'compact' },
        data: JSON.parse(
          JSON.stringify(
            mockData.compare[1].result_default
          )
        )
      }],
    };
    api.loadResponseInternal(income);
    expect(ResultSet).toHaveBeenCalled();
    expect(ResultSet).toHaveBeenCalledTimes(1);
    expect(ResultSet).toHaveBeenCalledWith(outcome, {
      parseDateMeasures: undefined
    });
  });

  test('CubejsApi#loadResponseInternal should work with the "compact" resType for blending query', () => {
    const api = new CubejsApi(undefined, {
      apiUrl: 'http://localhost:4000/cubejs-api/v1',
    });
    const income = {
      results: [{
        query: { responseFormat: 'compact' },
        data: JSON.parse(
          JSON.stringify(
            mockData.blending[0].result_compact
          )
        )
      }, {
        query: { responseFormat: 'compact' },
        data: JSON.parse(
          JSON.stringify(
            mockData.blending[1].result_compact
          )
        )
      }],
    };
    const outcome = {
      results: [{
        query: { responseFormat: 'compact' },
        data: JSON.parse(
          JSON.stringify(
            mockData.blending[0].result_default
          )
        )
      }, {
        query: { responseFormat: 'compact' },
        data: JSON.parse(
          JSON.stringify(
            mockData.blending[1].result_default
          )
        )
      }],
    };
    api.loadResponseInternal(income);
    expect(ResultSet).toHaveBeenCalled();
    expect(ResultSet).toHaveBeenCalledTimes(1);
    expect(ResultSet).toHaveBeenCalledWith(outcome, {
      parseDateMeasures: undefined
    });
  });
});
