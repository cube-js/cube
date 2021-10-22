import 'jest';
import ResultSet from '../ResultSet';

describe('resultSet tablePivot and tableColumns', () => {
  describe('it works with one measure', () => {
    const resultSet = new ResultSet({
      query: {
        measures: ['Orders.count'],
        dimensions: ['Users.country', 'Users.gender'],
      },
      data: [
        {
          'Users.country': 'Germany',
          'Users.gender': 'male',
          'Orders.count': 10,
          'Orders.amount': 11,
        },
        {
          'Users.country': 'Germany',
          'Users.gender': 'female',
          'Orders.count': 12,
          'Orders.amount': 13,
        },
        {
          'Users.country': 'US',
          'Users.gender': 'male',
          'Orders.count': 5,
          'Orders.amount': 6,
        },
        {
          'Users.country': 'US',
          'Users.gender': 'female',
          'Orders.count': 7,
          'Orders.amount': 8,
        },
      ],
      annotation: {
        measures: {
          'Orders.count': {
            title: 'Orders Count',
            shortTitle: 'Count',
            type: 'number',
          },
          'Orders.amount': {
            title: 'Orders Amount',
            shortTitle: 'Amount',
            type: 'number',
          },
        },
        dimensions: {
          'Users.country': {
            title: 'Users Country',
            shortTitle: 'Country',
            type: 'string',
          },
          'Users.gender': {
            title: 'Users Gender',
            shortTitle: 'Gender',
            type: 'string',
          },
        },
        segments: {},
        timeDimensions: {},
      },
    });

    describe('all dimensions on `x` axis', () => {
      const pivotConfig = {
        x: ['Users.country', 'Users.gender'],
        y: ['measures'],
      };

      test('tablePivot', () => {
        expect(resultSet.tablePivot(pivotConfig)).toEqual([
          {
            'Users.country': 'Germany',
            'Users.gender': 'male',
            'Orders.count': 10,
          },
          {
            'Users.country': 'Germany',
            'Users.gender': 'female',
            'Orders.count': 12,
          },
          {
            'Users.country': 'US',
            'Users.gender': 'male',
            'Orders.count': 5,
          },
          {
            'Users.country': 'US',
            'Users.gender': 'female',
            'Orders.count': 7,
          },
        ]);
      });

      test('tableColumns', () => {
        expect(resultSet.tableColumns(pivotConfig)).toEqual([
          {
            key: 'Users.country',
            dataIndex: 'Users.country',
            title: 'Users Country',
            shortTitle: 'Country',
            type: 'string',
            format: undefined,
            meta: undefined,
          },
          {
            key: 'Users.gender',
            dataIndex: 'Users.gender',
            title: 'Users Gender',
            shortTitle: 'Gender',
            type: 'string',
            format: undefined,
            meta: undefined,
          },
          {
            key: 'Orders.count',
            dataIndex: 'Orders.count',
            title: 'Orders Count',
            shortTitle: 'Count',
            type: 'number',
            format: undefined,
            meta: undefined,
          },
        ]);
      });
    });

    describe('one dimension on `x` and one one `y` axis', () => {
      const pivotConfig = {
        x: ['Users.country'],
        y: ['Users.gender', 'measures'],
      };

      test('tablePivot', () => {
        expect(resultSet.tablePivot(pivotConfig)).toEqual([
          {
            'Users.country': 'Germany',
            'male,Orders.count': 10,
            'female,Orders.count': 12,
          },
          {
            'Users.country': 'US',
            'male,Orders.count': 5,
            'female,Orders.count': 7,
          },
        ]);
      });

      test('tableColumns', () => {
        expect(resultSet.tableColumns(pivotConfig)).toEqual([
          {
            key: 'Users.country',
            title: 'Users Country',
            shortTitle: 'Country',
            type: 'string',
            dataIndex: 'Users.country',
            format: undefined,
            meta: undefined,
          },
          {
            key: 'male',
            type: 'string',
            title: 'Users Gender male',
            shortTitle: 'male',
            format: undefined,
            meta: undefined,
            children: [
              {
                key: 'Orders.count',
                type: 'number',
                dataIndex: 'male,Orders.count',
                title: 'Orders Count',
                shortTitle: 'Count',
                format: undefined,
                meta: undefined,
              },
            ],
          },
          {
            key: 'female',
            type: 'string',
            title: 'Users Gender female',
            shortTitle: 'female',
            format: undefined,
            meta: undefined,
            children: [
              {
                key: 'Orders.count',
                type: 'number',
                dataIndex: 'female,Orders.count',
                title: 'Orders Count',
                shortTitle: 'Count',
                format: undefined,
                meta: undefined,
              },
            ],
          },
        ]);
      });
    });

    describe('all dimensions and measures on `y` axis', () => {
      const pivotConfig = {
        x: [],
        y: ['Users.country', 'Users.gender', 'measures'],
      };

      test('tablePivot', () => {
        expect(resultSet.tablePivot(pivotConfig)).toEqual([
          {
            'Germany,male,Orders.count': 10,
            'Germany,female,Orders.count': 12,
            'US,male,Orders.count': 5,
            'US,female,Orders.count': 7,
          },
        ]);
      });

      test('tableColumns', () => {
        expect(resultSet.tableColumns(pivotConfig)).toEqual([
          {
            key: 'Germany',
            type: 'string',
            title: 'Users Country Germany',
            shortTitle: 'Germany',
            meta: undefined,
            format: undefined,
            children: [
              {
                key: 'male',
                type: 'string',
                title: 'Users Gender male',
                shortTitle: 'male',
                meta: undefined,
                format: undefined,
                children: [
                  {
                    key: 'Orders.count',
                    type: 'number',
                    dataIndex: 'Germany,male,Orders.count',
                    title: 'Orders Count',
                    shortTitle: 'Count',
                    meta: undefined,
                    format: undefined,
                  },
                ],
              },
              {
                key: 'female',
                type: 'string',
                title: 'Users Gender female',
                shortTitle: 'female',
                meta: undefined,
                format: undefined,
                children: [
                  {
                    key: 'Orders.count',
                    type: 'number',
                    dataIndex: 'Germany,female,Orders.count',
                    title: 'Orders Count',
                    shortTitle: 'Count',
                    meta: undefined,
                    format: undefined,
                  },
                ],
              },
            ],
          },
          {
            key: 'US',
            type: 'string',
            title: 'Users Country US',
            shortTitle: 'US',
            meta: undefined,
            format: undefined,
            children: [
              {
                key: 'male',
                type: 'string',
                title: 'Users Gender male',
                shortTitle: 'male',
                meta: undefined,
                format: undefined,
                children: [
                  {
                    key: 'Orders.count',
                    type: 'number',
                    dataIndex: 'US,male,Orders.count',
                    title: 'Orders Count',
                    shortTitle: 'Count',
                    meta: undefined,
                    format: undefined,
                  },
                ],
              },
              {
                key: 'female',
                type: 'string',
                title: 'Users Gender female',
                shortTitle: 'female',
                meta: undefined,
                format: undefined,
                children: [
                  {
                    key: 'Orders.count',
                    type: 'number',
                    dataIndex: 'US,female,Orders.count',
                    title: 'Orders Count',
                    shortTitle: 'Count',
                    meta: undefined,
                    format: undefined,
                  },
                ],
              },
            ],
          },
        ]);
      });
    });

    describe('all dimensions and measures on `x` axis', () => {
      const pivotConfig = {
        x: ['Users.country', 'Users.gender', 'measures'],
        y: [],
      };

      test('tablePivot', () => {
        expect(resultSet.tablePivot(pivotConfig)).toEqual([
          {
            'Users.country': 'Germany',
            'Users.gender': 'male',
            measures: 'Orders.count',
            value: 10,
          },
          {
            'Users.country': 'Germany',
            'Users.gender': 'female',
            measures: 'Orders.count',
            value: 12,
          },
          {
            'Users.country': 'US',
            'Users.gender': 'male',
            measures: 'Orders.count',
            value: 5,
          },
          {
            'Users.country': 'US',
            'Users.gender': 'female',
            measures: 'Orders.count',
            value: 7,
          },
        ]);
      });

      test('tableColumns', () => {
        expect(resultSet.tableColumns(pivotConfig)).toEqual([
          {
            key: 'Users.country',
            title: 'Users Country',
            shortTitle: 'Country',
            type: 'string',
            dataIndex: 'Users.country',
            format: undefined,
            meta: undefined,
          },
          {
            key: 'Users.gender',
            title: 'Users Gender',
            shortTitle: 'Gender',
            type: 'string',
            dataIndex: 'Users.gender',
            format: undefined,
            meta: undefined,
          },
          {
            key: 'measures',
            dataIndex: 'measures',
            title: 'Measures',
            shortTitle: 'Measures',
            type: 'string',
          },
          {
            key: 'value',
            dataIndex: 'value',
            title: 'Value',
            shortTitle: 'Value',
            type: 'string',
          },
        ]);
      });
    });

    test('measures on `x` axis', () => {
      const pivotConfig = {
        x: ['Users.gender', 'measures'],
        y: ['Users.country'],
      };

      expect(resultSet.tablePivot(pivotConfig)).toEqual([
        {
          'Users.gender': 'male',
          measures: 'Orders.count',
          US: 5,
          Germany: 10,
        },
        {
          'Users.gender': 'female',
          measures: 'Orders.count',
          US: 7,
          Germany: 12,
        },
      ]);
    });
  });

  describe('it works with more than one measure', () => {
    const resultSet = new ResultSet({
      query: {
        measures: ['Orders.count', 'Orders.amount'],
        dimensions: ['Users.country', 'Users.gender'],
      },
      data: [
        {
          'Users.country': 'Germany',
          'Users.gender': 'male',
          'Orders.count': 10,
          'Orders.amount': 11,
        },
        {
          'Users.country': 'Germany',
          'Users.gender': 'female',
          'Orders.count': 12,
          'Orders.amount': 13,
        },
        {
          'Users.country': 'US',
          'Users.gender': 'male',
          'Orders.count': 5,
          'Orders.amount': 6,
        },
        {
          'Users.country': 'US',
          'Users.gender': 'female',
          'Orders.count': 7,
          'Orders.amount': 8,
        },
      ],
      annotation: {
        measures: {
          'Orders.count': {
            title: 'Orders Count',
            shortTitle: 'Count',
            type: 'number',
          },
          'Orders.amount': {
            title: 'Orders Amount',
            shortTitle: 'Amount',
            type: 'number',
          },
        },
        dimensions: {
          'Users.country': {
            title: 'Users Country',
            shortTitle: 'Country',
            type: 'string',
          },
          'Users.gender': {
            title: 'Users Gender',
            shortTitle: 'Gender',
            type: 'string',
          },
        },
        segments: {},
        timeDimensions: {},
      },
    });

    test('all dimensions on `x` axis', () => {
      const pivotConfig = {
        x: ['Users.country', 'Users.gender'],
        y: ['measures'],
      };

      expect(resultSet.tablePivot(pivotConfig)).toEqual([
        {
          'Users.country': 'Germany',
          'Users.gender': 'male',
          'Orders.count': 10,
          'Orders.amount': 11,
        },
        {
          'Users.country': 'Germany',
          'Users.gender': 'female',
          'Orders.count': 12,
          'Orders.amount': 13,
        },
        {
          'Users.country': 'US',
          'Users.gender': 'male',
          'Orders.count': 5,
          'Orders.amount': 6,
        },
        {
          'Users.country': 'US',
          'Users.gender': 'female',
          'Orders.count': 7,
          'Orders.amount': 8,
        },
      ]);
    });

    test('one dimension on `x` and one one `y` axis', () => {
      const pivotConfig = {
        x: ['Users.country'],
        y: ['Users.gender', 'measures'],
      };

      expect(resultSet.tablePivot(pivotConfig)).toEqual([
        {
          'Users.country': 'Germany',
          'male,Orders.count': 10,
          'female,Orders.count': 12,
          'male,Orders.amount': 11,
          'female,Orders.amount': 13,
        },
        {
          'Users.country': 'US',
          'male,Orders.count': 5,
          'female,Orders.count': 7,
          'male,Orders.amount': 6,
          'female,Orders.amount': 8,
        },
      ]);
    });

    test('all dimensions and measures on `y` axis', () => {
      const pivotConfig = {
        x: [],
        y: ['Users.country', 'Users.gender', 'measures'],
      };

      expect(resultSet.tablePivot(pivotConfig)).toEqual([
        {
          'Germany,male,Orders.count': 10,
          'Germany,male,Orders.amount': 11,
          'Germany,female,Orders.count': 12,
          'Germany,female,Orders.amount': 13,
          'US,male,Orders.count': 5,
          'US,male,Orders.amount': 6,
          'US,female,Orders.count': 7,
          'US,female,Orders.amount': 8,
        },
      ]);
    });

    test('measures on `x` axis', () => {
      const pivotConfig = {
        x: ['Users.gender', 'measures'],
        y: ['Users.country'],
      };

      expect(resultSet.tablePivot(pivotConfig)).toEqual([
        {
          'Users.gender': 'male',
          measures: 'Orders.count',
          US: 5,
          Germany: 10,
        },
        {
          'Users.gender': 'male',
          measures: 'Orders.amount',
          US: 6,
          Germany: 11,
        },
        {
          'Users.gender': 'female',
          measures: 'Orders.count',
          US: 7,
          Germany: 12,
        },
        {
          'Users.gender': 'female',
          measures: 'Orders.amount',
          US: 8,
          Germany: 13,
        },
      ]);
    });
  });

  describe('it works with no data', () => {
    const resultSet = new ResultSet({
      query: {
        measures: ['Orders.count'],
        dimensions: ['Users.country', 'Users.gender'],
      },
      data: [],
      annotation: {
        measures: {
          'Orders.count': {
            title: 'Orders Count',
            shortTitle: 'Count',
            type: 'number',
          },
        },
        dimensions: {
          'Users.country': {
            title: 'Users Country',
            shortTitle: 'Country',
            type: 'string',
          },
          'Users.gender': {
            title: 'Users Gender',
            shortTitle: 'Gender',
            type: 'string',
          },
        },
        segments: {},
        timeDimensions: {},
      },
    });

    test('all dimensions on `x` axis', () => {
      const pivotConfig = {
        x: ['Users.country', 'Users.gender'],
        y: ['measures'],
      };

      expect(resultSet.tablePivot(pivotConfig)).toEqual([]);

      expect(resultSet.tableColumns(pivotConfig)).toEqual([
        {
          dataIndex: 'Users.country',
          format: undefined,
          key: 'Users.country',
          meta: undefined,
          shortTitle: 'Country',
          title: 'Users Country',
          type: 'string',
        },
        {
          dataIndex: 'Users.gender',
          format: undefined,
          key: 'Users.gender',
          meta: undefined,
          shortTitle: 'Gender',
          title: 'Users Gender',
          type: 'string',
        },
        {
          dataIndex: 'Orders.count',
          format: undefined,
          key: 'Orders.count',
          meta: undefined,
          shortTitle: 'Count',
          title: 'Orders Count',
          type: 'number',
        },
      ]);
    });

    test('one dimension on `y` axis', () => {
      const pivotConfig = {
        x: ['Users.gender'],
        y: ['Users.country', 'measures'],
      };

      expect(resultSet.tablePivot(pivotConfig)).toEqual([]);

      expect(resultSet.tableColumns(pivotConfig)).toEqual([
        {
          dataIndex: 'Users.gender',
          format: undefined,
          key: 'Users.gender',
          meta: undefined,
          shortTitle: 'Gender',
          title: 'Users Gender',
          type: 'string',
        },
        {
          dataIndex: 'Orders.count',
          format: undefined,
          key: 'Orders.count',
          meta: undefined,
          shortTitle: 'Count',
          title: 'Orders Count',
          type: 'number',
        },
      ]);
    });
  });

  test('order of values is preserved', () => {
    const resultSet = new ResultSet({
      query: {
        measures:  [
          'Branch.count'
        ],
        dimensions: [
          'Tenant.number'
        ],
        'order': [
          {
            'id': 'Tenant.number',
            'desc': true
          }
        ],
        filters: [],
        timezone: 'UTC'
      },
      data: [
        {
          'Tenant.number': '6',
          'Branch.count': '2'
        },
        {
          'Tenant.number': '1',
          'Branch.count': '2'
        },
      ],
      annotation: {
        measures: {
          'Branch.count': {
            type: 'number'
          }
        },
        dimensions: {
          'Tenant.number': {
            title: 'Tenant Number',
            shortTitle: 'Number',
            type: 'string'
          }
        },
        segments: {},
        timeDimensions: {}
      }
    });

    expect(resultSet.tableColumns({
      'x': [],
      'y': [
        'Tenant.number'
      ]
    })).toEqual(
        [
          {
            'key': '6',
            'type': 'string',
            'title': 'Tenant Number 6',
            'shortTitle': '6',
            'format': undefined,
            'meta': undefined,
            'children': [
              {
                'key': 'Branch.count',
                'type': 'number',
                'dataIndex': '6,Branch.count',
                'title': 'Branch.count',
                'shortTitle': 'Branch.count',
                'format': undefined,
                'meta': undefined,
              }
            ]
          },
          {
            'key': '1',
            'type': 'string',
            'title': 'Tenant Number 1',
            'shortTitle': '1',
            'format': undefined,
            'meta': undefined,
            'children': [
              {
                'key': 'Branch.count',
                'type': 'number',
                'dataIndex': '1,Branch.count',
                'title': 'Branch.count',
                'shortTitle': 'Branch.count',
                'format': undefined,
                "meta": undefined,
              }
            ]
          }
        ]
    );
  });
});
