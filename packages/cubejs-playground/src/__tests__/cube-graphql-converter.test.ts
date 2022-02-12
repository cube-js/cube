import { CubeGraphQLConverter } from '../components/GraphQL/CubeGraphQLConverter';

const types = {
  'Orders.count': 'number',
  'Orders.status': 'string',
  'Orders.createdAt': 'time',
  'Orders.amount': 'number',
  'Users.country': 'string',
  'Users.name': 'string',
} as const;

const queries = [
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status', 'Users.country', 'Orders.createdAt'],
  },
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status', 'Users.country', 'Orders.createdAt'],
    order: {
      'Orders.count': 'desc',
      'Orders.status': 'asc',
      'Users.country': 'desc',
    },
  },
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status', 'Users.country', 'Orders.createdAt'],
    order: [
      ['Orders.count', 'desc'],
      ['Orders.status', 'asc'],
      ['Users.country', 'desc'],
    ],
  },
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        granularity: 'day',
        dateRange: ['2020-01-01', '2021-01-01'],
      },
    ],
  },
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status'],
    timezone: 'America/Los_Angeles',
    limit: 100,
  },
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status'],
    timezone: 'America/Los_Angeles',
    limit: 100,
  },
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status'],
    filters: [
      {
        member: 'Orders.status',
        operator: 'equals',
        values: ['canceled', 'active'],
      },
      {
        or: [
          {
            member: 'Users.country',
            operator: 'notSet',
          },
          {
            member: 'Users.country',
            operator: 'equals',
            values: ['US']
          },
        ]
      }
    ],
  },
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status', 'Users.country'],
    filters: [
      {
        member: 'Users.country',
        operator: 'equals',
        values: ['US'],
      },
      {
        member: 'Orders.status',
        operator: 'equals',
        values: ['canceled', 'active'],
      },
    ],
  },
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status', 'Users.country'],
    filters: [
      {
        member: 'Users.country',
        operator: 'equals',
        values: ['US'],
      },
      {
        member: 'Users.country',
        operator: 'equals',
        values: ['Canada'],
      },
    ],
  },
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status', 'Users.country'],
    filters: [
      {
        member: 'Orders.amount',
        operator: 'equals',
        values: ['5', '10'],
      },
      {
        or: [
          {
            member: 'Users.country',
            operator: 'equals',
            values: ['US'],
          },
          {
            and: [
              {
                member: 'Orders.status',
                operator: 'equals',
                values: ['canceled', 'active'],
              },
              {
                member: 'Users.country',
                operator: 'equals',
                values: ['US'],
              },
            ],
          },
        ],
      },
    ],
  },
  {
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        granularity: 'quarter',
      },
    ],
    order: [
      ['Orders.count', 'asc'],
      ['Users.country', 'asc'],
    ],
    dimensions: ['Users.country', 'Orders.createdAt'],
  },
];

test('Cube GraphQL converter', () => {
  queries.forEach((query) => {
    const converter = new CubeGraphQLConverter(query, types);
    expect(converter.convert()).toMatchSnapshot(JSON.stringify(query));
  });
});
