import { Query } from '@cubejs-client/core';
import { CubeSQLConverter } from '../components/CubeSQL/CubeSQLConverter';

const queries: Query[] = [
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status', 'Users.state', 'Orders.createdAt'],
  },
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status', 'Users.state', 'Orders.createdAt'],
    order: {
      'Orders.count': 'desc',
      'Orders.status': 'asc',
      'Users.state': 'desc',
    },
  },
  {
    measures: ['Orders.count'],
    dimensions: ['Orders.status', 'Users.state', 'Orders.createdAt'],
    order: [
      ['Orders.count', 'desc'],
      ['Orders.status', 'asc'],
      ['Users.state', 'desc'],
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
    dimensions: ['Orders.status', 'Users.state'],
    filters: [
      {
        member: 'Users.state',
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
    dimensions: ['Orders.status', 'Users.state'],
    filters: [
      {
        member: 'Users.state',
        operator: 'equals',
        values: ['US'],
      },
      {
        member: 'Users.state',
        operator: 'equals',
        values: ['Canada'],
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
      ['Users.state', 'asc'],
    ],
    dimensions: ['Users.state', 'Orders.createdAt'],
  },
];

const q: Query =   {
  measures: ['Orders.count'],
  timeDimensions: [
    {
      dimension: 'Orders.createdAt',
      granularity: 'quarter',
    },
  ],
  filters: [
    {
      member: 'Orders.createdAt',
      operator: 'inDateRange',
      values: ['2020-01-01', '2020-01-01']
    },
    {
      member: 'Orders.status',
      // operator: 'notEquals',
      operator: 'equals',
      // values: ['completed']
      values: ['completed', 'shipped']
    }
  ],
  order: [
    ['Orders.count', 'asc'],
    ['Users.state', 'asc'],
  ],
  dimensions: ['Users.state', 'Orders.createdAt'],
};

test('Cube CubeSQL converter', () => {
  queries.forEach((query) => {
    const converter = new CubeSQLConverter(query);
    
    expect(converter.convert()).toMatchSnapshot(JSON.stringify(query));
  });
});
