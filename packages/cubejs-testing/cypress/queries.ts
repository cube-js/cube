export const ordersCountQuery = {
  measures: ['Orders.count'],
};

export const eventsCountQuery = {
  measures: ['Events.count'],
};

export const countWithTimedimenionQuery = {
  measures: ['Events.count'],
  timeDimensions: [
    {
      dimension: 'Events.createdAt',
      granularity: 'hour',
    },
  ],
  order: { 'Events.createdAt': 'asc' },
};

export const tableQuery = {
  measures: ['Events.count'],
  dimensions: ['Events.type'],
};
