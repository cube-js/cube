import { Meta } from '@cubejs-client/core';

const metaResponse: any = {
  cubes: [
    {
      name: 'Orders',
      title: 'Orders',
      type: 'cube',
      public: true,
      measures: [
        {
          name: 'Orders.count',
          title: 'Orders Count',
          shortTitle: 'Count',
          type: 'number',
          aggType: 'count',
        },
        {
          name: 'Orders.totalAmount',
          title: 'Orders Total Amount',
          shortTitle: 'Total Amount',
          type: 'number',
          aggType: 'sum',
        },
      ],
      dimensions: [
        {
          name: 'Orders.status',
          title: 'Orders Status',
          shortTitle: 'Status',
          type: 'string',
        },
        {
          name: 'Orders.createdAt',
          title: 'Orders Created at',
          shortTitle: 'Created at',
          type: 'time',
        },
      ],
      segments: [
        {
          name: 'Orders.completed',
          title: 'Orders Completed',
          shortTitle: 'Completed',
        },
      ],
    },
  ],
};

export function createMeta(): Meta {
  return new Meta(metaResponse);
}
