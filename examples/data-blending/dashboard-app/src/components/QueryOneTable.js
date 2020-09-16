import React, { useState, useEffect } from 'react';
import { useCubeQuery } from '@cubejs-client/react';

import Line from './Line';

export default () => {
  const [data, setData] = useState([]);
  const { resultSet } = useCubeQuery([
    {
      measures: ['Orders.count'],
      filters: [
        {
          member: 'Orders.status',
          operator: 'equals',
          values: ['processing'],
        },
      ],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          dateRange: ['2022-01-01', '2022-12-31'],
          granularity: 'month',
        },
      ],
    },
    {
      measures: ['Orders.count'],
      filters: [
        {
          member: 'Orders.status',
          operator: 'equals',
          values: ['shipped'],
        },
      ],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          dateRange: ['2022-01-01', '2022-12-31'],
          granularity: 'month',
        },
      ],
    },
    {
      measures: ['Orders.count'],
      filters: [
        {
          member: 'Orders.status',
          operator: 'equals',
          values: ['completed'],
        },
      ],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          dateRange: ['2022-01-01', '2022-12-31'],
          granularity: 'month',
        },
      ],
    },
  ]);
  useEffect(() => {
    if (resultSet) {
      const temp = [
        {
          name: 'Processed',
          data: [],
        },
        {
          name: 'Shipped',
          data: [],
        },
        {
          name: 'Completed',
          data: [],
        },
      ];
      resultSet.loadResponse.results.map((item, i) => {
        temp[i].data = item.data.map((el) => parseInt(el['Orders.count']));
      });
      setData(temp);
    }
  }, [resultSet]);

  return <Line data={data} title={'a Data Blending query'} />;
};
