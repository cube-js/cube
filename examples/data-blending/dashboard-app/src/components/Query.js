import React, { useState, useEffect } from 'react';
import { useCubeQuery } from '@cubejs-client/react';

import Line from './Line';

export default () => {
  const [data, setData] = useState([]);
  const { resultSet } = useCubeQuery([
    {
      measures: ['Orders.count'],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          dateRange: ['2022-01-01', '2022-12-31'],
          granularity: 'month',
        },
      ],
    },
    {
      measures: ['OrdersOffline.count'],
      timeDimensions: [
        {
          dimension: 'OrdersOffline.createdAt',
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
          name: 'Online',
          data: [],
        },
        {
          name: 'Offline',
          data: [],
        },
        {
          name: 'All sales',
          data: [],
        },
      ];
      resultSet.tablePivot().map((item) => {
        temp[0].data.push(parseInt(item['Orders.count']));
        temp[1].data.push(parseInt(item['OrdersOffline.count']));
        temp[2].data.push(
          parseInt(item['Orders.count']) + parseInt(item['OrdersOffline.count'])
        );
      });
      setData(temp);
    }
  }, [resultSet]);

  return <Line data={data} title={'a Data Blending query'} />;
};
