import React, { useState, useEffect } from 'react';
import { useCubeQuery } from '@cubejs-client/react';
import LineChart from './LineChart';
export default () => {
  const [usersByDate, setUsersByDate] = useState([0, 0]);
  const { resultSet: users } = useCubeQuery(
    {
      measures: ['Users.count'],
      timeDimensions: [
        {
          dimension: 'Users.updated',
          granularity: 'day',
        },
      ],
    },
    { subscribe: true }
  );

  useEffect(() => {
    if (users) {
      let temp = [];
      users.tablePivot().map((item) => {
        temp.push({
          date: new Date(item['Users.updated.day']),
          value: item['Users.count'],
        });
      });
      setUsersByDate(temp);
    }
  }, [users]);

  return (
    <React.Fragment>
      <LineChart data={usersByDate} />
    </React.Fragment>
  );
};
