import React from 'react';
import { QueryRenderer } from '@cubejs-client/react';

const Chart = ({ cubejsApi, title, query, render }) => (
  <div className="chart">
    <div className="chart-title">
      { title }
    </div>
    <div className="chart-body">
      <QueryRenderer
        query={query}
        cubejsApi={cubejsApi}
        render={({ resultSet }) => {
          if (!resultSet) {
            return <div className="loader"></div>;
          }

          return render(resultSet);
        }}
      />
    </div>
  </div>
);

export default Chart;
