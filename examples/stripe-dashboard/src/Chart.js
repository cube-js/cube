import React from 'react';
import { QueryRenderer } from '@cubejs-client/react';
import './Chart.css'

const Chart = ({ cubejsApi, title, query, render }) => (
  <div className="Chart">
    <div className="ChartTitle">
      { title }
    </div>
    <div className="ChartBody">
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
