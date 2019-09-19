import React from 'react';
import PropTypes from 'prop-types';
import { QueryRenderer } from '@cubejs-client/react';

const TypeToChartComponent = {};

const renderChart = (Component) => ({ resultSet, error }) => (
  (resultSet && <Component resultSet={resultSet} />)
  || (error && error.toString())
  || (<Spin />)
);

const ChartRenderer = ({ dashboardItem, cubejsApi }) => (
  dashboardItem && (
    <QueryRenderer
      query={dashboardItem.query}
      cubejsApi={cubejsApi}
      render={
        TypeToChartComponent[dashboardItem.chartType] && renderChart(TypeToChartComponent[dashboardItem.chartType])
      }
    />
  )
);

ChartRenderer.propTypes = {
  dashboardItem: PropTypes.object,
  cubejsApi: PropTypes.object.isRequired
};

ChartRenderer.defaultProps = {
  dashboardItem: null
};


export default ChartRenderer;
