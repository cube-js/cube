import React from 'react';
import PropTypes from 'prop-types';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin } from 'antd';

const TypeToChartComponent = {};

const renderChart = (Component) => ({ resultSet, error }) => (
  (resultSet && <Component resultSet={resultSet} />)
  || (error && error.toString())
  || (<Spin />)
);

const ChartRenderer = ({ vizState, cubejsApi }) => vizState && (
  <QueryRenderer
    query={vizState.query}
    cubejsApi={cubejsApi}
    render={
      TypeToChartComponent[vizState.chartType]
      && renderChart(TypeToChartComponent[vizState.chartType])
    }
  />
);

ChartRenderer.propTypes = {
  vizState: PropTypes.object,
  cubejsApi: PropTypes.object.isRequired
};

ChartRenderer.defaultProps = {
  vizState: null
};


export default ChartRenderer;
