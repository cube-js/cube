import React from 'react';
import PropTypes from 'prop-types';
import { useCubeQuery } from '@cubejs-client/react';
import { Spin } from 'antd';

const TypeToChartComponent = {};

const TypeToMemoChartComponent = Object.keys(TypeToChartComponent)
  .map(key => ({ [key]: React.memo(TypeToChartComponent[key]) }))
  .reduce((a, b) => ({ ...a, ...b }));

const renderChart = Component => ({ resultSet, error }) => (resultSet && <Component resultSet={resultSet} />)
  || (error && error.toString()) || <Spin />;

const ChartRenderer = ({ vizState }) => {
  const { query, chartType } = vizState;
  const component = TypeToMemoChartComponent[chartType];
  const renderProps = useCubeQuery(query);

  return component && renderChart(component)(renderProps);
};

ChartRenderer.propTypes = {
  vizState: PropTypes.object,
  cubejsApi: PropTypes.object
};

ChartRenderer.defaultProps = {
  vizState: {},
  cubejsApi: null
};

export default ChartRenderer;
