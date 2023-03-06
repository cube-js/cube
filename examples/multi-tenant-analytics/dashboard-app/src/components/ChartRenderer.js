import React from 'react';
import PropTypes from 'prop-types';
import { useCubeQuery } from '@cubejs-client/react';
import { Spin, Row, Col, Statistic, Table } from 'antd';
import { Chart, Axis, Tooltip, Geom, PieChart } from 'bizcharts';

const stackedChartData = resultSet => {
  const data = resultSet.pivot().map(({
    xValues,
    yValuesArray
  }) => yValuesArray.map(([yValues, m]) => ({
    x: resultSet.axisValuesString(xValues, ', '),
    color: resultSet.axisValuesString(yValues, ', '),
    measure: m && Number.parseFloat(m)
  }))).reduce((a, b) => a.concat(b), []);
  return data;
};

const TypeToChartComponent = {
  line: ({
    resultSet
  }) => {
    return <Chart scale={{
      x: {
        tickCount: 8
      }
    }} autoFit height={400} data={stackedChartData(resultSet)} forceFit>
        <Axis name="x" />
        <Axis name="measure" />
        <Tooltip crosshairs={{
        type: 'y'
      }} />
        <Geom type="line" position="x*measure" size={2} color="color" />
      </Chart>;
  },
  bar: ({
    resultSet
  }) => {
    return <Chart scale={{
      x: {
        tickCount: 8
      }
    }} autoFit height={400} data={stackedChartData(resultSet)} forceFit>
        <Axis name="x" />
        <Axis name="measure" />
        <Tooltip />
        <Geom type="interval" position="x*measure" color="color" />
      </Chart>;
  },
  area: ({
    resultSet
  }) => {
    return <Chart scale={{
      x: {
        tickCount: 8
      }
    }} autoFit height={400} data={stackedChartData(resultSet)} forceFit>
        <Axis name="x" />
        <Axis name="measure" />
        <Tooltip crosshairs={{
        type: 'y'
      }} />
        <Geom type="area" position="x*measure" size={2} color="color" />
      </Chart>;
  },
  pie: ({
    resultSet
  }) => {
    return <PieChart data={resultSet.chartPivot()} radius={0.8} angleField={resultSet.series()[0].key} colorField="x" label={{
      visible: true,
      offset: 20
    }} legend={{
      position: 'bottom'
    }} />;
  },
  number: ({
    resultSet
  }) => {
    return <Row type="flex" justify="center" align="middle" style={{
      height: '100%'
    }}>
        <Col>
          {resultSet.seriesNames().map(s => <Statistic value={resultSet.totalRow()[s.key]} />)}
        </Col>
      </Row>;
  },
  table: ({
    resultSet,
    pivotConfig
  }) => {
    return <Table pagination={false} columns={resultSet.tableColumns(pivotConfig)} dataSource={resultSet.tablePivot(pivotConfig)} />;
  }
};
const TypeToMemoChartComponent = Object.keys(TypeToChartComponent).map(key => ({
  [key]: React.memo(TypeToChartComponent[key])
})).reduce((a, b) => ({ ...a,
  ...b
}));

const renderChart = Component => ({
  resultSet,
  error,
  pivotConfig
}) => resultSet && (<Component resultSet={resultSet} pivotConfig={pivotConfig} /> || (error && error.toString()) || <Spin />);

const ChartRenderer = ({
  vizState
}) => {
  const {
    query,
    chartType,
    pivotConfig
  } = vizState;
  const component = TypeToMemoChartComponent[chartType];
  const renderProps = useCubeQuery(query);
  return component && renderChart(component)({ ...renderProps,
    pivotConfig
  });
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