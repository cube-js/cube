import * as bizcharts from 'bizcharts';
import moment from 'moment';

const chartTypeToTemplate = {
  line: `
  <Chart scale={{ x: { tickCount: 8 } }} height={400} data={stackedChartData(resultSet)} forceFit>
    <Axis name="x" />
    <Axis name="measure" />
    <Tooltip crosshairs={{type : 'y'}} />
    <Geom type="line" position={\`x*measure\`} size={2} color="color" />
  </Chart>`,
  bar: `
  <Chart scale={{ x: { tickCount: 8 } }} height={400} data={stackedChartData(resultSet)} forceFit>
    <Axis name="x" />
    <Axis name="measure" />
    <Tooltip />
    <Geom type="intervalStack" position={\`x*measure\`} color="color" />
  </Chart>`,
  area: `
  <Chart scale={{ x: { tickCount: 8 } }} height={400} data={stackedChartData(resultSet)} forceFit>
    <Axis name="x" />
    <Axis name="measure" />
    <Tooltip crosshairs={{type : 'y'}} />
    <Geom type="areaStack" position={\`x*measure\`} size={2} color="color" />
  </Chart>`,
  barStacked: `
<Chart height={400} data={resultSet.rawData()} forceFit>
  <Legend />
  <Axis name="Stories.time" label={{ formatter: val => moment(val).format("MMM 'YY") }} />
  <Axis name="Stories.count" />
  <Tooltip />
  <Geom type='intervalStack' position="Stories.time*Stories.count" color="Stories.category" />
</Chart>`,
  pie: `
  <Chart height={400} data={resultSet.chartPivot()} forceFit>
    <Coord type='theta' radius={0.75} />
    {resultSet.seriesNames().map(s => (<Axis name={s.key} />))}
    <Legend position='right' />
    <Tooltip />
    {resultSet.seriesNames().map(s => (<Geom type="intervalStack" position={s.key} color="category" />))}
  </Chart>`,
  number: `
  <Row type="flex" justify="center" align="middle" style={{ height: '100%' }}>
    <Col>
      {resultSet
        .seriesNames()
        .map(s => (
          <Statistic value={resultSet.totalRow()[s.key]} />
        ))}
    </Col>
  </Row>
  `,
  table: `
  <Table 
    pagination={false}
    columns={resultSet.tableColumns().map(c => ({ ...c, dataIndex: c.key }))} 
    dataSource={resultSet.tablePivot()} 
  />
  `
};


export const sourceCodeTemplate = ({ chartType, renderFnName }) => (
  `import { Chart, Axis, Tooltip, Geom, Coord, Legend } from 'bizcharts';
import { Row, Col, Statistic, Table } from 'antd';
import moment from 'moment';

const stackedChartData = (resultSet) => {
  const data = resultSet.pivot().map(
    ({ xValues, yValuesArray }) =>
      yValuesArray.map(([yValues, m]) => ({
        x: resultSet.axisValuesString(xValues, ', '),
        color: resultSet.axisValuesString(yValues, ', '),
        measure: m && Number.parseFloat(m)
      }))
  ).reduce((a, b) => a.concat(b));

  return data;
}

const ${renderFnName} = ({ resultSet }) => (${chartTypeToTemplate[chartType]}
);`
);

export const imports = {
  bizcharts,
  moment
};
