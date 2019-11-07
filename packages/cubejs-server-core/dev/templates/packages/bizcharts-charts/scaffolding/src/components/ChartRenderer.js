import { Chart, Axis, Tooltip, Geom, Coord, Legend } from 'bizcharts';

const stackedChartData = (resultSet) => {
  const data = resultSet.pivot().map(
    ({ xValues, yValuesArray }) =>
      yValuesArray.map(([yValues, m]) => ({
        x: resultSet.axisValuesString(xValues, ', '),
        color: resultSet.axisValuesString(yValues, ', '),
        measure: m && Number.parseFloat(m)
      }))
  ).reduce((a, b) => a.concat(b), []);

  return data;
};

const TypeToChartComponent = {
  line: ({ resultSet }) => (
    <Chart scale={{ x: { tickCount: 8 } }} height={400} data={stackedChartData(resultSet)} forceFit>
      <Axis name="x"/>
      <Axis name="measure"/>
      <Tooltip crosshairs={{ type: 'y' }}/>
      <Geom type="line" position={`x*measure`} size={2} color="color"/>
    </Chart>
  ),
  bar: ({ resultSet }) => (
    <Chart scale={{ x: { tickCount: 8 } }} height={400} data={stackedChartData(resultSet)} forceFit>
      <Axis name="x"/>
      <Axis name="measure"/>
      <Tooltip/>
      <Geom type="intervalStack" position={`x*measure`} color="color"/>
    </Chart>
  ),
  area: ({ resultSet }) => (
    <Chart scale={{ x: { tickCount: 8 } }} height={400} data={stackedChartData(resultSet)} forceFit>
      <Axis name="x"/>
      <Axis name="measure"/>
      <Tooltip crosshairs={{ type: 'y' }}/>
      <Geom type="areaStack" position={`x*measure`} size={2} color="color"/>
    </Chart>
  ),
  pie: ({ resultSet }) => (
    <Chart height={400} data={resultSet.chartPivot()} forceFit>
      <Coord type='theta' radius={0.75}/>
      {resultSet.seriesNames().map(s => (<Axis name={s.key}/>))}
      <Legend position='right'/>
      <Tooltip/>
      {resultSet.seriesNames().map(s => (<Geom type="intervalStack" position={s.key} color="category"/>))}
    </Chart>
  )
};
