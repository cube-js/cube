import * as bizcharts from 'bizcharts';
import moment from 'moment';

const chartTypeToTemplate = {
  line: `
  <Chart scale={{ category: { tickCount: 8 } }} height={400} data={resultSet.chartPivot()} forceFit>
    <Axis name="category" />
    {resultSet.seriesNames().map(s => (<Axis name={s.key} />))}
    <Tooltip crosshairs={{type : 'y'}} />
    {resultSet.seriesNames().map(s => (<Geom type="line" position={\`category*\${s.key}\`} size={2} />))}
  </Chart>`,
  bar: `
  <Chart scale={{ category: { tickCount: 8 } }} height={400} data={resultSet.chartPivot()} forceFit>
    <Axis name="category" />
    {resultSet.seriesNames().map(s => (<Axis name={s.key} />))}
    <Tooltip />
    {resultSet.seriesNames().map(s => (<Geom type="interval" position={\`category*\${s.key}\`} />))}
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
  </Chart>`
};


export const sourceCodeTemplate = ({ chartType, renderFnName }) => (
  `import { Chart, Axis, Tooltip, Geom, Coord, Legend } from 'bizcharts';
import moment from 'moment';

const ${renderFnName} = ({ resultSet }) => (${chartTypeToTemplate[chartType]}
);`
);

export const imports = {
  bizcharts,
  moment
};
