import React from 'react';
import cubejs from '@cubejs-client/core';
import * as cubejsReact from '@cubejs-client/react';
import * as antd from 'antd';
import * as bizcharts from 'bizcharts';
import moment from 'moment';

const chartTypeToTemplate = {
  line: `
<Chart scale={{ category: { tickCount: 8 } }} height={400} data={resultSet.chartPivot()} forceFit>
  <Axis name="category" label={{ formatter: val => moment(val).format("MMM 'YY") }} />
  <Axis name="Stories.count" />
  <Tooltip crosshairs={{type : 'y'}} />
  <Geom type="line" position="category*Stories.count" size={2} />
</Chart>`,
  lineMulti: `
<Chart scale={{ category: { tickCount: 8 } }} height={400} data={resultSet.chartPivot()} forceFit>
  <Axis name="category" label={{ formatter: val => moment(val).format("MMM 'YY") }} />
  <Axis name="Stories.count" />
  <Tooltip crosshairs={{type : 'y'}} />
  <Geom type="line" position="category*Stories.count" />
  <Geom type="line" position="category*Stories.totalScore" color="#9AD681"/>
</Chart>`,
  bar: `
<Chart height={400} data={resultSet.chartPivot()} forceFit>
  <Axis name="category" label={{ formatter: val => moment(val).format("MMM 'YY") }} />
  <Axis name="Stories.count" />
  <Tooltip />
  <Geom type="interval" position="category*Stories.count" />
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
  <Axis name="Stories.count" />
  <Legend position='right' />
  <Tooltip />
  <Geom
    type="intervalStack"
    position="Stories.count"
    color='category'>
  </Geom>
</Chart>`
};


export const sourceCodeTemplate = (chartType, query) => (
  `import React from 'react';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin } from 'antd';
import { Chart, Axis, Tooltip, Geom, Coord, Legend } from 'bizcharts';
import moment from 'moment';

const HACKER_NEWS_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw';

const query =
${JSON.stringify(query, null, 2)}

const renderChart = (resultSet) => (${chartTypeToTemplate[chartType]}
);

const Example = <QueryRenderer
  query={query}
  cubejsApi={cubejs(HACKER_NEWS_API_KEY)}
  render={ ({ resultSet }) => (
    resultSet && renderChart(resultSet) || (<Spin />)
  )}
/>;

export default Example;
`
);

export const imports = {
  '@cubejs-client/core': cubejs,
  '@cubejs-client/react': cubejsReact,
  antd,
  react: React,
  bizcharts,
  moment
};