import React from 'react';
import cubejs from '@cubejs-client/core';
import * as cubejsReact from '@cubejs-client/react';
import * as antd from 'antd';
import * as bizcharts from 'bizcharts';
import * as reactChartjs from 'react-chartjs-2'
import moment from 'moment';

const chartTypeToTemplate = {
  line: `
  const data = {
    labels: resultSet.categories().map(c => new Date(c.category)),
    datasets: resultSet.series().map((s, index) => (
      {
        label: s.title,
        data: s.series.map(r => r.value),
        borderColor: COLORS_SERIES[index],
        fill: false
      }
    )),
  };
  const options = {
    scales: { xAxes: [{ type: 'time', time: { unit: 'month' }}] }
  };
  return <Line data={data} options={options} />;`,
  lineMulti: `
  const data = {
    labels: resultSet.categories().map(c => new Date(c.category)),
    datasets: resultSet.series().map((s, index) => (
      {
        label: s.title,
        data: s.series.map(r => r.value),
        borderColor: COLORS_SERIES[index],
        fill: false
      }
    )),
  };
  const options = {
    scales: { xAxes: [{ type: 'time', time: { unit: 'month' }}] }
  };
  return <Line data={data} options={options} />;`,
  bar: `
  const data = {
    labels: resultSet.categories().map(c => new Date(c.category)),
    datasets: resultSet.series().map((s, index) => (
      {
        label: s.title,
        data: s.series.map(r => r.value),
        backgroundColor: COLORS_SERIES[index],
        fill: false
      }
    )),
  };
  const options = {
    scales: { xAxes: [{ type: 'time', time: { unit: 'month' }}] }
  };
  return <Bar data={data} options={options} />;`,
  barStacked: `
  const data = {
    labels: resultSet.categories().map(c => new Date(c.category)),
    datasets: resultSet.series().map((s, index) => (
      {
        label: s.title,
        data: s.series.map(r => r.value),
        backgroundColor: COLORS_SERIES[index],
        fill: false
      }
    )),
  };
  const options = {
    scales: { xAxes: [{ type: 'time', time: { unit: 'month' }}] }
  };
  return <Bar data={data} options={options} />;`,
  pie: `
  const data = {
    labels: resultSet.categories().map(c => c.category),
    datasets: resultSet.series().map(s => (
      {
        label: s.title,
        data: s.series.map(r => r.value),
        backgroundColor: COLORS_SERIES,
        hoverBackgroundColor: COLORS_SERIES,
      }
    ))
  };
  const options = {};
  return <Pie data={data} options={options} />;`
};


export const sourceCodeTemplate = (chartType, query) => (
  `import React from 'react';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin } from 'antd';
import { Line, Bar, Pie } from 'react-chartjs-2';
import moment from 'moment';

const HACKER_NEWS_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw';

const query =
${JSON.stringify(query, null, 2)}

const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];

const renderChart = (resultSet) => {${chartTypeToTemplate[chartType]}
};

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
  'react-chartjs-2': reactChartjs,
  moment
};