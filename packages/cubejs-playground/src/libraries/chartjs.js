import React from 'react';
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
  categoryFilter: `
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
  `import { Line, Bar, Pie } from 'react-chartjs-2';
import moment from 'moment';

const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];

const renderChart = (resultSet) => {${chartTypeToTemplate[chartType]}
};`
);

export const imports = {
  'react-chartjs-2': reactChartjs,
  moment
};