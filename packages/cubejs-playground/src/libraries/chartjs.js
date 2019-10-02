import * as reactChartjs from 'react-chartjs-2';

const chartTypeToTemplate = {
  line: `
  const data = {
    labels: resultSet.categories().map(c => c.category),
    datasets: resultSet.series().map((s, index) => (
      {
        label: s.title,
        data: s.series.map(r => r.value),
        borderColor: COLORS_SERIES[index],
        fill: false
      }
    )),
  };
  const options = {};
  return <Line data={data} options={options} />;`,
  bar: `
  const data = {
    labels: resultSet.categories().map(c => c.category),
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
    scales: { xAxes: [{ stacked: true }] }
  };
  return <Bar data={data} options={options} />;`,
  area: `
  const data = {
    labels: resultSet.categories().map(c => c.category),
    datasets: resultSet.series().map((s, index) => (
      {
        label: s.title,
        data: s.series.map(r => r.value),
        backgroundColor: COLORS_SERIES[index]
      }
    )),
  };
  const options = {
    scales: { yAxes: [{ stacked: true }] }
  };
  return <Line data={data} options={options} />;`,
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


export const sourceCodeTemplate = ({ chartType, renderFnName }) => (
  `import { Line, Bar, Pie } from 'react-chartjs-2';

const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];

const ${renderFnName} = ({ resultSet }) => {${chartTypeToTemplate[chartType]}
};`
);

export const imports = {
  'react-chartjs-2': reactChartjs
};
