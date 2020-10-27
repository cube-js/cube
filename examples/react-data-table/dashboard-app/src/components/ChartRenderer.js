import React from 'react';
import PropTypes from 'prop-types';
import { useCubeQuery } from '@cubejs-client/react';
import CircularProgress from '@material-ui/core/CircularProgress';
import { Line, Bar, Pie } from 'react-chartjs-2';
import Typography from '@material-ui/core/Typography';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';

const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];
const TypeToChartComponent = {
  line: ({ resultSet }) => {
    const data = {
      labels: resultSet.categories().map((c) => c.category),
      datasets: resultSet.series().map((s, index) => ({
        label: s.title,
        data: s.series.map((r) => r.value),
        borderColor: COLORS_SERIES[index],
        fill: false,
      })),
    };
    const options = {};
    return <Line data={data} options={options} />;
  },
  bar: ({ resultSet }) => {
    const data = {
      labels: resultSet.categories().map((c) => c.category),
      datasets: resultSet.series().map((s, index) => ({
        label: s.title,
        data: s.series.map((r) => r.value),
        backgroundColor: COLORS_SERIES[index],
        fill: false,
      })),
    };
    const options = {
      scales: {
        xAxes: [
          {
            stacked: true,
          },
        ],
      },
    };
    return <Bar data={data} options={options} />;
  },
  area: ({ resultSet }) => {
    const data = {
      labels: resultSet.categories().map((c) => c.category),
      datasets: resultSet.series().map((s, index) => ({
        label: s.title,
        data: s.series.map((r) => r.value),
        backgroundColor: COLORS_SERIES[index],
      })),
    };
    const options = {
      scales: {
        yAxes: [
          {
            stacked: true,
          },
        ],
      },
    };
    return <Line data={data} options={options} />;
  },
  pie: ({ resultSet }) => {
    const data = {
      labels: resultSet.categories().map((c) => c.category),
      datasets: resultSet.series().map((s) => ({
        label: s.title,
        data: s.series.map((r) => r.value),
        backgroundColor: COLORS_SERIES,
        hoverBackgroundColor: COLORS_SERIES,
      })),
    };
    const options = {};
    return <Pie data={data} options={options} />;
  },
  number: ({ resultSet }) => (
    <Typography
      variant="h4"
      style={{
        textAlign: 'center',
      }}
    >
      {resultSet.seriesNames().map((s) => resultSet.totalRow()[s.key])}
    </Typography>
  ),
  table: ({ resultSet }) => (
    <Table aria-label="simple table">
      <TableHead>
        <TableRow>
          {resultSet.tableColumns().map((c) => (
            <TableCell key={c.key}>{c.title}</TableCell>
          ))}
        </TableRow>
      </TableHead>
      <TableBody>
        {resultSet.tablePivot().map((row, index) => (
          <TableRow key={index}>
            {resultSet.tableColumns().map((c) => (
              <TableCell key={c.key}>{row[c.key]}</TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  ),
};
const TypeToMemoChartComponent = Object.keys(TypeToChartComponent)
  .map((key) => ({
    [key]: React.memo(TypeToChartComponent[key]),
  }))
  .reduce((a, b) => ({ ...a, ...b }));

const renderChart = (Component) => ({ resultSet, error, ...props }) =>
  (resultSet && <Component resultSet={resultSet} {...props} />) ||
  (error && error.toString()) || <CircularProgress />;

const ChartRenderer = ({ vizState }) => {
  const { query, chartType, ...options } = vizState;
  const component = TypeToMemoChartComponent[chartType];
  const renderProps = useCubeQuery(query);
  return component && renderChart(component)({ ...options, ...renderProps });
};

ChartRenderer.propTypes = {
  vizState: PropTypes.object,
  cubejsApi: PropTypes.object,
};
ChartRenderer.defaultProps = {
  vizState: {},
  cubejsApi: null,
};
export default ChartRenderer;
