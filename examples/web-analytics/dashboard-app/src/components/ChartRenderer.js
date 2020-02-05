import React from "react";
import PropTypes from "prop-types";
import { useCubeQuery } from "@cubejs-client/react";
import Skeleton from "@material-ui/lab/Skeleton";
import {
  CartesianGrid,
  PieChart,
  Pie,
  Cell,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
  BarChart,
  Bar,
  LineChart,
  Line
} from "recharts";
import Typography from "@material-ui/core/Typography";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";

import moment from "moment";
import numeral from "numeral";

const numberFormatter = item => numeral(item).format("0,0");
const dateFormatter = item => moment(item).format("MMM DD");
const xAxisFormatter = (item) => {
  if (moment(item).isValid()) {
    return dateFormatter(item)
  } else {
    return item;
  }
}

const CartesianChart = ({ resultSet, legend, children, ChartComponent }) => (
  <ResponsiveContainer width="100%" height={250}>
    <ChartComponent
      margin={{
            top: 16,
            right: 16,
            bottom: 0,
            left: 0,
          }}
      data={resultSet.chartPivot()}>
      <XAxis axisLine={false} tickLine={false} tickFormatter={xAxisFormatter} dataKey="x" minTickGap={20} />
      <YAxis axisLine={false} tickLine={false} tickFormatter={numberFormatter} />
      <CartesianGrid vertical={false} />
      {children}
      { legend && <Legend /> }
      <Tooltip labelFormatter={dateFormatter} formatter={numberFormatter} />
    </ChartComponent>
  </ResponsiveContainer>
);

CartesianChart.defaultProps = {
  legend: true
}

const colors = ["#4791db", "#e33371", "#e57373"];

const stackedChartData = resultSet => {
  const data = resultSet
    .pivot()
    .map(({ xValues, yValuesArray }) =>
      yValuesArray.map(([yValues, m]) => ({
        x: resultSet.axisValuesString(xValues, ", "),
        color: resultSet.axisValuesString(yValues, ", "),
        measure: m && Number.parseFloat(m)
      }))
    )
    .reduce((a, b) => a.concat(b), []);
  return data;
};

const TypeToChartComponent = {
  line: ({ resultSet, ...props }) => (
    <CartesianChart resultSet={resultSet} ChartComponent={LineChart} {...props}>
      {resultSet.seriesNames().map((series, i) => (
        <Line
          key={series.key}
          stackId="a"
          dataKey={series.key}
          name={series.title}
          stroke={colors[i]}
        />
      ))}
    </CartesianChart>
  ),
  bar: ({ resultSet }) => (
    <CartesianChart resultSet={resultSet} ChartComponent={BarChart}>
      {resultSet.seriesNames().map((series, i) => (
        <Bar
          key={series.key}
          stackId="a"
          dataKey={series.key}
          name={series.title}
          fill={colors[i]}
        />
      ))}
    </CartesianChart>
  ),
  area: ({ resultSet }) => (
    <CartesianChart resultSet={resultSet} ChartComponent={AreaChart}>
      {resultSet.seriesNames().map((series, i) => (
        <Area
          key={series.key}
          stackId="a"
          dataKey={series.key}
          name={series.title}
          stroke={colors[i]}
          fill={colors[i]}
        />
      ))}
    </CartesianChart>
  ),
  pie: ({ resultSet }) => (
    <ResponsiveContainer width="100%" height={350}>
      <PieChart>
        <Pie
          isAnimationActive={false}
          data={resultSet.chartPivot()}
          nameKey="x"
          dataKey={resultSet.seriesNames()[0].key}
          fill="#8884d8"
        >
          {resultSet.chartPivot().map((e, index) => (
            <Cell key={index} fill={colors[index % colors.length]} />
          ))}
        </Pie>
        <Legend />
        <Tooltip />
      </PieChart>
    </ResponsiveContainer>
  ),
  number: ({ resultSet }) => (
    <Typography component="p" variant="h4">
      {resultSet.seriesNames().map(s => resultSet.totalRow()[s.key])}
    </Typography>
  ),
  table: ({ resultSet }) => (
    <Table aria-label="simple table">
      <TableHead>
        <TableRow>
          {resultSet.tableColumns().map(c => (
            <TableCell key={c.key}>{c.title}</TableCell>
          ))}
        </TableRow>
      </TableHead>
      <TableBody>
        {resultSet.tablePivot().map((row, index) => (
          <TableRow key={index}>
            {resultSet.tableColumns().map(c => (
              <TableCell key={c.key}>{row[c.key]}</TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
};
const TypeToMemoChartComponent = Object.keys(TypeToChartComponent)
  .map(key => ({
    [key]: React.memo(TypeToChartComponent[key])
  }))
  .reduce((a, b) => ({ ...a, ...b }));

const renderChart = Component => ({ resultSet, error, ...props }) =>
  (resultSet && <Component resultSet={resultSet} {...props} />) ||
  (error && error.toString()) || <Skeleton height={250} />;

const ChartRenderer = ({ vizState }) => {
  const { query, chartType, ...options } = vizState;
  const component = TypeToMemoChartComponent[chartType];
  const renderProps = useCubeQuery(query);
  return component && renderChart(component)({ ...options, ...renderProps });
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
