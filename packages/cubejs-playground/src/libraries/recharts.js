import * as recharts from 'recharts';

const chartTypeToTemplate = {
  line: `
  <CartesianChart resultSet={resultSet} ChartComponent={LineChart}>
      {resultSet.seriesNames().map((series, i) => (
        <Line
          key={series.key}
          stackId="a"
          dataKey={series.key}
          name={series.title}
          stroke={colors[i]}
        />
      ))}
  </CartesianChart>`,
  bar: `
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
  </CartesianChart>`,
  area: `
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
  </CartesianChart>`,
  pie: `
  <ResponsiveContainer width="100%" height={350}>
    <PieChart>
      <Pie
        isAnimationActive={false}
        data={resultSet.chartPivot()}
        nameKey="x"
        dataKey={resultSet.seriesNames()[0].key}
        fill="#8884d8"
      >
      {
        resultSet.chartPivot().map((e, index) =>
          <Cell key={index} fill={colors[index % colors.length]}/>
        )
      }
      </Pie>
      <Legend />
      <Tooltip />
    </PieChart>
  </ResponsiveContainer>`,
};

export const sourceCodeTemplate = ({ chartType, renderFnName }) =>
  `import {
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

const CartesianChart = ({ resultSet, children, ChartComponent }) => (
  <ResponsiveContainer width="100%" height={350}>
    <ChartComponent data={resultSet.chartPivot()}>
      <XAxis dataKey="x" />
      <YAxis />
      <CartesianGrid />
      { children }
      <Legend />
      <Tooltip />
    </ChartComponent>
  </ResponsiveContainer>
)

const colors = ['#FF6492', '#141446', '#7A77FF'];

const ${renderFnName} = ({ resultSet }) => (${chartTypeToTemplate[chartType]}
);`;

export const imports = {
  recharts,
};
