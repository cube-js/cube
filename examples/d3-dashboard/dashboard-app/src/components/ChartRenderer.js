import React from "react";
import PropTypes from "prop-types";
import { useCubeQuery } from "@cubejs-client/react";
import CircularProgress from "@material-ui/core/CircularProgress";
import * as d3 from "d3";
import Typography from "@material-ui/core/Typography";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
const COLORS_SERIES = ["#7A77FF", "#141446", "#FF6492", "#727290", "#43436B", "#BEF3BE", "#68B68C", "#FFE7AA", "#B2A58D", "#64C8E0"];
const CHART_HEIGHT = 300;

const drawPieChart = (node, resultSet, options) => {
  const data = resultSet.series()[0].series.map(s => s.value);
  const data_ready = d3.pie()(data);
  d3.select(node).html(""); // The radius of the pieplot is half the width or half the height (smallest one).

  const radius = CHART_HEIGHT / 2 - 40; // Seprate container to center align pie chart

  const svg = d3
    .select(node)
    .append("svg")
    .attr("width", node.clientWidth)
    .attr("height", CHART_HEIGHT)
    .append("g")
    .attr(
      "transform",
      "translate(" + node.clientWidth / 2 + "," + CHART_HEIGHT / 2 + ")"
    );
  svg
    .selectAll("pieArcs")
    .data(data_ready)
    .enter()
    .append("path")
    .attr(
      "d",
      d3
        .arc()
        .innerRadius(0)
        .outerRadius(radius)
    )
    .attr("fill", d => COLORS_SERIES[d.index]);

    const size = 12;
    const labels = resultSet.series()[0].series.map(s => s.x);
    svg.selectAll("myrect")
      .data(labels)
      .enter()
      .append("rect")
        .attr("x", 150)
        .attr("y", function(d,i){ return -50 + i*(size+5)})
        .attr("width", size)
        .attr("height", size)
        .style("fill", (d,i) => COLORS_SERIES[i])

    svg.selectAll("mylabels")
      .data(labels)
      .enter()
      .append("text")
        .attr("x", 150 + size*1.2)
        .attr("y", function(d,i){ return -50 + i*(size+5) + (size/2)})
        .text(function(d){ return d})
        .attr("text-anchor", "left")
        .attr("font-size", "12px")
        .style("alignment-baseline", "middle")
};

const drawChart = (node, resultSet, chartType, options = {}) => {
  if (chartType === "pie") {
    return drawPieChart(node, resultSet, options);
  }

  const margin = {
      top: 10,
      right: 30,
      bottom: 30,
      left: 60
    },
    width = node.clientWidth - margin.left - margin.right,
    height = CHART_HEIGHT - margin.top - margin.bottom;
  d3.select(node).html("");
  const svg = d3
    .select(node)
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  const keys = resultSet.seriesNames(options.pivotConfig).map(s => s.key);

  let data, maxData;
  if (chartType === "line") {
    data = resultSet.series(options.pivotConfig).map(series => ({
      key: series.key,
      values: series.series
    }));
    maxData = d3.max(data.map(s => d3.max(s.values, i => i.value)));
  } else {
    data = d3.stack().keys(keys)(resultSet.chartPivot(options.pivotConfig));
    maxData = d3.max(data.map(s => d3.max(s, i => i[1])));
  }

  const color = d3
    .scaleOrdinal()
    .domain(keys)
    .range(COLORS_SERIES);

  let x;
  if (chartType === "bar") {
    x = d3
      .scaleBand()
      .range([0, width])
      .domain(resultSet.chartPivot(options.pivotConfig).map(c => c.x))
      .padding(0.3);
  } else {
    x = d3
      .scaleTime()
      .domain(d3.extent(resultSet.chartPivot(options.pivotConfig), c => d3.isoParse(c.x))).nice()
      .range([0, width]);
  }

  svg
    .append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x));
  const y = d3
    .scaleLinear()
    .domain([0, maxData])
    .range([height, 0]);
  svg.append("g").call(d3.axisLeft(y));

  if (chartType === "line") {
    svg
      .selectAll(".line")
      .data(data)
      .enter()
      .append("path")
      .attr("fill", "none")
      .attr("stroke", d => color(d.key))
      .attr("stroke-width", 1.5)
      .attr("d", d => {
        return d3
          .line()
          .x(d => x(d3.isoParse(d.x)))
          .y(d => y(+d.value))(d.values);
      });
  } else if (chartType === "area") {
    svg
      .selectAll("mylayers")
      .data(data)
      .enter()
      .append("path")
      .style("fill", d => color(d.key))
      .attr(
        "d",
        d3
          .area()
          .x(d => x(d3.isoParse(d.data.x)))
          .y0(d => y(d[0]))
          .y1(d => y(d[1]))
      );
  } else {
    svg
      .append("g")
      .selectAll("g") // Enter in the stack data = loop key per key = group per group
      .data(data)
      .enter()
      .append("g")
      .attr("fill", d => color(d.key))
      .selectAll("rect") // enter a second time = loop subgroup per subgroup to add all rectangles
      .data(d => d)
      .enter()
      .append("rect")
      .attr("x", d => x(d.data.x))
      .attr("y", d => y(d[1]))
      .attr("height", d => y(d[0]) - y(d[1]))
      .attr("width", x.bandwidth());
  }
};

const D3Chart = ({ resultSet, type, ...props }) => (
  <div ref={el => el && drawChart(el, resultSet, type, props)} />
);

const TypeToChartComponent = {
  line: (props) => <D3Chart type="line" {...props} />,
  bar: (props) => <D3Chart type="bar" {...props} />,
  area: (props) => <D3Chart type="area" {...props} />,
  pie: (props) => <D3Chart type="pie" {...props} />,
  number: ({ resultSet }) => (
    <Typography
      variant="h4"
      style={{
        textAlign: "center"
      }}
    >
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
  (error && error.toString()) || <CircularProgress />;

const ChartRenderer = ({ vizState }) => {
  const { query, chartType, pivotConfig = null } = vizState;
  const component = TypeToMemoChartComponent[chartType];
  const renderProps = useCubeQuery(query);
  return component && renderChart(component)({ pivotConfig, ...renderProps });
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
