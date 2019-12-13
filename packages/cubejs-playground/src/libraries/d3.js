import * as d3 from 'd3';

export const sourceCodeTemplate = ({ chartType, renderFnName }) => (
  `
import * as d3 from 'd3';
const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];

const draw = (node, resultSet, chartType) => {
  if (chartType != 'line' || node === null) {
    return null;
  }
  // set the dimensions and margins of the graph
  var margin = {top: 10, right: 30, bottom: 30, left: 60},
      width = node.clientWidth - margin.left - margin.right, height = 400 - margin.top - margin.bottom;

  var svg = d3.select(node)
  .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");

  // Add X axis --> it is a date format
  var x = d3.scaleTime()
    .domain(d3.extent(resultSet.chartPivot(), function(c) { return d3.isoParse(c.x); }))
    .range([ 0, width ]);
  svg.append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x));

  // Add Y axis
  const axisData = resultSet.series()[0].series;
  var y = d3.scaleLinear()
    .domain([0, d3.max(axisData, function(d) { return +d.value; })])
    .range([ height, 0 ]);
  svg.append("g")
    .call(d3.axisLeft(y));

  // Add the lines
  resultSet.series().forEach((series) => {
    svg.append("path")
      .datum(series.series)
      .attr("fill", "none")
      .attr("stroke", "steelblue")
      .attr("stroke-width", 1.5)
      .attr("d", d3.line()
        .x(function(d) { return x(d3.isoParse(d.x)) })
        .y(function(d) { return y(d.value) })
        )
  });
}

const ${renderFnName} = ({ resultSet }) => {
    return (
      <div
        ref={el => draw(el, resultSet, '${chartType}')}
      />
    );
};
`
);

export const imports = {
  'd3': d3
};
