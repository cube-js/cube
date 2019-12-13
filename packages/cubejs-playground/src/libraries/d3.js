import * as d3 from 'd3';

const drawFrame = `
  // set the dimensions and margins of the graph
  var margin = {top: 10, right: 30, bottom: 30, left: 60},
      width = node.clientWidth - margin.left - margin.right, height = 400 - margin.top - margin.bottom;

  d3.select(node).html("");
  var svg = d3.select(node)
  .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");`;

const yAxis = (max) => (`
  // Add Y axis
  var y = d3.scaleLinear()
    .domain([0, ${max}])
    .range([ height, 0 ]);
  svg.append("g")
    .call(d3.axisLeft(y));`);

const xAxisTime = `
  // Add X axis
  var x = d3.scaleTime()
    .domain(d3.extent(resultSet.chartPivot(), function(c) { return d3.isoParse(c.x); }))
    .range([ 0, width ]);
  svg.append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x));`;

const drawByChartType = {
  line: `
  ${xAxisTime}

  // prepare data
  const data = resultSet.series().map((series) => ({
    key: series.title, values: series.series
  }));
  ${yAxis(`d3.max(data.map((s) => d3.max(s.values, (i) => i.value)))`)}

  // color palette
  var color = d3.scaleOrdinal()
    .domain(data.map(d => d.key ))
    .range(COLORS_SERIES)

  // Draw the lines
  svg.selectAll(".line")
    .data(data)
    .enter()
    .append("path")
      .attr("fill", "none")
      .attr("stroke", d => color(d.key))
      .attr("stroke-width", 1.5)
      .attr("d", function(d){
        return d3.line()
          .x(d => x(d3.isoParse(d.x)))
          .y(d => y(+d.value))
          (d.values)
      })
  `,
  bar: `
  // Add X axis
  var x = d3.scaleBand()
    .range([ 0, width ])
    .domain(resultSet.chartPivot().map(c => c.x))
    .padding(0.2);
  svg.append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x))
    .selectAll("text")
      .attr("transform", "translate(-10,0)rotate(-45)")
      .style("text-anchor", "end");

  // Transform data into D3 format
  var keys = resultSet.seriesNames().map(s => s.key)
  const data = d3.stack()
    .keys(keys)
    (resultSet.chartPivot())

  // color palette
  var color = d3.scaleOrdinal()
    .domain(keys)
    .range(COLORS_SERIES)

  ${yAxis(`d3.max(data.map((s) => d3.max(s, (i) => i[1])))`)}

  // Show the bars
  svg.append("g")
    .selectAll("g")
    // Enter in the stack data = loop key per key = group per group
    .data(data)
    .enter().append("g")
      .attr("fill", d => color(d.key))
      .selectAll("rect")
      // enter a second time = loop subgroup per subgroup to add all rectangles
      .data(function(d) { return d; })
      .enter().append("rect")
        .attr("x", d => x(d.data.x))
        .attr("y", d => y(d[1]))
        .attr("height", d => y(d[0]) - y(d[1]))
        .attr("width",x.bandwidth())
    `,
  area: `
    ${xAxisTime}
    ${yAxis}
    // Add the area
    svg.append("path")
      .datum(resultSet.series()[0].series)
      .attr("fill", "#cce5df")
      .attr("stroke", "#69b3a2")
      .attr("stroke-width", 1.5)
      .attr("d", d3.area()
        .x(function(d) { return x(d3.isoParse(d.x)) })
        .y0(y(0))
        .y1(function(d) { return y(d.value) })
      )
  `
};

export const sourceCodeTemplate = ({ chartType, renderFnName }) => (
  `
import * as d3 from 'd3';
const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];

const draw = (node, resultSet, chartType) => {
  if (node === null) {
    return null;
  }
  ${drawFrame}
  ${drawByChartType[chartType]}
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
