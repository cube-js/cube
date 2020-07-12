import * as d3 from 'd3';

const drawFrame = `// Set the dimensions and margins of the graph
  const margin = {top: 10, right: 30, bottom: 30, left: 60},
    width = node.clientWidth - margin.left - margin.right,
    height = 400 - margin.top - margin.bottom;

  d3.select(node).html("");
  const svg = d3.select(node)
  .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");`;

const yAxis = (max) => (`// Add Y axis
  const y = d3.scaleLinear()
    .domain([0, ${max}])
    .range([ height, 0 ]);
  svg.append("g")
    .call(d3.axisLeft(y));`);

const xAxisTime = `// Add X axis
  const x = d3.scaleTime()
    .domain(d3.extent(resultSet.chartPivot(), c => d3.isoParse(c.x)))
    .range([ 0, width ]);
  svg.append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x));`;

const stackData = `// Transform data into D3 format
  const keys = resultSet.seriesNames().map(s => s.key)
  const data = d3.stack()
    .keys(keys)
    (resultSet.chartPivot())

  // Color palette
  const color = d3.scaleOrdinal()
    .domain(keys)
    .range(COLORS_SERIES)`;

const drawByChartType = {
  line: `
  // Prepare data in D3 format
  const data = resultSet.series().map((series) => ({
    key: series.title, values: series.series
  }));

  // color palette
  const color = d3.scaleOrdinal()
    .domain(data.map(d => d.key ))
    .range(COLORS_SERIES)

  ${xAxisTime}

  ${yAxis('d3.max(data.map((s) => d3.max(s.values, (i) => i.value)))')}

  // Draw the lines
  svg.selectAll(".line")
    .data(data)
    .enter()
    .append("path")
      .attr("fill", "none")
      .attr("stroke", d => color(d.key))
      .attr("stroke-width", 1.5)
      .attr("d", (d) => {
        return d3.line()
          .x(d => x(d3.isoParse(d.x)))
          .y(d => y(+d.value))
          (d.values)
      })
  `,
  bar: `
  ${stackData}

  // Add X axis
  const x = d3.scaleBand()
    .range([ 0, width ])
    .domain(resultSet.chartPivot().map(c => c.x))
    .padding(0.3);
  svg.append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x))

  ${yAxis('d3.max(data.map((s) => d3.max(s, (i) => i[1])))')}

  // Add the bars
  svg.append("g")
    .selectAll("g")
    // Enter in the stack data = loop key per key = group per group
    .data(data)
    .enter().append("g")
      .attr("fill", d => color(d.key))
      .selectAll("rect")
      // enter a second time = loop subgroup per subgroup to add all rectangles
      .data(d => d)
      .enter().append("rect")
        .attr("x", d => x(d.data.x))
        .attr("y", d => y(d[1]))
        .attr("height", d => y(d[0]) - y(d[1]))
        .attr("width",x.bandwidth())
    `,
  area: `
  ${stackData}

  ${xAxisTime}

  ${yAxis('d3.max(data.map((s) => d3.max(s, (i) => i[1])))')}

  // Add the areas
  svg
  .selectAll("mylayers")
  .data(data)
  .enter().append("path")
    .style("fill", d => color(d.key))
    .attr("d", d3.area()
      .x(d => x(d3.isoParse(d.data.x)))
      .y0(d => y(d[0]))
      .y1(d => y(d[1]))
  )
  `,
  pie: `const data = resultSet.series()[0].series.map(s => s.value);
  const data_ready = d3.pie()(data);

  // The radius of the pieplot is half the width or half the height (smallest one).
  const radius = Math.min(400, 400) / 2 - 40;

  // Seprate container to center align pie chart
  const pieContainer = svg.attr('height', height)
      .append('g')
      .attr('transform', 'translate(' + width/2 +  ',' + height/2 +')');

  pieContainer
    .selectAll('pieArcs')
    .data(data_ready)
    .enter()
    .append('path')
    .attr('d', d3.arc()
      .innerRadius(0)
      .outerRadius(radius)
    )
    .attr('fill', d => COLORS_SERIES[d.index])
  `
};

export const sourceCodeTemplate = ({ chartType, renderFnName }) => (
  `
import * as d3 from 'd3';
const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];

const draw = (node, resultSet, chartType) => {
  ${drawFrame}
  ${drawByChartType[chartType]}
}

const ${renderFnName} = ({ resultSet }) => (
  <div ref={el => el && draw(el, resultSet, '${chartType}')} />
)
`
);

export const imports = {
  d3
};
