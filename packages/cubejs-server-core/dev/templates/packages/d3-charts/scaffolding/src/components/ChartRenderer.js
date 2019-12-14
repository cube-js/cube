import * as d3 from 'd3';

const COLORS_SERIES = ["#FF6492", "#141446", "#7A77FF"];
const CHART_HEIGHT = 300;

const drawPieChart = (node, resultSet) => {
  const data = resultSet.series()[0].series.map(s => s.value);
  const data_ready = d3.pie()(data);

  d3.select(node).html("");

  // The radius of the pieplot is half the width or half the height (smallest one).
  const radius = CHART_HEIGHT / 2 - 40;

  // Seprate container to center align pie chart
  const svg = d3.select(node)
    .append("svg")
      .attr("width", node.clientWidth)
      .attr("height", CHART_HEIGHT)
    .append("g")
      .attr('transform', 'translate(' + node.clientWidth/2 +  ',' + CHART_HEIGHT/2 +')');

  svg
    .selectAll('pieArcs')
    .data(data_ready)
    .enter()
    .append('path')
    .attr('d', d3.arc()
      .innerRadius(0)
      .outerRadius(radius)
    )
    .attr('fill', d => COLORS_SERIES[d.index])
}

const drawChart = (node, resultSet, chartType) => {
  if (chartType === 'pie') {
    return drawPieChart(node, resultSet);
  }

  const margin = { top: 10, right: 30, bottom: 30, left: 60 },
    width = node.clientWidth - margin.left - margin.right,
    height = CHART_HEIGHT - margin.top - margin.bottom;

  d3.select(node).html("");
  const svg = d3.select(node)
    .append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
    .append("g")
      .attr("transform",
            "translate(" + margin.left + "," + margin.top + ")");

  const keys = resultSet.seriesNames().map(s => s.key)

  let data, maxData;
  if (chartType === 'line') {
    data = resultSet.series().map((series) => ({
      key: series.title, values: series.series
    }));
    maxData = d3.max(data.map((s) => d3.max(s.values, (i) => i.value)))
  } else {
    data = d3.stack()
      .keys(keys)
      (resultSet.chartPivot())
    maxData = d3.max(data.map((s) => d3.max(s, (i) => i[1])))
  }


  const color = d3.scaleOrdinal()
    .domain(keys)
    .range(COLORS_SERIES);

  let x;
  if (chartType === 'bar') {
    x = d3.scaleBand()
      .range([ 0, width ])
      .domain(resultSet.chartPivot().map(c => c.x))
      .padding(0.3);
  } else {
    x = d3.scaleTime()
      .domain(d3.extent(resultSet.chartPivot(), c => d3.isoParse(c.x)))
      .range([ 0, width ]);
  }

  svg.append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x));

  const y = d3.scaleLinear()
    .domain([0, maxData])
    .range([ height, 0 ]);
  svg.append("g")
    .call(d3.axisLeft(y));

  if (chartType === 'line') {
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
  } else if (chartType === 'area') {
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
  } else {
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
  }
}

const D3Chart = ({ resultSet, type }) => (
  <div
    ref={el => el && drawChart(el, resultSet, type)}
  />
);

const TypeToChartComponent = {
  line: ({ resultSet }) => (
    <D3Chart type='line' resultSet={resultSet} />
  ),
  bar: ({ resultSet }) => (
    <D3Chart type='bar' resultSet={resultSet} />
  ),
  area: ({ resultSet }) => (
    <D3Chart type='area' resultSet={resultSet} />
  ),
  pie: ({ resultSet }) => (
    <D3Chart type='pie' resultSet={resultSet} />
  ),
};
