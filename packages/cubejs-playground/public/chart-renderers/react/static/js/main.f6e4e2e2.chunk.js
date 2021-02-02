(this["webpackJsonpreact-charts"]=this["webpackJsonpreact-charts"]||[]).push([[0],{553:function(t,e,n){},555:function(t,e,n){},748:function(t,e,n){"use strict";n.r(e);var r={};n.r(r),n.d(r,"getChartComponent",(function(){return f})),n.d(r,"getCommon",(function(){return b})),n.d(r,"getImports",(function(){return g}));var a={};n.r(a),n.d(a,"getChartComponent",(function(){return j})),n.d(a,"getCommon",(function(){return y})),n.d(a,"getImports",(function(){return C}));var s={};n.r(s),n.d(s,"getChartComponent",(function(){return S})),n.d(s,"getCommon",(function(){return O})),n.d(s,"getImports",(function(){return k}));var i={};n.r(i),n.d(i,"getChartComponent",(function(){return R})),n.d(i,"getCommon",(function(){return w})),n.d(i,"getImports",(function(){return F}));var o=n(10),c=n(0),l=n.n(c),u=n(32),d=n.n(u),p=(n(553),n(50)),m=(n(554),n(555),n(450)),h=["import React from 'react';","import { Chart, Axis, Tooltip, Geom, Coord, Legend } from 'bizcharts';","import { Row, Col, Statistic, Table } from 'antd';"];function f(t){return"line"===t?'return (\n  <Chart\n    scale={{\n      x: {\n        tickCount: 8,\n      },\n    }}\n    height={400}\n    data={stackedChartData(resultSet)}\n    forceFit\n  >\n    <Axis name="x" />\n    <Axis name="measure" />\n    <Tooltip\n      crosshairs={{\n        type: \'y\',\n      }}\n    />\n    <Geom type="line" position="x*measure" size={2} color="color" />\n  </Chart>\n);\n':"x"===t||"tickCount"===t||"type"===t?"":"bar"===t?'return (\n  <Chart\n    scale={{\n      x: {\n        tickCount: 8,\n      },\n    }}\n    height={400}\n    data={stackedChartData(resultSet)}\n    forceFit\n  >\n    <Axis name="x" />\n    <Axis name="measure" />\n    <Tooltip />\n    <Geom type="interval" position="x*measure" color="color" />\n  </Chart>\n);\n':"x"===t||"tickCount"===t?"":"area"===t?'return (\n  <Chart\n    scale={{\n      x: {\n        tickCount: 8,\n      },\n    }}\n    height={400}\n    data={stackedChartData(resultSet)}\n    forceFit\n  >\n    <Axis name="x" />\n    <Axis name="measure" />\n    <Tooltip\n      crosshairs={{\n        type: \'y\',\n      }}\n    />\n    <Geom type="area" position="x*measure" size={2} color="color" />\n  </Chart>\n);\n':"x"===t||"tickCount"===t||"type"===t?"":"pie"===t?'return (\n  <Chart height={400} data={resultSet.chartPivot()} forceFit>\n    <Coord type="theta" radius={0.75} />\n    {resultSet.seriesNames().map((s) => (\n      <Axis name={s.key} />\n    ))}\n    <Legend position="right" />\n    <Tooltip />\n    {resultSet.seriesNames().map((s) => (\n      <Geom type="interval" position={s.key} color="category" />\n    ))}\n  </Chart>\n);\n':"height"===t?"":void 0}function b(){return"const stackedChartData = (resultSet) => {\n  const data = resultSet\n    .pivot()\n    .map(({ xValues, yValuesArray }) =>\n      yValuesArray.map(([yValues, m]) => ({\n        x: resultSet.axisValuesString(xValues, ', '),\n        color: resultSet.axisValuesString(yValues, ', '),\n        measure: m && Number.parseFloat(m),\n      }))\n    )\n    .reduce((a, b) => a.concat(b), []);\n  return data;\n};\n"}function g(){return h}var x=["import React from 'react';","import { CartesianGrid, PieChart, Pie, Cell, AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend, BarChart, Bar, LineChart, Line } from 'recharts';","import { Row, Col, Statistic, Table } from 'antd';"];function j(t){return"line"===t?'return (\n  <CartesianChart resultSet={resultSet} ChartComponent={LineChart}>\n    {resultSet.seriesNames().map((series, i) => (\n      <Line\n        key={series.key}\n        stackId="a"\n        dataKey={series.key}\n        name={series.title}\n        stroke={colors[i]}\n      />\n    ))}\n  </CartesianChart>\n);\n':"bar"===t?'return (\n  <CartesianChart resultSet={resultSet} ChartComponent={BarChart}>\n    {resultSet.seriesNames().map((series, i) => (\n      <Bar\n        key={series.key}\n        stackId="a"\n        dataKey={series.key}\n        name={series.title}\n        fill={colors[i]}\n      />\n    ))}\n  </CartesianChart>\n);\n':"area"===t?'return (\n  <CartesianChart resultSet={resultSet} ChartComponent={AreaChart}>\n    {resultSet.seriesNames().map((series, i) => (\n      <Area\n        key={series.key}\n        stackId="a"\n        dataKey={series.key}\n        name={series.title}\n        stroke={colors[i]}\n        fill={colors[i]}\n      />\n    ))}\n  </CartesianChart>\n);\n':"pie"===t?'return (\n  <ResponsiveContainer width="100%" height={350}>\n    <PieChart>\n      <Pie\n        isAnimationActive={false}\n        data={resultSet.chartPivot()}\n        nameKey="x"\n        dataKey={resultSet.seriesNames()[0].key}\n        fill="#8884d8"\n      >\n        {resultSet.chartPivot().map((e, index) => (\n          <Cell key={index} fill={colors[index % colors.length]} />\n        ))}\n      </Pie>\n      <Legend />\n      <Tooltip />\n    </PieChart>\n  </ResponsiveContainer>\n);\n':"height"===t?"":void 0}function y(){return"const CartesianChart = ({ resultSet, children, ChartComponent }) => (\n  <ResponsiveContainer width=\"100%\" height={350}>\n    <ChartComponent data={resultSet.chartPivot()}>\n      <XAxis dataKey=\"x\" />\n      <YAxis />\n      <CartesianGrid />\n      {children}\n      <Legend />\n      <Tooltip />\n    </ChartComponent>\n  </ResponsiveContainer>\n);\n\nconst colors = ['#FF6492', '#141446', '#7A77FF'];\n\nconst stackedChartData = (resultSet) => {\n  const data = resultSet\n    .pivot()\n    .map(({ xValues, yValuesArray }) =>\n      yValuesArray.map(([yValues, m]) => ({\n        x: resultSet.axisValuesString(xValues, ', '),\n        color: resultSet.axisValuesString(yValues, ', '),\n        measure: m && Number.parseFloat(m),\n      }))\n    )\n    .reduce((a, b) => a.concat(b), []);\n  return data;\n};\n"}function C(){return x}var v=["import React from 'react';","import { Line, Bar, Pie } from 'react-chartjs-2';","import { Row, Col, Statistic, Table } from 'antd';"];function S(t){return"line"===t?"const data = {\n  labels: resultSet.categories().map((c) => c.category),\n  datasets: resultSet.series().map((s, index) => ({\n    label: s.title,\n    data: s.series.map((r) => r.value),\n    borderColor: COLORS_SERIES[index],\n    fill: false,\n  })),\n};\nconst options = { ...commonOptions };\nreturn <Line data={data} options={options} />;\n":"labels"===t||"datasets"===t||"label"===t||"data"===t||"borderColor"===t||"fill"===t?"":"bar"===t?"const data = {\n  labels: resultSet.categories().map((c) => c.category),\n  datasets: resultSet.series().map((s, index) => ({\n    label: s.title,\n    data: s.series.map((r) => r.value),\n    backgroundColor: COLORS_SERIES[index],\n    fill: false,\n  })),\n};\nconst options = {\n  ...commonOptions,\n  scales: {\n    xAxes: [\n      {\n        stacked: true,\n      },\n    ],\n  },\n};\nreturn <Bar data={data} options={options} />;\n":"labels"===t||"datasets"===t||"label"===t||"data"===t||"backgroundColor"===t||"fill"===t||"scales"===t||"xAxes"===t||"stacked"===t?"":"area"===t?"const data = {\n  labels: resultSet.categories().map((c) => c.category),\n  datasets: resultSet.series().map((s, index) => ({\n    label: s.title,\n    data: s.series.map((r) => r.value),\n    backgroundColor: COLORS_SERIES[index],\n  })),\n};\nconst options = {\n  ...commonOptions,\n  scales: {\n    yAxes: [\n      {\n        stacked: true,\n      },\n    ],\n  },\n};\nreturn <Line data={data} options={options} />;\n":"labels"===t||"datasets"===t||"label"===t||"data"===t||"backgroundColor"===t||"scales"===t||"yAxes"===t||"stacked"===t?"":"pie"===t?"const data = {\n  labels: resultSet.categories().map((c) => c.category),\n  datasets: resultSet.series().map((s) => ({\n    label: s.title,\n    data: s.series.map((r) => r.value),\n    backgroundColor: COLORS_SERIES,\n    hoverBackgroundColor: COLORS_SERIES,\n  })),\n};\nconst options = { ...commonOptions };\nreturn <Pie data={data} options={options} />;\n":"labels"===t||"datasets"===t||"label"===t||"data"===t||"backgroundColor"===t||"hoverBackgroundColor"===t||"height"===t?"":void 0}function O(){return"const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];\nconst commonOptions = {\n  maintainAspectRatio: false,\n};\n"}function k(){return v}var A=["import React from 'react';","import * as d3 from 'd3';","import { Row, Col, Statistic, Table } from 'antd';"];function R(t){return"line"===t?'return <D3Chart type="line" {...props} />;\n':"bar"===t?'return <D3Chart type="bar" {...props} />;\n':"area"===t?'return <D3Chart type="area" {...props} />;\n':"pie"===t?'return <D3Chart type="pie" {...props} />;\n':"height"===t?"":void 0}function w(){return"const COLORS_SERIES = [\n  '#7A77FF',\n  '#141446',\n  '#FF6492',\n  '#727290',\n  '#43436B',\n  '#BEF3BE',\n  '#68B68C',\n  '#FFE7AA',\n  '#B2A58D',\n  '#64C8E0',\n];\nconst CHART_HEIGHT = 300;\n\nconst drawPieChart = (node, resultSet, options) => {\n  const data = resultSet.series()[0].series.map((s) => s.value);\n  const data_ready = d3.pie()(data);\n  d3.select(node).html(''); // The radius of the pieplot is half the width or half the height (smallest one).\n\n  const radius = CHART_HEIGHT / 2 - 40; // Seprate container to center align pie chart\n\n  const svg = d3\n    .select(node)\n    .append('svg')\n    .attr('width', node.clientWidth)\n    .attr('height', CHART_HEIGHT)\n    .append('g')\n    .attr(\n      'transform',\n      'translate(' + node.clientWidth / 2 + ',' + CHART_HEIGHT / 2 + ')'\n    );\n  svg\n    .selectAll('pieArcs')\n    .data(data_ready)\n    .enter()\n    .append('path')\n    .attr('d', d3.arc().innerRadius(0).outerRadius(radius))\n    .attr('fill', (d) => COLORS_SERIES[d.index]);\n  const size = 12;\n  const labels = resultSet.series()[0].series.map((s) => s.x);\n  svg\n    .selectAll('myrect')\n    .data(labels)\n    .enter()\n    .append('rect')\n    .attr('x', 150)\n    .attr('y', function (d, i) {\n      return -50 + i * (size + 5);\n    })\n    .attr('width', size)\n    .attr('height', size)\n    .style('fill', (d, i) => COLORS_SERIES[i]);\n  svg\n    .selectAll('mylabels')\n    .data(labels)\n    .enter()\n    .append('text')\n    .attr('x', 150 + size * 1.2)\n    .attr('y', function (d, i) {\n      return -50 + i * (size + 5) + size / 2;\n    })\n    .text(function (d) {\n      return d;\n    })\n    .attr('text-anchor', 'left')\n    .attr('font-size', '12px')\n    .style('alignment-baseline', 'middle');\n};\n\nconst drawChart = (node, resultSet, chartType, options = {}) => {\n  if (chartType === 'pie') {\n    return drawPieChart(node, resultSet, options);\n  }\n\n  const margin = {\n      top: 10,\n      right: 30,\n      bottom: 30,\n      left: 60,\n    },\n    width = node.clientWidth - margin.left - margin.right,\n    height = CHART_HEIGHT - margin.top - margin.bottom;\n  d3.select(node).html('');\n  const svg = d3\n    .select(node)\n    .append('svg')\n    .attr('width', width + margin.left + margin.right)\n    .attr('height', height + margin.top + margin.bottom)\n    .append('g')\n    .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');\n  const keys = resultSet.seriesNames(options.pivotConfig).map((s) => s.key);\n  let data, maxData;\n\n  if (chartType === 'line') {\n    data = resultSet.series(options.pivotConfig).map((series) => ({\n      key: series.key,\n      values: series.series,\n    }));\n    maxData = d3.max(data.map((s) => d3.max(s.values, (i) => i.value)));\n  } else {\n    data = d3.stack().keys(keys)(resultSet.chartPivot(options.pivotConfig));\n    maxData = d3.max(data.map((s) => d3.max(s, (i) => i[1])));\n  }\n\n  const color = d3.scaleOrdinal().domain(keys).range(COLORS_SERIES);\n  let x;\n\n  if (chartType === 'bar') {\n    x = d3\n      .scaleBand()\n      .range([0, width])\n      .domain(resultSet.chartPivot(options.pivotConfig).map((c) => c.x))\n      .padding(0.3);\n  } else {\n    x = d3\n      .scaleTime()\n      .domain(\n        d3.extent(resultSet.chartPivot(options.pivotConfig), (c) =>\n          d3.isoParse(c.x)\n        )\n      )\n      .nice()\n      .range([0, width]);\n  }\n\n  svg\n    .append('g')\n    .attr('transform', 'translate(0,' + height + ')')\n    .call(d3.axisBottom(x));\n  const y = d3.scaleLinear().domain([0, maxData]).range([height, 0]);\n  svg.append('g').call(d3.axisLeft(y));\n\n  if (chartType === 'line') {\n    svg\n      .selectAll('.line')\n      .data(data)\n      .enter()\n      .append('path')\n      .attr('fill', 'none')\n      .attr('stroke', (d) => color(d.key))\n      .attr('stroke-width', 1.5)\n      .attr('d', (d) => {\n        return d3\n          .line()\n          .x((d) => x(d3.isoParse(d.x)))\n          .y((d) => y(+d.value))(d.values);\n      });\n  } else if (chartType === 'area') {\n    svg\n      .selectAll('mylayers')\n      .data(data)\n      .enter()\n      .append('path')\n      .style('fill', (d) => color(d.key))\n      .attr(\n        'd',\n        d3\n          .area()\n          .x((d) => x(d3.isoParse(d.data.x)))\n          .y0((d) => y(d[0]))\n          .y1((d) => y(d[1]))\n      );\n  } else {\n    svg\n      .append('g')\n      .selectAll('g') // Enter in the stack data = loop key per key = group per group\n      .data(data)\n      .enter()\n      .append('g')\n      .attr('fill', (d) => color(d.key))\n      .selectAll('rect') // enter a second time = loop subgroup per subgroup to add all rectangles\n      .data((d) => d)\n      .enter()\n      .append('rect')\n      .attr('x', (d) => x(d.data.x))\n      .attr('y', (d) => y(d[1]))\n      .attr('height', (d) => y(d[0]) - y(d[1]))\n      .attr('width', x.bandwidth());\n  }\n};\n\nconst D3Chart = ({ resultSet, type, ...props }) => (\n  <div ref={(el) => el && drawChart(el, resultSet, type, props)} />\n);\n"}function F(){return A}var E={bizchartsCharts:r,rechartsCharts:a,chartjsCharts:s,d3Charts:i},T=["react-dom","@cubejs-client/core","@cubejs-client/react","antd"];var P=n(105),L=n.n(P),I=n(310),B=n(423),_=window.parent.window.__cubejsPlayground||{},N=L()(_.token||"secret",{apiUrl:_.apiUrl||"http://localhost:4000/cubejs-api/v1"}),D=function(t){var e=t.renderFunction,n=t.query,r=t.pivotConfig,a=Object(I.b)(n),s=a.isLoading,i=a.error,l=a.resultSet,u=a.progress;return Object(c.useEffect)((function(){var t=(window.parent.window.__cubejsPlayground||{}).onQueryLoad;s||"function"===typeof t&&l&&t({resultSet:l,error:i,progress:u})}),[i,s,l,u]),i?Object(o.jsx)("div",{children:i.toString()}):l?e({resultSet:l,pivotConfig:r}):Object(o.jsx)(B.a,{})},V=function(t){var e=t.renderFunction,n=t.query,r=t.pivotConfig,a=void 0===r?null:r;return Object(o.jsx)(I.a,{cubejsApi:N,children:Object(o.jsx)(D,{renderFunction:e,query:n,pivotConfig:a})})},z=n(82),G=n(453),H=n(46),K=n(765),q=n(766),W=n(760),U=n(759),M=["#7A77FF","#141446","#FF6492","#727290","#43436B","#BEF3BE","#68B68C","#FFE7AA","#B2A58D","#64C8E0"],Q=300,J=function(t,e,n){var r=e.series()[0].series.map((function(t){return t.value})),a=H.i()(r);H.n(t).html("");var s=H.n(t).append("svg").attr("width",t.clientWidth).attr("height",Q).append("g").attr("transform","translate("+t.clientWidth/2+",150)");s.selectAll("pieArcs").data(a).enter().append("path").attr("d",H.a().innerRadius(0).outerRadius(110)).attr("fill",(function(t){return M[t.index]}));var i=12,o=e.series()[0].series.map((function(t){return t.x}));s.selectAll("myrect").data(o).enter().append("rect").attr("x",150).attr("y",(function(t,e){return 17*e-50})).attr("width",i).attr("height",i).style("fill",(function(t,e){return M[e]})),s.selectAll("mylabels").data(o).enter().append("text").attr("x",164.4).attr("y",(function(t,e){return 17*e-50+6})).text((function(t){return t})).attr("text-anchor","left").attr("font-size","12px").style("alignment-baseline","middle")},X=function(t){var e=t.resultSet,n=t.type,r=Object(G.a)(t,["resultSet","type"]);return Object(o.jsx)("div",{ref:function(t){return t&&function(t,e,n){var r=arguments.length>3&&void 0!==arguments[3]?arguments[3]:{};if("pie"===n)return J(t,e);var a={top:10,right:30,bottom:30,left:60},s=t.clientWidth-a.left-a.right,i=Q-a.top-a.bottom;H.n(t).html("");var o,c,l=H.n(t).append("svg").attr("width",s+a.left+a.right).attr("height",i+a.top+a.bottom).append("g").attr("transform","translate("+a.left+","+a.top+")"),u=e.seriesNames(r.pivotConfig).map((function(t){return t.key}));"line"===n?(o=e.series(r.pivotConfig).map((function(t){return{key:t.key,values:t.series}})),c=H.h(o.map((function(t){return H.h(t.values,(function(t){return t.value}))})))):(o=H.o().keys(u)(e.chartPivot(r.pivotConfig)),c=H.h(o.map((function(t){return H.h(t,(function(t){return t[1]}))}))));var d,p=H.l().domain(u).range(M);d="bar"===n?H.j().range([0,s]).domain(e.chartPivot(r.pivotConfig).map((function(t){return t.x}))).padding(.3):H.m().domain(H.e(e.chartPivot(r.pivotConfig),(function(t){return H.f(t.x)}))).nice().range([0,s]),l.append("g").attr("transform","translate(0,"+i+")").call(H.c(d));var m=H.k().domain([0,c]).range([i,0]);l.append("g").call(H.d(m)),"line"===n?l.selectAll(".line").data(o).enter().append("path").attr("fill","none").attr("stroke",(function(t){return p(t.key)})).attr("stroke-width",1.5).attr("d",(function(t){return H.g().x((function(t){return d(H.f(t.x))})).y((function(t){return m(+t.value)}))(t.values)})):"area"===n?l.selectAll("mylayers").data(o).enter().append("path").style("fill",(function(t){return p(t.key)})).attr("d",H.b().x((function(t){return d(H.f(t.data.x))})).y0((function(t){return m(t[0])})).y1((function(t){return m(t[1])}))):l.append("g").selectAll("g").data(o).enter().append("g").attr("fill",(function(t){return p(t.key)})).selectAll("rect").data((function(t){return t})).enter().append("rect").attr("x",(function(t){return d(t.data.x)})).attr("y",(function(t){return m(t[1])})).attr("height",(function(t){return m(t[0])-m(t[1])})).attr("width",d.bandwidth())}(t,e,n,r)}})},Y={line:function(t){return Object(o.jsx)(X,Object(z.a)({type:"line"},t))},bar:function(t){return Object(o.jsx)(X,Object(z.a)({type:"bar"},t))},area:function(t){return Object(o.jsx)(X,Object(z.a)({type:"area"},t))},pie:function(t){return Object(o.jsx)(X,Object(z.a)({type:"pie"},t))},number:function(t){var e=t.resultSet;return Object(o.jsx)(K.a,{type:"flex",justify:"center",align:"middle",style:{height:"100%"},children:Object(o.jsx)(q.a,{children:e.seriesNames().map((function(t){return Object(o.jsx)(W.a,{value:e.totalRow()[t.key]})}))})})},table:function(t){var e=t.resultSet,n=t.pivotConfig;return Object(o.jsx)(U.a,{pagination:!1,columns:e.tableColumns(n),dataSource:e.tablePivot(n)})}},Z=n(51),$=function(t){return t.pivot().map((function(e){var n=e.xValues;return e.yValuesArray.map((function(e){var r=Object(p.a)(e,2),a=r[0],s=r[1];return{x:t.axisValuesString(n,", "),color:t.axisValuesString(a,", "),measure:s&&Number.parseFloat(s)}}))})).reduce((function(t,e){return t.concat(e)}),[])},tt={line:function(t){var e=t.resultSet;return Object(o.jsxs)(Z.Chart,{scale:{x:{tickCount:8}},height:400,data:$(e),forceFit:!0,children:[Object(o.jsx)(Z.Axis,{name:"x"}),Object(o.jsx)(Z.Axis,{name:"measure"}),Object(o.jsx)(Z.Tooltip,{crosshairs:{type:"y"}}),Object(o.jsx)(Z.Geom,{type:"line",position:"x*measure",size:2,color:"color"})]})},bar:function(t){var e=t.resultSet;return Object(o.jsxs)(Z.Chart,{scale:{x:{tickCount:8}},height:400,data:$(e),forceFit:!0,children:[Object(o.jsx)(Z.Axis,{name:"x"}),Object(o.jsx)(Z.Axis,{name:"measure"}),Object(o.jsx)(Z.Tooltip,{}),Object(o.jsx)(Z.Geom,{type:"interval",position:"x*measure",color:"color"})]})},area:function(t){var e=t.resultSet;return Object(o.jsxs)(Z.Chart,{scale:{x:{tickCount:8}},height:400,data:$(e),forceFit:!0,children:[Object(o.jsx)(Z.Axis,{name:"x"}),Object(o.jsx)(Z.Axis,{name:"measure"}),Object(o.jsx)(Z.Tooltip,{crosshairs:{type:"y"}}),Object(o.jsx)(Z.Geom,{type:"area",position:"x*measure",size:2,color:"color"})]})},pie:function(t){var e=t.resultSet;return Object(o.jsxs)(Z.Chart,{height:400,data:e.chartPivot(),forceFit:!0,children:[Object(o.jsx)(Z.Coord,{type:"theta",radius:.75}),e.seriesNames().map((function(t){return Object(o.jsx)(Z.Axis,{name:t.key})})),Object(o.jsx)(Z.Legend,{position:"right"}),Object(o.jsx)(Z.Tooltip,{}),e.seriesNames().map((function(t){return Object(o.jsx)(Z.Geom,{type:"interval",position:t.key,color:"category"})}))]})},number:function(t){var e=t.resultSet;return Object(o.jsx)(K.a,{type:"flex",justify:"center",align:"middle",style:{height:"100%"},children:Object(o.jsx)(q.a,{children:e.seriesNames().map((function(t){return Object(o.jsx)(W.a,{value:e.totalRow()[t.key]})}))})})},table:function(t){var e=t.resultSet,n=t.pivotConfig;return Object(o.jsx)(U.a,{pagination:!1,columns:e.tableColumns(n),dataSource:e.tablePivot(n)})}},et=n(194),nt=["#FF6492","#141446","#7A77FF"],rt={maintainAspectRatio:!1},at={line:function(t){var e=t.resultSet,n={labels:e.categories().map((function(t){return t.category})),datasets:e.series().map((function(t,e){return{label:t.title,data:t.series.map((function(t){return t.value})),borderColor:nt[e],fill:!1}}))},r=Object(z.a)({},rt);return Object(o.jsx)(et.Line,{data:n,options:r})},bar:function(t){var e=t.resultSet,n={labels:e.categories().map((function(t){return t.category})),datasets:e.series().map((function(t,e){return{label:t.title,data:t.series.map((function(t){return t.value})),backgroundColor:nt[e],fill:!1}}))},r=Object(z.a)(Object(z.a)({},rt),{},{scales:{xAxes:[{stacked:!0}]}});return Object(o.jsx)(et.Bar,{data:n,options:r})},area:function(t){var e=t.resultSet,n={labels:e.categories().map((function(t){return t.category})),datasets:e.series().map((function(t,e){return{label:t.title,data:t.series.map((function(t){return t.value})),backgroundColor:nt[e]}}))},r=Object(z.a)(Object(z.a)({},rt),{},{scales:{yAxes:[{stacked:!0}]}});return Object(o.jsx)(et.Line,{data:n,options:r})},pie:function(t){var e=t.resultSet,n={labels:e.categories().map((function(t){return t.category})),datasets:e.series().map((function(t){return{label:t.title,data:t.series.map((function(t){return t.value})),backgroundColor:nt,hoverBackgroundColor:nt}}))},r=Object(z.a)({},rt);return Object(o.jsx)(et.Pie,{data:n,options:r})},number:function(t){var e=t.resultSet;return Object(o.jsx)(K.a,{type:"flex",justify:"center",align:"middle",style:{height:"100%"},children:Object(o.jsx)(q.a,{children:e.seriesNames().map((function(t){return Object(o.jsx)(W.a,{value:e.totalRow()[t.key]})}))})})},table:function(t){var e=t.resultSet,n=t.pivotConfig;return Object(o.jsx)(U.a,{pagination:!1,columns:e.tableColumns(n),dataSource:e.tablePivot(n)})}},st=n(762),it=n(158),ot=n(159),ct=n(754),lt=n(193),ut=n(257),dt=n(756),pt=n(440),mt=n(757),ht=n(442),ft=n(758),bt=n(443),gt=n(764),xt=n(444),jt=n(252),yt=function(t){var e=t.resultSet,n=t.children,r=t.ChartComponent;return Object(o.jsx)(st.a,{width:"100%",height:350,children:Object(o.jsxs)(r,{data:e.chartPivot(),children:[Object(o.jsx)(it.a,{dataKey:"x"}),Object(o.jsx)(ot.a,{}),Object(o.jsx)(ct.a,{}),n,Object(o.jsx)(lt.a,{}),Object(o.jsx)(ut.a,{})]})})},Ct=["#FF6492","#141446","#7A77FF"],vt={line:function(t){var e=t.resultSet;return Object(o.jsx)(yt,{resultSet:e,ChartComponent:dt.a,children:e.seriesNames().map((function(t,e){return Object(o.jsx)(pt.a,{stackId:"a",dataKey:t.key,name:t.title,stroke:Ct[e]},t.key)}))})},bar:function(t){var e=t.resultSet;return Object(o.jsx)(yt,{resultSet:e,ChartComponent:mt.a,children:e.seriesNames().map((function(t,e){return Object(o.jsx)(ht.a,{stackId:"a",dataKey:t.key,name:t.title,fill:Ct[e]},t.key)}))})},area:function(t){var e=t.resultSet;return Object(o.jsx)(yt,{resultSet:e,ChartComponent:ft.a,children:e.seriesNames().map((function(t,e){return Object(o.jsx)(bt.a,{stackId:"a",dataKey:t.key,name:t.title,stroke:Ct[e],fill:Ct[e]},t.key)}))})},pie:function(t){var e=t.resultSet;return Object(o.jsx)(st.a,{width:"100%",height:350,children:Object(o.jsxs)(gt.a,{children:[Object(o.jsx)(xt.a,{isAnimationActive:!1,data:e.chartPivot(),nameKey:"x",dataKey:e.seriesNames()[0].key,fill:"#8884d8",children:e.chartPivot().map((function(t,e){return Object(o.jsx)(jt.a,{fill:Ct[e%Ct.length]},e)}))}),Object(o.jsx)(lt.a,{}),Object(o.jsx)(ut.a,{})]})})},number:function(t){var e=t.resultSet;return Object(o.jsx)(K.a,{type:"flex",justify:"center",align:"middle",style:{height:"100%"},children:Object(o.jsx)(q.a,{children:e.seriesNames().map((function(t){return Object(o.jsx)(W.a,{value:e.totalRow()[t.key]})}))})})},table:function(t){var e=t.resultSet,n=t.pivotConfig;return Object(o.jsx)(U.a,{pagination:!1,columns:e.tableColumns(n),dataSource:e.tablePivot(n)})}};window.__cubejsPlayground={getCodesandboxFiles:function(t,e){var n=e.query,r=e.pivotConfig,a=e.chartType,s=e.cubejsToken,i=e.apiUrl,o=E["".concat(t,"Charts")],c=o.getCommon,l=o.getImports,u=o.getChartComponent;return{"index.js":"import ReactDOM from 'react-dom';\nimport cubejs from '@cubejs-client/core';\nimport { QueryRenderer } from '@cubejs-client/react';\nimport { Spin } from 'antd';\nimport 'antd/dist/antd.css';\n".concat(l().join("\n"),"\n\n").concat(c(),"\n\nconst cubejsApi = cubejs(\n  '").concat(s,"',\n  { apiUrl: '").concat(i,"' }\n);\n\nconst renderChart = ({ resultSet, error, pivotConfig }) => {\n  if (error) {\n    return <div>{error.toString()}</div>;\n  }\n\n  if (!resultSet) {\n    return <Spin />;\n  }\n\n  ").concat(u(a),"\n};\n\nconst ChartRenderer = () => {\n  return (\n    <QueryRenderer\n      query={").concat(n,"}\n      cubejsApi={cubejsApi}\n      resetResultSetOnChange={false}\n      render={(props) => renderChart({\n        ...props,\n        pivotConfig: ").concat(r,"\n      })}\n    />\n  );\n};\n\nconst rootElement = document.getElementById('root');\nReactDOM.render(<ChartRenderer />, rootElement);\n      ")}},getDependencies:function(t){var e=E["".concat(t,"Charts")].getImports;return[].concat(T,Object(m.a)(e().map((function(t){var e=t.match(/['"]([^'"]+)['"]/);return Object(p.a)(e,2)[1]}))))}};var St={d3:Y,bizcharts:tt,chartjs:at,recharts:vt},Ot=function(){var t,e=Object(c.useState)(null),n=Object(p.a)(e,2),r=n[0],a=n[1],s=Object(c.useState)(null),i=Object(p.a)(s,2),l=i[0],u=i[1],d=Object(c.useState)(null),m=Object(p.a)(d,2),h=m[0],f=m[1],b=Object(c.useState)(null),g=Object(p.a)(b,2),x=g[0],j=g[1];return Object(c.useEffect)((function(){var t=(window.parent.window.__cubejsPlayground||{}).onChartRendererReady;"function"===typeof t&&t()}),[]),Object(c.useLayoutEffect)((function(){window.addEventListener("cubejs",(function(t){var e=t.detail,n=e.query,r=e.chartingLibrary,s=e.chartType,i=e.pivotConfig;n&&a(n),i&&u(i),r&&f(r),s&&("bizcharts"===r?(j(null),setTimeout((function(){return j(s)}),0)):j(s))}))}),[]),Object(o.jsx)("div",{className:"App",children:(null===(t=St[h])||void 0===t?void 0:t[x])?Object(o.jsx)(V,{renderFunction:St[h][x],query:r,pivotConfig:l}):null})},kt=function(t){t&&t instanceof Function&&n.e(3).then(n.bind(null,768)).then((function(e){var n=e.getCLS,r=e.getFID,a=e.getFCP,s=e.getLCP,i=e.getTTFB;n(t),r(t),a(t),s(t),i(t)}))};d.a.render(Object(o.jsx)(l.a.StrictMode,{children:Object(o.jsx)(Ot,{})}),document.getElementById("root")),kt()}},[[748,1,2]]]);