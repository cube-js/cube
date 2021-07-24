(this["webpackJsonpreact-charts"]=this["webpackJsonpreact-charts"]||[]).push([[0],{656:function(e,t,n){},803:function(e,t,n){},944:function(e,t,n){"use strict";n.r(t);var r={};n.r(r),n.d(r,"getChartComponent",(function(){return b})),n.d(r,"getCommon",(function(){return C})),n.d(r,"getImports",(function(){return g}));var a={};n.r(a),n.d(a,"getChartComponent",(function(){return x})),n.d(a,"getCommon",(function(){return S})),n.d(a,"getImports",(function(){return y}));var i={};n.r(i),n.d(i,"getChartComponent",(function(){return O})),n.d(i,"getCommon",(function(){return k})),n.d(i,"getImports",(function(){return R}));var s={};n.r(s),n.d(s,"getChartComponent",(function(){return w})),n.d(s,"getCommon",(function(){return F})),n.d(s,"getImports",(function(){return E}));var o=n(0),l=n.n(o),u=n(33),c=n.n(u),d=(n(656),n(20)),p=n(553),f=n(279),m=(n(802),n(803),n(59)),h=["import React from 'react';","import { Chart, Axis, Tooltip, Geom, PieChart } from 'bizcharts';","import { Row, Col, Statistic, Table } from 'antd';","import { useDeepCompareMemo } from 'use-deep-compare';"];function b(e){return"line"===e?"return <LineChartRenderer resultSet={resultSet} />;\n":"bar"===e?"return <BarChartRenderer resultSet={resultSet} />;\n":"area"===e?"return <AreaChartRenderer resultSet={resultSet} />;\n":"pie"===e?"return <PieChartRenderer resultSet={resultSet} />;\n":"number"===e?'return (\n  <Row\n    type="flex"\n    justify="center"\n    align="middle"\n    style={{\n      height: \'100%\',\n    }}\n  >\n    <Col>\n      {resultSet.seriesNames().map((s) => (\n        <Statistic value={resultSet.totalRow()[s.key]} />\n      ))}\n    </Col>\n  </Row>\n);\n':"height"===e?"":"table"===e?"return <TableRenderer resultSet={resultSet} pivotConfig={pivotConfig} />;\n":void 0}function C(){return'const stackedChartData = (resultSet) => {\n  const data = resultSet\n    .pivot()\n    .map(({ xValues, yValuesArray }) =>\n      yValuesArray.map(([yValues, m]) => ({\n        x: resultSet.axisValuesString(xValues, \', \'),\n        color: resultSet.axisValuesString(yValues, \', \'),\n        measure: m && Number.parseFloat(m),\n      }))\n    )\n    .reduce((a, b) => a.concat(b), []);\n  return data;\n};\n\nconst LineChartRenderer = ({ resultSet }) => {\n  const data = useDeepCompareMemo(\n    () => stackedChartData(resultSet),\n    [resultSet]\n  );\n  return (\n    <Chart\n      scale={{\n        x: {\n          tickCount: 8,\n        },\n      }}\n      autoFit\n      height={400}\n      data={data}\n      forceFit\n    >\n      <Axis name="x" />\n      <Axis name="measure" />\n      <Tooltip\n        crosshairs={{\n          type: \'y\',\n        }}\n      />\n      <Geom type="line" position="x*measure" size={2} color="color" />\n    </Chart>\n  );\n};\n\nconst BarChartRenderer = ({ resultSet }) => {\n  const data = useDeepCompareMemo(\n    () => stackedChartData(resultSet),\n    [resultSet.serialize()]\n  );\n  return (\n    <Chart\n      scale={{\n        x: {\n          tickCount: 8,\n        },\n      }}\n      autoFit\n      height={400}\n      data={data}\n      forceFit\n    >\n      <Axis name="x" />\n      <Axis name="measure" />\n      <Tooltip />\n      <Geom type="interval" position="x*measure" color="color" />\n    </Chart>\n  );\n};\n\nconst AreaChartRenderer = ({ resultSet }) => {\n  const data = useDeepCompareMemo(\n    () => stackedChartData(resultSet),\n    [resultSet.serialize()]\n  );\n  return (\n    <Chart\n      scale={{\n        x: {\n          tickCount: 8,\n        },\n      }}\n      autoFit\n      height={400}\n      data={data}\n      forceFit\n    >\n      <Axis name="x" />\n      <Axis name="measure" />\n      <Tooltip\n        crosshairs={{\n          type: \'y\',\n        }}\n      />\n      <Geom\n        type="area"\n        adjust="stack"\n        position="x*measure"\n        size={2}\n        color="color"\n      />\n    </Chart>\n  );\n};\n\nconst PieChartRenderer = ({ resultSet }) => {\n  const [data, angleField] = useDeepCompareMemo(() => {\n    return [resultSet.chartPivot(), resultSet.series()];\n  }, [resultSet]);\n  return (\n    <PieChart\n      data={data}\n      radius={0.8}\n      angleField={angleField[0].key}\n      colorField="x"\n      label={{\n        visible: true,\n        offset: 20,\n      }}\n      legend={{\n        position: \'bottom\',\n      }}\n    />\n  );\n};\n\nconst TableRenderer = ({ resultSet, pivotConfig }) => {\n  const [tableColumns, dataSource] = useDeepCompareMemo(() => {\n    return [\n      resultSet.tableColumns(pivotConfig),\n      resultSet.tablePivot(pivotConfig),\n    ];\n  }, [resultSet, pivotConfig]);\n  return (\n    <Table pagination={false} columns={tableColumns} dataSource={dataSource} />\n  );\n};\n'}function g(){return h}var j=["import React from 'react';","import { CartesianGrid, PieChart, Pie, Cell, AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend, BarChart, Bar, LineChart, Line } from 'recharts';","import { Row, Col, Statistic, Table } from 'antd';"];function x(e){return"line"===e?'return (\n  <CartesianChart resultSet={resultSet} ChartComponent={LineChart}>\n    {resultSet.seriesNames().map((series, i) => (\n      <Line\n        key={series.key}\n        stackId="a"\n        dataKey={series.key}\n        name={series.title}\n        stroke={colors[i]}\n      />\n    ))}\n  </CartesianChart>\n);\n':"bar"===e?'return (\n  <CartesianChart resultSet={resultSet} ChartComponent={BarChart}>\n    {resultSet.seriesNames().map((series, i) => (\n      <Bar\n        key={series.key}\n        stackId="a"\n        dataKey={series.key}\n        name={series.title}\n        fill={colors[i]}\n      />\n    ))}\n  </CartesianChart>\n);\n':"area"===e?'return (\n  <CartesianChart resultSet={resultSet} ChartComponent={AreaChart}>\n    {resultSet.seriesNames().map((series, i) => (\n      <Area\n        key={series.key}\n        stackId="a"\n        dataKey={series.key}\n        name={series.title}\n        stroke={colors[i]}\n        fill={colors[i]}\n      />\n    ))}\n  </CartesianChart>\n);\n':"pie"===e?'return (\n  <ResponsiveContainer width="100%" height={350}>\n    <PieChart>\n      <Pie\n        isAnimationActive={false}\n        data={resultSet.chartPivot()}\n        nameKey="x"\n        dataKey={resultSet.seriesNames()[0].key}\n        fill="#8884d8"\n      >\n        {resultSet.chartPivot().map((e, index) => (\n          <Cell key={index} fill={colors[index % colors.length]} />\n        ))}\n      </Pie>\n      <Legend />\n      <Tooltip />\n    </PieChart>\n  </ResponsiveContainer>\n);\n':"number"===e?'return (\n  <Row\n    type="flex"\n    justify="center"\n    align="middle"\n    style={{\n      height: \'100%\',\n    }}\n  >\n    <Col>\n      {resultSet.seriesNames().map((s) => (\n        <Statistic value={resultSet.totalRow()[s.key]} />\n      ))}\n    </Col>\n  </Row>\n);\n':"height"===e?"":"table"===e?"return (\n  <Table\n    pagination={false}\n    columns={resultSet.tableColumns(pivotConfig)}\n    dataSource={resultSet.tablePivot(pivotConfig)}\n  />\n);\n":void 0}function S(){return"const CartesianChart = ({ resultSet, children, ChartComponent }) => (\n  <ResponsiveContainer width=\"100%\" height={350}>\n    <ChartComponent data={resultSet.chartPivot()}>\n      <XAxis dataKey=\"x\" />\n      <YAxis />\n      <CartesianGrid />\n      {children}\n      <Legend />\n      <Tooltip />\n    </ChartComponent>\n  </ResponsiveContainer>\n);\n\nconst colors = ['#FF6492', '#141446', '#7A77FF'];\n\nconst stackedChartData = (resultSet) => {\n  const data = resultSet\n    .pivot()\n    .map(({ xValues, yValuesArray }) =>\n      yValuesArray.map(([yValues, m]) => ({\n        x: resultSet.axisValuesString(xValues, ', '),\n        color: resultSet.axisValuesString(yValues, ', '),\n        measure: m && Number.parseFloat(m),\n      }))\n    )\n    .reduce((a, b) => a.concat(b), []);\n  return data;\n};\n"}function y(){return j}var v=["import React from 'react';","import { Line, Bar, Pie } from 'react-chartjs-2';","import { useDeepCompareMemo } from 'use-deep-compare';","import { Row, Col, Statistic, Table } from 'antd';"];function O(e){return"line"===e?"return <LineChartRenderer resultSet={resultSet} />;\n":"bar"===e?"return <BarChartRenderer resultSet={resultSet} pivotConfig={pivotConfig} />;\n":"area"===e?"return <AreaChartRenderer resultSet={resultSet} />;\n":"pie"===e?'const data = {\n  labels: resultSet.categories().map((c) => c.x),\n  datasets: resultSet.series().map((s) => ({\n    label: s.title,\n    data: s.series.map((r) => r.value),\n    backgroundColor: COLORS_SERIES,\n    hoverBackgroundColor: COLORS_SERIES,\n  })),\n};\nreturn <Pie type="pie" data={data} options={commonOptions} />;\n':"labels"===e||"datasets"===e||"label"===e||"data"===e||"backgroundColor"===e||"hoverBackgroundColor"===e?"":"number"===e?'return (\n  <Row\n    type="flex"\n    justify="center"\n    align="middle"\n    style={{\n      height: \'100%\',\n    }}\n  >\n    <Col>\n      {resultSet.seriesNames().map((s) => (\n        <Statistic value={resultSet.totalRow()[s.key]} />\n      ))}\n    </Col>\n  </Row>\n);\n':"height"===e?"":"table"===e?"return (\n  <Table\n    pagination={false}\n    columns={resultSet.tableColumns(pivotConfig)}\n    dataSource={resultSet.tablePivot(pivotConfig)}\n  />\n);\n":void 0}function k(){return"const COLORS_SERIES = [\n  '#5b8ff9',\n  '#5ad8a6',\n  '#5e7092',\n  '#f6bd18',\n  '#6f5efa',\n  '#6ec8ec',\n  '#945fb9',\n  '#ff9845',\n  '#299796',\n  '#fe99c3',\n];\nconst PALE_COLORS_SERIES = [\n  '#d7e3fd',\n  '#daf5e9',\n  '#d6dbe4',\n  '#fdeecd',\n  '#dad8fe',\n  '#dbf1fa',\n  '#e4d7ed',\n  '#ffe5d2',\n  '#cce5e4',\n  '#ffe6f0',\n];\nconst commonOptions = {\n  maintainAspectRatio: false,\n  interaction: {\n    intersect: false,\n  },\n  plugins: {\n    legend: {\n      position: 'bottom',\n    },\n  },\n  scales: {\n    x: {\n      ticks: {\n        autoSkip: true,\n        maxRotation: 0,\n        padding: 12,\n        minRotation: 0,\n      },\n    },\n  },\n};\n\nconst LineChartRenderer = ({ resultSet }) => {\n  const datasets = useDeepCompareMemo(\n    () =>\n      resultSet.series().map((s, index) => ({\n        label: s.title,\n        data: s.series.map((r) => r.value),\n        borderColor: COLORS_SERIES[index],\n        pointRadius: 1,\n        tension: 0.1,\n        pointHoverRadius: 1,\n        borderWidth: 2,\n        tickWidth: 1,\n        fill: false,\n      })),\n    [resultSet]\n  );\n  const data = {\n    labels: resultSet.categories().map((c) => c.x),\n    datasets,\n  };\n  return <Line type=\"line\" data={data} options={commonOptions} />;\n};\n\nconst BarChartRenderer = ({ resultSet, pivotConfig }) => {\n  const datasets = useDeepCompareMemo(\n    () =>\n      resultSet.series().map((s, index) => ({\n        label: s.title,\n        data: s.series.map((r) => r.value),\n        backgroundColor: COLORS_SERIES[index],\n        fill: false,\n      })),\n    [resultSet]\n  );\n  const data = {\n    labels: resultSet.categories().map((c) => c.x),\n    datasets,\n  };\n  const options = {\n    ...commonOptions,\n    scales: {\n      x: {\n        ...commonOptions.scales.x,\n        stacked: !(pivotConfig.x || []).includes('measures'),\n      },\n    },\n  };\n  return <Bar type=\"bar\" data={data} options={options} />;\n};\n\nconst AreaChartRenderer = ({ resultSet }) => {\n  const datasets = useDeepCompareMemo(\n    () =>\n      resultSet.series().map((s, index) => ({\n        label: s.title,\n        data: s.series.map((r) => r.value),\n        pointRadius: 1,\n        pointHoverRadius: 1,\n        backgroundColor: PALE_COLORS_SERIES[index],\n        borderWidth: 0,\n        fill: true,\n        tension: 0,\n      })),\n    [resultSet]\n  );\n  const data = {\n    labels: resultSet.categories().map((c) => c.x),\n    datasets,\n  };\n  const options = {\n    ...commonOptions,\n    scales: {\n      ...commonOptions.scales,\n      y: {\n        stacked: true,\n      },\n    },\n  };\n  return <Line type=\"area\" data={data} options={options} />;\n};\n"}function R(){return v}var A=["import React from 'react';","import * as d3 from 'd3';","import { Row, Col, Statistic, Table } from 'antd';"];function w(e){return"line"===e?'return <D3Chart type="line" {...props} />;\n':"bar"===e?'return <D3Chart type="bar" {...props} />;\n':"area"===e?'return <D3Chart type="area" {...props} />;\n':"pie"===e?'return <D3Chart type="pie" {...props} />;\n':"number"===e?'return (\n  <Row\n    type="flex"\n    justify="center"\n    align="middle"\n    style={{\n      height: \'100%\',\n    }}\n  >\n    <Col>\n      {resultSet.seriesNames().map((s) => (\n        <Statistic value={resultSet.totalRow()[s.key]} />\n      ))}\n    </Col>\n  </Row>\n);\n':"height"===e?"":"table"===e?"return (\n  <Table\n    pagination={false}\n    columns={resultSet.tableColumns(pivotConfig)}\n    dataSource={resultSet.tablePivot(pivotConfig)}\n  />\n);\n":void 0}function F(){return"const COLORS_SERIES = [\n  '#7A77FF',\n  '#141446',\n  '#FF6492',\n  '#727290',\n  '#43436B',\n  '#BEF3BE',\n  '#68B68C',\n  '#FFE7AA',\n  '#B2A58D',\n  '#64C8E0',\n];\nconst CHART_HEIGHT = 300;\n\nconst drawPieChart = (node, resultSet, options) => {\n  const data = resultSet.series()[0].series.map((s) => s.value);\n  const data_ready = d3.pie()(data);\n  d3.select(node).html(''); // The radius of the pieplot is half the width or half the height (smallest one).\n\n  const radius = CHART_HEIGHT / 2 - 40; // Seprate container to center align pie chart\n\n  const svg = d3\n    .select(node)\n    .append('svg')\n    .attr('width', node.clientWidth)\n    .attr('height', CHART_HEIGHT)\n    .append('g')\n    .attr(\n      'transform',\n      'translate(' + node.clientWidth / 2 + ',' + CHART_HEIGHT / 2 + ')'\n    );\n  svg\n    .selectAll('pieArcs')\n    .data(data_ready)\n    .enter()\n    .append('path')\n    .attr('d', d3.arc().innerRadius(0).outerRadius(radius))\n    .attr('fill', (d) => COLORS_SERIES[d.index]);\n  const size = 12;\n  const labels = resultSet.series()[0].series.map((s) => s.x);\n  svg\n    .selectAll('myrect')\n    .data(labels)\n    .enter()\n    .append('rect')\n    .attr('x', 150)\n    .attr('y', function (d, i) {\n      return -50 + i * (size + 5);\n    })\n    .attr('width', size)\n    .attr('height', size)\n    .style('fill', (d, i) => COLORS_SERIES[i]);\n  svg\n    .selectAll('mylabels')\n    .data(labels)\n    .enter()\n    .append('text')\n    .attr('x', 150 + size * 1.2)\n    .attr('y', function (d, i) {\n      return -50 + i * (size + 5) + size / 2;\n    })\n    .text(function (d) {\n      return d;\n    })\n    .attr('text-anchor', 'left')\n    .attr('font-size', '12px')\n    .style('alignment-baseline', 'middle');\n};\n\nconst drawChart = (node, resultSet, chartType, options = {}) => {\n  if (chartType === 'pie') {\n    return drawPieChart(node, resultSet, options);\n  }\n\n  const margin = {\n      top: 10,\n      right: 30,\n      bottom: 30,\n      left: 60,\n    },\n    width = node.clientWidth - margin.left - margin.right,\n    height = CHART_HEIGHT - margin.top - margin.bottom;\n  d3.select(node).html('');\n  const svg = d3\n    .select(node)\n    .append('svg')\n    .attr('width', width + margin.left + margin.right)\n    .attr('height', height + margin.top + margin.bottom)\n    .append('g')\n    .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');\n  const keys = resultSet.seriesNames(options.pivotConfig).map((s) => s.key);\n  let data, maxData;\n\n  if (chartType === 'line') {\n    data = resultSet.series(options.pivotConfig).map((series) => ({\n      key: series.key,\n      values: series.series,\n    }));\n    maxData = d3.max(data.map((s) => d3.max(s.values, (i) => i.value)));\n  } else {\n    data = d3.stack().keys(keys)(resultSet.chartPivot(options.pivotConfig));\n    maxData = d3.max(data.map((s) => d3.max(s, (i) => i[1])));\n  }\n\n  const color = d3.scaleOrdinal().domain(keys).range(COLORS_SERIES);\n  let x;\n\n  if (chartType === 'bar') {\n    x = d3\n      .scaleBand()\n      .range([0, width])\n      .domain(resultSet.chartPivot(options.pivotConfig).map((c) => c.x))\n      .padding(0.3);\n  } else {\n    x = d3\n      .scaleTime()\n      .domain(\n        d3.extent(resultSet.chartPivot(options.pivotConfig), (c) =>\n          d3.isoParse(c.x)\n        )\n      )\n      .nice()\n      .range([0, width]);\n  }\n\n  svg\n    .append('g')\n    .attr('transform', 'translate(0,' + height + ')')\n    .call(d3.axisBottom(x));\n  const y = d3.scaleLinear().domain([0, maxData]).range([height, 0]);\n  svg.append('g').call(d3.axisLeft(y));\n\n  if (chartType === 'line') {\n    svg\n      .selectAll('.line')\n      .data(data)\n      .enter()\n      .append('path')\n      .attr('fill', 'none')\n      .attr('stroke', (d) => color(d.key))\n      .attr('stroke-width', 1.5)\n      .attr('d', (d) => {\n        return d3\n          .line()\n          .x((d) => x(d3.isoParse(d.x)))\n          .y((d) => y(+d.value))(d.values);\n      });\n  } else if (chartType === 'area') {\n    svg\n      .selectAll('mylayers')\n      .data(data)\n      .enter()\n      .append('path')\n      .style('fill', (d) => color(d.key))\n      .attr(\n        'd',\n        d3\n          .area()\n          .x((d) => x(d3.isoParse(d.data.x)))\n          .y0((d) => y(d[0]))\n          .y1((d) => y(d[1]))\n      );\n  } else {\n    svg\n      .append('g')\n      .selectAll('g') // Enter in the stack data = loop key per key = group per group\n      .data(data)\n      .enter()\n      .append('g')\n      .attr('fill', (d) => color(d.key))\n      .selectAll('rect') // enter a second time = loop subgroup per subgroup to add all rectangles\n      .data((d) => d)\n      .enter()\n      .append('rect')\n      .attr('x', (d) => x(d.data.x))\n      .attr('y', (d) => y(d[1]))\n      .attr('height', (d) => y(d[0]) - y(d[1]))\n      .attr('width', x.bandwidth());\n  }\n};\n\nconst D3Chart = ({ resultSet, type, ...props }) => (\n  <div ref={(el) => el && drawChart(el, resultSet, type, props)} />\n);\n"}function E(){return A}var P={bizchartsCharts:r,rechartsCharts:a,chartjsCharts:i,d3Charts:s},T=["react-dom","@cubejs-client/core","@cubejs-client/react","antd"];var L=n(7),I=function(e){var t=e.queryId,n=e.renderFunction,r=e.query,a=e.pivotConfig,i=e.refetchCounter,s=(0,(window.parent.window.__cubejsPlayground||{}).forQuery)(t),l=s.onQueryStart,u=s.onQueryLoad,c=s.onQueryProgress,d=Object(f.b)(r),p=d.isLoading,m=d.error,h=d.resultSet,b=d.progress,C=d.refetch;return Object(o.useEffect)((function(){p&&"function"===typeof l&&l(t)}),[p]),Object(o.useEffect)((function(){i>0&&C()}),[i]),Object(o.useEffect)((function(){p||"function"!==typeof u||u({resultSet:h,error:m}),"function"===typeof c&&c(b)}),[m,p,h,b]),!h||m?null:n({resultSet:h,pivotConfig:a})},D=function(e){var t=e.queryId,n=e.renderFunction,r=e.query,a=e.pivotConfig,i=void 0===a?null:a,s=e.refetchCounter;return Object(L.jsx)(I,{queryId:t,renderFunction:n,query:r,pivotConfig:i,refetchCounter:s})},_=n(50),B=n(215),N=n(102),z=n(955),V=n(956),H=n(952),G=n(951),M=["#5b8ff9","#5ad8a6","#5e7092","#f6bd18","#6f5efa","#6ec8ec","#945fb9","#ff9845","#299796","#fe99c3"],K=["#d7e3fd","#daf5e9","#d6dbe4","#fdeecd","#dad8fe","#dbf1fa","#e4d7ed","#ffe5d2","#cce5e4","#ffe6f0"],W={maintainAspectRatio:!1,interaction:{intersect:!1},plugins:{legend:{position:"bottom"}},scales:{x:{ticks:{autoSkip:!0,maxRotation:0,padding:12,minRotation:0}}}},q=function(e){var t=e.resultSet,n=Object(N.a)((function(){return t.series().map((function(e,t){return{label:e.title,data:e.series.map((function(e){return e.value})),borderColor:M[t],pointRadius:1,tension:.1,pointHoverRadius:1,borderWidth:2,tickWidth:1,fill:!1}}))}),[t]),r={labels:t.categories().map((function(e){return e.x})),datasets:n};return Object(L.jsx)(B.b,{type:"line",data:r,options:W})},Q=function(e){var t=e.resultSet,n=e.pivotConfig,r=Object(N.a)((function(){return t.series().map((function(e,t){return{label:e.title,data:e.series.map((function(e){return e.value})),backgroundColor:M[t],fill:!1}}))}),[t]),a={labels:t.categories().map((function(e){return e.x})),datasets:r},i=Object(_.a)(Object(_.a)({},W),{},{scales:{x:Object(_.a)(Object(_.a)({},W.scales.x),{},{stacked:!(n.x||[]).includes("measures")})}});return Object(L.jsx)(B.a,{type:"bar",data:a,options:i})},U=function(e){var t=e.resultSet,n=Object(N.a)((function(){return t.series().map((function(e,t){return{label:e.title,data:e.series.map((function(e){return e.value})),pointRadius:1,pointHoverRadius:1,backgroundColor:K[t],borderWidth:0,fill:!0,tension:0}}))}),[t]),r={labels:t.categories().map((function(e){return e.x})),datasets:n},a=Object(_.a)(Object(_.a)({},W),{},{scales:Object(_.a)(Object(_.a)({},W.scales),{},{y:{stacked:!0}})});return Object(L.jsx)(B.b,{type:"area",data:r,options:a})},J={line:function(e){var t=e.resultSet;return Object(L.jsx)(q,{resultSet:t})},bar:function(e){var t=e.resultSet,n=e.pivotConfig;return Object(L.jsx)(Q,{resultSet:t,pivotConfig:n})},area:function(e){var t=e.resultSet;return Object(L.jsx)(U,{resultSet:t})},pie:function(e){var t=e.resultSet,n={labels:t.categories().map((function(e){return e.x})),datasets:t.series().map((function(e){return{label:e.title,data:e.series.map((function(e){return e.value})),backgroundColor:M,hoverBackgroundColor:M}}))};return Object(L.jsx)(B.c,{type:"pie",data:n,options:W})},number:function(e){var t=e.resultSet;return Object(L.jsx)(z.a,{type:"flex",justify:"center",align:"middle",style:{height:"100%"},children:Object(L.jsx)(V.a,{children:t.seriesNames().map((function(e){return Object(L.jsx)(H.a,{value:t.totalRow()[e.key]})}))})})},table:function(e){var t=e.resultSet,n=e.pivotConfig;return Object(L.jsx)(G.a,{pagination:!1,columns:t.tableColumns(n),dataSource:t.tablePivot(n)})}},X=n(61),Y=function(e){return e.pivot().map((function(t){var n=t.xValues;return t.yValuesArray.map((function(t){var r=Object(d.a)(t,2),a=r[0],i=r[1];return{x:e.axisValuesString(n,", "),color:e.axisValuesString(a,", "),measure:i&&Number.parseFloat(i)}}))})).reduce((function(e,t){return e.concat(t)}),[])},Z=function(e){var t=e.resultSet,n=Object(N.a)((function(){return Y(t)}),[t]);return Object(L.jsxs)(X.Chart,{scale:{x:{tickCount:8}},autoFit:!0,height:400,data:n,forceFit:!0,children:[Object(L.jsx)(X.Axis,{name:"x"}),Object(L.jsx)(X.Axis,{name:"measure"}),Object(L.jsx)(X.Tooltip,{crosshairs:{type:"y"}}),Object(L.jsx)(X.Geom,{type:"line",position:"x*measure",size:2,color:"color"})]})},$=function(e){var t=e.resultSet,n=Object(N.a)((function(){return Y(t)}),[t.serialize()]);return Object(L.jsxs)(X.Chart,{scale:{x:{tickCount:8}},autoFit:!0,height:400,data:n,forceFit:!0,children:[Object(L.jsx)(X.Axis,{name:"x"}),Object(L.jsx)(X.Axis,{name:"measure"}),Object(L.jsx)(X.Tooltip,{}),Object(L.jsx)(X.Geom,{type:"interval",position:"x*measure",color:"color"})]})},ee=function(e){var t=e.resultSet,n=Object(N.a)((function(){return Y(t)}),[t.serialize()]);return Object(L.jsxs)(X.Chart,{scale:{x:{tickCount:8}},autoFit:!0,height:400,data:n,forceFit:!0,children:[Object(L.jsx)(X.Axis,{name:"x"}),Object(L.jsx)(X.Axis,{name:"measure"}),Object(L.jsx)(X.Tooltip,{crosshairs:{type:"y"}}),Object(L.jsx)(X.Geom,{type:"area",adjust:"stack",position:"x*measure",size:2,color:"color"})]})},te=function(e){var t=e.resultSet,n=Object(N.a)((function(){return[t.chartPivot(),t.series()]}),[t]),r=Object(d.a)(n,2),a=r[0],i=r[1];return Object(L.jsx)(X.PieChart,{data:a,radius:.8,angleField:i[0].key,colorField:"x",label:{visible:!0,offset:20},legend:{position:"bottom"}})},ne=function(e){var t=e.resultSet,n=e.pivotConfig,r=Object(N.a)((function(){return[t.tableColumns(n),t.tablePivot(n)]}),[t,n]),a=Object(d.a)(r,2),i=a[0],s=a[1];return Object(L.jsx)(G.a,{pagination:!1,columns:i,dataSource:s})},re={line:function(e){var t=e.resultSet;return Object(L.jsx)(Z,{resultSet:t})},bar:function(e){var t=e.resultSet;return Object(L.jsx)($,{resultSet:t})},area:function(e){var t=e.resultSet;return Object(L.jsx)(ee,{resultSet:t})},pie:function(e){var t=e.resultSet;return Object(L.jsx)(te,{resultSet:t})},number:function(e){var t=e.resultSet;return Object(L.jsx)(z.a,{type:"flex",justify:"center",align:"middle",style:{height:"100%"},children:Object(L.jsx)(V.a,{children:t.seriesNames().map((function(e){return Object(L.jsx)(H.a,{value:t.totalRow()[e.key]})}))})})},table:function(e){var t=e.resultSet,n=e.pivotConfig;return Object(L.jsx)(ne,{resultSet:t,pivotConfig:n})}},ae=n(558),ie=n(42),se=["resultSet","type"],oe=["#7A77FF","#141446","#FF6492","#727290","#43436B","#BEF3BE","#68B68C","#FFE7AA","#B2A58D","#64C8E0"],le=300,ue=function(e,t,n){var r=t.series()[0].series.map((function(e){return e.value})),a=ie.i()(r);ie.n(e).html("");var i=ie.n(e).append("svg").attr("width",e.clientWidth).attr("height",le).append("g").attr("transform","translate("+e.clientWidth/2+",150)");i.selectAll("pieArcs").data(a).enter().append("path").attr("d",ie.a().innerRadius(0).outerRadius(110)).attr("fill",(function(e){return oe[e.index]}));var s=12,o=t.series()[0].series.map((function(e){return e.x}));i.selectAll("myrect").data(o).enter().append("rect").attr("x",150).attr("y",(function(e,t){return 17*t-50})).attr("width",s).attr("height",s).style("fill",(function(e,t){return oe[t]})),i.selectAll("mylabels").data(o).enter().append("text").attr("x",164.4).attr("y",(function(e,t){return 17*t-50+6})).text((function(e){return e})).attr("text-anchor","left").attr("font-size","12px").style("alignment-baseline","middle")},ce=function(e){var t=e.resultSet,n=e.type,r=Object(ae.a)(e,se);return Object(L.jsx)("div",{ref:function(e){return e&&function(e,t,n){var r=arguments.length>3&&void 0!==arguments[3]?arguments[3]:{};if("pie"===n)return ue(e,t);var a={top:10,right:30,bottom:30,left:60},i=e.clientWidth-a.left-a.right,s=le-a.top-a.bottom;ie.n(e).html("");var o,l,u=ie.n(e).append("svg").attr("width",i+a.left+a.right).attr("height",s+a.top+a.bottom).append("g").attr("transform","translate("+a.left+","+a.top+")"),c=t.seriesNames(r.pivotConfig).map((function(e){return e.key}));"line"===n?(o=t.series(r.pivotConfig).map((function(e){return{key:e.key,values:e.series}})),l=ie.h(o.map((function(e){return ie.h(e.values,(function(e){return e.value}))})))):(o=ie.o().keys(c)(t.chartPivot(r.pivotConfig)),l=ie.h(o.map((function(e){return ie.h(e,(function(e){return e[1]}))}))));var d,p=ie.l().domain(c).range(oe);d="bar"===n?ie.j().range([0,i]).domain(t.chartPivot(r.pivotConfig).map((function(e){return e.x}))).padding(.3):ie.m().domain(ie.e(t.chartPivot(r.pivotConfig),(function(e){return ie.f(e.x)}))).nice().range([0,i]),u.append("g").attr("transform","translate(0,"+s+")").call(ie.c(d));var f=ie.k().domain([0,l]).range([s,0]);u.append("g").call(ie.d(f)),"line"===n?u.selectAll(".line").data(o).enter().append("path").attr("fill","none").attr("stroke",(function(e){return p(e.key)})).attr("stroke-width",1.5).attr("d",(function(e){return ie.g().x((function(e){return d(ie.f(e.x))})).y((function(e){return f(+e.value)}))(e.values)})):"area"===n?u.selectAll("mylayers").data(o).enter().append("path").style("fill",(function(e){return p(e.key)})).attr("d",ie.b().x((function(e){return d(ie.f(e.data.x))})).y0((function(e){return f(e[0])})).y1((function(e){return f(e[1])}))):u.append("g").selectAll("g").data(o).enter().append("g").attr("fill",(function(e){return p(e.key)})).selectAll("rect").data((function(e){return e})).enter().append("rect").attr("x",(function(e){return d(e.data.x)})).attr("y",(function(e){return f(e[1])})).attr("height",(function(e){return f(e[0])-f(e[1])})).attr("width",d.bandwidth())}(e,t,n,r)}})},de={line:function(e){return Object(L.jsx)(ce,Object(_.a)({type:"line"},e))},bar:function(e){return Object(L.jsx)(ce,Object(_.a)({type:"bar"},e))},area:function(e){return Object(L.jsx)(ce,Object(_.a)({type:"area"},e))},pie:function(e){return Object(L.jsx)(ce,Object(_.a)({type:"pie"},e))},number:function(e){var t=e.resultSet;return Object(L.jsx)(z.a,{type:"flex",justify:"center",align:"middle",style:{height:"100%"},children:Object(L.jsx)(V.a,{children:t.seriesNames().map((function(e){return Object(L.jsx)(H.a,{value:t.totalRow()[e.key]})}))})})},table:function(e){var t=e.resultSet,n=e.pivotConfig;return Object(L.jsx)(G.a,{pagination:!1,columns:t.tableColumns(n),dataSource:t.tablePivot(n)})}},pe=n(51),fe=function(e){var t=e.resultSet,n=e.children,r=e.ChartComponent;return Object(L.jsx)(pe.l,{width:"100%",height:350,children:Object(L.jsxs)(r,{data:t.chartPivot(),children:[Object(L.jsx)(pe.n,{dataKey:"x"}),Object(L.jsx)(pe.o,{}),Object(L.jsx)(pe.e,{}),n,Object(L.jsx)(pe.g,{}),Object(L.jsx)(pe.m,{})]})})},me=["#FF6492","#141446","#7A77FF"],he={line:function(e){var t=e.resultSet;return Object(L.jsx)(fe,{resultSet:t,ChartComponent:pe.i,children:t.seriesNames().map((function(e,t){return Object(L.jsx)(pe.h,{stackId:"a",dataKey:e.key,name:e.title,stroke:me[t]},e.key)}))})},bar:function(e){var t=e.resultSet;return Object(L.jsx)(fe,{resultSet:t,ChartComponent:pe.d,children:t.seriesNames().map((function(e,t){return Object(L.jsx)(pe.c,{stackId:"a",dataKey:e.key,name:e.title,fill:me[t]},e.key)}))})},area:function(e){var t=e.resultSet;return Object(L.jsx)(fe,{resultSet:t,ChartComponent:pe.b,children:t.seriesNames().map((function(e,t){return Object(L.jsx)(pe.a,{stackId:"a",dataKey:e.key,name:e.title,stroke:me[t],fill:me[t]},e.key)}))})},pie:function(e){var t=e.resultSet;return Object(L.jsx)(pe.l,{width:"100%",height:350,children:Object(L.jsxs)(pe.k,{children:[Object(L.jsx)(pe.j,{isAnimationActive:!1,data:t.chartPivot(),nameKey:"x",dataKey:t.seriesNames()[0].key,fill:"#8884d8",children:t.chartPivot().map((function(e,t){return Object(L.jsx)(pe.f,{fill:me[t%me.length]},t)}))}),Object(L.jsx)(pe.g,{}),Object(L.jsx)(pe.m,{})]})})},number:function(e){var t=e.resultSet;return Object(L.jsx)(z.a,{type:"flex",justify:"center",align:"middle",style:{height:"100%"},children:Object(L.jsx)(V.a,{children:t.seriesNames().map((function(e){return Object(L.jsx)(H.a,{value:t.totalRow()[e.key]})}))})})},table:function(e){var t=e.resultSet,n=e.pivotConfig;return Object(L.jsx)(G.a,{pagination:!1,columns:t.tableColumns(n),dataSource:t.tablePivot(n)})}};window.__cubejsPlayground={getCodesandboxFiles:function(e,t){var n=t.query,r=t.pivotConfig,a=t.chartType,i=t.cubejsToken,s=t.apiUrl,o=P["".concat(e,"Charts")],l=o.getCommon,u=o.getImports,c=o.getChartComponent;return{"index.js":"import ReactDOM from 'react-dom';\nimport cubejs from '@cubejs-client/core';\nimport { QueryRenderer } from '@cubejs-client/react';\nimport { Spin } from 'antd';\nimport 'antd/dist/antd.css';\n".concat(u().join("\n"),"\n\n").concat(l(),"\n\nconst cubejsApi = cubejs(\n  '").concat(i,"',\n  { apiUrl: '").concat(s,"' }\n);\n\nconst renderChart = ({ resultSet, error, pivotConfig }) => {\n  if (error) {\n    return <div>{error.toString()}</div>;\n  }\n\n  if (!resultSet) {\n    return <Spin />;\n  }\n\n  ").concat(c(a),"\n};\n\nconst ChartRenderer = () => {\n  return (\n    <QueryRenderer\n      query={").concat(n,"}\n      cubejsApi={cubejsApi}\n      resetResultSetOnChange={false}\n      render={(props) => renderChart({\n        ...props,\n        chartType: '").concat(a,"',\n        pivotConfig: ").concat(r,"\n      })}\n    />\n  );\n};\n\nconst rootElement = document.getElementById('root');\nReactDOM.render(<ChartRenderer />, rootElement);\n      ")}},getDependencies:function(e){if(!e)throw new Error("`chartingLibrary` param is undefined");var t=P["".concat(e,"Charts")].getImports;return[].concat(T,Object(m.a)(t().map((function(e){var t=e.match(/['"]([^'"]+)['"]/);return Object(d.a)(t,2)[1]}))))}};var be={chartjs:J,bizcharts:re,d3:de,recharts:he},Ce=function(){var e,t=window.location.hash.replace(/#\\/,"").split("="),n=Object(d.a)(t,2),r=(n[0],n[1]),a=Object(o.useState)(null),i=Object(d.a)(a,2),s=i[0],l=i[1],u=Object(o.useState)(null),c=Object(d.a)(u,2),m=c[0],h=c[1],b=Object(o.useState)(null),C=Object(d.a)(b,2),g=C[0],j=C[1],x=Object(o.useState)(null),S=Object(d.a)(x,2),y=S[0],v=S[1],O=Object(o.useState)(0),k=Object(d.a)(O,2),R=k[0],A=k[1],w=Object(o.useState)(0),F=Object(d.a)(w,2),E=F[0],P=F[1],T=Object(o.useMemo)((function(){var e=window.parent.window.__cubejsPlayground||{};return Object(p.a)(e.token,{apiUrl:e.apiUrl})}),[R]);return Object(o.useEffect)((function(){var e=(window.parent.window.__cubejsPlayground||{}).forQuery;"function"===typeof e&&e(r).onChartRendererReady()}),[]),Object(o.useLayoutEffect)((function(){window.addEventListener("__cubejsPlaygroundEvent",(function(e){var t=e.detail,n=t.query,r=t.chartingLibrary,a=t.chartType,i=t.pivotConfig,s=t.eventType;"chart"===s?(n&&l(n),i&&h(i),r&&j(r),a&&("bizcharts"===r?(v(null),setTimeout((function(){return v(a)}),0)):v(a))):"credentials"===s?A((function(e){return e+1})):"refetch"===s&&P((function(e){return e+1}))}))}),[]),Object(L.jsx)(f.a,{cubejsApi:T,children:Object(L.jsx)("div",{className:"App",children:(null===(e=be[g])||void 0===e?void 0:e[y])?Object(L.jsx)(D,{queryId:r,renderFunction:be[g][y],query:s,pivotConfig:m,refetchCounter:E}):null})})},ge=function(e){e&&e instanceof Function&&n.e(3).then(n.bind(null,958)).then((function(t){var n=t.getCLS,r=t.getFID,a=t.getFCP,i=t.getLCP,s=t.getTTFB;n(e),r(e),a(e),i(e),s(e)}))};c.a.render(Object(L.jsx)(l.a.StrictMode,{children:Object(L.jsx)(Ce,{})}),document.getElementById("root")),ge()}},[[944,1,2]]]);