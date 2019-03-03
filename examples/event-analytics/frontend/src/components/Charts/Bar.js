import React from 'react';

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend
} from 'recharts';

import {
  format,
  COLORS,
  extractSeries,
  humanName,
} from './helpers.js';
import ResponsiveContainer from './ResponsiveContainer.js';

// for default chart pivot config
const resolveFormat = (resultSet) => {
  const query = resultSet.loadResponse.query;
  const annotation = resultSet.loadResponse.annotation;
  const timeDimensions = (query.timeDimensions || []).filter(td => !!td.granularity);
  if (timeDimensions.length) {
    return `time:${timeDimensions[0].granularity}`
  } else if (query.dimensions.length) {
    return annotation.dimensions[query.dimensions[0]].type
  } else {
    return undefined
  }
}

export default ({ resultSet, label }) => {
  return (
  <ResponsiveContainer>
    <BarChart margin={{ top: 20 }} data={format("x", resultSet.chartPivot(), resolveFormat(resultSet))}>
      <CartesianGrid strokeDasharray="3 3"/>
      <XAxis dataKey="x" minTickGap={20}/>
      <YAxis/>
      <Tooltip/>
      {extractSeries(resultSet).map((s, i) =>
        <Bar label={label} key={i} dataKey={s} name={humanName(resultSet, s)} stackId="a" fill={COLORS[i % COLORS.length]} />
      )}
      <Legend />
    </BarChart>
  </ResponsiveContainer>
  )
}
