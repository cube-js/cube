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
  resolveFormat
} from './helpers.js';
import ResponsiveContainer from './ResponsiveContainer.js';

export default ({ resultSet }) => {
  return (
  <ResponsiveContainer>
    <BarChart data={format("x", resultSet.chartPivot(), resolveFormat(resultSet))}>
      <CartesianGrid strokeDasharray="3 3"/>
      <XAxis dataKey="x" minTickGap={20}/>
      <YAxis/>
      <Tooltip/>
      {extractSeries(resultSet).map((s, i) =>
        <Bar key={i} dataKey={s} name={humanName(resultSet, s)} stackId="a" fill={COLORS[i % COLORS.length]} />
      )}
      <Legend />
    </BarChart>
  </ResponsiveContainer>
  )
}
