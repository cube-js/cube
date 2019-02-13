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

import { format, COLORS } from './helpers.js';
import ResponsiveContainer from './ResponsiveContainer.js';

export default ({ resultSet }) => {
  debugger
  return (
  <ResponsiveContainer>
    <BarChart data={format("x", resultSet.chartPivot(), 'date')}>
      <CartesianGrid strokeDasharray="3 3"/>
      <XAxis dataKey="x"/>
      <YAxis/>
      <Tooltip/>
      {Object.keys(resultSet.chartPivot()[0]).filter((s) => !["category", "x"].includes(s)).map((s, i) =>
        <Bar key={i} dataKey={s} name={s} stackId="a" fill={COLORS[i % COLORS.length]} />
      )}
      <Legend />
    </BarChart>
  </ResponsiveContainer>
  )
}
