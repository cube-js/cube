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

import { formatters, COLORS } from './helpers.js';
import ResponsiveContainer from './ResponsiveContainer.js';

export default ({ resultSet }) => (
  <ResponsiveContainer>
    <BarChart data={resultSet.chartPivot()}>
      <CartesianGrid strokeDasharray="3 3"/>
      <XAxis dataKey="x"/>
      <YAxis/>
      <Tooltip/>
      {resultSet.seriesNames().map((s, i) =>
        <Bar key={i} dataKey={s.key} name={s.title} stackId="a" fill={COLORS[i % COLORS.length]} />
      )}
    </BarChart>
  </ResponsiveContainer>
);
