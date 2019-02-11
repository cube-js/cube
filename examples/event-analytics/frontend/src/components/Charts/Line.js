import React from 'react';

import ResponsiveContainer from 'recharts/lib/component/ResponsiveContainer';
import LineChart from 'recharts/lib/chart/LineChart';
import Line from 'recharts/lib/cartesian/Line';
import XAxis from 'recharts/lib/cartesian/XAxis';
import YAxis from 'recharts/lib/cartesian/YAxis';
import CartesianGrid from 'recharts/lib/cartesian/CartesianGrid';
import Tooltip from 'recharts/lib/component/Tooltip';
import Legend from 'recharts/lib/component/Legend';

import {
  format,
  DASHBOARD_CHART_MIN_HEIGHT,
  COLORS,
  RECHARTS_RESPONSIVE_WIDTH
} from './helpers.js';

export default ({ resultSet }) => (
  <ResponsiveContainer width={RECHARTS_RESPONSIVE_WIDTH} height={DASHBOARD_CHART_MIN_HEIGHT}>
    <LineChart data={format("x", resultSet.chartPivot(), 'date')}>
      <XAxis dataKey="x" />
      <YAxis />
      <CartesianGrid vertical={false} strokeDasharray="3 3" />
      <Tooltip />
      <Legend />
      {resultSet.seriesNames().map((s, i) =>
        <Line key={i} dataKey={s.key} name={s.title} stroke={COLORS[i % COLORS.length]} />
      )}
    </LineChart>
  </ResponsiveContainer>
);
