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
  extractSeries,
  humanName,
  resolveFormat,
  RECHARTS_RESPONSIVE_WIDTH
} from './helpers.js';

const LineComponent = ({ resultSet }) => (
  <ResponsiveContainer width={RECHARTS_RESPONSIVE_WIDTH} height={DASHBOARD_CHART_MIN_HEIGHT}>
    <LineChart data={format("x", resultSet.chartPivot(), resolveFormat(resultSet))}>
      <XAxis dataKey="x" minTickGap={20}/>
      <YAxis />
      <CartesianGrid strokeDasharray="3 3" />
      <Tooltip />
      <Legend />
      {extractSeries(resultSet).map((s, i) =>
        <Line key={i} dataKey={s} name={humanName(resultSet, s)} stroke={COLORS[i % COLORS.length]} />
      )}
    </LineChart>
  </ResponsiveContainer>
);

export default LineComponent;
