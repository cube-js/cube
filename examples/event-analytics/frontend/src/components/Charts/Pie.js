import React from 'react';

import { PieChart, Pie, Tooltip, Legend, Cell } from 'recharts';

import { formatters, COLORS } from './helpers.js';
import ResponsiveContainer from './ResponsiveContainer.js';

export default ({ resultSet }) => (
  <ResponsiveContainer>
    <PieChart>
      <Pie
        isAnimationActive={false}
        data={resultSet.chartPivot()}
        nameKey="x"
        dataKey={resultSet.seriesNames()[0].key}
        fill="#8884d8"
      >
      {
        resultSet.chartPivot().map((e, index) =>
          <Cell key={index} fill={COLORS[index % COLORS.length]}/>
        )
      }
      </Pie>
      <Legend />
      <Tooltip formatter={formatters.number} />
    </PieChart>
  </ResponsiveContainer>
)
