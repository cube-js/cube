import React from 'react';

import { PieChart, Pie, Tooltip, Legend, Cell } from 'recharts';

import { formatters, format, COLORS } from './helpers.js';
import ResponsiveContainer from './ResponsiveContainer.js';

const dimensionOnX = (resultSet) => (
  resultSet.query().dimensions[0]
)

const anyDimensions = (resultSet) => (
  resultSet.query().dimensions.length > 0
)

const piePivot = (resultSet) => {
  if (anyDimensions(resultSet)) {
    return resultSet.chartPivot({
      x: [dimensionOnX(resultSet)],
      y: ["measures"],
      fillMissingDates: false
    })
  } else {
    return resultSet.chartPivot({ x: ['measures'], y: [] })
  }
}

const findFormat = (resultSet, dimension) => {
  if (anyDimensions(resultSet)) {
    return resultSet.loadResponse.annotation.dimensions[dimension].type
  }
  return undefined
}

const dataKey = (resultSet) => {
  if (anyDimensions(resultSet)) {
    return Object.keys(resultSet.loadResponse.annotation.measures)[0]
  }

  return ""
}

const PieComponent = ({ resultSet }) => {
  return (
  <ResponsiveContainer>
    <PieChart>
      <Pie
        isAnimationActive={false}
        data={format("x", piePivot(resultSet), findFormat(resultSet, dimensionOnX(resultSet)))}
        nameKey="x"
        dataKey={dataKey(resultSet)}
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
};

export default PieComponent;
