import React from 'react';

import { PieChart, Pie, Tooltip, Legend, Cell } from 'recharts';

import { formatters, format, COLORS } from './helpers.js';
import ResponsiveContainer from './ResponsiveContainer.js';

const dimensionOnX = (resultSet) => (
  resultSet.loadResponse.query.dimensions[0] ||
  (resultSet.loadResponse.query.timeDimensions[0] &&
    resultSet.loadResponse.query.timeDimensions[0].dimension)
)

const anyDimensions = (resultSet) => (
  resultSet.loadResponse.query.dimensions.length > 0 ||
    resultSet.loadResponse.query.timeDimensions.length > 0
)

const piePivot = (resultSet) => {
  if (anyDimensions(resultSet)) {
    return resultSet.chartPivot({
      x: [dimensionOnX(resultSet)],
      y: ["measures"],
      fillMissingDates: false
    })
  } else {
    return resultSet.chartPivot()
  }
}

const findFormat = (resultSet, dimension) => {
  const dim = resultSet.loadResponse.annotation.dimensions[dimension] ||
    resultSet.loadResponse.annotation.timeDimensions[dimension]
  return dim.type
}

export default ({ resultSet }) => {
  return (
  <ResponsiveContainer>
    <PieChart>
      <Pie
        isAnimationActive={false}
        data={format("x", piePivot(resultSet), findFormat(resultSet, dimensionOnX(resultSet)))}
        nameKey="x"
        dataKey={Object.keys(resultSet.loadResponse.annotation.measures)[0]}
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
}
