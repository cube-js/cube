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

const customShape = (shape, resultSet) => {
  if (shape) {
    return React.cloneElement(shape, { resultSet: resultSet })
  }

  return null
}

const BarComponent = ({ resultSet, label, margin, shape }) => {
  return (
  <ResponsiveContainer>
    <BarChart margin={margin} data={format("x", resultSet.chartPivot(), resolveFormat(resultSet))}>
      <CartesianGrid strokeDasharray="3 3"/>
      <XAxis dataKey="x" minTickGap={20}/>
      <YAxis/>
      <Tooltip/>
      {extractSeries(resultSet).map((s, i) =>
        <Bar label={label} key={i} dataKey={s} shape={customShape(shape, resultSet)} name={humanName(resultSet, s)} stackId="a" fill={COLORS[i % COLORS.length]} />
      )}
      <Legend />
    </BarChart>
  </ResponsiveContainer>
  )
};

export default BarComponent;
