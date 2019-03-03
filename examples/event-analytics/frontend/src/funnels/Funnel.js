import React from 'react';
import Chart from '../components/Charts';

const calculateCompletionRate = (resultSet, id) => {
  const data = resultSet.rawData()
  const last = Number(data[data.length - 1][`${id}.conversions`])
  if (last === 0) { return 0 }

  const first = Number(data[0][`${id}.conversions`])

  return Math.round(last/first)
}

const Funnel = ({ query, dateRange }) => (
  <Chart
    type="bar"
    options={{label: { position: 'top' }}}
    query={query}
  />
)

export default Funnel;
