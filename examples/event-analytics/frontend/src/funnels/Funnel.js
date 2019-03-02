import React from 'react';
import Chart from '../components/Charts';

const Funnel = ({ id }) => (
  <Chart
    type="bar"
    options={{label: { position: 'top' }}}
    query={{
      measures: [`${id}.conversions`],
      dimensions: [`${id}.step`]
    }}
  />
)

export default Funnel;
