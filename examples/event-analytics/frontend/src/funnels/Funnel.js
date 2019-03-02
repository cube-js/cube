import React from 'react';
import Chart from '../components/Charts';

const Funnel = ({ id }) => (
  <Chart
    type="bar"
    query={{
      measures: [`${id}Funnel.conversions`],
      dimensions: [`${id}Funnel.step`]
    }}
  />
)

export default Funnel;
