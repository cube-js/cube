import React from 'react';
import Pie from '../amCharts/Pie';
import PropTypes from 'prop-types';

export default function WeekChart(props) {
  const { data } = props;

  const options = [
    {
      x: 'date',
      y: 'members',
      color: 'rgb(127, 90, 200)',
    },
    {
      x: 'date',
      y: 'joins',
      color: 'rgb(240, 76, 88)',
    },
  ];

  return (
    <div>
      <h2>{props.title}</h2>
      <Pie data={data} options={options} />
    </div>
  );
}

WeekChart.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
};
