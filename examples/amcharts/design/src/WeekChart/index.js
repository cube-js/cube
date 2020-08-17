import React from 'react';
import styles from './styles.module.css';
import Heatmap from '../amCharts/Heatmap';
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
    <div className={styles.root}>
      <h2>
        Messages by day of week <small>according to your local timezone</small>
      </h2>
      <Heatmap data={data} options={options} />
    </div>
  );
}

WeekChart.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
};
