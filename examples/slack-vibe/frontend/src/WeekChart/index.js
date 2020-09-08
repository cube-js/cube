import React from 'react';
import styles from './styles.module.css';
import Heatmap from '../amCharts/Heatmap';
import PropTypes from 'prop-types';

export default function WeekChart(props) {
  const { data } = props;

  const options = {
    colors: {
      min: '#fff',
      max: '#46D89B',
    }
  };

  return (
    <div className={styles.root}>
      <h2 title='With respect to your local time zone'>
        <span className={styles.messages}>Messages</span> by day of week
      </h2>
      <div className={styles.chart}>
        <Heatmap data={data} options={options} />
      </div>
    </div>
  );
}

WeekChart.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
};
