import React from 'react';
import styles from './styles.module.css';
import Map from '../amCharts/Map';
import PropTypes from 'prop-types';

export default function MapChart(props) {
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
      <h2>Members by time zone</h2>
      <Map data={data} options={options} />
    </div>
  );
}

MapChart.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
};
