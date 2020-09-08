import React from 'react';
import styles from './styles.module.css';
import LineChart from "../amCharts/LineChart"
import PropTypes from "prop-types"

export default function MembersChart(props) {
  const { data } = props

  const options = [
    {
      x: "date",
      y: "members",
      color: "#7A77FF"
    },
    {
      x: "date",
      y: "joins",
      color: "#AFADFF"
    }
  ];

  return (
    <div className={styles.root}>
      <h2>
        <span className={styles.members}>All members</span> and{' '}
        <span className={styles.joins}>new members</span>
      </h2>
      <div className={styles.chart}>
        <LineChart data={data} options={options} />
      </div>
    </div>
  )
}

MembersChart.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired
}