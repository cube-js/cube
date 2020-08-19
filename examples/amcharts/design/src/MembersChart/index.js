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
      color: "#FF6492"
    },
    {
      x: "date",
      y: "joins",
      color: "#FFA2BE"
    }
  ];

  return (
    <div className={styles.root}>
      <h2>
        <span className={styles.members}>All members</span> and{' '}
        <span className={styles.joins}>new members</span>
      </h2>
      <LineChart data={data} options={options} />
    </div>
  )
}

MembersChart.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired
}