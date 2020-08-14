import React from 'react';
import styles from './styles.module.css';
import LineChart from "../amCharts/LineChart"
import PropTypes from "prop-types"

export default function MessagesChart(props) {
  const { data, granularity } = props

  const options = [
    {
      x: "date",
      y: "messages",
      color: "rgb(29, 155, 209)",
      granularity,
    },
    {
      x: "date",
      y: "reactions",
      color: "rgb(0, 122, 90)",
      granularity,
    }
  ];

  return (
    <div className={styles.root}>
      <h2>
        <span className={styles.messages}>Messages</span> and <span className={styles.reactions}>reactions</span>
      </h2>
      <LineChart data={data} options={options} />
    </div>
  )
}

MessagesChart.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
  granularity: PropTypes.string.isRequired,
}