import React from 'react';
import PropTypes from 'prop-types';
import styles from './styles.module.css';

const options = {
  'Last Week by Hour': ['last week', 'hour'],
  'Last Week by Day': ['last week', 'day'],
  'Last Month by Day': ['last month', 'day'],
  'Last Month by Week': ['last month', 'week'],
  'Last Quarter by Day': ['last quarter', 'day'],
  'Last Quarter by Week': ['last quarter', 'week'],
  'Last Quarter by Month': ['last quarter', 'month'],
  'Last Year by Day': ['last year', 'day'],
  'Last Year by Week': ['last year', 'week'],
  'Last Year by Month': ['last year', 'month'],
  'Last 5 Years by Week': ['last 5 years', 'week'],
  'Last 5 Years by Month': ['last 5 years', 'month'],
  'Last 5 Years by Year': ['last 5 years', 'year'],
};

export default function PeriodAndGranularitySelector(props) {
  const { period, granularity, onSelect } = props;

  const value = Object
    .keys(options)
    .find(option => option.toLowerCase() === period + ' by ' + granularity)

  function onChange(value) {
    onSelect(options[value][0], options[value][1]);
  }

  return (
    <div className={styles.root}>
      Show data from
      <select value={value} onChange={event => onChange(event.target.value)}>
        {Object.keys(options).map(option => (
          <option key={option} value={option}>{option}</option>
        ))}
      </select>
    </div>
  );
}

PeriodAndGranularitySelector.propTypes = {
  period: PropTypes.string.isRequired,
  granularity: PropTypes.string.isRequired,
  onSelect: PropTypes.func.isRequired,
};