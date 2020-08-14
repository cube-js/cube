import React from 'react';
import PropTypes from 'prop-types';
import styles from './styles.module.css';
import logo from "./logo.svg"

export default function Header(props) {
  const { onClick } = props;

  return (
    <div className={styles.root}>
      <div className={styles.logo}>
        <h1>
          <span className={styles.name} onClick={onClick}>
            Slack Vibe&nbsp;&nbsp;<span role='img' aria-label=''>🎉</span>
          </span>
          <span className={styles.attribution}>by</span>
          <span><img src={logo} alt='Cube.js' /></span>
        </h1>
        <div className={styles.description}>An&nbsp;open source dashboard which visualizes public activity in
          a&nbsp;Slack workspace of an&nbsp;open community or a&nbsp;private team
        </div>
      </div>
    </div>
  )
}

Header.propTypes = {
  onClick: PropTypes.func.isRequired,
};