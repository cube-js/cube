import React from 'react';
import styles from './Header.module.css';
import logo from "./logo.svg"

export default function Header() {
  return (
    <div className={styles.root}>
      <div className={styles.logo}>
        <h1>
          <span>Slack Vibe&nbsp;&nbsp;<span role='img' aria-label=''>🎉</span></span>
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