import React from 'react';
import styles from './Banner.module.css';

export default function Banner() {
  return (
    <div className={styles.root}>
      <div className={styles.banner}>
        <p><em>Slave Vibe</em> is created and powered by <em>Cube.js</em>, an&nbsp;open source analytical
          data access layer for modern web applications.</p>
        <p>Learn more at <a href='https://cube.dev' target='_blank' rel='noopener noreferrer'>cube.dev</a> and <a
          href='https://github.com/cube-js/cube.js' target='_blank' rel='noopener noreferrer'>GitHub</a>.</p>
      </div>
    </div>
  )
}