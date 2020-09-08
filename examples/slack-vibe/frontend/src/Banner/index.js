import React from 'react';
import styles from './styles.module.css';

export default function Banner() {
  return (
    <div className={styles.root}>
      <div className={styles.banner}>
        <div><em>Slack Vibe</em> is created and powered by <em>Cube.js</em>, an&nbsp;open source analytical
          data access layer for modern web applications.</div>
        <ul className={styles.buttons}>
          <li>
            <a href='https://cube.dev'
               target='_blank'
               rel='noopener noreferrer'
            >
              Learn more about Cube.js
            </a>
          </li>
        </ul>
      </div>
    </div>
  )
}