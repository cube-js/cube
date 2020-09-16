import React from 'react';
import styles from './styles.module.css';

export default function Banner() {
  return (
    <div className={styles.root}>
      <div className={styles.banner}>
        <div>Slack Vibe is created and powered by Cube.js, an&nbsp;open source
          analytical API platform for modern applications.</div>
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