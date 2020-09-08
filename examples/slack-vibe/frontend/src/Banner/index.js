import React from 'react';
import styles from './styles.module.css';

export default function Banner() {
  return (
    <div className={styles.root}>
      <div className={styles.banner}>
        <p><em>Slack Vibe</em> is created and powered by <em>Cube.js</em>, an&nbsp;open source analytical
          data access layer for modern web applications.</p>
        <ul className={styles.buttons}>
          <li>
            <a href='https://cube.dev'
               className='button'
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