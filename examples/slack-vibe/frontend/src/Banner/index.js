import React from 'react';
import styles from './styles.module.css';

export default function Banner() {
  return (
    <div className={styles.root}>
      <div className={styles.banner}>
        <ul className={styles.buttons}>
        <li>
            <a href='https://github.com/cube-js/cube.js/tree/master/examples/slack-vibe'
               target='_blank'
               rel='noopener noreferrer'
            >
              Browse on GitHub
            </a>
          </li>
          <li>
            <a href='https://heroku.com/deploy?template=https://github.com/cube-js/cube.js/tree/heroku/slack-vibe/'
               target='_blank'
               rel='noopener noreferrer'
            >
              Deploy to Heroku
            </a>
          </li>
          <li>
            <a href='https://hub.docker.com/r/cubejs/slack-vibe'
               target='_blank'
               rel='noopener noreferrer'
            >
              Get Docker container
            </a>
          </li>
        </ul>
      </div>
    </div>
  )
}