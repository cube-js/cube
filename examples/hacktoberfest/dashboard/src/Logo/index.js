import React from 'react'
import styles from './styles.module.css'

export default function Logo() {
  return (
    <div className={styles.root}>
      <div className={styles.header}>
        <div className={styles.image} />
        <h1>
          <span className={styles.small}>The open source story of</span><br />
          Hacktoberfest 2020<br />
          <span className={styles.small}>told with data and a bit of snark</span><br />
        </h1>
      </div>
      <div className={styles.footer}>
        <a href='https://cube.dev?utm_source=product&utm_medium=app&utm_campaign=hacktoberfest' target='_blank' rel='noreferrer'>
          <div className={styles.sponsors} />
        </a>
      </div>
    </div>
  )
}