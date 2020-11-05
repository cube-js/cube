import React from 'react'
import styles from './styles.module.css'
import LanguageSelector from '../LanguageSelector'

export default function App() {
  return (
    <>
      <div className={styles.banner}>
        <a href='https://cube.dev' target='_blank' rel='noreferrer'>
          This data story is powered by Cube.js,
          an open source analytical API platform
        </a>
      </div>
      <div className={styles.content}>
        <div className={styles.logo}>
          <div className={styles.image} />
          <div className={styles.text}>
            <h2>The open source story of</h2>
            <h1>Hacktoberfest 2020</h1>
            <h2>told with data and admiration</h2>
          </div>
        </div>
        <LanguageSelector
          initial={[ 'all' ]}
          onUpdate={selected => console.log(selected)}
        />
      </div>
    </>
  )
}