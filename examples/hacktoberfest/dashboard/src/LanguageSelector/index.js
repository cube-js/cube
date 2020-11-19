import React, { useEffect, useState } from 'react'
import { useCubeQuery } from '@cubejs-client/react'
import styles from './styles.module.css'
import { colors } from '../styles'

const limit = 15

const defaultLanguages = [
  'JavaScript',
  'Python',
  'HTML',
  'Java',
  'TypeScript',
  'PHP',
  'C',
  'CSS',
  'Go',
  'C++',
  'Dart',
  'Shell',
  'Ruby',
  'Kotlin',
  'Jupyter',
  'C#',
  'Rust',
  'Vue',
  'Swift',
  'Dockerfile',
  'SCSS',
]

function toState(languages) {
  return languages
    .map(x => x === 'Jupyter Notebook' ? 'Jupyter' : x)
    .slice(0, limit)
}

export default function LanguageSelector({ defaultSelected, onUpdate }) {
  const [ languages, setLanguages ] = useState(toState(defaultLanguages))

  const { resultSet } = useCubeQuery({
    measures: [ 'Repos.count' ],
    dimensions: [ 'Repos.language' ],
    filters: [ {
      dimension: 'Repos.language',
      operator: 'notEquals',
      values: [ 'Unknown' ],
    } ],
    order: {
      'Repos.count': 'desc',
    },
    limit,
  })

  useEffect(() => {
    if (resultSet) {
      const languages = resultSet.tablePivot().map(row => row['Repos.language'])
      setLanguages(toState(languages))
    }
  }, [ resultSet ])

  const [ selected, setSelected ] = useState(defaultSelected)

  function toggleSelect(language) {
    let updated = selected.indexOf(language) === -1
      ? [ ...selected, language ]
      : selected.filter(x => x !== language)

    if (updated.length === 0) {
      updated = [ '' ]
    }

    setSelected(updated)
    onUpdate(updated)
  }

  return (
    <ul className={styles.root}>
      <li
        key=''
        className={selected.indexOf('') !== -1 ? styles.selected : ''}
        onClick={() => toggleSelect('')}
      >
          <span
            className={styles.dot}
            style={{ background: colors.languages[''] }}
          />
        All languages
      </li>
      {languages.map(language => (
        <li
          key={language}
          className={selected.indexOf(language) !== -1 ? styles.selected : ''}
          onClick={() => toggleSelect(language)}
        >
          <span
            className={styles.dot}
            style={{ background: colors.languages[language] }}
          />
          {language}
        </li>
      ))}
    </ul>
  )
}