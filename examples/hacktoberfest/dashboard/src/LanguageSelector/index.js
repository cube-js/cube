import React, { useState } from 'react'
import styles from './styles.module.css'

const backgroundColors = {
  'JavaScript': '#f1e05a',
  'Python': '#3572A5',
  'HTML': '#e34c26',
  'Java': '#b07219',
  'C++': '#f34b7d',
  'TypeScript': '#2b7489',
  'PHP': '#4F5D95',
  'CSS': '#563d7c',
  'Go': '#00ADD8',
  'Rust': '#dea584',
  'Dockerfile': '#384d54',
  'C': '#555555',
  'C#': '#178600',
  'Ruby': '#701516',
  'Objective-C': '#438eff',
  'Assembly': '#6E4C13',
  'Shell': '#89e051',
  'Perl': '#0298c3',
  'Jupyter': '#DA5B0B',
  'Kotlin': '#F18E33',
  'Dart': '#00B4AB',
  'Vue': '#2c3e50',
  'Swift': '#ffac45',
  'SCSS': '#c6538c',
}

const invertForegroundColors = [
  'JavaScript',
  'Kotlin',
  'Rust',
  'Shell',
  'Swift',
]

export default function LanguageSelector({ initial, onUpdate }) {
  const [ languages ] = useState([
    'JavaScript',
    'Python',
    'HTML',
    'Java',
    'C++',
    'TypeScript',
    'PHP',
    'CSS',
    'Go',
    'Jupyter',
    'C',
    'Dart',
    'C#',
    'Shell',
    'Kotlin',
    'Rust',
    'Vue',
    'Ruby',
    'Swift',
    'Dockerfile',
    'SCSS',
  ])
  const [ selected, setSelected ] = useState(initial)

  function toggleSelect(language) {
    let updated = [ ...selected ]

    if (updated.indexOf(language) === -1) {
      if (updated.length === 2) {
        updated.shift()
        updated.push(language)
      } else if (updated.length === 1) {
        updated.push(language)
      }
    } else {
      if (updated.length === 2) {
        updated = updated.filter(e => e !== language)
        updated.push('all')
      }
    }

    setSelected(updated)
    onUpdate(updated)
  }

  return (
    <ul className={styles.root}>
      <li
        key='all'
        className={selected.indexOf('all') !== -1 ? styles.selected : undefined}
        onClick={() => toggleSelect('all')}
      >
        All languages
      </li>
      {languages && languages.map(language => (
        <li
          key={language}
          style={selected.indexOf(language) === -1 ? {
            background: backgroundColors[language],
            color: invertForegroundColors.indexOf(language) !== -1 ? '#000000' : 'inherit',
          } : undefined}
          className={selected.indexOf(language) !== -1 ? styles.selected : undefined}
          onClick={() => toggleSelect(language)}
        >
          {language}
        </li>
      ))}
    </ul>
  )
}