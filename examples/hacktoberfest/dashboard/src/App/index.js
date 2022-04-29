import React, { useState, useEffect } from 'react'
import { page } from 'cubedev-tracking'
import { Col, Row } from 'antd'
import styles from './styles.module.css'
import LanguageSelector from '../LanguageSelector'
import UserCountCard from '../Card/UserCountCard'
import RepoCountCard from '../Card/RepoCountCard'
import PrCountCard from '../Card/PrCountCard'
import LanguageCard from '../Card/LanguageCard'
import PrPerUserRatioCard from '../Card/PrPerUserRatioCard'
import SuccUserCountCard from '../Card/SuccUserCountCard'
import PrPerSuccUserRatioCard from '../Card/PrPerSuccUserRatioCard'
import PrPerRepoRatioCard from '../Card/PrPerRepoRatioCard'
import ForkedRepoCountCard from '../Card/ForkedRepoCountCard'
import PrToOwnRepoCountCard from '../Card/PrToOwnRepoCountCard'
import RepoStarCountCard from '../Card/RepoStarCountCard'
import UnmergedPrCountCard from '../Card/UnmergedPrCountCard'
import PrContentsCountsCard from '../Card/PrContentsCountsCard'
import PrsChart from '../Chart/PrsChart'
import ReposChart from '../Chart/ReposChart'

const menu = [
  {
    id: 'key-highlights',
    title: 'Key Highlights',
  },
  {
    id: 'participants',
    title: 'Participants',
  },
  {
    id: 'repositories',
    title: 'Repositories',
  },
  {
    id: 'pull-requests',
    title: 'Pull Requests',
  },
]

function Solo({ of, filters }) {
  return (
    <Row style={{ marginBottom: 25 }}>
      {of[0] && (
        <Col span={24}>{of[0]({ filters })}</Col>
      )}
    </Row>
  )
}

function Pair({ of, filters }) {
  return (
    <Row gutter={75} style={{ marginBottom: 25 }}>
      {of[0] && (
        <Col span={12}>{of[0]({ filters })}</Col>
      )}
      {of[1] && (
        <Col span={12}>{of[1]({ filters })}</Col>
      )}
    </Row>
  )
}

const defaultLanguages = [
  '',
  'JavaScript',
  'Python'
]

export default function App() {
  const [ languages, setLanguages ] = useState(defaultLanguages)

  const filters = languages.map(language => ({
    dimension: 'Repos.language',
    operator: 'equals',
    values: [ language ],
  }))

  useEffect(() => {
    page()
  })

  return (
    <div className={styles.wrapper}>
      <div className={styles.root}>
        <div className={styles.content}>
          <h2 id='key-highlights'>Key Highlights 🔦</h2>

          <div className={styles.paragraph}>
            Over the past 7 years, Hacktoberfest has been gaining in popularity. This year, however, the statistics are slightly skewed due to the <a href='https://hacktoberfest.digitalocean.com/hacktoberfest-update' target='_blank' rel='noreferrer'>opt-in policy</a>.
          </div>

          <Pair filters={filters} of={[ UserCountCard, PrCountCard ]} />
          <Solo filters={filters} of={[ PrsChart ]} />
          <Pair filters={filters} of={[ RepoCountCard, LanguageCard ]} />

          <h2 id='participants'>Participants 👩‍💻👨‍💻</h2>

          <div className={styles.paragraph}>
            Enthusiasm and T-shirts motivate award-winning developers to outperform the rest of participants
            substantially.
          </div>

          <Pair filters={filters} of={[ UserCountCard, PrPerUserRatioCard ]} />
          <Pair filters={filters} of={[ SuccUserCountCard, PrPerSuccUserRatioCard ]} />

          <h2 id='repositories'>Repositories 📚</h2>

          <div className={styles.paragraph}>
            As always, open source maintainers experienced a noticeable uptick in attention to their repositories.
          </div>

          <Pair filters={filters} of={[ RepoCountCard, RepoStarCountCard ]} />
          <Solo filters={filters} of={[ ReposChart ]} />
          <Pair filters={filters} of={[ PrPerRepoRatioCard, ForkedRepoCountCard ]} />

          <h2 id='pull-requests'>Pull Requests 🙏</h2>

          <Pair filters={filters} of={[ PrCountCard, PrToOwnRepoCountCard ]} />
          <Pair filters={filters} of={[ UnmergedPrCountCard, PrContentsCountsCard ]} />

          <div className={styles.footer}>This story is brought to you by <a href='https://twitter.com/igorlukanin' target='_blank' rel='noreferrer'>Igor Lukanin</a> and <a href='https://twitter.com/Leonid_frontend' target='_blank' rel='noreferrer'>Leonid Yakovlev</a>.</div>

          <div className={styles.footnote}>Based on pull requests with the <span className={styles.code}>hacktoberfest-accepted</span> label collected via GitHub API in the first week of November 2020.</div>
        </div>
        <div className={styles.sidebar}>
          <ul className={styles.menu}>
            {menu.map(({ id, title }) => (
              <li key={id}><a href={`#${id}`}>{title}</a></li>
            ))}
          </ul>
          <LanguageSelector defaultSelected={defaultLanguages} onUpdate={setLanguages}/>
        </div>
      </div>
    </div>
  )
}