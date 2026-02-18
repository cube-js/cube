'use client'

import { useState, useEffect, useMemo } from 'react'
import { Pre } from 'nextra/components'
import styles from './CodeTabs.module.css'

const langs = {
  js: 'JavaScript',
  javascript: 'JavaScript',
  bash: 'Bash',
  go: 'Go',
  graphql: 'GraphQL',
  json: 'JSON',
  jsx: 'JSX',
  python: 'Python',
  sql: 'SQL',
  tsx: 'TSX',
  typescript: 'TypeScript',
  yaml: 'YAML'
}

const STORAGE_KEY = 'cube-docs.default-code-lang'
const PREFERRED_LANGS = ['yaml', 'python']

export const CodeTabs = ({ children }) => {
  const [selectedTab, setSelectedTab] = useState(PREFERRED_LANGS[0])

  const tabs = useMemo(() => {
    let tabsMap = children.reduce((dict, tab, i) => {
      const result = { ...dict }
      if (result[tab.props['data-language']] === undefined) {
        result[tab.props['data-language']] = i
      }
      return result
    }, {})

    // Place the tab with the preferred language on the first position
    let tabWithPreferredLangKey = Object.keys(tabsMap).find(key => PREFERRED_LANGS.includes(key))
    if (tabWithPreferredLangKey !== undefined) {
      let tabWithPreferredLangValue = tabsMap[tabWithPreferredLangKey]
      delete tabsMap[tabWithPreferredLangKey]
      tabsMap = {
        [tabWithPreferredLangKey]: tabWithPreferredLangValue,
        ...tabsMap
      }
    }

    return tabsMap
  }, [children])

  useEffect(() => {
    const defaultLang = localStorage.getItem(STORAGE_KEY)

    if (defaultLang && tabs[defaultLang] !== undefined) {
      setSelectedTab(defaultLang)
    } else {
      const [lang] = Object.entries(tabs).find(tab => tab[1] === 0)
      setSelectedTab(lang)
    }

    const syncHandler = (e) => {
      const lang = e.detail.lang
      if (tabs[lang] !== undefined) {
        setSelectedTab(lang)
      }
    }

    const storageHandler = (e) => {
      if (e.key === STORAGE_KEY) {
        const lang = e.newValue
        if (lang && tabs[lang] !== undefined) {
          setSelectedTab(lang)
        }
      }
    }

    window.addEventListener('storage', storageHandler)
    window.addEventListener('codetabs.changed', syncHandler)

    return () => {
      window.removeEventListener('storage', storageHandler)
      window.removeEventListener('codetabs.changed', syncHandler)
    }
  }, [tabs])

  return (
    <div className={styles.codeBlock}>
      <div className={styles.tabs}>
        {Object.entries(tabs)
          .map(tab => children.find(child => child.props['data-language'] === tab[0]))
          .filter(tab => tab !== undefined && !!tab.props['data-language'])
          .map((tab, i) => {
            if (tab === undefined) return null
            let lang = tab.props['data-language']
            if (lang === 'js') {
              lang = 'javascript'
            }
            return (
              <div
                key={i}
                className={`${styles.tab} ${lang === selectedTab ? styles.selectedTab : ''}`}
                onClick={() => {
                  if (
                    lang !== selectedTab &&
                    (lang === 'python' || lang === 'javascript' || lang === 'yaml')
                  ) {
                    localStorage.setItem(STORAGE_KEY, lang)
                    window.dispatchEvent(
                      new CustomEvent('codetabs.changed', {
                        detail: { lang }
                      })
                    )
                  }
                  setSelectedTab(lang)
                }}
              >
                {langs[lang] || lang}
              </div>
            )
          })}
      </div>
      <Pre hasCopyCode={true} className={styles.pre}>
        {children && children.find(child => child.props['data-language'] === selectedTab)?.props.children}
      </Pre>
    </div>
  )
}
