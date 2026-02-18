'use client'

import React, { useState, useEffect, useRef, useCallback } from 'react'
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
  const containerRef = useRef(null)
  const [codeBlocks, setCodeBlocks] = useState([])
  const [selectedTab, setSelectedTab] = useState(null)
  const [isInitialized, setIsInitialized] = useState(false)

  // Extract languages from DOM after render
  useEffect(() => {
    if (!containerRef.current) return

    const blocks = []
    // Find all code blocks with data-language attribute (from our custom Pre wrapper)
    const wrappers = containerRef.current.querySelectorAll(':scope > [data-language]')

    wrappers.forEach((wrapper, index) => {
      const lang = wrapper.getAttribute('data-language')
      if (lang) {
        blocks.push({ lang, index, wrapper })
      }
    })

    // Sort: preferred languages first
    blocks.sort((a, b) => {
      const aPreferred = PREFERRED_LANGS.indexOf(a.lang)
      const bPreferred = PREFERRED_LANGS.indexOf(b.lang)
      if (aPreferred !== -1 && bPreferred === -1) return -1
      if (bPreferred !== -1 && aPreferred === -1) return 1
      if (aPreferred !== -1 && bPreferred !== -1) return aPreferred - bPreferred
      return 0
    })

    setCodeBlocks(blocks)

    // Set initial tab
    const defaultLang = localStorage.getItem(STORAGE_KEY)
    const availableLangs = blocks.map(b => b.lang)

    if (defaultLang && availableLangs.includes(defaultLang)) {
      setSelectedTab(defaultLang)
    } else if (blocks.length > 0) {
      setSelectedTab(blocks[0].lang)
    }

    setIsInitialized(true)
  }, [children])

  // Sync with other CodeTabs instances
  useEffect(() => {
    if (!isInitialized) return

    const availableLangs = codeBlocks.map(b => b.lang)

    const syncHandler = (e) => {
      const lang = e.detail.lang
      if (availableLangs.includes(lang)) {
        setSelectedTab(lang)
      }
    }

    const storageHandler = (e) => {
      if (e.key === STORAGE_KEY) {
        const lang = e.newValue
        if (lang && availableLangs.includes(lang)) {
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
  }, [codeBlocks, isInitialized])

  // Update visibility of code blocks based on selected tab
  useEffect(() => {
    if (!containerRef.current || !selectedTab || codeBlocks.length === 0) return

    codeBlocks.forEach((block) => {
      if (block.wrapper) {
        block.wrapper.style.display = block.lang === selectedTab ? 'block' : 'none'
      }
    })
  }, [selectedTab, codeBlocks])

  // Get unique languages maintaining order
  const tabsList = React.useMemo(() => {
    const seen = new Set()
    return codeBlocks
      .filter(block => {
        if (seen.has(block.lang)) return false
        seen.add(block.lang)
        return true
      })
      .map(block => block.lang)
  }, [codeBlocks])

  const handleTabClick = useCallback((lang) => {
    if (lang !== selectedTab) {
      if (lang === 'python' || lang === 'javascript' || lang === 'yaml') {
        localStorage.setItem(STORAGE_KEY, lang)
        window.dispatchEvent(
          new CustomEvent('codetabs.changed', {
            detail: { lang }
          })
        )
      }
      setSelectedTab(lang)
    }
  }, [selectedTab])

  return (
    <div className={styles.codeBlock}>
      {tabsList.length > 0 && (
        <div className={styles.tabs}>
          {tabsList.map((lang) => {
            const displayLang = lang === 'js' ? 'javascript' : lang
            return (
              <div
                key={lang}
                className={`${styles.tab} ${displayLang === selectedTab || lang === selectedTab ? styles.selectedTab : ''}`}
                onClick={() => handleTabClick(lang)}
              >
                {langs[displayLang] || displayLang}
              </div>
            )
          })}
        </div>
      )}
      <div ref={containerRef} className={styles.codeContent}>
        {children}
      </div>
    </div>
  )
}
