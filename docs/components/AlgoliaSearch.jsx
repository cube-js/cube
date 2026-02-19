'use client'

import { SearchIcon, SearchTrigger } from '@cube-dev/marketing-ui'

export function AlgoliaSearch() {
  return (
    <SearchTrigger
      style={{
        display: 'flex',
        alignItems: 'center',
        background: 'none',
        border: 'none',
        padding: 0,
        cursor: 'pointer',
        color: 'currentColor'
      }}
    >
      <SearchIcon style={{ width: 25, height: 25 }} />
    </SearchTrigger>
  )
}
