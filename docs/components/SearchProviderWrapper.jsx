'use client'

import { SearchProvider } from '@cube-dev/marketing-ui'
import '@cube-dev/marketing-ui/dist/index.css'

export function SearchProviderWrapper({ children }) {
  return (
    <SearchProvider
      algoliaAppId={process.env.NEXT_PUBLIC_ALGOLIA_APP_ID}
      algoliaApiKey={process.env.NEXT_PUBLIC_ALGOLIA_API_KEY}
      algoliaIndexName={process.env.NEXT_PUBLIC_ALGOLIA_INDEX_NAME}
    >
      {children}
    </SearchProvider>
  )
}
