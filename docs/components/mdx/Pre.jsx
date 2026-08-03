'use client'

import { Pre as NextraPre } from 'nextra/components'

// Custom Pre wrapper that exposes data-language attribute to the DOM
// Nextra's Pre receives data-language but intentionally doesn't render it
export const Pre = (props) => {
  const language = props['data-language']

  return (
    <div data-language={language}>
      <NextraPre {...props} />
    </div>
  )
}
