'use client'

import { useEffect, useRef } from 'react'
import { usePathname } from 'next/navigation'

export function AnalyticsProvider({ children }) {
  const pathname = usePathname()
  const isFirstRender = useRef(true)

  useEffect(() => {
    // Skip tracking on first render (initial page load is tracked by GTM)
    if (isFirstRender.current) {
      isFirstRender.current = false
      return
    }

    // Track page view on route change
    const trackPageView = async () => {
      if (typeof window !== 'undefined') {
        const { page } = await import('cubedev-tracking')
        page()
      }
    }

    trackPageView()
  }, [pathname])

  return children
}
