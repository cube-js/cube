'use client'

import { useState, useEffect } from 'react'
import dynamic from 'next/dynamic'

const PurpleBanner = dynamic(
  () => import('@cube-dev/purple-banner').then(mod => mod.default || mod),
  { ssr: false }
)

export function PurpleBannerWrapper() {
  const [pbVisible, setPbVisible] = useState(false)

  useEffect(() => {
    requestAnimationFrame(() => {
      setPbVisible(true)
    })
  }, [])

  return (
    <div className={`pb-wrapper${pbVisible ? ' pb-wrapper--visible' : ''}`}>
      <PurpleBanner
        utmSource="cube.dev"
        debugMode={process.env.NEXT_PUBLIC_SHOW_PURPLE_BANNER === 'true'}
      />
    </div>
  )
}
