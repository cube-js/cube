'use client'

import Link from 'next/link'
import styles from './EnvVar.module.css'

export function EnvVar({ children }) {
  const name = typeof children === 'string' ? children.trim() : children
  const anchor = name.toLowerCase()

  return (
    <Link href={`/product/configuration/reference/environment-variables#${anchor}`} className={styles.link}>
      <code className={styles.code}>{name}</code>
    </Link>
  )
}
