import { CubeLogo } from './CubeLogo'
import styles from './LogoWithVersion.module.css'

const PACKAGE_VERSION = require('../../lerna.json').version

export const LogoWithVersion = () => {
  return (
    <div className={styles.container}>
      <a href="https://cube.dev" className={styles.logoLink}>
        <CubeLogo />
      </a>
      <div className={styles.version}>
        {PACKAGE_VERSION}
      </div>
    </div>
  )
}
