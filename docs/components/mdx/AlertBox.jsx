import styles from './AlertBox.module.css'

export const InfoBox = ({ children }) => (
  <div className={`${styles.box} ${styles.info}`}>
    {children}
  </div>
)

export const WarningBox = ({ children }) => (
  <div className={`${styles.box} ${styles.warning}`}>
    {children}
  </div>
)

export const SuccessBox = ({ children }) => (
  <div className={`${styles.box} ${styles.success}`}>
    {children}
  </div>
)

export const ReferenceBox = ({ children }) => (
  <div className={`${styles.box} ${styles.reference}`}>
    {children}
  </div>
)
