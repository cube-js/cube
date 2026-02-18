import styles from './Btn.module.css'

export const Btn = ({ children }) => {
  return <span className={styles.button}>{children}</span>
}
