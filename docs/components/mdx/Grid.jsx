import styles from './Grid.module.css'

export const Grid = ({ children, cols = 3 }) => {
  const colsClass = cols === 2 ? styles.cols2 : cols === 3 ? styles.cols3 : ''

  return (
    <div className={`${styles.grid} ${colsClass}`}>
      {children}
    </div>
  )
}
