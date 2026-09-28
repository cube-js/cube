import Link from 'next/link'
import styles from './GridItem.module.css'

export const GridItem = ({ imageUrl, title, url }) => (
  <Link className={styles.wrapper} href={url}>
    <div className={styles.item}>
      <img
        className={styles.image}
        src={imageUrl}
        alt={title}
      />
      <span className={styles.title}>{title}</span>
    </div>
  </Link>
)
