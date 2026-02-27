import styles from './GetStartedButton.module.css'

export const GetStartedButton = () => {
  return (
    <a
      href="https://cubecloud.dev/auth/signup?utm_source=docs&utm_medium=site&UTM_Publisher=Cube"
      className={styles.button}
      target="_blank"
      rel="noopener noreferrer"
    >
      Get started for free
    </a>
  )
}
