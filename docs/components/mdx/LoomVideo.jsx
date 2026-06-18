import styles from './LoomVideo.module.css'

export const LoomVideo = ({ url }) => {
  return (
    <div
      className={styles.wrapper}
      style={{ position: 'relative', paddingBottom: '56.25%', height: 0 }}
    >
      <iframe
        src={url}
        frameBorder="0"
        allowFullScreen={true}
        style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: '100%' }}
      />
    </div>
  )
}
