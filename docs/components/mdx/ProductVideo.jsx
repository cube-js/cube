import styles from './ProductVideo.module.css'

export const ProductVideo = (props) => {
  return (
    <div className={styles.productVideo}>
      <video
        autoPlay={props.autoPlay !== false}
        muted={props.muted !== false}
        loop={props.loop !== false}
        playsInline={props.playsInline !== false}
        src={props.src}
        className={styles.video}
      />
    </div>
  )
}
