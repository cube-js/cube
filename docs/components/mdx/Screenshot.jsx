import styles from './Screenshot.module.css'

function getOptimizedLink(url) {
  if (url.startsWith('https://ucarecdn.com/')) {
    url = url.substring(0, url.lastIndexOf('/') + 1)
    return `${url}-/format/webp/`
  }
  return url
}

const ScreenshotHighlight = ({ highlight, src }) => (
  <div
    className={styles.highlight}
    style={{
      backgroundImage: `url(${src})`,
      clipPath: highlight,
    }}
  />
)

export const Screenshot = ({ alt, src, highlight }) => {
  return (
    <div className={styles.screenshot} style={{ textAlign: 'center' }}>
      {highlight ? <ScreenshotHighlight highlight={highlight} src={src} /> : null}
      <img
        alt={alt}
        src={getOptimizedLink(src)}
        style={{ border: 'none', filter: highlight ? 'brightness(0.5)' : 'none' }}
        width="100%"
      />
    </div>
  )
}

export const Diagram = ({ alt, src, highlight }) => (
  <div className={styles.diagram} style={{ textAlign: 'center' }}>
    {highlight ? <ScreenshotHighlight highlight={highlight} src={src} /> : null}
    <img
      alt={alt}
      src={getOptimizedLink(src)}
      style={{ border: 'none', filter: highlight ? 'brightness(0.5)' : 'none' }}
      width="100%"
    />
  </div>
)
