import React, { CSSProperties } from 'react';
import * as styles from './styles.module.scss';

interface ScreenshotProps {
  alt?: string;
  src: string;

  /**
   * Use CSS `clip-path` to highlight a specific area of the screenshot.
   *
   * @example inset(20% 64% 72% 20% round 10px)
   */
  highlight?: CSSProperties['clipPath'];
}

const ScreenshotHighlight = ({ highlight, src }: ScreenshotProps) => (
  <div
    className={styles.highlight}
    style={{
      backgroundImage: `url(${src})`,
      clipPath: highlight,
    }}
  />
)

export const Screenshot = (props: ScreenshotProps) => {
  return (
    <div className={styles.screenshot} style={{ textAlign: 'center'}}>
      {props.highlight ? (<ScreenshotHighlight {...props} />) : null}
      <img
        alt={props.alt}
        src={props.src}
        style={{ border: 'none', filter: props.highlight ? 'brightness(0.5)' : 'none' }}
        width="100%"
      />
    </div>
  );
};

