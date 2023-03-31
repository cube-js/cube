import React from 'react';
import * as styles from './styles.module.scss';

interface ScreenshotProps {
  alt?: string;
  src: string;
}

export const Screenshot = ({ alt, src }: ScreenshotProps) => {
  return (
    <div className={styles.screenshot} style={{ textAlign: 'center'}}>
      <img
        alt={alt}
        src={src}
        style={{ border: 'none' }}
        width="100%"
      />
    </div>
  );
};

