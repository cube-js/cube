import React from 'react';

import classnames from 'classnames/bind';

import * as styles from './styles.module.scss';

const cn = classnames.bind(styles);

interface ProductVideoProps {
  src: string;
  autoPlay?: boolean;
  muted?: boolean;
  loop?: boolean;
  playsInline?: boolean;
}

export const ProductVideo = (props: ProductVideoProps) => {
  return (
    <div className={cn('productVideo')} style={{ textAlign: 'center' }}>
      <video
        autoPlay={props.autoPlay !== false}
        muted={props.muted !== false}
        loop={props.loop !== false}
        playsInline={props.playsInline !== false}
        src={props.src}
        style={{ width: '100%', height: '525px', objectFit: 'contain' }}
      />
    </div>
  );
};

