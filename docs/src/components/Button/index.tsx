import React from 'react';
import LikeIcon from '../../../static/icons/like.inline.svg';
import DislikeIcon from '../../../static/icons/dislike.inline.svg';
import * as styles from './styles.module.scss';

const Button = (props: propsType) => {
  const { view = 'primary' } = props;
  return (
    <button
      type="button"
      {...props}
      className={
        props.className
          ? `${styles.button} ${styles[view]} ${props.className}`
          : `${styles.button} ${styles[view]}`
      }
    >
      {view === 'like' && <LikeIcon />}
      {view === 'dislike' && <DislikeIcon />}
      <span>{props.children}</span>
    </button>
  );
};

export default Button;

interface propsType {
  view: 'primary' | 'secondary' | 'like' | 'dislike' | 'outline';
  children: string;
}
