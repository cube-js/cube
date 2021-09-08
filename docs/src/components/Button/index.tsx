import React from 'react';
import LikeIcon from '../../../static/icons/like.inline.svg';
import DislikeIcon from '../../../static/icons/dislike.inline.svg';
import * as styles from './styles.module.scss';

const Button = (props: propsType) => {
  const { type = 'primary' } = props;
  return (
    <button className={`${styles.button} ${styles[type]}`} {...props}>
      {type === 'like' && <LikeIcon />}
      {type === 'dislike' && <DislikeIcon />}
      <span>{props.children}</span>
    </button>
  );
};

export default Button;

interface propsType {
  type: string,
  children: string
}
