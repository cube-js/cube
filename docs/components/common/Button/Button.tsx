import classnames from 'classnames/bind';
import React from 'react';
import Image from 'next/image';


import LikeIcon from './like.inline.svg';
import DislikeIcon from './dislike.inline.svg';
import * as styles from './Button.module.scss';

const cn = classnames.bind(styles);

export const ButtonGroup = (props) => (
  <div
    {...props}
    className={cn('ButtonGroup', props.className)}
  />
)

export interface ButtonProps extends React.ComponentProps<'button'> {
  variant?: 'primary' | 'secondary' | 'success' | 'danger';
  icon?: React.ReactNode;
}

export const Button = ({ variant = 'primary', children, icon, ...rest }: ButtonProps) => {
  return (
    <button
      {...rest}
      className={cn('Button', styles[variant])}
    >
      {icon ?? null}
      <span>{children}</span>
    </button>
  );
};

export const LikeButton = (props) => {
  return (
    <Button
      {...props}
      variant="success"
      icon={<Image alt='Like' src={LikeIcon} />}
    />
  );
};

export const DislikeButton = (props) => {
  return (
    <Button
      {...props}
      variant="danger"
      icon={<Image alt='Dislike' src={DislikeIcon} />}
    />
  );
};
