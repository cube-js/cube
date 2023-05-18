import classnames from 'classnames/bind';
import React from 'react';
import styles from './GridItem.module.css';

const cn = classnames.bind(styles);

export type GridItemProps = {
  imageUrl: string;
  title: string;
  url: string;
};

export const GridItem = ({
  imageUrl,
  title,
  url,
}: GridItemProps) => (
  <a className={cn('GridItem__Wrapper')} href={url}>
    <div className={cn('GridItem')}>
      <img
        className={cn('GridItem__Image')}
        src={imageUrl}
        alt={title}
      />
      <span className={cn('GridItem__Title')}>{title}</span>
    </div>
  </a>
);
