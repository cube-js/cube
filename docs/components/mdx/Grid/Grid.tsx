import classnames from 'classnames/bind';
import React from 'react';
import styles from './Grid.module.css';

const cn = classnames.bind(styles);

export type GridProps = {
  children: React.ReactNode;
  cols?: number;
};

export const GridContext = React.createContext('grid');

const defaultProps = {
  cols: 3,
};
export const Grid = ({
  children,
  ...restProps
}: GridProps) => {
  const normalizedProps = { ...defaultProps, ...restProps };
  const classNames = cn('Grid', `Grid--${normalizedProps.cols}`);

  return (
    <div className="ant-row">
      <div className={classNames}>
        {children}
      </div>
    </div>
  );
};
