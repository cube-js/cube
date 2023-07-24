import classnames from 'classnames/bind';

import type { ComponentProps } from 'react'

import * as styles from './Table.module.scss';

const cn = classnames.bind(styles);

export const Table = ({
  className = '',
  ...props
}: ComponentProps<'table'>) => (
  <table className={cn(
    'Table',
    // 'nx-block nx-overflow-x-scroll nextra-scrollbar nx-mt-6 nx-p-0 first:nx-mt-0',
    className
  )} {...props} />
)
