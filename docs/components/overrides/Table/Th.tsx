import classnames from 'classnames/bind';

import type { ComponentProps } from 'react'

import styles from './Th.module.scss';

const cn = classnames.bind(styles);

export const Th = ({ className = '', ...props }: ComponentProps<'th'>) => (
  <th
    className={cn(
      // 'nx-m-0 nx-border nx-border-gray-300 nx-px-4 nx-py-2 nx-font-semibold dark:nx-border-gray-600 ' +
      'Th',
      className
    )}
    {...props}
  />
)
