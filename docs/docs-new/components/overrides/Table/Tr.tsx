import classnames from 'classnames/bind';

import type { ComponentProps } from 'react'

import * as styles from './Tr.module.scss';

const cn = classnames.bind(styles);

export const Tr = ({ className = '', ...props }: ComponentProps<'tr'>) => (
  <tr
    className={cn(
      'Tr',
      // 'nx-m-0 nx-border-t nx-border-gray-300 nx-p-0 dark:nx-border-gray-600 ' +
      // 'even:nx-bg-gray-100 even:dark:nx-bg-gray-600/20 ' +
      className
    )}
    {...props}
  />
)
