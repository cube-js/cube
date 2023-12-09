import classnames from 'classnames/bind';

import type { ComponentProps } from 'react'

import * as styles from './Td.module.scss';

const cn = classnames.bind(styles);

export const Td = ({ className = '', ...props }: ComponentProps<'td'>) => (
  <td
    className={cn(
      'Td',
      // 'nx-m-0 nx-border nx-border-gray-300 nx-px-4 nx-py-2 dark:nx-border-gray-600 ' +
      className
    )}
    {...props}
  />
)
