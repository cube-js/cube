import classnames from 'classnames/bind'
import type { ComponentProps, ReactElement } from 'react'

import * as styles from './Code.module.scss';

const cn = classnames.bind(styles);

export const Code = ({
  children,
  className,
  ...props
}: ComponentProps<'code'>): ReactElement => {
  const hasLineNumbers = 'data-line-numbers' in props
  return (
    <code
      className={cn(
        'Code',
        'nx-border-black nx-border-opacity-[0.04] nx-bg-opacity-[0.03] nx-bg-black nx-rounded-md nx-border nx-py-0.5 nx-px-[.25em] nx-text-[.9em]',
        'dark:nx-border-white/10 dark:nx-bg-white/10 ',
        hasLineNumbers && '[counter-reset:line]',
        className
      )}
      // always show code blocks in ltr
      dir="ltr"
      {...props}
    >
      {children}
    </code>
  )
}
