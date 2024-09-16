import * as React from 'react';
import { useEffect, DetailedHTMLProps, ButtonHTMLAttributes, AnchorHTMLAttributes } from 'react';
import Link from 'next/link';
import classNames from 'classnames/bind';

import { useState } from 'react';

import styles from './Button.module.css';

const cn = classNames.bind(styles);
const loader = (
  <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="none">
    <path
      d="M8 15a7 7 0 0 1-7-7 .5.5 0 1 1 1 0 6 6 0 0 0 3.7 5.5 6 6 0 0 0 6.6-1.2 6 6 0 0 0 0-8.6A6 6 0 0 0 8 2a.5.5 0 1 1 0-1 7 7 0 0 1 7 7 7 7 0 0 1-7 7Z"
      fill="currentColor"
      stroke="currentColor"
      strokeWidth=".5"
    />
  </svg>
);

export type ButtonColor = 'transparent' | 'pink' | 'purple' | 'cherry' | 'back';
export type ButtonVariant = 'default' | 'outline' | 'clear';
export type ButtonSize = 's' | 'm' | 'l';

export type ButtonProps = {
  size?: ButtonSize;
  appearance?: 'dark' | 'light';
  color?: ButtonColor;
  variant?: ButtonVariant;
  pseudoHover?: boolean;
  pseudoFocus?: boolean;
  pseudoActive?: boolean;
  isLoad?: boolean;
} & Omit<DetailedHTMLProps<ButtonHTMLAttributes<HTMLButtonElement>, HTMLButtonElement>, 'ref'> &
  Omit<DetailedHTMLProps<AnchorHTMLAttributes<HTMLAnchorElement>, HTMLAnchorElement>, 'ref'>;

export const Button = React.forwardRef<HTMLButtonElement & HTMLAnchorElement, ButtonProps>(
  (
    {
      appearance = 'light',
      color = 'transparent',
      variant = 'default',
      type = 'button',
      size = 'm',
      disabled = false,
      pseudoHover = false,
      pseudoFocus = false,
      pseudoActive = false,
      isLoad = false,
      className = '',
      children,
      href = '',
      ...rest
    },
    ref
  ) => {
    const loaderAppearAnimationDuration = 700;

    const classNames = cn('Button', `Button--appearance-${appearance}`, className, {
      'Button--pink': color === 'pink',
      'Button--purple': color === 'purple',
      'Button--cherry': color === 'cherry',
      'Button--transparent': color === 'transparent',
      'Button--back': color === 'back',
      'Button--size-s': size === 's',
      'Button--size-m': size === 'm',
      'Button--size-l': size === 'l',
      'Button--variant-outline': variant === 'outline',
      'Button--pseudoHover': pseudoHover,
      'Button--pseudoFocus': pseudoFocus,
      'Button--pseudoActive': pseudoActive,
    });

    const [isLoaderVisible, setIsLoaderVisible] = useState(isLoad);

    useEffect(() => {
      if (isLoad === false) {
        setTimeout(() => {
          setIsLoaderVisible(false);
        }, loaderAppearAnimationDuration);
      } else {
        setIsLoaderVisible(true);
      }
    }, [isLoad]);

    let prefix;
    if (isLoad || isLoaderVisible) {
      const prefixClassName = cn('Button__prefix', 'Button__prefix--loader', {
        'Button__prefix--removing': isLoaderVisible !== isLoad && !isLoad,
      });
      prefix = <span className={prefixClassName}>{loader}</span>;
    }

    if (href) {
      if (href.startsWith('/')) {
        return (
          <Link href={href} passHref legacyBehavior>
            <a ref={ref} {...rest} className={classNames}>
              {children}
            </a>
          </Link>
        );
      }
      return (
        <a ref={ref} {...rest} href={href} className={classNames}>
          {children}
        </a>
      );
    }

    return (
      <button
        ref={ref}
        {...rest}
        className={classNames}
        type={type}
        disabled={disabled || isLoad}
        // @ts-ignore
        style={{ '--loader-time': loaderAppearAnimationDuration + 'ms' }}
      >
        {prefix}
        {children}
      </button>
    );
  }
);

export default Button;