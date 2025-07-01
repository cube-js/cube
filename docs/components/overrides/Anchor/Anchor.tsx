// eslint-disable-next-line no-restricted-imports -- only in this file we determine either we include <a /> as child of <NextLink /> based of `newNextLinkBehavior` value
import NextLink from 'next/link'
import next from 'next/package.json'
import { useConfig } from 'nextra-theme-docs';
import type { ComponentProps, ReactElement } from 'react'
import { forwardRef } from 'react'

export type AnchorProps = Omit<ComponentProps<'a'>, 'ref'> & {
  newWindow?: boolean
}

const nextVersion = Number(next.version.split('.')[0])

export const Anchor = forwardRef<HTMLAnchorElement, AnchorProps>(function (
  { href = '', children, newWindow, ...props },
  // ref is used in <NavbarMenu />
  forwardedRef
): ReactElement {
  const config = useConfig()
  // console.log(useConfig())

  if (newWindow) {
    return (
      <a
        ref={forwardedRef}
        href={href}
        target="_blank"
        rel="noreferrer"
        {...props}
      >
        {children}
        <span className="nx-sr-only"> (opens in a new tab)</span>
      </a>
    )
  }

  if (!href) {
    return (
      <a ref={forwardedRef} {...props}>
        {children}
      </a>
    )
  }

  if (nextVersion > 12 || config.newNextLinkBehavior) {
    return (
      <NextLink ref={forwardedRef} href={href} {...props}>
        {children}
      </NextLink>
    )
  }

  return (
    <NextLink href={href} passHref>
      <a ref={forwardedRef} {...props}>
        {children}
      </a>
    </NextLink>
  )
})

Anchor.displayName = 'Anchor'
