import classnames from 'classnames/bind';
import { Anchor, AnchorProps } from './Anchor';

import styles from './Link.module.scss';

const cn = classnames.bind(styles);

const EXTERNAL_HREF_REGEX = /https?:\/\//

export const Link = ({ href = '', className, ...props }: AnchorProps) => (
  <Anchor
    href={href}
    newWindow={EXTERNAL_HREF_REGEX.test(href)}
    className={cn(
      'Link',
      // 'nx-text-primary-600 nx-underline nx-decoration-from-font [text-underline-position:from-font]',
      className
    )}
    {...props}
  />
)
