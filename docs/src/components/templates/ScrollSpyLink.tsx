import React from 'react';
import { Link } from 'react-scroll';
import styles from '../../../static/styles/index.module.scss';

export const SCROLL_OFFSET = -66;
export const SCROLL_DURATION = 300;


const ScrollLink = props => (
  <div className={styles.scrollspyLinkWrapper} >
    <Link
      offset={SCROLL_OFFSET}
      smooth
      {...props}
      spy
      duration={SCROLL_DURATION}
      onClick={() => window.history.pushState('', '', `#${props.to}`)}
    >
      {props.children}
    </Link>
  </div>
);

export default ScrollLink;
