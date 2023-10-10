import React from 'react';
import ScrollLink from '../templates/ScrollSpyLink';
import * as styles from '../../../static/styles/index.module.scss';
import cx from 'classnames';
import { Icon } from 'antd';

import { getHashFromContent } from './getHashFromContent';

const ScrollSpyH2 = (props) => {

  const hash = getHashFromContent(props.children);

  return (
    <h2 name={hash} className={styles.hTag}>
      <ScrollLink
        activeClass={styles.scrollspyCurrent}
        to={hash}
        key={hash + Math.random()}
        className={cx(styles.scrollspyLink)}
      >
        <Icon type="link" className={styles.hTagIcon} />
        {props.children}
      </ScrollLink>
    </h2>
  );
};
export default ScrollSpyH2;
