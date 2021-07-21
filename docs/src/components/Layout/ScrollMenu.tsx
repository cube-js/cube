import React from 'react';
import { Col, Icon } from 'antd';
import cx from 'classnames';

import ScrollLink from '../templates/ScrollSpyLink';

import * as styles from '../../../static/styles/index.module.scss';
import { SectionWithoutNodes } from '../../types';
import { layout } from '../../theme';

const EditPage = ({ githubUrl }: { githubUrl: string }) => {
  return (
    <div className={styles.scrollspyLinkWrapper}>
      <p className={cx(styles.editPage, styles.scrollspyLink)}>
        <a href={githubUrl}>
          {' '}
          <Icon type="github" /> Edit this page{' '}
        </a>
      </p>
    </div>
  );
};

const HeadingLink = ({ id, title, type }: SectionWithoutNodes) => {
  return (
    <ScrollLink
      activeClass={styles.scrollspyCurrent}
      to={id}
      key={id}
      className={cx(styles.scrollspyLink, {
        [styles.scrollspySubitem]: type === 'h3',
        [styles.scrollspyTop]: id === 'top',
      })}
    >
      {title}
    </ScrollLink>
  );
};

type Props = {
  githubUrl: string;
  sections: SectionWithoutNodes[];
};

const defaultProps: Partial<Props> = {
  sections: [],
};

const ScrollMenu: React.FC<Props> = (props) => {
  const mergedProps = { ...defaultProps, ...props };
  return (
    <Col {...layout.rightSidebar.width}>
      <div className={styles.scrollspy}>
        {mergedProps.sections.length > 1 && (
          <EditPage githubUrl={mergedProps.githubUrl} />
        )}
        {mergedProps.sections.length > 1 &&
          mergedProps.sections.map(HeadingLink)}
      </div>
    </Col>
  );
};

export default ScrollMenu;
