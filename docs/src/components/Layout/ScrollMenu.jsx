import React from 'react';
import { Col, Icon } from 'antd';
import cx from 'classnames';
import PropTypes from 'prop-types';

import ScrollLink from '../templates/ScrollSpyLink';

import styles from '../../../static/styles/index.module.scss';

const EditPage = ({ githubUrl }) => {
  return (
    <div className={styles.scrollspyLinkWrapper}>
      <p className={cx(styles.editPage, styles.scrollspyLink)}>
        <a href={githubUrl}> <Icon type="github" width={20} height={20} /> Edit this page </a>
      </p>
    </div>
  );
};

const HeadingLink = ({ id, title, type }) => {
  return (
    <ScrollLink
      activeClass={styles.scrollspyCurrent}
      to={id}
      key={id}
      className={cx(styles.scrollspyLink, {
        [styles.scrollspySubitem]: type === 'h3',
        [styles.scrollspyTop]: id === 'top'
      })}
    >
      {title}
    </ScrollLink>
  );
};

const ScrollMenu = props => {
  return (
    <Col
      xxl={{ span: 3, offset: 1 }}
      xl={{ span: 4, offset: 1 }}
      xs={0}
    >
      <div className={styles.scrollspy}>
        {props.sections.length > 1 && <EditPage githubUrl={props.githubUrl} />}
        {props.sections.length > 1 && props.sections.map(HeadingLink)}
      </div>
    </Col>
  );
}

ScrollMenu.propTypes = {
  githubUrl: PropTypes.string,
  sections: PropTypes.array,
}

ScrollMenu.defaultProps = {
  sections: []
}

export default ScrollMenu;
