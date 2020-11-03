import React from 'react';
import { Col } from 'antd';
import cx from 'classnames';
import PropTypes from 'prop-types';

import ScrollLink from '../templates/ScrollSpyLink';

import styles from '../../../static/styles/index.module.scss';

const ScrollMenu = props => (
  <Col
    xxl={{ span: 3, offset: 1 }}
    xl={{ span: 4, offset: 1 }}
    xs={0}
  >
    <div className={styles.scrollspy}>
      {props.sections.length > 1 && props.sections.map(s => 
        <ScrollLink
          activeClass={styles.scrollspyCurrent}
          to={s.id}
          key={s.id}
          className={cx(styles.scrollspyLink, {
            [styles.scrollspySubitem]: s.type === 'h3',
            [styles.scrollspyTop]: s.id === 'top'
          })}
        >
          {s.title}
        </ScrollLink>
      )}
    </div>
  </Col>
);

ScrollMenu.propTypes = {
  sections: PropTypes.array
}

ScrollMenu.defaultProps = {
  sections: []
}

export default ScrollMenu;
