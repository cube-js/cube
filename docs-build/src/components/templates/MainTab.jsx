import React from 'react';
import Link from 'gatsby-link';
import cx from 'classnames';
import PropTypes from 'prop-types';
import { Row, Col } from 'antd';

import styles from '../../../static/styles/index.module.scss';

const MainTab = props => (
  <Col
    xl={12}
    md={24}
    sm={24}
    xs={24}
    className={cx(styles.mainTab, {[styles.mainTabRight]: props.right })}
  >
    <Link to={props.to}>
      <Row className={styles.mainTabContent}>
        <Col span={6}>
          <img className={styles.mainTabImg} src={props.img} alt={props.title} />
        </Col>
        <Col span={18} style={{ paddingTop: 10, paddingRight: 10 }}>
          <span className={styles.mainTabLink}>{props.title}</span>
          <div className={styles.mainTabText}>{props.desc}</div>
        </Col>
      </Row>
    </Link>
  </Col>
)

MainTab.propTypes = {
  title: PropTypes.string.isRequired,
  to: PropTypes.string.isRequired,
  desc: PropTypes.string.isRequired,
  img: PropTypes.string.isRequired,
  right: PropTypes.bool
}

MainTab.defaultProps = {
  right: false
}

export default MainTab;
