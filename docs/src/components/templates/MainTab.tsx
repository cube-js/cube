import React from 'react';
import Link from 'gatsby-link';
import cx from 'classnames';
import { Row, Col } from 'antd';

import * as styles from '../../../static/styles/index.module.scss';

type Props = {
  title: string;
  to: string;
  desc: string;
  img: string;
  right?: boolean;
};

const defaultProps: Partial<Props> = {
  right: false,
};

const MainTab: React.FC<Props> = (props) => {
  const mergedProps = { ...defaultProps, ...props };

  return (
    <Col
      xl={12}
      md={24}
      sm={24}
      xs={24}
      className={cx(styles.mainTab, {
        [styles.mainTabRight]: mergedProps.right,
      })}
    >
      <Link to={mergedProps.to}>
        <Row className={styles.mainTabContent}>
          <Col span={6}>
            <img
              className={styles.mainTabImg}
              src={mergedProps.img}
              alt={mergedProps.title}
            />
          </Col>
          <Col span={18} style={{ paddingTop: 10, paddingRight: 10 }}>
            <span className={styles.mainTabLink}>{mergedProps.title}</span>
            <div className={styles.mainTabText}>{mergedProps.desc}</div>
          </Col>
        </Row>
      </Link>
    </Col>
  );
};

export default MainTab;
