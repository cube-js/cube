import React from 'react';
import { Layout, Row, Col, Button, Icon } from 'antd';

import * as styles from '../../../static/styles/index.module.scss';
import { DocsSwitcher } from '../DocsSwitcher';
import * as switchStyles from '../DocsSwitcher/styles.module.scss';

const layout = {
  leftSidebar: {
    width: {
      xxl: 6,
      xl: 6,
      lg: 7,
      md: 7,
      xs: 24,
    },
  },
  contentArea: {
    width: {
      xxl: { span: 12, offset: 1 },
      xl: { span: 12, offset: 1 },
      lg: { span: 9, offset: 1 },
      md: { span: 7, offset: 1 },
      xs: 0,
    },
  },
  rightSidebar: {
    width: {
      xxl: { span: 4, offset: 1 },
      xl: { span: 4, offset: 1 },
      lg: { span: 6, offset: 1 },
      md: { span: 8, offset: 1 },
      xs: 0,
    },
  },
};

const DiscourseIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width="1em"
    height="1em"
    viewBox="0 0 32 32"
    fill="currentColor"
  >
    <path d="M16.135 0c8.75 0 15.865 7.313 15.865 15.995s-7.104 15.99-15.865 15.99l-16.135 0.016v-16.281c0-8.677 7.375-15.719 16.135-15.719zM16.292 6.083c-3.458-0.005-6.661 1.802-8.448 4.76-1.776 2.943-1.849 6.609-0.198 9.625l-1.781 5.677 6.396-1.432c3.656 1.635 7.953 0.901 10.844-1.854 2.896-2.734 3.818-6.969 2.318-10.661-1.51-3.703-5.12-6.12-9.12-6.115z" />
  </svg>
);

type Props = {
  className?: string;
};

const Header: React.FC<Props> = (props) => (
  <Layout.Header className={props.className}>
    <div className={styles.searchDimmer}></div>
    <Row>
      <Col {...layout.leftSidebar.width} style={{ height: 'inherit' }}>
        <div className={switchStyles.docsSwitcherWrapper}>
          <DocsSwitcher />
        </div>
      </Col>
      <Col {...layout.contentArea.width}>{props.children}</Col>
      <Col
        {...layout.rightSidebar.width}
        style={{ height: 'inherit', textAlign: 'right' }}
      >
        <div className={styles.headerButtonWrapper}>
          {/*<Button href="https://github.com/statsbotco/cube.js#community" className={styles.headerButton}>*/}
          {/*  Community*/}
          {/*</Button>*/}
          <Button
            href="https://cubejs-community.herokuapp.com/"
            target="_blank"
            className={styles.headerButton}
          >
            <Icon style={{ fontSize: '22px' }} type="slack" />
            Slack
          </Button>

          <Button
            href="https://forum.cube.dev/"
            target="_blank"
            className={styles.headerButton}
          >
            <Icon style={{ fontSize: '22px' }} component={DiscourseIcon} />
            Discourse
          </Button>

          <Button
            href="https://github.com/cube-js/cube.js"
            target="_blank"
            className={styles.headerButton}
          >
            <Icon style={{ fontSize: '22px' }} type="github" />
            GitHub
          </Button>
        </div>
      </Col>
    </Row>
  </Layout.Header>
);

export default Header;
