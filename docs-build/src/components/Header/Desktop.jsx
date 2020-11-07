import React from 'react';
import { Layout, Row, Col, Button, Icon } from 'antd';
import PropTypes from 'prop-types';

import logo from '../../pages/images/Logo.png';

import styles from '../../../static/styles/index.module.scss';

const { Header: AntHeader } = Layout;

const Header = props => (
  <AntHeader className={props.className}>
    <div className={styles.searchDimmer}></div>
    <Row>
      <Col
        xxl={{ span: 4, offset: 1 }}
        xl={{ span: 5, offset: 1 }}
        lg={{ span: 7, offset: 1 }}
        md={{ span: 9, offset: 1 }}
        xs={24}
      >
        <div className={styles.logoWrapper}>
          <a href="/" className={styles.logo}>
            <img src={logo} alt="Logo" style={{ height: 36 }} />
          </a>
          &nbsp;
          <a href="/docs" className={styles.logo}>
            <span className={styles.logoDocs}>docs</span>
          </a>
        </div>
      </Col>
      <Col
        xxl={15}
        xl={13}
        lg={10}
        md={14}
        xs={0}
      >
        {props.children}
      </Col>
      <Col
        xxl={4}
        xl={5}
        lg={5}
        xs={0}
      >
        <div style={{ display: 'flex', justifyContent: 'center' }}>
          {/*<Button href="https://github.com/statsbotco/cube.js#community" className={styles.headerButton}>*/}
          {/*  Community*/}
          {/*</Button>*/}
          <Button href="https://cubejs-community.herokuapp.com/" target="_blank" className={styles.headerButton}>
            <Icon style={{ fontSize: '22px' }} type="slack"/>
            Slack
          </Button>
          <Button href="https://github.com/cube-js/cube.js" target="_blank" className={styles.headerButton}>
            <Icon style={{ fontSize: '22px' }} type="github"/>
            Github
          </Button>
        </div>
      </Col>
    </Row>
  </AntHeader>
);

Header.propTypes = {
  className: PropTypes.string
}

Header.defaultProps = {
  className: ''
}

export default Header;
