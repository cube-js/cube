import React from 'react';
import { Layout } from 'antd';
import * as Icon from '@ant-design/icons';
import logo from './../logo.svg';

const Header = () => (
  <Layout.Header className='example__header'>
    <div className='examples__nav'>
      <div className='examples__title'>
        <a href='//cube.dev' target='_blank' rel='noopener noreferrer'>
          <img src={logo} alt='Cube.js' />
        </a>
        <h1>Data Blending example</h1>
      </div>
      <div className='examples__nav__buttons'>
        <a href='//github.com/cube-js/cube.js/tree/master/examples/compare-date-range'>
          <Icon.GithubOutlined />
          Github
        </a>
        <a href='//slack.cube.dev'>
          <Icon.SlackOutlined />
          Slack
        </a>
      </div>
    </div>
  </Layout.Header>
);

export default Header;
