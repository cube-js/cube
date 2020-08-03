import React from 'react';
import { Link } from 'react-router-dom';
import { Layout } from 'antd';
import * as Icon from '@ant-design/icons';
import logo from './../logo.svg';

export default () => (
  <Layout.Header className='example__header'>
    <div className='examples__nav'>
      <a href='//cube.dev' target='_blank'>
        <img src={logo} alt='Cube.js' />
      </a>
      <div className='examples__nav__buttons'>
        <a href='//github.com/statsbotco/cube.js'>
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
