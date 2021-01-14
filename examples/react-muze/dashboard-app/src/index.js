import React from 'react';
import ReactDOM from 'react-dom';
import { Layout } from 'antd';
import 'antd/dist/antd.less';
import App from './App';
import Header from './Header';
import reportWebVitals from './reportWebVitals';
import { layout, content } from './index.module.less';
import "./index.less";

const { Content } = Layout;

ReactDOM.render(
  <Layout className={layout}>
    <Header />
    <Content className={content}>
      <App />
    </Content>
  </Layout>,
  document.getElementById('root')
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
