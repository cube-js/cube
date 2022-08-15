import createExampleWrapper from "@cube-dev/example-wrapper";
import React from 'react';
import ReactDOM from 'react-dom';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import 'antd/dist/antd.css';
import './index.css';
import App from './App';

createExampleWrapper({
  title: "Open Source Story of Hacktoberfest 2020",
  text: `This story reveals the unofficial results of <a href='https://hacktoberfest.digitalocean.com' target='_blank' rel='noreferrer'>Hacktoberfest 2020</a> based on a subset of public data.`,
});

const api = cubejs(process.env.REACT_APP_CUBE_TOKEN, {
  apiUrl: `${process.env.REACT_APP_CUBE_API}`,
});

ReactDOM.render(
  <React.StrictMode>
    <CubeProvider cubejsApi={api}>
      <App />
    </CubeProvider>
  </React.StrictMode>,
  document.getElementById('root')
);
