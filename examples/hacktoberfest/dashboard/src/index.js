import createExampleWrapper from "cube-example-wrapper";
import React from 'react';
import ReactDOM from 'react-dom';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import 'antd/dist/antd.css';
import './index.css';
import App from './App';

createExampleWrapper({
  title: "demo title",
  text: `demo description text`,
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
