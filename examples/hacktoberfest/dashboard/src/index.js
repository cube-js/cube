import React from 'react';
import ReactDOM from 'react-dom';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import 'antd/dist/antd.css';
import './index.css';
import App from './App';

const API_URL =
  process.env.NODE_ENV === 'production'
    ? 'https://serious-tobias.gcp-us-central1.cubecloudapp.dev'
    : 'http://localhost:4000';

const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2Mjc0NjM2MDZ9.1boj2JrVcsxVkbQsZxuOP21VDxNQyHpxrh3go45k9pc';

const api = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`,
});

ReactDOM.render(
  <React.StrictMode>
    <CubeProvider cubejsApi={api}>
      <App />
    </CubeProvider>
  </React.StrictMode>,
  document.getElementById('root')
);
