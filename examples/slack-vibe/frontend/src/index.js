import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';

import createExampleWrapper from '@cube-dev/example-wrapper';

const exampleDescription={
  title: "Slack Vibe  🎉",
  text: `
    <p>
      An open source dashboard of public activity in a Slack <br />
      workspace of an open community or a private team. 
    </p>
    <p>
      Read
      the <a href="https://dev.to/cubejs/slack-vibe-the-open-source-analytics-for-slack-2khl">story</a>
      or explore
      the <a href="https://github.com/cube-js/cube.js/tree/master/examples/slack-vibe">source code</a>
      to learn more.
    </p>
  `
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
  document.getElementById('root')
);
