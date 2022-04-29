import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';

import createExampleWrapper from 'cube-example-wrapper'

const exampleDescription={
  title: "Slack Vibe  🎉",
  text: `
    An open source dashboard of public activity in a Slack workspace 
    of an open community or a private team
  `
}

createExampleWrapper(exampleDescription)

ReactDOM.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
  document.getElementById('root')
);
