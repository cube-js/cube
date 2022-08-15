import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';

import createExampleWrapper from "@cube-dev/example-wrapper";

const exampleDescription={
  title: "Data Blending",
  text: `
    <p>This live demo shows 
      <a href="https://cube.dev/docs/schema/advanced/data-blending">data blending</a> 
      approach built with Cube, React, and Highcharts.
    </p>
    <p>You can use it to merge several data sources in a single chart by time dimensions.</p>
    <p>Read 
      the <a href="https://cube.dev/blog/introducing-data-blending-api">story</a> 
      or explore 
      the <a href="https://github.com/cube-js/cube.js/tree/master/examples/data-blending">source code</a> 
      to learn more.
    </p>
  `
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
  <React.StrictMode>
    <App></App>
  </React.StrictMode>,
  document.getElementById('root')
);
