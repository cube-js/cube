import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';

import createExampleWrapper from "@cube-dev/example-wrapper";

const exampleDescription = {
  title: "Compare Date Range",
  text: `
    <p>This live demo shows 
      <a href="https://cube.dev/docs/query-format#time-dimensions-format">data comparison over time periods</a> 
      built with Cube and Highcharts.
    </p>
    <p>You can use it to calculate YoY or MoM growth.</p>
    <p>Read 
      the <a href="https://cube.dev/blog/comparing-data-over-different-time-periods">story</a> 
      or explore 
      the <a href="https://github.com/cube-js/cube.js/tree/master/examples/compare-date-range">source code</a> 
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
