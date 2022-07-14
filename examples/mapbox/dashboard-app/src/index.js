import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import { HashRouter as Router } from 'react-router-dom';

import createExampleWrapper from "@cube-dev/example-wrapper";

const exampleDescritpion = {
  title: "Mapbox",
  text: `<p>This live demo shows a map-based data visualization created with Mapbox, Cube, and React.</p>
    <p>
      Follow 
      the <a href="https://mapbox-guide.cube.dev/">tutorial</a> 
      or explore 
      the <a href="https://github.com/cube-js/cube.js/tree/master/guides/mapbox">source code</a> 
      to learn more.
    </p>`
};

createExampleWrapper(exampleDescritpion);

ReactDOM.render(
  <React.StrictMode>
    <Router>
      <App></App>
    </Router>
  </React.StrictMode>,
  document.getElementById('root')
);
