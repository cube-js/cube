import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';

import CubeExampleWrapper from "cube-example-wrapper";
import "cube-example-wrapper/public/style.css";

CubeExampleWrapper.description = {
  title: "Data Blending",
  text: "This example shows Data Blending vizualization built with Cube.js and React",
  tutorialLabel: "story",
  tutorialSrc: "https://cube.dev/blog/introducing-data-blending-api/",
  sourceCodeSrc: "https://github.com/cube-js/cube.js/tree/master/examples/data-blending",
};
CubeExampleWrapper.render("root");


ReactDOM.render(
  <React.StrictMode>
    <App></App>
  </React.StrictMode>,
  document.getElementById('root')
);
