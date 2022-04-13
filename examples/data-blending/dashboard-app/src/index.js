import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import createExampleWrapper from "cube-example-wrapper"

const exampleDescription = {
  title: "Data Blending",
  text: "This example shows Data Blending vizualization built with Cube.js and React",
  tutorialLabel: "story",
  tutorialSrc: "https://cube.dev/blog/introducing-data-blending-api/",
  sourceCodeSrc: "https://github.com/cube-js/cube.js/tree/master/examples/data-blending",
};
createExampleWrapper(exampleDescription)

ReactDOM.render(
  <React.StrictMode>
    <App></App>
  </React.StrictMode>,
  document.getElementById('root')
);
