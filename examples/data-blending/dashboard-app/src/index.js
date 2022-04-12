import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import Wrapper from "cube-example-wrapper"

const root = document.getElementById('root')

const exampleDescription = {
  title: "Data Blending",
  text: "This example shows Data Blending vizualization built with Cube.js and React",
  tutorialLabel: "story",
  tutorialSrc: "https://cube.dev/blog/introducing-data-blending-api/",
  sourceCodeSrc: "https://github.com/cube-js/cube.js/tree/master/examples/data-blending",
};
const cubeExampleWrapper = new Wrapper(exampleDescription)
cubeExampleWrapper.render(root);

ReactDOM.render(
  <React.StrictMode>
    <App></App>
  </React.StrictMode>,
  root
);
