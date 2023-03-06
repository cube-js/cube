import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import * as serviceWorker from './serviceWorker';
import createExampleWrapper from "@cube-dev/example-wrapper";

createExampleWrapper({
    title: "AWS Web Analytics Dashboard",
    text: `This example project contains a web analytics POC built with Cube<br>from the <a href="https://cube.dev/blog/building-open-source-google-analytics-from-scratch/">Building Open Source Google Analytics from Scratch</a>`
});

ReactDOM.render(<App />, document.getElementById('root'));

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
