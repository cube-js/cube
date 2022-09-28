import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import 'bootstrap/dist/css/bootstrap.min.css';
import App from './App';
import reportWebVitals from './reportWebVitals';
import createExampleWrapper from "@cube-dev/example-wrapper";
createExampleWrapper({
  title: "ClickHouse vs. MySQL, built with Cube and multi-tenancy",
  text: `This sample data app displays airline data from the <br>Bureau of Transportation Statistics by comparing the performance between MySQL and ClickHouse.`
});

ReactDOM.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
  document.getElementById('root')
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
