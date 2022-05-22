import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import reportWebVitals from './reportWebVitals';
import { clientWithCubeCloud } from "./ApolloClient/clientWithCubeCloud";
import { ApolloProvider } from '@apollo/client';
import createExampleWrapper from "cube-example-wrapper";

const exampleDescription = {
  title: "GraphQL API Metrics Dashboard",
  text: `A dashboard app built with Cube's GraphQL API, Postgres, Chart.js, and react-chartjs-2.`,
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
  <ApolloProvider client={clientWithCubeCloud}>
    <App />
  </ApolloProvider>,
  document.getElementById('root')
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
