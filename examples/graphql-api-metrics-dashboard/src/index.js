import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './App';
import reportWebVitals from './reportWebVitals';
import { clientWithCubeCloud } from "./ApolloClient/clientWithCubeCloud";
import { ApolloProvider } from '@apollo/client';
import createExampleWrapper from "@cube-dev/example-wrapper";

const exampleDescription = {
  title: "Chart.js Metrics Dashboard",
  text: `
    <p>This live demo shows a metrics dashboard built with Cube's GraphQL API, PostgreSQL as a database, Chart.js and react-chartjs-2 for data visualization, and Cube.</p>
    <p>
      Follow 
      the <a href="https://cube.dev/blog/graphql-postgres-metrics-dashboard-with-cube">tutorial</a>
      or explore 
      the <a href="https://github.com/cube-js/cube.js/tree/master/examples/graphql-api-metrics-dashboard">source code</a>
      to learn more.
    </p>`,
};

createExampleWrapper(exampleDescription);

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <ApolloProvider client={clientWithCubeCloud}>
    <App />
  </ApolloProvider>
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
