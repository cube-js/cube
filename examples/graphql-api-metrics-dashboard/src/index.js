import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import reportWebVitals from './reportWebVitals';
import { client } from "./ApolloClient/client";
import { clientWithJwt } from "./ApolloClient/clientWithJwt";
import { clientWithCubeCloud } from "./ApolloClient/clientWithCubeCloud";
import { ApolloProvider } from '@apollo/client';

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
