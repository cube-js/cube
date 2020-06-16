import React, { Component } from 'react';
import { BrowserRouter } from 'react-router-dom';
import { createBrowserHistory } from 'history';
import { Chart } from 'react-chartjs-2';
import { ThemeProvider } from '@material-ui/styles';

import { chartjs } from './helpers';
import theme from './theme';
import 'react-perfect-scrollbar/dist/css/styles.css';
import Routes from './Routes';

const browserHistory = createBrowserHistory();

Chart.helpers.extend(Chart.elements.Rectangle.prototype, {
  draw: chartjs.draw
});

export default class App extends Component {
  render() {
    return (
      <ThemeProvider theme={theme}>
        <BrowserRouter history={browserHistory} basename="/#/">
          <Routes />
        </BrowserRouter>
      </ThemeProvider>
    );
  }
}
