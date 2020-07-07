import React from 'react';
import './App.css';
import './body.css';
import { makeStyles, ThemeProvider } from '@material-ui/core/styles';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import theme from './theme';
import 'typeface-roboto';
import { Main } from './layouts';

const API_URL = 'http://localhost:4000';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTMxODM0MzYsImV4cCI6MTU5MzI2OTgzNn0.KRPeq-JYxBx4Ow-9Dkxz6s8aCV_h9A3TB353eIabUNk';
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`,
});

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
  },
}));

const AppLayout = ({ children }) => {
  const classes = useStyles();
  return (
    <ThemeProvider theme={theme}>
      <Main>
        <div className={classes.root}>
          <div>{children}</div>
        </div>
      </Main>
    </ThemeProvider>
  );
};

const App = ({ children }) => (
  <CubeProvider cubejsApi={cubejsApi}>
    <AppLayout>{children}</AppLayout>
  </CubeProvider>
);

export default App;
