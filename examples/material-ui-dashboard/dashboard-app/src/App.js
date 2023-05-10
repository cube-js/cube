import React from 'react';
import { makeStyles, ThemeProvider } from '@material-ui/core/styles';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import theme from './theme';
import 'typeface-roboto';
import { Main } from './layouts';

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});

const useStyles = makeStyles((theme) => ({
  root: {
    padding: 32
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
