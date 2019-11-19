import './body.css';
import { makeStyles } from '@material-ui/core/styles';
import React from "react";
import { Layout } from "antd";
import cubejs from '@cubejs-client/core';
import { CubeProvider } from "@cubejs-client/react";
import Header from "./components/Header";

const API_URL = undefined;

const CUBEJS_TOKEN = undefined;

const cubejsApi = cubejs(
  CUBEJS_TOKEN,
  { apiUrl: `${API_URL}/cubejs-api/v1` }
);

const useStyles = makeStyles(theme => ({
  root: {
    flexGrow: 1,
  }
}));

const AppLayout = ({ children }) => {
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <Header />
      <div>{children}</div>
    </div>
  )
};

const App = ({ children }) => (
  <CubeProvider cubejsApi={cubejsApi}>
    <AppLayout>
      {children}
    </AppLayout>
  </CubeProvider>
);

export default App;
