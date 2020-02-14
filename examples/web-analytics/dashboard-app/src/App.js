import React from "react";
import logo from "./logo.svg";
import "./App.css";
import "./body.css";
import { makeStyles } from "@material-ui/core/styles";
import { Layout } from "antd";
import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";
import { MuiPickersUtilsProvider } from "@material-ui/pickers";
import MomentUtils from '@date-io/moment';

import Header from "./components/Header";
import SidePanel from "./components/SidePanel";

const API_URL = process.env.NODE_ENV === 'production' ? '' : "http://localhost:4000";
const CUBEJS_TOKEN =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1ODE0NzY4MjZ9.31_4J0M0oqqkwtur2_gaX7fhL6vOOLjKk_HvFEZjeq0";
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});
const useStyles = makeStyles(theme => ({
  root: {
    display: 'flex'
  },
  content: {
    flexGrow: 1,
    padding: theme.spacing(3),
  },
  toolbar: theme.mixins.toolbar,
}));

const AppLayout = ({ children }) => {
  const classes = useStyles();
  return (
    <div className={classes.root}>
      <Header />
      <SidePanel />
      <div className={classes.content}>
        <div className={classes.toolbar} />
        {children}
      </div>
    </div>
  );
};

const App = ({ children }) => (
  <MuiPickersUtilsProvider utils={MomentUtils}>
    <CubeProvider cubejsApi={cubejsApi}>
      <AppLayout>{children}</AppLayout>
    </CubeProvider>
  </MuiPickersUtilsProvider>
);

export default App;
