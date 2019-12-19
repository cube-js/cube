import React from "react";
import "./App.css";
import "./body.css";
import { makeStyles } from "@material-ui/core/styles";
import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";
import Header from "./components/Header";
let API_URL;
if (process.env.NODE_ENV === 'production') {
  API_URL = window.location.origin;
} else {
  API_URL = "http://localhost:4000";
}
const CUBEJS_TOKEN =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1NzY3MTExODl9.K3-dV7mFDCmAL8LAzPSjdNWp9JCoeVczZ3IgnTTH94I";
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});
const useStyles = makeStyles(theme => ({
  root: {
    flexGrow: 1
  }
}));

const AppLayout = ({ children }) => {
  const classes = useStyles();
  return (
    <div className={classes.root}>
      <Header />
      <div>{children}</div>
    </div>
  );
};

const App = ({ children }) => (
  <CubeProvider cubejsApi={cubejsApi}>
    <AppLayout>{children}</AppLayout>
  </CubeProvider>
);

export default App;
