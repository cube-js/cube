import React, { useEffect, useState, useCallback } from 'react';
import logo from './logo.svg';
import './App.css';
import './body.css';
import { makeStyles } from '@material-ui/core/styles';
import { Layout } from 'antd';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import Header from './components/Header';
import { useAuth0 } from './react-auth0-spa';
import initCubejsApi from './init-cubejs-api';

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
  },
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

const App = ({ children }) => {
  const [cubejsApi, setCubejsApi] = useState(null);

  // Get all necessary auth0 data
  const {
    loading,
    isAuthenticated,
    loginWithRedirect,
    getTokenSilently,
    user
  } = useAuth0();

  // Force to work only for logged in users bye checking isAuthenticated
  useEffect(() => {
    if (!loading && !isAuthenticated) {
      // Redirect not logged users
      loginWithRedirect();
    }
  }, [isAuthenticated, loginWithRedirect, loading]);

  // Get CubeJS instance with access_token and set to component state
  const initCubejs = useCallback(async () => {
    const accessToken = await getTokenSilently();

    setCubejsApi(await initCubejsApi(accessToken));
  }, [getTokenSilently]);

  // Init CubeJS instance with access_token
  useEffect(() => {
    if (!cubejsApi && !loading && isAuthenticated) {
      initCubejs();
    }
  }, [cubejsApi, initCubejs, isAuthenticated, loading]);

  // show loading indicator while loading
  if (loading || !isAuthenticated || !cubejsApi) {
    return <span>Loading</span>;
  }

  return (
    <CubeProvider cubejsApi={cubejsApi}>
      <AppLayout>
        { JSON.stringify(user) }
        {children}
      </AppLayout>
    </CubeProvider>
  );
};

export default App;
