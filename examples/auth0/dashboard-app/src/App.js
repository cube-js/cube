import React, { useEffect, useState, useCallback } from 'react';
import './App.css';
import './body.css';
import { makeStyles } from '@material-ui/core/styles';
import { CubeProvider } from '@cubejs-client/react';
import { useAuth0 } from "@auth0/auth0-react";
import Header from './components/Header';
import initCubejsApi from './init-cubejs-api';
import config from './auth_config';

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
    isLoading,
    error,
    isAuthenticated,
    loginWithRedirect,
    getAccessTokenSilently,
    user
  } = useAuth0();

  // Force to work only for logged in users bye checking isAuthenticated
  useEffect(() => {
    if (!isLoading && !isAuthenticated) {
      // Redirect not logged users
      loginWithRedirect();
    }
  }, [isAuthenticated, loginWithRedirect, isLoading]);

  // Get CubeJS instance with access_token and set to component state
  const initCubejs = useCallback(async () => {
    const accessToken = await getAccessTokenSilently({
      audience: config.audience,
      scope: config.scope
    });

    setCubejsApi(initCubejsApi(accessToken));
  }, [getAccessTokenSilently]);

  // Init CubeJS instance with access_token
  useEffect(() => {
    if (!cubejsApi && !isLoading && isAuthenticated) {
      initCubejs();
    }
  }, [cubejsApi, initCubejs, isAuthenticated, isLoading]);

  if (error) {
    return <span>{error.message}</span>;
  }

  // show loading indicator while loading
  if (isLoading || !isAuthenticated || !cubejsApi) {
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
