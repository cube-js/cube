import React, { useCallback, useEffect, useState } from 'react';
import './App.css';
import './body.css';
import { makeStyles } from '@material-ui/core/styles';
import { Auth } from '@aws-amplify/auth';
import { CubeProvider } from '@cubejs-client/react';
import Header from './components/Header';

import { useAmplify } from './libs/amplify';
import { initCubeClient } from './init-cubejs-api';

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
  const { userData, initializing, authenticated } = useAmplify();

  useEffect(() => {
    if (!initializing && !authenticated) {
      // Redirect not logged users
      Auth.federatedSignIn();
    }
  }, [initializing, authenticated]);

  // Get CubeJS instance with access_token and set to component state
  const initCubejs = useCallback(async () => {
    setCubejsApi(initCubeClient(userData.signInUserSession.accessToken.jwtToken));
  }, [userData]);

  // Init CubeJS instance with access_token
  useEffect(
    () => {
      if (!cubejsApi && !initializing && authenticated) {
        initCubejs();
      }
    },
    [initCubejs, cubejsApi, initializing, authenticated, userData]
  );

  if (initializing || !authenticated || !cubejsApi) {
    return null;
  }

  return (
    <AppLayout>
      <CubeProvider cubejsApi={cubejsApi}>
        {children}
      </CubeProvider>
    </AppLayout>
  );
};

export default App;
