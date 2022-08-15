import './body.css';
import 'antd/dist/antd.css';
import React, { useEffect, useState, useCallback } from 'react';
import '@ant-design/compatible';
import { ApolloProvider } from '@apollo/client';
import { Layout } from 'antd';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import client from './graphql/client';
import Header from './components/Header';
import { useAuth0 } from '@auth0/auth0-react';

const AppLayout = ({
  children
}) => <Layout style={{
  height: '100%'
}}>
    <Header />
    <Layout.Content>{children}</Layout.Content>
  </Layout>;

const App = ({ children }) => {
  const [ cubejsApi, setCubejsApi ] = useState(null);

  // Get all necessary auth0 data
  const {
    isLoading,
    error,
    isAuthenticated,
    loginWithRedirect,
    getAccessTokenWithPopup,
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
    const accessToken = await getAccessTokenWithPopup({
      audience: process.env.REACT_APP_AUTH0_AUDIENCE,
      scope: 'openid profile email',
    });

    setCubejsApi(cubejs({
      apiUrl: process.env.REACT_APP_API_URL,
      headers: {
        Authorization: `${accessToken}`
      },
    }));
  }, [ getAccessTokenWithPopup ]);

  // Init CubeJS instance with access_token
  useEffect(() => {
    if (!cubejsApi && !isLoading && isAuthenticated) {
      initCubejs();
    }
  }, [ cubejsApi, initCubejs, isAuthenticated, isLoading ]);

  if (error) {
    return <span>{error.message}</span>;
  }

  // show loading indicator while loading
  if (isLoading || !isAuthenticated || !cubejsApi) {
    return <span>Loading</span>;
  }

  return <CubeProvider cubejsApi={cubejsApi}>
    <ApolloProvider client={client}>
      <AppLayout>{children}</AppLayout>
    </ApolloProvider>
  </CubeProvider>;
}

export default App;