import React from "react";
import { withRouter } from "react-router";
import { Layout } from "antd";
import { ApolloProvider as ApolloHooksProvider } from "@apollo/react-hooks";
import { ApolloProvider } from "react-apollo";
import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";
import client from "./graphql/client";

import Header from './components/Header';
// import aws_exports from './aws-exports';

const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2MDY1OTA0NjEsImV4cCI6MTkyMjE2NjQ2MX0.DdY7GaiHsQWyTH_xkslHb17Cbc3yLFfMFwoEpx89JiA'

const cubejsApi = cubejs(token, {
  apiUrl: 'https://aquamarine-galliform.aws-us-east-2.cubecloudapp.dev/cubejs-api/v1'
});

// Amplify.configure(aws_exports);

//const client = new AWSAppSyncClient(
//  {
//    disableOffline: true,
//    url: aws_exports.aws_appsync_graphqlEndpoint,
//    region: aws_exports.aws_appsync_region,
//    auth: {
//      type: AUTH_TYPE.AMAZON_COGNITO_USER_POOLS,
//      jwtToken: async () => (await Auth.currentSession()).getIdToken().getJwtToken()
//    },
//  },
//  { cache: new InMemoryCache() }
//);

// Hub.listen('auth', (data) => {
//   if (data.payload.event === 'signOut') {
//     client.resetStore();
//   }
// });

const AppLayout = ({ location, children }) => (
  <Layout style={{ height: "100%" }}>
    <Header location={location} />
    <Layout.Content>{children}</Layout.Content>
  </Layout>
);

const App = withRouter(({ location, children }) => (
  <CubeProvider cubejsApi={cubejsApi}>
    <ApolloProvider client={client}>
      <ApolloHooksProvider client={client}>
          <AppLayout location={location}>{children}</AppLayout>
      </ApolloHooksProvider>
    </ApolloProvider>
  </CubeProvider>
));

export default App;
