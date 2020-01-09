---
title: "Authentication and GraphQL API"
order: 2
---

Now we have a basic version of our app, which uses local storage to save charts on the dashboard. It is handy for development and prototyping, but is not suitable for real-world use cases. We want to let our users create dashboards and not lose them when they change the browser.

To do so, we first need to add authentication to our application and then save the users’ dashboard in the database. We are going to use [AWS Cognito](https://aws.amazon.com/cognito/) for authentication. AWS Cognito User Pool makes it easy for developers to add sign-up and sign-in functionality to web and mobile applications. It supports user registration and sign-in, as well as provisioning identity tokens for signed-in users.

To store the dashboards, we will use [AWS AppSync](https://aws.amazon.com/appsync/). It allows us to create a flexible API to access and manipulate data and uses GraphQL as a query language. AppSync natively integrates with Cognito and can use its identity tokens to manage the ownership of the data—and in our case, the ownership of the dashboards. As a prerequisite to this part you need to have an AWS account, so you can use its services.

Besides AWS AppSync you can use any other GraphQL server to persist your dashboard data and athenticate/authorize your users.
Cube.js itself doesn't have any dependencies on dashboard data persistance and it's completely up to your frontend application on how to handle this implementation.

## Install and Configure Amplify CLI

I highly recommend using [Yarn](https://yarnpkg.com) instead of NPM while working with our
dashboard app. It is better at managing dependieces, and specifically in our
case we'll use some of its features such as
[resolutions](https://yarnpkg.com/lang/en/docs/selective-version-resolutions/) to make sure all the
dependieces are installed correctly.

To switch to Yarn, delete `node/_modules` folder and `package-lock.json` inside `dashboard-folder`

```bash
$ cd dashboard-app && rm -rf node_modules && rm package-lock.json
```

To configure all these services, we will use AWS Amplify and its CLI tool. It uses AWS CloudFormation and enables us to easily add and modify backend configurations. First, let’s install the CLI itself.

```bash
$ yarn global add @aws-amplify/cli
```

Once installed, we need to setup the CLI with the appropriate permissions (a handy step by step video tutorial is also available [here](https://www.youtube.com/watch?v=fWbM5DLh25U)). Execute the following command to configure Amplify. It will prompt the creation of an IAM User in the AWS Console—once you create it, just copy and paste the credentials and select a profile name.

```bash
$ amplify configure
```

To initialize Amplify in our application, run the following command inside the `dashboard-app` folder.

```bash
$ cd project-folder/dashboard-app
$ amplify init
```

## Create and Deploy AppSync GraphQL API

Next, let’s add Cognito and AppSync GraphQL API.

```bash
$ amplify add api
? Please select from one of the below mentioned services GraphQL
? Provide API name: yourAppName
? Choose the default authorization type for the API Amazon Cognito User Pool
Using service: Cognito, provided by: awscloudformation

 The current configured provider is Amazon Cognito.

 Do you want to use the default authentication and security configuration? Default configuration
 Warning: you will not be able to edit these selections.
 How do you want users to be able to sign in? Email
 Do you want to configure advanced settings? No, I am done.
Successfully added auth resource
? Do you want to configure advanced settings for the GraphQL API? No, I am done.
? Do you have an annotated GraphQL schema? No
? Do you want a guided schema creation? Yes
? What best describes your project: Single object with fields (e.g., “Todo” with ID, name, description)
? Do you want to edit the schema now? Yes
```

At this point, your default editor will be opened. Delete the provided sample GraphQL schema and replace it with:

```graphql
type DashboardItem @model @auth(rules: [{allow: owner}]) {
  id: ID!
  name: String
  layout: AWSJSON
  vizState: AWSJSON
}
```

Back to the terminal, finish running the command and then execute:

```bash
$ amplify push
? Do you want to generate code for your newly created GraphQL API No
```

The command above will configure and deploy the Cognito Users Pool and the AppSync GraphQL API backend by DynamoDB table. It will also wire up everything together, so Cognito’s tokens can be used to control the ownership of the dashboard items.

After everything is deployed and set up, the identifiers for each resource are automatically added to a local `aws_exports.js` file that is used by AWS Amplify to reference the specific Auth and API cloud backend resources.

## Cube.js Backend Authentication

We're going to use Cognito's identity tokens to manage access to Cube.js and the
underlying analytics data. Cube.js comes with a flexible [security
model](https://cube.dev/docs/security), designed to manage access to the data on
different levels. The usual flow is to use JSON Web Tokens (JWT) for
the authentication/authorization. The JWT tokens can carry a payload, such as a user
ID, which can then be passed to the data schema as a [security context](https://cube.dev/docs/security#security-context) to restrict access to some part
of the data.

In our tutorial, we're not going to restrict users to access
data, but we'll just authenticate them based on JWT tokens from Cognito. When a user
signs in to our app, we'll request a JWT token for that user and then sign all
the requests to the Cube.js backend with this token.

To verify the token on the Cube.js side, we need to download the public JSON Web Key Set (JWKS) for our Cognito User Pool. It is a JSON file and you can locate it at `https://cognito-idp.{region}.amazonaws.com/{userPoolId}/.well-known/jwks.json`.

You can find `region` and `userPoolId` in your `src/aws_exports.js`. Your file
should look like the following, just copy the region and user pool id values.

```javascript
// WARNING: DO NOT EDIT. This file is automatically generated by AWS Amplify. It will be overwritten.

const awsmobile = {
    "aws_project_region": "XXX",
    "aws_cognito_identity_pool_id": "XXX",
    "aws_cognito_region": "REGION",
    "aws_user_pools_id": "USER-POOL-ID",
    "aws_user_pools_web_client_id": "XXX",
    "oauth": {},
    "aws_appsync_graphqlEndpoint": "XXX",
    "aws_appsync_region": "XXX",
    "aws_appsync_authenticationType": "XXX"
};

export default awsmobile;
```

Next, run the following command in the terminal to download JWKS into the root folder of your project. Make sure to replace `region` and `userPoolId` with the values from `aws_exports.js`.

```bash
$ cd react-dashboard
$ curl https://cognito-idp.{region}.amazonaws.com/{userPoolId}/.well-known/jwks.json > jwks.json
```

Now, we can use the JWKS to verify the JWT token from the client. Cube.js Server has the `checkAuth` option for this purpose. It is a function that accepts an `auth` token and expects you to provide a security context for the schema or throw an error in case the token is not valid.

Let's first install some packages we would need to work with JWT. Run the
following command in your project root folder.

```bash
$ npm install -s jsonwebtoken jwk-to-pem lodash
```

Now, we need to update the `index.js` file, which starts a Cube.js Backend. Replace
the content of the `index.js` file with the following. Make sure to make these
changes in the Cube.js root folder and not in the `dashboard-app` folder.

```javascript
const CubejsServer = require("@cubejs-backend/server");
const fs = require("fs");
const jwt = require("jsonwebtoken");
const jwkToPem = require("jwk-to-pem");
const jwks = JSON.parse(fs.readFileSync("jwks.json"));
const _ = require("lodash");

const server = new CubejsServer({
  checkAuth: async (req, auth) => {
    const decoded = jwt.decode(auth, { complete: true });
    const jwk = _.find(jwks.keys, x => x.kid === decoded.header.kid);
    const pem = jwkToPem(jwk);
    req.authInfo = jwt.verify(auth, pem);
  }
});

server.listen().then(({ port }) => {
  console.log(`🚀 Cube.js server is listening on ${port}`);
});
```

Here we first decode the incoming JWT token to find its `kid`. Then, based on
the `kid` we pick a corresponding JWK and convert it into PEM. And, finally,
verify the token. If either the decode or verification process fails, the error will
be thrown.

That's all on the backend side. Now, let's add the authentication to our
frontend app.

## Add Authentication to the App

First, we need to install Amplify and AppSync-related dependencies to make our application work with a backend we just created. It is currently known that some versions conflict in the packages, so please make sure to install specific versions as listed below. To solve this issue, we'll use [Yarn resolutions](https://yarnpkg.com/lang/en/docs/selective-version-resolutions/) feature and specify a version of `apollo-client` we need to use. Open your `package.json` file and add the following property.

```json
"resolutions": {
  "apollo-client": "2.6.3"
}
```

Then, install the following packages.

```bash
$ yarn add apollo-client aws-amplify aws-amplify-react aws-appsync aws-appsync-react react-apollo@2.5.8
```


Now we need to update our `App.js` to add Cognito authentication and AppSync GraphQL API. First, we are wrapping our App with `withAuthenticator` HOC. It will handle sign-up and sign-in in our application. You can customize the set of the fields in the forms or completely rebuild the UI. [Amplify documentation](https://aws-amplify.github.io/docs/js/authentication#using-auth-components-in-react--react-native) covers authentication configuration and customization.

Next, we are initiating the `AWSAppSyncClient` client to work with our AppSync backend. It is going to use credentials from Cognito to access data in AppSync and scope it on a per-user basis.

Update the content of the `src/App.js` file with the following.

```jsx
import React from "react";
import { withRouter } from "react-router";
import { Layout } from "antd";
import { InMemoryCache } from "apollo-cache-inmemory";
import { ApolloProvider as ApolloHooksProvider } from "@apollo/react-hooks";
import { ApolloProvider } from "react-apollo";
import AWSAppSyncClient, { AUTH_TYPE } from "aws-appsync";
import { Rehydrated } from "aws-appsync-react";
import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";
import { withAuthenticator } from "aws-amplify-react";
import Amplify, { Auth, Hub } from 'aws-amplify';

import Header from './components/Header';
import aws_exports from './aws-exports';

const API_URL = "http://localhost:4000";
const cubejsApi = cubejs(
  async () => (await Auth.currentSession()).getIdToken().getJwtToken(),
  { apiUrl: `${API_URL}/cubejs-api/v1` }
);

Amplify.configure(aws_exports);

const client = new AWSAppSyncClient(
  {
    disableOffline: true,
    url: aws_exports.aws_appsync_graphqlEndpoint,
    region: aws_exports.aws_appsync_region,
    auth: {
      type: AUTH_TYPE.AMAZON_COGNITO_USER_POOLS,
      jwtToken: async () => (await Auth.currentSession()).getIdToken().getJwtToken()
    },
  },
  { cache: new InMemoryCache() }
);

Hub.listen('auth', (data) => {
  if (data.payload.event === 'signOut') {
    client.resetStore();
  }
});

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
        <Rehydrated>
          <AppLayout location={location}>{children}</AppLayout>
        </Rehydrated>
      </ApolloHooksProvider>
    </ApolloProvider>
  </CubeProvider>
));

export default withAuthenticator(App, {
  signUpConfig: {
    hiddenDefaults: ["phone_number"]
  }
});
```

## Update GraphQL Queries and Mutations

The next step is to update our GraphQL queries and mutations to work with the just-created AppSync backend.

Replace the content of the `src/graphql/mutations.js` file with following.

```javascript
import gql from "graphql-tag";

export const CREATE_DASHBOARD_ITEM = gql`
  mutation CreateDashboardItem($input: CreateDashboardItemInput!) {
    createDashboardItem(input: $input) {
      id
      layout
      vizState
      name
    }
  }
`;

export const UPDATE_DASHBOARD_ITEM = gql`
  mutation UpdateDashboardItem($input: UpdateDashboardItemInput!) {
    updateDashboardItem(input: $input) {
      id
      layout
      vizState
      name
    }
  }
`;

export const DELETE_DASHBOARD_ITEM = gql`
  mutation DeleteDashboardItem($id: ID!) {
    deleteDashboardItem(input: { id: $id }) {
      id
      layout
      vizState
      name
    }
  }
`;
```

And then replace `src/graphql/queries.js` with the following.

```javascript
import gql from "graphql-tag";

export const GET_DASHBOARD_ITEMS = gql`query ListDashboardItems {
    listDashboardItems {
      items {
        id
        layout
        vizState
        name
      }
    }
  }
`

export const GET_DASHBOARD_ITEM = gql`query GetDashboardItem($id: ID!) {
    dashboardItem: getDashboardItem(id: $id) {
      id
      layout
      vizState
      name
    }
  }
`;
```

Our new updated queries are a little bit different from the original ones. We need to make some small updates to our components’ code to make it work new queries and mutations.

First, in the `src/components/Dashboard.js` and `src/components/TitleModal.js` files, change how the variables are passed to the `updateDashboardItem` function.

```javascript
// on the line 30 in src/components/Dashboard.js
// update the variables passed to `updateDashboardItem` function
updateDashboardItem({
  variables: {
    input: {
      id: item.id,
      layout: toUpdate
    }
  }
});

// Similarly update variables on the line 44 in src/components/TitleModal.js
await (itemId ? updateDashboardItem : addDashboardItem)({
  variables: {
    input: {
      id: itemId,
      vizState: JSON.stringify(finalVizState),
      name: finalTitle
    }
  }
});
```

Lastly, update how data is accessed in `src/pages/DashboardPage.js`.

```javascript
// on the line 66 and the following change data.dashboardItems to
// data.listDashboardItems.items
return !data || data.listDashboardItems.items.length ? (
  <Dashboard dashboardItems={data && data.listDashboardItems.items}>
    {data && data.listDashboardItems.items.map(deserializeItem).map(dashboardItem)}
  </Dashboard>
) : <Empty />;

```

Those are all the changes required to make our application work with AWS Cognito and AppSync. Now we have a fully functional application with authorization and a GraphQL backend.

Go ahead and restart your Cube.js backend and dashboard app servers and then navigate to https://localhost:3000 to test it locally.
You should see Cognito’s default sign-up and sign-in pages. Once registered, you can create your own dashboard, which is going to be stored in the cloud by AppSync.

<GIF OR VIDEO>

In the next chapter, we will start customizing our application by editing default theme and updating the design of the top menu.

<SHOW THE DEMO OF WHAT WE HAVE BUILT>
