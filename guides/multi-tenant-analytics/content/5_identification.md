---
order: 5
title: "Step 3. Identification via Auth0"
---

As we already know, the essence of identification is asking users who they are. An external authentication provider can take care of this, allowing users to authenticate via various means (e.g., their Google accounts or social profiles) and providing complementary infrastructure and libraries to integrate with your app.

[Auth0](https://auth0.com) is a leading identity management platform for developers, [recently acquired](https://techcrunch.com/2021/03/04/making-sense-of-the-6-5b-okta-auth0-deal/) by Okta, an even larger identity management platform. It securely stores all sensitive user data, has a convenient web admin panel, and provides front-end libraries for various frameworks. We'll use Auth0's integration with React but it's worth noting that Auth0 has integrations with all major front-end frameworks, just like Cube.js.

On top of that, Auth0 provides many advanced features:
* User roles — you can have admins, users, etc.
* Scopes — you can set special permissions per user or per role, e.g, to allow some users to change your app’s settings or perform particular Cube.js queries.
* Mailing — you can connect third-party systems, like SendGrid, to send emails: reset passwords, welcome, etc.
* Management — you can invite users, change their data, remove or block them, etc.
* Invites — you can allow users to log in only via invite emails sent from Auth0.

Auth0 allows you to implement an industry-standard [OAuth 2.0 flow](https://oauth.net/2/) with ease. OAuth 2.0 is a proven protocol for external authentication. In principle, it works like this:
* Our application redirects an unauthenticated user to an external authentication provider.
* The provider asks the user for its identity, verifies it, generates additional information (JWT included), and redirects the user back to our application.
* Our application assumes that the user is now authenticated and uses their information. In our case, the user's JWT can be sent further to Cube.js API.

So, now it's time to use Auth0 to perform identification and issue different JWTs for each user.

**First, let's set up an Auth0 account.** You'll need to go to [Auth0](https://auth0.com) website and sign up for a new account. After that, navigate to the "[Applications](https://manage.auth0.com/dashboard/us/dev-vubjtv0z/applications)" page of the admin panel. To create an application matching the one we're developing, click the "+ Create Application" button, select "Single Page Web Applications". Done!

Proceed to the "Settings" tab and take note of the following fields: "Domain", "Client ID", and "Client Secret". We'll need their values later.

Then scroll down to the "Allowed Callback URLs" field and add the following URL as its value: `http://localhost:3000`. Auth0 requires this URL as an additional security measure to make sure that users will be redirected to our very application.

"Save Changes" at the very bottom, and proceed to the "[Rules](https://manage.auth0.com/dashboard/us/dev-vubjtv0z/rules)" page of the admin panel. There, we'll need to create a rule to assign "roles" to users. Click the "+ Create Rule" button, choose an "Empty rule", and paste this script, and "Save Changes":

```js
function (user, context, callback) {
  const namespace = "http://localhost:3000";
  context.accessToken[namespace] = {
    role: user.email.split('@')[1] === 'cube.dev' ? 'admin' : 'user',
  };
  callback(null, user, context);
}
```

This rule will check the domain in users' emails, and if that domain is equal to "cube.dev", the user will get the admin role. You can specify your company's domain or any other condition, e.g., `user.email === 'YOUR_EMAIL'` to assign the admin role only to yourself.

The last thing here will be to register a new Auth0 API. To do so, navigate to the "[APIs](https://manage.auth0.com/dashboard/us/dev-vubjtv0z/apis)" page, click "+ Create API", enter any name and `cubejs` as the "Identifier" (later we'll refer to this value as "audience").

That's all, now we're done with the Auth0 setup.

**Second, let's update the web application.** We'll need to add the integration with Auth0, use redirects, and consume the information after users are redirected back.

We'll need to add a few configuration options to the `dashboard-app/.env` file. Note that two values should be taken from our application's settings in the admin panel:

```ini
REACT_APP_AUTH0_AUDIENCE=cubejs
REACT_APP_AUTH0_DOMAIN=<VALUE_OF_DOMAIN_FROM_AUTH0>
REACT_APP_AUTH0_CLIENT_ID=<VALUE_OF_CLIENT_ID_FROM_AUTH0>
```

Also, we'll need to add Auth0 React library to the `dashboard-app` with this command:

```sh
npm install --save @auth0/auth0-react
```

Then, we'll need to wrap the React app with `Auth0Provider`, a companion component that provides Auth0 configuration to all React components down the tree. Update your `dashboard-app/src/index.js` file as follows:

```diff
  import React from 'react';
  import ReactDOM from 'react-dom';
  import { HashRouter as Router, Route } from 'react-router-dom';
  import ExplorePage from './pages/ExplorePage';
  import DashboardPage from './pages/DashboardPage';
  import App from './App';
+ import { Auth0Provider } from "@auth0/auth0-react";

  ReactDOM.render(
+   <Auth0Provider
+     audience={process.env.REACT_APP_AUTH0_AUDIENCE}
+     domain={process.env.REACT_APP_AUTH0_DOMAIN}
+     clientId={process.env.REACT_APP_AUTH0_CLIENT_ID}
+     scope={'openid profile email'}
+     redirectUri={process.env.REACT_APP_AUTH0_REDIRECT_URI || window.location.origin}
+     onRedirectCallback={() => {}}
+   >
      <Router>
        <App>
          <Route key="index" exact path="/" component={DashboardPage} />
          <Route key="explore" path="/explore" component={ExplorePage} />
        </App>
      </Router>
+   </Auth0Provider>,
  document.getElementById('root'));
```

The last change will be applied to the `dashboard-app/src/App.js` file where the Cube.js client library is instantiated. We'll update the `App` component to interact with Auth0 and re-instantiate the client library with appropriate JWTs when Auth0 returns them.

First, remove these lines from `dashboard-app/src/App.js`, we don't need them anymore:

```diff
- const API_URL = "http://localhost:4000";
- const CUBEJS_TOKEN = "<OLD_JWT>";
- const cubejsApi = cubejs(CUBEJS_TOKEN, {
-   apiUrl: `${API_URL}/cubejs-api/v1`
- });
```

After that, add the import of an Auth0 React hook:

```diff
+ import { useAuth0 } from '@auth0/auth0-react';
```

Finally, update the `App` functional component to match these code:

```js
const App = ({ children }) => {
  const [ cubejsApi, setCubejsApi ] = useState(null);

  // Get all Auth0 data
  const {
    isLoading,
    error,
    isAuthenticated,
    loginWithRedirect,
    getAccessTokenSilently,
    user
  } = useAuth0();

  // Force to work only for logged in users
  useEffect(() => {
    if (!isLoading && !isAuthenticated) {
      // Redirect not logged users
      loginWithRedirect();
    }
  }, [ isAuthenticated, loginWithRedirect, isLoading ]);

  // Get Cube.js instance with accessToken
  const initCubejs = useCallback(async () => {
    const accessToken = await getAccessTokenSilently({
      audience: process.env.REACT_APP_AUTH0_AUDIENCE,
      scope: 'openid profile email',
    });

    setCubejsApi(cubejs({
      apiUrl: `http://localhost:4000/cubejs-api/v1`,
      headers: { Authorization: `${accessToken}` },
    }));
  }, [ getAccessTokenSilently ]);

  // Init Cube.js instance with accessToken
  useEffect(() => {
    if (!cubejsApi && !isLoading && isAuthenticated) {
      initCubejs();
    }
  }, [ cubejsApi, initCubejs, isAuthenticated, isLoading ]);

  if (error) {
    return <span>{error.message}</span>;
  }

  // Show indicator while loading
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
```

Done! Now, you can stop the web application (by pressing `CTRL+C`), and run it again with `npm start`. You'll be redirected to Auth0 and invited to log in. Use any method you prefer (e.g., Google) and get back to your app. Here's what you'll see:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/dwuc6nq1kjbvai518yky.png)

It appears that our application receives a JWT from Auth0, sends it to the API, and fails with "Invalid token". Why is that? Surely, because the API knows nothing about our decision to identify users and issue JWT via Auth0. We'll fix it now.

**Third, let's configure Cube.js to use Auth0.** Cube.js provides convenient built-in integrations with [Auth0](https://cube.dev/docs/security/jwt/auth0?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics) and [Cognito](https://cube.dev/docs/security/jwt/aws-cognito?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics) that can be configured solely through the `.env` file. Add these options to this file, substituting `<VALUE_OF_DOMAIN_FROM_AUTH0>` with an appropriate value from above:

```ini
CUBEJS_JWK_URL=https://<VALUE_OF_DOMAIN_FROM_AUTH0>/.well-known/jwks.json
CUBEJS_JWT_ISSUER=https://<VALUE_OF_DOMAIN_FROM_AUTH0>/
CUBEJS_JWT_AUDIENCE=cubejs
CUBEJS_JWT_ALGS=RS256
CUBEJS_JWT_CLAIMS_NAMESPACE=http://localhost:3000
```

After that, save the updated `.env` file, stop Cube.js (by pressing `CTRL+C`), and run Cube.js again with `npm run dev`. Now, if you refresh the web application, you should see the result from the API back, the full dataset or just 10 % of it depending on your user and the rule you've set up earlier:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/jtj7semow5g95mtpsnb6.png)

‼️ **We were able to integrate the web application and the API based on Cube.js with Auth0 as an external authentication provider.** Auth0 identifies all users and generates JWTs for them. Now only logged-in users are able to access the app and perform queries to Cube.js. Huge success!

The only question remains: once we have users with different roles interacting with the API, how to make sure we can review their actions in the future? Let's see what Cube.js can offer 🤿