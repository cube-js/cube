---
title: Auth0 Guide
permalink: /security/jwt/auth0
category: Authentication & Authorization
subCategory: Guides
menuOrder: 3
---

## Introduction

In this guide, you'll learn how to integrate Auth0 authentication with a Cube.js
deployment. If you already have a pre-existing application on Auth0 that you'd
like to re-use, please skip ahead to
[Configure Cube.js to use Auth0](#configure-cube-js-to-use-auth-0).

## Create and configure an application on Auth0

First, go to the [Auth0 dashboard][link-auth0-app], and click on the
'Applications' menu option on the left and then click the '+ Create Application'
button.

<p
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Auth/auth0-01-new-app-01.png"
  style="border: none"
  width="80%"
  />
</p>

In the 'Create Application' popup, set the name of your application and select
'Single Page Web Applications'.

<p
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Auth/auth0-01-new-app-02.png"
  style="border: none"
  width="80%"
  />
</p>

Next, go to the application's settings and add the appropriate callback URLs for
your application (`http://localhost:4000` for the Developer Playground).

### Custom claims

You can also configure custom claims for your JWT token. Auth0 has two SDKs
available; [Auth0.js][link-auth0-js] and the [Auth0 SPA
SDK][link-auth0-spa-sdk]. In either case, you’ll want to open the Auth0
dashboard, click on 'Rules' and add a rule to add any custom claims to the JWT.

#### Auth0.js

Take note of the value of `namespace` here, you will need it later to configure
Cube.js.

```javascript
function (user, context, callback) {
  const namespace = "http://localhost:4000/";
  context.idToken[namespace] =
    {
      'company_id': 'company1',
      'user_id': user.user_id,
      'roles': ['user'],
    };
  callback(null, user, context);
}
```

#### Auth0 SPA SDK

Take note of the value of `namespace` here, you will need it later to configure
Cube.js.

```javascript
function (user, context, callback) {
  const namespace = "http://localhost:4000/";
  context.accessToken[namespace] =
    {
      'company_id': 'company1',
      'user_id': user.user_id,
      'roles': ['user'],
    };
  callback(null, user, context);
}
```

## Create an API on Auth0

If you're using the Auth0 SPA SDK, you'll also need to [create an
API][link-auth0-api]. First, go to the [Auth0 dashboard][link-auth0-app] and
click on the 'APIs' menu option from the left sidebar, then click the '+ Create
API' button.

<p
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Auth/auth0-02-new-api-01.png"
  style="border: none"
  width="80%"
  />
</p>

In the 'New API' popup, set a name for this API and an identifier (e.g.
`cubejs`)

<!-- prettier-ignore-start -->
[[info |]]
| After creating the API, take note of the Client ID and Secret as you'll need
| them later.
<!-- prettier-ignore-end -->

<p
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Auth/auth0-02-new-api-02.png"
  style="border: none"
  width="80%"
  />
</p>

In your application code, configure your API identifier as the audience when
initializing Auth0:

```typescript jsx
<Auth0Provider
  domain={process.env.AUTH_DOMAIN}
  client_id={process.env.AUTH_CLIENT_ID}
  redirect_uri={window.location.origin}
  onRedirectCallback={() => {}}
  audience="cubejs"
>
```

## Configure Cube.js to use Auth0

Now we're ready to configure Cube.js to use Auth0. Go to your Cube.js project
and open the `.env` file and add the following, replacing the values wrapped in
`<>`.

```dotenv
CUBEJS_JWK_URL=https://<AUTH0-SUBDOMAIN>.auth0.com/.well-known/jwks.json
CUBEJS_JWT_AUDIENCE=<APPLICATION_URL>
CUBEJS_JWT_ISSUER=https://<AUTH0-SUBDOMAIN>.auth0.com/
CUBEJS_JWT_ALGS=RS256
CUBEJS_JWT_CLAIMS_NAMESPACE=<CLAIMS_NAMESPACE>
```

## Testing with the Developer Playground

### Retrieving a JWT

Go to the [OpenID Playground from Auth0][link-openid-playground] to and click
Configuration.

<p
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Auth/auth0-03-get-jwt-01.png"
  style="border: none"
  width="80%"
  />
</p>

Enter the following values:

- **Auth0 domain**: `<AUTH0-SUBDOMAIN>.auth0.com`
- **OIDC Client ID**: Retrieve from Auth0 API settings page
- **OIDC Client Secret**: Retrieve from Auth0 API settings page

Click 'Use Auth0 Discovery Document' to auto-fill the remaining values, then
click Save.

<p
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Auth/auth0-03-get-jwt-02.png"
  style="border: none"
  width="80%"
  />
</p>

<!-- prettier-ignore-start -->
[[warning |]]
| If you haven't already, go back to the Auth0 application's settings and add
| `https://openidconnect.net/callback` to the list of allowed callback URLs.
<!-- prettier-ignore-end -->

Now click Start; if the login is successful, you should see the code, as well as
a button called 'Exchange'. Click on it to exchange the token for a JWT, then
click Next. You should now have a valid JWT. Copy it for use in the next
section.

<p
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Auth/auth0-03-get-jwt-03.png"
  style="border: none"
  width="80%"
  />
</p>

### Set JWT in Developer Playground

Now open the Developer Playground (at `http://localhost:4000`) and on the Build
page, click Add Security Context.

<p
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Auth/auth0-04-dev-playground-01.png"
  style="border: none"
  width="80%"
  />
</p>

Click the Token tab, paste the JWT from OpenID Playground and click the Save
button.

<p
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Auth/auth0-04-dev-playground-02.png"
  style="border: none"
  width="80%"
  />
</p>

Close the popup and use the Developer Playground to make a request. Any schemas
using the [Security Context][ref-sec-ctx] should now work as expected.

## Example

To help you get up and running, we have [an example project which is configured
to use Auth0][gh-cubejs-auth0-example]. You can use it as a starting point for
your own Cube.js application.

[link-auth0-app]: https://manage.auth0.com/
[link-auth0-js]: https://auth0.com/docs/libraries/auth0js
[link-auth0-spa-sdk]: https://auth0.com/docs/libraries/auth0-spa-js
[link-auth0-api]:
  https://auth0.com/docs/tokens/access-tokens#json-web-token-access-tokens
[link-openid-playground]: https://openidconnect.net/
[ref-sec-ctx]: /security/context
[gh-cubejs-auth0-example]:
  https://github.com/cube-js/cube.js/tree/master/examples/auth0
