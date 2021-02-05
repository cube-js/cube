export default {
  domain: process.env.REACT_APP_AUTH0_DOMAIN,
  clientId: process.env.REACT_APP_AUTH0_CLIENT_ID,
  audience: process.env.REACT_APP_AUTH0_AUDIENCE,
  scope: 'openid profile email',
}
