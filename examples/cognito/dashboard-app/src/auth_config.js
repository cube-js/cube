export default {
  region: process.env.REACT_APP_AWS_REGION,
  userPoolId: process.env.REACT_APP_AWS_COGNITO_POOL_ID,
  userPoolRegion: process.env.REACT_APP_AWS_COGNITO_REGION,
  userPoolWebClientId: process.env.REACT_APP_AWS_COGNITO_CLIENT_ID,
  oauth: {
    domain: process.env.REACT_APP_AWS_OAUTH_DOMAIN,
    scope: ["profile", "email", "openid"],
    redirectSignIn: process.env.REACT_APP_AWS_SIGN_IN,
    redirectSignOut: process.env.REACT_APP_AWS_SIGN_OUT,
    responseType: "code"
  }
}
