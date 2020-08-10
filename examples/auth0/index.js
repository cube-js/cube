require('dotenv').config();
const http = require('http');
const express = require('express');
const bodyParser = require('body-parser');
const cookieParser = require('cookie-parser');
const session = require('express-session');
const CubejsServerCore = require('@cubejs-backend/server-core');
const jwt = require('express-jwt');
const jwksRsa = require('jwks-rsa');

const app = express();
app.use(require('cors')());
app.use(cookieParser());
app.use(bodyParser.json({ limit: '50mb' }));
app.use(session({ secret: process.env.CUBEJS_API_SECRET }));

// Set up Auth0 configuration
const authConfig = {
  domain: process.env.AUTH0_DOMAIN,
  audience: process.env.AUTH0_AUDIENCE,
};

// checkJWT middleware
const checkJwt = jwt({
  secret: jwksRsa.expressJwtSecret({
    cache: true,
    rateLimit: true,
    jwksRequestsPerMinute: 5,
    jwksUri: `https://${authConfig.domain}/.well-known/jwks.json`,
  }),

  audience: authConfig.audience,
  issuer: `https://${authConfig.domain}/`,
  algorithms: ['RS256'],
});

// Use middleware for ALL endpoints
app.use(checkJwt);

const serverCore = CubejsServerCore.create({
  // Check auth only on production
  checkAuth: (req) => {
    if (!req.user && process.env.NODE_ENV !== 'development') {
      throw new Error(`Unauthorized`);
    }
  },
});

serverCore.initApp(app);

const port = process.env.PORT || 4000;
const server = http.createServer(app);

app.get('/check-user-data-at-some-point', (req, res) => {
  res.json({
    // user data available by req.user
    user: req.user
  })
})

server.listen(port, (err) => {
  if (err) {
    console.error('Fatal error during server start: ');
    console.error(e.stack || e);
  }

  console.log(
    `🚀 Cube.js server (${CubejsServerCore.version()}) is listening on ${port}`,
  );
});
 