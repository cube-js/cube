const CubejsServerCore = require('@cubejs-backend/server-core');
const MySQLDriver = require('@cubejs-backend/mysql-driver');
const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');
const http = require('http');
const serveStatic = require('serve-static');
const session = require('express-session');
const passport = require('passport');
require('dotenv').config();

const app = express();
app.use(bodyParser.json({ limit: '50mb' }));
app.use(require('cors')());

const cubejsServer = CubejsServerCore.create({
  externalDbType: 'mysql',
  externalDriverFactory: () => new MySQLDriver({
    host: process.env.CUBEJS_EXT_DB_HOST,
    database: process.env.CUBEJS_EXT_DB_NAME,
    port: process.env.CUBEJS_EXT_DB_PORT,
    user: process.env.CUBEJS_EXT_DB_USER,
    password: process.env.CUBEJS_EXT_DB_PASS,
  }),
  preAggregationsSchema: 'wa_pre_aggregations',
  scheduledRefreshTimer: true
});

app.get('/healthy', (req, res) => {
  res.json({ status: 'ok' });
});

app.use(session({ secret: process.env.CUBEJS_API_SECRET }));
app.use(passport.initialize());
app.use(passport.session());

if (process.env.GOOGLE_AUTH_DOMAIN) {
  const { Strategy: GoogleStrategy } = require('passport-google-oauth20');
  const authURL = process.env.NODE_ENV === 'production' ? `${process.env.GOOGLE_AUTH_REDIRECT}` : 'http://localhost:4000';
  passport.use(new GoogleStrategy({
    clientID: process.env.GOOGLE_AUTH_CLIENT_ID,
    clientSecret: process.env.GOOGLE_AUTH_CLIENT_SECRET,
    callbackURL: `${authURL}/auth/google/callback`
  },
  (accessToken, refreshToken, profile, cb) => {
    if (profile.emails && profile.emails.find(e => e.value.match(new RegExp(`@${process.env.GOOGLE_AUTH_DOMAIN}$`)) && e.verified)) {
      return cb(null, profile);
    }
    return cb(`${profile.emails && profile.emails[0] && profile.emails[0].value} not within @${process.env.GOOGLE_AUTH_DOMAIN}`);
  }));

  passport.serializeUser((user, done) => {
    done(null, user);
  });

  passport.deserializeUser((user, done) => {
    done(null, user);
  });

  app.get('/auth/google',
    passport.authenticate('google', { scope: ['profile', 'email'] }));

  app.get('/auth/google/callback',
    passport.authenticate('google', { failureRedirect: '/auth/google' }),
    (req, res) => {
      // Successful authentication, redirect home.
      res.redirect('/');
    });

  app.use((req, res, next) => {
    if (!req.user) {
      res.redirect('/auth/google');
      return;
    }
    next();
  });
}

if (process.env.NODE_ENV === 'production') {
  app.use(serveStatic(path.join(__dirname, 'dashboard-app/build')));
}

cubejsServer.initApp(app);

const port = process.env.PORT || 4000;
const server = http.createServer({}, app);

server.listen(port, () => {
  console.log(`🚀 Cube.js server (${CubejsServerCore.version()}) is listening on ${port}`);
});
