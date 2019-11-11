---
order: 5
title: "Deployment"
---

There are multiple ways you can deploy a Cube.js Backend service; you can learn more about them [here in the docs](https://cube.dev/docs/deployment/). In this tutorial, we'll deploy both backend and the dashboard on Heroku.

The tutorial assumes that you have a free [Heroku account](https://signup.heroku.com/signup/dc). You'd also need a Heroku CLI; you can [learn how to install it here](https://devcenter.heroku.com/articles/heroku-cli).

First, let's create a new Heroku app. Run the following command inside your
Cube.js project folder.

```bash
$ heroku create real-time-dashboard-demo
```

We also need to provide credentials to access the database. I assume you have
your database already deployed and externally accessible. The example below
shows setting up credentials for MongoDB.

```bash
$ heroku config:set \
  CUBEJS_DB_TYPE=mongobi \
  CUBEJS_DB_HOST=<YOUR-DB-HOST> \
  CUBEJS_DB_NAME=<YOUR-DB-NAME> \
  CUBEJS_DB_USER=<YOUR-DB-USER> \
  CUBEJS_DB_PASS=<YOUR-DB-PASSWORD>
```

Next, we need to make some changes to our `index.js` file to make it serve
static files from `dashboard-app/build` folder. Update the content of the
`index.js` with the following.

```javascript
const CubejsServerCore = require('@cubejs-backend/server-core');
const WebSocketServer = require('@cubejs-backend/server/WebSocketServer');
const express = require('express');
const bodyParser = require("body-parser");
const http = require("http");
const path = require("path");
const serveStatic = require('serve-static');
require('dotenv').config();

var app = express();

app.use(require("cors")());
app.use(bodyParser.json({ limit: "50mb" }));

const cubejsServer = CubejsServerCore.create({
  orchestratorOptions: {
    queryCacheOptions: {
      refreshKeyRenewalThreshold: 1,
    }
  }
});

cubejsServer.initApp(app);
const server = http.createServer({}, app);

const socketServer = new WebSocketServer(
  cubejsServer,
  { processSubscriptionsInterval: 1 }
);
socketServer.initServer(server);

if (process.env.NODE_ENV === 'production') {
  app.use(serveStatic(path.join(__dirname, 'dashboard-app/build')));
}

const port = process.env.PORT || 4000;
server.listen(port, () => {
  console.log(`🚀 Cube.js server is listening on ${port}`);
});
```

Add a `start` command to your `package.json` to tell Heroku how to start up the
application.

```diff
   "scripts": {
-    "dev": "./node_modules/.bin/cubejs-dev-server"
+    "dev": "./node_modules/.bin/cubejs-dev-server",
+    "start": "node index.js"
   },
```

Finally, we need to run `npm build` command inside the `dashboard-app` folder
and make sure the build folder is tracked by Git. By default, `.gitignore`
excludes that folder, so you need to remove it from `.gitignore`.

Once done, commit your changes and push to Heroku 🚀

```bash
$ git add -A
$ git commit -am "Initial"
$ git push heroku master
```

That’s it! You can run `heroku open` command to open your dashboard.

Congratulations on completing this guide! 🎉

I’d love to hear from you about your experience following this guide. Please send any comments or feedback you might have in this [Slack Community](https://slack.cube.dev). Thank you and I hope you found this guide helpful!
