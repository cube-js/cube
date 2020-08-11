---
title: "Deployment"
order: 8
---

## Deploy Cube.js Backend

There are multiple ways you can deploy a Cube.js Backend service; you can learn
more about them [here in the docs](https://cube.dev/docs/deployment). In this tutorial, we'll deploy it on Heroku.

The tutorial assumes that you have a free [Heroku account](https://signup.heroku.com/signup/dc). You'd also need a Heroku CLI; you can [learn how to install it here](https://devcenter.heroku.com/articles/heroku-cli).

First, let's create a new Heroku app. Run the following command inside your
Cube.js project folder.

```bash
$ heroku create react-dashboard-demo
```

Next, we need to add the Redis addon to our Heroku app. Cube.js uses Redis in
production as a cache storage. You can learn more about [Cube.js caching here](https://cube.dev/docs/caching).
Run the following command to add Redis to our Heroku app.

```bash
$ heroku addons:create heroku-redis:hobby-dev -a react-dashboard-demo
```

We also need to provide credentials to access the database. I assume you have
your database already deployed and externally accessible.

```bash
$ heroku config:set \
  CUBEJS_DB_TYPE=postgres \
  CUBEJS_DB_HOST=<YOUR-DB-HOST> \
  CUBEJS_DB_NAME=<YOUR-DB-NAME> \
  CUBEJS_DB_USER=<YOUR-DB-USER> \
  CUBEJS_DB_PASS=<YOUR-DB-PASSWORD>
```

Add a `start` command to your `package.json`.

```diff
   "scripts": {
-    "dev": "./node_modules/.bin/cubejs-dev-server"
+    "dev": "./node_modules/.bin/cubejs-dev-server",
+    "start": "node index.js"
   },
```

Commit your changes and push to Heroku 🚀

```bash
$ git add -A
$ git commit -am "Initial"
$ git push heroku master
```

## Deploy React Dashboard

Since our frontend application is just a static app, it easy to build and
deploy. Same as for the backend, there are multiple ways you can deploy it. You can serve it with your favorite HTTP server or just select one of the popular cloud providers. We'll
use [Netlify](https://www.netlify.com/) in this tutorial.

First, we need to set Cube.js API URL for production. It is Cube.js Backend URL on Heroku; it has the form of https://your-app-name.herokuapp.com.

Update the `src/App.js` file with the following.

```diff
-const API_URL = "http://localhost:4000";
+if (process.env.NODE_ENV === 'production) {
+  const API_URL = "YOUR-HEROKU-URL";
+} else {
+  const API_URL = "http://localhost:4000";
+}
```

Next, install Netlify CLI.

```bash
$ npm install netlify-cli -g
```

Then, we need to run a `build` command inside our `dashboard-app`. This command
creates an optimized build for production and puts it into a `build` folder.

```bash
$ npm run build
```

Finally, we are ready to deploy our dashboard to Netlify; just run the
following command to do so.

```bash
$ netlify deploy
```

Follow the command line prompts and choose `yes` for a new project and `build` as your deploy folder.

That’s it! You can copy a link from your command line and check your dashboard
live!

Congratulations on completing this guide! 🎉

I’d love to hear from you about your experience following this guide. Please send any comments or feedback you might have in this [Slack Community](https://slack.cube.dev). Thank you and we hope you found this guide helpful!
