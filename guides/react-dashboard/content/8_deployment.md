---
title: "Deployment"
order: 8
---

## Deploy Cube.js API

There are multiple ways you can deploy a Cube.js API; you can learn more about them [here in the docs](https://cube.dev/docs/deployment/). In this tutorial, we'll deploy it on Heroku.

The tutorial assumes that you have a free [Heroku account](https://signup.heroku.com/signup/dc). You'd also need a Heroku CLI; you can [learn how to install it here](https://devcenter.heroku.com/articles/heroku-cli).

First, let's create a new Heroku app. Run the following command inside your
Cube.js project folder.

```bash
$ heroku create react-dashboard-api
```

We also need to provide credentials to access the database. I assume you have
your database already deployed and externally accessible.

```bash
$ heroku config:set \
  CUBEJS_DB_TYPE=postgres \
  CUBEJS_DB_HOST=<YOUR-DB-HOST> \
  CUBEJS_DB_NAME=<YOUR-DB-NAME> \
  CUBEJS_DB_USER=<YOUR-DB-USER> \
  CUBEJS_DB_PASS=<YOUR-DB-PASSWORD> \
  CUBEJS_API_SECRET=<YOUR-SECRET> \
  --app react-dashboard-api
```

Then, we need to create two files for Docker. The first file, `Dockerfile`, describes how to build a Docker image. Add these contents:

```dockerfile
FROM cubejs/cube:latest

COPY . .
```

The second file, `.dockerignore`, provides a list of files to be excluded from the image. Add these patterns:

```
node_modules
npm-debug.log
dashboard-app
.env
```

Now we need to build the image, push it to the Heroku Container Registry, and release it to our app:

```bash
$ heroku container:login
$ heroku container:push web --app react-dashboard-api
$ heroku container:release web --app react-dashboard-api
```

Let's also provision a free Redis server provided by Heroku:

```bash
$ heroku addons:create heroku-redis:hobby-dev --app react-dashboard-api
```

Great! You can run the `heroku open --app react-dashboard-api` command to open your Cube.js API and see this message in your browser:

```
Cube.js server is running in production mode.
```

## Deploy React Dashboard

Since our frontend application is just a static app, it easy to build and
deploy. Same as for the backend, there are multiple ways you can deploy it. You can serve it with your favorite HTTP server or just select one of the popular cloud providers. We'll
use [Netlify](https://www.netlify.com/) in this tutorial.

Also, we need to set Cube.js API URL to the newly created Heroku app URL. In the `src/App.js` file, change this line:

```diff
- const API_URL = "http://localhost:4000";
+ const API_URL = "https://react-dashboard-api.herokuapp.com";
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
