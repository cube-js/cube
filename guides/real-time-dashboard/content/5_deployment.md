---
order: 5
title: "Deployment"
---

Now, let's deploy our Cube.js API and the dashboard application. In this tutorial, we'll deploy both the Cube.js API and the dashboard application to Heroku.

## Cube.js API Deployment

There are multiple ways you can deploy a Cube.js API microservice; you can learn more about them [here in the docs](https://cube.dev/docs/deployment/).

The tutorial assumes that you have a free [Heroku account](https://signup.heroku.com/signup/dc). You'd also need a Heroku CLI; you can [learn how to install it here](https://devcenter.heroku.com/articles/heroku-cli).

First, let's create a new Heroku app. Run the following command inside your
Cube.js project folder.

```bash
$ heroku create real-time-dashboard-api
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
  CUBEJS_DB_PASS=<YOUR-DB-PASSWORD> \
  CUBEJS_API_SECRET=<YOUR-SECRET> \
  --app real-time-dashboard-api
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
$ heroku container:push web --app real-time-dashboard-api
$ heroku container:release web --app real-time-dashboard-api
```

Let's also provision a free Redis server provided by Heroku:

```bash
$ heroku addons:create heroku-redis:hobby-dev --app real-time-dashboard-api
```

Great! You can run the `heroku open --app real-time-dashboard-api` command to open your Cube.js API and see this message in your browser:

```
Cube.js server is running in production mode.
```

## Dashboard App Deployment

The dashboard app should be deployed as a static website.

To do so on Heroku, we need to create the second Heroku app. Run the following command inside the `dashboard-app` folder:

```bash
$ heroku create real-time-dashboard-web
```

Then, enable the static website build pack:

```bash
$ heroku buildpacks:set https://github.com/heroku/heroku-buildpack-static.git
```

Next, we need to create the `static.json` file under the `dashboard-app` folder with the following contents:

```json
{
  "root": "build/"
}
```

Also, we need to set Cube.js API URL to the newly created Heroku app URL. In the `src/App.js` file, change this line:

```diff
- const API_URL = "http://localhost:4000";
+ const API_URL = "https://real-time-dashboard-api.herokuapp.com";
```

Finally, we need to run the `npm run build` command
and make sure the `build` folder is tracked by Git. By default, `.gitignore`
excludes that folder, so you need to remove it from `.gitignore`.

Once done, commit your changes and push to Heroku. 🚀

```bash
$ git add -A
$ git commit -am "Initial"
$ git push heroku master
```

That's it! You can run the `heroku open` command to open your dashboard application in your browser and see it working with Cube.js.

Congratulations on completing this guide! 🎉

I’d love to hear from you about your experience following this guide. Please send any comments or feedback you might have in this [Slack Community](https://slack.cube.dev). Thank you and I hope you found this guide helpful!
