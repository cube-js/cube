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
  CUBEJS_DB_PASS=<YOUR-DB-PASSWORD>
```

Then, we need to create two files:

```bash
$ touch Dockerfile
$ touch .dockerignore
```

The first file, `Dockerfile`, describes how to build a Docker image. Add these contents:

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
$ heroku container:push web -a cubejs-heroku-api
$ heroku container:release web -a cubejs-heroku-api
```

Let's also provision a free Redis server provided by Heroku:

```bash
$ heroku addons:create heroku-redis:hobby-dev -a cubejs-heroku-api
```

Great! You can run the `heroku open` command to open your Cube.js API and see the message that Cube.js is running in production mode in your browser.

## Dashboard App Deployment

The dashboard app should be deployed as a static website.

To do so on Heroku, we need to ebable the static websites build pack:

```bash
$ heroku buildpacks:set https://github.com/heroku/heroku-buildpack-static.git
```

Next, we need to create the `static.json` file under the `dashboard-app` folder with the following contents:

```json
{
  "root": "build/"
}
```

Finally, we need to run the `npm build` command inside the `dashboard-app` folder
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
