---
order: 3
title: "How to Create an Analytical API"
---

Why do we need an API in the first place?

The most obvious reason is that BigQuery can't provide a sub-second query response time, meaning that an application that talks directly to BigQuery will have a suboptimal user experience. Also, BigQuery bills you by the amount of transferred data, so if you have a popular app, you might suddenly know about that from a billing alert.

Also, direct interaction with BigQuery means that you'll need to write SQL queries. There's nothing wrong with SQL; it's a great domain-specific language, but having SQL queries all over your codebase smells like a leaky abstraction — your application layers will know about column names and data types in your database.

**So, what are we going to do? In this tutorial, we'll use Cube.js.**

[Cube.js](https://cube.dev) is an open-source analytical API platform, and it allows you to create an API over any database, BigQuery included.

Cube.js provides an abstraction called a "semantic layer," or a "data schema," which encapsulates database-specific things, generates SQL queries for you, and lets you use high-level, domain-specific identifiers to work with data.

Also, Cube.js has a built-in caching layer that provides predictable, low-latency response query times. It means that an API built with Cube.js is a perfect middleware between your database and your analytical app.

Let's try it in action.

**The first step is to create a new Cube.js project.** Here, I assume that you already have [Node.js](https://nodejs.org/en/) installed on your machine. Note that you can also [use Docker](https://cube.dev/docs/getting-started-docker) to run Cube.js. Run in your console:

```bash
npx cubejs-cli create bigquery-public-datasets -d bigquery
```

Now you have your new Cube.js project in the `bigquery-public-datasets` folder containing a few files. Let's navigate to this folder.

**The second step is to add BigQuery and Google Cloud credentials to the `.env` file.** Cube.js will pick up its configuration options from this file. Make sure your `.env` file looks like this:

```
# Cube.js environment variables: https://cube.dev/docs/reference/environment-variables

CUBEJS_DB_TYPE=bigquery
CUBEJS_DB_BQ_PROJECT_ID=your-project-id
CUBEJS_DB_BQ_KEY_FILE=./your-key-file-name.json

CUBEJS_DEV_MODE=true
CUBEJS_API_SECRET=SECRET
```

Here's what all these options mean and how to fill them:
* Obviously, `CUBEJS_DB_TYPE` says we'll be connecting to BigQuery.
* `CUBEJS_DB_BQ_PROJECT_ID` should be set to the identifier of your project in Google Cloud. First, go to the [web console](https://console.cloud.google.com) of Google Cloud. Create an account if you don't have one. Then go to the [new project creation page](https://console.cloud.google.com/projectcreate) and create one. Your project identifier is just below the name text field:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/oysxbawk9kq7li6uz5c8.png)

* `CUBEJS_DB_BQ_KEY_FILE` should be set to the key file name for your Google Cloud user that will connect to BigQuery. It's better to use a service account, a special kind of Google Cloud account with restricted access. Go to the [service account creation page](https://console.cloud.google.com/iam-admin/serviceaccounts/create) and create one. On the second step, you'll be asked to specify the roles for this service account. The only roles needed for read-only access to public datasets are `BigQuery Data Viewer` and `BigQuery Job User`. After the user is created, you need to add a new authentication key — use the `...` button on the right to manage the keys for this account and add a new one of JSON type. The key file will be automatically downloaded to your machine. Please put it in the `bigquery-public-datasets` folder and update your `.env` file with its name.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/125qlk9meqvecw9jw34w.png)

* The rest of the options configure Cube.js and have nothing to do with BigQuery. Save your `.env` file.

**The third step is to start Cube.js.** Run in your console:

```bash
npm run dev
```

And that's it! Here's what you should see:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/5rdos5okrrindw23m5di.png)

Great, the API is up and running. Let's describe our data! 🦠