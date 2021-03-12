---
order: 2
title: "Step 0. Openly accessible analytical app"
---

To secure a web application, we need one. So, we'll use [Cube.js](https://cube.dev?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics) to create an analytical API as well as a front-end app that talks to API and allows users to access e-commerce data stored in a database.

{% github cube-js/cube.js no-readme %}

[Cube.js](https://cube.dev?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics) is an open-source analytical API platform that allows you to create an API over any database and provides tools to explore the data, help build a data visualization, and tune the performance. Let's see how it works.

**The first step is to create a new Cube.js project.** Here I assume that you already have [Node.js](https://nodejs.org/en/) installed on your machine. Note that you can also [use Docker](https://cube.dev/docs/getting-started-docker?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics) with Cube.js. Run in your console:

```bash
npx cubejs-cli create multi-tenant-analytics -d postgres
```

Now you have your new Cube.js project in the `multi-tenant-analytics` folder which contains a few files. Let's navigate to this folder.

**The second step is to add database credentials to the `.env` file.** Cube.js will pick up its configuration options from this file. Let's put the credentials of a demo e-commerce dataset hosted in a cloud-based Postgres database. Make sure your `.env` file looks like this, or specify your own credentials:

```ini
# Cube.js environment variables: https://cube.dev/docs/reference/environment-variables

CUBEJS_DB_TYPE=postgres
CUBEJS_DB_HOST=demo-db.cube.dev
CUBEJS_DB_PORT=5432
CUBEJS_DB_SSL=true
CUBEJS_DB_USER=cube
CUBEJS_DB_PASS=12345
CUBEJS_DB_NAME=ecom

CUBEJS_DEV_MODE=true
CUBEJS_WEB_SOCKETS=false
CUBEJS_API_SECRET=SECRET
```

**The third step is to start Cube.js API.** Run in your console:

```bash
npm run dev
```

So, our analytical API is ready! Here's what you should see in the console:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/dcbsbclyriboyw0lm4ci.png)

Please note it says that currently the API is running in development mode, so authentication checks are disabled. It means that it's openly accessible to anyone. We'll fix that soon.

**The fourth step is to check that authentication is disabled.** Open `http://localhost:4000` in your browser to access Developer Playground. It's a part of Cube.js that helps to explore the data, create front-end apps from templates, etc.

Please go to the "Schema" tab, tick `public` tables in the sidebar, and click `Generate Schema`. Cube.js will generate a [data schema](https://cube.dev/docs/getting-started-cubejs-schema?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics) which is a high-level description of the data in the database. It allows you to send domain-specific requests to the API without writing lengthy SQL queries.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/rainawhcpq2rs4sy7apj.png)

Let's say that we know that e-commerce orders in our dataset might be in different statuses (*processing*, *shipped*, etc.) and we want to know how many orders belong to each status. You can select these measures and dimensions on the "Build" tab and instantly see the result. Here's how it looks after the `Orders.count` measure and the `Orders.status` dimension are selected:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/71hpkkkb7683voabhwc5.png)

It works because Developer Playground sends requests to the API. So, you can get the same result by running the following command in the console:

```sh
curl http://localhost:4000/cubejs-api/v1/load \
  -G -s --data-urlencode 'query={"measures": ["Orders.count"], "dimensions": ["Orders.status"]}' \
  | jq '.data'
```

Please note that it employs the `jq` utility, a command-line [JSON processor](https://stedolan.github.io/jq/tutorial/), to beautify the output. You can [install](https://stedolan.github.io/jq/download/) `jq` or just remove the last line from the command. Anyway, you'll get the result you're already familiar with:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/ufukvu1arvo6x314z5wu.png)

‼️ **We were able to retrieve the data without any authentication.** No security headers were sent to the API, yet it returned the result. So, we've created an openly accessible analytical API.

**The last step is to create a front-end app.** Please get back to Developer Playground at `http://localhost:4000`, go to the "Dashboard App" tab, choose to "Create your Own" and accept the defaults by clicking "OK".

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/i/43ljijihw21cpknz4i22.png)

In just a few seconds you'll have a newly created front-end app in the `dashboard-app` folder. Click "Start dashboard app" to run it, or do the same by navigating to the `dashboard-app` folder and running in the console:

```
npm run start
```

You'll see a front-end app like this:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/seqbbxsskkwlccjfvcmy.png)

If you go to the "Explore" tab, select the `Orders Count` measure and the `Orders Status` dimension once again, you'll see:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/lpelpjdemi8bc9nk2ma1.png)

That means that we've successfully created a front-end app that makes requests to our insecure API. You can also click the "Add to Dashboard" button to persist this query on the "Dashboard" tab.

Now, as we're navigating some dangerous waters, it's time to proceed to the next step and add authentication 🤿 