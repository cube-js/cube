<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

# [Cube.js Templates](https://cube.dev/templates/)

Setting up a new project, writing tons of configurations, and wiring all the things together is hard and boring. It's fun to write actual application code, not Webpack config. That's why Create React App is so extremely popular. 

Same for analytics apps and dashboards. Although [Cube.js](https://github.com/cube-js/cube.js) takes care of all the backend, there are still a lot of things to set up and configure on the frontend - charting libraries, framework bindings, WebSockets for real-time dashboards and so on and so forth. 

[Cube.js Templates](https://cube.dev/templates/) to the rescue! Templates are open-source, ready-to-use frontend analytics apps. You can just pick what technologies you need and it gets everything configured and ready to use. 

React with WebSockets, Chart.js and Material UI? You got it. Template wires it all together and configure to work with the Cube.js backend. 

[![Cube.js Templates Demo](https://img.youtube.com/vi/YsbF95tbSAQ/0.jpg)](https://www.youtube.com/watch?v=YsbF95tbSAQ)

Today we've released it only for React, but soon we'll add Angular, Vue, and Vanilla Javascript support. And it is open-sourced, same as Cube.js. Contributions are very welcomed! ❤️


## 5 Minute Tutorial

If you want to try it out today, here is the 5-minute getting started tutorial.

### Install Cube.js CLI


```bash
$ npm isntall cubejs-cli -g 
```

### Create New Project and Connect your database

Cube.js CLI has `create` command to setup new project. We also need to pass a database type with `-d` option. Here is the [list of supported databases](https://cube.dev/docs/connecting-to-the-database).

```bash
$ cubejs create hello-world -d postgres
```

Once created, `cd` into your new project and edit `.env` file to configure the database.

```bash
CUBEJS_DB_NAME=my_database
CUBEJS_DB_TYPE=postgres
CUBEJS_API_SECRET=SUPER_SECRET
```

Now, run the following command to start a dev server.

```bash
$ npm run dev
```

And navigate to the Cube.js Playground at [http://localhost:4000](http://localhost:4000)

### Generate Schema

Cube.js uses schema to know how to query your database. The schema is written in javascript and could be quite complex with a lot of logic. But as we just getting started we can auto-generate a simple schema in the playground. 

![](https://react-dashboard.cube.dev/images/1-screenshot-1.png)

### Use Cube.js Templates to create a frontend app
As we already have a Cube.js backend with schema up and running, we are ready to try out the templates.

Navigate to the "Dashboard App" tab in the playground. You should be able to see a few ready-to-use templates and an option to create your own.

![Alt Text](https://thepracticaldev.s3.amazonaws.com/i/1suc88w9p7b6w16yr6xk.png)

Feel free to click select whatever template works for you. Or you can mix different options and create your own template.

![Alt Text](https://thepracticaldev.s3.amazonaws.com/i/hxgrw6qdcmp68vjzbyfg.png)

Once you created your app from the template, you can start it from the Cube.js playground. You can edit it later in the `dashboard-app` folder inside the project. 

That's it! Now, you have a full working both backend and frontend for your dashboard. You can follow [React Dashboard Guide](https://react-dashboard.cube.dev/) or [Real-Time Dashboard Guide](https://real-time-dashboard.cube.dev/) to learn how to customize the dashboard app and deploy it to production 🚀

Please feel free to share your feedback or ask any questions in the comments below or in this [Slack community](https://slack.cube.dev/).

## What's in this directory?

Here's repository of all template packages supported by Cube.js Templates.
Each package consists of `TemplatePackage` description and `scaffolding/` sources.
Babel is used to parse apply code changes and generate resulting code.
Each template packages has default rules how they applied. 
Those defalts can easily be overriden by providing custom `SourceSnippet` implementations.
