---
order: 1
title: "Overview"
---

In this tutorial, I’ll cover building a basic dashboard application with Cube.js and the most popular library for visualizing data—D3.js. Although Cube.js doesn’t provide a visualization layer itself, it is very easy to integrate with any existing charting library.

You can check [the online demo of this dashboard here](http://d3-dashboard-demo.cube.dev/) and [the complete source code of the example app is available on Github](https://github.com/cube-js/cube.js/tree/master/examples/d3-dashboard).

We are going to use Postgres to store our data. Cube.js will connect to it and act as a middleware between the database and the client, providing API, abstraction, caching, and a lot more. On the frontend, we’ll have React with Material UI and D3 for chart rendering. Below, you can find a schema of the whole architecture of the example app.

![](/images/schema-1.png)

If you have any questions while going through this guide, please feel free to join this [Slack community](http://slack.cube.dev/) and post your question there.

Happy Hacking! 💻
