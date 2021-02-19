---
order: 4
title: "How to Explore the Data"
---

As the console output suggests, let's navigate to [localhost:4000](http://localhost:4000) and behold Cube.js Developer Playground. It provides a lot of features, but we have a clear path to follow.

**First, let's generate the data schema.** To do so, go to the "Schema" tab, select all necessary tables, and click "Generate Schema".

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/i/uxqed2kz3xgw7xnwlx3n.png)

The [data schema](https://cube.dev/docs/getting-started-cubejs-schema) is a high-level domain-specific description of your data. It allows you to skip writing SQL queries and rely on Cube.js query generation engine. You can see how the data schema files look like if you go to `HitsV1.js` or `VisitsV1.js` files in the sidebar.

**Second, let's build a query.** To do so, go to the "Build" tab, click "+ Measure", and select a measure. For example, select `Hits V1 Count`. Our dataset is all about web traffic: web page hits and user visits. As you can see, the "Hits V1 Eventtime" time dimension has been automatically selected, and the chart below displays the count of page hits for every day from `2014-03-16` to `2014-03-23`. What an old dataset that is! Want to see more data points? Click "Day" and select "Hour" instead. Now it's more interesting!

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/i/4mdqlwzj4t46dsuaxolk.png)

Definitely feel free to experiment and try your own queries, measures, dimensions, time dimensions, and filters.

**Third, let's check the query.** Note there're a lot of controls and options just above the chart. You can switch between different views and charting libraries, view Cube.js query in JSON format, or browse what SQL was generated for that query. You don't really want to write SQL like that from scratch, right?

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/i/cz2xpvnan9xuga7scaqr.png)

It's turbulence, so brace for impact! ✈️