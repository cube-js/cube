---
order: 3
title: "How to Explore the Data"
---

Before we can tinker with the data, we need to describe it with a data schema. The [data schema](https://cube.dev/docs/getting-started-cubejs-schema?utm_source=dev-to&utm_medium=post&utm_campaign=react-pivot-table) is a high-level domain-specific description of your data. It allows you to skip writing SQL queries and rely on Cube.js to generate them for you.

As the console output suggests, please navigate to [localhost:4000](http://localhost:4000) — this application is the Cube.js Developer Playground. It's able to generate an initial version of the data schema automatically. Go to the "Schema" tab, select all tables under "public", and click the "Generate Schema" button.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/ppnwb0pr2gjmlernv2u5.png)

That's all. You can check that in the `schema` folder there's a number of files containing the data schema files: `Orders.js`, `Products.js`, `Users.js`, etc.

Now we have the data schema in place. Let's explore the data!

Go to the "Build" tab, click "+ Dimension" or "+ Measure," and select any number of dimensions and measures. For example, let's select these measures and dimensions:

* `Orders Count` measure
* `Line Items Price`  measure
* `Line Items Quantity`  measure
* `Products Name` dimension
* `Orders Status` dimension
* `Users City` dimension

As the result, you should get a complex, lengthy table with the data about our e-commerce enterprise:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/lqwki78rj4vnr5kmrb4a.png)

Looks interesting, right? Definitely feel free to experiment and try your own queries, measures, dimensions, time dimensions, granularities, and filters.

Take note that, at any time, you can click the "JSON Query" button and see the query being sent to Cube.js API in JSON format which, essentially lists the measures and dimensions you were selecting in the UI.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/6gqza5fniv7uqaxwk9pb.png)

Later, we'll use this query to fill our upcoming pivot table with data. So, let's move on and build a pivot table! 🔀
