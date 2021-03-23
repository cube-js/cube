---
order: 1
title: "What is a pivot table?"
---

[Pivot tables](https://en.wikipedia.org/wiki/Pivot_table), also known as multi-dimensional tables or cross-tables, are tables that display the statistical summary of the data in usual, flat tables. Often, such tables come from databases, but it's not always easy to make sense of the data in large tables. Pivot tables summarize the data in a meaningful way by aggregating it with sums, averages, or other statistics.

**Here's how a pivot table is explained in Wikipedia.** Consider you have a flat table like this with e-commerce T-shirt inventory data: regions, ship dates, units, prices, etc.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/upcvyrhxaft6eb9fsyp1.png)

An inventory might be overwhelmingly lengthy, but we can easily explore the data with a pivot table. Let's say we want to know `how many items` were shipped to `each region` on `each date`. Here's the pivot table that answers exactly to this question:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/g29u48y40h6kftyo1q99.PNG)

**Analytics 101.** Note that `how many items` is an aggregated, numerical value — a sum of items that were shipped. In analytical applications, such aggregated values are called "measures". Also note that `each region` and `each date` are categorial, textual values that be can enumerate. In analytical apps, such categorial values are called "dimensions".

Actually, that's everything one should know about data analytics to work with pivot tables. We'll use this knowledge later.

# Why AG Grid?

AG Grid is a feature-rich implementation of a JavaScript data table. It supports React, Angular, and Vue as well as vanilla JavaScript. Honestly, it's no exaggeration to say that it contains every feature possible (for a data table):

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/rn02rqsxwcquhfuq03gw.png)

AG Grid's authors emphasize that it's particularly useful for building enterprise applications. So, it's understandable that it comes in two versions:
* free and open-source, MIT-licensed Community version
* free-to-evaluate but paid and non-OSS Enterprise version

Almost all features are included in the Community version, but a few are available only as a part of the Enterprise version: server-side row model, Excel export, various tool panels, and — oh, my! — *pivoting* and grouping.

It's totally okay for the purpose of this tutorial, but make sure to purchase the license if you decide to develop a production app with an AG Grid pivot table.

**Here's what our end result will look like:**

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/pq0xxdnziks2copbfy3r.png)

**Want to try it? Here's the [live demo](https://react-pivot-table-demo.cube.dev) you can use right away.** Also, the full source code is on [GitHub](https://github.com/cube-js/cube.js/tree/master/examples/react-pivot-table/).

Now we're all set, so let's pivot! 🔀