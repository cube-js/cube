---
order: 1
title: "Overview"
---

Building your own analytics engine, like the one behind Google Analytics, sounds like a very
sophisticated engineering problem. And it truly is. Back then it would require
years of engineering time to ship such a piece of software. But as data
landscape changes, now we have a lot of tools which solve different part of
this problem extremely well: data collection, storage, aggregations and query
engine. By breaking the problem into smaller pieces and solving them one-by-one
by using existing open-source tools we will be able to build our own web
analytics engine.

If you’re familiar with Google Analytics (GA), you probably already know that every web page tracked by GA contains a GA tracking code. It loads an async script that assigns a tracking cookie to a user if it isn’t set yet. It also sends an XHR for every user interaction, like a page load. These XHR requests are then processed and raw event data is stored and scheduled for aggregation processing. Depending on the total amount of incoming requests the data will also be sampled.

Even though this is a high level overview of Google Analytics essentials, it’s enough to reproduce most of the functionality.

You can check the online demo of the application we are going to build here and the complete source code is available on Github.

## Architecture overview

Below you can see the architecture of the application we are going to build.
We'll use Snowplow for data collection, Athena as the main data warehouse, MySQL to store pre-aggregations and Cube.js as the aggregation and querying engine. The frontend will be built with React, Material UI, and Recharts. Although the schema below shows some AWS services, they can be partially or fully substituted by open-source alternatives: Kafka, MinIO and PrestoDB instead of Kinesis, S3 and Athena respectively.

![](https://raw.githubusercontent.com/cube-js/cube.js/master/examples/web-analytics/web-analytics-schema.png)

We'll start with data collection and gradually build the whole application
including the frontend. If you have any questions while going through this guide, please feel free to join this Slack community and post your question there.

Happy Hacking! 💻
