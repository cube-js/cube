---
order: 1
title: "What is Google BigQuery?"
---

Before you can ask, here's the application we're going to build:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/rlibk2mzd3x3ai34i7uh.png)

*And not only for the United States but for every country.*

Now to BigQuery! [BigQuery](https://cloud.google.com/bigquery) is a serverless big data warehouse available as a part of Google Cloud Platform. It's highly scalable, meaning that it can process tiny datasets as well as petabytes of data in seconds, using more cloud capacity as needed. (However, due to BigQuery's distributed architecture, you can't possibly expect it to have a sub-second query response time.)

BigQuery has a gentle learning curve, in part due to its excellent support for SQL, although (big surprise!) we won't be writing SQL queries in this tutorial.

BigQuery also has a free usage tier: you get up to 1 TB of processed data per month and some free credits to spend on Google Cloud during the first 90 days. You can probably guess that BigQuery is billed by the amount of processed data.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/dtip1f1mcth7svxvv7v2.png)

*BigQuery web console in Google Cloud, with the most important information being: "Query complete (2.3 sec elapsed, 2.1 GB processed)."*

So, let's see what datasets are waiting to be explored! 🦠