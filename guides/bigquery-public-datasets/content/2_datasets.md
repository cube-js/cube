---
order: 2
title: "What are BigQuery Public Datasets?"
---

[BigQuery public datasets](https://console.cloud.google.com/marketplace/browse?filter=solution-type:dataset) are made available without any restrictions to all Google Cloud users. Google pays for the storage of these datasets. You can use them to learn how to work with BigQuery or even build your application on top of them, exactly as we're going to do.

You could query them just if they were your own. However, always pay attention to the license and other relevant info, like update frequency and last update date. Unfortunately, some datasets are really outdated:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/v7gvj282p12yfvzhz6tf.png)

So, what to expect? Some of these [212 public datasets](https://console.cloud.google.com/marketplace/browse?filter=solution-type:dataset) are quite interesting:

* [28 datasets](https://console.cloud.google.com/marketplace/browse?filter=solution-type:dataset,category:climate) about climate, including the [Real-time Air Quality](https://console.cloud.google.com/marketplace/product/openaq/real-time-air-quality) dataset
* [24 datasets](https://console.cloud.google.com/marketplace/browse?filter=solution-type:dataset,category:developer-tools) related to developer tools, including [GitHub Activity Data](https://console.cloud.google.com/marketplace/product/github/github-repos)
* [33 datasets](https://console.cloud.google.com/marketplace/browse?filter=solution-type:dataset,category:encyclopedic) marked encyclopedic, including [Hacker News](https://console.cloud.google.com/marketplace/product/y-combinator/hacker-news) dataset
* and [33 datasets](https://console.cloud.google.com/marketplace/browse?filter=solution-type:dataset,category:covid19) for COVID-19 research — let's talk about them!

**COVID-19 Government Response Tracker.** This [dataset](https://console.cloud.google.com/marketplace/product/university-of-oxford/covid19_govt_policy) is maintained by the University of Oxford Blavatnik School of Government. It tracks policy responses to COVID-19 from governments around the world. Basically, all lockdowns, curfews, and workplace closures worldwide are registered in this dataset.

**Google Community Mobility Reports.** This [dataset](https://console.cloud.google.com/marketplace/product/bigquery-public-datasets/covid19_google_mobility) is maintained by Google. It provides insights into what has changed in people's habits and behavior in response to policies aimed at combating COVID-19. It reports movement trends over time by geography, across different retail and recreation categories, groceries and pharmacies, parks, transit stations, workplaces, and residential.

We can use both datasets to visualize and correlate the time measures against COVID-19 with changes in social mobility. Here's how it might look like:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/amefzcyiqamzfleopqw8.png)

For that, we need to create an analytical API over BigQuery and a web application talking to that API. So, let's get hacking! 🦠
