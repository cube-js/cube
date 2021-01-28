---
order: 1
title: "Overview"
---

There are many ways to visualize data, but when it comes to location-based (or geospatial) data, map-based data visualizations are the most comprehensible and graphic.

In this guide, we'll explore how to build a map data visualization with JavaScript (and React) using [MapBox](https://www.mapbox.com), a very popular set of tools for working with maps, navigation, and location-based search, etc.

We'll also learn how to make this map data visualization interactive (or dynamic), allowing users to control what data is being visualized on the map.

To make this guide even more interesting, we'll use [Stack Overflow](https://stackoverflow.com/questions) open dataset, publicly available in [Google BigQuery](https://console.cloud.google.com/marketplace/product/stack-exchange/stack-overflow) and on [Kaggle](https://www.kaggle.com/stackoverflow/stackoverflow). With this dataset, we'll be able to find answers to the following questions:

- Where do Stack Overflow users live?
- Is there any correlation between Stack Overflow users' locations and their ratings?
- What is the total and average Stack Oerflow users' rating by country?
- Is there any difference between the locations of people who ask and answer questions?

Also, to host and serve this dataset via an API, we'll use [PostgreSQL](https://www.postgresql.org) as a database and [Cube.js](https://cube.dev) as an analytical API platfrom which allows to bootstrap an backend for an analytical app in minutes.

So, that's our plan — and let's get hacking! 🤘

Oh, wait! Here's what our result is going to look like! Amazing, huh?

[![](/images/demo.gif)](https://mapbox-demo.cube.dev)

If you can't wait, feel free to study the [demo](https://mapbox-demo.cube.dev) and the [source code](https://github.com/cube-js/cube.js/tree/master/examples/mapbox) on GitHub.