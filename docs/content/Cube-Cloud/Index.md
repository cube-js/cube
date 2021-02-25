---
title: Cube Cloud
permalink: /cloud
category: Overview
menuOrder: 1
---

<!-- prettier-ignore-start -->
[[info |]]
| [Cube Cloud][link-cube-cloud] currently is in early access. If you don't have
| an account yet, you can [sign up to the waitlist here][link-cube-cloud].
<!-- prettier-ignore-end -->

[link-cube-cloud]: https://cube.dev/cloud

Cube Cloud offers hosted Cube.js service with extra features for reliability and
performance. It includes all the core features of Cube.js, while taking care of
infrastructure concerns such as the number of instances, memory,
high-availability, pre-aggregations management, caching, scalability, real-time
monitoring and tracing.

[Quickstart](/cloud/quickstart)

## Develop Cube.js projects

You can use Cube Cloud IDE to develop and run Cube.js applications. By connecting your GitHub account, you can keep your data schema under version control.

![](https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/cube-ide.png)

## Run and Scale Cube.js applications

Cube Cloud provides on-demand scalable infrastructure and pre-aggregations storage. Cube Cloud runs hundreds of Cube Store instances to ingest and query pre-aggregations with low latency and high concurrency. It is available to all users on the Standard plan and higher.

## Live preview your feature branchs

Cube Cloud can spin up Cube.js API instances to test changes to the data schema
in feature branches. You can use branch-based development API URLs in your
frontend application to test changes before shipping them to production.


## Inspect Cube.js queries

You can trace and inspect your Cube.js queries to find performance flaws and
apply optimizations. Cube Cloud also provides tips and suggestions on what
pre-aggregation should be used.
