---
order: 2
title: "How to Run ClickHouse"
---

Surprisingly, there're plenty of options to get started with ClickHouse:

**[Install and run](https://clickhouse.tech/docs/en/getting-started/install/#available-installation-options) ClickHouse on your macOS or Linux developer machine.** It's good for testing purposes, but somewhat suboptimal if you want to get trustworthy insights about production-like ClickHouse performance.

**Install and run ClickHouse on AWS, GCP, or any other cloud computing platform.** It's great for testing and production use, especially if you (or your company) already have active accounts there. While setting up ClickHouse in [AWS EC2](https://medium.com/left-join/installing-clickhouse-on-aws-e4223a8002ec) from scratch is easy, there's also a ready-to-use ClickHouse container for [AWS EKS](https://aws.amazon.com/marketplace/pp/B08P7NZYJT?qid=1613039745696&sr=0-2&ref_=srh_res_product_title).

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/i/76fbharplzjemh3cwwr3.png)

**Run managed ClickHouse in [Yandex Cloud](https://cloud.yandex.com/services/managed-clickhouse), yet another cloud computing platform.** It's also a great option for testing and production use. First, ClickHouse was originally developed and open-sourced by [Yandex](https://yandex.com/company/), a large technology company, in June 2016. Second, setting up ClickHouse in Yandex Cloud in a fully managed fashion will require less time and effort.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/i/o97mzpaow5q1vhv9vp5k.png)

And that's not all! You can also...

**Use ClickHouse Playground, a publicly available read-only installation with a [web console](https://play.clickhouse.tech) and [API access](https://clickhouse.tech/docs/en/getting-started/playground/).** While it doesn't allow to run `INSERT` or data-definition queries such as `CREATE TABLE`, ClickHouse Playground is a great zero-setup tool to start working with ClickHouse.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/i/o7ymw2ol9u7kxg3vjjz9.png)

**Already have a ClickHouse installation?** Great! You can use your own credentials to proceed with this tutorial. Otherwise, we'll use these readily available credentials from ClickHouse Playground:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/i/9ho8bvf613ls636a8vgb.png)

We're almost at 35,000 feet, so get ready for your snack! ✈️