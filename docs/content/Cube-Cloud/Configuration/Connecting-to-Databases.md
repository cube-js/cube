---
title: Connecting to Databases
permalink: /cloud/configuration/connecting-to-databases
category: Configuring Cube Cloud
menuOrder: 1
---

You can connect all Cube.js supported databases to your Cube Cloud deployment.

![Cube Cloud Supported Databases Screen](https://cube.dev/downloads/images/cube-cloud-databases-list.png)

Below you can find guides on how to use Cube Cloud with specific database
vendors.

- Snowflake
- BigQuery
- AWS Athena

## Connecting to multiple databases

If you are connecting to multiple databases you can skip the database connection
step during the deployment creation. First, make sure you have the correct
configuration in your `cube.js` file according to your
[multitenancy setup](/multitenancy-setup). Next, configure the corresponding
environment variables on the **Settings - Env Vars page**.

## Allowing connections from Cube Cloud IP

In some cases you'd need to allow connections from your Cube Cloud deployment IP
address to your database. You can copy the IP address from either the Database
Setup step in deployment creation, or from the Env Vars tab in your deployment
Settings page.

## Connecting to a database not exposed over the internet

[Contact us](mailto:support@cube.dev) for VPC peering and on-premise solutions.
