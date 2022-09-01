---
title: Deployment Types
permalink: /cloud/configuration/deployment-types
category: Configuration
menuOrder: 2
---

Cube Cloud deployment type determines the features, scalability, and
availability of your deployment.

## Single Instance Deployments

Single Instance deployments are designed for development use-cases only.
If not used for over an hour, it'll take additional time to process the first request.
Refresh Worker is active only when a Single Instance serves the workload.

## Cluster Deployments

Cluster deployments are designed for production workload. Cluster deployment
includes the following components:

- Cube API instances
- Cube Refresh Workers
- Redis for Cache and Queue
