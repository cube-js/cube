# Prod Setup for K8s with `kubectl`

A set of YAML files to configure Cube with K8s running in prod.

This is a generic config to cover as many deployment cases as possible.

The Cube API is configured to run with schema files loaded a `ConfigMap`. A Cube Refresh Worker is included in the config.

The Cube Store is configured to persist data with `PersistentVolume`s and `PersistentVolumeClaim`s. It's also configured by default to run with 3 workers. This config will store pre-aggregations with Cube Store by default.

Redis will store query results and metadata between the Cube API and Cube Store. Deployed per this config it will work out-of-the-box.

The Ingress resource contains a sample of how you should configure your own Ingress, after you generate a TLS cert and add it as a secret to your K8s cluster.

Contains:
- Cube API - `Deployment`, `Service`, and `ConfigMap` (the `ConfigMap` contains the schema files)
- Cube Refresh Worker - `Deployment`
- Cube Store - `StatefulSet`s for the Router and Workers, `Service`s for the Router and Workers, `PersistentVolume`s, and `PersistentVolumeClaim`s
- Redis - `Deployment` and `Service`
- Ingress - `Ingress`, and `Secret` , and a sample Nginx Ingress Controller

Tested with:
- `apiVersion: v1`.
- `Kubernetes v1.21.2`
- `kubectl v1.22.1`
- `Docker 20.10.7`

Maintainers:
- email: adnan@cube.dev  
  name: Adnan Rahic

Contributors:
- email: luc.vauvillier@gmail.com  
  name: Luc Vauvillier
