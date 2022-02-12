# Setup for Kubernetes Minikube

This is a generic config for Minikube.

The Cube Server is configured to run with a `hostPath` volume.

The Cube Store is configured to persist data with a `hostPath` volume.

In comparison to the `examples/cluster` folder, this example is only suited for Minikube or other single-instance Kubernetes clusters. The example is using `hostPath` volumes, which is not suited for production. Additionally, this example does not have a refresh worker. It also lacks a production ready setup of Cube Store with router and worker nodes. To see how to configure all of that, check out the `examples/cluster` folder, or look at the `examples/helm-charts`.

Contains:
- Cube API - `Deployment` and `Service`
- Cube Store - `Deployment` and `Service`
- Redis - `Deployment` and `Service`

Tested with:
- `apiVersion: v1`.
- `Kubernetes v1.21.2`
- `kubectl v1.22.1`
- `Docker 20.10.7`

Maintainers:
- email: adnan@cube.dev  
  name: Adnan Rahic
