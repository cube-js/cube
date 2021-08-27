# Minikube Setup for K8s with `kubectl`

This is a generic config for Minikube.

The Cube API is configured to run with a `hostPath` volume.

The Cube Store is configured to persist data with a `hostPath` volume.

Contains:
- Cube API - `Deployment` and `Service`
- Cube Store - `Deployment` and `Service`

Tested with:
- `apiVersion: v1`.
- `Kubernetes v1.21.2`
- `kubectl v1.22.1`
- `Docker 20.10.7`

Maintainers:
- email: adnan@cube.dev  
  name: Adnan Rahic
