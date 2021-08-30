# Prod Setup for K8s with `kubectl`

This is a generic config to cover as many deployment cases as possible.

## Cube API

The Cube API is configured to run with schema files loaded a `ConfigMap`.

This example will load schemas from, and connect to, the Cube sample `ecom` database.

### Editing the Schema ConfigMap

We opted for what we think is best practice. We create a single configmap containing all the cube schema files:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cube-api-schema
data:
  Cube1.js: |
    cube(`Cube1`, {
      sql: `SELECT * FROM cube1_data`,

      measures: {
        count: {
          type: `count`,
        },
      },
    });
  Cube2.js: |
    cube(`Cube2`, {
      sql: `SELECT * FROM cube2_data`,

      measures: {
        count: {
          type: `count`,
        },
      },
    });
```

## Cube Refresh Worker

A Cube Refresh Worker is included in the config.

It is configured exactly like the Cube API with the difference of having one additional env var.

```yaml
...
spec:
  containers:
    - env:
      - name: CUBEJS_REFRESH_WORKER
        value: "true"
...
```

## Cube Store

The Cube Store is configured to persist data with `PersistentVolume`s and `PersistentVolumeClaim`s.
The `PersistentVolume` and `PersistentVolumeClaim` is bound to `/cube/data` in both the router and workers.
This default example is configured to run with one router and three workers. This config will store pre-aggregations with Cube Store by default.

### Cube Store Router

The important config in the `cinestore-router-statefulset.yaml` is the `CUBESTORE_WORKERS` env var.

```yaml
...
spec:
  containers:
    - env:
        ...
        - name: CUBESTORE_WORKERS
          value: cubestore-workers-0.cubestore-workers:10000,cubestore-workers-1.cubestore-workers:10000,cubestore-workers-2.cubestore-workers:10000
          ...
...
```

These resolve to `$POD_NAME.$SERVICE_NAME.$PORT`. In the case of `cubestore-workers-0.cubestore-workers:10000`, the `cubestore-workers-0` is the name of the worker pod, where `cubestore-workers` is the name of the workers `Headless Service`, and `10000` is the worker port.

These are configured in the `cubestore-workers-statefulset.yaml`.

### Cube Store Workers

We use one `StatefulSet` for the workers. By increasing the replica count we increase the worker count.

The important config to make sure the workers are connected is in the `cubestore-workers-statefulset.yaml`.

```yaml
...
spec:
  containers:
    - env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
          # The $(POD_NAME) is a way of referencing dependent env vars
          # It will resolve to "cubestore-workers-n" where n is the replica number
        - name: CUBESTORE_SERVER_NAME
          value: "$(POD_NAME).cubestore-workers:10000"
          # With 3 replicas the POD_NAMEs will be cubestore-workers-0,cubestore-workers-1, and cubestore-workers-2
          # These will then be needed in the CUBESTORE_WORKERS env var as an array
          # We use "cubestore-workers-0.cubestore-workers:10000" as it points to the headless service
          # See also cubestore-router-statefulset.yaml
        - name: CUBESTORE_WORKERS
          value: cubestore-workers-0.cubestore-workers:10000,cubestore-workers-1.cubestore-workers:10000,cubestore-workers-2.cubestore-workers:10000
        ...
...
```

To make sure these DNS resolutions are expected, we use a Headless Service.
The `cubestore-workers-service.yaml` is configured like this to make it Headless.

```yaml
...
spec:
  type: ClusterIP
  clusterIP: None # Headless Service
...
```

## Redis

Redis will store query results and metadata between the Cube API and Cube Store. Deployed per this config it will work out-of-the-box.

## Ingress

The Ingress resource contains a sample of how you should configure your own Ingress, after you generate a TLS cert and add it as a secret to your K8s cluster.

## Contains

- Cube API - `Deployment`, `Service`, and `ConfigMap` (the `ConfigMap` contains the schema files)
- Cube Refresh Worker - `Deployment`
- Cube Store - `StatefulSet`s for the Router and Workers, `Service`s for the Router and Workers, `PersistentVolume`s, and `PersistentVolumeClaim`s
- Redis - `Deployment` and `Service`
- Ingress - `Ingress`, and `Secret` , and a sample Nginx Ingress Controller

## Tested with

- `apiVersion: v1`.
- `Kubernetes v1.21.2`
- `kubectl v1.22.1`
- `Docker 20.10.7`


## Maintainers:

- email: adnan@cube.dev  
  name: Adnan Rahic

## Contributors

- email: luc.vauvillier@gmail.com  
  name: Luc Vauvillier
