---
title: Kubernetes
permalink: /deployment/platforms/kubernetes
category: Deployment
subCategory: Platforms
menuOrder: 1
---

This guide walks you through deploying Cube.js with [Kubernetes][k8s]. This particular
deployment makes use of a `hostPath` volume to mount schema files into the
containers.

<!-- prettier-ignore-start -->
[[warning |]]
| This is an example of a production-ready deployment, but real-world
| deployments can vary significantly depending on desired performance and
| scale. For an example of deploying with [Helm][helm-k8s] and
| [Kubernetes][k8s], check out a community-contributed and supported
| [project here][gh-cubejs-examples-k8s-helm].
<!-- prettier-ignore-end -->

## Prerequisites

- [Kubernetes][k8s]

## Configuration

### Create Cube.js API instance and Refresh Worker

To deploy Cube.js, we will use [`Deployment`][k8s-docs-deployment]s and
[Service][k8s-docs-service]s. We'll start by creating a Redis deployment in a
file called `redis-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    service: redis
  name: redis
spec:
  replicas: 1
  selector:
    matchLabels:
      service: redis
  strategy: {}
  template:
    metadata:
      labels:
        service: redis
    spec:
      containers:
        - env:
            - name: ALLOW_EMPTY_PASSWORD
              value: 'yes'
          image: bitnami/redis:latest
          name: redis
          ports:
            - containerPort: 6379
          resources: {}
      restartPolicy: Always
status: {}
```

And a corresponding service for the Redis deployment in a file named
`redis-service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  labels:
    service: redis
  name: redis
spec:
  ports:
    - name: '6379'
      port: 6379
      targetPort: 6379
  selector:
    service: redis
status:
  loadBalancer: {}
```

Next, create a file called `cube-api-deployment.yaml` with the following
contents:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    service: cube-api
  name: cube-api
spec:
  replicas: 1
  selector:
    matchLabels:
      service: cube-api
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        service: cube-api
    spec:
      containers:
        - env:
            - name: CUBEJS_API_SECRET
              value: '<API_SECRET>'
            - name: CUBEJS_CUBESTORE_HOST
              value: 'cubestore-router'
            - name: CUBEJS_CUBESTORE_PORT
              value: '3030'
            - name: CUBEJS_DB_HOST
              value: '<DB_HOST>'
            - name: CUBEJS_DB_NAME
              value: '<DB_NAME>'
            - name: CUBEJS_DB_USER
              value: '<DB_USER>'
            - name: CUBEJS_DB_PASS
              value: '<DB_PASSWORD>'
            - name: CUBEJS_DB_SSL
              value: 'true'
            - name: CUBEJS_DB_TYPE
              value: 'postgres'
            - name: CUBEJS_EXTERNAL_DEFAULT
              value: 'true'
            - name: CUBEJS_REDIS_URL
              value: redis://redis:6379
          image: cubejs/cube:v%CURRENT_VERSION
          name: cube-api
          ports:
            - containerPort: 3000
            - containerPort: 4000
          resources: {}
          volumeMounts:
            - mountPath: /cube/conf
              name: cube-api-hostpath0
      restartPolicy: Always
      volumes:
        - hostPath:
            path: /home/docker/conf
          name: cube-api-hostpath0
status: {}
```

The exact set of `CUBEJS_DB_*` environment variables depends on your database;
please reference [Connecting to the Database page][ref-config-db] for specific
configuration instructions.

We'll also create a corresponding service for this deployment in a file called
`cube-api-service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  labels:
    service: cube-api
  name: cube-api
spec:
  ports:
    - name: '3000'
      port: 3000
      targetPort: 3000
    - name: '4000'
      port: 4000
      targetPort: 4000
  # THIS IS ONLY USED FOR EXPOSING AN IP WHEN USING MINIKUBE
  # externalIPs:
  #   - xxx.zzz.yyy.www
  selector:
    service: cube-api
status:
  loadBalancer: {}
```

Now that we've configured our Cube.js API, let's also set up a deployment for a
Refresh Worker in `cube-refresh-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    service: cube-refresh-worker
  name: cube-refresh-worker
spec:
  replicas: 1
  selector:
    matchLabels:
      service: cube-refresh-worker
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        service: cube-refresh-worker
    spec:
      containers:
        - env:
            - name: CUBEJS_API_SECRET
              value: '<API_SECRET>'
            - name: CUBEJS_CUBESTORE_HOST
              value: 'cubestore-router'
            - name: CUBEJS_CUBESTORE_PORT
              value: '3030'
            - name: CUBEJS_DB_HOST
              value: '<DB_HOST>'
            - name: CUBEJS_DB_NAME
              value: '<DB_NAME>'
            - name: CUBEJS_DB_USER
              value: '<DB_USER>'
            - name: CUBEJS_DB_PASS
              value: '<DB_PASSWORD>'
            - name: CUBEJS_DB_SSL
              value: 'true'
            - name: CUBEJS_DB_TYPE
              value: 'postgres'
            - name: CUBEJS_EXTERNAL_DEFAULT
              value: 'true'
            - name: CUBEJS_REDIS_URL
              value: redis://redis:6379
            - name: CUBEJS_REFRESH_WORKER
              value: 'true'
          image: cubejs/cube:v%CURRENT_VERSION
          name: cube-refresh-worker
          resources: {}
          volumeMounts:
            - mountPath: /cube/conf
              name: cube-refresh-worker-hostpath0
      restartPolicy: Always
      volumes:
        - hostPath:
            path: /home/docker/conf
          name: cube-refresh-worker-hostpath0
status: {}
```

### Create Cube Store Router and Worker nodes

With our Cube.js API and Refresh Worker set up, we can now begin setting up Cube
Store. We will make two [`StatefulSet`][k8s-docs-statefulset]s with
corresponding [Service][k8s-docs-service]s; one for the router node and one for
the worker nodes.

Create a new file called `cubestore-router-statefulset.yaml`:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  labels:
    service: cubestore-router
  name: cubestore-router
spec:
  serviceName: cubestore-router
  selector:
    matchLabels:
      service: cubestore-router
  template:
    metadata:
      labels:
        service: cubestore-router
    spec:
      containers:
        - env:
            - name: CUBESTORE_META_PORT
              value: '9999'
            - name: CUBESTORE_REMOTE_DIR
              value: /cube/data
            - name: CUBESTORE_SERVER_NAME
              value: cubestore-router:9999
            - name: CUBESTORE_WORKERS # Edit the workers if you change the number of worker replicas
              value: cubestore-workers-0.cubestore-workers:10000,cubestore-workers-1.cubestore-workers:10000,cubestore-workers-2.cubestore-workers:10000
          image: cubejs/cubestore:v0.28.14
          name: cubestore-router
          ports:
            - containerPort: 9999
          resources: {}
          volumeMounts:
            - mountPath: /cube/data
              name: cubestore-pv
      restartPolicy: Always
      volumes:
        - name: cubestore-pv
          persistentVolumeClaim:
            claimName: cubestore-pvc
```

And a corresponding service declaration in a file called
`cubestore-router-service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  labels:
    service: cubestore-router
  name: cubestore-router
spec:
  ports:
    - name: '9999'
      port: 9999
      targetPort: 9999
    - name: '3306'
      port: 3306
      targetPort: 3306
    - name: '3030'
      port: 3030
      targetPort: 3030
  selector:
    service: cubestore-router
status:
  loadBalancer: {}
```

With the router set up, let's move onto setting up our worker nodes. Let's
create a new file called `cubestore-workers-statefulset.yaml`:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  creationTimestamp: null
  labels:
    service: cubestore-workers
  name: cubestore-workers
spec:
  replicas: 3 # If you edit this, make sure to edit the CUBESTORE_WORKERS as well
  serviceName: cubestore-workers
  selector:
    matchLabels:
      service: cubestore-workers
  template:
    metadata:
      creationTimestamp: null
      labels:
        service: cubestore-workers
    spec:
      containers:
        - env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: CUBESTORE_META_ADDR
              value: cubestore-router:9999
            - name: CUBESTORE_REMOTE_DIR
              value: /cube/data
              # The $(POD_NAME) is a way of referencing dependent env vars
              # It will resolve to "cubestore-workers-n" where n is the replica number
            - name: CUBESTORE_SERVER_NAME
              value: '$(POD_NAME).cubestore-workers:10000'
              # With 3 replicas the POD_NAMEs will be cubestore-workers-0,cubestore-workers-1, and cubestore-workers-2
              # These will then be needed in the CUBESTORE_WORKERS env var as an array
              # We use "cubestore-workers-0.cubestore-workers:10000" as it points to the headless service
              # See also cubestore-router-statefulset.yaml
            - name: CUBESTORE_WORKERS
              value: cubestore-workers-0.cubestore-workers:10000,cubestore-workers-1.cubestore-workers:10000,cubestore-workers-2.cubestore-workers:10000
            - name: CUBESTORE_WORKER_PORT
              value: '10000'
          image: cubejs/cubestore:v0.28.14
          name: cubestore-workers
          ports:
            - containerPort: 10000
          volumeMounts:
            - mountPath: /cube/data
              name: cubestore-pv
      restartPolicy: Always
      initContainers:
        - name: init-router
          image: busybox
          command:
            [
              'sh',
              '-c',
              'until nc -vz cubestore-router:9999; do echo "Waiting for router";
              sleep 2; done;',
            ]
      volumes:
        - name: cubestore-pv
          persistentVolumeClaim:
            claimName: cubestore-pvc
```

Next, create its corresponding service in `cubestore-workers-service.yaml`. By
specifying `clusterIP: None`, you create a [headless
service][k8s-docs-headless-svc]. For this use case, a headless service is the
better solution.

```yaml
apiVersion: v1
kind: Service
metadata:
  creationTimestamp: null
  labels:
    service: cubestore-workers
  name: cubestore-workers
spec:
  type: ClusterIP
  clusterIP: None # Headless Service
  ports:
    - name: '10000'
      port: 10000
      targetPort: 10000
  selector:
    service: cubestore-workers
```

## Set up reverse proxy

In production, the Cube.js API should be served over an HTTPS connection to
ensure security of the data in-transit. We recommend using a reverse proxy; as
an example, let's assume we're running the Kubernetes cluster on a cloud
provider such as AWS or GCP. We'll also assume we already have the SSL
certificate and key available on our local filesystem.

<!-- prettier-ignore-start -->
[[info |]]
| You can also use a reverse proxy to enable HTTP 2.0 and GZIP compression
<!-- prettier-ignore-end -->

First, we'll run the following to create a secret in Kubernetes:

```bash
kubectl create secret tls <NAME_OF_CERTIFICATE> --key <PATH_TO_KEY_FILE> --cert <PATH_TO_CERT_FILE>
```

Next, we'll create a new ingress rule in a file called `ingress.yml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ingress-cube-api
spec:
  tls:
    - hosts:
        - <HOST_FROM_CERTIFICATE>
      secretName: <NAME_OF_CERTIFICATE>
  rules:
    - host: <HOST_FROM_CERTIFICATE>
      http:
        paths:
          - path: '/'
            pathType: Prefix
            backend:
              service:
                name: cube-api
                port:
                  number: 4000
```

<!-- prettier-ignore-start -->
[[info |]]
| For more advanced configuration, or for platforms where ingress is manually
| managed, please refer to the Kubernetes documentation for [Ingress
| Controllers][k8s-docs-ingress-controllers].
<!-- prettier-ignore-end -->

## Security

### Use JSON Web Tokens

Cube.js can be configured to use industry-standard JSON Web Key Sets for
securing its API and limiting access to data. To do this, we'll define the
relevant options on our Cube.js API instance and Refresh Worker deployments:

<!-- prettier-ignore-start -->
[[warning |]]
| If you have cubes that use `SECURITY_CONTEXT` in their `sql` property, then
| you must configure [`scheduledRefreshContexts`][ref-config-sched-ref-ctx] so
| the refresh workers can correctly create pre-aggregations.
<!-- prettier-ignore-end -->

```yaml
apiVersion: apps/v1
kind: Deployment
...
spec:
  template:
    ...
    spec:
      containers:
        - env:
          ...
          - name: CUBEJS_JWK_URL
            value: https://cognito-idp.<AWS_REGION>.amazonaws.com/<USER_POOL_ID>/.well-known/jwks.json
          - name: CUBEJS_JWT_AUDIENCE
            value: <APPLICATION_URL>
          - name: CUBEJS_JWT_ISSUER
            value: https://cognito-idp.<AWS_REGION>.amazonaws.com/<USER_POOL_ID>
          - name: CUBEJS_JWT_ALGS
            value: RS256
          - name: CUBEJS_JWT_CLAIMS_NAMESPACE
            value: <CLAIMS_NAMESPACE>
```

### Securing Cube Store

All Cube Store nodes, both router and workers, should only be accessible to
Cube.js API instances and refresh workers. To do this with Kubernetes, we need
to make sure that none of the Cube Store services are exposed in our Ingress
configuration. The Cube Store services should only be accessible from other
services within the cluster.

## Monitoring

All Cube.js logs can be found through the Kubernetes CLI:

```bash
kubectl logs
```

## Update to the latest version

Specify the tag for the Docker image available from [from Docker
Hub][dockerhub-cubejs] (currently `v%CURRENT_VERSION`). Then update your
`cube-api-deployment.yaml` and `cube-refresh-deployment.yaml` to use the new
tag:

```yaml
apiVersion: apps/v1
kind: Deployment
...
spec:
  ...
  template:
    ...
    spec:
      containers:
        - name: api
          image: cubejs/cube:v%CURRENT_VERSION
```

[dockerhub-cubejs]: https://hub.docker.com/r/cubejs/cube
[gh-cubejs-examples-k8s-helm]: https://github.com/cube-js/cube.js/tree/master/examples/helm-charts
[helm-k8s]: https://helm.sh/
[k8s-docs-deployment]:
  https://kubernetes.io/docs/concepts/workloads/controllers/deployment/
[k8s-docs-headless-svc]:
  https://kubernetes.io/docs/concepts/services-networking/service/#headless-services
[k8s-docs-ingress-controllers]:
  https://kubernetes.io/docs/concepts/services-networking/ingress-controllers/
[k8s-docs-service]:
  https://kubernetes.io/docs/concepts/services-networking/service/
[k8s-docs-statefulset]:
  https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/
[k8s]: https://kubernetes.io/
[ref-config-db]: /config/databases
[ref-config-sched-ref-ctx]: /config#options-reference-scheduled-refresh-contexts
