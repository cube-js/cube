# Cube Stack Chart

This Helm Chart is an Umbrella chart wrapping everything that's needed to run an entire Cube Stack:

- Cube Server
- Cube Store
- Redis

## Add the Bitnami Helm Repo

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
```

## Update Dependencies

```bash
helm dependency update
```

## Install the Cube Stack

Installing the Chart with the `values.yaml` file looks like this:

```bash
helm install cubestack . -f ./values.yaml
```

Because this is an Umbrella Chart, you can set any value from the dependent charts in the `values.yaml`.

Any value from the `../cubejs/values.yaml`, `../cubestore/values.yaml`, and `bitnami/redis` can be set in the `./values.yaml`.

The requirement is to prefix the values with `cubejs`, `cubestore`, and `redis` respectively.

Here's an example:

```yaml
# values.yaml

cubejs:
...

  global:
    ## The port for a Cube.js deployment to listen to API connections on
    ##
    apiPort: 4000

    ## If true, enables development mode.
    ##
    devMode: false

  redis:
    ## The host URL for a Redis server
    ## Naming this release "cubestack" will give you this default Redis URL 
    url: redis://cubestack-redis-master:6379

    ## The password used to connect to the Redis server
    ##
    password: "your-password"

...

redis:
  enabled: true

  global:
    redis:
      password: "your-password"
...
```

> Note: If you name the Helm release `cubestack`, the default value for the Redis URL will be valid.

## Details

To view more config options please look at the `../cubejs` and `../cubestore` Charts respectively.
