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
helm install cube-stack . -f ./values.yaml
```

Because this is an Umbrella Chart, you can set any value from the dependent charts in the `values.yaml`.

Any value from the `../cubejs/values.yaml`, `../cubestore/values.yaml`, and `bitnami/redis` can be set in the `./values.yaml`.

The requirement is to prefix the values with `cubejs`, `cubestore`, and `redis` respectively.

Here's an example:

```yaml
# values.yaml

global:
  cubejs:
    enabled: true
  cubestore:
    enabled: true
  redis:
    enabled: true

cubejs:
...

  config:
    apiSecret: secret

    volumes:
      - name: schema
        configMap:
          name: schema
    volumeMounts:
      - name: schema
        readOnly: true
        mountPath: /cube/conf/schema
...
```

## Details

To view more config options please look at the `../cubejs` and `../cubestore` Charts respectively.
