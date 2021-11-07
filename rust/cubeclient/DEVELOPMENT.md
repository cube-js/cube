# Regenerating models

## Prerequirements

You need to install Open API Generator:

```sh
brew install openapi-generator
```

Regenerate models:

```bash
$ cd rust
$ openapi-generator generate -i ../packages/cubejs-api-gateway/openspec.yml -g rust -o cubeclient
```
