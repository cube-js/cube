# Regenerating models

## Code generation

### Homebrew

You need to install Open API Generator:

```sh
brew install openapi-generator
```

Regenerate models:

```bash
cd rust/cubesql
openapi-generator generate -i ../../packages/cubejs-api-gateway/openspec.yml -g rust -o cubeclient
```

### Docker

From repo root

```sh
docker run --rm -v ".:/cube" --workdir /cube/rust/cubesql openapitools/openapi-generator-cli:v7.14.0 generate -i ../../packages/cubejs-api-gateway/openspec.yml -g rust -o cubeclient
```

Take care around Docker on root and files owner and mode

## Post-processing

The above command will also overwrite `src/apis/default_api.rs`, remember to do the following:

1. Revert block for long polling in `load_v1()`
2. Revert block containing `tests` module

Finally, run `cargo fmt`
