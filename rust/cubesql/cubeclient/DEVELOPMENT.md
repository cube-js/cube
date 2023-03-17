# Regenerating models

## Prerequirements

You need to install Open API Generator:

```sh
brew install openapi-generator
```

Regenerate models:

```bash
cd rust/cubesql
openapi-generator generate -i ../../packages/cubejs-api-gateway/openspec.yml -g rust -o cubeclient
```

The above command will also overwrite `src/apis/default_api.rs`, remember to do the following:

1. Revert block for long polling in `load_v1()`
2. Revert block containing `tests` module

Finally, run `cargo fmt`
