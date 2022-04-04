# CubeClient

> API client for Cube.JS powered on Rust language

Models are generated from [Cube.js Open API Specificiation](https://github.com/cube-js/cube.js/blob/master/packages/cubejs-api-gateway/openspec.yml).

# Protocols

- [X] HTTP (v1)
- [ ] WS (v1)

# Example

```rust
use cubeclient::apis::{configuration::Configuration, default_api as cube_api};
use cubeclient::models::{V1LoadRequest, V1LoadRequestQuery};

let mut cube_config = Configuration::default();
cube_config.bearer_access_token = Some("my token".to_string());
cube_config.base_path = Some("https://myapi.mydomain.mysubdomain/".to_string());

let query = {}; // build your own query
let request = V1LoadRequest {
    query: Some(query),
    query_type: Some("multi".to_string()),
};
let response = cube_api::load_v1(&self.get_client_config_for_ctx(ctx), Some(request)).await?;
```

## License

Apache 2.0 licensed
