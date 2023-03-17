<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) •
[Examples](#examples) • [Blog](https://cube.dev/blog) •
[Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/thecubejs)

[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Rust/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ARust+branch%3Amaster)
[![Crates.io (latest)](https://img.shields.io/crates/dv/cubeclient)](https://crates.io/crates/cubeclient)
[![Documentation (latest)](https://img.shields.io/badge/Documentation-latest-orange)](https://docs.rs/pg-srv/latest/cubeclient/)

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
