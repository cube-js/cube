<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Dremio Driver

Pure Javascript Dremio driver.

## Dremio Cloud

To use this driver with [Dremio Cloud](https://docs.dremio.com/cloud/reference/api/), use the following setup:

| Environment Variable        | Value                                              |
| --------------------------- | -------------------------------------------------- |
| CUBEJS_DB_TYPE              | dremio                                             |
| CUBEJS_DB_URL               | https://api.dremio.cloud/v0/projects/${PROJECT_ID} |
| CUBEJS_DB_NAME              | ${DB_NAME}                                         |
| CUBEJS_DB_DREMIO_AUTH_TOKEN | ${PERSONAL_ACCESS_TOKEN}                           |

> [!NOTE]
> When `CUBEJS_DB_URL` is set it takes precedence over `CUBEJS_DB_HOST` and it
> is assumed that the driver is connecting to the Dremio Cloud API.

## Support

This package is **community supported** and should be used at your own risk.

While the Cube Dev team is happy to review and accept future community contributions, we don't have active plans for further development. This includes bug fixes unless they affect different parts of Cube.js. **We're looking for maintainers for this package.** If you'd like to become a maintainer, please contact us in Cube.js Slack.

## License

Cube.js Dremio Driver is [Apache 2.0 licensed](./LICENSE).
