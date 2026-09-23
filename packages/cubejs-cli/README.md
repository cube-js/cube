<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://docs.cube.dev) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube/workflows/Build/badge.svg)](https://github.com/cube-js/cube/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube CLI

> [!WARNING]
> `cubejs-cli` is deprecated. Please use the new native Cube CLI (`cube`) instead:
>
> ```bash
> curl -fsSL https://raw.githubusercontent.com/cube-js/cube/master/install-cli.sh | sh
> ```
>
> See the [Cube CLI reference](https://docs.cube.dev/reference/cli) for details.
> Set `CUBEJS_CLI_NO_DEPRECATION_WARNING=true` to suppress the warning.

Install:

```
npm install -g cubejs-cli
```

Use:

```
cubejs create hello-world -d postgres
```

[Learn more](https://github.com/cube-js/cube#getting-started)

## Tests

Tests should be run as part of the `pre-commit` hook for any changes to this package.

You can run tests manually by running

```bash
npm test
```

### License

Cube CLI is [Apache 2.0 licensed](./LICENSE).
