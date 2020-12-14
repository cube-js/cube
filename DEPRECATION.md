# Deprecation

This page provides an overview of features that are deprecated in Cube.js.
Changes in packaging, and supported (Linux) distributions are not included. To
learn about end of support for Linux distributions, refer to the
[changelog](CHANGELOG.md).

## Feature Deprecation Policy

As changes are made to Cube.js, there may be times when existing features need
to be removed or replaced with newer features. Before an existing feature is
removed it is marked as "deprecated" within the documentation and remains in
Cube.js for at least one stable release unless specified explicitly otherwise.
After that time it may be removed.

Users are expected to take note of the list of deprecated features each release
and plan their migration away from those features, and (if applicable) towards
the replacement features as soon as possible.

## Deprecated Features

The table below provides an overview of the current status of deprecated
features:

- **Deprecated**: the feature is marked "deprecated" and should no longer be
  used. The feature may be removed, disabled, or change behavior in a future
  release. The _"Deprecated"_ column contains the release in which the feature
  was marked deprecated, whereas the _"Remove"_ column contains a tentative
  release in which the feature is to be removed.
- **Removed**: the feature was removed, disabled, or hidden. Refer to the linked
  section for details. Some features are "soft" deprecated, which means that
  they remain functional for backward compatibility, and to allow users to
  migrate to alternatives. In such cases, a warning may be printed, and users
  should not rely on this feature.

| Status     | Feature                                                               | Deprecated | Remove    |
| ---------- | --------------------------------------------------------------------- | ---------- | --------- |
| Deprecated | [Embedding Cube.js within Express](#embedding-cubejs-within-express)  | v0.24.0    | June 2021 |
| Deprecated | [`CUBEJS_ENABLE_TLS`](#cubejs_enable_tls)                             | v0.23.11   | June 2021 |
| Deprecated | [`hearBeatInterval`](#hearbeatinterval)                               | v0.23.8    | June 2021 |
| Deprecated | [Node.js 8](#nodejs-8)                                                | v0.22.4    |   v0.26.0 |

### Embedding Cube.js within Express

**Deprecated in Release: v0.24.0**

Embedding Cube.js into Express applications is deprecated due to performance and
reliability considerations. [Read more about this change
here][link-cube-docker].

Developers are encouraged to [migrate to the new `cube.js` configuration
file][link-migration] and deploy Cube.js as a microservice (or multiple
microservices, if necessary).

[link-cube-docker]: https://cube.dev/blog/cubejs-loves-docker
[link-migration]:
  https://cube.dev/docs/configuration/overview#migrating-from-express-to-docker

### `CUBEJS_ENABLE_TLS`

**Deprecated in Release: v0.23.11**

We no longer recommend setting TLS options via Cube.js. Developers should set up
TLS on a load balancer or reverse proxy instead. [Read more
here][link-enable-https].

[link-enable-https]:
  https://cube.dev/docs/deployment/production-checklist#enable-https

### `hearBeatInterval`

**Deprecated in Release: v0.23.8**

This option for [`@cubejs-client/ws-transport`][link-hearbeatinterval] has been
replaced by `heartBeatInterval`.

[link-hearbeatinterval]:
  https://cube.dev/docs/@cubejs-client-ws-transport#web-socket-transport-hear-beat-interval

### Node.js 8

**Deprecated in Release: v0.22.4**

Node.js 8 reached [End of Life on December 31, 2019][link-nodejs-eol]. This
means no more updates. Please upgrade to Node.js 10 or higher.

[link-nodejs-eol]: https://github.com/nodejs/Release#end-of-life-releases
