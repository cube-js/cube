# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.11.18](https://github.com/cube-js/cube.js/compare/v0.11.17...v0.11.18) (2019-11-09)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.11.6](https://github.com/statsbotco/cubejs-client/compare/v0.11.5...v0.11.6) (2019-10-17)


### Bug Fixes

* TypeError: Cannot read property 'table_name' of undefined: Drop orphaned tables implementation drops recent tables in cluster environments ([84ea78a](https://github.com/statsbotco/cubejs-client/commit/84ea78a))





# [0.11.0](https://github.com/statsbotco/cubejs-client/compare/v0.10.62...v0.11.0) (2019-10-15)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.10.58](https://github.com/statsbotco/cubejs-client/compare/v0.10.57...v0.10.58) (2019-10-04)


### Bug Fixes

* `continueWaitTimout` option is ignored in LocalQueueDriver ([#224](https://github.com/statsbotco/cubejs-client/issues/224)) ([4f72a52](https://github.com/statsbotco/cubejs-client/commit/4f72a52))





## [0.10.35](https://github.com/statsbotco/cubejs-client/compare/v0.10.34...v0.10.35) (2019-09-09)


### Bug Fixes

* LocalQueueDriver key interference for multitenant deployment ([aa860e4](https://github.com/statsbotco/cubejs-client/commit/aa860e4))


### Features

* Serve pre-aggregated data right from external database without hitting main one if pre-aggregation is available ([931fb7c](https://github.com/statsbotco/cubejs-client/commit/931fb7c))





## [0.10.33](https://github.com/statsbotco/cubejs-client/compare/v0.10.32...v0.10.33) (2019-09-06)


### Bug Fixes

* Revert to default queue concurrency for external pre-aggregations as driver pools expect this be aligned with default pool size ([c695ddd](https://github.com/statsbotco/cubejs-client/commit/c695ddd))





## [0.10.32](https://github.com/statsbotco/cubejs-client/compare/v0.10.31...v0.10.32) (2019-09-06)


### Bug Fixes

* In memory queue driver drop state if rollups are building too long ([ad4c062](https://github.com/statsbotco/cubejs-client/commit/ad4c062))





## [0.10.30](https://github.com/statsbotco/cubejs-client/compare/v0.10.29...v0.10.30) (2019-08-26)


### Features

* `REDIS_TLS=true` env variable support ([55858cf](https://github.com/statsbotco/cubejs-client/commit/55858cf))





## [0.10.28](https://github.com/statsbotco/cubejs-client/compare/v0.10.27...v0.10.28) (2019-08-19)


### Bug Fixes

* BigQuery to Postgres external rollup doesn't work ([feccdb5](https://github.com/statsbotco/cubejs-client/commit/feccdb5)), closes [#178](https://github.com/statsbotco/cubejs-client/issues/178)





## [0.10.16](https://github.com/statsbotco/cubejs-client/compare/v0.10.15...v0.10.16) (2019-07-20)


### Features

* BigQuery external rollup support ([10c635c](https://github.com/statsbotco/cubejs-client/commit/10c635c))





## [0.10.15](https://github.com/statsbotco/cubejs-client/compare/v0.10.14...v0.10.15) (2019-07-13)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.10.12](https://github.com/statsbotco/cubejs-client/compare/v0.10.11...v0.10.12) (2019-07-06)


### Bug Fixes

* QUERIES_undefined redis key for QueryQueue ([4c44886](https://github.com/statsbotco/cubejs-client/commit/4c44886))





## [0.10.11](https://github.com/statsbotco/cubejs-client/compare/v0.10.10...v0.10.11) (2019-07-02)


### Bug Fixes

* TypeError: Cannot read property 'startsWith' of undefined at tableDefinition.filter.column: support uppercase databases ([995b115](https://github.com/statsbotco/cubejs-client/commit/995b115))





# [0.10.0](https://github.com/statsbotco/cubejs-client/compare/v0.9.24...v0.10.0) (2019-06-21)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.9.14](https://github.com/statsbotco/cubejs-client/compare/v0.9.13...v0.9.14) (2019-06-07)


### Features

* Add option to run in production without redis ([a7de417](https://github.com/statsbotco/cubejs-client/commit/a7de417)), closes [#110](https://github.com/statsbotco/cubejs-client/issues/110)





## [0.9.4](https://github.com/statsbotco/cubejs-client/compare/v0.9.3...v0.9.4) (2019-05-22)


### Features

* Add `refreshKeyRenewalThreshold` option ([aa69449](https://github.com/statsbotco/cubejs-client/commit/aa69449)), closes [#112](https://github.com/statsbotco/cubejs-client/issues/112)





## [0.9.2](https://github.com/statsbotco/cubejs-client/compare/v0.9.1...v0.9.2) (2019-05-11)


### Bug Fixes

* External rollups serverless implementation ([6d13370](https://github.com/statsbotco/cubejs-client/commit/6d13370))





# [0.9.0](https://github.com/statsbotco/cubejs-client/compare/v0.8.7...v0.9.0) (2019-05-11)


### Features

* External rollup implementation ([d22a809](https://github.com/statsbotco/cubejs-client/commit/d22a809))





## [0.8.7](https://github.com/statsbotco/cubejs-client/compare/v0.8.6...v0.8.7) (2019-05-09)


### Bug Fixes

* **query-orchestrator:** Athena got swamped by fetch schema requests ([d8b5440](https://github.com/statsbotco/cubejs-client/commit/d8b5440))





## [0.8.4](https://github.com/statsbotco/cubejs-client/compare/v0.8.3...v0.8.4) (2019-05-02)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.8.1](https://github.com/statsbotco/cubejs-client/compare/v0.8.0...v0.8.1) (2019-04-30)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





# [0.8.0](https://github.com/statsbotco/cubejs-client/compare/v0.7.10...v0.8.0) (2019-04-29)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.7.6](https://github.com/statsbotco/cubejs-client/compare/v0.7.5...v0.7.6) (2019-04-23)


### Bug Fixes

* **query-orchestrator:** add RedisFactory and promisify methods manually ([#89](https://github.com/statsbotco/cubejs-client/issues/89)) ([cdfcd87](https://github.com/statsbotco/cubejs-client/commit/cdfcd87)), closes [#84](https://github.com/statsbotco/cubejs-client/issues/84)





# [0.7.0](https://github.com/statsbotco/cubejs-client/compare/v0.6.2...v0.7.0) (2019-04-15)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





# [0.6.0](https://github.com/statsbotco/cubejs-client/compare/v0.5.2...v0.6.0) (2019-04-09)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





# [0.5.0](https://github.com/statsbotco/cubejs-client/compare/v0.4.6...v0.5.0) (2019-04-01)


### Features

* use local queue and cache for local dev server instead of Redis one ([50f1bbb](https://github.com/statsbotco/cubejs-client/commit/50f1bbb))





## [0.4.4](https://github.com/statsbotco/cubejs-client/compare/v0.4.3...v0.4.4) (2019-03-17)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator
