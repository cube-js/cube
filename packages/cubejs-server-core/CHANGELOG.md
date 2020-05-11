# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.19.18](https://github.com/cube-js/cube.js/compare/v0.19.17...v0.19.18) (2020-05-11)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.19.17](https://github.com/cube-js/cube.js/compare/v0.19.16...v0.19.17) (2020-05-09)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.19.16](https://github.com/cube-js/cube.js/compare/v0.19.15...v0.19.16) (2020-05-07)


### Features

* Update type defs for query transformer ([#619](https://github.com/cube-js/cube.js/issues/619)) Thanks to [@jcw](https://github.com/jcw)-! ([b396b05](https://github.com/cube-js/cube.js/commit/b396b05))





## [0.19.15](https://github.com/cube-js/cube.js/compare/v0.19.14...v0.19.15) (2020-05-04)


### Features

* Include version in startup message ([#615](https://github.com/cube-js/cube.js/issues/615)) Thanks to jcw-! ([d2f1732](https://github.com/cube-js/cube.js/commit/d2f1732))
* Tweak server type definitions ([#623](https://github.com/cube-js/cube.js/issues/623)) Thanks to [@willhausman](https://github.com/willhausman)! ([23da279](https://github.com/cube-js/cube.js/commit/23da279))





## [0.19.14](https://github.com/cube-js/cube.js/compare/v0.19.13...v0.19.14) (2020-04-24)


### Bug Fixes

* Show Postgres params in logs ([a678ca7](https://github.com/cube-js/cube.js/commit/a678ca7))





## [0.19.13](https://github.com/cube-js/cube.js/compare/v0.19.12...v0.19.13) (2020-04-21)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.19.11](https://github.com/cube-js/cube.js/compare/v0.19.10...v0.19.11) (2020-04-20)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.19.10](https://github.com/cube-js/cube.js/compare/v0.19.9...v0.19.10) (2020-04-18)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.19.9](https://github.com/cube-js/cube.js/compare/v0.19.8...v0.19.9) (2020-04-16)


### Features

* add await when invoking schemaVersion -- support async schemaVersion ([#557](https://github.com/cube-js/cube.js/issues/557)) Thanks to [@barakcoh](https://github.com/barakcoh)! ([964c6d8](https://github.com/cube-js/cube.js/commit/964c6d8))





## [0.19.8](https://github.com/cube-js/cube.js/compare/v0.19.7...v0.19.8) (2020-04-15)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.19.7](https://github.com/cube-js/cube.js/compare/v0.19.6...v0.19.7) (2020-04-14)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.19.6](https://github.com/cube-js/cube.js/compare/v0.19.5...v0.19.6) (2020-04-14)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.19.5](https://github.com/cube-js/cube.js/compare/v0.19.4...v0.19.5) (2020-04-13)


### Bug Fixes

* RefreshScheduler refreshes pre-aggregations during cache key refresh ([51d1214](https://github.com/cube-js/cube.js/commit/51d1214))





## [0.19.4](https://github.com/cube-js/cube.js/compare/v0.19.3...v0.19.4) (2020-04-12)


### Bug Fixes

* **serverless-aws:** cubejsProcess agent doesn't collect all events after process has been finished ([939e25a](https://github.com/cube-js/cube.js/commit/939e25a))





## [0.19.2](https://github.com/cube-js/cube.js/compare/v0.19.1...v0.19.2) (2020-04-12)


### Bug Fixes

* Do not DoS agent with huge payloads ([7886130](https://github.com/cube-js/cube.js/commit/7886130))





## [0.19.1](https://github.com/cube-js/cube.js/compare/v0.19.0...v0.19.1) (2020-04-11)


### Bug Fixes

* TypeError: Cannot read property 'dataSource' of null ([5bef81b](https://github.com/cube-js/cube.js/commit/5bef81b))
* TypeError: Cannot read property 'path' of undefined -- Case when partitioned originalSql is resolved for query without time dimension and incremental refreshKey is used ([ca0f1f6](https://github.com/cube-js/cube.js/commit/ca0f1f6))


### Features

* Provide status messages for ``/cubejs-api/v1/run-scheduled-refresh` API ([fb6623f](https://github.com/cube-js/cube.js/commit/fb6623f))
* Renamed OpenDistro to AWSElasticSearch. Added `elasticsearch` dialect ([#577](https://github.com/cube-js/cube.js/issues/577)) Thanks to [@chad-codeworkshop](https://github.com/chad-codeworkshop)! ([a4e41cb](https://github.com/cube-js/cube.js/commit/a4e41cb))





# [0.19.0](https://github.com/cube-js/cube.js/compare/v0.18.32...v0.19.0) (2020-04-09)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.31](https://github.com/cube-js/cube.js/compare/v0.18.30...v0.18.31) (2020-04-07)


### Bug Fixes

* Pass query options such as timezone ([#570](https://github.com/cube-js/cube.js/issues/570)) Thanks to [@jcw](https://github.com/jcw)-! ([089f307](https://github.com/cube-js/cube.js/commit/089f307))





## [0.18.30](https://github.com/cube-js/cube.js/compare/v0.18.29...v0.18.30) (2020-04-04)


### Features

* Native X-Pack SQL ElasticSearch Driver ([#551](https://github.com/cube-js/cube.js/issues/551)) ([efde731](https://github.com/cube-js/cube.js/commit/efde731))





## [0.18.29](https://github.com/cube-js/cube.js/compare/v0.18.28...v0.18.29) (2020-04-04)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.28](https://github.com/cube-js/cube.js/compare/v0.18.27...v0.18.28) (2020-04-03)


### Bug Fixes

* TypeError: date.match is not a function at BaseTimeDimension.formatFromDate ([7379b84](https://github.com/cube-js/cube.js/commit/7379b84))





## [0.18.27](https://github.com/cube-js/cube.js/compare/v0.18.26...v0.18.27) (2020-04-03)


### Bug Fixes

* TypeError: date.match is not a function at BaseTimeDimension.formatFromDate ([4ac7307](https://github.com/cube-js/cube.js/commit/4ac7307))





## [0.18.26](https://github.com/cube-js/cube.js/compare/v0.18.25...v0.18.26) (2020-04-03)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.25](https://github.com/cube-js/cube.js/compare/v0.18.24...v0.18.25) (2020-04-02)


### Features

* Basic query rewrites ([af07865](https://github.com/cube-js/cube.js/commit/af07865))





## [0.18.24](https://github.com/cube-js/cube.js/compare/v0.18.23...v0.18.24) (2020-04-01)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.23](https://github.com/cube-js/cube.js/compare/v0.18.22...v0.18.23) (2020-03-30)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.19](https://github.com/cube-js/cube.js/compare/v0.18.18...v0.18.19) (2020-03-29)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.18](https://github.com/cube-js/cube.js/compare/v0.18.17...v0.18.18) (2020-03-28)


### Features

* Executing SQL logging message that shows final SQL ([26b8758](https://github.com/cube-js/cube.js/commit/26b8758))





## [0.18.17](https://github.com/cube-js/cube.js/compare/v0.18.16...v0.18.17) (2020-03-24)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.16](https://github.com/cube-js/cube.js/compare/v0.18.15...v0.18.16) (2020-03-24)


### Features

* Log canUseTransformedQuery ([5b2ab90](https://github.com/cube-js/cube.js/commit/5b2ab90))





## [0.18.14](https://github.com/cube-js/cube.js/compare/v0.18.13...v0.18.14) (2020-03-24)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.13](https://github.com/cube-js/cube.js/compare/v0.18.12...v0.18.13) (2020-03-21)


### Bug Fixes

* Various cleanup errors ([538f6d0](https://github.com/cube-js/cube.js/commit/538f6d0)), closes [#525](https://github.com/cube-js/cube.js/issues/525)





## [0.18.12](https://github.com/cube-js/cube.js/compare/v0.18.11...v0.18.12) (2020-03-19)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.11](https://github.com/cube-js/cube.js/compare/v0.18.10...v0.18.11) (2020-03-18)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.7](https://github.com/cube-js/cube.js/compare/v0.18.6...v0.18.7) (2020-03-17)


### Features

* Log `requestId` in compiling schema events ([4c457c9](https://github.com/cube-js/cube.js/commit/4c457c9))





## [0.18.6](https://github.com/cube-js/cube.js/compare/v0.18.5...v0.18.6) (2020-03-16)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.5](https://github.com/cube-js/cube.js/compare/v0.18.4...v0.18.5) (2020-03-15)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.4](https://github.com/cube-js/cube.js/compare/v0.18.3...v0.18.4) (2020-03-09)


### Features

* Add API gateway request logging support ([#475](https://github.com/cube-js/cube.js/issues/475)) ([465471e](https://github.com/cube-js/cube.js/commit/465471e))





## [0.18.3](https://github.com/cube-js/cube.js/compare/v0.18.2...v0.18.3) (2020-03-02)


### Bug Fixes

* antd 4 support for dashboard ([84bb164](https://github.com/cube-js/cube.js/commit/84bb164)), closes [#463](https://github.com/cube-js/cube.js/issues/463)
* Duration string is not printed for all messages -- Load Request SQL case ([e0d3aff](https://github.com/cube-js/cube.js/commit/e0d3aff))





## [0.18.2](https://github.com/cube-js/cube.js/compare/v0.18.1...v0.18.2) (2020-03-01)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.18.1](https://github.com/cube-js/cube.js/compare/v0.18.0...v0.18.1) (2020-03-01)

**Note:** Version bump only for package @cubejs-backend/server-core





# [0.18.0](https://github.com/cube-js/cube.js/compare/v0.17.10...v0.18.0) (2020-03-01)


### Bug Fixes

* Handle missing body-parser error ([b90dd89](https://github.com/cube-js/cube.js/commit/b90dd89))


### Features

* COMPILE_CONTEXT and async driverFactory support ([160f931](https://github.com/cube-js/cube.js/commit/160f931))





## [0.17.10](https://github.com/cube-js/cube.js/compare/v0.17.9...v0.17.10) (2020-02-20)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.17.9](https://github.com/cube-js/cube.js/compare/v0.17.8...v0.17.9) (2020-02-18)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.17.8](https://github.com/cube-js/cube.js/compare/v0.17.7...v0.17.8) (2020-02-14)


### Bug Fixes

* typings export ([#373](https://github.com/cube-js/cube.js/issues/373)) Thanks to [@lvauvillier](https://github.com/lvauvillier)! ([f4ea839](https://github.com/cube-js/cube.js/commit/f4ea839))





## [0.17.7](https://github.com/cube-js/cube.js/compare/v0.17.6...v0.17.7) (2020-02-12)


### Bug Fixes

* Wrong typings ([c32fb0e](https://github.com/cube-js/cube.js/commit/c32fb0e))


### Features

* Add more Typescript typings. Thanks to [@lvauvillier](https://github.com/lvauvillier)! ([fdd1141](https://github.com/cube-js/cube.js/commit/fdd1141))
* Cube.js agent ([35366aa](https://github.com/cube-js/cube.js/commit/35366aa))
* improve server-core typings ([9d59300](https://github.com/cube-js/cube.js/commit/9d59300))
* Set warn to be default log level for production logging ([c4298ea](https://github.com/cube-js/cube.js/commit/c4298ea))





## [0.17.6](https://github.com/cube-js/cube.js/compare/v0.17.5...v0.17.6) (2020-02-10)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.17.5](https://github.com/cube-js/cube.js/compare/v0.17.4...v0.17.5) (2020-02-07)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.17.4](https://github.com/cube-js/cube.js/compare/v0.17.3...v0.17.4) (2020-02-06)


### Bug Fixes

* Don't fetch schema twice when generating in Playground. Big schemas take a lot of time to fetch. ([3eeb73a](https://github.com/cube-js/cube.js/commit/3eeb73a))





## [0.17.3](https://github.com/cube-js/cube.js/compare/v0.17.2...v0.17.3) (2020-02-06)


### Bug Fixes

* Fix typescript type definition ([66e2fe5](https://github.com/cube-js/cube.js/commit/66e2fe5))





## [0.17.2](https://github.com/cube-js/cube.js/compare/v0.17.1...v0.17.2) (2020-02-04)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.17.1](https://github.com/cube-js/cube.js/compare/v0.17.0...v0.17.1) (2020-02-04)

**Note:** Version bump only for package @cubejs-backend/server-core





# [0.17.0](https://github.com/cube-js/cube.js/compare/v0.16.0...v0.17.0) (2020-02-04)

**Note:** Version bump only for package @cubejs-backend/server-core





# [0.16.0](https://github.com/cube-js/cube.js/compare/v0.15.4...v0.16.0) (2020-02-04)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.15.3](https://github.com/cube-js/cube.js/compare/v0.15.2...v0.15.3) (2020-01-26)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.15.1](https://github.com/cube-js/cube.js/compare/v0.15.0...v0.15.1) (2020-01-21)

**Note:** Version bump only for package @cubejs-backend/server-core





# [0.15.0](https://github.com/cube-js/cube.js/compare/v0.14.3...v0.15.0) (2020-01-18)


### Features

* Slow Query Warning and scheduled refresh for cube refresh keys ([8768b0e](https://github.com/cube-js/cube.js/commit/8768b0e))





## [0.14.3](https://github.com/cube-js/cube.js/compare/v0.14.2...v0.14.3) (2020-01-17)


### Features

* Skip contents for huge queries in dev logs ([c873a83](https://github.com/cube-js/cube.js/commit/c873a83))





## [0.14.2](https://github.com/cube-js/cube.js/compare/v0.14.1...v0.14.2) (2020-01-17)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.14.1](https://github.com/cube-js/cube.js/compare/v0.14.0...v0.14.1) (2020-01-17)

**Note:** Version bump only for package @cubejs-backend/server-core





# [0.14.0](https://github.com/cube-js/cube.js/compare/v0.13.12...v0.14.0) (2020-01-16)


### Features

* Scheduled refresh for pre-aggregations ([c87b525](https://github.com/cube-js/cube.js/commit/c87b525))
* Scheduled Refresh REST API ([472a0c3](https://github.com/cube-js/cube.js/commit/472a0c3))





## [0.13.12](https://github.com/cube-js/cube.js/compare/v0.13.11...v0.13.12) (2020-01-12)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.13.11](https://github.com/cube-js/cube.js/compare/v0.13.10...v0.13.11) (2020-01-03)


### Bug Fixes

* Can't parse /node_modules/.bin/sha.js during dashboard creation ([e13ad50](https://github.com/cube-js/cube.js/commit/e13ad50))





## [0.13.10](https://github.com/cube-js/cube.js/compare/v0.13.9...v0.13.10) (2020-01-03)


### Bug Fixes

* More details for parsing errors during dashboard creation ([a8cb9d3](https://github.com/cube-js/cube.js/commit/a8cb9d3))





## [0.13.9](https://github.com/cube-js/cube.js/compare/v0.13.8...v0.13.9) (2020-01-03)


### Features

* Improve logging ([8a692c1](https://github.com/cube-js/cube.js/commit/8a692c1))





## [0.13.8](https://github.com/cube-js/cube.js/compare/v0.13.7...v0.13.8) (2019-12-31)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.13.7](https://github.com/cube-js/cube.js/compare/v0.13.6...v0.13.7) (2019-12-31)


### Bug Fixes

* schemaVersion called with old context ([#293](https://github.com/cube-js/cube.js/issues/293)) ([da10e39](https://github.com/cube-js/cube.js/commit/da10e39)), closes [#294](https://github.com/cube-js/cube.js/issues/294)


### Features

* Extendable context ([#299](https://github.com/cube-js/cube.js/issues/299)) ([38e33ce](https://github.com/cube-js/cube.js/commit/38e33ce)), closes [#295](https://github.com/cube-js/cube.js/issues/295) [#296](https://github.com/cube-js/cube.js/issues/296)
* Health check methods ([#308](https://github.com/cube-js/cube.js/issues/308)) Thanks to [@willhausman](https://github.com/willhausman)! ([49ca36b](https://github.com/cube-js/cube.js/commit/49ca36b))





## [0.13.6](https://github.com/cube-js/cube.js/compare/v0.13.5...v0.13.6) (2019-12-19)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.13.5](https://github.com/cube-js/cube.js/compare/v0.13.4...v0.13.5) (2019-12-17)


### Features

* Elasticsearch driver preview ([d6a6a07](https://github.com/cube-js/cube.js/commit/d6a6a07))





## [0.13.3](https://github.com/cube-js/cube.js/compare/v0.13.2...v0.13.3) (2019-12-16)


### Bug Fixes

* Logging failing when pre-aggregations are built ([22f77a6](https://github.com/cube-js/cube.js/commit/22f77a6))


### Features

* d3-charts template package ([f9bd3fb](https://github.com/cube-js/cube.js/commit/f9bd3fb))





## [0.13.2](https://github.com/cube-js/cube.js/compare/v0.13.1...v0.13.2) (2019-12-13)


### Features

* hooks for dynamic schemas ([#287](https://github.com/cube-js/cube.js/issues/287)). Thanks to [@willhausman](https://github.com/willhausman)! ([47b256d](https://github.com/cube-js/cube.js/commit/47b256d))
* Propagate `requestId` for trace logging ([24d7b41](https://github.com/cube-js/cube.js/commit/24d7b41))





## [0.13.1](https://github.com/cube-js/cube.js/compare/v0.13.0...v0.13.1) (2019-12-10)

**Note:** Version bump only for package @cubejs-backend/server-core





# [0.13.0](https://github.com/cube-js/cube.js/compare/v0.12.3...v0.13.0) (2019-12-10)


### Features

* Sqlite driver implementation ([f9b43d3](https://github.com/cube-js/cube.js/commit/f9b43d3))





## [0.12.2](https://github.com/cube-js/cube.js/compare/v0.12.1...v0.12.2) (2019-12-02)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.12.1](https://github.com/cube-js/cube.js/compare/v0.12.0...v0.12.1) (2019-11-26)


### Features

* Show used pre-aggregations and match rollup results in Playground ([4a67346](https://github.com/cube-js/cube.js/commit/4a67346))





# [0.12.0](https://github.com/cube-js/cube.js/compare/v0.11.25...v0.12.0) (2019-11-25)


### Features

* Show `refreshKey` values in Playground ([b49e184](https://github.com/cube-js/cube.js/commit/b49e184))





## [0.11.25](https://github.com/cube-js/cube.js/compare/v0.11.24...v0.11.25) (2019-11-23)


### Bug Fixes

* **playground:** Multiple conflicting packages applied at the same time: check for creation state before applying ([35f6325](https://github.com/cube-js/cube.js/commit/35f6325))





## [0.11.24](https://github.com/cube-js/cube.js/compare/v0.11.23...v0.11.24) (2019-11-20)


### Bug Fixes

* Material UI template doesn't work ([deccca1](https://github.com/cube-js/cube.js/commit/deccca1))





## [0.11.22](https://github.com/cube-js/cube.js/compare/v0.11.21...v0.11.22) (2019-11-20)


### Bug Fixes

* Error: Router element is not found: Template Gallery source enumeration returns empty array ([459a4a7](https://github.com/cube-js/cube.js/commit/459a4a7))





## [0.11.21](https://github.com/cube-js/cube.js/compare/v0.11.20...v0.11.21) (2019-11-20)


### Features

* Template gallery ([#272](https://github.com/cube-js/cube.js/issues/272)) ([f5ac516](https://github.com/cube-js/cube.js/commit/f5ac516))





## [0.11.20](https://github.com/cube-js/cube.js/compare/v0.11.19...v0.11.20) (2019-11-18)


### Features

* per cube `dataSource` support ([6dc3fef](https://github.com/cube-js/cube.js/commit/6dc3fef))





## [0.11.19](https://github.com/cube-js/cube.js/compare/v0.11.18...v0.11.19) (2019-11-16)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.11.18](https://github.com/cube-js/cube.js/compare/v0.11.17...v0.11.18) (2019-11-09)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.11.17](https://github.com/statsbotco/cubejs-client/compare/v0.11.16...v0.11.17) (2019-11-08)


### Bug Fixes

* **server-core:** the schemaPath option does not work when generating schema ([#255](https://github.com/statsbotco/cubejs-client/issues/255)) ([92f17b2](https://github.com/statsbotco/cubejs-client/commit/92f17b2))


### Features

* Default root path message for servers running in production ([5b7ef41](https://github.com/statsbotco/cubejs-client/commit/5b7ef41))





## [0.11.16](https://github.com/statsbotco/cubejs-client/compare/v0.11.15...v0.11.16) (2019-11-04)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.11.15](https://github.com/statsbotco/cubejs-client/compare/v0.11.14...v0.11.15) (2019-11-01)


### Bug Fixes

* Reduce output for logging ([aaf55e0](https://github.com/statsbotco/cubejs-client/commit/aaf55e0))





## [0.11.14](https://github.com/statsbotco/cubejs-client/compare/v0.11.13...v0.11.14) (2019-11-01)


### Features

* pretty default logger and log levels ([#244](https://github.com/statsbotco/cubejs-client/issues/244)) ([b1302d2](https://github.com/statsbotco/cubejs-client/commit/b1302d2))





## [0.11.13](https://github.com/statsbotco/cubejs-client/compare/v0.11.12...v0.11.13) (2019-10-30)


### Features

* **playground:** Static dashboard template ([2458aad](https://github.com/statsbotco/cubejs-client/commit/2458aad))





## [0.11.11](https://github.com/statsbotco/cubejs-client/compare/v0.11.10...v0.11.11) (2019-10-26)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.11.8](https://github.com/statsbotco/cubejs-client/compare/v0.11.7...v0.11.8) (2019-10-22)


### Bug Fixes

* Pass `checkAuth` option to API Gateway ([d3d690e](https://github.com/statsbotco/cubejs-client/commit/d3d690e))





## [0.11.7](https://github.com/statsbotco/cubejs-client/compare/v0.11.6...v0.11.7) (2019-10-22)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.11.6](https://github.com/statsbotco/cubejs-client/compare/v0.11.5...v0.11.6) (2019-10-17)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.11.5](https://github.com/statsbotco/cubejs-client/compare/v0.11.4...v0.11.5) (2019-10-17)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.11.2](https://github.com/statsbotco/cubejs-client/compare/v0.11.1...v0.11.2) (2019-10-15)


### Bug Fixes

* Error: ENOENT: no such file or directory, open 'Orders.js' ([74a8875](https://github.com/statsbotco/cubejs-client/commit/74a8875))





# [0.11.0](https://github.com/statsbotco/cubejs-client/compare/v0.10.62...v0.11.0) (2019-10-15)


### Bug Fixes

* TypeError: Cannot destructure property authInfo of 'undefined' or 'null'. ([1886d13](https://github.com/statsbotco/cubejs-client/commit/1886d13))


### Features

* Read schema subfolders ([#230](https://github.com/statsbotco/cubejs-client/issues/230)). Thanks to [@lksilva](https://github.com/lksilva)! ([aa736b1](https://github.com/statsbotco/cubejs-client/commit/aa736b1))
* Sockets Preview ([#231](https://github.com/statsbotco/cubejs-client/issues/231)) ([89fc762](https://github.com/statsbotco/cubejs-client/commit/89fc762)), closes [#221](https://github.com/statsbotco/cubejs-client/issues/221)





## [0.10.62](https://github.com/statsbotco/cubejs-client/compare/v0.10.61...v0.10.62) (2019-10-11)


### Features

* `ungrouped` queries support ([c6ac873](https://github.com/statsbotco/cubejs-client/commit/c6ac873))





## [0.10.61](https://github.com/statsbotco/cubejs-client/compare/v0.10.60...v0.10.61) (2019-10-10)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.59](https://github.com/statsbotco/cubejs-client/compare/v0.10.58...v0.10.59) (2019-10-08)


### Bug Fixes

* hostname: command not found ([8ca1f21](https://github.com/statsbotco/cubejs-client/commit/8ca1f21))





## [0.10.58](https://github.com/statsbotco/cubejs-client/compare/v0.10.57...v0.10.58) (2019-10-04)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.57](https://github.com/statsbotco/cubejs-client/compare/v0.10.56...v0.10.57) (2019-10-04)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.55](https://github.com/statsbotco/cubejs-client/compare/v0.10.54...v0.10.55) (2019-10-03)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.54](https://github.com/statsbotco/cubejs-client/compare/v0.10.53...v0.10.54) (2019-10-02)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.46](https://github.com/statsbotco/cubejs-client/compare/v0.10.45...v0.10.46) (2019-09-30)


### Features

* Restructure Dashboard scaffolding to make it more user friendly and reliable ([78ba3bc](https://github.com/statsbotco/cubejs-client/commit/78ba3bc))





## [0.10.44](https://github.com/statsbotco/cubejs-client/compare/v0.10.43...v0.10.44) (2019-09-27)


### Bug Fixes

* npm installs old dependencies on dashboard creation ([a7d519c](https://github.com/statsbotco/cubejs-client/commit/a7d519c))
* **playground:** use default 3000 port for dashboard app as it's more appropriate ([ec4f3f4](https://github.com/statsbotco/cubejs-client/commit/ec4f3f4))





## [0.10.43](https://github.com/statsbotco/cubejs-client/compare/v0.10.42...v0.10.43) (2019-09-27)


### Features

* Dynamic dashboards ([#218](https://github.com/statsbotco/cubejs-client/issues/218)) ([2c6cdc9](https://github.com/statsbotco/cubejs-client/commit/2c6cdc9))





## [0.10.41](https://github.com/statsbotco/cubejs-client/compare/v0.10.40...v0.10.41) (2019-09-13)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.39](https://github.com/statsbotco/cubejs-client/compare/v0.10.38...v0.10.39) (2019-09-09)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.38](https://github.com/statsbotco/cubejs-client/compare/v0.10.37...v0.10.38) (2019-09-09)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.36](https://github.com/statsbotco/cubejs-client/compare/v0.10.35...v0.10.36) (2019-09-09)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.35](https://github.com/statsbotco/cubejs-client/compare/v0.10.34...v0.10.35) (2019-09-09)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.34](https://github.com/statsbotco/cubejs-client/compare/v0.10.33...v0.10.34) (2019-09-06)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.33](https://github.com/statsbotco/cubejs-client/compare/v0.10.32...v0.10.33) (2019-09-06)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.32](https://github.com/statsbotco/cubejs-client/compare/v0.10.31...v0.10.32) (2019-09-06)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.30](https://github.com/statsbotco/cubejs-client/compare/v0.10.29...v0.10.30) (2019-08-26)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.29](https://github.com/statsbotco/cubejs-client/compare/v0.10.28...v0.10.29) (2019-08-21)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.28](https://github.com/statsbotco/cubejs-client/compare/v0.10.27...v0.10.28) (2019-08-19)


### Bug Fixes

* Show dev server errors in console ([e8c3af9](https://github.com/statsbotco/cubejs-client/commit/e8c3af9))





## [0.10.27](https://github.com/statsbotco/cubejs-client/compare/v0.10.26...v0.10.27) (2019-08-18)


### Features

* Make `preAggregationsSchema` an option of CubejsServerCore - missed option propagation ([60d5704](https://github.com/statsbotco/cubejs-client/commit/60d5704)), closes [#96](https://github.com/statsbotco/cubejs-client/issues/96)





## [0.10.26](https://github.com/statsbotco/cubejs-client/compare/v0.10.25...v0.10.26) (2019-08-18)


### Features

* Make `preAggregationsSchema` an option of CubejsServerCore ([3b1b082](https://github.com/statsbotco/cubejs-client/commit/3b1b082)), closes [#96](https://github.com/statsbotco/cubejs-client/issues/96)





## [0.10.24](https://github.com/statsbotco/cubejs-client/compare/v0.10.23...v0.10.24) (2019-08-16)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.23](https://github.com/statsbotco/cubejs-client/compare/v0.10.22...v0.10.23) (2019-08-14)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.21](https://github.com/statsbotco/cubejs-client/compare/v0.10.20...v0.10.21) (2019-08-05)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.20](https://github.com/statsbotco/cubejs-client/compare/v0.10.19...v0.10.20) (2019-08-03)


### Features

* **playground:** Various dashboard hints ([eed2b55](https://github.com/statsbotco/cubejs-client/commit/eed2b55))





## [0.10.18](https://github.com/statsbotco/cubejs-client/compare/v0.10.17...v0.10.18) (2019-07-31)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.17](https://github.com/statsbotco/cubejs-client/compare/v0.10.16...v0.10.17) (2019-07-31)


### Features

* **playground:** Show editable files hint ([2dffe6c](https://github.com/statsbotco/cubejs-client/commit/2dffe6c))





## [0.10.16](https://github.com/statsbotco/cubejs-client/compare/v0.10.15...v0.10.16) (2019-07-20)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.15](https://github.com/statsbotco/cubejs-client/compare/v0.10.14...v0.10.15) (2019-07-13)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.14](https://github.com/statsbotco/cubejs-client/compare/v0.10.13...v0.10.14) (2019-07-13)


### Features

* Oracle driver ([#160](https://github.com/statsbotco/cubejs-client/issues/160)) ([854ebff](https://github.com/statsbotco/cubejs-client/commit/854ebff))





## [0.10.12](https://github.com/statsbotco/cubejs-client/compare/v0.10.11...v0.10.12) (2019-07-06)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.11](https://github.com/statsbotco/cubejs-client/compare/v0.10.10...v0.10.11) (2019-07-02)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.9](https://github.com/statsbotco/cubejs-client/compare/v0.10.8...v0.10.9) (2019-06-30)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.10.8](https://github.com/statsbotco/cubejs-client/compare/v0.10.7...v0.10.8) (2019-06-28)


### Features

* More readable compiling schema log message ([246805b](https://github.com/statsbotco/cubejs-client/commit/246805b))
* Presto driver ([1994083](https://github.com/statsbotco/cubejs-client/commit/1994083))





## [0.10.7](https://github.com/statsbotco/cubejs-client/compare/v0.10.6...v0.10.7) (2019-06-27)


### Bug Fixes

* Module not found: Can't resolve 'react' ([a00e588](https://github.com/statsbotco/cubejs-client/commit/a00e588))





## [0.10.5](https://github.com/statsbotco/cubejs-client/compare/v0.10.4...v0.10.5) (2019-06-26)


### Bug Fixes

* Update version to fix audit warnings ([f8f5225](https://github.com/statsbotco/cubejs-client/commit/f8f5225))





## [0.10.4](https://github.com/statsbotco/cubejs-client/compare/v0.10.3...v0.10.4) (2019-06-26)


### Bug Fixes

* Gray screen for Playground on version update ([b08333f](https://github.com/statsbotco/cubejs-client/commit/b08333f))





## [0.10.1](https://github.com/statsbotco/cubejs-client/compare/v0.10.0...v0.10.1) (2019-06-26)


### Features

* Snowflake driver ([35861b5](https://github.com/statsbotco/cubejs-client/commit/35861b5)), closes [#142](https://github.com/statsbotco/cubejs-client/issues/142)





# [0.10.0](https://github.com/statsbotco/cubejs-client/compare/v0.9.24...v0.10.0) (2019-06-21)


### Features

* **api-gateway:** `queryTransformer` security hook ([a9c41b2](https://github.com/statsbotco/cubejs-client/commit/a9c41b2))
* **schema-compiler:** `asyncModules` and Node.js `require()`: support loading cube definitions from DB and other async sources ([397cceb](https://github.com/statsbotco/cubejs-client/commit/397cceb)), closes [#141](https://github.com/statsbotco/cubejs-client/issues/141)





## [0.9.24](https://github.com/statsbotco/cubejs-client/compare/v0.9.23...v0.9.24) (2019-06-17)


### Bug Fixes

* Fix dev server in production mode message ([7586ad5](https://github.com/statsbotco/cubejs-client/commit/7586ad5))





## [0.9.23](https://github.com/statsbotco/cubejs-client/compare/v0.9.22...v0.9.23) (2019-06-17)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.9.21](https://github.com/statsbotco/cubejs-client/compare/v0.9.20...v0.9.21) (2019-06-16)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.9.20](https://github.com/statsbotco/cubejs-client/compare/v0.9.19...v0.9.20) (2019-06-16)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.9.19](https://github.com/statsbotco/cubejs-client/compare/v0.9.18...v0.9.19) (2019-06-13)


### Features

* Add Typescript typings for server-core ([#111](https://github.com/statsbotco/cubejs-client/issues/111)) ([b1b895e](https://github.com/statsbotco/cubejs-client/commit/b1b895e))





## [0.9.17](https://github.com/statsbotco/cubejs-client/compare/v0.9.16...v0.9.17) (2019-06-11)


### Bug Fixes

* **cli:** jdbc-driver fail hides db type not supported errors ([6f7c675](https://github.com/statsbotco/cubejs-client/commit/6f7c675))





## [0.9.16](https://github.com/statsbotco/cubejs-client/compare/v0.9.15...v0.9.16) (2019-06-10)


### Bug Fixes

* **playground:** Do not cache index.html to prevent missing resource errors on version upgrades ([4f20955](https://github.com/statsbotco/cubejs-client/commit/4f20955)), closes [#116](https://github.com/statsbotco/cubejs-client/issues/116)





## [0.9.15](https://github.com/statsbotco/cubejs-client/compare/v0.9.14...v0.9.15) (2019-06-07)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.9.14](https://github.com/statsbotco/cubejs-client/compare/v0.9.13...v0.9.14) (2019-06-07)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.9.13](https://github.com/statsbotco/cubejs-client/compare/v0.9.12...v0.9.13) (2019-06-06)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.9.12](https://github.com/statsbotco/cubejs-client/compare/v0.9.11...v0.9.12) (2019-06-03)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.9.11](https://github.com/statsbotco/cubejs-client/compare/v0.9.10...v0.9.11) (2019-05-31)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.9.6](https://github.com/statsbotco/cubejs-client/compare/v0.9.5...v0.9.6) (2019-05-24)


### Features

* better npm fail message in Playground ([545a020](https://github.com/statsbotco/cubejs-client/commit/545a020))





## [0.9.5](https://github.com/statsbotco/cubejs-client/compare/v0.9.4...v0.9.5) (2019-05-22)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.9.4](https://github.com/statsbotco/cubejs-client/compare/v0.9.3...v0.9.4) (2019-05-22)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.9.3](https://github.com/statsbotco/cubejs-client/compare/v0.9.2...v0.9.3) (2019-05-21)


### Bug Fixes

* **playground:** revert back create-react-app to npx as there're much more problems with global npm ([e434939](https://github.com/statsbotco/cubejs-client/commit/e434939))





## [0.9.2](https://github.com/statsbotco/cubejs-client/compare/v0.9.1...v0.9.2) (2019-05-11)

**Note:** Version bump only for package @cubejs-backend/server-core





# [0.9.0](https://github.com/statsbotco/cubejs-client/compare/v0.8.7...v0.9.0) (2019-05-11)


### Features

* External rollup implementation ([d22a809](https://github.com/statsbotco/cubejs-client/commit/d22a809))





## [0.8.7](https://github.com/statsbotco/cubejs-client/compare/v0.8.6...v0.8.7) (2019-05-09)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.8.6](https://github.com/statsbotco/cubejs-client/compare/v0.8.5...v0.8.6) (2019-05-05)


### Features

* Replace codesandbox by running dashboard react-app directly ([861c817](https://github.com/statsbotco/cubejs-client/commit/861c817))





## [0.8.4](https://github.com/statsbotco/cubejs-client/compare/v0.8.3...v0.8.4) (2019-05-02)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.8.3](https://github.com/statsbotco/cubejs-client/compare/v0.8.2...v0.8.3) (2019-05-01)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.8.1](https://github.com/statsbotco/cubejs-client/compare/v0.8.0...v0.8.1) (2019-04-30)


### Features

* Driver for ClickHouse database support ([#94](https://github.com/statsbotco/cubejs-client/issues/94)) ([0f05321](https://github.com/statsbotco/cubejs-client/commit/0f05321)), closes [#1](https://github.com/statsbotco/cubejs-client/issues/1)





# [0.8.0](https://github.com/statsbotco/cubejs-client/compare/v0.7.10...v0.8.0) (2019-04-29)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.7.9](https://github.com/statsbotco/cubejs-client/compare/v0.7.8...v0.7.9) (2019-04-24)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.7.8](https://github.com/statsbotco/cubejs-client/compare/v0.7.7...v0.7.8) (2019-04-24)


### Bug Fixes

* **playground:** Dashboard doesn't work on Windows ([48a2ec4](https://github.com/statsbotco/cubejs-client/commit/48a2ec4)), closes [#82](https://github.com/statsbotco/cubejs-client/issues/82)





## [0.7.7](https://github.com/statsbotco/cubejs-client/compare/v0.7.6...v0.7.7) (2019-04-24)


### Bug Fixes

* **playground:** Dashboard doesn't work on Windows ([7c48aa4](https://github.com/statsbotco/cubejs-client/commit/7c48aa4)), closes [#82](https://github.com/statsbotco/cubejs-client/issues/82)





## [0.7.6](https://github.com/statsbotco/cubejs-client/compare/v0.7.5...v0.7.6) (2019-04-23)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.7.5](https://github.com/statsbotco/cubejs-client/compare/v0.7.4...v0.7.5) (2019-04-18)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.7.4](https://github.com/statsbotco/cubejs-client/compare/v0.7.3...v0.7.4) (2019-04-17)


### Bug Fixes

* Make dashboard app creation explicit. Show error messages if dashboard failed to create. ([3b2a22b](https://github.com/statsbotco/cubejs-client/commit/3b2a22b))





## [0.7.2](https://github.com/statsbotco/cubejs-client/compare/v0.7.1...v0.7.2) (2019-04-15)


### Bug Fixes

* Avoid 502 for Playground in serverless: minimize babel ([f9d3171](https://github.com/statsbotco/cubejs-client/commit/f9d3171))


### Features

* MS SQL database driver ([48fbe66](https://github.com/statsbotco/cubejs-client/commit/48fbe66)), closes [#76](https://github.com/statsbotco/cubejs-client/issues/76)





## [0.7.1](https://github.com/statsbotco/cubejs-client/compare/v0.7.0...v0.7.1) (2019-04-15)

**Note:** Version bump only for package @cubejs-backend/server-core





# [0.7.0](https://github.com/statsbotco/cubejs-client/compare/v0.6.2...v0.7.0) (2019-04-15)


### Features

* App multi-tenancy support in single ServerCore instance ([6f0220f](https://github.com/statsbotco/cubejs-client/commit/6f0220f))





## [0.6.2](https://github.com/statsbotco/cubejs-client/compare/v0.6.1...v0.6.2) (2019-04-12)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.6.1](https://github.com/statsbotco/cubejs-client/compare/v0.6.0...v0.6.1) (2019-04-11)


### Bug Fixes

* Get Playground API_URL from window.location until provided explicitly in env. Remote server playground case. ([7b1a0ff](https://github.com/statsbotco/cubejs-client/commit/7b1a0ff))


### Features

* Disable authentication checks in developer mode ([bc09eba](https://github.com/statsbotco/cubejs-client/commit/bc09eba))
* Formatted error logging in developer mode ([3376a50](https://github.com/statsbotco/cubejs-client/commit/3376a50))





# [0.6.0](https://github.com/statsbotco/cubejs-client/compare/v0.5.2...v0.6.0) (2019-04-09)


### Bug Fixes

* **playground:** no such file or directory, scandir 'dashboard-app/src' ([64ec481](https://github.com/statsbotco/cubejs-client/commit/64ec481))





## [0.5.2](https://github.com/statsbotco/cubejs-client/compare/v0.5.1...v0.5.2) (2019-04-05)


### Features

* Add redshift to postgres driver link ([#71](https://github.com/statsbotco/cubejs-client/issues/71)) ([4797588](https://github.com/statsbotco/cubejs-client/commit/4797588))
* Playground UX improvements ([6760a1d](https://github.com/statsbotco/cubejs-client/commit/6760a1d))





## [0.5.1](https://github.com/statsbotco/cubejs-client/compare/v0.5.0...v0.5.1) (2019-04-02)


### Features

* BigQuery driver ([654edac](https://github.com/statsbotco/cubejs-client/commit/654edac))





# [0.5.0](https://github.com/statsbotco/cubejs-client/compare/v0.4.6...v0.5.0) (2019-04-01)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.4.6](https://github.com/statsbotco/cubejs-client/compare/v0.4.5...v0.4.6) (2019-03-27)


### Features

* Dashboard Generator for Playground ([28a42ee](https://github.com/statsbotco/cubejs-client/commit/28a42ee))





## [0.4.5](https://github.com/statsbotco/cubejs-client/compare/v0.4.4...v0.4.5) (2019-03-21)


### Features

* Make API path namespace configurable ([#53](https://github.com/statsbotco/cubejs-client/issues/53)) ([b074a3d](https://github.com/statsbotco/cubejs-client/commit/b074a3d))





## [0.4.4](https://github.com/statsbotco/cubejs-client/compare/v0.4.3...v0.4.4) (2019-03-17)


### Features

* Introduce Schema generation UI in Playground ([349c7d0](https://github.com/statsbotco/cubejs-client/commit/349c7d0))





## [0.4.3](https://github.com/statsbotco/cubejs-client/compare/v0.4.2...v0.4.3) (2019-03-15)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.4.1](https://github.com/statsbotco/cubejs-client/compare/v0.4.0...v0.4.1) (2019-03-14)


### Features

* Allow to use custom checkAuth middleware ([19d5cd8](https://github.com/statsbotco/cubejs-client/commit/19d5cd8)), closes [#42](https://github.com/statsbotco/cubejs-client/issues/42)





# [0.4.0](https://github.com/statsbotco/cubejs-client/compare/v0.3.5-alpha.0...v0.4.0) (2019-03-13)

**Note:** Version bump only for package @cubejs-backend/server-core





## [0.3.5-alpha.0](https://github.com/statsbotco/cubejs-client/compare/v0.3.5...v0.3.5-alpha.0) (2019-03-12)

**Note:** Version bump only for package @cubejs-backend/server-core
