# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.19.19](https://github.com/cube-js/cube.js/compare/v0.19.18...v0.19.19) (2020-05-15)


### Features

* ability to add custom meta data for measures, dimensions and segments ([#641](https://github.com/cube-js/cube.js/issues/641)) ([88d5c9b](https://github.com/cube-js/cube.js/commit/88d5c9b)), closes [#625](https://github.com/cube-js/cube.js/issues/625)





## [0.19.18](https://github.com/cube-js/cube.js/compare/v0.19.17...v0.19.18) (2020-05-11)


### Bug Fixes

* Offset doesn't affect actual queries ([1feaa38](https://github.com/cube-js/cube.js/commit/1feaa38)), closes [#636](https://github.com/cube-js/cube.js/issues/636)





## [0.19.14](https://github.com/cube-js/cube.js/compare/v0.19.13...v0.19.14) (2020-04-24)


### Features

* Postgres HLL improvements: always round to int ([#611](https://github.com/cube-js/cube.js/issues/611)) Thanks to [@milanbella](https://github.com/milanbella)! ([680a613](https://github.com/cube-js/cube.js/commit/680a613))





## [0.19.13](https://github.com/cube-js/cube.js/compare/v0.19.12...v0.19.13) (2020-04-21)


### Features

* Postgres Citus Data HLL plugin implementation ([#601](https://github.com/cube-js/cube.js/issues/601)) Thanks to [@milanbella](https://github.com/milanbella) ! ([be85ac6](https://github.com/cube-js/cube.js/commit/be85ac6)), closes [#563](https://github.com/cube-js/cube.js/issues/563)





## [0.19.11](https://github.com/cube-js/cube.js/compare/v0.19.10...v0.19.11) (2020-04-20)


### Bug Fixes

* Strict date range and rollup granularity alignment check ([deb62b6](https://github.com/cube-js/cube.js/commit/deb62b6)), closes [#103](https://github.com/cube-js/cube.js/issues/103)





## [0.19.10](https://github.com/cube-js/cube.js/compare/v0.19.9...v0.19.10) (2020-04-18)


### Bug Fixes

* Recursive pre-aggregation description generation: support propagateFiltersToSubQuery with partitioned originalSql ([6a2b9dd](https://github.com/cube-js/cube.js/commit/6a2b9dd))





## [0.19.1](https://github.com/cube-js/cube.js/compare/v0.19.0...v0.19.1) (2020-04-11)


### Bug Fixes

* TypeError: Cannot read property 'path' of undefined -- Case when partitioned originalSql is resolved for query without time dimension and incremental refreshKey is used ([ca0f1f6](https://github.com/cube-js/cube.js/commit/ca0f1f6))


### Features

* Renamed OpenDistro to AWSElasticSearch. Added `elasticsearch` dialect ([#577](https://github.com/cube-js/cube.js/issues/577)) Thanks to [@chad-codeworkshop](https://github.com/chad-codeworkshop)! ([a4e41cb](https://github.com/cube-js/cube.js/commit/a4e41cb))





# [0.19.0](https://github.com/cube-js/cube.js/compare/v0.18.32...v0.19.0) (2020-04-09)


### Features

* Multi-level query structures in-memory caching ([38aa32d](https://github.com/cube-js/cube.js/commit/38aa32d))





## [0.18.31](https://github.com/cube-js/cube.js/compare/v0.18.30...v0.18.31) (2020-04-07)


### Bug Fixes

* Rewrite converts left outer to inner join due to filtering in where: ensure `OR` is supported ([93a1250](https://github.com/cube-js/cube.js/commit/93a1250))





## [0.18.30](https://github.com/cube-js/cube.js/compare/v0.18.29...v0.18.30) (2020-04-04)


### Bug Fixes

* Rewrite converts left outer to inner join due to filtering in where ([2034d37](https://github.com/cube-js/cube.js/commit/2034d37))


### Features

* Native X-Pack SQL ElasticSearch Driver ([#551](https://github.com/cube-js/cube.js/issues/551)) ([efde731](https://github.com/cube-js/cube.js/commit/efde731))





## [0.18.29](https://github.com/cube-js/cube.js/compare/v0.18.28...v0.18.29) (2020-04-04)


### Features

* Hour partition granularity support ([5f78974](https://github.com/cube-js/cube.js/commit/5f78974))
* Rewrite SQL for more places ([2412821](https://github.com/cube-js/cube.js/commit/2412821))





## [0.18.28](https://github.com/cube-js/cube.js/compare/v0.18.27...v0.18.28) (2020-04-03)


### Bug Fixes

* TypeError: date.match is not a function at BaseTimeDimension.formatFromDate ([7379b84](https://github.com/cube-js/cube.js/commit/7379b84))





## [0.18.26](https://github.com/cube-js/cube.js/compare/v0.18.25...v0.18.26) (2020-04-03)


### Bug Fixes

* `AND 1 = 1` case ([cd189d5](https://github.com/cube-js/cube.js/commit/cd189d5))





## [0.18.25](https://github.com/cube-js/cube.js/compare/v0.18.24...v0.18.25) (2020-04-02)


### Bug Fixes

* TypeError: Cannot read property \'replace\' of null for `scheduledRefresh: true` ([28e45c0](https://github.com/cube-js/cube.js/commit/28e45c0)), closes [#558](https://github.com/cube-js/cube.js/issues/558)


### Features

* Basic query rewrites ([af07865](https://github.com/cube-js/cube.js/commit/af07865))





## [0.18.24](https://github.com/cube-js/cube.js/compare/v0.18.23...v0.18.24) (2020-04-01)


### Bug Fixes

* TypeError: Cannot read property \'replace\' of null for `scheduledRefresh: true` ([ea88edf](https://github.com/cube-js/cube.js/commit/ea88edf))





## [0.18.23](https://github.com/cube-js/cube.js/compare/v0.18.22...v0.18.23) (2020-03-30)


### Bug Fixes

* Cannot read property 'timeDimensions' of null -- originalSql scheduledRefresh support ([e7667a5](https://github.com/cube-js/cube.js/commit/e7667a5))
* minute requests incorrectly snapped to daily partitions ([8fd7876](https://github.com/cube-js/cube.js/commit/8fd7876))





## [0.18.19](https://github.com/cube-js/cube.js/compare/v0.18.18...v0.18.19) (2020-03-29)


### Bug Fixes

* Empty default `originalSql` refreshKey ([dd8536b](https://github.com/cube-js/cube.js/commit/dd8536b))
* incorrect WHERE for refreshKey every ([bf8b648](https://github.com/cube-js/cube.js/commit/bf8b648))
* Return single table for one partition queries ([54083ef](https://github.com/cube-js/cube.js/commit/54083ef))


### Features

* `propagateFiltersToSubQuery` flag ([6b253c0](https://github.com/cube-js/cube.js/commit/6b253c0))
* Partitioned `originalSql` support ([133857e](https://github.com/cube-js/cube.js/commit/133857e))





## [0.18.17](https://github.com/cube-js/cube.js/compare/v0.18.16...v0.18.17) (2020-03-24)


### Bug Fixes

* Unknown function NOW for Snowflake -- Incorrect now timestamp implementation ([036f68a](https://github.com/cube-js/cube.js/commit/036f68a)), closes [#537](https://github.com/cube-js/cube.js/issues/537)





## [0.18.16](https://github.com/cube-js/cube.js/compare/v0.18.15...v0.18.16) (2020-03-24)


### Features

* Log canUseTransformedQuery ([5b2ab90](https://github.com/cube-js/cube.js/commit/5b2ab90))





## [0.18.14](https://github.com/cube-js/cube.js/compare/v0.18.13...v0.18.14) (2020-03-24)


### Bug Fixes

* MySQL segment references support ([be42298](https://github.com/cube-js/cube.js/commit/be42298))





## [0.18.6](https://github.com/cube-js/cube.js/compare/v0.18.5...v0.18.6) (2020-03-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.18.5](https://github.com/cube-js/cube.js/compare/v0.18.4...v0.18.5) (2020-03-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.18.4](https://github.com/cube-js/cube.js/compare/v0.18.3...v0.18.4) (2020-03-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.18.3](https://github.com/cube-js/cube.js/compare/v0.18.2...v0.18.3) (2020-03-02)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





# [0.18.0](https://github.com/cube-js/cube.js/compare/v0.17.10...v0.18.0) (2020-03-01)


### Bug Fixes

* Handle multiple occurrences in the first event of a funnel: conversion percent discrepancies. ([0989482](https://github.com/cube-js/cube.js/commit/0989482))


### Features

* COMPILE_CONTEXT and async driverFactory support ([160f931](https://github.com/cube-js/cube.js/commit/160f931))





## [0.17.10](https://github.com/cube-js/cube.js/compare/v0.17.9...v0.17.10) (2020-02-20)


### Features

* Support external rollups from readonly source ([#395](https://github.com/cube-js/cube.js/issues/395)) ([b17e841](https://github.com/cube-js/cube.js/commit/b17e841))





## [0.17.9](https://github.com/cube-js/cube.js/compare/v0.17.8...v0.17.9) (2020-02-18)


### Features

* Extend meta response with aggregation type ([#394](https://github.com/cube-js/cube.js/issues/394)) Thanks to [@pyrooka](https://github.com/pyrooka)! ([06eed0b](https://github.com/cube-js/cube.js/commit/06eed0b))





## [0.17.8](https://github.com/cube-js/cube.js/compare/v0.17.7...v0.17.8) (2020-02-14)


### Bug Fixes

* Wrong interval functions for BigQuery ([#367](https://github.com/cube-js/cube.js/issues/367)) Thanks to [@lvauvillier](https://github.com/lvauvillier)! ([0e09d4d](https://github.com/cube-js/cube.js/commit/0e09d4d))


### Features

* Athena HLL support ([45c7b83](https://github.com/cube-js/cube.js/commit/45c7b83))





## [0.17.7](https://github.com/cube-js/cube.js/compare/v0.17.6...v0.17.7) (2020-02-12)


### Bug Fixes

* Invalid Date: Incorrect MySQL minutes granularity ([dc553b9](https://github.com/cube-js/cube.js/commit/dc553b9))





## [0.17.6](https://github.com/cube-js/cube.js/compare/v0.17.5...v0.17.6) (2020-02-10)


### Bug Fixes

* `sqlAlias` isn't used for pre-aggregation table names ([b757175](https://github.com/cube-js/cube.js/commit/b757175))
* Multiplied measures rollup select case and leaf measure additive exact match ([c897dec](https://github.com/cube-js/cube.js/commit/c897dec))





## [0.17.3](https://github.com/cube-js/cube.js/compare/v0.17.2...v0.17.3) (2020-02-06)


### Features

* Pre-aggregation indexes support ([d443585](https://github.com/cube-js/cube.js/commit/d443585))





## [0.17.2](https://github.com/cube-js/cube.js/compare/v0.17.1...v0.17.2) (2020-02-04)


### Bug Fixes

* Funnel step names cannot contain spaces ([aff1891](https://github.com/cube-js/cube.js/commit/aff1891)), closes [#359](https://github.com/cube-js/cube.js/issues/359)





# [0.17.0](https://github.com/cube-js/cube.js/compare/v0.16.0...v0.17.0) (2020-02-04)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





# [0.16.0](https://github.com/cube-js/cube.js/compare/v0.15.4...v0.16.0) (2020-02-04)


### Features

* Allow `null` filter values ([9e339f7](https://github.com/cube-js/cube.js/commit/9e339f7)), closes [#362](https://github.com/cube-js/cube.js/issues/362)





## [0.15.1](https://github.com/cube-js/cube.js/compare/v0.15.0...v0.15.1) (2020-01-21)


### Features

* `updateWindow` property for incremental partitioned rollup refreshKey ([09c0a86](https://github.com/cube-js/cube.js/commit/09c0a86))





# [0.15.0](https://github.com/cube-js/cube.js/compare/v0.14.3...v0.15.0) (2020-01-18)


### Bug Fixes

* "Illegal input character" when using originalSql pre-aggregation with BigQuery and USER_CONTEXT ([904cf17](https://github.com/cube-js/cube.js/commit/904cf17)), closes [#197](https://github.com/cube-js/cube.js/issues/197)


### Features

* `dynRef` for dynamic member referencing ([41b644c](https://github.com/cube-js/cube.js/commit/41b644c))
* New refreshKeyRenewalThresholds and foreground renew defaults ([9fb0abb](https://github.com/cube-js/cube.js/commit/9fb0abb))
* Slow Query Warning and scheduled refresh for cube refresh keys ([8768b0e](https://github.com/cube-js/cube.js/commit/8768b0e))





## [0.14.3](https://github.com/cube-js/cube.js/compare/v0.14.2...v0.14.3) (2020-01-17)


### Bug Fixes

* originalSql pre-aggregations with FILTER_PARAMS params mismatch ([f4ee7b6](https://github.com/cube-js/cube.js/commit/f4ee7b6))


### Features

* RefreshKeys helper extension of popular implementations ([f2000c0](https://github.com/cube-js/cube.js/commit/f2000c0))





## [0.14.2](https://github.com/cube-js/cube.js/compare/v0.14.1...v0.14.2) (2020-01-17)


### Bug Fixes

* TypeError: Cannot read property 'evaluateSymbolSqlWithContext' of undefined ([125afd7](https://github.com/cube-js/cube.js/commit/125afd7))





## [0.14.1](https://github.com/cube-js/cube.js/compare/v0.14.0...v0.14.1) (2020-01-17)


### Features

* Default refreshKey implementations for mutable and immutable pre-aggregations. ([bef0626](https://github.com/cube-js/cube.js/commit/bef0626))





# [0.14.0](https://github.com/cube-js/cube.js/compare/v0.13.12...v0.14.0) (2020-01-16)


### Bug Fixes

* Time dimension can't be selected twice within same query with and without granularity ([aa65129](https://github.com/cube-js/cube.js/commit/aa65129))


### Features

* Scheduled refresh for pre-aggregations ([c87b525](https://github.com/cube-js/cube.js/commit/c87b525))





## [0.13.7](https://github.com/cube-js/cube.js/compare/v0.13.6...v0.13.7) (2019-12-31)


### Bug Fixes

* ER_TRUNCATED_WRONG_VALUE: Truncated incorrect datetime value ([fcbbe84](https://github.com/cube-js/cube.js/commit/fcbbe84)), closes [#309](https://github.com/cube-js/cube.js/issues/309)
* **client-core:** Uncaught TypeError: cubejs is not a function ([b5c32cd](https://github.com/cube-js/cube.js/commit/b5c32cd))





## [0.13.5](https://github.com/cube-js/cube.js/compare/v0.13.4...v0.13.5) (2019-12-17)


### Features

* Elasticsearch driver preview ([d6a6a07](https://github.com/cube-js/cube.js/commit/d6a6a07))





## [0.13.3](https://github.com/cube-js/cube.js/compare/v0.13.2...v0.13.3) (2019-12-16)


### Features

* **sqlite-driver:** Pre-aggregations support ([5ffb3d2](https://github.com/cube-js/cube.js/commit/5ffb3d2))





# [0.13.0](https://github.com/cube-js/cube.js/compare/v0.12.3...v0.13.0) (2019-12-10)


### Bug Fixes

* cube validation from updating BasePreAggregation ([#285](https://github.com/cube-js/cube.js/issues/285)). Thanks to [@ferrants](https://github.com/ferrants)! ([f4bda4e](https://github.com/cube-js/cube.js/commit/f4bda4e))


### Features

* Minute and second granularities support ([34c5d4c](https://github.com/cube-js/cube.js/commit/34c5d4c))
* Sqlite driver implementation ([f9b43d3](https://github.com/cube-js/cube.js/commit/f9b43d3))





## [0.12.1](https://github.com/cube-js/cube.js/compare/v0.12.0...v0.12.1) (2019-11-26)


### Features

* Show used pre-aggregations and match rollup results in Playground ([4a67346](https://github.com/cube-js/cube.js/commit/4a67346))





# [0.12.0](https://github.com/cube-js/cube.js/compare/v0.11.25...v0.12.0) (2019-11-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.11.21](https://github.com/cube-js/cube.js/compare/v0.11.20...v0.11.21) (2019-11-20)


### Features

* **schema-compiler:** Upgrade babel and support `objectRestSpread` for schema generation ([ac97c44](https://github.com/cube-js/cube.js/commit/ac97c44))





## [0.11.20](https://github.com/cube-js/cube.js/compare/v0.11.19...v0.11.20) (2019-11-18)


### Features

*  support for pre-aggregation time hierarchies ([#258](https://github.com/cube-js/cube.js/issues/258)) Thanks to @Justin-ZS! ([ea78c84](https://github.com/cube-js/cube.js/commit/ea78c84)), closes [#246](https://github.com/cube-js/cube.js/issues/246)
* per cube `dataSource` support ([6dc3fef](https://github.com/cube-js/cube.js/commit/6dc3fef))





## [0.11.19](https://github.com/cube-js/cube.js/compare/v0.11.18...v0.11.19) (2019-11-16)


### Bug Fixes

* Merge back `sqlAlias` support ([80b312f](https://github.com/cube-js/cube.js/commit/80b312f))





## [0.11.18](https://github.com/cube-js/cube.js/compare/v0.11.17...v0.11.18) (2019-11-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.11.16](https://github.com/statsbotco/cubejs-client/compare/v0.11.15...v0.11.16) (2019-11-04)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.11.11](https://github.com/statsbotco/cubejs-client/compare/v0.11.10...v0.11.11) (2019-10-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.11.7](https://github.com/statsbotco/cubejs-client/compare/v0.11.6...v0.11.7) (2019-10-22)


### Features

* dynamic case label ([#236](https://github.com/statsbotco/cubejs-client/issues/236)) ([1a82605](https://github.com/statsbotco/cubejs-client/commit/1a82605)), closes [#235](https://github.com/statsbotco/cubejs-client/issues/235)





# [0.11.0](https://github.com/statsbotco/cubejs-client/compare/v0.10.62...v0.11.0) (2019-10-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.10.62](https://github.com/statsbotco/cubejs-client/compare/v0.10.61...v0.10.62) (2019-10-11)


### Features

* `ungrouped` queries support ([c6ac873](https://github.com/statsbotco/cubejs-client/commit/c6ac873))





## [0.10.61](https://github.com/statsbotco/cubejs-client/compare/v0.10.60...v0.10.61) (2019-10-10)


### Features

* **schema-compiler:** Allow access raw data in `USER_CONTEXT` using `unsafeValue()` method ([52ef146](https://github.com/statsbotco/cubejs-client/commit/52ef146))





## [0.10.59](https://github.com/statsbotco/cubejs-client/compare/v0.10.58...v0.10.59) (2019-10-08)


### Bug Fixes

* Rolling window returns dates in incorrect time zone for Postgres ([71a3baa](https://github.com/statsbotco/cubejs-client/commit/71a3baa)), closes [#216](https://github.com/statsbotco/cubejs-client/issues/216)





## [0.10.41](https://github.com/statsbotco/cubejs-client/compare/v0.10.40...v0.10.41) (2019-09-13)


### Features

* Provide date filter with hourly granularity ([e423d82](https://github.com/statsbotco/cubejs-client/commit/e423d82)), closes [#179](https://github.com/statsbotco/cubejs-client/issues/179)





## [0.10.39](https://github.com/statsbotco/cubejs-client/compare/v0.10.38...v0.10.39) (2019-09-09)


### Bug Fixes

* Requiring local node files is restricted: adding test for relative path resolvers ([f328d07](https://github.com/statsbotco/cubejs-client/commit/f328d07))





## [0.10.38](https://github.com/statsbotco/cubejs-client/compare/v0.10.37...v0.10.38) (2019-09-09)


### Bug Fixes

* Requiring local node files is restricted ([ba3c390](https://github.com/statsbotco/cubejs-client/commit/ba3c390))





## [0.10.36](https://github.com/statsbotco/cubejs-client/compare/v0.10.35...v0.10.36) (2019-09-09)


### Bug Fixes

* all queries forwarded to external DB instead of original one for zero pre-aggregation query ([2c230f4](https://github.com/statsbotco/cubejs-client/commit/2c230f4))





## [0.10.35](https://github.com/statsbotco/cubejs-client/compare/v0.10.34...v0.10.35) (2019-09-09)


### Features

* `originalSql` external pre-aggregations support ([0db2282](https://github.com/statsbotco/cubejs-client/commit/0db2282))





## [0.10.30](https://github.com/statsbotco/cubejs-client/compare/v0.10.29...v0.10.30) (2019-08-26)


### Bug Fixes

* Athena doesn't support `_` in contains filter ([d330be4](https://github.com/statsbotco/cubejs-client/commit/d330be4))





## [0.10.29](https://github.com/statsbotco/cubejs-client/compare/v0.10.28...v0.10.29) (2019-08-21)


### Bug Fixes

* MS SQL segment pre-aggregations support ([f8e37bf](https://github.com/statsbotco/cubejs-client/commit/f8e37bf)), closes [#186](https://github.com/statsbotco/cubejs-client/issues/186)





## [0.10.24](https://github.com/statsbotco/cubejs-client/compare/v0.10.23...v0.10.24) (2019-08-16)


### Bug Fixes

* MS SQL has unusual CTAS syntax ([1a00e4a](https://github.com/statsbotco/cubejs-client/commit/1a00e4a)), closes [#185](https://github.com/statsbotco/cubejs-client/issues/185)





## [0.10.23](https://github.com/statsbotco/cubejs-client/compare/v0.10.22...v0.10.23) (2019-08-14)


### Bug Fixes

* Unexpected string literal Bigquery ([8768895](https://github.com/statsbotco/cubejs-client/commit/8768895)), closes [#182](https://github.com/statsbotco/cubejs-client/issues/182)





## [0.10.21](https://github.com/statsbotco/cubejs-client/compare/v0.10.20...v0.10.21) (2019-08-05)


### Features

* Offset pagination support ([7fb1715](https://github.com/statsbotco/cubejs-client/commit/7fb1715)), closes [#117](https://github.com/statsbotco/cubejs-client/issues/117)





## [0.10.18](https://github.com/statsbotco/cubejs-client/compare/v0.10.17...v0.10.18) (2019-07-31)


### Bug Fixes

* BigQuery external rollup compatibility: use `__` separator for member aliases. Fix missed override. ([c1eb113](https://github.com/statsbotco/cubejs-client/commit/c1eb113))





## [0.10.17](https://github.com/statsbotco/cubejs-client/compare/v0.10.16...v0.10.17) (2019-07-31)


### Bug Fixes

* BigQuery external rollup compatibility: use `__` separator for member aliases. Fix all tests. ([723359c](https://github.com/statsbotco/cubejs-client/commit/723359c))
* Moved joi dependency to it's new availability ([#171](https://github.com/statsbotco/cubejs-client/issues/171)) ([1c20838](https://github.com/statsbotco/cubejs-client/commit/1c20838))





## [0.10.16](https://github.com/statsbotco/cubejs-client/compare/v0.10.15...v0.10.16) (2019-07-20)


### Bug Fixes

* Added correct string concat for Mysql. ([#162](https://github.com/statsbotco/cubejs-client/issues/162)) ([287411b](https://github.com/statsbotco/cubejs-client/commit/287411b))
* remove redundant hacks: primaryKey filter for method dimensionColumns ([#161](https://github.com/statsbotco/cubejs-client/issues/161)) ([f910a56](https://github.com/statsbotco/cubejs-client/commit/f910a56))


### Features

* BigQuery external rollup support ([10c635c](https://github.com/statsbotco/cubejs-client/commit/10c635c))





## [0.10.15](https://github.com/statsbotco/cubejs-client/compare/v0.10.14...v0.10.15) (2019-07-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.10.14](https://github.com/statsbotco/cubejs-client/compare/v0.10.13...v0.10.14) (2019-07-13)


### Features

* Oracle driver ([#160](https://github.com/statsbotco/cubejs-client/issues/160)) ([854ebff](https://github.com/statsbotco/cubejs-client/commit/854ebff))





## [0.10.9](https://github.com/statsbotco/cubejs-client/compare/v0.10.8...v0.10.9) (2019-06-30)


### Bug Fixes

* Syntax error during parsing: Unexpected token, expected: escaping back ticks ([9638a1a](https://github.com/statsbotco/cubejs-client/commit/9638a1a))





## [0.10.4](https://github.com/statsbotco/cubejs-client/compare/v0.10.3...v0.10.4) (2019-06-26)


### Features

* More descriptive error for SyntaxError ([f6d12d3](https://github.com/statsbotco/cubejs-client/commit/f6d12d3))





# [0.10.0](https://github.com/statsbotco/cubejs-client/compare/v0.9.24...v0.10.0) (2019-06-21)


### Features

* **schema-compiler:** `asyncModules` and Node.js `require()`: support loading cube definitions from DB and other async sources ([397cceb](https://github.com/statsbotco/cubejs-client/commit/397cceb)), closes [#141](https://github.com/statsbotco/cubejs-client/issues/141)





## [0.9.23](https://github.com/statsbotco/cubejs-client/compare/v0.9.22...v0.9.23) (2019-06-17)


### Bug Fixes

* **hive:** Fix count when id is not defined ([5a5fffd](https://github.com/statsbotco/cubejs-client/commit/5a5fffd))





## [0.9.21](https://github.com/statsbotco/cubejs-client/compare/v0.9.20...v0.9.21) (2019-06-16)


### Features

* Hive dialect for simple queries ([30d4a30](https://github.com/statsbotco/cubejs-client/commit/30d4a30))





## [0.9.19](https://github.com/statsbotco/cubejs-client/compare/v0.9.18...v0.9.19) (2019-06-13)


### Bug Fixes

* Handle rollingWindow queries without dateRange: TypeError: Cannot read property '0' of undefined at BaseTimeDimension.dateFromFormatted ([409a238](https://github.com/statsbotco/cubejs-client/commit/409a238))
* More descriptive SyntaxError messages ([acd17ad](https://github.com/statsbotco/cubejs-client/commit/acd17ad))





## [0.9.17](https://github.com/statsbotco/cubejs-client/compare/v0.9.16...v0.9.17) (2019-06-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.9.16](https://github.com/statsbotco/cubejs-client/compare/v0.9.15...v0.9.16) (2019-06-10)


### Bug Fixes

* force escape cubeAlias to work with restricted column names such as "case" ([#128](https://github.com/statsbotco/cubejs-client/issues/128)) ([b8a59da](https://github.com/statsbotco/cubejs-client/commit/b8a59da))





## [0.9.15](https://github.com/statsbotco/cubejs-client/compare/v0.9.14...v0.9.15) (2019-06-07)


### Bug Fixes

* **schema-compiler:** subquery in FROM must have an alias -- fix Redshift rollingWindow ([70b752f](https://github.com/statsbotco/cubejs-client/commit/70b752f))





## [0.9.13](https://github.com/statsbotco/cubejs-client/compare/v0.9.12...v0.9.13) (2019-06-06)


### Bug Fixes

* Schema generation with joins having case sensitive table and column names ([#124](https://github.com/statsbotco/cubejs-client/issues/124)) ([c7b706a](https://github.com/statsbotco/cubejs-client/commit/c7b706a)), closes [#120](https://github.com/statsbotco/cubejs-client/issues/120) [#120](https://github.com/statsbotco/cubejs-client/issues/120)





## [0.9.12](https://github.com/statsbotco/cubejs-client/compare/v0.9.11...v0.9.12) (2019-06-03)


### Bug Fixes

* **schema-compiler:** cast parameters for IN filters ([28f3e48](https://github.com/statsbotco/cubejs-client/commit/28f3e48)), closes [#119](https://github.com/statsbotco/cubejs-client/issues/119)





## [0.9.11](https://github.com/statsbotco/cubejs-client/compare/v0.9.10...v0.9.11) (2019-05-31)


### Bug Fixes

* **schema-compiler:** TypeError: Cannot read property 'filterToWhere' of undefined ([6b399ea](https://github.com/statsbotco/cubejs-client/commit/6b399ea))





## [0.9.6](https://github.com/statsbotco/cubejs-client/compare/v0.9.5...v0.9.6) (2019-05-24)


### Bug Fixes

* contains filter does not work with MS SQL Server database ([35210f6](https://github.com/statsbotco/cubejs-client/commit/35210f6)), closes [#113](https://github.com/statsbotco/cubejs-client/issues/113)





# [0.9.0](https://github.com/statsbotco/cubejs-client/compare/v0.8.7...v0.9.0) (2019-05-11)


### Features

* External rollup implementation ([d22a809](https://github.com/statsbotco/cubejs-client/commit/d22a809))





## [0.8.4](https://github.com/statsbotco/cubejs-client/compare/v0.8.3...v0.8.4) (2019-05-02)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.8.3](https://github.com/statsbotco/cubejs-client/compare/v0.8.2...v0.8.3) (2019-05-01)


### Features

* clickhouse dialect implementation ([#98](https://github.com/statsbotco/cubejs-client/issues/98)) ([7236e29](https://github.com/statsbotco/cubejs-client/commit/7236e29)), closes [#93](https://github.com/statsbotco/cubejs-client/issues/93)





## [0.8.1](https://github.com/statsbotco/cubejs-client/compare/v0.8.0...v0.8.1) (2019-04-30)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





# [0.8.0](https://github.com/statsbotco/cubejs-client/compare/v0.7.10...v0.8.0) (2019-04-29)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.7.9](https://github.com/statsbotco/cubejs-client/compare/v0.7.8...v0.7.9) (2019-04-24)


### Features

* **schema-compiler:** Allow to pass functions to USER_CONTEXT ([b489090](https://github.com/statsbotco/cubejs-client/commit/b489090)), closes [#88](https://github.com/statsbotco/cubejs-client/issues/88)





## [0.7.6](https://github.com/statsbotco/cubejs-client/compare/v0.7.5...v0.7.6) (2019-04-23)


### Features

* **schema-compiler:** Athena rollingWindow support ([f112c69](https://github.com/statsbotco/cubejs-client/commit/f112c69))





## [0.7.5](https://github.com/statsbotco/cubejs-client/compare/v0.7.4...v0.7.5) (2019-04-18)


### Bug Fixes

* **schema-compiler:** Athena, Mysql and BigQuery doesn't respect multiple contains filter ([0a8f324](https://github.com/statsbotco/cubejs-client/commit/0a8f324))





# [0.7.0](https://github.com/statsbotco/cubejs-client/compare/v0.6.2...v0.7.0) (2019-04-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





# [0.6.0](https://github.com/statsbotco/cubejs-client/compare/v0.5.2...v0.6.0) (2019-04-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





# [0.5.0](https://github.com/statsbotco/cubejs-client/compare/v0.4.6...v0.5.0) (2019-04-01)


### Bug Fixes

* **schema-compiler:** joi@10.6.0 upgrade to joi@14.3.1 ([#59](https://github.com/statsbotco/cubejs-client/issues/59)) ([f035531](https://github.com/statsbotco/cubejs-client/commit/f035531))
* mongobi issue with parsing schema file with nested fields ([eaf1631](https://github.com/statsbotco/cubejs-client/commit/eaf1631)), closes [#55](https://github.com/statsbotco/cubejs-client/issues/55)





## [0.4.4](https://github.com/statsbotco/cubejs-client/compare/v0.4.3...v0.4.4) (2019-03-17)


### Bug Fixes

* Postgres doesn't show any data for queries with time dimension. ([e95e6fe](https://github.com/statsbotco/cubejs-client/commit/e95e6fe))





## [0.4.3](https://github.com/statsbotco/cubejs-client/compare/v0.4.2...v0.4.3) (2019-03-15)


### Bug Fixes

* **mongobi-driver:** implement `convert_tz` as a simple hour shift ([c97e451](https://github.com/statsbotco/cubejs-client/commit/c97e451)), closes [#50](https://github.com/statsbotco/cubejs-client/issues/50)





# [0.4.0](https://github.com/statsbotco/cubejs-client/compare/v0.3.5-alpha.0...v0.4.0) (2019-03-13)


### Features

* Add MongoBI connector and schema adapter support ([3ebbbf0](https://github.com/statsbotco/cubejs-client/commit/3ebbbf0))
