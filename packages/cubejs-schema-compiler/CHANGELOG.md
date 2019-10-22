# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

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
