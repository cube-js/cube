# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.29.33](https://github.com/cube-js/cube.js/compare/v0.29.32...v0.29.33) (2022-03-17)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.29.29](https://github.com/cube-js/cube.js/compare/v0.29.28...v0.29.29) (2022-03-03)


### Bug Fixes

* Timestamp for quarter range in time series has incorrect ending period of 23:59:99  ([#4162](https://github.com/cube-js/cube.js/issues/4162)) Thanks @Yashkochar20 ! ([8e27ae7](https://github.com/cube-js/cube.js/commit/8e27ae73bcb4c8524690122779f2d9e5f1ba7c12))





## [0.29.28](https://github.com/cube-js/cube.js/compare/v0.29.27...v0.29.28) (2022-02-10)


### Bug Fixes

* **@cubejs-backend/athena-driver:** Batching and export support ([#4039](https://github.com/cube-js/cube.js/issues/4039)) ([108f42a](https://github.com/cube-js/cube.js/commit/108f42afdd58ae0027b1b81730f7ca9e72ab9122))





## [0.29.23](https://github.com/cube-js/cube.js/compare/v0.29.22...v0.29.23) (2022-01-26)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.29.21](https://github.com/cube-js/cube.js/compare/v0.29.20...v0.29.21) (2022-01-17)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.29.20](https://github.com/cube-js/cube.js/compare/v0.29.19...v0.29.20) (2022-01-10)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.29.18](https://github.com/cube-js/cube.js/compare/v0.29.17...v0.29.18) (2022-01-09)


### Bug Fixes

* Try to fix Zalgo bug https://github.com/Marak/colors.js/issues/285 ([796c404](https://github.com/cube-js/cube.js/commit/796c4040be5f37d39b519b33fa36c61e3c1b0707))





## [0.29.15](https://github.com/cube-js/cube.js/compare/v0.29.14...v0.29.15) (2021-12-30)


### Features

* Introduce single unified CUBEJS_DB_QUERY_TIMEOUT env variable to set all various variables that control database query timeouts ([#3864](https://github.com/cube-js/cube.js/issues/3864)) ([33c6292](https://github.com/cube-js/cube.js/commit/33c6292059e65e293a7e3d61e1f1e0c1413eeece))





## [0.29.12](https://github.com/cube-js/cube.js/compare/v0.29.11...v0.29.12) (2021-12-29)


### Features

* Split batching upload of pre-aggregations into multiple files to enhance performance and avoid load balancer upload limits ([#3857](https://github.com/cube-js/cube.js/issues/3857)) ([6f71419](https://github.com/cube-js/cube.js/commit/6f71419b1c921a0d4e39231370b181d390b01f3d))





# [0.29.0](https://github.com/cube-js/cube.js/compare/v0.28.67...v0.29.0) (2021-12-14)


### Reverts

* Revert "BREAKING CHANGE: 0.29 (#3809)" (#3811) ([db005ed](https://github.com/cube-js/cube.js/commit/db005edc04d48e8251250ab9d0e19f496cf3b52b)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809) [#3811](https://github.com/cube-js/cube.js/issues/3811)


* BREAKING CHANGE: 0.29 (#3809) ([6f1418b](https://github.com/cube-js/cube.js/commit/6f1418b9963774844f341682e594601a56bb0084)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809)


### BREAKING CHANGES

* Drop support for Node.js 10 (12.x is a minimal version)
* Upgrade Node.js to 14 for Docker images
* Drop support for Node.js 15





## [0.28.55](https://github.com/cube-js/cube.js/compare/v0.28.54...v0.28.55) (2021-11-12)


### Features

* Introduce checkSqlAuth (auth hook for SQL API) ([3191b73](https://github.com/cube-js/cube.js/commit/3191b73816cd63d242349041c54a7037e9027c1a))





## [0.28.52](https://github.com/cube-js/cube.js/compare/v0.28.51...v0.28.52) (2021-11-03)


### Bug Fixes

* Empty data partitioned pre-aggregations are incorrectly handled -- value provided is not in a recognized RFC2822 or ISO format ([9f3acd5](https://github.com/cube-js/cube.js/commit/9f3acd572bcd2421bf8d3581c4c6287b62e77313))





## [0.28.50](https://github.com/cube-js/cube.js/compare/v0.28.49...v0.28.50) (2021-10-28)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.28.42](https://github.com/cube-js/cube.js/compare/v0.28.41...v0.28.42) (2021-10-15)


### Features

* Integrate SQL Connector to Cube.js ([#3544](https://github.com/cube-js/cube.js/issues/3544)) ([f90de4c](https://github.com/cube-js/cube.js/commit/f90de4c9283178962f501826a8a64abb674c37d1))





## [0.28.29](https://github.com/cube-js/cube.js/compare/v0.28.28...v0.28.29) (2021-08-31)


### Features

* Mixed rolling window and regular measure queries from rollup support ([#3326](https://github.com/cube-js/cube.js/issues/3326)) ([3147e33](https://github.com/cube-js/cube.js/commit/3147e339f14ede73e5b0d14d05b9dd1f8b79e7b8))





## [0.28.24](https://github.com/cube-js/cube.js/compare/v0.28.23...v0.28.24) (2021-08-19)


### Features

* Added Quarter to the timeDimensions of ([3f62b2c](https://github.com/cube-js/cube.js/commit/3f62b2c125b2b7b752e370b65be4c89a0c65a623))
* Support quarter granularity ([4ad1356](https://github.com/cube-js/cube.js/commit/4ad1356ac2d2c4d479c25e60519b0f7b4c1605bb))





## [0.28.22](https://github.com/cube-js/cube.js/compare/v0.28.21...v0.28.22) (2021-08-17)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.28.19](https://github.com/cube-js/cube.js/compare/v0.28.18...v0.28.19) (2021-08-13)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.28.17](https://github.com/cube-js/cube.js/compare/v0.28.16...v0.28.17) (2021-08-11)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.28.14](https://github.com/cube-js/cube.js/compare/v0.28.13...v0.28.14) (2021-08-05)


### Bug Fixes

* **@cubejs-client/playground:** live preview styles ([#3191](https://github.com/cube-js/cube.js/issues/3191)) ([862ee27](https://github.com/cube-js/cube.js/commit/862ee2761e0e41a2598b16c13d4d8b0e68b8bb29))





## [0.28.10](https://github.com/cube-js/cube.js/compare/v0.28.9...v0.28.10) (2021-07-30)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.28.9](https://github.com/cube-js/cube.js/compare/v0.28.8...v0.28.9) (2021-07-29)


### Bug Fixes

* Optimize timestamp formatting and table names loading for large partition range serving ([#3166](https://github.com/cube-js/cube.js/issues/3166)) ([e1f8dc5](https://github.com/cube-js/cube.js/commit/e1f8dc5aab469b060f0fe8c69467117171c070fd))





## [0.28.6](https://github.com/cube-js/cube.js/compare/v0.28.5...v0.28.6) (2021-07-22)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.28.2](https://github.com/cube-js/cube.js/compare/v0.28.1...v0.28.2) (2021-07-20)


### Bug Fixes

* Use experimental flag for pre-aggregations queue events bus, debug API ([#3130](https://github.com/cube-js/cube.js/issues/3130)) ([4f141d7](https://github.com/cube-js/cube.js/commit/4f141d789cd0058b2e7c07af6a16642d6c145834))





# [0.28.0](https://github.com/cube-js/cube.js/compare/v0.27.53...v0.28.0) (2021-07-17)


### Features

* Move partition range evaluation from Schema Compiler to Query Orchestrator to allow unbounded queries on partitioned pre-aggregations ([8ea654e](https://github.com/cube-js/cube.js/commit/8ea654e93b57014fb2409e070b3a4c381985a9fd))





## [0.27.47](https://github.com/cube-js/cube.js/compare/v0.27.46...v0.27.47) (2021-07-06)


### Bug Fixes

* **@cubejs-client/playground:** wrong redirect to schema page ([#3064](https://github.com/cube-js/cube.js/issues/3064)) ([2c6f9e8](https://github.com/cube-js/cube.js/commit/2c6f9e878604055e051ea37420379ea0a1cdca13))





## [0.27.46](https://github.com/cube-js/cube.js/compare/v0.27.45...v0.27.46) (2021-07-01)


### Bug Fixes

* Priorities for REFRESH_WORKER/SCHEDULED_REFRESH/SCHEDULED_REFRESH_TIMER ([176cdfd](https://github.com/cube-js/cube.js/commit/176cdfd00e63d8d7d6715e9079ad01edae1eb84a))





## [0.27.45](https://github.com/cube-js/cube.js/compare/v0.27.44...v0.27.45) (2021-06-30)


### Features

* Introduce CUBEJS_REFRESH_WORKER and CUBEJS_ROLLUP_ONLY ([68cb358](https://github.com/cube-js/cube.js/commit/68cb358d627d3ffcf5b73fecc687db66e44209db))





## [0.27.37](https://github.com/cube-js/cube.js/compare/v0.27.36...v0.27.37) (2021-06-21)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.27.35](https://github.com/cube-js/cube.js/compare/v0.27.34...v0.27.35) (2021-06-18)


### Features

* **@cubejs-client/playground:** cli connection wizard ([#2969](https://github.com/cube-js/cube.js/issues/2969)) ([77652d7](https://github.com/cube-js/cube.js/commit/77652d7e0121140b0d52912a862c25aed911ed9d))





## [0.27.33](https://github.com/cube-js/cube.js/compare/v0.27.32...v0.27.33) (2021-06-15)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.27.31](https://github.com/cube-js/cube.js/compare/v0.27.30...v0.27.31) (2021-06-11)


### Features

* **snowflake-driver:** Support UNLOAD to GCS ([91691e9](https://github.com/cube-js/cube.js/commit/91691e9513742496793d0d06a356976fc43b24fd))





## [0.27.30](https://github.com/cube-js/cube.js/compare/v0.27.29...v0.27.30) (2021-06-04)


### Features

* Introduce lock for dropOrphanedTables (concurrency bug) ([29509fa](https://github.com/cube-js/cube.js/commit/29509fa6714f636ba28960119138fbc20d604b4f))
* Make scheduledRefresh true by default (preview period) ([f3e648c](https://github.com/cube-js/cube.js/commit/f3e648c7a3d05bfe4719a8f820794f11611fb8c7))
* **cubestore:** Use NPM's proxy settings in post-installer ([0b4daec](https://github.com/cube-js/cube.js/commit/0b4daec01eb41ff67daf97918df664a3dbab300a))





## [0.27.22](https://github.com/cube-js/cube.js/compare/v0.27.21...v0.27.22) (2021-05-27)


### Features

* **bigquery-driver:** Support CUBEJS_DB_EXPORT_BUCKET ([400c163](https://github.com/cube-js/cube.js/commit/400c1632e978de6c00b4c996088d1b61a9223404))





## [0.27.17](https://github.com/cube-js/cube.js/compare/v0.27.16...v0.27.17) (2021-05-22)


### Bug Fixes

* CUBEJS_JWT_KEY - allow string ([1bd9832](https://github.com/cube-js/cube.js/commit/1bd9832ca1f93dc685ff9633482ebe7c8537e11b))


### Features

* **snowflake-driver:** Support UNLOAD to S3 ([d984f97](https://github.com/cube-js/cube.js/commit/d984f973547d1ff339dbf6ba26f0e363bddf2e98))





## [0.27.15](https://github.com/cube-js/cube.js/compare/v0.27.14...v0.27.15) (2021-05-18)


### Features

* Enable external pre-aggregations by default for new users ([22de035](https://github.com/cube-js/cube.js/commit/22de0358ec35017c45e6a716faaacf176c49c652))





## [0.27.13](https://github.com/cube-js/cube.js/compare/v0.27.12...v0.27.13) (2021-05-13)


### Bug Fixes

* **@cubejs-client/playground:** telemetry ([#2727](https://github.com/cube-js/cube.js/issues/2727)) ([366435a](https://github.com/cube-js/cube.js/commit/366435aeae5ed2d30d520cd0ced2fb6d6b6bbe29))





## [0.27.5](https://github.com/cube-js/cube.js/compare/v0.27.4...v0.27.5) (2021-05-03)


### Features

* Use Cube Store as default EXTERNAL_DB, prefix variables ([30d52c4](https://github.com/cube-js/cube.js/commit/30d52c4f8f1ae1d6619ec4976d9db1eeb1e44140))





## [0.27.4](https://github.com/cube-js/cube.js/compare/v0.27.3...v0.27.4) (2021-04-29)


### Bug Fixes

* Show warning for deprecated variables only once ([fecbda4](https://github.com/cube-js/cube.js/commit/fecbda456b2b66005fb230e093b117db76c4919e))





## [0.27.2](https://github.com/cube-js/cube.js/compare/v0.27.1...v0.27.2) (2021-04-28)


### Bug Fixes

* Disable Scheduling when querying is not ready (before connection Wizard) ([b518ccd](https://github.com/cube-js/cube.js/commit/b518ccdd66a2f51af541bc54de2df816e138bac5))





## [0.27.1](https://github.com/cube-js/cube.js/compare/v0.27.0...v0.27.1) (2021-04-27)


### Bug Fixes

* Deprecate CUBEJS_REDIS_PASSWORD, similar to all REDIS_ variables ([#2604](https://github.com/cube-js/cube.js/issues/2604)) ([ee54aeb](https://github.com/cube-js/cube.js/commit/ee54aebae9db828263feef0f4f5285754abcc5c2)), closes [#ch484](https://github.com/cube-js/cube.js/issues/ch484)





# [0.27.0](https://github.com/cube-js/cube.js/compare/v0.26.104...v0.27.0) (2021-04-26)


### Features

* Add CUBEJS_ prefix for REDIS_URL/REDIS_TLS ([#2312](https://github.com/cube-js/cube.js/issues/2312)) ([b5e7099](https://github.com/cube-js/cube.js/commit/b5e7099da90d0f8c6def0af648b45b18c1e56569))





## [0.26.103](https://github.com/cube-js/cube.js/compare/v0.26.102...v0.26.103) (2021-04-24)


### Bug Fixes

* Typo JWK_KEY -> JWT_KEY ([#2516](https://github.com/cube-js/cube.js/issues/2516)) ([7bdb576](https://github.com/cube-js/cube.js/commit/7bdb576e2130c61eec87e88575fdb96c55d6b4a6)), closes [#ch449](https://github.com/cube-js/cube.js/issues/ch449)





## [0.26.95](https://github.com/cube-js/cube.js/compare/v0.26.94...v0.26.95) (2021-04-13)


### Bug Fixes

* **playground:** Test connection in Connection Wizard ([715fa1c](https://github.com/cube-js/cube.js/commit/715fa1c0ab6667017e12851cbd97e97cf6807d60))





## [0.26.87](https://github.com/cube-js/cube.js/compare/v0.26.86...v0.26.87) (2021-04-10)


### Bug Fixes

* **cubestore:** Something wrong with downloading Cube Store before running it. ([208dd31](https://github.com/cube-js/cube.js/commit/208dd31f20aa64a5c79e143d40055ac2658d0745))





## [0.26.81](https://github.com/cube-js/cube.js/compare/v0.26.80...v0.26.81) (2021-04-07)


### Features

* Dev mode and live preview  ([#2440](https://github.com/cube-js/cube.js/issues/2440)) ([1a7cde8](https://github.com/cube-js/cube.js/commit/1a7cde816ee231ea00d1a952f5000dcc5a8c0ca4))
* Introduce databricks-jdbc-driver ([bb0b31f](https://github.com/cube-js/cube.js/commit/bb0b31fb333f2aa379f11f6733c4efc17ec12dde))





## [0.26.79](https://github.com/cube-js/cube.js/compare/v0.26.78...v0.26.79) (2021-04-06)


### Features

* Check Node.js version in dev-server/server ([d20e9c2](https://github.com/cube-js/cube.js/commit/d20e9c217e0acd894ecbfbf91e24c3d12cfba1c1))





## [0.26.74](https://github.com/cube-js/cube.js/compare/v0.26.73...v0.26.74) (2021-04-01)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.26.69](https://github.com/cube-js/cube.js/compare/v0.26.68...v0.26.69) (2021-03-25)


### Features

* Introduce @cubejs-backend/maven ([#2432](https://github.com/cube-js/cube.js/issues/2432)) ([6dc6034](https://github.com/cube-js/cube.js/commit/6dc6034c3cdcc8e2c2b0568c218228a18b64f44b))





## [0.26.65](https://github.com/cube-js/cube.js/compare/v0.26.64...v0.26.65) (2021-03-24)


### Bug Fixes

* Warning/skip Cube Store on unsupported platforms ([c187e11](https://github.com/cube-js/cube.js/commit/c187e119b8747e1f6bb3fe2bd84f66ae3822ac7d))





## [0.26.54](https://github.com/cube-js/cube.js/compare/v0.26.53...v0.26.54) (2021-03-12)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.26.45](https://github.com/cube-js/cube.js/compare/v0.26.44...v0.26.45) (2021-03-04)


### Bug Fixes

* **schema-compiler:** Lock antlr4ts to 0.5.0-alpha.4, fix [#2264](https://github.com/cube-js/cube.js/issues/2264) ([37b3a0d](https://github.com/cube-js/cube.js/commit/37b3a0d61433ae1b3e41c1264298d1409b7f95b7))


### Features

* Fetch JWK in background only ([954ce30](https://github.com/cube-js/cube.js/commit/954ce30a8d85e51360340558468a5ea4e2e4ca68))





## [0.26.35](https://github.com/cube-js/cube.js/compare/v0.26.34...v0.26.35) (2021-02-25)


### Features

* Use Cube Store as default external storage for CUBEJS_DEV_MODE ([e526676](https://github.com/cube-js/cube.js/commit/e52667617e5e687c92d383045fb1a8d5fd19cab6))





## [0.26.25](https://github.com/cube-js/cube.js/compare/v0.26.24...v0.26.25) (2021-02-20)


### Features

* **bigquery-driver:** Support changing location, CUBEJS_DB_BQ_LOCATION env variable ([204c73c](https://github.com/cube-js/cube.js/commit/204c73c4290760234242b1d3eecdd498bf848e0f))





## [0.26.23](https://github.com/cube-js/cube.js/compare/v0.26.22...v0.26.23) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.26.22](https://github.com/cube-js/cube.js/compare/v0.26.21...v0.26.22) (2021-02-20)


### Features

* Introduce CUBEJS_AGENT_FRAME_SIZE to tweak agent performance ([4550b64](https://github.com/cube-js/cube.js/commit/4550b6475536af016dd5b584adb0f8d3f583dd2a))





## [0.26.19](https://github.com/cube-js/cube.js/compare/v0.26.18...v0.26.19) (2021-02-19)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.26.16](https://github.com/cube-js/cube.js/compare/v0.26.15...v0.26.16) (2021-02-18)


### Bug Fixes

* continueWaitTimeout is ignored: expose single centralized continueWaitTimeout option ([#2120](https://github.com/cube-js/cube.js/issues/2120)) ([2735d2c](https://github.com/cube-js/cube.js/commit/2735d2c1b60c0a31e3970c65b7dd1c293351614e)), closes [#225](https://github.com/cube-js/cube.js/issues/225)





## [0.26.15](https://github.com/cube-js/cube.js/compare/v0.26.14...v0.26.15) (2021-02-16)


### Features

* Support JWK in authentication, improve JWT configuration([#1962](https://github.com/cube-js/cube.js/issues/1962)) ([6e5d2ac](https://github.com/cube-js/cube.js/commit/6e5d2ac0dc05757498b95f308be41d1be86fe206))





## [0.26.13](https://github.com/cube-js/cube.js/compare/v0.26.12...v0.26.13) (2021-02-12)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.26.11](https://github.com/cube-js/cube.js/compare/v0.26.10...v0.26.11) (2021-02-10)


### Bug Fixes

* CUBEJS_SCHEDULED_REFRESH_TIMER, fix [#1972](https://github.com/cube-js/cube.js/issues/1972) ([#1975](https://github.com/cube-js/cube.js/issues/1975)) ([dac7e52](https://github.com/cube-js/cube.js/commit/dac7e52ee0d3a118c9d69c9d030e58a3c048cca1))
* hostname: command not found. fix [#1998](https://github.com/cube-js/cube.js/issues/1998) ([#2027](https://github.com/cube-js/cube.js/issues/2027)) ([8d03cee](https://github.com/cube-js/cube.js/commit/8d03ceec27b2ac3dd057fce4dfc21ff97c6f12a2))





## [0.26.7](https://github.com/cube-js/cube.js/compare/v0.26.6...v0.26.7) (2021-02-09)


### Features

* Support for Redis Sentinel + IORedis driver. fix [#1769](https://github.com/cube-js/cube.js/issues/1769) ([a5e7972](https://github.com/cube-js/cube.js/commit/a5e7972485fa97faaf9965b9794b0cf48256f484))
* Use REDIS_URL for IORedis options (with santinels) ([988bfe5](https://github.com/cube-js/cube.js/commit/988bfe5526be3506fe7b773d247ad89b3287fad4))





## [0.26.2](https://github.com/cube-js/cube.js/compare/v0.26.1...v0.26.2) (2021-02-01)


### Bug Fixes

* Cannot create proxy with a non-object as target or handler ([790a3ba](https://github.com/cube-js/cube.js/commit/790a3ba8887ca00b4ec9ed3e31c7ff4875ae26c5))





# [0.26.0](https://github.com/cube-js/cube.js/compare/v0.25.33...v0.26.0) (2021-02-01)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.25.32](https://github.com/cube-js/cube.js/compare/v0.25.31...v0.25.32) (2021-01-29)


### Bug Fixes

* **shared:** Value True is not valid for CUBEJS_SCHEDULED_REFRESH_TIMER ([99a5759](https://github.com/cube-js/cube.js/commit/99a5759e619824666b48c589a5c98c82c1817025))





## [0.25.30](https://github.com/cube-js/cube.js/compare/v0.25.29...v0.25.30) (2021-01-26)


### Bug Fixes

* **shared:** 1st interval unexpected call on onDuplicatedStateResolved ([6265503](https://github.com/cube-js/cube.js/commit/62655036d337e2ca491d0bda4f7f1b98a6811c4c))





## [0.25.29](https://github.com/cube-js/cube.js/compare/v0.25.28...v0.25.29) (2021-01-26)


### Features

* Improve logs for RefreshScheduler and too long execution ([d0f1f1b](https://github.com/cube-js/cube.js/commit/d0f1f1bbc32473452c763d22ff8ee728c74f6462))





## [0.25.22](https://github.com/cube-js/cube.js/compare/v0.25.21...v0.25.22) (2021-01-21)


### Bug Fixes

* **server:** Unexpected kill on graceful shutdown ([fc99239](https://github.com/cube-js/cube.js/commit/fc992398037719e5d7cc56b35f5e52e59d7c71f2))


### Features

* Log warnings from createCancelableInterval ([44d09c4](https://github.com/cube-js/cube.js/commit/44d09c44da6ddfa845dd457bb766172698c8f334))
* **@cubejs-client/playground:** Database connection wizard ([#1671](https://github.com/cube-js/cube.js/issues/1671)) ([ba30883](https://github.com/cube-js/cube.js/commit/ba30883617c806c9f19ed6c879d0b0c2d656aae1))





## [0.25.18](https://github.com/cube-js/cube.js/compare/v0.25.17...v0.25.18) (2021-01-14)


### Features

* **server:** Kill Cube.js if it's stuck in gracefull shutdown ([0874de8](https://github.com/cube-js/cube.js/commit/0874de8a1b7216d783d947914e2396b35f17d130))





## [0.25.15](https://github.com/cube-js/cube.js/compare/v0.25.14...v0.25.15) (2021-01-12)


### Features

* introduce graceful shutdown ([#1683](https://github.com/cube-js/cube.js/issues/1683)) ([118232f](https://github.com/cube-js/cube.js/commit/118232f56b6c66b7dff6ed11e914ccc107a25881))





## [0.25.6](https://github.com/cube-js/cube.js/compare/v0.25.5...v0.25.6) (2020-12-30)


### Bug Fixes

* Allow CUBEJS_SCHEDULED_REFRESH_TIMER to be boolean ([4e80645](https://github.com/cube-js/cube.js/commit/4e80645259cbd3a5ad7d92f3d07a1d5a58a6c5ef))





## [0.25.5](https://github.com/cube-js/cube.js/compare/v0.25.4...v0.25.5) (2020-12-30)


### Features

* Allow to specify socket for PORT/TLS_PORT, fix [#1681](https://github.com/cube-js/cube.js/issues/1681) ([b9c4669](https://github.com/cube-js/cube.js/commit/b9c466987ffa41f31fa8b3bda88432175e57cd86))





## [0.25.4](https://github.com/cube-js/cube.js/compare/v0.25.3...v0.25.4) (2020-12-30)


### Features

* **server-core:** Introduce CUBEJS_PRE_AGGREGATIONS_SCHEMA, use dev_preaggregations/prod_preaggregations by default ([e5bdf3d](https://github.com/cube-js/cube.js/commit/e5bdf3dfbd28d5e1c1e775c554c275304a0941f3))
* **server-core:** Move to TS ([d7b7431](https://github.com/cube-js/cube.js/commit/d7b743156751dbc2202a7138bc7603dc6861f001))





## [0.25.2](https://github.com/cube-js/cube.js/compare/v0.25.1...v0.25.2) (2020-12-27)


### Features

* Ability to set timeouts for polling in BigQuery/Athena ([#1675](https://github.com/cube-js/cube.js/issues/1675)) ([dc944b1](https://github.com/cube-js/cube.js/commit/dc944b1aaacc69dd74a9d9d31ceaf43e16d37ccd)), closes [#1672](https://github.com/cube-js/cube.js/issues/1672)





# [0.25.0](https://github.com/cube-js/cube.js/compare/v0.24.15...v0.25.0) (2020-12-21)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.24.13](https://github.com/cube-js/cube.js/compare/v0.24.12...v0.24.13) (2020-12-18)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.24.5](https://github.com/cube-js/cube.js/compare/v0.24.4...v0.24.5) (2020-12-09)


### Features

* **cubejs-cli:** improve DX for docker ([#1457](https://github.com/cube-js/cube.js/issues/1457)) ([72ad782](https://github.com/cube-js/cube.js/commit/72ad782090c52e677b9e51e43818f1dca40db791))





## [0.24.4](https://github.com/cube-js/cube.js/compare/v0.24.3...v0.24.4) (2020-12-07)


### Features

* Ability to load SSL keys from FS ([#1512](https://github.com/cube-js/cube.js/issues/1512)) ([71da5bb](https://github.com/cube-js/cube.js/commit/71da5bb529294fabd92b3a914b1e8bceb464643c))





## [0.24.3](https://github.com/cube-js/cube.js/compare/v0.24.2...v0.24.3) (2020-12-01)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.24.2](https://github.com/cube-js/cube.js/compare/v0.24.1...v0.24.2) (2020-11-27)

**Note:** Version bump only for package @cubejs-backend/shared





# [0.24.0](https://github.com/cube-js/cube.js/compare/v0.23.15...v0.24.0) (2020-11-26)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.23.15](https://github.com/cube-js/cube.js/compare/v0.23.14...v0.23.15) (2020-11-25)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.23.14](https://github.com/cube-js/cube.js/compare/v0.23.13...v0.23.14) (2020-11-22)

**Note:** Version bump only for package @cubejs-backend/shared





## [0.23.12](https://github.com/cube-js/cube.js/compare/v0.23.11...v0.23.12) (2020-11-17)


### Features

* Introduce CUBEJS_DEV_MODE & improve ENV variables experience ([#1356](https://github.com/cube-js/cube.js/issues/1356)) ([cc2aa92](https://github.com/cube-js/cube.js/commit/cc2aa92bbec87b21b147d5003fa546d4b1807185))
