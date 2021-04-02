# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.26.74](https://github.com/cube-js/cube.js/compare/v0.26.73...v0.26.74) (2021-04-01)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.69](https://github.com/cube-js/cube.js/compare/v0.26.68...v0.26.69) (2021-03-25)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.65](https://github.com/cube-js/cube.js/compare/v0.26.64...v0.26.65) (2021-03-24)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.60](https://github.com/cube-js/cube.js/compare/v0.26.59...v0.26.60) (2021-03-16)


### Features

* introduce GET /cubejs-system/v1/context ([d97858d](https://github.com/cube-js/cube.js/commit/d97858d528f6efa65400bf54b81b3a8a4039ecb0))





## [0.26.54](https://github.com/cube-js/cube.js/compare/v0.26.53...v0.26.54) (2021-03-12)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.53](https://github.com/cube-js/cube.js/compare/v0.26.52...v0.26.53) (2021-03-11)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.45](https://github.com/cube-js/cube.js/compare/v0.26.44...v0.26.45) (2021-03-04)


### Features

* Fetch JWK in background only ([954ce30](https://github.com/cube-js/cube.js/commit/954ce30a8d85e51360340558468a5ea4e2e4ca68))





## [0.26.35](https://github.com/cube-js/cube.js/compare/v0.26.34...v0.26.35) (2021-02-25)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.25](https://github.com/cube-js/cube.js/compare/v0.26.24...v0.26.25) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.23](https://github.com/cube-js/cube.js/compare/v0.26.22...v0.26.23) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.22](https://github.com/cube-js/cube.js/compare/v0.26.21...v0.26.22) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.19](https://github.com/cube-js/cube.js/compare/v0.26.18...v0.26.19) (2021-02-19)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.16](https://github.com/cube-js/cube.js/compare/v0.26.15...v0.26.16) (2021-02-18)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.15](https://github.com/cube-js/cube.js/compare/v0.26.14...v0.26.15) (2021-02-16)


### Features

* Support JWK in authentication, improve JWT configuration([#1962](https://github.com/cube-js/cube.js/issues/1962)) ([6e5d2ac](https://github.com/cube-js/cube.js/commit/6e5d2ac0dc05757498b95f308be41d1be86fe206))





## [0.26.13](https://github.com/cube-js/cube.js/compare/v0.26.12...v0.26.13) (2021-02-12)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.11](https://github.com/cube-js/cube.js/compare/v0.26.10...v0.26.11) (2021-02-10)


### Bug Fixes

* CUBEJS_SCHEDULED_REFRESH_TIMER, fix [#1972](https://github.com/cube-js/cube.js/issues/1972) ([#1975](https://github.com/cube-js/cube.js/issues/1975)) ([dac7e52](https://github.com/cube-js/cube.js/commit/dac7e52ee0d3a118c9d69c9d030e58a3c048cca1))





## [0.26.7](https://github.com/cube-js/cube.js/compare/v0.26.6...v0.26.7) (2021-02-09)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.26.6](https://github.com/cube-js/cube.js/compare/v0.26.5...v0.26.6) (2021-02-08)


### Features

* Improve typings for extendContext ([8e9c3bc](https://github.com/cube-js/cube.js/commit/8e9c3bcafc3f9acbc8e1a53113202b4be19bb12c))





## [0.26.4](https://github.com/cube-js/cube.js/compare/v0.26.3...v0.26.4) (2021-02-02)


### Bug Fixes

* coerceForSqlQuery - dont mutate securityContext, fix [#1974](https://github.com/cube-js/cube.js/issues/1974) ([95e0536](https://github.com/cube-js/cube.js/commit/95e05364712b9539b564f948dccb44b7367abe26))





## [0.26.2](https://github.com/cube-js/cube.js/compare/v0.26.1...v0.26.2) (2021-02-01)


### Bug Fixes

* Cannot create proxy with a non-object as target or handler ([790a3ba](https://github.com/cube-js/cube.js/commit/790a3ba8887ca00b4ec9ed3e31c7ff4875ae26c5))





## [0.26.1](https://github.com/cube-js/cube.js/compare/v0.26.0...v0.26.1) (2021-02-01)


### Bug Fixes

* **api-gateway:** Await checkAuth middleware ([b3b8ccb](https://github.com/cube-js/cube.js/commit/b3b8ccb86f7a882b30c6d3df407ae024d1c08670))





# [0.26.0](https://github.com/cube-js/cube.js/compare/v0.25.33...v0.26.0) (2021-02-01)


### Features

* Storing userContext inside payload.u is deprecated, moved to root ([559bd87](https://github.com/cube-js/cube.js/commit/559bd8757d9754ab486eed88d1fdb0c280b82dc9))
* USER_CONTEXT -> SECURITY_CONTEXT, authInfo -> securityInfo ([fa5d17c](https://github.com/cube-js/cube.js/commit/fa5d17c0bb703b087f442c41a5bf0a3dca1c5faa))





## [0.25.31](https://github.com/cube-js/cube.js/compare/v0.25.30...v0.25.31) (2021-01-28)


### Features

* Ability to specify dataSource from request ([e8fe83a](https://github.com/cube-js/cube.js/commit/e8fe83abacfd2a47ad440fa2d52f3bf78d7a8c72))





## [0.25.29](https://github.com/cube-js/cube.js/compare/v0.25.28...v0.25.29) (2021-01-26)


### Features

* Improve logs for RefreshScheduler and too long execution ([d0f1f1b](https://github.com/cube-js/cube.js/commit/d0f1f1bbc32473452c763d22ff8ee728c74f6462))





## [0.25.23](https://github.com/cube-js/cube.js/compare/v0.25.22...v0.25.23) (2021-01-22)


### Bug Fixes

* **api-gateway:** Validate a case when chrono can return empty array ([#1848](https://github.com/cube-js/cube.js/issues/1848)) ([e7349f7](https://github.com/cube-js/cube.js/commit/e7349f7bd71800e51a9c1d7cefecc8783bd886d6))





## [0.25.22](https://github.com/cube-js/cube.js/compare/v0.25.21...v0.25.22) (2021-01-21)


### Features

* **@cubejs-client/playground:** Database connection wizard ([#1671](https://github.com/cube-js/cube.js/issues/1671)) ([ba30883](https://github.com/cube-js/cube.js/commit/ba30883617c806c9f19ed6c879d0b0c2d656aae1))





## [0.25.21](https://github.com/cube-js/cube.js/compare/v0.25.20...v0.25.21) (2021-01-19)


### Bug Fixes

* **@cubejs-backend/api-gateway:** readiness fix ([#1791](https://github.com/cube-js/cube.js/issues/1791)) ([d5dad60](https://github.com/cube-js/cube.js/commit/d5dad60e1dda655d67d5d8df4f4d6ee4345dbe42))





## [0.25.15](https://github.com/cube-js/cube.js/compare/v0.25.14...v0.25.15) (2021-01-12)


### Features

* **@cubejs-client/playground:** display slow query warning ([#1649](https://github.com/cube-js/cube.js/issues/1649)) ([ce33f88](https://github.com/cube-js/cube.js/commit/ce33f8849b96ac25dd6f242b61f81e29600f511a))
* introduce graceful shutdown ([#1683](https://github.com/cube-js/cube.js/issues/1683)) ([118232f](https://github.com/cube-js/cube.js/commit/118232f56b6c66b7dff6ed11e914ccc107a25881))





## [0.25.14](https://github.com/cube-js/cube.js/compare/v0.25.13...v0.25.14) (2021-01-11)


### Bug Fixes

* **gateway:** Allow healthchecks to be requested without auth ([95c0c57](https://github.com/cube-js/cube.js/commit/95c0c57d739e6ce46de958883d7dbfe04616a7a0))





## [0.25.2](https://github.com/cube-js/cube.js/compare/v0.25.1...v0.25.2) (2020-12-27)


### Bug Fixes

* **api-gateway:** /readyz /healthz - correct response for partial outage ([1e5bdf5](https://github.com/cube-js/cube.js/commit/1e5bdf556f6f14698945a72c0332e0f6982ba8e7))


### Features

* **api-gateway:** Support schema inside Authorization header, fix [#1297](https://github.com/cube-js/cube.js/issues/1297) ([2549004](https://github.com/cube-js/cube.js/commit/25490048661738e273629c73368ca03f821ee096))





## [0.25.1](https://github.com/cube-js/cube.js/compare/v0.25.0...v0.25.1) (2020-12-24)


### Bug Fixes

* **playground:** Use basePath from configuration, fix [#377](https://github.com/cube-js/cube.js/issues/377) ([c94cbce](https://github.com/cube-js/cube.js/commit/c94cbce50e31617086ec458f934fefaf779b76f4))





# [0.25.0](https://github.com/cube-js/cube.js/compare/v0.24.15...v0.25.0) (2020-12-21)


### Features

* Allow cross data source joins ([a58336e](https://github.com/cube-js/cube.js/commit/a58336e3840f8ac02d83de43ec7661419bceb71c))





## [0.24.14](https://github.com/cube-js/cube.js/compare/v0.24.13...v0.24.14) (2020-12-19)


### Bug Fixes

* **api-gateway:** Fix broken POST /v1/dry-run ([fa0cae0](https://github.com/cube-js/cube.js/commit/fa0cae01fa471e01d88d7db6f1d17046392167d0))





## [0.24.13](https://github.com/cube-js/cube.js/compare/v0.24.12...v0.24.13) (2020-12-18)


### Features

* **api-gateway:** Dont run all health checks, when the one is down ([f5957f4](https://github.com/cube-js/cube.js/commit/f5957f4824372d5e22de25a23a3a1e78445df5d0))





## [0.24.12](https://github.com/cube-js/cube.js/compare/v0.24.11...v0.24.12) (2020-12-17)


### Bug Fixes

* random test crash on Node.js 10 ([b18690e](https://github.com/cube-js/cube.js/commit/b18690e7156ac2ee8892be72e603dbb32836d667))


### Features

* **api-gateway:** Support POST for /v1/dry-run ([d9af942](https://github.com/cube-js/cube.js/commit/d9af9421f8ddf9c6e8ba46ee1afb96e367636aaa))
* Introduce health checks ([#1607](https://github.com/cube-js/cube.js/issues/1607)) ([d96c662](https://github.com/cube-js/cube.js/commit/d96c66201ca8202907af8dc563eaaf908a5ece89))





## [0.24.9](https://github.com/cube-js/cube.js/compare/v0.24.8...v0.24.9) (2020-12-16)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.24.6](https://github.com/cube-js/cube.js/compare/v0.24.5...v0.24.6) (2020-12-13)


### Bug Fixes

* **@cubejs-backend/api-gateway:** SubscriptionServer - support dry-run ([#1581](https://github.com/cube-js/cube.js/issues/1581)) ([43fbc20](https://github.com/cube-js/cube.js/commit/43fbc20a66b4aad335ba198960cc1f626fb909a4))





## [0.24.5](https://github.com/cube-js/cube.js/compare/v0.24.4...v0.24.5) (2020-12-09)


### Bug Fixes

* **@cubejs-backend/api-gateway:** Export UserError/CubejsHandlerError ([#1540](https://github.com/cube-js/cube.js/issues/1540)) ([20124ba](https://github.com/cube-js/cube.js/commit/20124ba26f8330801fd23e33c7c36a2005ae98e8))





## [0.24.4](https://github.com/cube-js/cube.js/compare/v0.24.3...v0.24.4) (2020-12-07)


### Features

* **@cubejs-backend/api-gateway:** Migrate some parts to TS ([c1166d7](https://github.com/cube-js/cube.js/commit/c1166d744ccd562db492e5dedd01eab63e07bfd4))
* **@cubejs-backend/api-gateway:** Migrate to TS initial ([1edef6d](https://github.com/cube-js/cube.js/commit/1edef6d269fd1877f0bfcdcf17d2f780abd4404c))





# [0.24.0](https://github.com/cube-js/cube.js/compare/v0.23.15...v0.24.0) (2020-11-26)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.23.6](https://github.com/cube-js/cube.js/compare/v0.23.5...v0.23.6) (2020-11-02)

**Note:** Version bump only for package @cubejs-backend/api-gateway





# [0.23.0](https://github.com/cube-js/cube.js/compare/v0.22.4...v0.23.0) (2020-10-28)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.22.2](https://github.com/cube-js/cube.js/compare/v0.22.1...v0.22.2) (2020-10-26)

**Note:** Version bump only for package @cubejs-backend/api-gateway





# [0.22.0](https://github.com/cube-js/cube.js/compare/v0.21.2...v0.22.0) (2020-10-20)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.21.1](https://github.com/cube-js/cube.js/compare/v0.21.0...v0.21.1) (2020-10-15)

**Note:** Version bump only for package @cubejs-backend/api-gateway





# [0.21.0](https://github.com/cube-js/cube.js/compare/v0.20.15...v0.21.0) (2020-10-09)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.20.11](https://github.com/cube-js/cube.js/compare/v0.20.10...v0.20.11) (2020-09-28)


### Bug Fixes

* propagate drill down parent filters ([#1143](https://github.com/cube-js/cube.js/issues/1143)) ([314985e](https://github.com/cube-js/cube.js/commit/314985e))





## [0.20.10](https://github.com/cube-js/cube.js/compare/v0.20.9...v0.20.10) (2020-09-23)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.20.9](https://github.com/cube-js/cube.js/compare/v0.20.8...v0.20.9) (2020-09-19)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.20.8](https://github.com/cube-js/cube.js/compare/v0.20.7...v0.20.8) (2020-09-16)


### Features

* refreshKey every support for CRON format interval ([#1048](https://github.com/cube-js/cube.js/issues/1048)) ([3e55f5c](https://github.com/cube-js/cube.js/commit/3e55f5c))





## [0.20.7](https://github.com/cube-js/cube.js/compare/v0.20.6...v0.20.7) (2020-09-11)


### Bug Fixes

* member-dimension query normalization for queryTransformer and additional complex boolean logic tests ([#1047](https://github.com/cube-js/cube.js/issues/1047)) ([65ef327](https://github.com/cube-js/cube.js/commit/65ef327)), closes [#1007](https://github.com/cube-js/cube.js/issues/1007)





## [0.20.6](https://github.com/cube-js/cube.js/compare/v0.20.5...v0.20.6) (2020-09-10)


### Bug Fixes

* pivot control ([05ce626](https://github.com/cube-js/cube.js/commit/05ce626))





## [0.20.5](https://github.com/cube-js/cube.js/compare/v0.20.4...v0.20.5) (2020-09-10)


### Bug Fixes

* query logger ([e5d6ce9](https://github.com/cube-js/cube.js/commit/e5d6ce9))





## [0.20.3](https://github.com/cube-js/cube.js/compare/v0.20.2...v0.20.3) (2020-09-03)


### Features

* Complex boolean logic ([#1038](https://github.com/cube-js/cube.js/issues/1038)) ([a5b44d1](https://github.com/cube-js/cube.js/commit/a5b44d1)), closes [#259](https://github.com/cube-js/cube.js/issues/259)





## [0.20.2](https://github.com/cube-js/cube.js/compare/v0.20.1...v0.20.2) (2020-09-02)


### Bug Fixes

* subscribe option, new query types to work with ws ([dbf602e](https://github.com/cube-js/cube.js/commit/dbf602e))





# [0.20.0](https://github.com/cube-js/cube.js/compare/v0.19.61...v0.20.0) (2020-08-26)


### Features

* add post method for the load endpoint ([#982](https://github.com/cube-js/cube.js/issues/982)). Thanks to @RusovDmitriy ([1524ede](https://github.com/cube-js/cube.js/commit/1524ede))
* Data blending ([#1012](https://github.com/cube-js/cube.js/issues/1012)) ([19fd00e](https://github.com/cube-js/cube.js/commit/19fd00e))
* date range comparison support ([#979](https://github.com/cube-js/cube.js/issues/979)) ([ca21cfd](https://github.com/cube-js/cube.js/commit/ca21cfd))





## [0.19.61](https://github.com/cube-js/cube.js/compare/v0.19.60...v0.19.61) (2020-08-11)


### Features

* add support of array of tuples order format ([#973](https://github.com/cube-js/cube.js/issues/973)). Thanks to @RusovDmitriy ([0950b94](https://github.com/cube-js/cube.js/commit/0950b94))





## [0.19.54](https://github.com/cube-js/cube.js/compare/v0.19.53...v0.19.54) (2020-07-23)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.19.50](https://github.com/cube-js/cube.js/compare/v0.19.49...v0.19.50) (2020-07-16)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.19.49](https://github.com/cube-js/cube.js/compare/v0.19.48...v0.19.49) (2020-07-11)


### Bug Fixes

* TypeError: exports.en is not a function ([ade2ccd](https://github.com/cube-js/cube.js/commit/ade2ccd))





## [0.19.48](https://github.com/cube-js/cube.js/compare/v0.19.47...v0.19.48) (2020-07-11)


### Bug Fixes

* chrono-node upgrade changed `from 60 minutes ago to now` behavior ([e456829](https://github.com/cube-js/cube.js/commit/e456829))





## [0.19.35](https://github.com/cube-js/cube.js/compare/v0.19.34...v0.19.35) (2020-06-22)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.19.33](https://github.com/cube-js/cube.js/compare/v0.19.32...v0.19.33) (2020-06-10)


### Bug Fixes

* **cubejs-api-gateway:** fromEntries replacement ([#715](https://github.com/cube-js/cube.js/issues/715)) ([998c735](https://github.com/cube-js/cube.js/commit/998c735))





## [0.19.31](https://github.com/cube-js/cube.js/compare/v0.19.30...v0.19.31) (2020-06-10)


### Features

* Query builder order by ([#685](https://github.com/cube-js/cube.js/issues/685)) ([d3c735b](https://github.com/cube-js/cube.js/commit/d3c735b))





## [0.19.23](https://github.com/cube-js/cube.js/compare/v0.19.22...v0.19.23) (2020-06-02)


### Features

* drill down queries support ([#664](https://github.com/cube-js/cube.js/issues/664)) ([7e21545](https://github.com/cube-js/cube.js/commit/7e21545)), closes [#190](https://github.com/cube-js/cube.js/issues/190)





## [0.19.19](https://github.com/cube-js/cube.js/compare/v0.19.18...v0.19.19) (2020-05-15)


### Features

* ability to add custom meta data for measures, dimensions and segments ([#641](https://github.com/cube-js/cube.js/issues/641)) ([88d5c9b](https://github.com/cube-js/cube.js/commit/88d5c9b)), closes [#625](https://github.com/cube-js/cube.js/issues/625)





## [0.19.16](https://github.com/cube-js/cube.js/compare/v0.19.15...v0.19.16) (2020-05-07)


### Features

* Update type defs for query transformer ([#619](https://github.com/cube-js/cube.js/issues/619)) Thanks to [@jcw](https://github.com/jcw)-! ([b396b05](https://github.com/cube-js/cube.js/commit/b396b05))





## [0.19.15](https://github.com/cube-js/cube.js/compare/v0.19.14...v0.19.15) (2020-05-04)


### Bug Fixes

* Max date measures incorrectly converted for MySQL ([e704867](https://github.com/cube-js/cube.js/commit/e704867))





## [0.19.5](https://github.com/cube-js/cube.js/compare/v0.19.4...v0.19.5) (2020-04-13)


### Bug Fixes

* Include data transformation in Load Request time ([edf2461](https://github.com/cube-js/cube.js/commit/edf2461))





## [0.19.2](https://github.com/cube-js/cube.js/compare/v0.19.1...v0.19.2) (2020-04-12)


### Bug Fixes

* TypeError: Cannot read property 'timeDimensions' of null ([7d3329b](https://github.com/cube-js/cube.js/commit/7d3329b))





## [0.19.1](https://github.com/cube-js/cube.js/compare/v0.19.0...v0.19.1) (2020-04-11)


### Features

* Provide status messages for ``/cubejs-api/v1/run-scheduled-refresh` API ([fb6623f](https://github.com/cube-js/cube.js/commit/fb6623f))





# [0.19.0](https://github.com/cube-js/cube.js/compare/v0.18.32...v0.19.0) (2020-04-09)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.18.7](https://github.com/cube-js/cube.js/compare/v0.18.6...v0.18.7) (2020-03-17)


### Features

* Log `requestId` in compiling schema events ([4c457c9](https://github.com/cube-js/cube.js/commit/4c457c9))





## [0.18.5](https://github.com/cube-js/cube.js/compare/v0.18.4...v0.18.5) (2020-03-15)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.18.4](https://github.com/cube-js/cube.js/compare/v0.18.3...v0.18.4) (2020-03-09)


### Bug Fixes

* Request span for WebSocketTransport is incorrectly set ([54ba5da](https://github.com/cube-js/cube.js/commit/54ba5da))
* results not converted to timezone unless granularity is set: value fails to match the required pattern ([715ba71](https://github.com/cube-js/cube.js/commit/715ba71)), closes [#443](https://github.com/cube-js/cube.js/issues/443)


### Features

* Add API gateway request logging support ([#475](https://github.com/cube-js/cube.js/issues/475)) ([465471e](https://github.com/cube-js/cube.js/commit/465471e))





# [0.18.0](https://github.com/cube-js/cube.js/compare/v0.17.10...v0.18.0) (2020-03-01)


### Bug Fixes

* Handle primaryKey shown: false pitfall error ([5bbf5f0](https://github.com/cube-js/cube.js/commit/5bbf5f0))





## [0.17.1](https://github.com/cube-js/cube.js/compare/v0.17.0...v0.17.1) (2020-02-04)


### Bug Fixes

* TypeError: Cannot read property 'map' of undefined ([a12610d](https://github.com/cube-js/cube.js/commit/a12610d))





# [0.17.0](https://github.com/cube-js/cube.js/compare/v0.16.0...v0.17.0) (2020-02-04)

**Note:** Version bump only for package @cubejs-backend/api-gateway





# [0.16.0](https://github.com/cube-js/cube.js/compare/v0.15.4...v0.16.0) (2020-02-04)


### Bug Fixes

* Do not pad `last 24 hours` interval to day ([6554611](https://github.com/cube-js/cube.js/commit/6554611)), closes [#361](https://github.com/cube-js/cube.js/issues/361)


### Features

* Allow `null` filter values ([9e339f7](https://github.com/cube-js/cube.js/commit/9e339f7)), closes [#362](https://github.com/cube-js/cube.js/issues/362)





## [0.15.3](https://github.com/cube-js/cube.js/compare/v0.15.2...v0.15.3) (2020-01-26)


### Bug Fixes

* TypeError: Cannot read property 'title' of undefined ([3f76066](https://github.com/cube-js/cube.js/commit/3f76066))





# [0.15.0](https://github.com/cube-js/cube.js/compare/v0.14.3...v0.15.0) (2020-01-18)

**Note:** Version bump only for package @cubejs-backend/api-gateway





# [0.14.0](https://github.com/cube-js/cube.js/compare/v0.13.12...v0.14.0) (2020-01-16)


### Bug Fixes

* dateRange gets translated to incorrect value ([71d07e6](https://github.com/cube-js/cube.js/commit/71d07e6)), closes [#348](https://github.com/cube-js/cube.js/issues/348)
* Time dimension can't be selected twice within same query with and without granularity ([aa65129](https://github.com/cube-js/cube.js/commit/aa65129))


### Features

* Scheduled Refresh REST API ([472a0c3](https://github.com/cube-js/cube.js/commit/472a0c3))





## [0.13.9](https://github.com/cube-js/cube.js/compare/v0.13.8...v0.13.9) (2020-01-03)


### Bug Fixes

* define context outside try-catch ([3075624](https://github.com/cube-js/cube.js/commit/3075624))





## [0.13.8](https://github.com/cube-js/cube.js/compare/v0.13.7...v0.13.8) (2019-12-31)


### Bug Fixes

* UnhandledPromiseRejectionWarning: TypeError: Converting circular structure to JSON ([44c5065](https://github.com/cube-js/cube.js/commit/44c5065))





## [0.13.7](https://github.com/cube-js/cube.js/compare/v0.13.6...v0.13.7) (2019-12-31)


### Features

* Extendable context ([#299](https://github.com/cube-js/cube.js/issues/299)) ([38e33ce](https://github.com/cube-js/cube.js/commit/38e33ce)), closes [#295](https://github.com/cube-js/cube.js/issues/295) [#296](https://github.com/cube-js/cube.js/issues/296)





## [0.13.6](https://github.com/cube-js/cube.js/compare/v0.13.5...v0.13.6) (2019-12-19)


### Bug Fixes

* Date parser returns 31 days for `last 30 days` date range ([bedbe9c](https://github.com/cube-js/cube.js/commit/bedbe9c)), closes [#303](https://github.com/cube-js/cube.js/issues/303)





## [0.13.2](https://github.com/cube-js/cube.js/compare/v0.13.1...v0.13.2) (2019-12-13)


### Features

* Error type for returning specific http status codes ([#288](https://github.com/cube-js/cube.js/issues/288)). Thanks to [@willhausman](https://github.com/willhausman)! ([969e609](https://github.com/cube-js/cube.js/commit/969e609))
* Propagate `requestId` for trace logging ([24d7b41](https://github.com/cube-js/cube.js/commit/24d7b41))





## [0.13.1](https://github.com/cube-js/cube.js/compare/v0.13.0...v0.13.1) (2019-12-10)


### Bug Fixes

* **api-gateway:** getTime on undefined call in case of web socket auth error ([9807b1e](https://github.com/cube-js/cube.js/commit/9807b1e))





# [0.13.0](https://github.com/cube-js/cube.js/compare/v0.12.3...v0.13.0) (2019-12-10)


### Bug Fixes

* Errors during web socket subscribe returned with status 200 code ([6df008e](https://github.com/cube-js/cube.js/commit/6df008e))


### Features

* Minute and second granularities support ([34c5d4c](https://github.com/cube-js/cube.js/commit/34c5d4c))





## [0.12.1](https://github.com/cube-js/cube.js/compare/v0.12.0...v0.12.1) (2019-11-26)


### Features

* Show used pre-aggregations and match rollup results in Playground ([4a67346](https://github.com/cube-js/cube.js/commit/4a67346))





# [0.12.0](https://github.com/cube-js/cube.js/compare/v0.11.25...v0.12.0) (2019-11-25)


### Features

* Show `refreshKey` values in Playground ([b49e184](https://github.com/cube-js/cube.js/commit/b49e184))





## [0.11.20](https://github.com/cube-js/cube.js/compare/v0.11.19...v0.11.20) (2019-11-18)


### Features

* per cube `dataSource` support ([6dc3fef](https://github.com/cube-js/cube.js/commit/6dc3fef))





## [0.11.18](https://github.com/cube-js/cube.js/compare/v0.11.17...v0.11.18) (2019-11-09)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.11.17](https://github.com/statsbotco/cubejs-client/compare/v0.11.16...v0.11.17) (2019-11-08)


### Bug Fixes

* Default Express middleware security check is ignored in production ([4bdf6bd](https://github.com/statsbotco/cubejs-client/commit/4bdf6bd))





## [0.11.16](https://github.com/statsbotco/cubejs-client/compare/v0.11.15...v0.11.16) (2019-11-04)


### Bug Fixes

* Respect timezone for natural language date parsing and align custom date ranges to dates by default to ensure backward compatibility ([af6f3c2](https://github.com/statsbotco/cubejs-client/commit/af6f3c2))
* Respect timezone for natural language date parsing and align custom date ranges to dates by default to ensure backward compatibility ([2104492](https://github.com/statsbotco/cubejs-client/commit/2104492))





## [0.11.6](https://github.com/statsbotco/cubejs-client/compare/v0.11.5...v0.11.6) (2019-10-17)


### Bug Fixes

* Yesterday date range doesn't work ([6c81a02](https://github.com/statsbotco/cubejs-client/commit/6c81a02))





## [0.11.5](https://github.com/statsbotco/cubejs-client/compare/v0.11.4...v0.11.5) (2019-10-17)


### Bug Fixes

* **api-gateway:** TypeError: res.json is not a function ([7f3f0a8](https://github.com/statsbotco/cubejs-client/commit/7f3f0a8))





# [0.11.0](https://github.com/statsbotco/cubejs-client/compare/v0.10.62...v0.11.0) (2019-10-15)


### Features

* Sockets Preview ([#231](https://github.com/statsbotco/cubejs-client/issues/231)) ([89fc762](https://github.com/statsbotco/cubejs-client/commit/89fc762)), closes [#221](https://github.com/statsbotco/cubejs-client/issues/221)





## [0.10.62](https://github.com/statsbotco/cubejs-client/compare/v0.10.61...v0.10.62) (2019-10-11)


### Features

* `ungrouped` queries support ([c6ac873](https://github.com/statsbotco/cubejs-client/commit/c6ac873))





## [0.10.34](https://github.com/statsbotco/cubejs-client/compare/v0.10.33...v0.10.34) (2019-09-06)


### Bug Fixes

* Athena timezone conversion issue for non-UTC server ([7085d2f](https://github.com/statsbotco/cubejs-client/commit/7085d2f))





## [0.10.24](https://github.com/statsbotco/cubejs-client/compare/v0.10.23...v0.10.24) (2019-08-16)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.10.21](https://github.com/statsbotco/cubejs-client/compare/v0.10.20...v0.10.21) (2019-08-05)


### Features

* Offset pagination support ([7fb1715](https://github.com/statsbotco/cubejs-client/commit/7fb1715)), closes [#117](https://github.com/statsbotco/cubejs-client/issues/117)





## [0.10.17](https://github.com/statsbotco/cubejs-client/compare/v0.10.16...v0.10.17) (2019-07-31)


### Bug Fixes

* Moved joi dependency to it's new availability ([#171](https://github.com/statsbotco/cubejs-client/issues/171)) ([1c20838](https://github.com/statsbotco/cubejs-client/commit/1c20838))





## [0.10.15](https://github.com/statsbotco/cubejs-client/compare/v0.10.14...v0.10.15) (2019-07-13)

**Note:** Version bump only for package @cubejs-backend/api-gateway





# [0.10.0](https://github.com/statsbotco/cubejs-client/compare/v0.9.24...v0.10.0) (2019-06-21)


### Features

* **api-gateway:** `queryTransformer` security hook ([a9c41b2](https://github.com/statsbotco/cubejs-client/commit/a9c41b2))





## [0.9.20](https://github.com/statsbotco/cubejs-client/compare/v0.9.19...v0.9.20) (2019-06-16)


### Bug Fixes

* **api-gateway:** Unexpected token u in JSON at position 0 at JSON.parse ([f95cea8](https://github.com/statsbotco/cubejs-client/commit/f95cea8))





## [0.9.19](https://github.com/statsbotco/cubejs-client/compare/v0.9.18...v0.9.19) (2019-06-13)


### Bug Fixes

* **api-gateway:** handle can't parse date: Cannot read property 'end' of undefined ([a61b0da](https://github.com/statsbotco/cubejs-client/commit/a61b0da))
* Handle requests for hidden members: TypeError: Cannot read property 'type' of undefined at R.pipe.R.map.p ([5cdf71b](https://github.com/statsbotco/cubejs-client/commit/5cdf71b))





## [0.9.12](https://github.com/statsbotco/cubejs-client/compare/v0.9.11...v0.9.12) (2019-06-03)


### Bug Fixes

* **api-gateway:** Unexpected token u in JSON at position 0 at JSON.parse ([91ca994](https://github.com/statsbotco/cubejs-client/commit/91ca994))





## [0.9.5](https://github.com/statsbotco/cubejs-client/compare/v0.9.4...v0.9.5) (2019-05-22)


### Features

* Propagate `renewQuery` option from API to orchestrator ([9c640ba](https://github.com/statsbotco/cubejs-client/commit/9c640ba)), closes [#112](https://github.com/statsbotco/cubejs-client/issues/112)





# [0.9.0](https://github.com/statsbotco/cubejs-client/compare/v0.8.7...v0.9.0) (2019-05-11)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.8.4](https://github.com/statsbotco/cubejs-client/compare/v0.8.3...v0.8.4) (2019-05-02)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.8.1](https://github.com/statsbotco/cubejs-client/compare/v0.8.0...v0.8.1) (2019-04-30)

**Note:** Version bump only for package @cubejs-backend/api-gateway





# [0.8.0](https://github.com/statsbotco/cubejs-client/compare/v0.7.10...v0.8.0) (2019-04-29)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.7.6](https://github.com/statsbotco/cubejs-client/compare/v0.7.5...v0.7.6) (2019-04-23)


### Features

* Support member key in filters in query ([#91](https://github.com/statsbotco/cubejs-client/issues/91)) ([e1fccc0](https://github.com/statsbotco/cubejs-client/commit/e1fccc0))





## [0.7.4](https://github.com/statsbotco/cubejs-client/compare/v0.7.3...v0.7.4) (2019-04-17)


### Bug Fixes

* **api-gateway:** measures is always required ([04adb7d](https://github.com/statsbotco/cubejs-client/commit/04adb7d))





# [0.7.0](https://github.com/statsbotco/cubejs-client/compare/v0.6.2...v0.7.0) (2019-04-15)


### Features

* App multi-tenancy support in single ServerCore instance ([6f0220f](https://github.com/statsbotco/cubejs-client/commit/6f0220f))





## [0.6.2](https://github.com/statsbotco/cubejs-client/compare/v0.6.1...v0.6.2) (2019-04-12)


### Features

* Natural language date range support ([b962e80](https://github.com/statsbotco/cubejs-client/commit/b962e80))
* **api-gateway:** Order support ([670237b](https://github.com/statsbotco/cubejs-client/commit/670237b))





## [0.6.1](https://github.com/statsbotco/cubejs-client/compare/v0.6.0...v0.6.1) (2019-04-11)


### Features

* Disable authentication checks in developer mode ([bc09eba](https://github.com/statsbotco/cubejs-client/commit/bc09eba))
* Formatted error logging in developer mode ([3376a50](https://github.com/statsbotco/cubejs-client/commit/3376a50))





# [0.6.0](https://github.com/statsbotco/cubejs-client/compare/v0.5.2...v0.6.0) (2019-04-09)


### Features

* query validation added in api-gateway ([#73](https://github.com/statsbotco/cubejs-client/issues/73)) ([21f6176](https://github.com/statsbotco/cubejs-client/commit/21f6176)), closes [#39](https://github.com/statsbotco/cubejs-client/issues/39)





# [0.5.0](https://github.com/statsbotco/cubejs-client/compare/v0.4.6...v0.5.0) (2019-04-01)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.4.5](https://github.com/statsbotco/cubejs-client/compare/v0.4.4...v0.4.5) (2019-03-21)


### Features

* Make API path namespace configurable ([#53](https://github.com/statsbotco/cubejs-client/issues/53)) ([b074a3d](https://github.com/statsbotco/cubejs-client/commit/b074a3d))





## [0.4.4](https://github.com/statsbotco/cubejs-client/compare/v0.4.3...v0.4.4) (2019-03-17)


### Bug Fixes

* Postgres doesn't show any data for queries with time dimension. ([e95e6fe](https://github.com/statsbotco/cubejs-client/commit/e95e6fe))





## [0.4.3](https://github.com/statsbotco/cubejs-client/compare/v0.4.2...v0.4.3) (2019-03-15)


### Bug Fixes

* **mongobi-driver:** implement `convert_tz` as a simple hour shift ([c97e451](https://github.com/statsbotco/cubejs-client/commit/c97e451)), closes [#50](https://github.com/statsbotco/cubejs-client/issues/50)





## [0.4.1](https://github.com/statsbotco/cubejs-client/compare/v0.4.0...v0.4.1) (2019-03-14)


### Features

* Allow to use custom checkAuth middleware ([19d5cd8](https://github.com/statsbotco/cubejs-client/commit/19d5cd8)), closes [#42](https://github.com/statsbotco/cubejs-client/issues/42)





## [0.3.5-alpha.0](https://github.com/statsbotco/cubejs-client/compare/v0.3.5...v0.3.5-alpha.0) (2019-03-12)

**Note:** Version bump only for package @cubejs-backend/api-gateway
