# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

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
