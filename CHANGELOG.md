# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.26.10](https://github.com/cube-js/cube.js/compare/v0.26.9...v0.26.10) (2021-02-09)


### Bug Fixes

* Using .end() without the flush parameter is deprecated and throws from v.3.0.0 ([7078f41](https://github.com/cube-js/cube.js/commit/7078f4146572a4eb447b9ed6f64e071b86e0aca2))
* **cubestore:** Speed up CSV parsing ([#2032](https://github.com/cube-js/cube.js/issues/2032)) ([39e95f0](https://github.com/cube-js/cube.js/commit/39e95f0347e068012e5e7d60060168c271edab98))





## [0.26.9](https://github.com/cube-js/cube.js/compare/v0.26.8...v0.26.9) (2021-02-09)


### Bug Fixes

* **@cubejs-client/playground:** closing tag ([b0777b3](https://github.com/cube-js/cube.js/commit/b0777b3cf07e563280dfe9a41e1c7628f21b6360))
* **@cubejs-client/playground:** tab switch ([ddd93f3](https://github.com/cube-js/cube.js/commit/ddd93f31fbe8dd90854cca782f72193860b777ba))





## [0.26.8](https://github.com/cube-js/cube.js/compare/v0.26.7...v0.26.8) (2021-02-09)


### Bug Fixes

* **@cubejs-client/playground:** allow to store multiple tokens ([bf3f49a](https://github.com/cube-js/cube.js/commit/bf3f49ae2357e9a7eff5027cfa5573f8290a110b))
* **@cubejs-client/playground:** angular chart renderer ([1132103](https://github.com/cube-js/cube.js/commit/1132103b8c1d9b2ad0f986518dbe48bc3ed4d377))
* **cubestore:** Increase job timeout to 10 minutes ([a874f60](https://github.com/cube-js/cube.js/commit/a874f6035bf374432dcfc775374ad373aeb5c118))


### Reverts

* Revert "fix(cubestore): skip WAL, partition data directly during ingestion (#2002)" ([c9a6527](https://github.com/cube-js/cube.js/commit/c9a6527375f1223d69f3163d18343a5d52bc4f05)), closes [#2002](https://github.com/cube-js/cube.js/issues/2002)





## [0.26.7](https://github.com/cube-js/cube.js/compare/v0.26.6...v0.26.7) (2021-02-09)


### Bug Fixes

* **cubestore:** skip WAL, partition data directly during ingestion ([#2002](https://github.com/cube-js/cube.js/issues/2002)) ([5442fad](https://github.com/cube-js/cube.js/commit/5442fad9e804615de6271f555b6084e4c5e45c28))


### Features

* **@cubejs-client/playground:** security context editing ([#1986](https://github.com/cube-js/cube.js/issues/1986)) ([90f2365](https://github.com/cube-js/cube.js/commit/90f2365eb21313fb5ea7a80583622e0ed742005c))
* Support for Redis Sentinel + IORedis driver. fix [#1769](https://github.com/cube-js/cube.js/issues/1769) ([a5e7972](https://github.com/cube-js/cube.js/commit/a5e7972485fa97faaf9965b9794b0cf48256f484))
* Use REDIS_URL for IORedis options (with santinels) ([988bfe5](https://github.com/cube-js/cube.js/commit/988bfe5526be3506fe7b773d247ad89b3287fad4))





## [0.26.6](https://github.com/cube-js/cube.js/compare/v0.26.5...v0.26.6) (2021-02-08)


### Bug Fixes

* **cubestore:** Increase default split thresholds as memory issues are fixed ([7771a86](https://github.com/cube-js/cube.js/commit/7771a869ae51d58a61dce2ffcbd3dd0f9dc8c483))
* **server-core:** add support for async driverFactory ([312f90b](https://github.com/cube-js/cube.js/commit/312f90b0c6d92f053f1033ecf15efea1c10a4c0a))
* **sqlite-driver:** Use workaround for FLOOR ([#1931](https://github.com/cube-js/cube.js/issues/1931)) ([fe64feb](https://github.com/cube-js/cube.js/commit/fe64febd1b970c4b8396d05a859f16b3d9e5a8a8))


### Features

* **@cubejs-client/playground:** Building pre-aggregations message ([#1984](https://github.com/cube-js/cube.js/issues/1984)) ([e1fff5d](https://github.com/cube-js/cube.js/commit/e1fff5de4584df1bd8ef518e2436e1dcb4962975))
* Block from uploading files and folders (recur) starting from "." ([d549fc4](https://github.com/cube-js/cube.js/commit/d549fc4ab6eff19b3c5273cafb7427be1cbaca98))
* Improve typings for extendContext ([8e9c3bc](https://github.com/cube-js/cube.js/commit/8e9c3bcafc3f9acbc8e1a53113202b4be19bb12c))
* Partitions warmup ([#1993](https://github.com/cube-js/cube.js/issues/1993)) ([200dab1](https://github.com/cube-js/cube.js/commit/200dab193eee43649b0a3e9f5240bc4bf3576fcc))
* **cubestore:** Distributed jobs implementation ([#2001](https://github.com/cube-js/cube.js/issues/2001)) ([064ca30](https://github.com/cube-js/cube.js/commit/064ca3056ac3c52f8514ac5fb21f23f6b6b43244))
* **server-core:** Correct typings for driverFactory/dialectFactory ([51fb117](https://github.com/cube-js/cube.js/commit/51fb117883d2e04c3a8fce4494ac48e0938a0097))





## [0.26.5](https://github.com/cube-js/cube.js/compare/v0.26.4...v0.26.5) (2021-02-03)


### Bug Fixes

* **cubestore:** Return physical memory to the system after compaction ([cdfec78](https://github.com/cube-js/cube.js/commit/cdfec78a43d7a2c3a25c2ee1842147c8060e3fb4))
* **cubestore:** return physical memory to the system at rest ([#1981](https://github.com/cube-js/cube.js/issues/1981)) ([7249a7d](https://github.com/cube-js/cube.js/commit/7249a7d0a96eaea3bb142f56f816690bec908618))


### Features

* **cubestore:** Multiple location load support ([#1982](https://github.com/cube-js/cube.js/issues/1982)) ([2b509ec](https://github.com/cube-js/cube.js/commit/2b509ec3c50be0688d613d2cda1ac3f53e80e093))





## [0.26.4](https://github.com/cube-js/cube.js/compare/v0.26.3...v0.26.4) (2021-02-02)


### Bug Fixes

* coerceForSqlQuery - dont mutate securityContext, fix [#1974](https://github.com/cube-js/cube.js/issues/1974) ([95e0536](https://github.com/cube-js/cube.js/commit/95e05364712b9539b564f948dccb44b7367abe26))





## [0.26.3](https://github.com/cube-js/cube.js/compare/v0.26.2...v0.26.3) (2021-02-02)


### Bug Fixes

* **@cubejs-client/playground:** table presentation ([09c953d](https://github.com/cube-js/cube.js/commit/09c953d54b0aebdf9174f00750652fde3787fc50))





## [0.26.2](https://github.com/cube-js/cube.js/compare/v0.26.1...v0.26.2) (2021-02-01)


### Bug Fixes

* **cubestore:** sort data in column order from the index ([#1956](https://github.com/cube-js/cube.js/issues/1956)) ([342491e](https://github.com/cube-js/cube.js/commit/342491e720c1d1c6239005edcb8423777aaafb83))
* Cannot create proxy with a non-object as target or handler ([790a3ba](https://github.com/cube-js/cube.js/commit/790a3ba8887ca00b4ec9ed3e31c7ff4875ae26c5))


### Features

* **cubestore:** filter rowgroups when reading parquet files ([#1957](https://github.com/cube-js/cube.js/issues/1957)) ([4df454c](https://github.com/cube-js/cube.js/commit/4df454c69542d72cc7846b69259d03c69d1a80c8))





## [0.26.1](https://github.com/cube-js/cube.js/compare/v0.26.0...v0.26.1) (2021-02-01)


### Bug Fixes

* **api-gateway:** Await checkAuth middleware ([b3b8ccb](https://github.com/cube-js/cube.js/commit/b3b8ccb86f7a882b30c6d3df407ae024d1c08670))





# [0.26.0](https://github.com/cube-js/cube.js/compare/v0.25.33...v0.26.0) (2021-02-01)


### Features

* Storing userContext inside payload.u is deprecated, moved to root ([559bd87](https://github.com/cube-js/cube.js/commit/559bd8757d9754ab486eed88d1fdb0c280b82dc9))
* USER_CONTEXT -> SECURITY_CONTEXT, authInfo -> securityInfo ([fa5d17c](https://github.com/cube-js/cube.js/commit/fa5d17c0bb703b087f442c41a5bf0a3dca1c5faa))
* Warning about Node.js 10 deprecation ([7d15099](https://github.com/cube-js/cube.js/commit/7d15099462e60cb666bd9342580583ddf325c2ab))





## [0.25.33](https://github.com/cube-js/cube.js/compare/v0.25.32...v0.25.33) (2021-01-30)


### Bug Fixes

* **cubestore:** min/max statistics on parquet writes ([#1925](https://github.com/cube-js/cube.js/issues/1925)) ([c7b5bbf](https://github.com/cube-js/cube.js/commit/c7b5bbf5add13eeb67e63dc76d0fe30304f54ab0))
* Use local dates for pre-aggregations to avoid timezone shift discrepancies on DST timezones for timezone unaware databases like MySQL ([#1941](https://github.com/cube-js/cube.js/issues/1941)) ([f138e6f](https://github.com/cube-js/cube.js/commit/f138e6fa3d97492c34527d0f04917e78c374eb57))
* **cubestore:** Correct `convert_tz` implementation ([f06d91e](https://github.com/cube-js/cube.js/commit/f06d91ed43d3b9d2a9398c03e30f2a86d70b64f6))
* **cubestore:** Correct `convert_tz` implementation: correct sign ([999e00a](https://github.com/cube-js/cube.js/commit/999e00a96e61b9420adcd04e94b329b93a8a03bb))
* **schema-compiler:** Wrong dayOffset in refreshKey for not UTC computers ([#1938](https://github.com/cube-js/cube.js/issues/1938)) ([5fe3431](https://github.com/cube-js/cube.js/commit/5fe3431a8f7320555fc3dba101c72547a0f41dac))


### Features

* Warning on unconfigured scheduledRefreshContexts in multitenancy mode, fix [#1904](https://github.com/cube-js/cube.js/issues/1904) ([cf1984b](https://github.com/cube-js/cube.js/commit/cf1984b754d804383a72733d895bbb3a42544f2a))





## [0.25.32](https://github.com/cube-js/cube.js/compare/v0.25.31...v0.25.32) (2021-01-29)


### Bug Fixes

* **@cubejs-client/playground:** base64 file upload ([#1915](https://github.com/cube-js/cube.js/issues/1915)) ([8ba70fd](https://github.com/cube-js/cube.js/commit/8ba70fdd1d3aa8907cb3dd4e0f4bce34ac4e6e70))
* **cubestore:** Revert back naive in list OR implementation ([99e9ca2](https://github.com/cube-js/cube.js/commit/99e9ca293555911497a9d8d45d05255e845b47c8))
* **shared:** Value True is not valid for CUBEJS_SCHEDULED_REFRESH_TIMER ([99a5759](https://github.com/cube-js/cube.js/commit/99a5759e619824666b48c589a5c98c82c1817025))


### Features

* **cubestore:** Rebase to datafusion 2021-01-27 version ([#1930](https://github.com/cube-js/cube.js/issues/1930)) ([309ce8e](https://github.com/cube-js/cube.js/commit/309ce8edee0cc49f1e4dc0536f0ef593ceaa428f))





## [0.25.31](https://github.com/cube-js/cube.js/compare/v0.25.30...v0.25.31) (2021-01-28)


### Bug Fixes

* **@cubejs-client/core:** propagate time dimension to the drill down query ([#1911](https://github.com/cube-js/cube.js/issues/1911)) ([59701da](https://github.com/cube-js/cube.js/commit/59701dad6f6cb6d78954d18b309716a9d51aa6b7))
* **cubestore:** Adjust default memory usage ([04c4bc8](https://github.com/cube-js/cube.js/commit/04c4bc850a801e5833641a4904144bb5e9f36ff8))
* **cubestore:** Bring back WAL removal ([1b2bd40](https://github.com/cube-js/cube.js/commit/1b2bd40131314afbb4e024867666615b199c80d3))
* **cubestore:** Drop temporary files on CSV import ([ab0affb](https://github.com/cube-js/cube.js/commit/ab0affb6ceed719444b3e0df6f8f56439d5b36a7))
* **cubestore:** Error processing event DeletePartition: No such object ([0208234](https://github.com/cube-js/cube.js/commit/0208234ee1f5f76efb7c7d2db6eee448af2f097d))
* **cubestore:** index out of bounds: the len is 0 but the index is 18446744073709551615 ([21bb226](https://github.com/cube-js/cube.js/commit/21bb226f911236353ed8625a674fe1fe1f1b7f51))
* **cubestore:** Limit memory usage on compaction -- zero compaction threshold case ([#1895](https://github.com/cube-js/cube.js/issues/1895)) ([fb516f5](https://github.com/cube-js/cube.js/commit/fb516f5f04790f51a8e37f24eb875e00704b1954))
* **cubestore:** Support single partition compactions ([c5eac36](https://github.com/cube-js/cube.js/commit/c5eac3655be86cfdf5d9035a5c98d72025521459))


### Features

* Ability to specify dataSource from request ([e8fe83a](https://github.com/cube-js/cube.js/commit/e8fe83abacfd2a47ad440fa2d52f3bf78d7a8c72))
* Disable graceful shutdown by default ([#1903](https://github.com/cube-js/cube.js/issues/1903)) ([19e2f54](https://github.com/cube-js/cube.js/commit/19e2f5491ba8f8b3aa76762382da98400fb71a1b))





## [0.25.30](https://github.com/cube-js/cube.js/compare/v0.25.29...v0.25.30) (2021-01-26)


### Bug Fixes

* **cubestore:** add custom type 'bytes', a synonym for 'varbinary' ([#1890](https://github.com/cube-js/cube.js/issues/1890)) ([4efc291](https://github.com/cube-js/cube.js/commit/4efc2914a0f9f0c960bf9af00a56c5562ac02bd4))
* **shared:** 1st interval unexpected call on onDuplicatedStateResolved ([6265503](https://github.com/cube-js/cube.js/commit/62655036d337e2ca491d0bda4f7f1b98a6811c4c))


### Features

* **cubestore:** allow to import base64-encoded bytes in CSV ([#1891](https://github.com/cube-js/cube.js/issues/1891)) ([2f43afa](https://github.com/cube-js/cube.js/commit/2f43afaa3776fb70526196734b1f3e97b942770e))





## [0.25.29](https://github.com/cube-js/cube.js/compare/v0.25.28...v0.25.29) (2021-01-26)


### Bug Fixes

* **cubestore:** CSV import escape sequence ([a3e118e](https://github.com/cube-js/cube.js/commit/a3e118e7be072d0763a2f0aa1044350e0a4ddd90))
* **cubestore:** More CSV import escaping cases ([9419128](https://github.com/cube-js/cube.js/commit/9419128d2653405c51d60c8b79c4d07971b54e0f))
* **cubestore:** Support NULL values in CSV import ([529e5ac](https://github.com/cube-js/cube.js/commit/529e5ac9f3d31fb8b7962c9dced6a5d8dd94c26a))


### Features

* **cubestore:** CUBESTORE_WAL_SPLIT_THRESHOLD env variable ([0d7e550](https://github.com/cube-js/cube.js/commit/0d7e550d825c129f5b21a6963182faebaa882132))
* Improve logs for RefreshScheduler and too long execution ([d0f1f1b](https://github.com/cube-js/cube.js/commit/d0f1f1bbc32473452c763d22ff8ee728c74f6462))





## [0.25.28](https://github.com/cube-js/cube.js/compare/v0.25.27...v0.25.28) (2021-01-25)


### Bug Fixes

* dependency version resolution ([f314ec5](https://github.com/cube-js/cube.js/commit/f314ec54d15c4c01b9eca602f5587d0896bdca23))
* **cubestore:** merge() and cardinality() now work on empty inputs ([#1875](https://github.com/cube-js/cube.js/issues/1875)) ([0e35861](https://github.com/cube-js/cube.js/commit/0e358612a133cdd0004d5e03b47e963a8dc66df6))


### Features

* **cubestore:** HyperLogLog++ support for BigQuery ([#1872](https://github.com/cube-js/cube.js/issues/1872)) ([357ecef](https://github.com/cube-js/cube.js/commit/357eceffcf56f46634b4f7de7550cfbe77911c2d))





## [0.25.27](https://github.com/cube-js/cube.js/compare/v0.25.26...v0.25.27) (2021-01-25)


### Bug Fixes

* **mongobi-driver:** authSwitchHandler api is deprecated, please use new authPlugins api ([5ee9349](https://github.com/cube-js/cube.js/commit/5ee93497972f1cbd0436f0179c2959867e4b3101))


### Features

* **server:** Dont accept new request(s) during shutdown ([#1855](https://github.com/cube-js/cube.js/issues/1855)) ([78f8f0b](https://github.com/cube-js/cube.js/commit/78f8f0ba395f061c5acb9055c2a83c2d573b950c))





## [0.25.26](https://github.com/cube-js/cube.js/compare/v0.25.25...v0.25.26) (2021-01-25)


### Features

* BigQuery CSV pre-aggregation download support ([#1867](https://github.com/cube-js/cube.js/issues/1867)) ([5a2ea3f](https://github.com/cube-js/cube.js/commit/5a2ea3f27058a01bf08f697495c8ccce5abf9fa2))





## [0.25.25](https://github.com/cube-js/cube.js/compare/v0.25.24...v0.25.25) (2021-01-24)


### Bug Fixes

* **cubestore:** Ignore CUBEJS_DB_SSL env ([86f06f7](https://github.com/cube-js/cube.js/commit/86f06f7955b3d230c9398953fec76c2569460701))


### Features

* **cubestore:** Migrate to tokio v1.0 and implement GCS support ([#1864](https://github.com/cube-js/cube.js/issues/1864)) ([803efd2](https://github.com/cube-js/cube.js/commit/803efd2a36d08f604af2ee31f14ddfeb2abe9468))





## [0.25.24](https://github.com/cube-js/cube.js/compare/v0.25.23...v0.25.24) (2021-01-22)


### Bug Fixes

* Non default data source cache key and table schema queries are forwarded to the default data source ([2f7c672](https://github.com/cube-js/cube.js/commit/2f7c67292468da60faea284751bf8c71d2e051f5))
* Non default data source cache key and table schema queries are forwarded to the default data source: broken test ([#1856](https://github.com/cube-js/cube.js/issues/1856)) ([8aad3f5](https://github.com/cube-js/cube.js/commit/8aad3f52f476836df4f93c266af96f30ceb57131))





## [0.25.23](https://github.com/cube-js/cube.js/compare/v0.25.22...v0.25.23) (2021-01-22)


### Bug Fixes

* Map int2/4/8 to generic int type. fix [#1796](https://github.com/cube-js/cube.js/issues/1796) ([78e20eb](https://github.com/cube-js/cube.js/commit/78e20eb304eda3086cda7dbc4ea5d33ef877facb))
* **api-gateway:** Validate a case when chrono can return empty array ([#1848](https://github.com/cube-js/cube.js/issues/1848)) ([e7349f7](https://github.com/cube-js/cube.js/commit/e7349f7bd71800e51a9c1d7cefecc8783bd886d6))
* **cubestore:** Increase queue buffer to avoid lagging on wait ([2605bdf](https://github.com/cube-js/cube.js/commit/2605bdf69cd4c19b80a51ec0f526c8d1dffb9681))
* **cubestore:** Queue uploads and downloads to avoid reads on unfinished S3 streams ([b94eb26](https://github.com/cube-js/cube.js/commit/b94eb266b98b2cbec494882931df8f3fbb40882a))
* **cubestore:** Speed up S3 uploads ([d7062c8](https://github.com/cube-js/cube.js/commit/d7062c825c96412c43e36b3fd09a2f630396117c))


### Features

* **schema-compiler:** Move some parts to TS ([2ad0e2e](https://github.com/cube-js/cube.js/commit/2ad0e2e377fce52f4967fc73ae2486d4365f3ac4))





## [0.25.22](https://github.com/cube-js/cube.js/compare/v0.25.21...v0.25.22) (2021-01-21)


### Bug Fixes

* **cubestore:** Add curl as a dependency for certs ([d364fc4](https://github.com/cube-js/cube.js/commit/d364fc454f013667001a9932ebd1e894c5a4b5fc))
* **cubestore:** Try to fix Invalid Parquet file on worker nodes ([aab87c8](https://github.com/cube-js/cube.js/commit/aab87c85ddaca156d20895efd29785b673bc5e2d))
* **playground:** Create schema directory on changing env ([f99f6cc](https://github.com/cube-js/cube.js/commit/f99f6cc658ffdd9f2ec58dcbfa3b2be67ca67bf8))
* **server:** Unexpected kill on graceful shutdown ([fc99239](https://github.com/cube-js/cube.js/commit/fc992398037719e5d7cc56b35f5e52e59d7c71f2))
* **server-core:** Clear refresh uncaughtException for DevServer ([1ea4882](https://github.com/cube-js/cube.js/commit/1ea4882c8afd8b13f7637bb641120dc104096515))


### Features

* Log warnings from createCancelableInterval ([44d09c4](https://github.com/cube-js/cube.js/commit/44d09c44da6ddfa845dd457bb766172698c8f334))
* **@cubejs-client/playground:** Database connection wizard ([#1671](https://github.com/cube-js/cube.js/issues/1671)) ([ba30883](https://github.com/cube-js/cube.js/commit/ba30883617c806c9f19ed6c879d0b0c2d656aae1))
* **cubestore:** Add column type for HLL ([#1827](https://github.com/cube-js/cube.js/issues/1827)) ([df97052](https://github.com/cube-js/cube.js/commit/df970523c5413a171578e14abdb792ce4c260fbe))
* **server:** Guard multiple restart in same time ([45f19b8](https://github.com/cube-js/cube.js/commit/45f19b84cd2eb2818e5053a4d5ae025b8aa2497c))





## [0.25.21](https://github.com/cube-js/cube.js/compare/v0.25.20...v0.25.21) (2021-01-19)


### Bug Fixes

* **@cubejs-backend/api-gateway:** readiness fix ([#1791](https://github.com/cube-js/cube.js/issues/1791)) ([d5dad60](https://github.com/cube-js/cube.js/commit/d5dad60e1dda655d67d5d8df4f4d6ee4345dbe42))
* **@cubejs-backend/query-orchestrator:** prevent generic pool infinite loop ([#1793](https://github.com/cube-js/cube.js/issues/1793)) ([d4129c4](https://github.com/cube-js/cube.js/commit/d4129c4d71b4afa66f62ae5d9666fcd9a08d9187))
* **@cubejs-client/playground:** avoid styles override ([4bdae02](https://github.com/cube-js/cube.js/commit/4bdae024d3d866acebb054e01106ed621a51a445))


### Features

* **cubestore:** Cluster support ([4846080](https://github.com/cube-js/cube.js/commit/48460807c9228a0b4db9193e6b36b5895a5d57b8))
* **cubestore:** S3 sub path support ([0cabd4c](https://github.com/cube-js/cube.js/commit/0cabd4c0820af8c4e4dbd338588bd49274e294c2))
* **schema-compiler:** Initial support for TS ([5926067](https://github.com/cube-js/cube.js/commit/5926067bf5314c7cbddfe59f26dd0ae3b8b60293))





## [0.25.20](https://github.com/cube-js/cube.js/compare/v0.25.19...v0.25.20) (2021-01-15)


### Bug Fixes

* Remove unnecessary `SELECT 1` during scheduled refresh. Fixes [#1592](https://github.com/cube-js/cube.js/issues/1592) ([#1786](https://github.com/cube-js/cube.js/issues/1786)) ([66f9d91](https://github.com/cube-js/cube.js/commit/66f9d91d12b1853b69903475af8338bfa586026b))





## [0.25.19](https://github.com/cube-js/cube.js/compare/v0.25.18...v0.25.19) (2021-01-14)


### Bug Fixes

* Do not renew historical refresh keys during scheduled refresh ([e5fbb12](https://github.com/cube-js/cube.js/commit/e5fbb120d5e848468999de59ba536b95be2e67e9))


### Features

* **cubestore:** Improve support for the binary data type ([#1759](https://github.com/cube-js/cube.js/issues/1759)) ([925f813](https://github.com/cube-js/cube.js/commit/925f81368494e1128eadd097462814d9a87493f8))





## [0.25.18](https://github.com/cube-js/cube.js/compare/v0.25.17...v0.25.18) (2021-01-14)


### Bug Fixes

* **@cubejs-client/playground:** ng number, crash ([#1770](https://github.com/cube-js/cube.js/issues/1770)) ([a2bce37](https://github.com/cube-js/cube.js/commit/a2bce37db86efe521410ddf7f30030c8c65b210b))
* **server:** Wrong path to typings. fix [#1766](https://github.com/cube-js/cube.js/issues/1766) ([59d279d](https://github.com/cube-js/cube.js/commit/59d279deef446fdfc6ebdd40c3bb9817b618fe88))


### Features

* **server:** Kill Cube.js if it's stuck in gracefull shutdown ([0874de8](https://github.com/cube-js/cube.js/commit/0874de8a1b7216d783d947914e2396b35f17d130))





## [0.25.17](https://github.com/cube-js/cube.js/compare/v0.25.16...v0.25.17) (2021-01-13)


### Reverts

* Revert "feat(server): Throw an exception when env file is not correct" (#1763) ([f899786](https://github.com/cube-js/cube.js/commit/f899786a424a56326bdff9a6c4c87bb160f994d6)), closes [#1763](https://github.com/cube-js/cube.js/issues/1763)





## [0.25.16](https://github.com/cube-js/cube.js/compare/v0.25.15...v0.25.16) (2021-01-13)


### Bug Fixes

* **cli:** Broken jdbc installation ([b37a134](https://github.com/cube-js/cube.js/commit/b37a134f121fd933bfb793462563486ff85273fe))
* **server:** configuration reload should overrite old env variables ([bbb5c4a](https://github.com/cube-js/cube.js/commit/bbb5c4aad249a8f62e0edccf29871abcd95feca6))
* **snowflake-driver:** Handle null values for numbers, dates. fix [#1741](https://github.com/cube-js/cube.js/issues/1741) ([51c2bb2](https://github.com/cube-js/cube.js/commit/51c2bb21d4d46daac89f21921f5c61982ab6547f))
* Pass dbType in DialectContext for dialectFactory ([#1756](https://github.com/cube-js/cube.js/issues/1756)) ([5cf88bf](https://github.com/cube-js/cube.js/commit/5cf88bf1eaaed1c70223631a76e3de77ecae46b7)), closes [#1728](https://github.com/cube-js/cube.js/issues/1728)


### Features

* **cubestore:** Filter mirroring push down optimization ([49685d3](https://github.com/cube-js/cube.js/commit/49685d3ca3a20b561d6fbfdb66a6b8cdd7b6b755))
* **server:** Throw an exception when env file is not correct ([abff7fc](https://github.com/cube-js/cube.js/commit/abff7fcc11b7c226346ecb9524b98548fdc4fe09))





## [0.25.15](https://github.com/cube-js/cube.js/compare/v0.25.14...v0.25.15) (2021-01-12)


### Bug Fixes

* Ensure agent events are delivered with a 50k backlog ([bf0b9ec](https://github.com/cube-js/cube.js/commit/bf0b9ec9f75b5e5f996fd4da855371ef6cd641f2))


### Features

* **@cubejs-client/playground:** display slow query warning ([#1649](https://github.com/cube-js/cube.js/issues/1649)) ([ce33f88](https://github.com/cube-js/cube.js/commit/ce33f8849b96ac25dd6f242b61f81e29600f511a))
* introduce graceful shutdown ([#1683](https://github.com/cube-js/cube.js/issues/1683)) ([118232f](https://github.com/cube-js/cube.js/commit/118232f56b6c66b7dff6ed11e914ccc107a25881))





## [0.25.14](https://github.com/cube-js/cube.js/compare/v0.25.13...v0.25.14) (2021-01-11)


### Bug Fixes

* **@cubejs-client/react:** useCubeQuery - clear resultSet on exception ([#1734](https://github.com/cube-js/cube.js/issues/1734)) ([a5d19ae](https://github.com/cube-js/cube.js/commit/a5d19aecffc6a613f6e0f0d9346143c4f2e335be))
* **cubestore:** filter pushdown optimization for aliased tables doesn't work ([decfa3a](https://github.com/cube-js/cube.js/commit/decfa3a9110fb4c82d125793838196eb0e0ac9a8))
* **cubestore:** Fix parquet-format dependency as new one isn't compatible with current arrow version ([f236314](https://github.com/cube-js/cube.js/commit/f2363147796f6724c3f53d6f62527a6ec93f8fa0))
* **cubestore:** Invalid argument error: Unable to get field named during merge resort ([031f4fe](https://github.com/cube-js/cube.js/commit/031f4fec492e821a5ef799e461785024aad09a6f))
* **cubestore:** Log 0.4.12 dependency is broken ([a484b12](https://github.com/cube-js/cube.js/commit/a484b12d21c7c445ec94687950efb29aba205ebf))
* **cubestore:** Merge sort early exit ([ddb292f](https://github.com/cube-js/cube.js/commit/ddb292fecc0db7f0a84199c9f932198d145a3170))
* **cubestore:** Merge sort seg fault on empty batch ([4eb1f28](https://github.com/cube-js/cube.js/commit/4eb1f2872cea59f5c5352c28e5980d7fb276d98d))
* **cubestore:** Remove debug output ([8706798](https://github.com/cube-js/cube.js/commit/8706798dc441895b3718167f4eac82e29b319298))
* **cubestore:** Union merge sort support ([8cd3994](https://github.com/cube-js/cube.js/commit/8cd3994bbaafdaeb2c1ccc77c6d786ad7b85c987))
* **gateway:** Allow healthchecks to be requested without auth ([95c0c57](https://github.com/cube-js/cube.js/commit/95c0c57d739e6ce46de958883d7dbfe04616a7a0))


### Features

* **cubestore:** Add CUBESTORE_DATA_DIR env variable ([3571916](https://github.com/cube-js/cube.js/commit/3571916c1e29ba84cd61f83aeb6611632a78b176))
* **cubestore:** Float column type support ([f427598](https://github.com/cube-js/cube.js/commit/f4275985fdfc0679b9ba89d86f7586b8c814d9dc))
* **cubestore:** Merge resort implementation to support three tables merge joins ([3fa675b](https://github.com/cube-js/cube.js/commit/3fa675bf9d7109c58847fe93219574e0cf287483))
* **docker:** Upgrade Node.js to Node v12.20.1 (security release) ([097a11a](https://github.com/cube-js/cube.js/commit/097a11a81402f26c90441d93bcdd8421f89bf2e8))





## [0.25.13](https://github.com/cube-js/cube.js/compare/v0.25.12...v0.25.13) (2021-01-07)


### Bug Fixes

* Guard from `undefined` dataSource in queue key ([6ae1fd6](https://github.com/cube-js/cube.js/commit/6ae1fd60a1e67bc73c0630b7de36b598397ce22b))
* **cubestore:** Root Cargo.toml isn't used for docker build ([8030fe3](https://github.com/cube-js/cube.js/commit/8030fe3796acc69e6dcd88c728430007c91dded6))
* **cubestore:** Set default scale to 5 for floats ([98d85eb](https://github.com/cube-js/cube.js/commit/98d85eb641e77225267a5e63351a4d72cf1c9531))
* **cubestore:** Support Utf8 to Boolean cast ([7ac9892](https://github.com/cube-js/cube.js/commit/7ac98921bb6c9999e1b59499fefb9a68578513fd))
* **cubestore:** Support Utf8 to Int64Decimal cast ([c523b46](https://github.com/cube-js/cube.js/commit/c523b4683c747d0cfcf9cc32c2319e83d56e7758))
* Reduce agent event queue on network failures ([548fb9a](https://github.com/cube-js/cube.js/commit/548fb9a23fe5fafa9d54c92c1d9425b83fafffbe))


### Features

* **cubestore:** Drop unused chunks and partitions after compaction and repartition ([94895a2](https://github.com/cube-js/cube.js/commit/94895a20bff4c6e2932e547f6c49fa5624644098))
* **cubestore:** Float with exp number support ([6e92c55](https://github.com/cube-js/cube.js/commit/6e92c5555efc8f76722994b2988d98850f9d10e9))





## [0.25.12](https://github.com/cube-js/cube.js/compare/v0.25.11...v0.25.12) (2021-01-05)


### Bug Fixes

* **@cubejs-client/react:** updated peer dependency version ([442a979](https://github.com/cube-js/cube.js/commit/442a979e9d5509ffcb71e48d42a4e4944eae98e1))
* **cubestore:** Join aliasing fails after rebase ([67ffd4d](https://github.com/cube-js/cube.js/commit/67ffd4df087d6a27855ab3cbe334125f3ff43293))


### Features

* **cubestore:** Distribute unions across workers the same way as partitions ([52f8a77](https://github.com/cube-js/cube.js/commit/52f8a771cc97d23e86e9e00a6fe7e5d5f291bda9))





## [0.25.11](https://github.com/cube-js/cube.js/compare/v0.25.10...v0.25.11) (2021-01-04)


### Bug Fixes

* **cubestore:** File not found if upstream mounted as a network volume ([68822ec](https://github.com/cube-js/cube.js/commit/68822ec8e0f2e7fe92ae80af1cb1b52cbbc22a61))
* **cubestore:** Fix write metastore locking ([cbbacce](https://github.com/cube-js/cube.js/commit/cbbacce44c4f8af734095d985d834071fb2f8b24))
* **cubestore:** Handle corrupted log files and discard them with error ([00a1c1a](https://github.com/cube-js/cube.js/commit/00a1c1a532be8cbb20a908c4bdb0e4f5eb802e71))
* **cubestore:** Handle corrupted upstream metastore ([d547677](https://github.com/cube-js/cube.js/commit/d547677c277c2233351266add966a2b019c38e3c))
* **cubestore:** Index repairs ([d5dc4cf](https://github.com/cube-js/cube.js/commit/d5dc4cf4f2313d1462a5d67cb5b7b7009680003a))
* **cubestore:** Set default worker pool timeout to 2 minutes ([139c8f6](https://github.com/cube-js/cube.js/commit/139c8f6844c9b923855a1d0a235aefd58288fd14))
* Declare Add missing externalQueueOptions for QueryCacheOptions ([563fcdc](https://github.com/cube-js/cube.js/commit/563fcdcb943622ad8ca391182652f2eb27000079))


### Features

* **cubestore:** Rebase arrow to 2020-01-02 version ([3cbb46d](https://github.com/cube-js/cube.js/commit/3cbb46d883445e2fbfb261d182e5cdaa6871bf2c))
* **cubestore:** Three tables join support ([b776398](https://github.com/cube-js/cube.js/commit/b776398ba45bc314f12a15a2f0861d5b01dcb90a))





## [0.25.10](https://github.com/cube-js/cube.js/compare/v0.25.9...v0.25.10) (2020-12-31)


### Bug Fixes

* **@cubejs-backend/cubestore-driver:** 2k batch size upload ([d1be31e](https://github.com/cube-js/cube.js/commit/d1be31e8adabd022a2be518405cbf403870b7f18))





## [0.25.9](https://github.com/cube-js/cube.js/compare/v0.25.8...v0.25.9) (2020-12-31)


### Bug Fixes

* **@cubejs-backend/cubestore-driver:** 10k batch size upload ([d863a10](https://github.com/cube-js/cube.js/commit/d863a10b1b025577ff302b73de15ff9d9f2fb9a6))





## [0.25.8](https://github.com/cube-js/cube.js/compare/v0.25.7...v0.25.8) (2020-12-31)


### Features

* **@cubejs-backend/mysql-driver:** More int and text types support for read only pre-aggregations ([5bb2a4f](https://github.com/cube-js/cube.js/commit/5bb2a4f40efa9b602a48f594052be0eb9484d31a))





## [0.25.7](https://github.com/cube-js/cube.js/compare/v0.25.6...v0.25.7) (2020-12-30)


### Bug Fixes

* **@cubejs-backend/mysql-driver:** Handle mediumint(9) type ([3d135b1](https://github.com/cube-js/cube.js/commit/3d135b16eee8fa4c35c584c28b8f18e47539fa54))





## [0.25.6](https://github.com/cube-js/cube.js/compare/v0.25.5...v0.25.6) (2020-12-30)


### Bug Fixes

* Allow CUBEJS_SCHEDULED_REFRESH_TIMER to be boolean ([4e80645](https://github.com/cube-js/cube.js/commit/4e80645259cbd3a5ad7d92f3d07a1d5a58a6c5ef))





## [0.25.5](https://github.com/cube-js/cube.js/compare/v0.25.4...v0.25.5) (2020-12-30)


### Features

* Allow to specify socket for PORT/TLS_PORT, fix [#1681](https://github.com/cube-js/cube.js/issues/1681) ([b9c4669](https://github.com/cube-js/cube.js/commit/b9c466987ffa41f31fa8b3bda88432175e57cd86))





## [0.25.4](https://github.com/cube-js/cube.js/compare/v0.25.3...v0.25.4) (2020-12-30)


### Bug Fixes

* **cubestore:** `next_table_seq` sanity check until transactions arrive ([f9b65ea](https://github.com/cube-js/cube.js/commit/f9b65eac837d102afb2b280a124dbe341a4cc058))
* **cubestore:** Atomic WAL activation ([0c64e69](https://github.com/cube-js/cube.js/commit/0c64e698253921973a7452cf2b0184c1a27553ef))
* **cubestore:** Migrate to memory sequence tracking until transactions arrive ([7308a63](https://github.com/cube-js/cube.js/commit/7308a632ddba43c9333b098eca34f71686922e4d))
* **cubestore:** Move to RocksDB Snapshot reading to ensure strong metastore read consistency ([68dac72](https://github.com/cube-js/cube.js/commit/68dac72cf6adff920c05c118cf297986e943a7f3))


### Features

* **@cubejs-backend/cubestore-driver:** Increase upload batch size to 50k ([1bebc1d](https://github.com/cube-js/cube.js/commit/1bebc1dd09845e547abea65dd24ace56a5cea40b))
* **server-core:** Compatibility shim, for legacy imports ([2116799](https://github.com/cube-js/cube.js/commit/21167995045d7a5c0d1056dc034b14ec18205277))
* **server-core:** Initial support for TS ([df45216](https://github.com/cube-js/cube.js/commit/df452164d8282074f926a980cbfe3284817e85a6))
* **server-core:** Introduce CUBEJS_PRE_AGGREGATIONS_SCHEMA, use dev_preaggregations/prod_preaggregations by default ([e5bdf3d](https://github.com/cube-js/cube.js/commit/e5bdf3dfbd28d5e1c1e775c554c275304a0941f3))
* **server-core:** Move to TS ([d7b7431](https://github.com/cube-js/cube.js/commit/d7b743156751dbc2202a7138bc7603dc6861f001))





## [0.25.3](https://github.com/cube-js/cube.js/compare/v0.25.2...v0.25.3) (2020-12-28)


### Bug Fixes

* `CUBEJS_SCHEDULED_REFRESH_CONCURRENCY` doesn't work ([1f6b505](https://github.com/cube-js/cube.js/commit/1f6b5054b1327547d86004fd95941b0f3099ca68))





## [0.25.2](https://github.com/cube-js/cube.js/compare/v0.25.1...v0.25.2) (2020-12-27)


### Bug Fixes

* **@cubejs-backend/query-orchestrator:** Throw an exception on empty pre-agg in readOnly mode, refs [#1597](https://github.com/cube-js/cube.js/issues/1597) ([17d5fdb](https://github.com/cube-js/cube.js/commit/17d5fdb82e0ce06d55e438913e32952f32db7923))
* **@cubejs-backend/schema-compiler:** MySQL double timezone conversion ([e5f1490](https://github.com/cube-js/cube.js/commit/e5f1490a897df4f0eac062dfabbc20aca2ea2f5b))
* **@cubejs-client/react:** prevent state updates on unmounted components ([#1684](https://github.com/cube-js/cube.js/issues/1684)) ([4f3796c](https://github.com/cube-js/cube.js/commit/4f3796c9f402a7b8b54311a08c632270be8e34c3))
* **api-gateway:** /readyz /healthz - correct response for partial outage ([1e5bdf5](https://github.com/cube-js/cube.js/commit/1e5bdf556f6f14698945a72c0332e0f6982ba8e7))


### Features

* Ability to set timeouts for polling in BigQuery/Athena ([#1675](https://github.com/cube-js/cube.js/issues/1675)) ([dc944b1](https://github.com/cube-js/cube.js/commit/dc944b1aaacc69dd74a9d9d31ceaf43e16d37ccd)), closes [#1672](https://github.com/cube-js/cube.js/issues/1672)
* Concurrency controls for scheduled refresh ([2132f0d](https://github.com/cube-js/cube.js/commit/2132f0dc7bb3aab994d559ea42dd0b0a934b1310))
* **api-gateway:** Support schema inside Authorization header, fix [#1297](https://github.com/cube-js/cube.js/issues/1297) ([2549004](https://github.com/cube-js/cube.js/commit/25490048661738e273629c73368ca03f821ee096))
* **cubestore:** Default decimal scale ([a79f98b](https://github.com/cube-js/cube.js/commit/a79f98b08c9be0688c0cea82b881230518575270))





## [0.25.1](https://github.com/cube-js/cube.js/compare/v0.25.0...v0.25.1) (2020-12-24)


### Bug Fixes

* **@cubejs-backend/schema-compiler:** Better error message for join member resolutions ([30cc3ab](https://github.com/cube-js/cube.js/commit/30cc3abc4e8c91e8d95b8794f892e1d1f2152798))
* **@cubejs-backend/schema-compiler:** Error: TypeError: R.eq is not a function -- existing joins in rollup support ([5f62aae](https://github.com/cube-js/cube.js/commit/5f62aaee88b7ecc281437601410b10ef04d7bbf3))
* **@cubejs-client/playground:** propagate cubejs token ([#1669](https://github.com/cube-js/cube.js/issues/1669)) ([f1fb563](https://github.com/cube-js/cube.js/commit/f1fb5634fa62b2f78cf6d8365c4a98094e114f6c))
* **cubestore:** Merge join empty side fixes ([5e65c3e](https://github.com/cube-js/cube.js/commit/5e65c3e251c7f9d7329a601bd467a4ef3b043463))
* **cubestore:** Non atomic primary key allocation conflicts ([073ac8c](https://github.com/cube-js/cube.js/commit/073ac8ce69cc294a15a5e59b11b2915a755ca81b))
* **cubestore:** Pass join on sort conditions explicitly. Avoid incorrectly selected sort keys. ([b6a2e4a](https://github.com/cube-js/cube.js/commit/b6a2e4a457bd38d40d8819443a2fc4fddf7465db))
* **playground:** Use basePath from configuration, fix [#377](https://github.com/cube-js/cube.js/issues/377) ([c94cbce](https://github.com/cube-js/cube.js/commit/c94cbce50e31617086ec458f934fefaf779b76f4))


### Features

* **@cubejs-backend/dremio-driver:** Add HTTPS support for Dremio ([#1666](https://github.com/cube-js/cube.js/issues/1666)), Thanks [@chipblox](https://github.com/chipblox) ([1143e9c](https://github.com/cube-js/cube.js/commit/1143e9cbdb78059a93e1419feff80c34ee29bdbf))
* **athena-driver:** Support readOnly option, add typings ([a519cb8](https://github.com/cube-js/cube.js/commit/a519cb880be2bb2b872c56b092f1273291fbd397))
* **elasticsearch-driver:** Support CUBEJS_DB_ELASTIC_QUERY_FORMAT, Thanks [@dylman79](https://github.com/dylman79) ([a7460f5](https://github.com/cube-js/cube.js/commit/a7460f5d45dc7e9d96b65f6cc36df810a5b9312e))





# [0.25.0](https://github.com/cube-js/cube.js/compare/v0.24.15...v0.25.0) (2020-12-21)


### Bug Fixes

* **@cubejs-client/playground:** chart renderer load ([#1658](https://github.com/cube-js/cube.js/issues/1658)) ([bbce716](https://github.com/cube-js/cube.js/commit/bbce71697a0d4c33a2d0bb277fd039cc5925f4ca))
* getQueryStage throws undefined is not a function ([0de1603](https://github.com/cube-js/cube.js/commit/0de1603293fc918c0da8ff8bd514b49f14de51d8))


### Features

* Allow cross data source joins ([a58336e](https://github.com/cube-js/cube.js/commit/a58336e3840f8ac02d83de43ec7661419bceb71c))
* Allow cross data source joins: Serverless support ([034cdc8](https://github.com/cube-js/cube.js/commit/034cdc8dbf8907988df0f999fd115b8acdb4990f))





## [0.24.15](https://github.com/cube-js/cube.js/compare/v0.24.14...v0.24.15) (2020-12-20)


### Bug Fixes

* **cubestore:** Atomic chunks repartition ([b1a23da](https://github.com/cube-js/cube.js/commit/b1a23dac8b82e2ab997ec060109948c355e37764))
* **cubestore:** Atomic index snapshotting ([8a50f34](https://github.com/cube-js/cube.js/commit/8a50f34c22db7cc9ddd13c4aa33c864a90e29b4f))


### Features

* Allow joins between data sources for external queries ([1dbfe2c](https://github.com/cube-js/cube.js/commit/1dbfe2cdc1b1904ce8567a7599b24e660c5047f3))
* **cubestore:** Support GROUP BY DECIMAL ([#1652](https://github.com/cube-js/cube.js/issues/1652)) ([4ad97dc](https://github.com/cube-js/cube.js/commit/4ad97dc8ae618fccb98020b50e335c5e8cf47459))





## [0.24.14](https://github.com/cube-js/cube.js/compare/v0.24.13...v0.24.14) (2020-12-19)


### Bug Fixes

* Rollup match results for rollupJoin ([0279b13](https://github.com/cube-js/cube.js/commit/0279b13a8696643ad95c374062ea059cea3b890b))
* **api-gateway:** Fix broken POST /v1/dry-run ([fa0cae0](https://github.com/cube-js/cube.js/commit/fa0cae01fa471e01d88d7db6f1d17046392167d0))


### Features

* Add HTTP Post to cubejs client core ([#1608](https://github.com/cube-js/cube.js/issues/1608)). Thanks to [@mnifakram](https://github.com/mnifakram)! ([1ebd6a0](https://github.com/cube-js/cube.js/commit/1ebd6a04ac97b31c6a51ef63bb1d4c040e524190))





## [0.24.13](https://github.com/cube-js/cube.js/compare/v0.24.12...v0.24.13) (2020-12-18)


### Bug Fixes

* **@cubejs-client/react:** reset the error on subsequent calls ([#1641](https://github.com/cube-js/cube.js/issues/1641)) ([2a65dae](https://github.com/cube-js/cube.js/commit/2a65dae8d1f327f47d387ff8bbf52193ebb7bf53))


### Features

* **api-gateway:** Dont run all health checks, when the one is down ([f5957f4](https://github.com/cube-js/cube.js/commit/f5957f4824372d5e22de25a23a3a1e78445df5d0))
* Rollup join implementation ([#1637](https://github.com/cube-js/cube.js/issues/1637)) ([bffd220](https://github.com/cube-js/cube.js/commit/bffd22095f58369f3d52474283951b4844657f2b))





## [0.24.11](https://github.com/cube-js/cube.js/compare/v0.24.10...v0.24.11) (2020-12-17)

**Note:** Version bump only for package cubejs





## [0.24.9](https://github.com/cube-js/cube.js/compare/v0.24.8...v0.24.9) (2020-12-16)


### Bug Fixes

* **@cubejs-backend/mysql-driver:** Revert back test on borrow with database pool error logging. ([2cdaf40](https://github.com/cube-js/cube.js/commit/2cdaf406a7d99116849f60e00e1b1bc25605e0d3))
* **docker:** Drop usage of VOLUME to protected unexpected behavior ([e3f20cd](https://github.com/cube-js/cube.js/commit/e3f20cdad7b72cb45b7a4eee5452dde918539df7))
* Warning about absolute import ([5f228bc](https://github.com/cube-js/cube.js/commit/5f228bc5e654ab9a4efba458b5c31614ac44a5aa))


### Features

* **@cubejs-client/playground:** Angular chart code generation support in Playground ([#1519](https://github.com/cube-js/cube.js/issues/1519)) ([4690e11](https://github.com/cube-js/cube.js/commit/4690e11f417ff65fea8426360f3f5a2b3acd2792)), closes [#1515](https://github.com/cube-js/cube.js/issues/1515) [#1612](https://github.com/cube-js/cube.js/issues/1612)
* **@cubejs-client/react:** dry run hook ([#1612](https://github.com/cube-js/cube.js/issues/1612)) ([9aea035](https://github.com/cube-js/cube.js/commit/9aea03556ae61f443598ed587538e60239a3be2d))





## [0.24.8](https://github.com/cube-js/cube.js/compare/v0.24.7...v0.24.8) (2020-12-15)


### Bug Fixes

* **@cubejs-backend/mysql-driver:** Use decimal(38,10) for external pre-aggregations, fix [#1563](https://github.com/cube-js/cube.js/issues/1563) ([3aec549](https://github.com/cube-js/cube.js/commit/3aec549f0344590185618427b854eef863d24287))
* **@cubejs-backend/schema-compiler:** CubeCheckDuplicatePropTranspiler - dont crash on not StringLiterals ([#1582](https://github.com/cube-js/cube.js/issues/1582)) ([a705a2e](https://github.com/cube-js/cube.js/commit/a705a2ed6885d5c08e654945682054a1421dfb51))
* **@cubejs-client/playground:** fix color name and change font to Inter ([010a106](https://github.com/cube-js/cube.js/commit/010a106442dfc39a2027733d5087ac6b7e2cdcb3))


### Features

* **@cubejs-backend/query-orchestrator:** Introduce AsyncRedisClient type ([728110e](https://github.com/cube-js/cube.js/commit/728110ed0ffe5697bd5e47e3920bf2e5377a0ffd))
* **@cubejs-backend/query-orchestrator:** Migrate createRedisClient to TS ([78e8422](https://github.com/cube-js/cube.js/commit/78e8422937e79457fdcec70535225bc9ccecfce8))
* **@cubejs-backend/query-orchestrator:** Move RedisPool to TS, export RedisPoolOptions ([8e8abde](https://github.com/cube-js/cube.js/commit/8e8abde85b9fa821d21f33fc286cfb2cc56891e4))
* **@cubejs-backend/query-orchestrator:** Set redis pool options from server config ([c1270d4](https://github.com/cube-js/cube.js/commit/c1270d4cfdc243b230ade0cb3a4c59171db70d20))
* **@cubejs-client/core:** Added pivotConfig option to alias series with a prefix ([#1594](https://github.com/cube-js/cube.js/issues/1594)). Thanks to @MattGson! ([a3342f7](https://github.com/cube-js/cube.js/commit/a3342f7fd0389ce3ad0bc62686c0e787de25f411))
* Set CUBEJS_SCHEDULED_REFRESH_TIMER default value to 30 seconds ([f69324c](https://github.com/cube-js/cube.js/commit/f69324c60ee4adfdfded67dddedab113fb5fdb95))





## [0.24.7](https://github.com/cube-js/cube.js/compare/v0.24.6...v0.24.7) (2020-12-14)


### Bug Fixes

* **@cubejs-backend/mysql-driver:** Do not validate connections in pool and expose all errors to clients ([b62f27f](https://github.com/cube-js/cube.js/commit/b62f27fb8319c5ea161d601586bd5cf0e3e940dd))





## [0.24.6](https://github.com/cube-js/cube.js/compare/v0.24.5...v0.24.6) (2020-12-13)


### Bug Fixes

* **@cubejs-backend/api-gateway:** SubscriptionServer - support dry-run ([#1581](https://github.com/cube-js/cube.js/issues/1581)) ([43fbc20](https://github.com/cube-js/cube.js/commit/43fbc20a66b4aad335ba198960cc1f626fb909a4))
* **cubejs-cli:** deploy --upload-env - filter CUBEJS_DEV_MODE ([81a835f](https://github.com/cube-js/cube.js/commit/81a835f033e44b945d0c3a6115491e337a7eddfd))


### Features

* **cubestore:** Explicit index selection for join ([290cab8](https://github.com/cube-js/cube.js/commit/290cab82586084ed464b50ecdc9ad8bfe1461c9e))
* Move index creation orchestration to the driver: allow to control drivers when to create indexes ([2a94e71](https://github.com/cube-js/cube.js/commit/2a94e710a89954ecedf4aa6f76b89578138e7aff))
* **cubestore:** String implicit casts. CREATE INDEX support. ([d42c199](https://github.com/cube-js/cube.js/commit/d42c1995f675c437812196c30d2ba08cd35f273a))





## [0.24.5](https://github.com/cube-js/cube.js/compare/v0.24.4...v0.24.5) (2020-12-09)


### Bug Fixes

* **@cubejs-backend/api-gateway:** Export UserError/CubejsHandlerError ([#1540](https://github.com/cube-js/cube.js/issues/1540)) ([20124ba](https://github.com/cube-js/cube.js/commit/20124ba26f8330801fd23e33c7c36a2005ae98e8))
* **@cubejs-client/playground:** fix user select on tab content ([7a0e4ef](https://github.com/cube-js/cube.js/commit/7a0e4ef10fb42597a402c69004c0d94178ce62ed))
* **cubestore:** Compaction fixes ([7441a26](https://github.com/cube-js/cube.js/commit/7441a267d382c126b9e567f59c2b06aed2ca34a5))
* **cubestore:** Partition range gap fix ([3610b61](https://github.com/cube-js/cube.js/commit/3610b612009016431859bdf18a3760ba029e8613))


### Features

* **@cubejs-backend/bigquery-driver:** Allow to make BigQueryDriver as readOnly, fix [#1028](https://github.com/cube-js/cube.js/issues/1028) ([d9395f6](https://github.com/cube-js/cube.js/commit/d9395f6df4e896c1b987ff5dfbf741829e3b51df))
* **@cubejs-backend/mysql-driver:** CAST all time dimensions with granularities to DATETIME in order to provide typing for rollup downloads. Add mediumtext and mediumint generic type conversions. ([3d8cb37](https://github.com/cube-js/cube.js/commit/3d8cb37d03716cd2768a0986643495e4a844cb8d))
* **cubejs-cli:** improve DX for docker ([#1457](https://github.com/cube-js/cube.js/issues/1457)) ([72ad782](https://github.com/cube-js/cube.js/commit/72ad782090c52e677b9e51e43818f1dca40db791))
* **cubestore:** CUBESTORE_PORT env variable ([11e36a7](https://github.com/cube-js/cube.js/commit/11e36a726b930a1952eb917868c93078e1a9308e))
* **cubestore:** IN Implementation ([945d8bc](https://github.com/cube-js/cube.js/commit/945d8bc3728b3ab462e7448b13d92d65d1581ac8))





## [0.24.4](https://github.com/cube-js/cube.js/compare/v0.24.3...v0.24.4) (2020-12-07)


### Bug Fixes

* **@cubejs-backend/server:** Versions inside error message ([1a8cc4f](https://github.com/cube-js/cube.js/commit/1a8cc4f9ec15c18744c1541499552fa2133484ac))
* **@cubejs-backend/server-core:** Allow to pass externalRefresh inside preAggregationsOptions, fix [#1524](https://github.com/cube-js/cube.js/issues/1524) ([a6959c9](https://github.com/cube-js/cube.js/commit/a6959c9f47d3751bdc6b5d132e858d55107d9a50))
* **@cubejs-client/playground:** always show scrollbars in menu if content is too big ([5e22a3a](https://github.com/cube-js/cube.js/commit/5e22a3a179fa38bdbd539ec00b09f2ca0e89b8b9))
* **cubestore:** Merge sort propagation fixes ([35125ad](https://github.com/cube-js/cube.js/commit/35125ad58296cef5a038dfce27a95941487c9ab0))
* **docker:** Add sqlite driver to built-in drivers ([3b7b0f7](https://github.com/cube-js/cube.js/commit/3b7b0f74a3474a561481fac80cb5bc4b9c8450c9))
* **docker:** Use latest snowflake driver ([f607ed0](https://github.com/cube-js/cube.js/commit/f607ed01366981f3f1b53ab0782cca867ed5d50c))


### Features

* **@cubejs-backend/api-gateway:** Migrate some parts to TS ([c1166d7](https://github.com/cube-js/cube.js/commit/c1166d744ccd562db492e5dedd01eab63e07bfd4))
* **@cubejs-backend/api-gateway:** Migrate to TS initial ([1edef6d](https://github.com/cube-js/cube.js/commit/1edef6d269fd1877f0bfcdcf17d2f780abd4404c))
* **@cubejs-backend/postgres-driver:** Support CUBEJS_DB_SSL_SERVERNAME ([f044372](https://github.com/cube-js/cube.js/commit/f04437236ca78cb23ef69f2a5de6be60006f2464))
* Ability to load SSL keys from FS ([#1512](https://github.com/cube-js/cube.js/issues/1512)) ([71da5bb](https://github.com/cube-js/cube.js/commit/71da5bb529294fabd92b3a914b1e8bceb464643c))
* **cubestore:** Decimal support ([6bdc68b](https://github.com/cube-js/cube.js/commit/6bdc68b4de96a050306044cb61e337961c76d898))
* **cubestore:** Left join support ([9d1fd09](https://github.com/cube-js/cube.js/commit/9d1fd0996dcb4838ff848d1905955d82132f1338))
* **cubestore:** Mediumint support ([f98540b](https://github.com/cube-js/cube.js/commit/f98540bb0db705ea53e5fb73dd242338c9145adc))





## [0.24.3](https://github.com/cube-js/cube.js/compare/v0.24.2...v0.24.3) (2020-12-01)


### Bug Fixes

* **cubestore:** Merge join support: not implemented: Merge join is not supported for data type Timestamp(Microsecond, None) ([6e3ebfc](https://github.com/cube-js/cube.js/commit/6e3ebfc10c87b7ff23901949f1caa0a6021202e2))
* **cubestore:** Unsupported data type Boolean. ([b286182](https://github.com/cube-js/cube.js/commit/b28618204b4e07507e5df0e822607900a3439ca4))


### Features

* **cubestore:** Hash join support ([8b1a5da](https://github.com/cube-js/cube.js/commit/8b1a5da50992fa784aa2da8bd0dd092162b5b853))
* **cubestore:** Merge join support ([d08d8e3](https://github.com/cube-js/cube.js/commit/d08d8e357ca7baeb113fb0a003f76e519162c3ee))
* **cubestore:** Update datafusion upstream to the version of 2020-11-27 ([b4685dd](https://github.com/cube-js/cube.js/commit/b4685dd5556f5a1448ef0bfbcae841fd7905f372))





## [0.24.2](https://github.com/cube-js/cube.js/compare/v0.24.1...v0.24.2) (2020-11-27)


### Bug Fixes

* add content-type to allowedHeaders ([d176269](https://github.com/cube-js/cube.js/commit/d176269fda12d7213c021026c02f7aec0df50ba6))
* **@cubejs-backend/server-core:** Allow to pass unknown options (such as http) ([f1e9402](https://github.com/cube-js/cube.js/commit/f1e9402ee5c1fa6695d44f8750602d0a2ccedd5f))


### Features

* **@cubejs-backend/query-orchestrator:** Initial move to TypeScript ([#1462](https://github.com/cube-js/cube.js/issues/1462)) ([101e8dc](https://github.com/cube-js/cube.js/commit/101e8dc90d4b1266c0327adb86cab3e3caa8d4d0))





## [0.24.1](https://github.com/cube-js/cube.js/compare/v0.24.0...v0.24.1) (2020-11-27)


### Bug Fixes

* Specifying `dateRange` in time dimension should produce same result as `inDateRange` in filter ([a7603d7](https://github.com/cube-js/cube.js/commit/a7603d724732a51301227f68c39ba699333c0e06)), closes [#962](https://github.com/cube-js/cube.js/issues/962)
* **cubejs-cli:** template/serverless - specify CORS ([#1449](https://github.com/cube-js/cube.js/issues/1449)) ([f8064d2](https://github.com/cube-js/cube.js/commit/f8064d292570804fb8d2ef04708d2f5c4e563be2))
* **cubestore:** Negative int insert support ([5f2ff55](https://github.com/cube-js/cube.js/commit/5f2ff552bc5042f4d0d87fc3678de8e21ff5424a))


### Features

* **cubestore:** Group by boolean ([fa1b1b2](https://github.com/cube-js/cube.js/commit/fa1b1b2a439d9dd98e3cbaf730a313033f39ad80))
* **cubestore:** Group by boolean ([45fe036](https://github.com/cube-js/cube.js/commit/45fe03677beb09ef7d83065566d1e0536543fea2))
* Specify CORS for server/serverless ([#1455](https://github.com/cube-js/cube.js/issues/1455)) ([8c371ad](https://github.com/cube-js/cube.js/commit/8c371add2821a851bc51e00fb24e7ad2d8620345))





# [0.24.0](https://github.com/cube-js/cube.js/compare/v0.23.15...v0.24.0) (2020-11-26)


### Bug Fixes

* Error: Type must be provided for null values. -- `null` parameter values are passed to BigQuery when used for dimensions that contain `?` ([6417e7d](https://github.com/cube-js/cube.js/commit/6417e7d120a95c4792557a4c4a0d6abb7c483db9))
* **cubejs-cli:** template/serverless - iamRoleStatements.Resource[0] unsupported configuration format ([9fbe683](https://github.com/cube-js/cube.js/commit/9fbe683d3d1464ab453d354331033775fe707dec))


### Features

* Make default refreshKey to be `every 10 seconds` and enable scheduled refresh in dev mode by default ([221003a](https://github.com/cube-js/cube.js/commit/221003aa73aa1ece3d649de9164a7379a4a690be))


### BREAKING CHANGES

* `every 10 seconds` refreshKey becomes a default refreshKey for all cubes.





## [0.23.15](https://github.com/cube-js/cube.js/compare/v0.23.14...v0.23.15) (2020-11-25)


### Bug Fixes

* Error: Cannot find module 'antlr4/index' ([0d2e330](https://github.com/cube-js/cube.js/commit/0d2e33040dfea3fb80df2a1af2ccff46db0f8673))
* **@cubejs-backend/server-core:** Correct type for orchestratorOptions ([#1422](https://github.com/cube-js/cube.js/issues/1422)) ([96c1691](https://github.com/cube-js/cube.js/commit/96c169150ccf2197812dafdebce8194dd2cf6294))


### Features

* **@cubejs-backend/postgres-driver:** Support CUBEJS_DB_SSL_KEY ([e6291fc](https://github.com/cube-js/cube.js/commit/e6291fcda283aa6ee22badec339a600db02a1ce9))
* **@cubejs-client/react:** support 'compareDateRange' when updating 'timeDimensions' ([#1426](https://github.com/cube-js/cube.js/issues/1426)). Thanks to @BeAnMo! ([6446a58](https://github.com/cube-js/cube.js/commit/6446a58c5d6c983f045dc2062732aacfd69d908a))





## [0.23.14](https://github.com/cube-js/cube.js/compare/v0.23.13...v0.23.14) (2020-11-22)


### Bug Fixes

* **@cubejs-backend/query-orchestrator:** Intermittent lags when pre-aggregation tables are refreshed ([4efe1fc](https://github.com/cube-js/cube.js/commit/4efe1fc006282d87ab2718918d1bdd174baa6be3))
* **@cubejs-backend/snowflake-driver:** Add keepConnectionAlive and release ([#1379](https://github.com/cube-js/cube.js/issues/1379)) ([f1acae5](https://github.com/cube-js/cube.js/commit/f1acae5e00e37ba1ab2c9fab0f5f94f8e7d20283))
* **@cubejs-client/core:** propagate segments to drillDown queries ([#1406](https://github.com/cube-js/cube.js/issues/1406)) ([d4ceb65](https://github.com/cube-js/cube.js/commit/d4ceb6502db9c62c0cf95f1e48879f95ea4544d7))
* **cubestore:** Error reporting in docker ([cba3c50](https://github.com/cube-js/cube.js/commit/cba3c50a9856e1fe6893e5e2a2c14f89ebc2ce41))
* **cubestore:** Tables are imported without location ([5e8cffb](https://github.com/cube-js/cube.js/commit/5e8cffb5cc5b0123157086b206cb565b0dca5bac))
* **examples:** Add deprecation warnings to Slack Vibe ([98783c6](https://github.com/cube-js/cube.js/commit/98783c6d0658e136912bbaf9d3c6da5385085738))
* **examples:** e-commerce backend 💸 ([dab7301](https://github.com/cube-js/cube.js/commit/dab7301b01eefd7d1c5c8cbf1f233ae9cc5cc4c8))
* **examples:** External Rollups 🗞 ([86172b7](https://github.com/cube-js/cube.js/commit/86172b752c18f0a785558aa5f4710d9155593208))
* **examples:** Migration to Netlify ([ad582a1](https://github.com/cube-js/cube.js/commit/ad582a144c3cc7d64ae55ff45bc684c8d967e98e))
* **examples:** React Dashboard ⚛️ ([eccae84](https://github.com/cube-js/cube.js/commit/eccae84bb8b76a3ee138445a2c648eeda11b3774))


### Features

* **cubestore:** Collect backtraces in docker ([d97bcb9](https://github.com/cube-js/cube.js/commit/d97bcb9f9b4d035d15192ba5bc559478cd850ff0))
* **cubestore:** Error reporting ([99ede83](https://github.com/cube-js/cube.js/commit/99ede8388699298c4bbe89462a1c1737a324ce53))
* **cubestore:** Table location support ([6b63ef8](https://github.com/cube-js/cube.js/commit/6b63ef8ac109cca40cca5d2787bc342938c56d7a))
* **docker:** Introduce alpine images ([#1413](https://github.com/cube-js/cube.js/issues/1413)) ([972c700](https://github.com/cube-js/cube.js/commit/972c7008c3dcf1febfdcb66af0dd674bedb04752))
* **docs-build:** add `gatsby-redirect-from` to allow redirects with SEO ([f3e680a](https://github.com/cube-js/cube.js/commit/f3e680a9542370a1efa126a85b86e8c425fcc8a3)), closes [#1395](https://github.com/cube-js/cube.js/issues/1395)
* Allow to run docker image without config file ([#1409](https://github.com/cube-js/cube.js/issues/1409)) ([bc53cd1](https://github.com/cube-js/cube.js/commit/bc53cd17296ea4fa53940b74eaa9e3c7823d1603))





## [0.23.13](https://github.com/cube-js/cube.js/compare/v0.23.12...v0.23.13) (2020-11-17)


### Bug Fixes

* **docker:** Use CMD instead of entrypoint for cubejs server ([d6066a8](https://github.com/cube-js/cube.js/commit/d6066a8049881ca5a53b5aa35b32c10f3adbd277))
* **docs:** fix broken link in 'Deployment Guide' page ([#1399](https://github.com/cube-js/cube.js/issues/1399)) ([4c01e2d](https://github.com/cube-js/cube.js/commit/4c01e2d9c548f0b2db9a19dc295dab5fe5179b68))





## [0.23.12](https://github.com/cube-js/cube.js/compare/v0.23.11...v0.23.12) (2020-11-17)


### Bug Fixes

* **@cubejs-client/core:** pivot should work well with null values ([#1386](https://github.com/cube-js/cube.js/issues/1386)). Thanks to [@mspiegel31](https://github.com/mspiegel31)! ([d4c2446](https://github.com/cube-js/cube.js/commit/d4c24469b8eea2d84f04c540b0a5f9a8d285ad1d))
* **cubestore:** CREATE SCHEMA IF NOT EXISTS support ([7c590b3](https://github.com/cube-js/cube.js/commit/7c590b30ca2c4bb3ef9ac6d9cbfc181f322de14c))


### Features

* Introduce CUBEJS_DEV_MODE & improve ENV variables experience ([#1356](https://github.com/cube-js/cube.js/issues/1356)) ([cc2aa92](https://github.com/cube-js/cube.js/commit/cc2aa92bbec87b21b147d5003fa546d4b1807185))
* **@cubejs-server:** Require the latest oclif packages to support Node.js 8 ([7019966](https://github.com/cube-js/cube.js/commit/70199662cc3370c0c8763bb69dcec045e4e52590))
* **cubestore:** Distributed query execution ([102c641](https://github.com/cube-js/cube.js/commit/102c64120e2488a6ba2eff960d674cd5aedb9e8f))





## [0.23.11](https://github.com/cube-js/cube.js/compare/v0.23.10...v0.23.11) (2020-11-13)


### Bug Fixes

* **@cubejs-backend/server-core:** Node.js 8 support (downgrade fs-extra to 8.x) ([#1367](https://github.com/cube-js/cube.js/issues/1367)) ([be10ac6](https://github.com/cube-js/cube.js/commit/be10ac6912ebbaa57d386625dd4b2e3c40808c48))
* **@cubejs-client/core:** annotation format type ([e5004f6](https://github.com/cube-js/cube.js/commit/e5004f6bf687e7df4b611bf1d772da278558759d))
* **@cubejs-client/ws-transport:** make auth optional ([#1368](https://github.com/cube-js/cube.js/issues/1368)) ([28a07bd](https://github.com/cube-js/cube.js/commit/28a07bdc0e7e506bbc60daa2ad621415c93b54e2))
* **@cubejs-playground:** boolean filters support ([#1269](https://github.com/cube-js/cube.js/issues/1269)) ([adda809](https://github.com/cube-js/cube.js/commit/adda809e4cd08436ffdf8f3396a6f35725f3dc22))
* **@cubejs-playground:** ng scaffolding support ([0444744](https://github.com/cube-js/cube.js/commit/0444744dda44250c35eb22c1a7e2da1f2183cbc6))
* **@cubejs-playground:** ng support notification, loader ([2f73f16](https://github.com/cube-js/cube.js/commit/2f73f16f49c1c7325ea2104f44c8e8e437bc1ab6))
* **ci:** Trigger on pull_request, not issue ([193dc81](https://github.com/cube-js/cube.js/commit/193dc81fbbc506141656c6b0cc879b7b241ad33b))
* CUBEJS_DB_SSL must be true to affect SSL ([#1252](https://github.com/cube-js/cube.js/issues/1252)) ([f2e9d9d](https://github.com/cube-js/cube.js/commit/f2e9d9db3f7b8fc5a7c5bbaaebca56f5331d4332)), closes [#1212](https://github.com/cube-js/cube.js/issues/1212)
* **cubejs-cli:** Generate/token should work inside docker ([67d7501](https://github.com/cube-js/cube.js/commit/67d7501a8419e9f5be6d39ae9116592134d99c91))
* **cubestore:** Endless upload loop ([0494122](https://github.com/cube-js/cube.js/commit/049412257688c3971449cff22a789697c1b5eb04))
* **cubestore:** Worker Pool graceful shutdown ([56377dc](https://github.com/cube-js/cube.js/commit/56377dca194e76d9be4ec1f8ad18055af041914b))
* **examples/real-time-dashboard:** Configure collect entrypoint by REACT_APP_COLLECT_URL ENV ([bde3ad8](https://github.com/cube-js/cube.js/commit/bde3ad8ef9f5d6dbe5b07a0e496709adedc8abf7))


### Features

* **@cubejs-backend/mysql-aurora-serverless-driver:** Add a new driver to support AWS Aurora Serverless MySql ([#1333](https://github.com/cube-js/cube.js/issues/1333)) Thanks to [@kcwinner](https://github.com/kcwinner)! ([154fab1](https://github.com/cube-js/cube.js/commit/154fab1a222685e1e83d5187a4f00f745c4613a3))
* **@cubejs-client/react:** Add minute and second granularities to React QueryBuilder ([#1332](https://github.com/cube-js/cube.js/issues/1332)). Thanks to [@danielnass](https://github.com/danielnass)! ([aa201ae](https://github.com/cube-js/cube.js/commit/aa201aecdc66d920e7a6f84a1043cf5964bc6cb9))
* **cubejs-cli:** .env file - add link to the docs ([b63405c](https://github.com/cube-js/cube.js/commit/b63405cf78acabf80faec5d910be7a53af8702b9))
* **cubejs-cli:** create - persist template name & version ([8555290](https://github.com/cube-js/cube.js/commit/8555290dda3f36bc3b185fecef2ad17fba5aae80))
* **cubejs-cli:** Share /dashboard-app directory by default ([#1380](https://github.com/cube-js/cube.js/issues/1380)) ([d571dcc](https://github.com/cube-js/cube.js/commit/d571dcc9ad5c14916cd33740c0a3dba85e8c8be2))
* **cubejs-cli:** Use index.js file instead of cube.js ([#1350](https://github.com/cube-js/cube.js/issues/1350)) ([9b6c593](https://github.com/cube-js/cube.js/commit/9b6c59359e10cba7ec37e8a5be2ac7cc7dabd9da))
* **cubestore:** Add avx2 target-feature for docker build ([68e5a8a](https://github.com/cube-js/cube.js/commit/68e5a8a4d14d8028b5060bb6825d391b3c7ce8e5))
* **cubestore:** CUBESTORE_SELECT_WORKERS env variable ([9e59b2d](https://github.com/cube-js/cube.js/commit/9e59b2dbd8bdb43327560320d863569e77ba507c))
* **cubestore:** Select worker process pool ([c282cdd](https://github.com/cube-js/cube.js/commit/c282cdd0c1f80b444991290ba5753f8ce9ac710c))
* **cubestore:** Slow query logging ([d854303](https://github.com/cube-js/cube.js/commit/d8543033d764157139f2ffe3b6c96adaac070940))
* **docs-build:** change code font to Source Code Pro ([#1338](https://github.com/cube-js/cube.js/issues/1338)) ([79ec3db](https://github.com/cube-js/cube.js/commit/79ec3db573739ca0bbe85e92a86493232fee2991)), closes [#1337](https://github.com/cube-js/cube.js/issues/1337)
* **examples/real-time-dashboard:** Automatically deploy ([54303d8](https://github.com/cube-js/cube.js/commit/54303d88604593f48661f5980fe105d1f1bea8b4))





## [0.23.10](https://github.com/cube-js/cube.js/compare/v0.23.9...v0.23.10) (2020-11-07)


### Bug Fixes

* **@cubejs-client/playground:** add horizontal scroll and sticky head for chart card ([#1256](https://github.com/cube-js/cube.js/issues/1256)) ([025f15d](https://github.com/cube-js/cube.js/commit/025f15dbee101e12f086ef3bbe4c6cceaf543670))
* **@cubejs-playground:** codesandbox dependencies ([1ed6309](https://github.com/cube-js/cube.js/commit/1ed63096d60b241b6966b4bc29cb455214a59ee5))
* **ci:** Force usinging /build directory for netlify deployment ([7ca10f0](https://github.com/cube-js/cube.js/commit/7ca10f05cfec6e85d8eaf3042d18bd15c33b84ce))
* **ci:** Install netlify-cli instead of netlify (api client) ([60bfaa2](https://github.com/cube-js/cube.js/commit/60bfaa2531efdecc3f76af50cfe53f89752c3ae2))
* **cubejs-cli:** scaffolding/ScaffoldingTemplate dependency not found. ([8f3e6c7](https://github.com/cube-js/cube.js/commit/8f3e6c7594a406b689ed43ba1c0dd004f0a14e3b))
* **examples/drill-down:** Automatically deploy ([b04148b](https://github.com/cube-js/cube.js/commit/b04148b6bf8a5a47b927b83771f7953ae2905631))
* **examples/drill-down:** Automatically deploy ([570b903](https://github.com/cube-js/cube.js/commit/570b90341e87b458ef12873ce43f01b630abc8ac))
* **examples/highcharts:** Switch configuration for production/development ([978eb89](https://github.com/cube-js/cube.js/commit/978eb89cb4179ce9d5e3eb4008b1744ead08041c))
* **examples/highcharts:** Warnings on build ([72bb74b](https://github.com/cube-js/cube.js/commit/72bb74b6a8493611b3e3f878a1e432c49e1961e6))
* **examples/react-dashboard:** Automatically deploy ([0036016](https://github.com/cube-js/cube.js/commit/0036016b5947d95d362a52e5fe8029ec3298c58d))
* update message in CLI template ([d5a24ba](https://github.com/cube-js/cube.js/commit/d5a24ba1fad5a9b8bb1e5abed09a30b7bc5a8751))
* Warnings on installation ([cecaa6e](https://github.com/cube-js/cube.js/commit/cecaa6e9797ef23c52964b3c3e76ace6fb567e8a))


### Features

* **@cubejs-backend/server:** dev-server/server - introduce project diagnostics ([#1330](https://github.com/cube-js/cube.js/issues/1330)) ([0606926](https://github.com/cube-js/cube.js/commit/0606926146abfd33edc707efc617460b6b77e006))
* **ci:** Automatically deploy examples/highcharts ([c227137](https://github.com/cube-js/cube.js/commit/c227137793f10914485d0c05f498d759e21e3ef6))
* **cubestore:** Upgrade datafusion to 3.0 ([85f2165](https://github.com/cube-js/cube.js/commit/85f216517c6c611aca39c5f775669749a9e74387))





## [0.23.9](https://github.com/cube-js/cube.js/compare/v0.23.8...v0.23.9) (2020-11-06)

**Note:** Version bump only for package cubejs





## [0.23.8](https://github.com/cube-js/cube.js/compare/v0.23.7...v0.23.8) (2020-11-06)


### Bug Fixes

* **@cubejs-playground:** undefined query ([7d87fa6](https://github.com/cube-js/cube.js/commit/7d87fa60f207c2fa3360405a05d84fb6ffaba4c7))
* **cubejs-cli:** proxyCommand - await external command run on try/catch ([dc84460](https://github.com/cube-js/cube.js/commit/dc84460d740eedfff3a874f13316c1c2dedb9135))


### Features

* **@cubejs-backend/server:** Init source-map-support for cubejs-server/cubejs-dev-server ([aed319a](https://github.com/cube-js/cube.js/commit/aed319a33e84ba924a21a1270ee18f2ab054b9d5))
* **@cubejs-client/ws-transport:** Move to TypeScript ([#1293](https://github.com/cube-js/cube.js/issues/1293)) ([e7e1100](https://github.com/cube-js/cube.js/commit/e7e1100ee2adc7e1e9f6368c2edc6208a8eea774))
* **docker:** Use --frozen-lockfile for docker image building ([60a0ca9](https://github.com/cube-js/cube.js/commit/60a0ca9e77a8f95c40cc501dbdfd8ae80c3f8481))





## [0.23.7](https://github.com/cube-js/cube.js/compare/v0.23.6...v0.23.7) (2020-11-04)


### Bug Fixes

* **docker:** Add missing MySQL and cubestore drivers to the docker ([a36e86e](https://github.com/cube-js/cube.js/commit/a36e86e4e2524602a2a8ac09e2703e89c72796f2))


### Features

* **@cubejs-backend/server:** Migrate WebSocketServer to TS ([#1295](https://github.com/cube-js/cube.js/issues/1295)) ([94c39df](https://github.com/cube-js/cube.js/commit/94c39dfb35c0e8bed81a77cde093fd346bcd5646))
* **cubejs-cli:** Completely move CLI to TypeScript ([#1281](https://github.com/cube-js/cube.js/issues/1281)) ([dd5f3e2](https://github.com/cube-js/cube.js/commit/dd5f3e2948c82713354743af4a2727becac81388))
* Generate source maps for client libraries ([#1292](https://github.com/cube-js/cube.js/issues/1292)) ([cb64118](https://github.com/cube-js/cube.js/commit/cb64118770dce58bf7f3a3e7181cf159b8f316d3))
* **@cubejs-backend/jdbc-driver:** Upgrade vendors ([#1282](https://github.com/cube-js/cube.js/issues/1282)) ([94b9b37](https://github.com/cube-js/cube.js/commit/94b9b37484c55a4155578a84ade409035d62e152))
* **cubejs-cli:** Use env_file to pass .env file instead of sharing inside volume ([#1287](https://github.com/cube-js/cube.js/issues/1287)) ([876f549](https://github.com/cube-js/cube.js/commit/876f549dd9c5a7a79664006f6614a72a836b63ca))





## [0.23.6](https://github.com/cube-js/cube.js/compare/v0.23.5...v0.23.6) (2020-11-02)


### Bug Fixes

* **cubejs-cli:** Incorrectly generated reference to `module.export` instead of `module.exports` ([7427d46](https://github.com/cube-js/cube.js/commit/7427d463e63f173d7069ee9d8065a77013c98c2b))


### Features

* **cubejs-cli:** Add --token option for deploy command ([#1279](https://github.com/cube-js/cube.js/issues/1279)) ([4fecd8c](https://github.com/cube-js/cube.js/commit/4fecd8ca2fe6f3f85defe0ecb20ccf9b3f9a7067))





## [0.23.5](https://github.com/cube-js/cube.js/compare/v0.23.4...v0.23.5) (2020-11-02)


### Bug Fixes

* **cubejs-cli:** Deploy and Windows-style for file hashes ([ac3f62a](https://github.com/cube-js/cube.js/commit/ac3f62afd8a1957eec7b265de5c3781b70faf76c))
* **cubestore:** File is not found during list_recursive ([1065875](https://github.com/cube-js/cube.js/commit/1065875599b33c953c7e0b77f5743477929c0dc2))





## [0.23.4](https://github.com/cube-js/cube.js/compare/v0.23.3...v0.23.4) (2020-11-02)


### Bug Fixes

* **cubejs-cli:** Deploy and Windows-style paths ([#1277](https://github.com/cube-js/cube.js/issues/1277)) ([aa02f01](https://github.com/cube-js/cube.js/commit/aa02f0183008d6b49941d53321a68c59b999254d))





## [0.23.3](https://github.com/cube-js/cube.js/compare/v0.23.2...v0.23.3) (2020-10-31)


### Bug Fixes

* **@cubejs-backend/schema-compiler:** deprecation warning ([#1272](https://github.com/cube-js/cube.js/issues/1272)) ([5515465](https://github.com/cube-js/cube.js/commit/5515465))
* **ci:** Update a Docker Hub repository description automatically ([4ad0b0d](https://github.com/cube-js/cube.js/commit/4ad0b0d))
* **cubejs-cli:** @cubejs-backend/server/dist/command/dev-server dependency not found. ([e552ee1](https://github.com/cube-js/cube.js/commit/e552ee1))


### Features

* **@cubejs-backend/query-orchestrator:** add support for MSSQL nvarchar ([#1260](https://github.com/cube-js/cube.js/issues/1260)) Thanks to @JoshMentzer! ([a9e9919](https://github.com/cube-js/cube.js/commit/a9e9919))
* Dynamic Angular template ([#1257](https://github.com/cube-js/cube.js/issues/1257)) ([86ba728](https://github.com/cube-js/cube.js/commit/86ba728))





## [0.23.2](https://github.com/cube-js/cube.js/compare/v0.23.1...v0.23.2) (2020-10-28)


### Bug Fixes

* Add default ports and fix dashboard creation fails in docker ([#1267](https://github.com/cube-js/cube.js/issues/1267)) ([2929dbb](https://github.com/cube-js/cube.js/commit/2929dbb))





## [0.23.1](https://github.com/cube-js/cube.js/compare/v0.23.0...v0.23.1) (2020-10-28)


### Bug Fixes

* Unavailable. @cubejs-backend/server inside current directory requires cubejs-cli (^0.22) ([#1265](https://github.com/cube-js/cube.js/issues/1265)) ([340746e](https://github.com/cube-js/cube.js/commit/340746e))





# [0.23.0](https://github.com/cube-js/cube.js/compare/v0.22.4...v0.23.0) (2020-10-28)


### Bug Fixes

* TypeError: CubejsServer.driverDependencies is not a function ([#1264](https://github.com/cube-js/cube.js/issues/1264)) ([9b1260a](https://github.com/cube-js/cube.js/commit/9b1260a))





## [0.22.4](https://github.com/cube-js/cube.js/compare/v0.22.3...v0.22.4) (2020-10-28)


### Bug Fixes

* **Web Analytics Guide:** add links ([065a637](https://github.com/cube-js/cube.js/commit/065a637))


### Features

* **@cubejs-backend/server:** Implement dev-server & server command ([#1227](https://github.com/cube-js/cube.js/issues/1227)) ([84c1eeb](https://github.com/cube-js/cube.js/commit/84c1eeb))
* Introduce Docker template ([#1243](https://github.com/cube-js/cube.js/issues/1243)) ([e0430bf](https://github.com/cube-js/cube.js/commit/e0430bf))





## [0.22.3](https://github.com/cube-js/cube.js/compare/v0.22.2...v0.22.3) (2020-10-26)


### Bug Fixes

* **@cubejs-backend/schema-compiler:** Dialect for 'undefined' is not found, fix [#1247](https://github.com/cube-js/cube.js/issues/1247) ([1069b47](https://github.com/cube-js/cube.js/commit/1069b47ff4f0a9d2e398ba194fe3eef5ad39f0d2))





## [0.22.2](https://github.com/cube-js/cube.js/compare/v0.22.1...v0.22.2) (2020-10-26)


### Bug Fixes

* Dialect class isn't looked up for external drivers ([b793f4a](https://github.com/cube-js/cube.js/commit/b793f4a))
* **@cubejs-client/core:** duplicate names in ResultSet.seriesNames() ([#1187](https://github.com/cube-js/cube.js/issues/1187)). Thanks to [@aviranmoz](https://github.com/aviranmoz)! ([8d9eb68](https://github.com/cube-js/cube.js/commit/8d9eb68))


### Features

* Short Cube Cloud auth token ([#1222](https://github.com/cube-js/cube.js/issues/1222)) ([7885089](https://github.com/cube-js/cube.js/commit/7885089))





## [0.22.1](https://github.com/cube-js/cube.js/compare/v0.22.0...v0.22.1) (2020-10-21)


### Bug Fixes

* **@cubejs-playground:** avoid unnecessary load calls, dryRun ([#1210](https://github.com/cube-js/cube.js/issues/1210)) ([aaf4911](https://github.com/cube-js/cube.js/commit/aaf4911))
* **cube-cli:** Missed deploy command ([4192e77](https://github.com/cube-js/cube.js/commit/4192e77))


### Features

* **cubejs-cli:** Check js files by tsc ([3b9f4a2](https://github.com/cube-js/cube.js/commit/3b9f4a2))
* **cubejs-cli:** Move deploy command to TS ([b38cb4a](https://github.com/cube-js/cube.js/commit/b38cb4a))





# [0.22.0](https://github.com/cube-js/cube.js/compare/v0.21.2...v0.22.0) (2020-10-20)


### Bug Fixes

* umd build default export ([#1219](https://github.com/cube-js/cube.js/issues/1219)) ([cc434eb](https://github.com/cube-js/cube.js/commit/cc434eb))
* **@cubejs-client/core:** Add parseDateMeasures field to CubeJSApiOptions (typings) ([e1a1ada](https://github.com/cube-js/cube.js/commit/e1a1ada))
* **@cubejs-client/vue:** Allow array props on query renderer to allow data blending usage ([#1213](https://github.com/cube-js/cube.js/issues/1213)). Thanks to [@richipargo](https://github.com/richipargo) ([2203a54](https://github.com/cube-js/cube.js/commit/2203a54))
* **ci:** Specify DOCKER_IMAGE ([59bf390](https://github.com/cube-js/cube.js/commit/59bf390))
* **docs-gen:** change signature generation ([e4703ad](https://github.com/cube-js/cube.js/commit/e4703ad))


### Features

* Cube Store driver ([85ca240](https://github.com/cube-js/cube.js/commit/85ca240))
* **@cubejs-backend/server:** Introduce external commands for CLI (demo) ([fed9285](https://github.com/cube-js/cube.js/commit/fed9285))
* **cubejs-cli:** adds USER_CONTEXT parameter to cli ([#1215](https://github.com/cube-js/cube.js/issues/1215)) Thanks to @TheSPD! ([66452b9](https://github.com/cube-js/cube.js/commit/66452b9))
* **cubejs-cli:** Improve external commands support ([c13a729](https://github.com/cube-js/cube.js/commit/c13a729))
* **cubejs-cli:** Move helpers to TypeScript ([06b5f01](https://github.com/cube-js/cube.js/commit/06b5f01))
* **cubejs-cli:** Run dev-server/server commands from @cubejs-backend/core ([a35244c](https://github.com/cube-js/cube.js/commit/a35244c))
* **cubejs-cli:** Run dev-server/server commands from @cubejs-backend/core ([a4d72fe](https://github.com/cube-js/cube.js/commit/a4d72fe))
* **cubejs-cli:** Use TypeScript ([009ff7a](https://github.com/cube-js/cube.js/commit/009ff7a))





## [0.21.2](https://github.com/cube-js/cube.js/compare/v0.21.1...v0.21.2) (2020-10-15)


### Bug Fixes

* **@cubejs-client/playground:** fix setting popovers ([#1209](https://github.com/cube-js/cube.js/issues/1209)) ([644bb9f](https://github.com/cube-js/cube.js/commit/644bb9f))





## [0.21.1](https://github.com/cube-js/cube.js/compare/v0.21.0...v0.21.1) (2020-10-15)


### Bug Fixes

* **@cubejs-client/react:** resultSet ts in for QueryBuilderRenderProps ([#1193](https://github.com/cube-js/cube.js/issues/1193)) ([7e15cf0](https://github.com/cube-js/cube.js/commit/7e15cf0))


### Features

* Introduce Official Docker Image ([#1201](https://github.com/cube-js/cube.js/issues/1201)) ([0647d1f](https://github.com/cube-js/cube.js/commit/0647d1f))





# [0.21.0](https://github.com/cube-js/cube.js/compare/v0.20.15...v0.21.0) (2020-10-09)

**Note:** Version bump only for package cubejs





## [0.20.15](https://github.com/cube-js/cube.js/compare/v0.20.14...v0.20.15) (2020-10-09)

**Note:** Version bump only for package cubejs





## [0.20.14](https://github.com/cube-js/cube.js/compare/v0.20.13...v0.20.14) (2020-10-09)


### Bug Fixes

* Filter values can't be changed in Playground -- revert back defaultHeuristic implementation ([30ee112](https://github.com/cube-js/cube.js/commit/30ee112))





## [0.20.13](https://github.com/cube-js/cube.js/compare/v0.20.12...v0.20.13) (2020-10-07)


### Bug Fixes

* **@cubejs-backend/mongobi-driver:** TypeError: v.toLowerCase is not a function ([16a15cb](https://github.com/cube-js/cube.js/commit/16a15cb))
* **@cubejs-schema-compilter:** MSSQL rollingWindow with granularity ([#1169](https://github.com/cube-js/cube.js/issues/1169)) Thanks to @JoshMentzer! ([16e6a9e](https://github.com/cube-js/cube.js/commit/16e6a9e))





## [0.20.12](https://github.com/cube-js/cube.js/compare/v0.20.11...v0.20.12) (2020-10-02)


### Bug Fixes

* respect npm proxy settings ([#1137](https://github.com/cube-js/cube.js/issues/1137)) ([c43adac](https://github.com/cube-js/cube.js/commit/c43adac))
* **@cubejs-client/playground:** prepublishOnly for exports ([#1171](https://github.com/cube-js/cube.js/issues/1171)) ([5b6b4dc](https://github.com/cube-js/cube.js/commit/5b6b4dc))


### Features

* angular query builder ([#1073](https://github.com/cube-js/cube.js/issues/1073)) ([ea088b3](https://github.com/cube-js/cube.js/commit/ea088b3))
* **@cubejs-client/playground:** Export playground components ([#1170](https://github.com/cube-js/cube.js/issues/1170)) ([fb22331](https://github.com/cube-js/cube.js/commit/fb22331))





## [0.20.11](https://github.com/cube-js/cube.js/compare/v0.20.10...v0.20.11) (2020-09-28)


### Bug Fixes

* **@cubejs-backend/prestodb-driver:** Wrong OFFSET/LIMIT order ([#1135](https://github.com/cube-js/cube.js/issues/1135)) ([3b94b2c](https://github.com/cube-js/cube.js/commit/3b94b2c)), closes [#988](https://github.com/cube-js/cube.js/issues/988) [#988](https://github.com/cube-js/cube.js/issues/988) [#988](https://github.com/cube-js/cube.js/issues/988)
* **example:** Material UI Dashboard link ([f4c4170](https://github.com/cube-js/cube.js/commit/f4c4170))
* propagate drill down parent filters ([#1143](https://github.com/cube-js/cube.js/issues/1143)) ([314985e](https://github.com/cube-js/cube.js/commit/314985e))


### Features

* Introduce Druid driver ([#1099](https://github.com/cube-js/cube.js/issues/1099)) ([2bfe20f](https://github.com/cube-js/cube.js/commit/2bfe20f))





## [0.20.10](https://github.com/cube-js/cube.js/compare/v0.20.9...v0.20.10) (2020-09-23)


### Bug Fixes

* **@cubejs-backend/server-core:** Allow initApp as server-core option ([#1115](https://github.com/cube-js/cube.js/issues/1115)) ([a9d06fd](https://github.com/cube-js/cube.js/commit/a9d06fd))
* **@cubejs-backend/server-core:** Allow processSubscriptionsInterval as an option ([#1122](https://github.com/cube-js/cube.js/issues/1122)) ([cf21d70](https://github.com/cube-js/cube.js/commit/cf21d70))
* drilling into any measure other than the first ([#1131](https://github.com/cube-js/cube.js/issues/1131)) ([e741a20](https://github.com/cube-js/cube.js/commit/e741a20))
* rollupOnlyMode option validation ([#1127](https://github.com/cube-js/cube.js/issues/1127)) ([89ee308](https://github.com/cube-js/cube.js/commit/89ee308))
* **@cubejs-backend/server-core:** Support apiSecret as option ([#1130](https://github.com/cube-js/cube.js/issues/1130)) ([9fbf544](https://github.com/cube-js/cube.js/commit/9fbf544))





## [0.20.9](https://github.com/cube-js/cube.js/compare/v0.20.8...v0.20.9) (2020-09-19)


### Bug Fixes

* Allow empty complex boolean filter arrays ([#1100](https://github.com/cube-js/cube.js/issues/1100)) ([80d112e](https://github.com/cube-js/cube.js/commit/80d112e))
* Allow scheduledRefreshContexts in server-core options validation  ([#1105](https://github.com/cube-js/cube.js/issues/1105)) ([7e43276](https://github.com/cube-js/cube.js/commit/7e43276))
* **@cubejs-backend/server-core:** orchestratorOptions validation breaks serverless deployments ([#1113](https://github.com/cube-js/cube.js/issues/1113)) ([48ca5aa](https://github.com/cube-js/cube.js/commit/48ca5aa))


### Features

* **cubejs-cli:** Ask question about database, if user forget to specify it with -d flag ([#1096](https://github.com/cube-js/cube.js/issues/1096)) ([8b5b9d3](https://github.com/cube-js/cube.js/commit/8b5b9d3))
* `sqlAlias` attribute for `preAggregations` and short format for pre-aggregation table names ([#1068](https://github.com/cube-js/cube.js/issues/1068)) ([98ffad3](https://github.com/cube-js/cube.js/commit/98ffad3)), closes [#86](https://github.com/cube-js/cube.js/issues/86) [#907](https://github.com/cube-js/cube.js/issues/907)
* Share Node's version for debug purposes ([#1107](https://github.com/cube-js/cube.js/issues/1107)) ([26c0420](https://github.com/cube-js/cube.js/commit/26c0420))





## [0.20.8](https://github.com/cube-js/cube.js/compare/v0.20.7...v0.20.8) (2020-09-16)


### Bug Fixes

* **@cubejs-backend/athena-driver:** Show views in Playground for Athena ([#1090](https://github.com/cube-js/cube.js/issues/1090)) ([f8ce729](https://github.com/cube-js/cube.js/commit/f8ce729))
* validated query behavior ([#1085](https://github.com/cube-js/cube.js/issues/1085)) ([e93891b](https://github.com/cube-js/cube.js/commit/e93891b))
* **@cubejs-backend/elasticsearch-driver:** Respect `ungrouped` flag ([#1098](https://github.com/cube-js/cube.js/issues/1098)) Thanks to [@vignesh-123](https://github.com/vignesh-123)! ([995b8f9](https://github.com/cube-js/cube.js/commit/995b8f9))


### Features

* Add server-core options validate ([#1089](https://github.com/cube-js/cube.js/issues/1089)) ([5580018](https://github.com/cube-js/cube.js/commit/5580018))
* refreshKey every support for CRON format interval ([#1048](https://github.com/cube-js/cube.js/issues/1048)) ([3e55f5c](https://github.com/cube-js/cube.js/commit/3e55f5c))
* Strict cube schema parsing, show duplicate property name errors ([#1095](https://github.com/cube-js/cube.js/issues/1095)) ([d4ab530](https://github.com/cube-js/cube.js/commit/d4ab530))





## [0.20.7](https://github.com/cube-js/cube.js/compare/v0.20.6...v0.20.7) (2020-09-11)


### Bug Fixes

* member-dimension query normalization for queryTransformer and additional complex boolean logic tests ([#1047](https://github.com/cube-js/cube.js/issues/1047)) ([65ef327](https://github.com/cube-js/cube.js/commit/65ef327)), closes [#1007](https://github.com/cube-js/cube.js/issues/1007)





## [0.20.6](https://github.com/cube-js/cube.js/compare/v0.20.5...v0.20.6) (2020-09-10)


### Bug Fixes

* pivot control ([05ce626](https://github.com/cube-js/cube.js/commit/05ce626))





## [0.20.5](https://github.com/cube-js/cube.js/compare/v0.20.4...v0.20.5) (2020-09-10)


### Bug Fixes

* cube-client-core resolveMember return type ([#1051](https://github.com/cube-js/cube.js/issues/1051)). Thanks to @Aaronkala ([662cfe0](https://github.com/cube-js/cube.js/commit/662cfe0))
* improve TimeDimensionGranularity type ([#1052](https://github.com/cube-js/cube.js/issues/1052)). Thanks to [@joealden](https://github.com/joealden) ([1e9bd99](https://github.com/cube-js/cube.js/commit/1e9bd99))
* query logger ([e5d6ce9](https://github.com/cube-js/cube.js/commit/e5d6ce9))





## [0.20.4](https://github.com/cube-js/cube.js/compare/v0.20.3...v0.20.4) (2020-09-04)


### Bug Fixes

* **@cubejs-backend/dremio-driver:** CAST doesn't work on string timestamps: replace CAST to TO_TIMESTAMP ([#1057](https://github.com/cube-js/cube.js/issues/1057)) ([59da9ae](https://github.com/cube-js/cube.js/commit/59da9ae))





## [0.20.3](https://github.com/cube-js/cube.js/compare/v0.20.2...v0.20.3) (2020-09-03)


### Bug Fixes

* Export the TimeDimensionGranularity type ([#1044](https://github.com/cube-js/cube.js/issues/1044)). Thanks to [@gudjonragnar](https://github.com/gudjonragnar) ([26b329e](https://github.com/cube-js/cube.js/commit/26b329e))


### Features

* Complex boolean logic ([#1038](https://github.com/cube-js/cube.js/issues/1038)) ([a5b44d1](https://github.com/cube-js/cube.js/commit/a5b44d1)), closes [#259](https://github.com/cube-js/cube.js/issues/259)





## [0.20.2](https://github.com/cube-js/cube.js/compare/v0.20.1...v0.20.2) (2020-09-02)


### Bug Fixes

* subscribe option, new query types to work with ws ([dbf602e](https://github.com/cube-js/cube.js/commit/dbf602e))


### Features

* custom date range ([#1027](https://github.com/cube-js/cube.js/issues/1027)) ([304985f](https://github.com/cube-js/cube.js/commit/304985f))





## [0.20.1](https://github.com/cube-js/cube.js/compare/v0.20.0...v0.20.1) (2020-09-01)


### Bug Fixes

* data blending query support ([#1033](https://github.com/cube-js/cube.js/issues/1033)) ([20fc979](https://github.com/cube-js/cube.js/commit/20fc979))
* Error: TypeError: Cannot read property ‘externalPreAggregationQuery’ of null ([e23f302](https://github.com/cube-js/cube.js/commit/e23f302))


### Features

* Expose the progress response in the useCubeQuery hook ([#990](https://github.com/cube-js/cube.js/issues/990)). Thanks to [@anton164](https://github.com/anton164) ([01da1fd](https://github.com/cube-js/cube.js/commit/01da1fd))
* scheduledRefreshContexts CubejsServerCore option ([789a098](https://github.com/cube-js/cube.js/commit/789a098))





# [0.20.0](https://github.com/cube-js/cube.js/compare/v0.19.61...v0.20.0) (2020-08-26)


### Bug Fixes

* **@cubejs-backend/athena-driver:** Error: Queries of this type are not supported for incremental refreshKey ([2d3018d](https://github.com/cube-js/cube.js/commit/2d3018d)), closes [#404](https://github.com/cube-js/cube.js/issues/404)
* Check partitionGranularity requires timeDimensionReference for `originalSql` ([2a2b256](https://github.com/cube-js/cube.js/commit/2a2b256))
* Refresh Scheduler should respect `dataSource` ([d7e7a57](https://github.com/cube-js/cube.js/commit/d7e7a57))
* respect timezone in drillDown queries ([#1003](https://github.com/cube-js/cube.js/issues/1003)) ([c128417](https://github.com/cube-js/cube.js/commit/c128417))
* **@cubejs-backend/clickhouse-driver:** allow default compound indexes: add parentheses to the pre-aggregation sql definition ([#1009](https://github.com/cube-js/cube.js/issues/1009)) Thanks to [@gudjonragnar](https://github.com/gudjonragnar)! ([6535cb6](https://github.com/cube-js/cube.js/commit/6535cb6))
* TypeError: Cannot read property '1' of undefined -- Using scheduled cube refresh endpoint not working with Athena ([ed6c9aa](https://github.com/cube-js/cube.js/commit/ed6c9aa)), closes [#1000](https://github.com/cube-js/cube.js/issues/1000)


### Features

* add post method for the load endpoint ([#982](https://github.com/cube-js/cube.js/issues/982)). Thanks to @RusovDmitriy ([1524ede](https://github.com/cube-js/cube.js/commit/1524ede))
* Data blending ([#1012](https://github.com/cube-js/cube.js/issues/1012)) ([19fd00e](https://github.com/cube-js/cube.js/commit/19fd00e))
* date range comparison support ([#979](https://github.com/cube-js/cube.js/issues/979)) ([ca21cfd](https://github.com/cube-js/cube.js/commit/ca21cfd))
* Dremio driver ([#1008](https://github.com/cube-js/cube.js/issues/1008)) ([617225f](https://github.com/cube-js/cube.js/commit/617225f))
* Make the Filter type more specific. ([#915](https://github.com/cube-js/cube.js/issues/915)) Thanks to [@ylixir](https://github.com/ylixir) ([cecdb36](https://github.com/cube-js/cube.js/commit/cecdb36))
* query limit control ([#910](https://github.com/cube-js/cube.js/issues/910)) ([c6e086b](https://github.com/cube-js/cube.js/commit/c6e086b))





## [0.19.61](https://github.com/cube-js/cube.js/compare/v0.19.60...v0.19.61) (2020-08-11)


### Bug Fixes

* avoid opening connection to the source database when caching tables from external rollup db ([#929](https://github.com/cube-js/cube.js/issues/929)) Thanks to [@jcw](https://github.com/jcw)-! ([92cd0b3](https://github.com/cube-js/cube.js/commit/92cd0b3))
* readOnly originalSql pre-aggregations aren't working without writing rights ([cfa7c7d](https://github.com/cube-js/cube.js/commit/cfa7c7d))


### Features

* add support of array of tuples order format ([#973](https://github.com/cube-js/cube.js/issues/973)). Thanks to @RusovDmitriy ([0950b94](https://github.com/cube-js/cube.js/commit/0950b94))
* **mssql-driver:** add readonly aggregation for mssql sources ([#920](https://github.com/cube-js/cube.js/issues/920)) Thanks to @JoshMentzer! ([dfeccca](https://github.com/cube-js/cube.js/commit/dfeccca))





## [0.19.60](https://github.com/cube-js/cube.js/compare/v0.19.59...v0.19.60) (2020-08-08)


### Bug Fixes

* Intermittent errors with empty rollups or not ready metadata for Athena and MySQL: HIVE_CANNOT_OPEN_SPLIT errors. ([fa2cf45](https://github.com/cube-js/cube.js/commit/fa2cf45))





## [0.19.59](https://github.com/cube-js/cube.js/compare/v0.19.58...v0.19.59) (2020-08-05)


### Bug Fixes

* appying templates in a git repo ([#952](https://github.com/cube-js/cube.js/issues/952)) ([3556a74](https://github.com/cube-js/cube.js/commit/3556a74))





## [0.19.58](https://github.com/cube-js/cube.js/compare/v0.19.57...v0.19.58) (2020-08-05)


### Bug Fixes

* Error: Cannot find module 'axios' ([5fcfa87](https://github.com/cube-js/cube.js/commit/5fcfa87))





## [0.19.57](https://github.com/cube-js/cube.js/compare/v0.19.56...v0.19.57) (2020-08-05)


### Bug Fixes

* bizcharts incorrect geom type ([#941](https://github.com/cube-js/cube.js/issues/941)) ([7df66d8](https://github.com/cube-js/cube.js/commit/7df66d8))


### Features

* Playground templates separate repository open for third party contributions ([#903](https://github.com/cube-js/cube.js/issues/903)) ([fb57bda](https://github.com/cube-js/cube.js/commit/fb57bda))
* support first chance to define routes ([#931](https://github.com/cube-js/cube.js/issues/931)) Thanks to [@jsw](https://github.com/jsw)- ([69fdebc](https://github.com/cube-js/cube.js/commit/69fdebc))





## [0.19.56](https://github.com/cube-js/cube.js/compare/v0.19.55...v0.19.56) (2020-08-03)


### Bug Fixes

* allow renewQuery in dev mode with warning ([#868](https://github.com/cube-js/cube.js/issues/868)) Thanks to [@jcw](https://github.com/jcw)-! ([dbdbb5f](https://github.com/cube-js/cube.js/commit/dbdbb5f))
* CubeContext ts type missing ([#913](https://github.com/cube-js/cube.js/issues/913)) ([f5f72cd](https://github.com/cube-js/cube.js/commit/f5f72cd))
* membersForQuery return type ([#909](https://github.com/cube-js/cube.js/issues/909)) ([4976fcf](https://github.com/cube-js/cube.js/commit/4976fcf))
* readme examples updates ([#893](https://github.com/cube-js/cube.js/issues/893)) ([0458af8](https://github.com/cube-js/cube.js/commit/0458af8))
* using limit and offset together in MSSql ([9ba875c](https://github.com/cube-js/cube.js/commit/9ba875c))
* Various ClickHouse improvements ([6f40847](https://github.com/cube-js/cube.js/commit/6f40847))





## [0.19.55](https://github.com/cube-js/cube.js/compare/v0.19.54...v0.19.55) (2020-07-23)


### Bug Fixes

* ngx client installation ([#898](https://github.com/cube-js/cube.js/issues/898)) ([31ab9a0](https://github.com/cube-js/cube.js/commit/31ab9a0))


### Features

* expose loadResponse annotation ([#894](https://github.com/cube-js/cube.js/issues/894)) ([2875d47](https://github.com/cube-js/cube.js/commit/2875d47))





## [0.19.54](https://github.com/cube-js/cube.js/compare/v0.19.53...v0.19.54) (2020-07-23)


### Bug Fixes

* Orphaned queries in Redis queue during intensive load ([101b85f](https://github.com/cube-js/cube.js/commit/101b85f))





## [0.19.53](https://github.com/cube-js/cube.js/compare/v0.19.52...v0.19.53) (2020-07-20)


### Bug Fixes

* preserve order of sorted data ([#870](https://github.com/cube-js/cube.js/issues/870)) ([861db10](https://github.com/cube-js/cube.js/commit/861db10))


### Features

* More logging info for Orphaned Queries debugging ([99bf957](https://github.com/cube-js/cube.js/commit/99bf957))





## [0.19.52](https://github.com/cube-js/cube.js/compare/v0.19.51...v0.19.52) (2020-07-18)


### Bug Fixes

* Redis driver execAsync ignores watch directives ([ac67e5b](https://github.com/cube-js/cube.js/commit/ac67e5b))





## [0.19.51](https://github.com/cube-js/cube.js/compare/v0.19.50...v0.19.51) (2020-07-17)

**Note:** Version bump only for package cubejs





## [0.19.50](https://github.com/cube-js/cube.js/compare/v0.19.49...v0.19.50) (2020-07-16)


### Bug Fixes

* **cubejs-client-vue:** added deep watch at query props object in Vue QueryBuilder ([#818](https://github.com/cube-js/cube.js/issues/818)) ([32402e6](https://github.com/cube-js/cube.js/commit/32402e6))
* filter out falsy members ([65b19c9](https://github.com/cube-js/cube.js/commit/65b19c9))


### Features

* Generic readOnly external rollup implementation. MongoDB support. ([79d7bfd](https://github.com/cube-js/cube.js/commit/79d7bfd)), closes [#239](https://github.com/cube-js/cube.js/issues/239)
* ResultSet serializaion and deserializaion ([#836](https://github.com/cube-js/cube.js/issues/836)) ([80b8d41](https://github.com/cube-js/cube.js/commit/80b8d41))
* Rollup mode ([#843](https://github.com/cube-js/cube.js/issues/843)) Thanks to [@jcw](https://github.com/jcw)-! ([cc41f97](https://github.com/cube-js/cube.js/commit/cc41f97))





## [0.19.49](https://github.com/cube-js/cube.js/compare/v0.19.48...v0.19.49) (2020-07-11)


### Bug Fixes

* TypeError: exports.en is not a function ([ade2ccd](https://github.com/cube-js/cube.js/commit/ade2ccd))





## [0.19.48](https://github.com/cube-js/cube.js/compare/v0.19.47...v0.19.48) (2020-07-11)


### Bug Fixes

* **cubejs-client-core:** enums exported from declaration files are not accessible ([#810](https://github.com/cube-js/cube.js/issues/810)) ([3396fbe](https://github.com/cube-js/cube.js/commit/3396fbe))
* chrono-node upgrade changed `from 60 minutes ago to now` behavior ([e456829](https://github.com/cube-js/cube.js/commit/e456829))





## [0.19.46](https://github.com/cube-js/cube.js/compare/v0.19.45...v0.19.46) (2020-07-06)


### Features

* Report query usage for Athena and BigQuery ([697b53f](https://github.com/cube-js/cube.js/commit/697b53f))





## [0.19.45](https://github.com/cube-js/cube.js/compare/v0.19.44...v0.19.45) (2020-07-04)


### Bug Fixes

* Error: Error: Class constructor cannot be invoked without 'new' ([beb75df](https://github.com/cube-js/cube.js/commit/beb75df))
* TypeError: (queryOptions.dialectClass || ADAPTERS[dbType]) is not a constructor ([502480c](https://github.com/cube-js/cube.js/commit/502480c))





## [0.19.44](https://github.com/cube-js/cube.js/compare/v0.19.43...v0.19.44) (2020-07-04)


### Bug Fixes

* Error: Unsupported db type: function ([13d1b93](https://github.com/cube-js/cube.js/commit/13d1b93))





## [0.19.43](https://github.com/cube-js/cube.js/compare/v0.19.42...v0.19.43) (2020-07-04)


### Bug Fixes

* **cubejs-client-core:** Display the measure value when the y axis is empty ([#789](https://github.com/cube-js/cube.js/issues/789)) ([7ec6ac6](https://github.com/cube-js/cube.js/commit/7ec6ac6))
* **docs-gen:** Menu order ([#783](https://github.com/cube-js/cube.js/issues/783)) ([11d974a](https://github.com/cube-js/cube.js/commit/11d974a))


### Features

* `CUBEJS_EXT_DB_*` env variables support ([3a4c921](https://github.com/cube-js/cube.js/commit/3a4c921))
* Adjust client options to send credentials when needed ([#790](https://github.com/cube-js/cube.js/issues/790)) Thanks to [@colefichter](https://github.com/colefichter) ! ([5203f6c](https://github.com/cube-js/cube.js/commit/5203f6c)), closes [#788](https://github.com/cube-js/cube.js/issues/788)
* Pluggable dialects support ([f786fdd](https://github.com/cube-js/cube.js/commit/f786fdd)), closes [#590](https://github.com/cube-js/cube.js/issues/590)





## [0.19.42](https://github.com/cube-js/cube.js/compare/v0.19.41...v0.19.42) (2020-07-01)


### Bug Fixes

* **docs-gen:** generation fixes ([1598a9b](https://github.com/cube-js/cube.js/commit/1598a9b))
* **docs-gen:** titles ([12a1a5f](https://github.com/cube-js/cube.js/commit/12a1a5f))


### Features

* `CUBEJS_SCHEDULED_REFRESH_TIMEZONES` env variable ([d22e3f0](https://github.com/cube-js/cube.js/commit/d22e3f0))





## [0.19.41](https://github.com/cube-js/cube.js/compare/v0.19.40...v0.19.41) (2020-06-30)


### Bug Fixes

* **docs-gen:** generator fixes, docs updates ([c5b26d0](https://github.com/cube-js/cube.js/commit/c5b26d0))
* **docs-gen:** minor fixes ([#771](https://github.com/cube-js/cube.js/issues/771)) ([ae32519](https://github.com/cube-js/cube.js/commit/ae32519))
* scheduledRefreshTimer.match is not a function ([caecc51](https://github.com/cube-js/cube.js/commit/caecc51)), closes [#772](https://github.com/cube-js/cube.js/issues/772)





## [0.19.40](https://github.com/cube-js/cube.js/compare/v0.19.39...v0.19.40) (2020-06-30)


### Bug Fixes

* Querying empty Postgres table with 'time' dimension in a cube results in null value ([07d00f8](https://github.com/cube-js/cube.js/commit/07d00f8)), closes [#639](https://github.com/cube-js/cube.js/issues/639)


### Features

* CUBEJS_SCHEDULED_REFRESH_TIMER env variable ([6d0096e](https://github.com/cube-js/cube.js/commit/6d0096e))
* **docs-gen:** Typedoc generator ([#769](https://github.com/cube-js/cube.js/issues/769)) ([15373eb](https://github.com/cube-js/cube.js/commit/15373eb))





## [0.19.39](https://github.com/cube-js/cube.js/compare/v0.19.38...v0.19.39) (2020-06-28)


### Bug Fixes

* treat wildcard Elasticsearch select as simple asterisk select: include * as part of RE to support elasticsearch indexes ([#760](https://github.com/cube-js/cube.js/issues/760)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar) ! ([099a888](https://github.com/cube-js/cube.js/commit/099a888))


### Features

* `refreshRangeStart` and `refreshRangeEnd` pre-aggregation params ([e4d2874](https://github.com/cube-js/cube.js/commit/e4d2874))





## [0.19.38](https://github.com/cube-js/cube.js/compare/v0.19.37...v0.19.38) (2020-06-28)


### Bug Fixes

* **cubejs-playground:** Long line ellipsis ([#761](https://github.com/cube-js/cube.js/issues/761)) ([4aee9dc](https://github.com/cube-js/cube.js/commit/4aee9dc))
* Refresh partitioned pre-aggregations sequentially to avoid excessive memory and Redis connection consumption ([38aab17](https://github.com/cube-js/cube.js/commit/38aab17))





## [0.19.37](https://github.com/cube-js/cube.js/compare/v0.19.36...v0.19.37) (2020-06-26)


### Bug Fixes

* **cubejs-client-core:** tableColumns empty data fix ([#750](https://github.com/cube-js/cube.js/issues/750)) ([0ac9b7a](https://github.com/cube-js/cube.js/commit/0ac9b7a))
* **cubejs-client-react:** order heuristic ([#758](https://github.com/cube-js/cube.js/issues/758)) ([498c10a](https://github.com/cube-js/cube.js/commit/498c10a))


### Features

* **cubejs-client-react:** Exposing updateQuery method ([#751](https://github.com/cube-js/cube.js/issues/751)) ([e2083c8](https://github.com/cube-js/cube.js/commit/e2083c8))
* query builder pivot config support ([#742](https://github.com/cube-js/cube.js/issues/742)) ([4e29057](https://github.com/cube-js/cube.js/commit/4e29057))





## [0.19.36](https://github.com/cube-js/cube.js/compare/v0.19.35...v0.19.36) (2020-06-24)


### Bug Fixes

* Avoid excessive pre-aggregation invalidation in presence of multiple structure versions ([fd5e602](https://github.com/cube-js/cube.js/commit/fd5e602))





## [0.19.35](https://github.com/cube-js/cube.js/compare/v0.19.34...v0.19.35) (2020-06-22)


### Bug Fixes

* **cubejs-client-core:** table pivot ([#672](https://github.com/cube-js/cube.js/issues/672)) ([70015f5](https://github.com/cube-js/cube.js/commit/70015f5))
* header ([#734](https://github.com/cube-js/cube.js/issues/734)) ([056275a](https://github.com/cube-js/cube.js/commit/056275a))
* Scheduler request annotation for `/v1/run-scheduled-refresh` ([8273544](https://github.com/cube-js/cube.js/commit/8273544))
* table ([#740](https://github.com/cube-js/cube.js/issues/740)) ([6f1a8e7](https://github.com/cube-js/cube.js/commit/6f1a8e7))





## [0.19.34](https://github.com/cube-js/cube.js/compare/v0.19.33...v0.19.34) (2020-06-10)


### Bug Fixes

* **cubejs-cli:** Check if correct directory is being deployed ([56b8319](https://github.com/cube-js/cube.js/commit/56b8319))





## [0.19.33](https://github.com/cube-js/cube.js/compare/v0.19.32...v0.19.33) (2020-06-10)


### Bug Fixes

* **cubejs-api-gateway:** fromEntries replacement ([#715](https://github.com/cube-js/cube.js/issues/715)) ([998c735](https://github.com/cube-js/cube.js/commit/998c735))





## [0.19.32](https://github.com/cube-js/cube.js/compare/v0.19.31...v0.19.32) (2020-06-10)


### Bug Fixes

* Cannot read property 'reorder' of undefined ([3f1d8d1](https://github.com/cube-js/cube.js/commit/3f1d8d1))





## [0.19.31](https://github.com/cube-js/cube.js/compare/v0.19.30...v0.19.31) (2020-06-10)


### Bug Fixes

* **cubejs-cli:** linter ([#712](https://github.com/cube-js/cube.js/issues/712)) ([53c053f](https://github.com/cube-js/cube.js/commit/53c053f))
* **cubejs-client-core:** Remove Content-Type header from requests in HttpTransport ([#709](https://github.com/cube-js/cube.js/issues/709)) ([f6e366c](https://github.com/cube-js/cube.js/commit/f6e366c))


### Features

* **cubejs-cli:** Save deploy credentials ([af7e930](https://github.com/cube-js/cube.js/commit/af7e930))
* add schema path as an environment variable. ([#711](https://github.com/cube-js/cube.js/issues/711)) ([5ee2e16](https://github.com/cube-js/cube.js/commit/5ee2e16)), closes [#695](https://github.com/cube-js/cube.js/issues/695)
* Query builder order by ([#685](https://github.com/cube-js/cube.js/issues/685)) ([d3c735b](https://github.com/cube-js/cube.js/commit/d3c735b))





## [0.19.30](https://github.com/cube-js/cube.js/compare/v0.19.29...v0.19.30) (2020-06-09)


### Bug Fixes

* **cubejs-cli:** Fix file hashing for Cube Cloud ([ce8e090](https://github.com/cube-js/cube.js/commit/ce8e090))





## [0.19.29](https://github.com/cube-js/cube.js/compare/v0.19.28...v0.19.29) (2020-06-09)


### Bug Fixes

* **cubejs-cli:** eslint fixes ([0aa8001](https://github.com/cube-js/cube.js/commit/0aa8001))





## [0.19.28](https://github.com/cube-js/cube.js/compare/v0.19.27...v0.19.28) (2020-06-09)


### Bug Fixes

* **cubejs-cli:** Correct missing auth error ([ceeaff7](https://github.com/cube-js/cube.js/commit/ceeaff7))





## [0.19.27](https://github.com/cube-js/cube.js/compare/v0.19.26...v0.19.27) (2020-06-09)

**Note:** Version bump only for package cubejs





## [0.19.26](https://github.com/cube-js/cube.js/compare/v0.19.25...v0.19.26) (2020-06-09)

**Note:** Version bump only for package cubejs





## [0.19.25](https://github.com/cube-js/cube.js/compare/v0.19.24...v0.19.25) (2020-06-09)


### Features

* **cubejs-cli:** Cube Cloud deploy implementation ([b34ba53](https://github.com/cube-js/cube.js/commit/b34ba53))





## [0.19.24](https://github.com/cube-js/cube.js/compare/v0.19.23...v0.19.24) (2020-06-06)


### Bug Fixes

* **@cubejs-backend/elasticsearch-driver:** respect ungrouped parameter ([#684](https://github.com/cube-js/cube.js/issues/684)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar)! ([27d0d49](https://github.com/cube-js/cube.js/commit/27d0d49))
* **@cubejs-backend/schema-compiler:** TypeError: methods.filter is not a function ([25c4ef6](https://github.com/cube-js/cube.js/commit/25c4ef6))





## [0.19.23](https://github.com/cube-js/cube.js/compare/v0.19.22...v0.19.23) (2020-06-02)


### Features

* drill down queries support ([#664](https://github.com/cube-js/cube.js/issues/664)) ([7e21545](https://github.com/cube-js/cube.js/commit/7e21545)), closes [#190](https://github.com/cube-js/cube.js/issues/190)





## [0.19.22](https://github.com/cube-js/cube.js/compare/v0.19.21...v0.19.22) (2020-05-26)

**Note:** Version bump only for package cubejs





## [0.19.21](https://github.com/cube-js/cube.js/compare/v0.19.20...v0.19.21) (2020-05-25)


### Bug Fixes

* **@cubejs-backend/sqlite-driver:** sqlite name and type extraction ([#659](https://github.com/cube-js/cube.js/issues/659)) Thanks to [@avin3sh](https://github.com/avin3sh) ! ([b1c179d](https://github.com/cube-js/cube.js/commit/b1c179d))
* **playground:** Dynamic dashboard templated doesn't work: graphql-tools version downgrade ([#665](https://github.com/cube-js/cube.js/issues/665)) ([f5dfe54](https://github.com/cube-js/cube.js/commit/f5dfe54)), closes [#661](https://github.com/cube-js/cube.js/issues/661)





## [0.19.20](https://github.com/cube-js/cube.js/compare/v0.19.19...v0.19.20) (2020-05-21)


### Bug Fixes

* **cubejs-playground:** header style ([8d0f6a9](https://github.com/cube-js/cube.js/commit/8d0f6a9))
* **cubejs-playground:** style fixes ([fadbdf2](https://github.com/cube-js/cube.js/commit/fadbdf2))
* **cubejs-postgres-driver:** updated pg version ([af758f6](https://github.com/cube-js/cube.js/commit/af758f6))





## [0.19.19](https://github.com/cube-js/cube.js/compare/v0.19.18...v0.19.19) (2020-05-15)


### Bug Fixes

* corejs version ([8bef3b2](https://github.com/cube-js/cube.js/commit/8bef3b2))
* **client-vue:** updateChartType fix ([#644](https://github.com/cube-js/cube.js/issues/644)) ([5c0e79c](https://github.com/cube-js/cube.js/commit/5c0e79c)), closes [#635](https://github.com/cube-js/cube.js/issues/635)


### Features

* ability to add custom meta data for measures, dimensions and segments ([#641](https://github.com/cube-js/cube.js/issues/641)) ([88d5c9b](https://github.com/cube-js/cube.js/commit/88d5c9b)), closes [#625](https://github.com/cube-js/cube.js/issues/625)





## [0.19.18](https://github.com/cube-js/cube.js/compare/v0.19.17...v0.19.18) (2020-05-11)


### Bug Fixes

* Offset doesn't affect actual queries ([1feaa38](https://github.com/cube-js/cube.js/commit/1feaa38)), closes [#636](https://github.com/cube-js/cube.js/issues/636)





## [0.19.17](https://github.com/cube-js/cube.js/compare/v0.19.16...v0.19.17) (2020-05-09)


### Bug Fixes

* Continue wait errors during tables fetch ([cafaa28](https://github.com/cube-js/cube.js/commit/cafaa28))





## [0.19.16](https://github.com/cube-js/cube.js/compare/v0.19.15...v0.19.16) (2020-05-07)


### Bug Fixes

* **@cubejs-client/react:** options dependency for useEffect: check if `subscribe` has been changed in `useCubeQuery` ([#632](https://github.com/cube-js/cube.js/issues/632)) ([13ab5de](https://github.com/cube-js/cube.js/commit/13ab5de))


### Features

* Update type defs for query transformer ([#619](https://github.com/cube-js/cube.js/issues/619)) Thanks to [@jcw](https://github.com/jcw)-! ([b396b05](https://github.com/cube-js/cube.js/commit/b396b05))





## [0.19.15](https://github.com/cube-js/cube.js/compare/v0.19.14...v0.19.15) (2020-05-04)


### Bug Fixes

* Max date measures incorrectly converted for MySQL ([e704867](https://github.com/cube-js/cube.js/commit/e704867))


### Features

* Include version in startup message ([#615](https://github.com/cube-js/cube.js/issues/615)) Thanks to jcw-! ([d2f1732](https://github.com/cube-js/cube.js/commit/d2f1732))
* More pre-aggregation info logging ([9d69f98](https://github.com/cube-js/cube.js/commit/9d69f98))
* Tweak server type definitions ([#623](https://github.com/cube-js/cube.js/issues/623)) Thanks to [@willhausman](https://github.com/willhausman)! ([23da279](https://github.com/cube-js/cube.js/commit/23da279))





## [0.19.14](https://github.com/cube-js/cube.js/compare/v0.19.13...v0.19.14) (2020-04-24)


### Bug Fixes

* More descriptive errors for download errors ([e834aba](https://github.com/cube-js/cube.js/commit/e834aba))
* Show Postgres params in logs ([a678ca7](https://github.com/cube-js/cube.js/commit/a678ca7))


### Features

* Postgres HLL improvements: always round to int ([#611](https://github.com/cube-js/cube.js/issues/611)) Thanks to [@milanbella](https://github.com/milanbella)! ([680a613](https://github.com/cube-js/cube.js/commit/680a613))





## [0.19.13](https://github.com/cube-js/cube.js/compare/v0.19.12...v0.19.13) (2020-04-21)


### Features

* Postgres Citus Data HLL plugin implementation ([#601](https://github.com/cube-js/cube.js/issues/601)) Thanks to [@milanbella](https://github.com/milanbella) ! ([be85ac6](https://github.com/cube-js/cube.js/commit/be85ac6)), closes [#563](https://github.com/cube-js/cube.js/issues/563)
* **react:** `resetResultSetOnChange` option for `QueryRenderer` and `useCubeQuery` ([c8c74d3](https://github.com/cube-js/cube.js/commit/c8c74d3))





## [0.19.12](https://github.com/cube-js/cube.js/compare/v0.19.11...v0.19.12) (2020-04-20)


### Bug Fixes

* Make date measure parsing optional ([d199cd5](https://github.com/cube-js/cube.js/commit/d199cd5)), closes [#602](https://github.com/cube-js/cube.js/issues/602)





## [0.19.11](https://github.com/cube-js/cube.js/compare/v0.19.10...v0.19.11) (2020-04-20)


### Bug Fixes

* Strict date range and rollup granularity alignment check ([deb62b6](https://github.com/cube-js/cube.js/commit/deb62b6)), closes [#103](https://github.com/cube-js/cube.js/issues/103)





## [0.19.10](https://github.com/cube-js/cube.js/compare/v0.19.9...v0.19.10) (2020-04-18)


### Bug Fixes

* Recursive pre-aggregation description generation: support propagateFiltersToSubQuery with partitioned originalSql ([6a2b9dd](https://github.com/cube-js/cube.js/commit/6a2b9dd))





## [0.19.9](https://github.com/cube-js/cube.js/compare/v0.19.8...v0.19.9) (2020-04-16)


### Features

* add await when invoking schemaVersion -- support async schemaVersion ([#557](https://github.com/cube-js/cube.js/issues/557)) Thanks to [@barakcoh](https://github.com/barakcoh)! ([964c6d8](https://github.com/cube-js/cube.js/commit/964c6d8))
* Added support for websocketsBasePath ([#584](https://github.com/cube-js/cube.js/issues/584)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar)! ([0fa7349](https://github.com/cube-js/cube.js/commit/0fa7349)), closes [#583](https://github.com/cube-js/cube.js/issues/583)
* Allow persisting multiple pre-aggregation structure versions to support staging pre-aggregation warm-up environments and multiple timezones ([ab9539a](https://github.com/cube-js/cube.js/commit/ab9539a))
* Parse dates on client side ([#522](https://github.com/cube-js/cube.js/issues/522)) Thanks to [@richipargo](https://github.com/richipargo)! ([11c1106](https://github.com/cube-js/cube.js/commit/11c1106))





## [0.19.8](https://github.com/cube-js/cube.js/compare/v0.19.7...v0.19.8) (2020-04-15)


### Bug Fixes

* Dead queries added to queue in serverless ([eca3d0c](https://github.com/cube-js/cube.js/commit/eca3d0c))





## [0.19.7](https://github.com/cube-js/cube.js/compare/v0.19.6...v0.19.7) (2020-04-14)


### Bug Fixes

* Associate Queue storage error with requestId ([ec2750e](https://github.com/cube-js/cube.js/commit/ec2750e))


### Features

* Including format and type in tableColumns ([#587](https://github.com/cube-js/cube.js/issues/587)) Thanks to [@danpanaite](https://github.com/danpanaite)! ([3f7d74f](https://github.com/cube-js/cube.js/commit/3f7d74f)), closes [#585](https://github.com/cube-js/cube.js/issues/585)





## [0.19.6](https://github.com/cube-js/cube.js/compare/v0.19.5...v0.19.6) (2020-04-14)


### Bug Fixes

* Consistent queryKey logging ([5f1a632](https://github.com/cube-js/cube.js/commit/5f1a632))





## [0.19.5](https://github.com/cube-js/cube.js/compare/v0.19.4...v0.19.5) (2020-04-13)


### Bug Fixes

* Broken query and pre-aggregation cancel ([aa82256](https://github.com/cube-js/cube.js/commit/aa82256))
* Include data transformation in Load Request time ([edf2461](https://github.com/cube-js/cube.js/commit/edf2461))
* RefreshScheduler refreshes pre-aggregations during cache key refresh ([51d1214](https://github.com/cube-js/cube.js/commit/51d1214))


### Features

* Log queue state on Waiting for query ([395c63c](https://github.com/cube-js/cube.js/commit/395c63c))





## [0.19.4](https://github.com/cube-js/cube.js/compare/v0.19.3...v0.19.4) (2020-04-12)


### Bug Fixes

* **serverless-aws:** cubejsProcess agent doesn't collect all events after process has been finished ([939e25a](https://github.com/cube-js/cube.js/commit/939e25a))





## [0.19.3](https://github.com/cube-js/cube.js/compare/v0.19.2...v0.19.3) (2020-04-12)


### Bug Fixes

* Handle invalid lambda process events ([37fc43f](https://github.com/cube-js/cube.js/commit/37fc43f))





## [0.19.2](https://github.com/cube-js/cube.js/compare/v0.19.1...v0.19.2) (2020-04-12)


### Bug Fixes

* Do not DoS agent with huge payloads ([7886130](https://github.com/cube-js/cube.js/commit/7886130))
* TypeError: Cannot read property 'timeDimensions' of null ([7d3329b](https://github.com/cube-js/cube.js/commit/7d3329b))





## [0.19.1](https://github.com/cube-js/cube.js/compare/v0.19.0...v0.19.1) (2020-04-11)


### Bug Fixes

* TypeError: Cannot read property 'dataSource' of null ([5bef81b](https://github.com/cube-js/cube.js/commit/5bef81b))
* TypeError: Cannot read property 'path' of undefined -- Case when partitioned originalSql is resolved for query without time dimension and incremental refreshKey is used ([ca0f1f6](https://github.com/cube-js/cube.js/commit/ca0f1f6))


### Features

* **postgres-driver:** Allow usage of CUBEJS_DB_SSL_CA parameter in postgres Driver. ([#582](https://github.com/cube-js/cube.js/issues/582)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar)! ([158bd10](https://github.com/cube-js/cube.js/commit/158bd10))
* Provide status messages for ``/cubejs-api/v1/run-scheduled-refresh` API ([fb6623f](https://github.com/cube-js/cube.js/commit/fb6623f))
* Renamed OpenDistro to AWSElasticSearch. Added `elasticsearch` dialect ([#577](https://github.com/cube-js/cube.js/issues/577)) Thanks to [@chad-codeworkshop](https://github.com/chad-codeworkshop)! ([a4e41cb](https://github.com/cube-js/cube.js/commit/a4e41cb))





# [0.19.0](https://github.com/cube-js/cube.js/compare/v0.18.32...v0.19.0) (2020-04-09)


### Features

* Multi-level query structures in-memory caching ([38aa32d](https://github.com/cube-js/cube.js/commit/38aa32d))





## [0.18.32](https://github.com/cube-js/cube.js/compare/v0.18.31...v0.18.32) (2020-04-07)


### Bug Fixes

* **mysql-driver:** Special characters in database name for readOnly database lead to Error: ER_PARSE_ERROR: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ([1464326](https://github.com/cube-js/cube.js/commit/1464326))





## [0.18.31](https://github.com/cube-js/cube.js/compare/v0.18.30...v0.18.31) (2020-04-07)


### Bug Fixes

* Pass query options such as timezone ([#570](https://github.com/cube-js/cube.js/issues/570)) Thanks to [@jcw](https://github.com/jcw)-! ([089f307](https://github.com/cube-js/cube.js/commit/089f307))
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





## [0.18.27](https://github.com/cube-js/cube.js/compare/v0.18.26...v0.18.27) (2020-04-03)


### Bug Fixes

* TypeError: date.match is not a function at BaseTimeDimension.formatFromDate ([4ac7307](https://github.com/cube-js/cube.js/commit/4ac7307))





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





## [0.18.22](https://github.com/cube-js/cube.js/compare/v0.18.21...v0.18.22) (2020-03-29)


### Features

* **mysql-driver:** Read only pre-aggregations support ([2e7cf58](https://github.com/cube-js/cube.js/commit/2e7cf58))





## [0.18.21](https://github.com/cube-js/cube.js/compare/v0.18.20...v0.18.21) (2020-03-29)


### Bug Fixes

* **mysql-driver:** Remove debug output ([3cd0bf3](https://github.com/cube-js/cube.js/commit/3cd0bf3))





## [0.18.20](https://github.com/cube-js/cube.js/compare/v0.18.19...v0.18.20) (2020-03-29)


### Features

* **mysql-driver:** `loadPreAggregationWithoutMetaLock` option ([a5bae69](https://github.com/cube-js/cube.js/commit/a5bae69))





## [0.18.19](https://github.com/cube-js/cube.js/compare/v0.18.18...v0.18.19) (2020-03-29)


### Bug Fixes

* Empty default `originalSql` refreshKey ([dd8536b](https://github.com/cube-js/cube.js/commit/dd8536b))
* incorrect WHERE for refreshKey every ([bf8b648](https://github.com/cube-js/cube.js/commit/bf8b648))
* Return single table for one partition queries ([54083ef](https://github.com/cube-js/cube.js/commit/54083ef))


### Features

* `propagateFiltersToSubQuery` flag ([6b253c0](https://github.com/cube-js/cube.js/commit/6b253c0))
* Partitioned `originalSql` support ([133857e](https://github.com/cube-js/cube.js/commit/133857e))





## [0.18.18](https://github.com/cube-js/cube.js/compare/v0.18.17...v0.18.18) (2020-03-28)


### Bug Fixes

* **postgres-driver:** Clean-up deprecation warning ([#531](https://github.com/cube-js/cube.js/issues/531)) ([ed1e8da](https://github.com/cube-js/cube.js/commit/ed1e8da))


### Features

* Executing SQL logging message that shows final SQL ([26b8758](https://github.com/cube-js/cube.js/commit/26b8758))





## [0.18.17](https://github.com/cube-js/cube.js/compare/v0.18.16...v0.18.17) (2020-03-24)


### Bug Fixes

* Unknown function NOW for Snowflake -- Incorrect now timestamp implementation ([036f68a](https://github.com/cube-js/cube.js/commit/036f68a)), closes [#537](https://github.com/cube-js/cube.js/issues/537)


### Features

* More places to fetch `readOnly` pre-aggregations flag from ([9877037](https://github.com/cube-js/cube.js/commit/9877037))





## [0.18.16](https://github.com/cube-js/cube.js/compare/v0.18.15...v0.18.16) (2020-03-24)


### Features

* Log canUseTransformedQuery ([5b2ab90](https://github.com/cube-js/cube.js/commit/5b2ab90))





## [0.18.15](https://github.com/cube-js/cube.js/compare/v0.18.14...v0.18.15) (2020-03-24)


### Bug Fixes

* Athena -> MySQL segmentReferences rollup support ([fd3f3d6](https://github.com/cube-js/cube.js/commit/fd3f3d6))





## [0.18.14](https://github.com/cube-js/cube.js/compare/v0.18.13...v0.18.14) (2020-03-24)


### Bug Fixes

* MySQL segment references support ([be42298](https://github.com/cube-js/cube.js/commit/be42298))


### Features

* **postgres-driver:** `CUBEJS_DB_MAX_POOL` env variable ([#528](https://github.com/cube-js/cube.js/issues/528)) Thanks to [@chaselmann](https://github.com/chaselmann)! ([fb0d34b](https://github.com/cube-js/cube.js/commit/fb0d34b))





## [0.18.13](https://github.com/cube-js/cube.js/compare/v0.18.12...v0.18.13) (2020-03-21)


### Bug Fixes

* Overriding of orchestratorOptions results in no usage of process cloud function -- deep merge Handlers options ([c879cb6](https://github.com/cube-js/cube.js/commit/c879cb6)), closes [#519](https://github.com/cube-js/cube.js/issues/519)
* Various cleanup errors ([538f6d0](https://github.com/cube-js/cube.js/commit/538f6d0)), closes [#525](https://github.com/cube-js/cube.js/issues/525)





## [0.18.12](https://github.com/cube-js/cube.js/compare/v0.18.11...v0.18.12) (2020-03-19)


### Bug Fixes

* **types:** Fix index.d.ts errors in cubejs-server. ([#521](https://github.com/cube-js/cube.js/issues/521)) Thanks to jwalton! ([0b01fd6](https://github.com/cube-js/cube.js/commit/0b01fd6))


### Features

* Add duration to error logging ([59a4255](https://github.com/cube-js/cube.js/commit/59a4255))





## [0.18.11](https://github.com/cube-js/cube.js/compare/v0.18.10...v0.18.11) (2020-03-18)


### Bug Fixes

* Orphaned pre-aggregation tables aren't dropped because LocalCacheDriver doesn't expire keys ([393af3d](https://github.com/cube-js/cube.js/commit/393af3d))





## [0.18.10](https://github.com/cube-js/cube.js/compare/v0.18.9...v0.18.10) (2020-03-18)


### Features

* **mysql-driver:** `CUBEJS_DB_MAX_POOL` env variable ([e67e0c7](https://github.com/cube-js/cube.js/commit/e67e0c7))
* **mysql-driver:** Provide a way to define pool options ([2dbf302](https://github.com/cube-js/cube.js/commit/2dbf302))





## [0.18.9](https://github.com/cube-js/cube.js/compare/v0.18.8...v0.18.9) (2020-03-18)


### Bug Fixes

* **mysql-driver:** use utf8mb4 charset for columns to fix ER_TRUNCATED_WRONG_VALUE_FOR_FIELD ([b68a7a8](https://github.com/cube-js/cube.js/commit/b68a7a8))





## [0.18.8](https://github.com/cube-js/cube.js/compare/v0.18.7...v0.18.8) (2020-03-18)


### Bug Fixes

* Publish index.d.ts for @cubejs-backend/server. ([#518](https://github.com/cube-js/cube.js/issues/518)) Thanks to [@jwalton](https://github.com/jwalton)! ([7e9861f](https://github.com/cube-js/cube.js/commit/7e9861f))
* **mysql-driver:** use utf8mb4 charset as default to fix ER_TRUNCATED_WRONG_VALUE_FOR_FIELD for string types ([17e084e](https://github.com/cube-js/cube.js/commit/17e084e))





## [0.18.7](https://github.com/cube-js/cube.js/compare/v0.18.6...v0.18.7) (2020-03-17)


### Bug Fixes

* Error: ER_TRUNCATED_WRONG_VALUE_FOR_FIELD for string types ([c2ee5ee](https://github.com/cube-js/cube.js/commit/c2ee5ee))


### Features

* Log `requestId` in compiling schema events ([4c457c9](https://github.com/cube-js/cube.js/commit/4c457c9))





## [0.18.6](https://github.com/cube-js/cube.js/compare/v0.18.5...v0.18.6) (2020-03-16)


### Bug Fixes

* Waiting for query isn't logged for Local Queue when query is already in progress ([e7be6d1](https://github.com/cube-js/cube.js/commit/e7be6d1))





## [0.18.5](https://github.com/cube-js/cube.js/compare/v0.18.4...v0.18.5) (2020-03-15)


### Bug Fixes

* **@cubejs-client/core:** make `progressCallback` optional ([#497](https://github.com/cube-js/cube.js/issues/497)) Thanks to [@hassankhan](https://github.com/hassankhan)! ([a41cf9a](https://github.com/cube-js/cube.js/commit/a41cf9a))
* `requestId` isn't propagating to all pre-aggregations messages ([650dd6e](https://github.com/cube-js/cube.js/commit/650dd6e))





## [0.18.4](https://github.com/cube-js/cube.js/compare/v0.18.3...v0.18.4) (2020-03-09)


### Bug Fixes

* Request span for WebSocketTransport is incorrectly set ([54ba5da](https://github.com/cube-js/cube.js/commit/54ba5da))
* results not converted to timezone unless granularity is set: value fails to match the required pattern ([715ba71](https://github.com/cube-js/cube.js/commit/715ba71)), closes [#443](https://github.com/cube-js/cube.js/issues/443)


### Features

* Add API gateway request logging support ([#475](https://github.com/cube-js/cube.js/issues/475)) ([465471e](https://github.com/cube-js/cube.js/commit/465471e))
* Use options pattern in constructor ([#468](https://github.com/cube-js/cube.js/issues/468)) Thanks to [@jcw](https://github.com/jcw)-! ([ff20167](https://github.com/cube-js/cube.js/commit/ff20167))





## [0.18.3](https://github.com/cube-js/cube.js/compare/v0.18.2...v0.18.3) (2020-03-02)


### Bug Fixes

* antd 4 support for dashboard ([84bb164](https://github.com/cube-js/cube.js/commit/84bb164)), closes [#463](https://github.com/cube-js/cube.js/issues/463)
* CUBEJS_REDIS_POOL_MAX=0 env variable setting isn't respected ([75f6889](https://github.com/cube-js/cube.js/commit/75f6889))
* Duration string is not printed for all messages -- Load Request SQL case ([e0d3aff](https://github.com/cube-js/cube.js/commit/e0d3aff))





## [0.18.2](https://github.com/cube-js/cube.js/compare/v0.18.1...v0.18.2) (2020-03-01)


### Bug Fixes

* Limit pre-aggregations fetch table requests using queue -- handle HA for pre-aggregations ([75833b1](https://github.com/cube-js/cube.js/commit/75833b1))





## [0.18.1](https://github.com/cube-js/cube.js/compare/v0.18.0...v0.18.1) (2020-03-01)


### Bug Fixes

* Remove user facing errors for pre-aggregations refreshes ([d15c551](https://github.com/cube-js/cube.js/commit/d15c551))





# [0.18.0](https://github.com/cube-js/cube.js/compare/v0.17.10...v0.18.0) (2020-03-01)


### Bug Fixes

* Error: client.readOnly is not a function ([6069499](https://github.com/cube-js/cube.js/commit/6069499))
* External rollup type conversions: cast double to decimal for postgres ([#421](https://github.com/cube-js/cube.js/issues/421)) Thanks to [@sandeepravi](https://github.com/sandeepravi)! ([a19410a](https://github.com/cube-js/cube.js/commit/a19410a))
* **athena-driver:** Remove debug output ([f538135](https://github.com/cube-js/cube.js/commit/f538135))
* Handle missing body-parser error ([b90dd89](https://github.com/cube-js/cube.js/commit/b90dd89))
* Handle multiple occurrences in the first event of a funnel: conversion percent discrepancies. ([0989482](https://github.com/cube-js/cube.js/commit/0989482))
* Handle primaryKey shown: false pitfall error ([5bbf5f0](https://github.com/cube-js/cube.js/commit/5bbf5f0))
* Redis query queue locking redesign ([a2eb9b2](https://github.com/cube-js/cube.js/commit/a2eb9b2)), closes [#459](https://github.com/cube-js/cube.js/issues/459)
* TypeError: Cannot read property 'queryKey' of null under load ([0c996d8](https://github.com/cube-js/cube.js/commit/0c996d8))


### Features

* Add role parameter to driver options ([#448](https://github.com/cube-js/cube.js/issues/448)) Thanks to [@smbkr](https://github.com/smbkr)! ([9bfb71d](https://github.com/cube-js/cube.js/commit/9bfb71d)), closes [#447](https://github.com/cube-js/cube.js/issues/447)
* COMPILE_CONTEXT and async driverFactory support ([160f931](https://github.com/cube-js/cube.js/commit/160f931))
* Redis connection pooling ([#433](https://github.com/cube-js/cube.js/issues/433)) Thanks to [@jcw](https://github.com/jcw)! ([cf133a9](https://github.com/cube-js/cube.js/commit/cf133a9)), closes [#104](https://github.com/cube-js/cube.js/issues/104)





## [0.17.10](https://github.com/cube-js/cube.js/compare/v0.17.9...v0.17.10) (2020-02-20)


### Bug Fixes

* Revert "feat: Bump corejs ([#378](https://github.com/cube-js/cube.js/issues/378))" ([b21cbe6](https://github.com/cube-js/cube.js/commit/b21cbe6)), closes [#418](https://github.com/cube-js/cube.js/issues/418)
* uuidv4 upgrade ([c46c721](https://github.com/cube-js/cube.js/commit/c46c721))


### Features

* **cubejs-cli:** Add node_modules to .gitignore ([207544b](https://github.com/cube-js/cube.js/commit/207544b))
* Support external rollups from readonly source ([#395](https://github.com/cube-js/cube.js/issues/395)) ([b17e841](https://github.com/cube-js/cube.js/commit/b17e841))





## [0.17.9](https://github.com/cube-js/cube.js/compare/v0.17.8...v0.17.9) (2020-02-18)


### Features

* Add .gitignore with .env content to templates.js ([#403](https://github.com/cube-js/cube.js/issues/403)) ([c0d1a76](https://github.com/cube-js/cube.js/commit/c0d1a76)), closes [#402](https://github.com/cube-js/cube.js/issues/402)
* Bump corejs ([#378](https://github.com/cube-js/cube.js/issues/378)) ([cb8d51c](https://github.com/cube-js/cube.js/commit/cb8d51c))
* Enhanced trace logging ([1fdd8e9](https://github.com/cube-js/cube.js/commit/1fdd8e9))
* Extend meta response with aggregation type ([#394](https://github.com/cube-js/cube.js/issues/394)) Thanks to [@pyrooka](https://github.com/pyrooka)! ([06eed0b](https://github.com/cube-js/cube.js/commit/06eed0b))
* Request id trace span ([880f65e](https://github.com/cube-js/cube.js/commit/880f65e))





## [0.17.8](https://github.com/cube-js/cube.js/compare/v0.17.7...v0.17.8) (2020-02-14)


### Bug Fixes

* typings export ([#373](https://github.com/cube-js/cube.js/issues/373)) Thanks to [@lvauvillier](https://github.com/lvauvillier)! ([f4ea839](https://github.com/cube-js/cube.js/commit/f4ea839))
* Wrong interval functions for BigQuery ([#367](https://github.com/cube-js/cube.js/issues/367)) Thanks to [@lvauvillier](https://github.com/lvauvillier)! ([0e09d4d](https://github.com/cube-js/cube.js/commit/0e09d4d))
* **@cubejs-backend/oracle-driver:** a pre-built node-oracledb binary was not found for Node.js v12.16.0 ([#375](https://github.com/cube-js/cube.js/issues/375)) ([fd66bb6](https://github.com/cube-js/cube.js/commit/fd66bb6)), closes [#370](https://github.com/cube-js/cube.js/issues/370)
* **@cubejs-client/core:** improve types ([#376](https://github.com/cube-js/cube.js/issues/376)) Thanks to [@hassankhan](https://github.com/hassankhan)! ([cfb65a2](https://github.com/cube-js/cube.js/commit/cfb65a2))


### Features

* Athena HLL support ([45c7b83](https://github.com/cube-js/cube.js/commit/45c7b83))





## [0.17.7](https://github.com/cube-js/cube.js/compare/v0.17.6...v0.17.7) (2020-02-12)


### Bug Fixes

* Invalid Date: Incorrect MySQL minutes granularity ([dc553b9](https://github.com/cube-js/cube.js/commit/dc553b9))
* Respect MySQL TIMESTAMP strict mode on rollup downloads ([c72ab07](https://github.com/cube-js/cube.js/commit/c72ab07))
* Wrong typings ([c32fb0e](https://github.com/cube-js/cube.js/commit/c32fb0e))


### Features

* add bigquery-driver typings ([0c5e0f7](https://github.com/cube-js/cube.js/commit/0c5e0f7))
* add postgres-driver typings ([364d9bf](https://github.com/cube-js/cube.js/commit/364d9bf))
* add sqlite-driver typings ([4446eba](https://github.com/cube-js/cube.js/commit/4446eba))
* Cube.js agent ([35366aa](https://github.com/cube-js/cube.js/commit/35366aa))
* improve server-core typings ([9d59300](https://github.com/cube-js/cube.js/commit/9d59300))
* Set warn to be default log level for production logging ([c4298ea](https://github.com/cube-js/cube.js/commit/c4298ea))





## [0.17.6](https://github.com/cube-js/cube.js/compare/v0.17.5...v0.17.6) (2020-02-10)


### Bug Fixes

* `sqlAlias` isn't used for pre-aggregation table names ([b757175](https://github.com/cube-js/cube.js/commit/b757175))
* Multiplied measures rollup select case and leaf measure additive exact match ([c897dec](https://github.com/cube-js/cube.js/commit/c897dec))





## [0.17.5](https://github.com/cube-js/cube.js/compare/v0.17.4...v0.17.5) (2020-02-07)


### Bug Fixes

* Sanity check for silent truncate name problem during pre-aggregation creation ([e7fb2f2](https://github.com/cube-js/cube.js/commit/e7fb2f2))





## [0.17.4](https://github.com/cube-js/cube.js/compare/v0.17.3...v0.17.4) (2020-02-06)


### Bug Fixes

* Don't fetch schema twice when generating in Playground. Big schemas take a lot of time to fetch. ([3eeb73a](https://github.com/cube-js/cube.js/commit/3eeb73a))





## [0.17.3](https://github.com/cube-js/cube.js/compare/v0.17.2...v0.17.3) (2020-02-06)


### Bug Fixes

* Fix typescript type definition ([66e2fe5](https://github.com/cube-js/cube.js/commit/66e2fe5))


### Features

* Pre-aggregation indexes support ([d443585](https://github.com/cube-js/cube.js/commit/d443585))





## [0.17.2](https://github.com/cube-js/cube.js/compare/v0.17.1...v0.17.2) (2020-02-04)


### Bug Fixes

* Funnel step names cannot contain spaces ([aff1891](https://github.com/cube-js/cube.js/commit/aff1891)), closes [#359](https://github.com/cube-js/cube.js/issues/359)





## [0.17.1](https://github.com/cube-js/cube.js/compare/v0.17.0...v0.17.1) (2020-02-04)


### Bug Fixes

* TypeError: Cannot read property 'map' of undefined ([a12610d](https://github.com/cube-js/cube.js/commit/a12610d))





# [0.17.0](https://github.com/cube-js/cube.js/compare/v0.16.0...v0.17.0) (2020-02-04)

**Note:** Version bump only for package cubejs





# [0.16.0](https://github.com/cube-js/cube.js/compare/v0.15.4...v0.16.0) (2020-02-04)


### Bug Fixes

* Do not pad `last 24 hours` interval to day ([6554611](https://github.com/cube-js/cube.js/commit/6554611)), closes [#361](https://github.com/cube-js/cube.js/issues/361)


### Features

* Allow `null` filter values ([9e339f7](https://github.com/cube-js/cube.js/commit/9e339f7)), closes [#362](https://github.com/cube-js/cube.js/issues/362)





## [0.15.4](https://github.com/cube-js/cube.js/compare/v0.15.3...v0.15.4) (2020-02-02)


### Features

* Return `shortTitle` in `tableColumns()` result ([810c812](https://github.com/cube-js/cube.js/commit/810c812))





## [0.15.3](https://github.com/cube-js/cube.js/compare/v0.15.2...v0.15.3) (2020-01-26)


### Bug Fixes

* TypeError: Cannot read property 'title' of undefined ([3f76066](https://github.com/cube-js/cube.js/commit/3f76066))





## [0.15.2](https://github.com/cube-js/cube.js/compare/v0.15.1...v0.15.2) (2020-01-25)


### Bug Fixes

* **@cubejs-client/core:** improve types ([55edf85](https://github.com/cube-js/cube.js/commit/55edf85)), closes [#350](https://github.com/cube-js/cube.js/issues/350)
* Time dimension ResultSet backward compatibility to allow work newer client with old server ([b6834b1](https://github.com/cube-js/cube.js/commit/b6834b1)), closes [#356](https://github.com/cube-js/cube.js/issues/356)





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
* Skip contents for huge queries in dev logs ([c873a83](https://github.com/cube-js/cube.js/commit/c873a83))





## [0.14.2](https://github.com/cube-js/cube.js/compare/v0.14.1...v0.14.2) (2020-01-17)


### Bug Fixes

* TypeError: Cannot read property 'evaluateSymbolSqlWithContext' of undefined ([125afd7](https://github.com/cube-js/cube.js/commit/125afd7))





## [0.14.1](https://github.com/cube-js/cube.js/compare/v0.14.0...v0.14.1) (2020-01-17)


### Features

* Default refreshKey implementations for mutable and immutable pre-aggregations. ([bef0626](https://github.com/cube-js/cube.js/commit/bef0626))





# [0.14.0](https://github.com/cube-js/cube.js/compare/v0.13.12...v0.14.0) (2020-01-16)


### Bug Fixes

* Cannot read property 'requestId' of null ([d087837](https://github.com/cube-js/cube.js/commit/d087837)), closes [#347](https://github.com/cube-js/cube.js/issues/347)
* dateRange gets translated to incorrect value ([71d07e6](https://github.com/cube-js/cube.js/commit/71d07e6)), closes [#348](https://github.com/cube-js/cube.js/issues/348)
* Time dimension can't be selected twice within same query with and without granularity ([aa65129](https://github.com/cube-js/cube.js/commit/aa65129))


### Features

* Scheduled refresh for pre-aggregations ([c87b525](https://github.com/cube-js/cube.js/commit/c87b525))
* Scheduled Refresh REST API ([472a0c3](https://github.com/cube-js/cube.js/commit/472a0c3))





## [0.13.12](https://github.com/cube-js/cube.js/compare/v0.13.11...v0.13.12) (2020-01-12)

**Note:** Version bump only for package cubejs





## [0.13.11](https://github.com/cube-js/cube.js/compare/v0.13.10...v0.13.11) (2020-01-03)


### Bug Fixes

* Can't parse /node_modules/.bin/sha.js during dashboard creation ([e13ad50](https://github.com/cube-js/cube.js/commit/e13ad50))





## [0.13.10](https://github.com/cube-js/cube.js/compare/v0.13.9...v0.13.10) (2020-01-03)


### Bug Fixes

* More details for parsing errors during dashboard creation ([a8cb9d3](https://github.com/cube-js/cube.js/commit/a8cb9d3))





## [0.13.9](https://github.com/cube-js/cube.js/compare/v0.13.8...v0.13.9) (2020-01-03)


### Bug Fixes

* define context outside try-catch ([3075624](https://github.com/cube-js/cube.js/commit/3075624))


### Features

* **@cubejs-client/core:** add types ([abdf089](https://github.com/cube-js/cube.js/commit/abdf089))
* Improve logging ([8a692c1](https://github.com/cube-js/cube.js/commit/8a692c1))
* **mysql-driver:** Increase external pre-aggregations upload batch size ([741e26c](https://github.com/cube-js/cube.js/commit/741e26c))





## [0.13.8](https://github.com/cube-js/cube.js/compare/v0.13.7...v0.13.8) (2019-12-31)


### Bug Fixes

* UnhandledPromiseRejectionWarning: TypeError: Converting circular structure to JSON ([44c5065](https://github.com/cube-js/cube.js/commit/44c5065))





## [0.13.7](https://github.com/cube-js/cube.js/compare/v0.13.6...v0.13.7) (2019-12-31)


### Bug Fixes

* ER_TRUNCATED_WRONG_VALUE: Truncated incorrect datetime value ([fcbbe84](https://github.com/cube-js/cube.js/commit/fcbbe84)), closes [#309](https://github.com/cube-js/cube.js/issues/309)
* schemaVersion called with old context ([#293](https://github.com/cube-js/cube.js/issues/293)) ([da10e39](https://github.com/cube-js/cube.js/commit/da10e39)), closes [#294](https://github.com/cube-js/cube.js/issues/294)
* **client-core:** Uncaught TypeError: cubejs is not a function ([b5c32cd](https://github.com/cube-js/cube.js/commit/b5c32cd))


### Features

* Extendable context ([#299](https://github.com/cube-js/cube.js/issues/299)) ([38e33ce](https://github.com/cube-js/cube.js/commit/38e33ce)), closes [#295](https://github.com/cube-js/cube.js/issues/295) [#296](https://github.com/cube-js/cube.js/issues/296)
* Health check methods ([#308](https://github.com/cube-js/cube.js/issues/308)) Thanks to [@willhausman](https://github.com/willhausman)! ([49ca36b](https://github.com/cube-js/cube.js/commit/49ca36b))





## [0.13.6](https://github.com/cube-js/cube.js/compare/v0.13.5...v0.13.6) (2019-12-19)


### Bug Fixes

* Date parser returns 31 days for `last 30 days` date range ([bedbe9c](https://github.com/cube-js/cube.js/commit/bedbe9c)), closes [#303](https://github.com/cube-js/cube.js/issues/303)
* **elasticsearch-driver:** TypeError: Cannot convert undefined or null to object ([2dc570f](https://github.com/cube-js/cube.js/commit/2dc570f))





## [0.13.5](https://github.com/cube-js/cube.js/compare/v0.13.4...v0.13.5) (2019-12-17)


### Features

* Elasticsearch driver preview ([d6a6a07](https://github.com/cube-js/cube.js/commit/d6a6a07))
* Return key in the resultSet.series alongside title ([#291](https://github.com/cube-js/cube.js/issues/291)) ([6144a86](https://github.com/cube-js/cube.js/commit/6144a86))





## [0.13.4](https://github.com/cube-js/cube.js/compare/v0.13.3...v0.13.4) (2019-12-16)

**Note:** Version bump only for package cubejs





## [0.13.3](https://github.com/cube-js/cube.js/compare/v0.13.2...v0.13.3) (2019-12-16)


### Bug Fixes

* **sqlite-driver:** Fixed table schema parsing: support for escape characters ([#289](https://github.com/cube-js/cube.js/issues/289)). Thanks to [@philippefutureboy](https://github.com/philippefutureboy)! ([42026fb](https://github.com/cube-js/cube.js/commit/42026fb))
* Logging failing when pre-aggregations are built ([22f77a6](https://github.com/cube-js/cube.js/commit/22f77a6))


### Features

* d3-charts template package ([f9bd3fb](https://github.com/cube-js/cube.js/commit/f9bd3fb))
* **sqlite-driver:** Pre-aggregations support ([5ffb3d2](https://github.com/cube-js/cube.js/commit/5ffb3d2))





## [0.13.2](https://github.com/cube-js/cube.js/compare/v0.13.1...v0.13.2) (2019-12-13)


### Features

* Error type for returning specific http status codes ([#288](https://github.com/cube-js/cube.js/issues/288)). Thanks to [@willhausman](https://github.com/willhausman)! ([969e609](https://github.com/cube-js/cube.js/commit/969e609))
* hooks for dynamic schemas ([#287](https://github.com/cube-js/cube.js/issues/287)). Thanks to [@willhausman](https://github.com/willhausman)! ([47b256d](https://github.com/cube-js/cube.js/commit/47b256d))
* Propagate `requestId` for trace logging ([24d7b41](https://github.com/cube-js/cube.js/commit/24d7b41))





## [0.13.1](https://github.com/cube-js/cube.js/compare/v0.13.0...v0.13.1) (2019-12-10)


### Bug Fixes

* **api-gateway:** getTime on undefined call in case of web socket auth error ([9807b1e](https://github.com/cube-js/cube.js/commit/9807b1e))





# [0.13.0](https://github.com/cube-js/cube.js/compare/v0.12.3...v0.13.0) (2019-12-10)


### Bug Fixes

* cube validation from updating BasePreAggregation ([#285](https://github.com/cube-js/cube.js/issues/285)). Thanks to [@ferrants](https://github.com/ferrants)! ([f4bda4e](https://github.com/cube-js/cube.js/commit/f4bda4e))
* Errors during web socket subscribe returned with status 200 code ([6df008e](https://github.com/cube-js/cube.js/commit/6df008e))


### Features

* Minute and second granularities support ([34c5d4c](https://github.com/cube-js/cube.js/commit/34c5d4c))
* Sqlite driver implementation ([f9b43d3](https://github.com/cube-js/cube.js/commit/f9b43d3))





## [0.12.3](https://github.com/cube-js/cube.js/compare/v0.12.2...v0.12.3) (2019-12-02)

**Note:** Version bump only for package cubejs





## [0.12.2](https://github.com/cube-js/cube.js/compare/v0.12.1...v0.12.2) (2019-12-02)


### Bug Fixes

* this.versionEntries typo ([#279](https://github.com/cube-js/cube.js/issues/279)) ([743f9fb](https://github.com/cube-js/cube.js/commit/743f9fb))
* **cli:** update list of supported db based on document ([#281](https://github.com/cube-js/cube.js/issues/281)). Thanks to [@lanphan](https://github.com/lanphan)! ([8aa5a2e](https://github.com/cube-js/cube.js/commit/8aa5a2e))


### Features

* support REDIS_PASSWORD env variable ([#280](https://github.com/cube-js/cube.js/issues/280)). Thanks to [@lanphan](https://github.com/lanphan)! ([5172745](https://github.com/cube-js/cube.js/commit/5172745))





## [0.12.1](https://github.com/cube-js/cube.js/compare/v0.12.0...v0.12.1) (2019-11-26)


### Features

* Show used pre-aggregations and match rollup results in Playground ([4a67346](https://github.com/cube-js/cube.js/commit/4a67346))





# [0.12.0](https://github.com/cube-js/cube.js/compare/v0.11.25...v0.12.0) (2019-11-25)


### Features

* Show `refreshKey` values in Playground ([b49e184](https://github.com/cube-js/cube.js/commit/b49e184))





## [0.11.25](https://github.com/cube-js/cube.js/compare/v0.11.24...v0.11.25) (2019-11-23)


### Bug Fixes

* **playground:** Multiple conflicting packages applied at the same time: check for creation state before applying ([35f6325](https://github.com/cube-js/cube.js/commit/35f6325))


### Features

* playground receipes - update copy and previews ([b11a8c3](https://github.com/cube-js/cube.js/commit/b11a8c3))





## [0.11.24](https://github.com/cube-js/cube.js/compare/v0.11.23...v0.11.24) (2019-11-20)


### Bug Fixes

* Material UI template doesn't work ([deccca1](https://github.com/cube-js/cube.js/commit/deccca1))





## [0.11.23](https://github.com/cube-js/cube.js/compare/v0.11.22...v0.11.23) (2019-11-20)


### Features

* Enable web sockets by default in Express template ([815fb2c](https://github.com/cube-js/cube.js/commit/815fb2c))





## [0.11.22](https://github.com/cube-js/cube.js/compare/v0.11.21...v0.11.22) (2019-11-20)


### Bug Fixes

* Error: Router element is not found: Template Gallery source enumeration returns empty array ([459a4a7](https://github.com/cube-js/cube.js/commit/459a4a7))





## [0.11.21](https://github.com/cube-js/cube.js/compare/v0.11.20...v0.11.21) (2019-11-20)


### Features

* **schema-compiler:** Upgrade babel and support `objectRestSpread` for schema generation ([ac97c44](https://github.com/cube-js/cube.js/commit/ac97c44))
* Template gallery ([#272](https://github.com/cube-js/cube.js/issues/272)) ([f5ac516](https://github.com/cube-js/cube.js/commit/f5ac516))





## [0.11.20](https://github.com/cube-js/cube.js/compare/v0.11.19...v0.11.20) (2019-11-18)


### Bug Fixes

* Fix postgres driver timestamp parsing by using pg per-query type parser ([#269](https://github.com/cube-js/cube.js/issues/269)) Thanks to [@berndartmueller](https://github.com/berndartmueller)! ([458c0c9](https://github.com/cube-js/cube.js/commit/458c0c9)), closes [#265](https://github.com/cube-js/cube.js/issues/265)


### Features

*  support for pre-aggregation time hierarchies ([#258](https://github.com/cube-js/cube.js/issues/258)) Thanks to @Justin-ZS! ([ea78c84](https://github.com/cube-js/cube.js/commit/ea78c84)), closes [#246](https://github.com/cube-js/cube.js/issues/246)
* per cube `dataSource` support ([6dc3fef](https://github.com/cube-js/cube.js/commit/6dc3fef))





## [0.11.19](https://github.com/cube-js/cube.js/compare/v0.11.18...v0.11.19) (2019-11-16)


### Bug Fixes

* Merge back `sqlAlias` support ([80b312f](https://github.com/cube-js/cube.js/commit/80b312f))





## [0.11.18](https://github.com/cube-js/cube.js/compare/v0.11.17...v0.11.18) (2019-11-09)

**Note:** Version bump only for package cubejs





## [0.11.17](https://github.com/cube-js/cube.js/compare/v0.11.16...v0.11.17) (2019-11-08)


### Bug Fixes

* **server-core:** the schemaPath option does not work when generating schema ([#255](https://github.com/cube-js/cube.js/issues/255)) ([92f17b2](https://github.com/cube-js/cube.js/commit/92f17b2))
* Default Express middleware security check is ignored in production ([4bdf6bd](https://github.com/cube-js/cube.js/commit/4bdf6bd))


### Features

* Default root path message for servers running in production ([5b7ef41](https://github.com/cube-js/cube.js/commit/5b7ef41))





## [0.11.16](https://github.com/cube-js/cube.js/compare/v0.11.15...v0.11.16) (2019-11-04)


### Bug Fixes

* **vue:** Error: Invalid query format: "order" is not allowed ([e6a738a](https://github.com/cube-js/cube.js/commit/e6a738a))
* Respect timezone for natural language date parsing and align custom date ranges to dates by default to ensure backward compatibility ([af6f3c2](https://github.com/cube-js/cube.js/commit/af6f3c2))
* Respect timezone for natural language date parsing and align custom date ranges to dates by default to ensure backward compatibility ([2104492](https://github.com/cube-js/cube.js/commit/2104492))
* Use `node index.js` for `npm run dev` where available to ensure it starts servers with changed code ([527e274](https://github.com/cube-js/cube.js/commit/527e274))





## [0.11.15](https://github.com/cube-js/cube.js/compare/v0.11.14...v0.11.15) (2019-11-01)


### Bug Fixes

* Reduce output for logging ([aaf55e0](https://github.com/cube-js/cube.js/commit/aaf55e0))





## [0.11.14](https://github.com/cube-js/cube.js/compare/v0.11.13...v0.11.14) (2019-11-01)


### Bug Fixes

* Catch unhandled rejections on server starts ([fd9d872](https://github.com/cube-js/cube.js/commit/fd9d872))


### Features

* pretty default logger and log levels ([#244](https://github.com/cube-js/cube.js/issues/244)) ([b1302d2](https://github.com/cube-js/cube.js/commit/b1302d2))





## [0.11.13](https://github.com/cube-js/cube.js/compare/v0.11.12...v0.11.13) (2019-10-30)


### Features

* **playground:** Static dashboard template ([2458aad](https://github.com/cube-js/cube.js/commit/2458aad))





## [0.11.12](https://github.com/cube-js/cube.js/compare/v0.11.11...v0.11.12) (2019-10-29)


### Bug Fixes

* Playground shouldn't be run in serverless environment by default ([41cd46c](https://github.com/cube-js/cube.js/commit/41cd46c))
* **react:** Refetch hook only actual query changes ([10b8988](https://github.com/cube-js/cube.js/commit/10b8988))





## [0.11.11](https://github.com/cube-js/cube.js/compare/v0.11.10...v0.11.11) (2019-10-26)


### Bug Fixes

* **postgres-driver:** `CUBEJS_DB_SSL=false` should disable SSL ([85064bc](https://github.com/cube-js/cube.js/commit/85064bc))





## [0.11.10](https://github.com/cube-js/cube.js/compare/v0.11.9...v0.11.10) (2019-10-25)


### Features

* client headers for CubejsApi ([#242](https://github.com/cube-js/cube.js/issues/242)). Thanks to [@ferrants](https://github.com/ferrants)! ([2f75ef3](https://github.com/cube-js/cube.js/commit/2f75ef3)), closes [#241](https://github.com/cube-js/cube.js/issues/241)





## [0.11.9](https://github.com/cube-js/cube.js/compare/v0.11.8...v0.11.9) (2019-10-23)


### Bug Fixes

* Support `apiToken` to be an async function: first request sends incorrect token ([a2d0c77](https://github.com/cube-js/cube.js/commit/a2d0c77))





## [0.11.8](https://github.com/cube-js/cube.js/compare/v0.11.7...v0.11.8) (2019-10-22)


### Bug Fixes

* Pass `checkAuth` option to API Gateway ([d3d690e](https://github.com/cube-js/cube.js/commit/d3d690e))





## [0.11.7](https://github.com/cube-js/cube.js/compare/v0.11.6...v0.11.7) (2019-10-22)


### Features

* dynamic case label ([#236](https://github.com/cube-js/cube.js/issues/236)) ([1a82605](https://github.com/cube-js/cube.js/commit/1a82605)), closes [#235](https://github.com/cube-js/cube.js/issues/235)
* Support `apiToken` to be an async function ([3a3b5f5](https://github.com/cube-js/cube.js/commit/3a3b5f5))





## [0.11.6](https://github.com/cube-js/cube.js/compare/v0.11.5...v0.11.6) (2019-10-17)


### Bug Fixes

* Postgres driver with redis in non UTC timezone returns timezone shifted results ([f1346da](https://github.com/cube-js/cube.js/commit/f1346da))
* TypeError: Cannot read property 'table_name' of undefined: Drop orphaned tables implementation drops recent tables in cluster environments ([84ea78a](https://github.com/cube-js/cube.js/commit/84ea78a))
* Yesterday date range doesn't work ([6c81a02](https://github.com/cube-js/cube.js/commit/6c81a02))





## [0.11.5](https://github.com/cube-js/cube.js/compare/v0.11.4...v0.11.5) (2019-10-17)


### Bug Fixes

* **api-gateway:** TypeError: res.json is not a function ([7f3f0a8](https://github.com/cube-js/cube.js/commit/7f3f0a8))





## [0.11.4](https://github.com/cube-js/cube.js/compare/v0.11.3...v0.11.4) (2019-10-16)


### Bug Fixes

* Remove legacy scaffolding comments ([123a929](https://github.com/cube-js/cube.js/commit/123a929))
* TLS redirect is failing if cube.js listening on port other than 80 ([0fe92ec](https://github.com/cube-js/cube.js/commit/0fe92ec))





## [0.11.3](https://github.com/cube-js/cube.js/compare/v0.11.2...v0.11.3) (2019-10-15)


### Bug Fixes

* `useCubeQuery` doesn't reset error and resultSet on query change ([805d5b1](https://github.com/cube-js/cube.js/commit/805d5b1))





## [0.11.2](https://github.com/cube-js/cube.js/compare/v0.11.1...v0.11.2) (2019-10-15)


### Bug Fixes

* Error: ENOENT: no such file or directory, open 'Orders.js' ([74a8875](https://github.com/cube-js/cube.js/commit/74a8875))
* Incorrect URL generation in HttpTransport ([7e7020b](https://github.com/cube-js/cube.js/commit/7e7020b))





## [0.11.1](https://github.com/cube-js/cube.js/compare/v0.11.0...v0.11.1) (2019-10-15)


### Bug Fixes

* Error: Cannot find module './WebSocketServer' ([df3b074](https://github.com/cube-js/cube.js/commit/df3b074))





# [0.11.0](https://github.com/cube-js/cube.js/compare/v0.10.62...v0.11.0) (2019-10-15)


### Bug Fixes

* TypeError: Cannot destructure property authInfo of 'undefined' or 'null'. ([1886d13](https://github.com/cube-js/cube.js/commit/1886d13))


### Features

* Read schema subfolders ([#230](https://github.com/cube-js/cube.js/issues/230)). Thanks to [@lksilva](https://github.com/lksilva)! ([aa736b1](https://github.com/cube-js/cube.js/commit/aa736b1))
* Sockets Preview ([#231](https://github.com/cube-js/cube.js/issues/231)) ([89fc762](https://github.com/cube-js/cube.js/commit/89fc762)), closes [#221](https://github.com/cube-js/cube.js/issues/221)





## [0.10.62](https://github.com/cube-js/cube.js/compare/v0.10.61...v0.10.62) (2019-10-11)


### Features

* **vue:** Add order, renewQuery, and reactivity to Vue component ([#229](https://github.com/cube-js/cube.js/issues/229)). Thanks to @TCBroad ([9293f13](https://github.com/cube-js/cube.js/commit/9293f13))
* `ungrouped` queries support ([c6ac873](https://github.com/cube-js/cube.js/commit/c6ac873))





## [0.10.61](https://github.com/cube-js/cube.js/compare/v0.10.60...v0.10.61) (2019-10-10)


### Bug Fixes

* Override incorrect button color in playground ([6b7d964](https://github.com/cube-js/cube.js/commit/6b7d964))
* playground scaffolding include antd styles via index.css ([881084e](https://github.com/cube-js/cube.js/commit/881084e))
* **playground:** Chart type doesn't switch in Dashboard App ([23f604f](https://github.com/cube-js/cube.js/commit/23f604f))


### Features

* Scaffolding Updates React ([#228](https://github.com/cube-js/cube.js/issues/228)) ([552fd9c](https://github.com/cube-js/cube.js/commit/552fd9c))
* **react:** Introduce `useCubeQuery` react hook and `CubeProvider` cubejsApi context provider ([19b6fac](https://github.com/cube-js/cube.js/commit/19b6fac))
* **schema-compiler:** Allow access raw data in `USER_CONTEXT` using `unsafeValue()` method ([52ef146](https://github.com/cube-js/cube.js/commit/52ef146))





## [0.10.60](https://github.com/cube-js/cube.js/compare/v0.10.59...v0.10.60) (2019-10-08)


### Bug Fixes

* **client-ngx:** Support Observables for config: runtime token change case ([0e30773](https://github.com/cube-js/cube.js/commit/0e30773))





## [0.10.59](https://github.com/cube-js/cube.js/compare/v0.10.58...v0.10.59) (2019-10-08)


### Bug Fixes

* hostname: command not found ([8ca1f21](https://github.com/cube-js/cube.js/commit/8ca1f21))
* Rolling window returns dates in incorrect time zone for Postgres ([71a3baa](https://github.com/cube-js/cube.js/commit/71a3baa)), closes [#216](https://github.com/cube-js/cube.js/issues/216)





## [0.10.58](https://github.com/cube-js/cube.js/compare/v0.10.57...v0.10.58) (2019-10-04)


### Bug Fixes

* **playground:** Fix recharts height ([cd75409](https://github.com/cube-js/cube.js/commit/cd75409))
* `continueWaitTimout` option is ignored in LocalQueueDriver ([#224](https://github.com/cube-js/cube.js/issues/224)) ([4f72a52](https://github.com/cube-js/cube.js/commit/4f72a52))





## [0.10.57](https://github.com/cube-js/cube.js/compare/v0.10.56...v0.10.57) (2019-10-04)


### Bug Fixes

* **react:** Evade unnecessary heavy chart renders ([b1eb63f](https://github.com/cube-js/cube.js/commit/b1eb63f))





## [0.10.56](https://github.com/cube-js/cube.js/compare/v0.10.55...v0.10.56) (2019-10-04)


### Bug Fixes

* **react:** Evade unnecessary heavy chart renders ([bdcc569](https://github.com/cube-js/cube.js/commit/bdcc569))





## [0.10.55](https://github.com/cube-js/cube.js/compare/v0.10.54...v0.10.55) (2019-10-03)


### Bug Fixes

* **client-core:** can't read property 'title' of undefined ([4b48c7f](https://github.com/cube-js/cube.js/commit/4b48c7f))
* **playground:** Dashboard item name edit performance issues ([73df3c7](https://github.com/cube-js/cube.js/commit/73df3c7))
* **playground:** PropTypes validations ([3d5faa1](https://github.com/cube-js/cube.js/commit/3d5faa1))
* **playground:** Recharts fixes ([bce0313](https://github.com/cube-js/cube.js/commit/bce0313))





## [0.10.54](https://github.com/cube-js/cube.js/compare/v0.10.53...v0.10.54) (2019-10-02)

**Note:** Version bump only for package cubejs





## [0.10.53](https://github.com/cube-js/cube.js/compare/v0.10.52...v0.10.53) (2019-10-02)


### Bug Fixes

* **playground:** antd styles are added as part of table scaffolding ([8a39c9d](https://github.com/cube-js/cube.js/commit/8a39c9d))
* **playground:** Can't delete dashboard item name in dashboard app ([0cf546f](https://github.com/cube-js/cube.js/commit/0cf546f))
* **playground:** Recharts extra code ([950541c](https://github.com/cube-js/cube.js/commit/950541c))


### Features

* **client-react:** provide isQueryPresent() as static API method ([59dc5ce](https://github.com/cube-js/cube.js/commit/59dc5ce))
* **playground:** Make dashboard loading errors permanent ([155380d](https://github.com/cube-js/cube.js/commit/155380d))
* **playground:** Recharts code generation support ([c8c8230](https://github.com/cube-js/cube.js/commit/c8c8230))





## [0.10.52](https://github.com/cube-js/cube.js/compare/v0.10.51...v0.10.52) (2019-10-01)


### Bug Fixes

* **client-ngx:** client.ts is missing from the TypeScript compilation. Fix files ([f4885b4](https://github.com/cube-js/cube.js/commit/f4885b4))





## [0.10.51](https://github.com/cube-js/cube.js/compare/v0.10.50...v0.10.51) (2019-10-01)


### Bug Fixes

* **client-ngx:** client.ts is missing from the TypeScript compilation. Fix files ([8fe80f6](https://github.com/cube-js/cube.js/commit/8fe80f6))





## [0.10.50](https://github.com/cube-js/cube.js/compare/v0.10.49...v0.10.50) (2019-10-01)


### Bug Fixes

* **client-ngx:** client.ts is missing from the TypeScript compilation. Fix files ([ae5c2df](https://github.com/cube-js/cube.js/commit/ae5c2df))





## [0.10.49](https://github.com/cube-js/cube.js/compare/v0.10.48...v0.10.49) (2019-10-01)


### Bug Fixes

* **client-ngx:** client.ts is missing from the TypeScript compilation ([65a30cf](https://github.com/cube-js/cube.js/commit/65a30cf))





## [0.10.48](https://github.com/cube-js/cube.js/compare/v0.10.47...v0.10.48) (2019-10-01)


### Bug Fixes

* **client-ngx:** client.ts is missing from the TypeScript compilation ([ffab1a1](https://github.com/cube-js/cube.js/commit/ffab1a1))





## [0.10.47](https://github.com/cube-js/cube.js/compare/v0.10.46...v0.10.47) (2019-10-01)


### Bug Fixes

* **client-ngx:** client.ts is missing from the TypeScript compilation ([7dfc071](https://github.com/cube-js/cube.js/commit/7dfc071))





## [0.10.46](https://github.com/cube-js/cube.js/compare/v0.10.45...v0.10.46) (2019-09-30)


### Features

* Restructure Dashboard scaffolding to make it more user friendly and reliable ([78ba3bc](https://github.com/cube-js/cube.js/commit/78ba3bc))





## [0.10.45](https://github.com/cube-js/cube.js/compare/v0.10.44...v0.10.45) (2019-09-27)


### Bug Fixes

* TypeError: "listener" argument must be a function ([5cfc61e](https://github.com/cube-js/cube.js/commit/5cfc61e))





## [0.10.44](https://github.com/cube-js/cube.js/compare/v0.10.43...v0.10.44) (2019-09-27)


### Bug Fixes

* npm installs old dependencies on dashboard creation ([a7d519c](https://github.com/cube-js/cube.js/commit/a7d519c))
* **playground:** use default 3000 port for dashboard app as it's more appropriate ([ec4f3f4](https://github.com/cube-js/cube.js/commit/ec4f3f4))


### Features

* **cubejs-server:** Integrated support for TLS ([#213](https://github.com/cube-js/cube.js/issues/213)) ([66fe156](https://github.com/cube-js/cube.js/commit/66fe156))
* **playground:** Rename Explore to Build ([ce067a9](https://github.com/cube-js/cube.js/commit/ce067a9))
* **playground:** Show empty dashboard note ([ef559e5](https://github.com/cube-js/cube.js/commit/ef559e5))
* **playground:** Support various chart libraries for dashboard generation ([a4ba9c5](https://github.com/cube-js/cube.js/commit/a4ba9c5))





## [0.10.43](https://github.com/cube-js/cube.js/compare/v0.10.42...v0.10.43) (2019-09-27)


### Bug Fixes

* empty array reduce error in `stackedChartData` ([#211](https://github.com/cube-js/cube.js/issues/211)) ([1dc44bb](https://github.com/cube-js/cube.js/commit/1dc44bb))


### Features

* Dynamic dashboards ([#218](https://github.com/cube-js/cube.js/issues/218)) ([2c6cdc9](https://github.com/cube-js/cube.js/commit/2c6cdc9))





## [0.10.42](https://github.com/cube-js/cube.js/compare/v0.10.41...v0.10.42) (2019-09-16)


### Bug Fixes

* **client-ngx:** Function calls are not supported in decorators but 'ɵangular_packages_core_core_a' was called. ([65871f9](https://github.com/cube-js/cube.js/commit/65871f9))





## [0.10.41](https://github.com/cube-js/cube.js/compare/v0.10.40...v0.10.41) (2019-09-13)


### Bug Fixes

* support for deep nested watchers on 'QueryRenderer' ([#207](https://github.com/cube-js/cube.js/issues/207)) ([8d3a500](https://github.com/cube-js/cube.js/commit/8d3a500))


### Features

* Provide date filter with hourly granularity ([e423d82](https://github.com/cube-js/cube.js/commit/e423d82)), closes [#179](https://github.com/cube-js/cube.js/issues/179)





## [0.10.40](https://github.com/cube-js/cube.js/compare/v0.10.39...v0.10.40) (2019-09-09)


### Bug Fixes

* missed Vue.js build ([1cf22d5](https://github.com/cube-js/cube.js/commit/1cf22d5))





## [0.10.39](https://github.com/cube-js/cube.js/compare/v0.10.38...v0.10.39) (2019-09-09)


### Bug Fixes

* Requiring local node files is restricted: adding test for relative path resolvers ([f328d07](https://github.com/cube-js/cube.js/commit/f328d07))





## [0.10.38](https://github.com/cube-js/cube.js/compare/v0.10.37...v0.10.38) (2019-09-09)


### Bug Fixes

* Requiring local node files is restricted ([ba3c390](https://github.com/cube-js/cube.js/commit/ba3c390))





## [0.10.37](https://github.com/cube-js/cube.js/compare/v0.10.36...v0.10.37) (2019-09-09)


### Bug Fixes

* **client-ngx:** Omit warnings for Angular import: Use cjs module as main ([97e8d48](https://github.com/cube-js/cube.js/commit/97e8d48))





## [0.10.36](https://github.com/cube-js/cube.js/compare/v0.10.35...v0.10.36) (2019-09-09)


### Bug Fixes

* all queries forwarded to external DB instead of original one for zero pre-aggregation query ([2c230f4](https://github.com/cube-js/cube.js/commit/2c230f4))





## [0.10.35](https://github.com/cube-js/cube.js/compare/v0.10.34...v0.10.35) (2019-09-09)


### Bug Fixes

* LocalQueueDriver key interference for multitenant deployment ([aa860e4](https://github.com/cube-js/cube.js/commit/aa860e4))


### Features

* **mysql-driver:** Faster external pre-aggregations upload ([b6e3ee6](https://github.com/cube-js/cube.js/commit/b6e3ee6))
* `originalSql` external pre-aggregations support ([0db2282](https://github.com/cube-js/cube.js/commit/0db2282))
* Serve pre-aggregated data right from external database without hitting main one if pre-aggregation is available ([931fb7c](https://github.com/cube-js/cube.js/commit/931fb7c))





## [0.10.34](https://github.com/cube-js/cube.js/compare/v0.10.33...v0.10.34) (2019-09-06)


### Bug Fixes

* Athena timezone conversion issue for non-UTC server ([7085d2f](https://github.com/cube-js/cube.js/commit/7085d2f))





## [0.10.33](https://github.com/cube-js/cube.js/compare/v0.10.32...v0.10.33) (2019-09-06)


### Bug Fixes

* Revert to default queue concurrency for external pre-aggregations as driver pools expect this be aligned with default pool size ([c695ddd](https://github.com/cube-js/cube.js/commit/c695ddd))





## [0.10.32](https://github.com/cube-js/cube.js/compare/v0.10.31...v0.10.32) (2019-09-06)


### Bug Fixes

* In memory queue driver drop state if rollups are building too long ([ad4c062](https://github.com/cube-js/cube.js/commit/ad4c062))


### Features

* Speedup PG external pre-aggregations ([#201](https://github.com/cube-js/cube.js/issues/201)) ([7abf504](https://github.com/cube-js/cube.js/commit/7abf504)), closes [#200](https://github.com/cube-js/cube.js/issues/200)
* vue limit, offset and measure filters support ([#194](https://github.com/cube-js/cube.js/issues/194)) ([33f365a](https://github.com/cube-js/cube.js/commit/33f365a)), closes [#188](https://github.com/cube-js/cube.js/issues/188)





## [0.10.31](https://github.com/cube-js/cube.js/compare/v0.10.30...v0.10.31) (2019-08-27)


### Bug Fixes

* **athena-driver:** TypeError: Cannot read property 'map' of undefined ([478c6c6](https://github.com/cube-js/cube.js/commit/478c6c6))





## [0.10.30](https://github.com/cube-js/cube.js/compare/v0.10.29...v0.10.30) (2019-08-26)


### Bug Fixes

* Athena doesn't support `_` in contains filter ([d330be4](https://github.com/cube-js/cube.js/commit/d330be4))
* Athena doesn't support `'` in contains filter ([40a36d5](https://github.com/cube-js/cube.js/commit/40a36d5))


### Features

* `REDIS_TLS=true` env variable support ([55858cf](https://github.com/cube-js/cube.js/commit/55858cf))





## [0.10.29](https://github.com/cube-js/cube.js/compare/v0.10.28...v0.10.29) (2019-08-21)


### Bug Fixes

* MS SQL segment pre-aggregations support ([f8e37bf](https://github.com/cube-js/cube.js/commit/f8e37bf)), closes [#186](https://github.com/cube-js/cube.js/issues/186)





## [0.10.28](https://github.com/cube-js/cube.js/compare/v0.10.27...v0.10.28) (2019-08-19)


### Bug Fixes

* BigQuery to Postgres external rollup doesn't work ([feccdb5](https://github.com/cube-js/cube.js/commit/feccdb5)), closes [#178](https://github.com/cube-js/cube.js/issues/178)
* Presto error messages aren't showed correctly ([5f41afe](https://github.com/cube-js/cube.js/commit/5f41afe))
* Show dev server errors in console ([e8c3af9](https://github.com/cube-js/cube.js/commit/e8c3af9))





## [0.10.27](https://github.com/cube-js/cube.js/compare/v0.10.26...v0.10.27) (2019-08-18)


### Features

* Make `preAggregationsSchema` an option of CubejsServerCore - missed option propagation ([60d5704](https://github.com/cube-js/cube.js/commit/60d5704)), closes [#96](https://github.com/cube-js/cube.js/issues/96)





## [0.10.26](https://github.com/cube-js/cube.js/compare/v0.10.25...v0.10.26) (2019-08-18)


### Features

* Make `preAggregationsSchema` an option of CubejsServerCore ([3b1b082](https://github.com/cube-js/cube.js/commit/3b1b082)), closes [#96](https://github.com/cube-js/cube.js/issues/96)





## [0.10.25](https://github.com/cube-js/cube.js/compare/v0.10.24...v0.10.25) (2019-08-17)


### Bug Fixes

* MS SQL has unusual CREATE SCHEMA syntax ([16b8c87](https://github.com/cube-js/cube.js/commit/16b8c87)), closes [#185](https://github.com/cube-js/cube.js/issues/185)





## [0.10.24](https://github.com/cube-js/cube.js/compare/v0.10.23...v0.10.24) (2019-08-16)


### Bug Fixes

* MS SQL has unusual CTAS syntax ([1a00e4a](https://github.com/cube-js/cube.js/commit/1a00e4a)), closes [#185](https://github.com/cube-js/cube.js/issues/185)





## [0.10.23](https://github.com/cube-js/cube.js/compare/v0.10.22...v0.10.23) (2019-08-14)


### Bug Fixes

* Unexpected string literal Bigquery ([8768895](https://github.com/cube-js/cube.js/commit/8768895)), closes [#182](https://github.com/cube-js/cube.js/issues/182)





## [0.10.22](https://github.com/cube-js/cube.js/compare/v0.10.21...v0.10.22) (2019-08-09)


### Bug Fixes

* **clickhouse-driver:** Empty schema when CUBEJS_DB_NAME is provided ([7117e89](https://github.com/cube-js/cube.js/commit/7117e89))





## [0.10.21](https://github.com/cube-js/cube.js/compare/v0.10.20...v0.10.21) (2019-08-05)


### Features

* Offset pagination support ([7fb1715](https://github.com/cube-js/cube.js/commit/7fb1715)), closes [#117](https://github.com/cube-js/cube.js/issues/117)





## [0.10.20](https://github.com/cube-js/cube.js/compare/v0.10.19...v0.10.20) (2019-08-03)


### Features

* **playground:** Various dashboard hints ([eed2b55](https://github.com/cube-js/cube.js/commit/eed2b55))





## [0.10.19](https://github.com/cube-js/cube.js/compare/v0.10.18...v0.10.19) (2019-08-02)


### Bug Fixes

* **postgres-driver:** ERROR: type "string" does not exist ([d472e89](https://github.com/cube-js/cube.js/commit/d472e89)), closes [#176](https://github.com/cube-js/cube.js/issues/176)





## [0.10.18](https://github.com/cube-js/cube.js/compare/v0.10.17...v0.10.18) (2019-07-31)


### Bug Fixes

* BigQuery external rollup compatibility: use `__` separator for member aliases. Fix missed override. ([c1eb113](https://github.com/cube-js/cube.js/commit/c1eb113))





## [0.10.17](https://github.com/cube-js/cube.js/compare/v0.10.16...v0.10.17) (2019-07-31)


### Bug Fixes

* BigQuery external rollup compatibility: use `__` separator for member aliases. Fix all tests. ([723359c](https://github.com/cube-js/cube.js/commit/723359c))
* Moved joi dependency to it's new availability ([#171](https://github.com/cube-js/cube.js/issues/171)) ([1c20838](https://github.com/cube-js/cube.js/commit/1c20838))


### Features

* **playground:** Show editable files hint ([2dffe6c](https://github.com/cube-js/cube.js/commit/2dffe6c))
* **playground:** Slack and Docs links ([3270e70](https://github.com/cube-js/cube.js/commit/3270e70))





## [0.10.16](https://github.com/cube-js/cube.js/compare/v0.10.15...v0.10.16) (2019-07-20)


### Bug Fixes

* Added correct string concat for Mysql. ([#162](https://github.com/cube-js/cube.js/issues/162)) ([287411b](https://github.com/cube-js/cube.js/commit/287411b))
* remove redundant hacks: primaryKey filter for method dimensionColumns ([#161](https://github.com/cube-js/cube.js/issues/161)) ([f910a56](https://github.com/cube-js/cube.js/commit/f910a56))


### Features

* BigQuery external rollup support ([10c635c](https://github.com/cube-js/cube.js/commit/10c635c))
* Lean more on vue slots for state ([#148](https://github.com/cube-js/cube.js/issues/148)) ([e8af88d](https://github.com/cube-js/cube.js/commit/e8af88d))





## [0.10.15](https://github.com/cube-js/cube.js/compare/v0.10.14...v0.10.15) (2019-07-13)

**Note:** Version bump only for package cubejs





## [0.10.14](https://github.com/cube-js/cube.js/compare/v0.10.13...v0.10.14) (2019-07-13)


### Features

* **playground:** Show Query ([dc45fcb](https://github.com/cube-js/cube.js/commit/dc45fcb))
* Oracle driver ([#160](https://github.com/cube-js/cube.js/issues/160)) ([854ebff](https://github.com/cube-js/cube.js/commit/854ebff))





## [0.10.13](https://github.com/cube-js/cube.js/compare/v0.10.12...v0.10.13) (2019-07-08)


### Bug Fixes

* **bigquery-driver:** Error with Cube.js pre-aggregations in BigQuery ([01815a1](https://github.com/cube-js/cube.js/commit/01815a1)), closes [#158](https://github.com/cube-js/cube.js/issues/158)
* **cli:** update mem dependency security alert ([06a07a2](https://github.com/cube-js/cube.js/commit/06a07a2))


### Features

* **playground:** Copy code to clipboard ([30a2528](https://github.com/cube-js/cube.js/commit/30a2528))





## [0.10.12](https://github.com/cube-js/cube.js/compare/v0.10.11...v0.10.12) (2019-07-06)


### Bug Fixes

* Empty array for BigQuery in serverless GCP deployment ([#155](https://github.com/cube-js/cube.js/issues/155)) ([045094c](https://github.com/cube-js/cube.js/commit/045094c)), closes [#153](https://github.com/cube-js/cube.js/issues/153)
* QUERIES_undefined redis key for QueryQueue ([4c44886](https://github.com/cube-js/cube.js/commit/4c44886))


### Features

* **playground:** Links to Vanilla, Angular and Vue.js docs ([184495c](https://github.com/cube-js/cube.js/commit/184495c))





## [0.10.11](https://github.com/statsbotco/cube.js/compare/v0.10.10...v0.10.11) (2019-07-02)


### Bug Fixes

* TypeError: Cannot read property 'startsWith' of undefined at tableDefinition.filter.column: support uppercase databases ([995b115](https://github.com/statsbotco/cube.js/commit/995b115))





## [0.10.10](https://github.com/statsbotco/cube.js/compare/v0.10.9...v0.10.10) (2019-07-02)


### Bug Fixes

* **mongobi-driver:** accessing password field of undefined ([#147](https://github.com/statsbotco/cube.js/issues/147)) ([bdd9580](https://github.com/statsbotco/cube.js/commit/bdd9580))





## [0.10.9](https://github.com/statsbotco/cube.js/compare/v0.10.8...v0.10.9) (2019-06-30)


### Bug Fixes

* Syntax error during parsing: Unexpected token, expected: escaping back ticks ([9638a1a](https://github.com/statsbotco/cube.js/commit/9638a1a))


### Features

* **playground:** Chart.js charting library support ([40bb5d0](https://github.com/statsbotco/cube.js/commit/40bb5d0))





## [0.10.8](https://github.com/statsbotco/cube.js/compare/v0.10.7...v0.10.8) (2019-06-28)


### Features

* More readable compiling schema log message ([246805b](https://github.com/statsbotco/cube.js/commit/246805b))
* Presto driver ([1994083](https://github.com/statsbotco/cube.js/commit/1994083))





## [0.10.7](https://github.com/statsbotco/cube.js/compare/v0.10.6...v0.10.7) (2019-06-27)


### Bug Fixes

* config provided password not passed to server ([#145](https://github.com/statsbotco/cube.js/issues/145)) ([4b1afb1](https://github.com/statsbotco/cube.js/commit/4b1afb1))
* Module not found: Can't resolve 'react' ([a00e588](https://github.com/statsbotco/cube.js/commit/a00e588))





## [0.10.6](https://github.com/statsbotco/cube.js/compare/v0.10.5...v0.10.6) (2019-06-26)


### Bug Fixes

* Update version to fix audit warnings ([1bce587](https://github.com/statsbotco/cube.js/commit/1bce587))





## [0.10.5](https://github.com/statsbotco/cube.js/compare/v0.10.4...v0.10.5) (2019-06-26)


### Bug Fixes

* Update version to fix audit warnings ([f8f5225](https://github.com/statsbotco/cube.js/commit/f8f5225))





## [0.10.4](https://github.com/statsbotco/cube.js/compare/v0.10.3...v0.10.4) (2019-06-26)


### Bug Fixes

* Gray screen for Playground on version update ([b08333f](https://github.com/statsbotco/cube.js/commit/b08333f))


### Features

* More descriptive error for SyntaxError ([f6d12d3](https://github.com/statsbotco/cube.js/commit/f6d12d3))





## [0.10.3](https://github.com/statsbotco/cube.js/compare/v0.10.2...v0.10.3) (2019-06-26)


### Bug Fixes

* Snowflake driver config var typo ([d729b9d](https://github.com/statsbotco/cube.js/commit/d729b9d))





## [0.10.2](https://github.com/statsbotco/cube.js/compare/v0.10.1...v0.10.2) (2019-06-26)


### Bug Fixes

* Snowflake driver missing dependency ([b7620b3](https://github.com/statsbotco/cube.js/commit/b7620b3))





## [0.10.1](https://github.com/statsbotco/cube.js/compare/v0.10.0...v0.10.1) (2019-06-26)


### Features

* **cli:** Revert back concise next steps ([f4fa1e1](https://github.com/statsbotco/cube.js/commit/f4fa1e1))
* Snowflake driver ([35861b5](https://github.com/statsbotco/cube.js/commit/35861b5)), closes [#142](https://github.com/statsbotco/cube.js/issues/142)





# [0.10.0](https://github.com/statsbotco/cube.js/compare/v0.9.24...v0.10.0) (2019-06-21)


### Features

* **api-gateway:** `queryTransformer` security hook ([a9c41b2](https://github.com/statsbotco/cube.js/commit/a9c41b2))
* **playground:** App layout for dashboard ([f5578dd](https://github.com/statsbotco/cube.js/commit/f5578dd))
* **schema-compiler:** `asyncModules` and Node.js `require()`: support loading cube definitions from DB and other async sources ([397cceb](https://github.com/statsbotco/cube.js/commit/397cceb)), closes [#141](https://github.com/statsbotco/cube.js/issues/141)





## [0.9.24](https://github.com/statsbotco/cube.js/compare/v0.9.23...v0.9.24) (2019-06-17)


### Bug Fixes

* **mssql-driver:** Fix domain passed as an empty string case: ConnectionError: Login failed. The login is from an untrusted domain and cannot be used with Windows authentication ([89383dc](https://github.com/statsbotco/cube.js/commit/89383dc))
* Fix dev server in production mode message ([7586ad5](https://github.com/statsbotco/cube.js/commit/7586ad5))


### Features

* **mssql-driver:** Support query cancellation ([22a4bba](https://github.com/statsbotco/cube.js/commit/22a4bba))





## [0.9.23](https://github.com/statsbotco/cube.js/compare/v0.9.22...v0.9.23) (2019-06-17)


### Bug Fixes

* **hive:** Fix count when id is not defined ([5a5fffd](https://github.com/statsbotco/cube.js/commit/5a5fffd))
* **hive-driver:** SparkSQL compatibility ([1f20225](https://github.com/statsbotco/cube.js/commit/1f20225))





## [0.9.22](https://github.com/statsbotco/cube.js/compare/v0.9.21...v0.9.22) (2019-06-16)


### Bug Fixes

* **hive-driver:** Incorrect default Hive version ([379bff2](https://github.com/statsbotco/cube.js/commit/379bff2))





## [0.9.21](https://github.com/statsbotco/cube.js/compare/v0.9.20...v0.9.21) (2019-06-16)


### Features

* Hive dialect for simple queries ([30d4a30](https://github.com/statsbotco/cube.js/commit/30d4a30))





## [0.9.20](https://github.com/statsbotco/cube.js/compare/v0.9.19...v0.9.20) (2019-06-16)


### Bug Fixes

* **api-gateway:** Unexpected token u in JSON at position 0 at JSON.parse ([f95cea8](https://github.com/statsbotco/cube.js/commit/f95cea8))


### Features

* Pure JS Hive Thrift Driver ([4ca169e](https://github.com/statsbotco/cube.js/commit/4ca169e))





## [0.9.19](https://github.com/statsbotco/cube.js/compare/v0.9.18...v0.9.19) (2019-06-13)


### Bug Fixes

* **api-gateway:** handle can't parse date: Cannot read property 'end' of undefined ([a61b0da](https://github.com/statsbotco/cube.js/commit/a61b0da))
* **serverless:** remove redundant CUBEJS_API_URL env variable: Serverless offline framework support ([84a20b3](https://github.com/statsbotco/cube.js/commit/84a20b3)), closes [#121](https://github.com/statsbotco/cube.js/issues/121)
* Handle requests for hidden members: TypeError: Cannot read property 'type' of undefined at R.pipe.R.map.p ([5cdf71b](https://github.com/statsbotco/cube.js/commit/5cdf71b))
* Handle rollingWindow queries without dateRange: TypeError: Cannot read property '0' of undefined at BaseTimeDimension.dateFromFormatted ([409a238](https://github.com/statsbotco/cube.js/commit/409a238))
* issue with query generator for Mongobi for nested fields in document ([907b234](https://github.com/statsbotco/cube.js/commit/907b234)), closes [#56](https://github.com/statsbotco/cube.js/issues/56)
* More descriptive SyntaxError messages ([acd17ad](https://github.com/statsbotco/cube.js/commit/acd17ad))


### Features

* Add Typescript typings for server-core ([#111](https://github.com/statsbotco/cube.js/issues/111)) ([b1b895e](https://github.com/statsbotco/cube.js/commit/b1b895e))





## [0.9.18](https://github.com/statsbotco/cube.js/compare/v0.9.17...v0.9.18) (2019-06-12)


### Bug Fixes

* **mssql-driver:** Set default request timeout to 10 minutes ([c411484](https://github.com/statsbotco/cube.js/commit/c411484))





## [0.9.17](https://github.com/statsbotco/cube.js/compare/v0.9.16...v0.9.17) (2019-06-11)


### Bug Fixes

* **cli:** jdbc-driver fail hides db type not supported errors ([6f7c675](https://github.com/statsbotco/cube.js/commit/6f7c675))


### Features

* **mssql-driver:** Add domain env variable ([bb4c4a8](https://github.com/statsbotco/cube.js/commit/bb4c4a8))





## [0.9.16](https://github.com/statsbotco/cube.js/compare/v0.9.15...v0.9.16) (2019-06-10)


### Bug Fixes

* force escape cubeAlias to work with restricted column names such as "case" ([#128](https://github.com/statsbotco/cube.js/issues/128)) ([b8a59da](https://github.com/statsbotco/cube.js/commit/b8a59da))
* **playground:** Do not cache index.html to prevent missing resource errors on version upgrades ([4f20955](https://github.com/statsbotco/cube.js/commit/4f20955)), closes [#116](https://github.com/statsbotco/cube.js/issues/116)


### Features

* **cli:** Edit .env after app create help instruction ([f039c01](https://github.com/statsbotco/cube.js/commit/f039c01))
* **playground:** Go to explore modal after schema generation ([5325c2d](https://github.com/statsbotco/cube.js/commit/5325c2d))





## [0.9.15](https://github.com/statsbotco/cube.js/compare/v0.9.14...v0.9.15) (2019-06-07)


### Bug Fixes

* **schema-compiler:** subquery in FROM must have an alias -- fix Redshift rollingWindow ([70b752f](https://github.com/statsbotco/cube.js/commit/70b752f))





## [0.9.14](https://github.com/statsbotco/cube.js/compare/v0.9.13...v0.9.14) (2019-06-07)


### Features

* Add option to run in production without redis ([a7de417](https://github.com/statsbotco/cube.js/commit/a7de417)), closes [#110](https://github.com/statsbotco/cube.js/issues/110)
* Added SparkSQL and Hive support to the JDBC driver. ([#127](https://github.com/statsbotco/cube.js/issues/127)) ([659c24c](https://github.com/statsbotco/cube.js/commit/659c24c))
* View Query SQL in Playground ([8ef28c8](https://github.com/statsbotco/cube.js/commit/8ef28c8))





## [0.9.13](https://github.com/statsbotco/cube.js/compare/v0.9.12...v0.9.13) (2019-06-06)


### Bug Fixes

* Schema generation with joins having case sensitive table and column names ([#124](https://github.com/statsbotco/cube.js/issues/124)) ([c7b706a](https://github.com/statsbotco/cube.js/commit/c7b706a)), closes [#120](https://github.com/statsbotco/cube.js/issues/120) [#120](https://github.com/statsbotco/cube.js/issues/120)





## [0.9.12](https://github.com/statsbotco/cube.js/compare/v0.9.11...v0.9.12) (2019-06-03)


### Bug Fixes

* **api-gateway:** Unexpected token u in JSON at position 0 at JSON.parse ([91ca994](https://github.com/statsbotco/cube.js/commit/91ca994))
* **client-core:** Update normalizePivotConfig method to not to fail if x or y are missing ([ee20863](https://github.com/statsbotco/cube.js/commit/ee20863)), closes [#10](https://github.com/statsbotco/cube.js/issues/10)
* **schema-compiler:** cast parameters for IN filters ([28f3e48](https://github.com/statsbotco/cube.js/commit/28f3e48)), closes [#119](https://github.com/statsbotco/cube.js/issues/119)





## [0.9.11](https://github.com/statsbotco/cube.js/compare/v0.9.10...v0.9.11) (2019-05-31)


### Bug Fixes

* **client-core:** ResultSet series returns a series with no data ([715e170](https://github.com/statsbotco/cube.js/commit/715e170)), closes [#38](https://github.com/statsbotco/cube.js/issues/38)
* **schema-compiler:** TypeError: Cannot read property 'filterToWhere' of undefined ([6b399ea](https://github.com/statsbotco/cube.js/commit/6b399ea))





## [0.9.10](https://github.com/statsbotco/cube.js/compare/v0.9.9...v0.9.10) (2019-05-29)


### Bug Fixes

* **cli:** @cubejs-backend/schema-compiler/scaffolding/ScaffoldingTemplate dependency not found ([4296204](https://github.com/statsbotco/cube.js/commit/4296204))





## [0.9.9](https://github.com/statsbotco/cube.js/compare/v0.9.8...v0.9.9) (2019-05-29)


### Bug Fixes

* **cli:** missing package files ([81e8549](https://github.com/statsbotco/cube.js/commit/81e8549))





## [0.9.8](https://github.com/statsbotco/cube.js/compare/v0.9.7...v0.9.8) (2019-05-29)


### Features

* **cubejs-cli:** add token generation ([#67](https://github.com/statsbotco/cube.js/issues/67)) ([2813fed](https://github.com/statsbotco/cube.js/commit/2813fed))
* **postgres-driver:** SSL error hint for Heroku users ([0e9b9cb](https://github.com/statsbotco/cube.js/commit/0e9b9cb))





## [0.9.7](https://github.com/statsbotco/cube.js/compare/v0.9.6...v0.9.7) (2019-05-27)


### Features

* **postgres-driver:** support CUBEJS_DB_SSL option ([67a767e](https://github.com/statsbotco/cube.js/commit/67a767e))





## [0.9.6](https://github.com/statsbotco/cube.js/compare/v0.9.5...v0.9.6) (2019-05-24)


### Bug Fixes

* contains filter does not work with MS SQL Server database ([35210f6](https://github.com/statsbotco/cube.js/commit/35210f6)), closes [#113](https://github.com/statsbotco/cube.js/issues/113)


### Features

* better npm fail message in Playground ([545a020](https://github.com/statsbotco/cube.js/commit/545a020))
* **playground:** better add to dashboard error messages ([94e8dbf](https://github.com/statsbotco/cube.js/commit/94e8dbf))





## [0.9.5](https://github.com/statsbotco/cube.js/compare/v0.9.4...v0.9.5) (2019-05-22)


### Features

* Propagate `renewQuery` option from API to orchestrator ([9c640ba](https://github.com/statsbotco/cube.js/commit/9c640ba)), closes [#112](https://github.com/statsbotco/cube.js/issues/112)





## [0.9.4](https://github.com/statsbotco/cube.js/compare/v0.9.3...v0.9.4) (2019-05-22)


### Features

* Add `refreshKeyRenewalThreshold` option ([aa69449](https://github.com/statsbotco/cube.js/commit/aa69449)), closes [#112](https://github.com/statsbotco/cube.js/issues/112)





## [0.9.3](https://github.com/statsbotco/cube.js/compare/v0.9.2...v0.9.3) (2019-05-21)


### Bug Fixes

* **playground:** revert back create-react-app to npx as there're much more problems with global npm ([e434939](https://github.com/statsbotco/cube.js/commit/e434939))





## [0.9.2](https://github.com/statsbotco/cube.js/compare/v0.9.1...v0.9.2) (2019-05-11)


### Bug Fixes

* External rollups serverless implementation ([6d13370](https://github.com/statsbotco/cube.js/commit/6d13370))





## [0.9.1](https://github.com/statsbotco/cube.js/compare/v0.9.0...v0.9.1) (2019-05-11)


### Bug Fixes

* update BaseDriver dependencies ([a7aef2b](https://github.com/statsbotco/cube.js/commit/a7aef2b))





# [0.9.0](https://github.com/statsbotco/cube.js/compare/v0.8.7...v0.9.0) (2019-05-11)


### Features

* External rollup implementation ([d22a809](https://github.com/statsbotco/cube.js/commit/d22a809))





## [0.8.7](https://github.com/statsbotco/cube.js/compare/v0.8.6...v0.8.7) (2019-05-09)


### Bug Fixes

* **cubejs-react:** add core-js dependency ([#107](https://github.com/statsbotco/cube.js/issues/107)) ([0e13ffe](https://github.com/statsbotco/cube.js/commit/0e13ffe))
* **query-orchestrator:** Athena got swamped by fetch schema requests ([d8b5440](https://github.com/statsbotco/cube.js/commit/d8b5440))





## [0.8.6](https://github.com/statsbotco/cube.js/compare/v0.8.5...v0.8.6) (2019-05-05)


### Bug Fixes

* **cli:** Update Slack Community Link ([#101](https://github.com/statsbotco/cube.js/issues/101)) ([c5fd43f](https://github.com/statsbotco/cube.js/commit/c5fd43f))
* **playground:** Update Slack Community Link ([#102](https://github.com/statsbotco/cube.js/issues/102)) ([61a9bb0](https://github.com/statsbotco/cube.js/commit/61a9bb0))


### Features

* Replace codesandbox by running dashboard react-app directly ([861c817](https://github.com/statsbotco/cube.js/commit/861c817))





## [0.8.5](https://github.com/statsbotco/cube.js/compare/v0.8.4...v0.8.5) (2019-05-02)


### Bug Fixes

* **clickhouse-driver:** merging config with custom queryOptions which were not passing along the database ([#100](https://github.com/statsbotco/cube.js/issues/100)) ([dedc279](https://github.com/statsbotco/cube.js/commit/dedc279))





## [0.8.4](https://github.com/statsbotco/cube.js/compare/v0.8.3...v0.8.4) (2019-05-02)


### Features

* Angular client ([#99](https://github.com/statsbotco/cube.js/issues/99)) ([640e6de](https://github.com/statsbotco/cube.js/commit/640e6de))





## [0.8.3](https://github.com/statsbotco/cube.js/compare/v0.8.2...v0.8.3) (2019-05-01)


### Features

* clickhouse dialect implementation ([#98](https://github.com/statsbotco/cube.js/issues/98)) ([7236e29](https://github.com/statsbotco/cube.js/commit/7236e29)), closes [#93](https://github.com/statsbotco/cube.js/issues/93)





## [0.8.2](https://github.com/statsbotco/cube.js/compare/v0.8.1...v0.8.2) (2019-04-30)


### Bug Fixes

* Wrong variables when creating new BigQuery backed project ([bae6348](https://github.com/statsbotco/cube.js/commit/bae6348)), closes [#97](https://github.com/statsbotco/cube.js/issues/97)





## [0.8.1](https://github.com/statsbotco/cube.js/compare/v0.8.0...v0.8.1) (2019-04-30)


### Bug Fixes

* add the missing @cubejs-client/vue package ([#95](https://github.com/statsbotco/cube.js/issues/95)) ([9e8c4be](https://github.com/statsbotco/cube.js/commit/9e8c4be))


### Features

* Driver for ClickHouse database support ([#94](https://github.com/statsbotco/cube.js/issues/94)) ([0f05321](https://github.com/statsbotco/cube.js/commit/0f05321)), closes [#1](https://github.com/statsbotco/cube.js/issues/1)
* Serverless Google Cloud Platform in CLI support ([392ba1e](https://github.com/statsbotco/cube.js/commit/392ba1e))





# [0.8.0](https://github.com/statsbotco/cube.js/compare/v0.7.10...v0.8.0) (2019-04-29)


### Features

* Serverless Google Cloud Platform support ([89ec0ec](https://github.com/statsbotco/cube.js/commit/89ec0ec))





## [0.7.10](https://github.com/statsbotco/cube.js/compare/v0.7.9...v0.7.10) (2019-04-25)


### Bug Fixes

* **client-core:** Table pivot incorrectly behaves with multiple measures ([adb2270](https://github.com/statsbotco/cube.js/commit/adb2270))
* **client-core:** use ',' as standard axisValue delimiter ([e889955](https://github.com/statsbotco/cube.js/commit/e889955)), closes [#19](https://github.com/statsbotco/cube.js/issues/19)





## [0.7.9](https://github.com/statsbotco/cube.js/compare/v0.7.8...v0.7.9) (2019-04-24)


### Features

* **schema-compiler:** Allow to pass functions to USER_CONTEXT ([b489090](https://github.com/statsbotco/cube.js/commit/b489090)), closes [#88](https://github.com/statsbotco/cube.js/issues/88)





## [0.7.8](https://github.com/statsbotco/cube.js/compare/v0.7.7...v0.7.8) (2019-04-24)


### Bug Fixes

* **playground:** Dashboard doesn't work on Windows ([48a2ec4](https://github.com/statsbotco/cube.js/commit/48a2ec4)), closes [#82](https://github.com/statsbotco/cube.js/issues/82)





## [0.7.7](https://github.com/statsbotco/cube.js/compare/v0.7.6...v0.7.7) (2019-04-24)


### Bug Fixes

* **playground:** Dashboard doesn't work on Windows ([7c48aa4](https://github.com/statsbotco/cube.js/commit/7c48aa4)), closes [#82](https://github.com/statsbotco/cube.js/issues/82)





## [0.7.6](https://github.com/statsbotco/cube.js/compare/v0.7.5...v0.7.6) (2019-04-23)


### Bug Fixes

* **playground:** Cannot read property 'content' of undefined at e.value ([7392feb](https://github.com/statsbotco/cube.js/commit/7392feb))
* Use cross-fetch instead of isomorphic-fetch to allow React-Native builds ([#92](https://github.com/statsbotco/cube.js/issues/92)) ([79150f4](https://github.com/statsbotco/cube.js/commit/79150f4))
* **query-orchestrator:** add RedisFactory and promisify methods manually ([#89](https://github.com/statsbotco/cube.js/issues/89)) ([cdfcd87](https://github.com/statsbotco/cube.js/commit/cdfcd87)), closes [#84](https://github.com/statsbotco/cube.js/issues/84)


### Features

* Support member key in filters in query ([#91](https://github.com/statsbotco/cube.js/issues/91)) ([e1fccc0](https://github.com/statsbotco/cube.js/commit/e1fccc0))
* **schema-compiler:** Athena rollingWindow support ([f112c69](https://github.com/statsbotco/cube.js/commit/f112c69))





## [0.7.5](https://github.com/statsbotco/cube.js/compare/v0.7.4...v0.7.5) (2019-04-18)


### Bug Fixes

* **schema-compiler:** Athena, Mysql and BigQuery doesn't respect multiple contains filter ([0a8f324](https://github.com/statsbotco/cube.js/commit/0a8f324))





## [0.7.4](https://github.com/statsbotco/cube.js/compare/v0.7.3...v0.7.4) (2019-04-17)


### Bug Fixes

* Make dashboard app creation explicit. Show error messages if dashboard failed to create. ([3b2a22b](https://github.com/statsbotco/cube.js/commit/3b2a22b))
* **api-gateway:** measures is always required ([04adb7d](https://github.com/statsbotco/cube.js/commit/04adb7d))
* **mongobi-driver:** fix ssl configuration ([#78](https://github.com/statsbotco/cube.js/issues/78)) ([ddc4dff](https://github.com/statsbotco/cube.js/commit/ddc4dff))





## [0.7.3](https://github.com/statsbotco/cube.js/compare/v0.7.2...v0.7.3) (2019-04-16)


### Bug Fixes

* Allow SSR: use isomorphic-fetch instead of whatwg-fetch. ([902e581](https://github.com/statsbotco/cube.js/commit/902e581)), closes [#1](https://github.com/statsbotco/cube.js/issues/1)





## [0.7.2](https://github.com/statsbotco/cube.js/compare/v0.7.1...v0.7.2) (2019-04-15)


### Bug Fixes

* Avoid 502 for Playground in serverless: minimize babel ([f9d3171](https://github.com/statsbotco/cube.js/commit/f9d3171))


### Features

* MS SQL database driver ([48fbe66](https://github.com/statsbotco/cube.js/commit/48fbe66)), closes [#76](https://github.com/statsbotco/cube.js/issues/76)





## [0.7.1](https://github.com/statsbotco/cube.js/compare/v0.7.0...v0.7.1) (2019-04-15)


### Bug Fixes

* **serverless:** `getApiHandler` called on undefined ([0ee5121](https://github.com/statsbotco/cube.js/commit/0ee5121))
* Allow Playground to work in Serverless mode ([2c0c89c](https://github.com/statsbotco/cube.js/commit/2c0c89c))





# [0.7.0](https://github.com/statsbotco/cube.js/compare/v0.6.2...v0.7.0) (2019-04-15)


### Features

* App multi-tenancy support in single ServerCore instance ([6f0220f](https://github.com/statsbotco/cube.js/commit/6f0220f))





## [0.6.2](https://github.com/statsbotco/cube.js/compare/v0.6.1...v0.6.2) (2019-04-12)


### Features

* Natural language date range support ([b962e80](https://github.com/statsbotco/cube.js/commit/b962e80))
* **api-gateway:** Order support ([670237b](https://github.com/statsbotco/cube.js/commit/670237b))





## [0.6.1](https://github.com/statsbotco/cube.js/compare/v0.6.0...v0.6.1) (2019-04-11)


### Bug Fixes

* Get Playground API_URL from window.location until provided explicitly in env. Remote server playground case. ([7b1a0ff](https://github.com/statsbotco/cube.js/commit/7b1a0ff))


### Features

* Disable authentication checks in developer mode ([bc09eba](https://github.com/statsbotco/cube.js/commit/bc09eba))
* Formatted error logging in developer mode ([3376a50](https://github.com/statsbotco/cube.js/commit/3376a50))





# [0.6.0](https://github.com/statsbotco/cube.js/compare/v0.5.2...v0.6.0) (2019-04-09)


### Bug Fixes

* **playground:** no such file or directory, scandir 'dashboard-app/src' ([64ec481](https://github.com/statsbotco/cube.js/commit/64ec481))


### Features

* query validation added in api-gateway ([#73](https://github.com/statsbotco/cube.js/issues/73)) ([21f6176](https://github.com/statsbotco/cube.js/commit/21f6176)), closes [#39](https://github.com/statsbotco/cube.js/issues/39)
* QueryBuilder heuristics. Playground area, table and number implementation. ([c883a48](https://github.com/statsbotco/cube.js/commit/c883a48))
* Vue.js reactivity on query update ([#70](https://github.com/statsbotco/cube.js/issues/70)) ([167fdbf](https://github.com/statsbotco/cube.js/commit/167fdbf))





## [0.5.2](https://github.com/statsbotco/cube.js/compare/v0.5.1...v0.5.2) (2019-04-05)


### Features

* Add redshift to postgres driver link ([#71](https://github.com/statsbotco/cube.js/issues/71)) ([4797588](https://github.com/statsbotco/cube.js/commit/4797588))
* Playground UX improvements ([6760a1d](https://github.com/statsbotco/cube.js/commit/6760a1d))





## [0.5.1](https://github.com/statsbotco/cube.js/compare/v0.5.0...v0.5.1) (2019-04-02)


### Features

* BigQuery driver ([654edac](https://github.com/statsbotco/cube.js/commit/654edac))
* Vue package improvements and docs ([fc38e69](https://github.com/statsbotco/cube.js/commit/fc38e69)), closes [#68](https://github.com/statsbotco/cube.js/issues/68)





# [0.5.0](https://github.com/statsbotco/cube.js/compare/v0.4.6...v0.5.0) (2019-04-01)


### Bug Fixes

* **schema-compiler:** joi@10.6.0 upgrade to joi@14.3.1 ([#59](https://github.com/statsbotco/cube.js/issues/59)) ([f035531](https://github.com/statsbotco/cube.js/commit/f035531))
* mongobi issue with parsing schema file with nested fields ([eaf1631](https://github.com/statsbotco/cube.js/commit/eaf1631)), closes [#55](https://github.com/statsbotco/cube.js/issues/55)


### Features

* add basic vue support ([#65](https://github.com/statsbotco/cube.js/issues/65)) ([f45468b](https://github.com/statsbotco/cube.js/commit/f45468b))
* use local queue and cache for local dev server instead of Redis one ([50f1bbb](https://github.com/statsbotco/cube.js/commit/50f1bbb))





## [0.4.6](https://github.com/statsbotco/cube.js/compare/v0.4.5...v0.4.6) (2019-03-27)


### Features

* Dashboard Generator for Playground ([28a42ee](https://github.com/statsbotco/cube.js/commit/28a42ee))





## [0.4.5](https://github.com/statsbotco/cube.js/compare/v0.4.4...v0.4.5) (2019-03-21)


### Bug Fixes

* client-react - query prop now has default blank value ([#54](https://github.com/statsbotco/cube.js/issues/54)) ([27e7090](https://github.com/statsbotco/cube.js/commit/27e7090))


### Features

* Make API path namespace configurable ([#53](https://github.com/statsbotco/cube.js/issues/53)) ([b074a3d](https://github.com/statsbotco/cube.js/commit/b074a3d))
* Playground filters implementation ([de4315d](https://github.com/statsbotco/cube.js/commit/de4315d))





## [0.4.4](https://github.com/statsbotco/cube.js/compare/v0.4.3...v0.4.4) (2019-03-17)


### Bug Fixes

* Postgres doesn't show any data for queries with time dimension. ([e95e6fe](https://github.com/statsbotco/cube.js/commit/e95e6fe))


### Features

* Introduce Schema generation UI in Playground ([349c7d0](https://github.com/statsbotco/cube.js/commit/349c7d0))





## [0.4.3](https://github.com/statsbotco/cube.js/compare/v0.4.2...v0.4.3) (2019-03-15)


### Bug Fixes

* **mongobi-driver:** implement `convert_tz` as a simple hour shift ([c97e451](https://github.com/statsbotco/cube.js/commit/c97e451)), closes [#50](https://github.com/statsbotco/cube.js/issues/50)





## [0.4.2](https://github.com/statsbotco/cube.js/compare/v0.4.1...v0.4.2) (2019-03-14)


### Bug Fixes

* **mongobi-driver:** Fix Server does not support secure connnection on connection to localhost ([3202508](https://github.com/statsbotco/cube.js/commit/3202508))





## [0.4.1](https://github.com/statsbotco/cube.js/compare/v0.4.0...v0.4.1) (2019-03-14)


### Bug Fixes

* concat called on undefined for empty MongoDB password ([7d75b1e](https://github.com/statsbotco/cube.js/commit/7d75b1e))


### Features

* Allow to use custom checkAuth middleware ([19d5cd8](https://github.com/statsbotco/cube.js/commit/19d5cd8)), closes [#42](https://github.com/statsbotco/cube.js/issues/42)





# [0.4.0](https://github.com/statsbotco/cube.js/compare/v0.3.5-alpha.0...v0.4.0) (2019-03-13)


### Features

* Add MongoBI connector and schema adapter support ([3ebbbf0](https://github.com/statsbotco/cube.js/commit/3ebbbf0))





## [0.3.5-alpha.0](https://github.com/statsbotco/cube.js/compare/v0.3.5...v0.3.5-alpha.0) (2019-03-12)

**Note:** Version bump only for package cubejs
