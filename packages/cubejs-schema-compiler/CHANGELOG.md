# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.29.33](https://github.com/cube-js/cube.js/compare/v0.29.32...v0.29.33) (2022-03-17)


### Features

* **playground:** non-additive measures message ([#4236](https://github.com/cube-js/cube.js/issues/4236)) ([ae18bbc](https://github.com/cube-js/cube.js/commit/ae18bbcb9030d0eef03c74410c25902602ec6d43))





## [0.29.31](https://github.com/cube-js/cube.js/compare/v0.29.30...v0.29.31) (2022-03-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.29.29](https://github.com/cube-js/cube.js/compare/v0.29.28...v0.29.29) (2022-03-03)


### Bug Fixes

* **@cubejs-backend/schema-compiler:** Add strictness to booleans ([#4157](https://github.com/cube-js/cube.js/issues/4157)) Thanks [@zpencerq](https://github.com/zpencerq)! ([e918837](https://github.com/cube-js/cube.js/commit/e918837ec8c5eb7965620f43408a92f4c2b2bec5))





## [0.29.28](https://github.com/cube-js/cube.js/compare/v0.29.27...v0.29.28) (2022-02-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.29.27](https://github.com/cube-js/cube.js/compare/v0.29.26...v0.29.27) (2022-02-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.29.26](https://github.com/cube-js/cube.js/compare/v0.29.25...v0.29.26) (2022-02-07)


### Bug Fixes

* Use prototype name matching instead of classes to allow exact version mismatches for AbstractExtension ([75545e8](https://github.com/cube-js/cube.js/commit/75545e8ffbf88fd693e4c8c4afd78d86830925b2))





## [0.29.25](https://github.com/cube-js/cube.js/compare/v0.29.24...v0.29.25) (2022-02-03)


### Features

* Load metrics from DBT project ([#4000](https://github.com/cube-js/cube.js/issues/4000)) ([2975d84](https://github.com/cube-js/cube.js/commit/2975d84cd2a2d3bba3c31a7744ab5a5fb3789b6e))





## [0.29.24](https://github.com/cube-js/cube.js/compare/v0.29.23...v0.29.24) (2022-02-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.29.23](https://github.com/cube-js/cube.js/compare/v0.29.22...v0.29.23) (2022-01-26)


### Bug Fixes

* Error: column does not exist during in case of subQuery for rolling window measure ([6084407](https://github.com/cube-js/cube.js/commit/6084407cb7cad3f0d239959b072f4fa011aa29a4))





## [0.29.22](https://github.com/cube-js/cube.js/compare/v0.29.21...v0.29.22) (2022-01-21)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.29.21](https://github.com/cube-js/cube.js/compare/v0.29.20...v0.29.21) (2022-01-17)


### Features

* **@cubejs-backend/schema-compiler:** extend the schema generation API ([#3936](https://github.com/cube-js/cube.js/issues/3936)) ([48b2335](https://github.com/cube-js/cube.js/commit/48b2335c7d9810dc433fd8c76f4b3ec8a7b83442))





## [0.29.20](https://github.com/cube-js/cube.js/compare/v0.29.19...v0.29.20) (2022-01-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.29.18](https://github.com/cube-js/cube.js/compare/v0.29.17...v0.29.18) (2022-01-09)


### Bug Fixes

* **schema-compiler:** Handle null Values in Security Context ([#3868](https://github.com/cube-js/cube.js/issues/3868)) Thanks [@joealden](https://github.com/joealden) ! ([739367b](https://github.com/cube-js/cube.js/commit/739367bd50c1863ee8bae17741c0591065ea47a1))





## [0.29.17](https://github.com/cube-js/cube.js/compare/v0.29.16...v0.29.17) (2022-01-05)


### Bug Fixes

* Do not instantiate SqlParser if rewriteQueries is false to save cache memory ([00a239f](https://github.com/cube-js/cube.js/commit/00a239fae57ae9448337bd816b2ae5ca17f15230))





## [0.29.16](https://github.com/cube-js/cube.js/compare/v0.29.15...v0.29.16) (2022-01-05)


### Bug Fixes

* `refreshKey` is evaluated ten times more frequently if `sql` and `every` are simultaneously defined ([#3873](https://github.com/cube-js/cube.js/issues/3873)) ([c93ae12](https://github.com/cube-js/cube.js/commit/c93ae127b4d0ce2c214d3665f8d86f6ade5cce6f))





## [0.29.15](https://github.com/cube-js/cube.js/compare/v0.29.14...v0.29.15) (2021-12-30)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.29.12](https://github.com/cube-js/cube.js/compare/v0.29.11...v0.29.12) (2021-12-29)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.29.7](https://github.com/cube-js/cube.js/compare/v0.29.6...v0.29.7) (2021-12-20)


### Bug Fixes

* Table cache incorrectly invalidated after merge multi-tenant queues by default change ([#3828](https://github.com/cube-js/cube.js/issues/3828)) ([540446a](https://github.com/cube-js/cube.js/commit/540446a76e19be3aec38b96ad81c149f567e9e40))





## [0.29.6](https://github.com/cube-js/cube.js/compare/v0.29.5...v0.29.6) (2021-12-19)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.29.4](https://github.com/cube-js/cube.js/compare/v0.29.3...v0.29.4) (2021-12-16)


### Bug Fixes

* Validate `contextToAppId` is in place when COMPILE_CONTEXT is used ([54a8b84](https://github.com/cube-js/cube.js/commit/54a8b843eca965c6a098118da04ec480ea06fa76))





# [0.29.0](https://github.com/cube-js/cube.js/compare/v0.28.67...v0.29.0) (2021-12-14)


### Reverts

* Revert "BREAKING CHANGE: 0.29 (#3809)" (#3811) ([db005ed](https://github.com/cube-js/cube.js/commit/db005edc04d48e8251250ab9d0e19f496cf3b52b)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809) [#3811](https://github.com/cube-js/cube.js/issues/3811)


* BREAKING CHANGE: 0.29 (#3809) ([6f1418b](https://github.com/cube-js/cube.js/commit/6f1418b9963774844f341682e594601a56bb0084)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809)


### BREAKING CHANGES

* Drop support for Node.js 10 (12.x is a minimal version)
* Upgrade Node.js to 14 for Docker images
* Drop support for Node.js 15





## [0.28.64](https://github.com/cube-js/cube.js/compare/v0.28.63...v0.28.64) (2021-12-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.63](https://github.com/cube-js/cube.js/compare/v0.28.62...v0.28.63) (2021-12-03)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.61](https://github.com/cube-js/cube.js/compare/v0.28.60...v0.28.61) (2021-11-30)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.60](https://github.com/cube-js/cube.js/compare/v0.28.59...v0.28.60) (2021-11-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.59](https://github.com/cube-js/cube.js/compare/v0.28.58...v0.28.59) (2021-11-21)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.58](https://github.com/cube-js/cube.js/compare/v0.28.57...v0.28.58) (2021-11-18)


### Bug Fixes

* **@cubejs-backend/clickhouse-driver:** clickhouse joins full key query aggregate fails ([#3600](https://github.com/cube-js/cube.js/issues/3600)) Thanks [@antnmxmv](https://github.com/antnmxmv)! ([c6451cd](https://github.com/cube-js/cube.js/commit/c6451cdef8498d4d645167ed3da7cf2f599a2e5b)), closes [#3534](https://github.com/cube-js/cube.js/issues/3534)





## [0.28.56](https://github.com/cube-js/cube.js/compare/v0.28.55...v0.28.56) (2021-11-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.55](https://github.com/cube-js/cube.js/compare/v0.28.54...v0.28.55) (2021-11-12)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.52](https://github.com/cube-js/cube.js/compare/v0.28.51...v0.28.52) (2021-11-03)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.50](https://github.com/cube-js/cube.js/compare/v0.28.49...v0.28.50) (2021-10-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.48](https://github.com/cube-js/cube.js/compare/v0.28.47...v0.28.48) (2021-10-22)


### Bug Fixes

* Use BaseQuery#evaluateSql() for evaluate refresh range references, pre-aggregations debug API ([#3352](https://github.com/cube-js/cube.js/issues/3352)) ([ea81650](https://github.com/cube-js/cube.js/commit/ea816509ee9c07707bb46fc8fac83e55c52aaf00))
* **@cubejs-backend/ksql-driver:** Scaffolding for empty schema generates empty prefix ([091e45c](https://github.com/cube-js/cube.js/commit/091e45c66b712491699856d0a203e442bdfbd888))





## [0.28.47](https://github.com/cube-js/cube.js/compare/v0.28.46...v0.28.47) (2021-10-22)


### Features

* ksql support ([#3507](https://github.com/cube-js/cube.js/issues/3507)) ([b7128d4](https://github.com/cube-js/cube.js/commit/b7128d43d2aaffdd7273555779176b3efe4e2aa6))





## [0.28.46](https://github.com/cube-js/cube.js/compare/v0.28.45...v0.28.46) (2021-10-20)


### Bug Fixes

* update error message for join across data sources ([#3435](https://github.com/cube-js/cube.js/issues/3435)) ([5ad72cc](https://github.com/cube-js/cube.js/commit/5ad72ccf0f6bbba3c362b04427b172210f0b8ada))
* **@cubejs-backend/snowflake-driver:** escape date_from and date_to in generated series SQL ([#3542](https://github.com/cube-js/cube.js/issues/3542)) Thanks to [@zpencerq](https://github.com/zpencerq) ! ([858b7fa](https://github.com/cube-js/cube.js/commit/858b7fa2dbc5ed08350a6f875189f6f608c6d55c)), closes [#3215](https://github.com/cube-js/cube.js/issues/3215)
* **schema-compiler:** assign isVisible to segments ([#3484](https://github.com/cube-js/cube.js/issues/3484)) Thanks to [@piktur](https://github.com/piktur)! ([53fdf27](https://github.com/cube-js/cube.js/commit/53fdf27bb522608f36343c4a14ef32bf64c43200))





## [0.28.42](https://github.com/cube-js/cube.js/compare/v0.28.41...v0.28.42) (2021-10-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.41](https://github.com/cube-js/cube.js/compare/v0.28.40...v0.28.41) (2021-10-12)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.40](https://github.com/cube-js/cube.js/compare/v0.28.39...v0.28.40) (2021-09-30)


### Bug Fixes

* Count distinct by week matches daily rollup in case of data range is daily ([#3490](https://github.com/cube-js/cube.js/issues/3490)) ([2401418](https://github.com/cube-js/cube.js/commit/24014188d63baa6f178242e738f72790369234a9))
* **@cubejs-backend/schema-compiler:** check segments when matching pre-aggregations ([#3494](https://github.com/cube-js/cube.js/issues/3494)) ([9357484](https://github.com/cube-js/cube.js/commit/9357484cdc924046b7371c7b701d307b1df84089))
* **@cubejs-backend/schema-compiler:** CubePropContextTranspiler expli… ([#3461](https://github.com/cube-js/cube.js/issues/3461)) ([2ae7f1d](https://github.com/cube-js/cube.js/commit/2ae7f1d51b0caca0fe9755a98463a6898960a11f))
* **@cubejs-backend/schema-compiler:** match query with no dimensions … ([#3472](https://github.com/cube-js/cube.js/issues/3472)) ([2a5dd4c](https://github.com/cube-js/cube.js/commit/2a5dd4cbe4805d9dea8f5f8e630bf613198195a3))





## [0.28.38](https://github.com/cube-js/cube.js/compare/v0.28.37...v0.28.38) (2021-09-20)


### Bug Fixes

* **@cubejs-backend/schema-compiler:** CubeValidator human readable error messages ([#3425](https://github.com/cube-js/cube.js/issues/3425)) ([22db0a6](https://github.com/cube-js/cube.js/commit/22db0a6b607b7ef1d2cbe399415ac64c639325f5))





## [0.28.37](https://github.com/cube-js/cube.js/compare/v0.28.36...v0.28.37) (2021-09-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.35](https://github.com/cube-js/cube.js/compare/v0.28.34...v0.28.35) (2021-09-13)


### Bug Fixes

* **gateway:** hidden members filtering ([#3384](https://github.com/cube-js/cube.js/issues/3384)) ([43ac8c3](https://github.com/cube-js/cube.js/commit/43ac8c30247944543f917cd893787ee8485fc985))





## [0.28.34](https://github.com/cube-js/cube.js/compare/v0.28.33...v0.28.34) (2021-09-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.33](https://github.com/cube-js/cube.js/compare/v0.28.32...v0.28.33) (2021-09-11)


### Bug Fixes

* `updateWindow` validation isn't consistent with `refreshKey` interval parsing -- allow `s` at the end of time interval ([#3403](https://github.com/cube-js/cube.js/issues/3403)) ([57559e7](https://github.com/cube-js/cube.js/commit/57559e7841657e1900a30522ce5a178d091b6474))





## [0.28.32](https://github.com/cube-js/cube.js/compare/v0.28.31...v0.28.32) (2021-09-06)


### Bug Fixes

* HLL Rolling window query fails ([#3380](https://github.com/cube-js/cube.js/issues/3380)) ([581a52a](https://github.com/cube-js/cube.js/commit/581a52a856aeee067bac9a680e22694bf507af04))





## [0.28.29](https://github.com/cube-js/cube.js/compare/v0.28.28...v0.28.29) (2021-08-31)


### Features

* Mixed rolling window and regular measure queries from rollup support ([#3326](https://github.com/cube-js/cube.js/issues/3326)) ([3147e33](https://github.com/cube-js/cube.js/commit/3147e339f14ede73e5b0d14d05b9dd1f8b79e7b8))
* Support multi-value filtering on same column through FILTER_PARAMS ([#2854](https://github.com/cube-js/cube.js/issues/2854)) Thanks to [@omab](https://github.com/omab)! ([efc5745](https://github.com/cube-js/cube.js/commit/efc57452af44ee31092b8dfbb33a7ba23e86bba5))





## [0.28.28](https://github.com/cube-js/cube.js/compare/v0.28.27...v0.28.28) (2021-08-26)


### Bug Fixes

* **playground:** reset state on query change ([#3321](https://github.com/cube-js/cube.js/issues/3321)) ([9c0d4fe](https://github.com/cube-js/cube.js/commit/9c0d4fe647f3f90a4d1a65782f5625469079b579))





## [0.28.27](https://github.com/cube-js/cube.js/compare/v0.28.26...v0.28.27) (2021-08-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.25](https://github.com/cube-js/cube.js/compare/v0.28.24...v0.28.25) (2021-08-20)


### Features

* **@cubejs-backend/dremio-driver:** support quarter granularity ([b193b04](https://github.com/cube-js/cube.js/commit/b193b048db4f104683ca5dc518b09647a8435a4e))
* **@cubejs-backend/mysql-driver:** Support quarter granularity ([#3289](https://github.com/cube-js/cube.js/issues/3289)) ([6922e5d](https://github.com/cube-js/cube.js/commit/6922e5da50d2056c00a2ca248665e133a6de28be))





## [0.28.24](https://github.com/cube-js/cube.js/compare/v0.28.23...v0.28.24) (2021-08-19)


### Features

* Added Quarter to the timeDimensions of ([3f62b2c](https://github.com/cube-js/cube.js/commit/3f62b2c125b2b7b752e370b65be4c89a0c65a623))
* Support quarter granularity ([4ad1356](https://github.com/cube-js/cube.js/commit/4ad1356ac2d2c4d479c25e60519b0f7b4c1605bb))





## [0.28.22](https://github.com/cube-js/cube.js/compare/v0.28.21...v0.28.22) (2021-08-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.21](https://github.com/cube-js/cube.js/compare/v0.28.20...v0.28.21) (2021-08-16)


### Bug Fixes

* Pre-aggregations should have default refreshKey of every 1 hour if it doesn't set for Cube ([#3259](https://github.com/cube-js/cube.js/issues/3259)) ([bc472aa](https://github.com/cube-js/cube.js/commit/bc472aac1a666c84ed9e7e00b2d4e9018a6b5719))





## [0.28.19](https://github.com/cube-js/cube.js/compare/v0.28.18...v0.28.19) (2021-08-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.17](https://github.com/cube-js/cube.js/compare/v0.28.16...v0.28.17) (2021-08-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.14](https://github.com/cube-js/cube.js/compare/v0.28.13...v0.28.14) (2021-08-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.13](https://github.com/cube-js/cube.js/compare/v0.28.12...v0.28.13) (2021-08-04)


### Bug Fixes

* Support rolling `countDistinctApprox` rollups ([#3185](https://github.com/cube-js/cube.js/issues/3185)) ([e731992](https://github.com/cube-js/cube.js/commit/e731992b351f68f1ee249c9412f679b1903a6f28))





## [0.28.11](https://github.com/cube-js/cube.js/compare/v0.28.10...v0.28.11) (2021-07-31)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.10](https://github.com/cube-js/cube.js/compare/v0.28.9...v0.28.10) (2021-07-30)


### Features

* Rolling window rollup support ([#3151](https://github.com/cube-js/cube.js/issues/3151)) ([109ab5b](https://github.com/cube-js/cube.js/commit/109ab5bec32255244412a24bf75402abd1cbfe49))





## [0.28.9](https://github.com/cube-js/cube.js/compare/v0.28.8...v0.28.9) (2021-07-29)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.8](https://github.com/cube-js/cube.js/compare/v0.28.7...v0.28.8) (2021-07-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.7](https://github.com/cube-js/cube.js/compare/v0.28.6...v0.28.7) (2021-07-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.6](https://github.com/cube-js/cube.js/compare/v0.28.5...v0.28.6) (2021-07-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.3](https://github.com/cube-js/cube.js/compare/v0.28.2...v0.28.3) (2021-07-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.28.2](https://github.com/cube-js/cube.js/compare/v0.28.1...v0.28.2) (2021-07-20)


### Features

* Support every for refreshKey with SQL ([63cd8f4](https://github.com/cube-js/cube.js/commit/63cd8f4673f9312f9b685352c18bb3ed01a40e6c))





## [0.28.1](https://github.com/cube-js/cube.js/compare/v0.28.0...v0.28.1) (2021-07-19)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





# [0.28.0](https://github.com/cube-js/cube.js/compare/v0.27.53...v0.28.0) (2021-07-17)


### Features

* Move partition range evaluation from Schema Compiler to Query Orchestrator to allow unbounded queries on partitioned pre-aggregations ([8ea654e](https://github.com/cube-js/cube.js/commit/8ea654e93b57014fb2409e070b3a4c381985a9fd))





## [0.27.53](https://github.com/cube-js/cube.js/compare/v0.27.52...v0.27.53) (2021-07-13)


### Features

* **@cubejs-client/playground:** save pre-aggregations from the Rollup Designer ([#3096](https://github.com/cube-js/cube.js/issues/3096)) ([866f949](https://github.com/cube-js/cube.js/commit/866f949f2fc05e189a30b943a963aa7a3f697c81))





## [0.27.49](https://github.com/cube-js/cube.js/compare/v0.27.48...v0.27.49) (2021-07-08)


### Features

* Execute refreshKeys in externalDb (only for every) ([#3061](https://github.com/cube-js/cube.js/issues/3061)) ([75167a0](https://github.com/cube-js/cube.js/commit/75167a0e92028dbdd6f24aca85331b84be8ec3c3))





## [0.27.47](https://github.com/cube-js/cube.js/compare/v0.27.46...v0.27.47) (2021-07-06)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.27.46](https://github.com/cube-js/cube.js/compare/v0.27.45...v0.27.46) (2021-07-01)


### Features

* Rename refreshRangeStart/End to buildRangeStart/End ([232d117](https://github.com/cube-js/cube.js/commit/232d1179623b567b96b026ce35522b177bcafce5))





## [0.27.45](https://github.com/cube-js/cube.js/compare/v0.27.44...v0.27.45) (2021-06-30)


### Bug Fixes

* Unexpected refresh value for refreshKey (earlier then expected) ([#3031](https://github.com/cube-js/cube.js/issues/3031)) ([55f75ac](https://github.com/cube-js/cube.js/commit/55f75ac95c93ab07b5e04158236b2356c2482f2c))





## [0.27.44](https://github.com/cube-js/cube.js/compare/v0.27.43...v0.27.44) (2021-06-29)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.27.41](https://github.com/cube-js/cube.js/compare/v0.27.40...v0.27.41) (2021-06-25)


### Bug Fixes

* Use timeDimension without s on the end ([#2997](https://github.com/cube-js/cube.js/issues/2997)) ([5313836](https://github.com/cube-js/cube.js/commit/531383699d888efbea87a8eec27e839cc6142f41))


### Features

* Fetch pre-aggregation data preview by partition, debug api ([#2951](https://github.com/cube-js/cube.js/issues/2951)) ([4207f5d](https://github.com/cube-js/cube.js/commit/4207f5dea4f6c7a0237428f2d6fad468b98161a3))





## [0.27.40](https://github.com/cube-js/cube.js/compare/v0.27.39...v0.27.40) (2021-06-23)


### Features

* **mssql-driver:** Use DATETIME2 for timeStampCast ([ed13768](https://github.com/cube-js/cube.js/commit/ed13768d842392491a2545ac2f465d24e43986ba))





## [0.27.37](https://github.com/cube-js/cube.js/compare/v0.27.36...v0.27.37) (2021-06-21)


### Bug Fixes

* **mssql-driver:** Use DATETIME2 type in dateTimeCast ([#2962](https://github.com/cube-js/cube.js/issues/2962)) ([c8563ab](https://github.com/cube-js/cube.js/commit/c8563abca030bf43c3ef5f72ab00e294dcac5cd0))


### Features

* Remove support for view (dead code) ([de41702](https://github.com/cube-js/cube.js/commit/de41702492342f379d4098c065cbf6a61e0c5314))
* Support schema without references postfix ([22388cc](https://github.com/cube-js/cube.js/commit/22388cce0773aa57ac8888507904f6c2bd15f5ed))





## [0.27.35](https://github.com/cube-js/cube.js/compare/v0.27.34...v0.27.35) (2021-06-18)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.27.33](https://github.com/cube-js/cube.js/compare/v0.27.32...v0.27.33) (2021-06-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.27.31](https://github.com/cube-js/cube.js/compare/v0.27.30...v0.27.31) (2021-06-11)


### Features

* Write preAggregations block in schema generation ([2c1e150](https://github.com/cube-js/cube.js/commit/2c1e150ce70787381a34b22da32bf5b1e9e26bc6))





## [0.27.30](https://github.com/cube-js/cube.js/compare/v0.27.29...v0.27.30) (2021-06-04)


### Bug Fixes

* **@cubejs-client/playground:** pre-agg status ([#2904](https://github.com/cube-js/cube.js/issues/2904)) ([b18685f](https://github.com/cube-js/cube.js/commit/b18685f55a5f2bde8060cc7345dfd38f762307e3))
* pass timezone to pre-aggregation description ([#2884](https://github.com/cube-js/cube.js/issues/2884)) ([9cca41e](https://github.com/cube-js/cube.js/commit/9cca41ee18ee6bb0dbd0d6abe6f778d467a9a240))


### Features

* Make scheduledRefresh true by default (preview period) ([f3e648c](https://github.com/cube-js/cube.js/commit/f3e648c7a3d05bfe4719a8f820794f11611fb8c7))





## [0.27.29](https://github.com/cube-js/cube.js/compare/v0.27.27...v0.27.29) (2021-06-02)


### Bug Fixes

* Resolve refresh key sql for pre-aggregations meta api ([#2881](https://github.com/cube-js/cube.js/issues/2881)) ([55383b6](https://github.com/cube-js/cube.js/commit/55383b6d1d9755ded6e8815fc04ba3070e675199))


### Features

* **snowflake-driver:** Support HLL ([7b57840](https://github.com/cube-js/cube.js/commit/7b578401a5271a2cbe43266f0190b786b2191aaf))





## [0.27.28](https://github.com/cube-js/cube.js/compare/v0.27.27...v0.27.28) (2021-06-02)


### Bug Fixes

* Resolve refresh key sql for pre-aggregations meta api ([#2881](https://github.com/cube-js/cube.js/issues/2881)) ([55383b6](https://github.com/cube-js/cube.js/commit/55383b6d1d9755ded6e8815fc04ba3070e675199))


### Features

* **snowflake-driver:** Support HLL ([7b57840](https://github.com/cube-js/cube.js/commit/7b578401a5271a2cbe43266f0190b786b2191aaf))





## [0.27.25](https://github.com/cube-js/cube.js/compare/v0.27.24...v0.27.25) (2021-06-01)


### Features

* Pre-aggregations Meta API, part 2 ([#2804](https://github.com/cube-js/cube.js/issues/2804)) ([84b6e70](https://github.com/cube-js/cube.js/commit/84b6e70ed81e80cff0ba8d0dd9ad507132bb1b24))





## [0.27.22](https://github.com/cube-js/cube.js/compare/v0.27.21...v0.27.22) (2021-05-27)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.27.19](https://github.com/cube-js/cube.js/compare/v0.27.18...v0.27.19) (2021-05-24)


### Features

* Make rollup default for preAggregation.type ([4875fa1](https://github.com/cube-js/cube.js/commit/4875fa1be360372e7ed9dcaf9ded8b28bb348e2f))
* Pre-aggregations Meta API, part 1 ([#2801](https://github.com/cube-js/cube.js/issues/2801)) ([2245a77](https://github.com/cube-js/cube.js/commit/2245a7774666a3a8bd36703b2b4001b20789b943))





## [0.27.17](https://github.com/cube-js/cube.js/compare/v0.27.16...v0.27.17) (2021-05-22)


### Features

* Dont introspect schema, if driver can detect types ([3467b44](https://github.com/cube-js/cube.js/commit/3467b4472e800d8345260a5765542486ed93647b))





## [0.27.15](https://github.com/cube-js/cube.js/compare/v0.27.14...v0.27.15) (2021-05-18)


### Features

* Enable external pre-aggregations by default for new users ([22de035](https://github.com/cube-js/cube.js/commit/22de0358ec35017c45e6a716faaacf176c49c652))





## [0.27.14](https://github.com/cube-js/cube.js/compare/v0.27.13...v0.27.14) (2021-05-13)


### Bug Fixes

* **schema-compiler:** Time-series query with minute/second granularity ([c4a6044](https://github.com/cube-js/cube.js/commit/c4a6044702df39629044802b0b5d9e1636cc99d0))





## [0.27.13](https://github.com/cube-js/cube.js/compare/v0.27.12...v0.27.13) (2021-05-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.27.11](https://github.com/cube-js/cube.js/compare/v0.27.10...v0.27.11) (2021-05-12)


### Bug Fixes

* **clickhouse-driver:** Support ungrouped query, fix [#2717](https://github.com/cube-js/cube.js/issues/2717) ([#2719](https://github.com/cube-js/cube.js/issues/2719)) ([82efc98](https://github.com/cube-js/cube.js/commit/82efc987c574960c4989b41b92776ba928a2f22b))





## [0.27.10](https://github.com/cube-js/cube.js/compare/v0.27.9...v0.27.10) (2021-05-11)


### Bug Fixes

* titleize case ([6acc100](https://github.com/cube-js/cube.js/commit/6acc100a6d50bf6970010843d0472e981399a602))





## [0.27.9](https://github.com/cube-js/cube.js/compare/v0.27.8...v0.27.9) (2021-05-11)


### Bug Fixes

* **@cubejs-backend/schema-compiler:** titleize fix ([#2695](https://github.com/cube-js/cube.js/issues/2695)) ([d997f49](https://github.com/cube-js/cube.js/commit/d997f4978de9f4b25b606ba6c17b225f3e6a6cb1))





## [0.27.7](https://github.com/cube-js/cube.js/compare/v0.27.6...v0.27.7) (2021-05-04)


### Bug Fixes

* TypeError: moment is not a function ([39662e4](https://github.com/cube-js/cube.js/commit/39662e468ed001a5b472a2c09fc9fa2d48af03d2))





## [0.27.5](https://github.com/cube-js/cube.js/compare/v0.27.4...v0.27.5) (2021-05-03)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.27.4](https://github.com/cube-js/cube.js/compare/v0.27.3...v0.27.4) (2021-04-29)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.27.2](https://github.com/cube-js/cube.js/compare/v0.27.1...v0.27.2) (2021-04-28)


### Bug Fixes

* Move Prettier & Jest to dev dep (reduce size) ([da59584](https://github.com/cube-js/cube.js/commit/da5958426b701b8a81506e8d74070b2977e3df56))





## [0.27.1](https://github.com/cube-js/cube.js/compare/v0.27.0...v0.27.1) (2021-04-27)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





# [0.27.0](https://github.com/cube-js/cube.js/compare/v0.26.104...v0.27.0) (2021-04-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.104](https://github.com/cube-js/cube.js/compare/v0.26.103...v0.26.104) (2021-04-26)


### Bug Fixes

* Original SQL table is not found when `useOriginalSqlPreAggregations` used together with `CUBE.sql()` reference ([#2603](https://github.com/cube-js/cube.js/issues/2603)) ([5fd8e42](https://github.com/cube-js/cube.js/commit/5fd8e42cbe361a66cf0ffe6542478b6beaad86c5))





## [0.26.103](https://github.com/cube-js/cube.js/compare/v0.26.102...v0.26.103) (2021-04-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.95](https://github.com/cube-js/cube.js/compare/v0.26.94...v0.26.95) (2021-04-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.87](https://github.com/cube-js/cube.js/compare/v0.26.86...v0.26.87) (2021-04-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.81](https://github.com/cube-js/cube.js/compare/v0.26.80...v0.26.81) (2021-04-07)


### Features

* Introduce databricks-jdbc-driver ([bb0b31f](https://github.com/cube-js/cube.js/commit/bb0b31fb333f2aa379f11f6733c4efc17ec12dde))





## [0.26.79](https://github.com/cube-js/cube.js/compare/v0.26.78...v0.26.79) (2021-04-06)


### Bug Fixes

* sqlAlias on non partitioned rollups ([0675925](https://github.com/cube-js/cube.js/commit/0675925efb61a6492344b28179b7647eabb01a1d))





## [0.26.74](https://github.com/cube-js/cube.js/compare/v0.26.73...v0.26.74) (2021-04-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.69](https://github.com/cube-js/cube.js/compare/v0.26.68...v0.26.69) (2021-03-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.67](https://github.com/cube-js/cube.js/compare/v0.26.66...v0.26.67) (2021-03-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.65](https://github.com/cube-js/cube.js/compare/v0.26.64...v0.26.65) (2021-03-24)


### Bug Fixes

* Allow using sub query dimensions in join conditions ([#2419](https://github.com/cube-js/cube.js/issues/2419)) ([496a075](https://github.com/cube-js/cube.js/commit/496a0755308f376666e732ea09a4c7816682cfb0))





## [0.26.56](https://github.com/cube-js/cube.js/compare/v0.26.55...v0.26.56) (2021-03-13)


### Bug Fixes

* Expected one parameter but nothing found ([#2362](https://github.com/cube-js/cube.js/issues/2362)) ([ce490d2](https://github.com/cube-js/cube.js/commit/ce490d2de60c200832966824e6b0300ba91cde41))





## [0.26.54](https://github.com/cube-js/cube.js/compare/v0.26.53...v0.26.54) (2021-03-12)


### Features

* Suggest to use rollUp & pre-agg for to join across data sources ([2cf1a63](https://github.com/cube-js/cube.js/commit/2cf1a630a9abaa7248526c284441e65212e82259))





## [0.26.49](https://github.com/cube-js/cube.js/compare/v0.26.48...v0.26.49) (2021-03-05)


### Features

* **elasticsearch-driver:** Support for elastic.co & improve docs ([#2240](https://github.com/cube-js/cube.js/issues/2240)) ([d8557f6](https://github.com/cube-js/cube.js/commit/d8557f6487ea98c19c055cc94b94b284dd273835))





## [0.26.45](https://github.com/cube-js/cube.js/compare/v0.26.44...v0.26.45) (2021-03-04)


### Bug Fixes

* **@cubejs-schema-compiler:** addInterval / subtractInterval for Mssql ([#2239](https://github.com/cube-js/cube.js/issues/2239)) Thanks to [@florian-fischer-swarm](https://github.com/florian-fischer-swarm)! ([0930e15](https://github.com/cube-js/cube.js/commit/0930e1526612b92db2d192e4444a2c2a1d2d15ce)), closes [#2237](https://github.com/cube-js/cube.js/issues/2237)
* **schema-compiler:** Lock antlr4ts to 0.5.0-alpha.4, fix [#2264](https://github.com/cube-js/cube.js/issues/2264) ([37b3a0d](https://github.com/cube-js/cube.js/commit/37b3a0d61433ae1b3e41c1264298d1409b7f95b7))





## [0.26.44](https://github.com/cube-js/cube.js/compare/v0.26.43...v0.26.44) (2021-03-02)


### Bug Fixes

* **schema-compiler:** @types/ramda is a dev dependecy, dont ship it ([0a87d11](https://github.com/cube-js/cube.js/commit/0a87d1152f454e0e4d9c30c3295ee975dd493d0d))





## [0.26.35](https://github.com/cube-js/cube.js/compare/v0.26.34...v0.26.35) (2021-02-25)


### Features

* Use Cube Store as default external storage for CUBEJS_DEV_MODE ([e526676](https://github.com/cube-js/cube.js/commit/e52667617e5e687c92d383045fb1a8d5fd19cab6))





## [0.26.25](https://github.com/cube-js/cube.js/compare/v0.26.24...v0.26.25) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.23](https://github.com/cube-js/cube.js/compare/v0.26.22...v0.26.23) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.22](https://github.com/cube-js/cube.js/compare/v0.26.21...v0.26.22) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.19](https://github.com/cube-js/cube.js/compare/v0.26.18...v0.26.19) (2021-02-19)


### Bug Fixes

* **@cubejs-schema-compilter:** MSSQL remove order by from subqueries ([75c1903](https://github.com/cube-js/cube.js/commit/75c19035e2732adfb7c4711197bba57245e9673e))





## [0.26.16](https://github.com/cube-js/cube.js/compare/v0.26.15...v0.26.16) (2021-02-18)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.15](https://github.com/cube-js/cube.js/compare/v0.26.14...v0.26.15) (2021-02-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.13](https://github.com/cube-js/cube.js/compare/v0.26.12...v0.26.13) (2021-02-12)


### Features

* **schema-compiler:** Generate parser by antlr4ts ([d8e68c7](https://github.com/cube-js/cube.js/commit/d8e68c77f4649ddc056322f2848c769e5311c6b1))
* **schema-compiler:** Wrap new generated parser. fix [#1798](https://github.com/cube-js/cube.js/issues/1798) ([c5fde21](https://github.com/cube-js/cube.js/commit/c5fde21cb4bbcd675a4eeb735cd0c48d7a3ade6d))
* Support for extra params in generating schema for tables. ([#1990](https://github.com/cube-js/cube.js/issues/1990)) ([a9b3df2](https://github.com/cube-js/cube.js/commit/a9b3df222f8eaca86724ed2e1c24c348b38f718c))





## [0.26.11](https://github.com/cube-js/cube.js/compare/v0.26.10...v0.26.11) (2021-02-10)


### Bug Fixes

* CUBEJS_SCHEDULED_REFRESH_TIMER, fix [#1972](https://github.com/cube-js/cube.js/issues/1972) ([#1975](https://github.com/cube-js/cube.js/issues/1975)) ([dac7e52](https://github.com/cube-js/cube.js/commit/dac7e52ee0d3a118c9d69c9d030e58a3c048cca1))





## [0.26.10](https://github.com/cube-js/cube.js/compare/v0.26.9...v0.26.10) (2021-02-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.26.6](https://github.com/cube-js/cube.js/compare/v0.26.5...v0.26.6) (2021-02-08)


### Bug Fixes

* **sqlite-driver:** Use workaround for FLOOR ([#1931](https://github.com/cube-js/cube.js/issues/1931)) ([fe64feb](https://github.com/cube-js/cube.js/commit/fe64febd1b970c4b8396d05a859f16b3d9e5a8a8))





# [0.26.0](https://github.com/cube-js/cube.js/compare/v0.25.33...v0.26.0) (2021-02-01)


### Features

* Storing userContext inside payload.u is deprecated, moved to root ([559bd87](https://github.com/cube-js/cube.js/commit/559bd8757d9754ab486eed88d1fdb0c280b82dc9))
* USER_CONTEXT -> SECURITY_CONTEXT, authInfo -> securityInfo ([fa5d17c](https://github.com/cube-js/cube.js/commit/fa5d17c0bb703b087f442c41a5bf0a3dca1c5faa))





## [0.25.33](https://github.com/cube-js/cube.js/compare/v0.25.32...v0.25.33) (2021-01-30)


### Bug Fixes

* Use local dates for pre-aggregations to avoid timezone shift discrepancies on DST timezones for timezone unaware databases like MySQL ([#1941](https://github.com/cube-js/cube.js/issues/1941)) ([f138e6f](https://github.com/cube-js/cube.js/commit/f138e6fa3d97492c34527d0f04917e78c374eb57))
* **schema-compiler:** Wrong dayOffset in refreshKey for not UTC computers ([#1938](https://github.com/cube-js/cube.js/issues/1938)) ([5fe3431](https://github.com/cube-js/cube.js/commit/5fe3431a8f7320555fc3dba101c72547a0f41dac))





## [0.25.23](https://github.com/cube-js/cube.js/compare/v0.25.22...v0.25.23) (2021-01-22)


### Features

* **schema-compiler:** Move some parts to TS ([2ad0e2e](https://github.com/cube-js/cube.js/commit/2ad0e2e377fce52f4967fc73ae2486d4365f3ac4))





## [0.25.21](https://github.com/cube-js/cube.js/compare/v0.25.20...v0.25.21) (2021-01-19)


### Features

* **schema-compiler:** Initial support for TS ([5926067](https://github.com/cube-js/cube.js/commit/5926067bf5314c7cbddfe59f26dd0ae3b8b60293))





## [0.25.19](https://github.com/cube-js/cube.js/compare/v0.25.18...v0.25.19) (2021-01-14)


### Bug Fixes

* Do not renew historical refresh keys during scheduled refresh ([e5fbb12](https://github.com/cube-js/cube.js/commit/e5fbb120d5e848468999de59ba536b95be2e67e9))





## [0.25.2](https://github.com/cube-js/cube.js/compare/v0.25.1...v0.25.2) (2020-12-27)


### Bug Fixes

* **@cubejs-backend/schema-compiler:** MySQL double timezone conversion ([e5f1490](https://github.com/cube-js/cube.js/commit/e5f1490a897df4f0eac062dfabbc20aca2ea2f5b))





## [0.25.1](https://github.com/cube-js/cube.js/compare/v0.25.0...v0.25.1) (2020-12-24)


### Bug Fixes

* **@cubejs-backend/schema-compiler:** Better error message for join member resolutions ([30cc3ab](https://github.com/cube-js/cube.js/commit/30cc3abc4e8c91e8d95b8794f892e1d1f2152798))
* **@cubejs-backend/schema-compiler:** Error: TypeError: R.eq is not a function -- existing joins in rollup support ([5f62aae](https://github.com/cube-js/cube.js/commit/5f62aaee88b7ecc281437601410b10ef04d7bbf3))





# [0.25.0](https://github.com/cube-js/cube.js/compare/v0.24.15...v0.25.0) (2020-12-21)


### Features

* Allow cross data source joins ([a58336e](https://github.com/cube-js/cube.js/commit/a58336e3840f8ac02d83de43ec7661419bceb71c))





## [0.24.15](https://github.com/cube-js/cube.js/compare/v0.24.14...v0.24.15) (2020-12-20)


### Features

* Allow joins between data sources for external queries ([1dbfe2c](https://github.com/cube-js/cube.js/commit/1dbfe2cdc1b1904ce8567a7599b24e660c5047f3))





## [0.24.14](https://github.com/cube-js/cube.js/compare/v0.24.13...v0.24.14) (2020-12-19)


### Bug Fixes

* Rollup match results for rollupJoin ([0279b13](https://github.com/cube-js/cube.js/commit/0279b13a8696643ad95c374062ea059cea3b890b))





## [0.24.13](https://github.com/cube-js/cube.js/compare/v0.24.12...v0.24.13) (2020-12-18)


### Features

* Rollup join implementation ([#1637](https://github.com/cube-js/cube.js/issues/1637)) ([bffd220](https://github.com/cube-js/cube.js/commit/bffd22095f58369f3d52474283951b4844657f2b))





## [0.24.8](https://github.com/cube-js/cube.js/compare/v0.24.7...v0.24.8) (2020-12-15)


### Bug Fixes

* **@cubejs-backend/schema-compiler:** CubeCheckDuplicatePropTranspiler - dont crash on not StringLiterals ([#1582](https://github.com/cube-js/cube.js/issues/1582)) ([a705a2e](https://github.com/cube-js/cube.js/commit/a705a2ed6885d5c08e654945682054a1421dfb51))





## [0.24.5](https://github.com/cube-js/cube.js/compare/v0.24.4...v0.24.5) (2020-12-09)


### Features

* **@cubejs-backend/mysql-driver:** CAST all time dimensions with granularities to DATETIME in order to provide typing for rollup downloads. Add mediumtext and mediumint generic type conversions. ([3d8cb37](https://github.com/cube-js/cube.js/commit/3d8cb37d03716cd2768a0986643495e4a844cb8d))





## [0.24.4](https://github.com/cube-js/cube.js/compare/v0.24.3...v0.24.4) (2020-12-07)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.24.1](https://github.com/cube-js/cube.js/compare/v0.24.0...v0.24.1) (2020-11-27)


### Bug Fixes

* Specifying `dateRange` in time dimension should produce same result as `inDateRange` in filter ([a7603d7](https://github.com/cube-js/cube.js/commit/a7603d724732a51301227f68c39ba699333c0e06)), closes [#962](https://github.com/cube-js/cube.js/issues/962)





# [0.24.0](https://github.com/cube-js/cube.js/compare/v0.23.15...v0.24.0) (2020-11-26)


### Bug Fixes

* Error: Type must be provided for null values. -- `null` parameter values are passed to BigQuery when used for dimensions that contain `?` ([6417e7d](https://github.com/cube-js/cube.js/commit/6417e7d120a95c4792557a4c4a0d6abb7c483db9))


### Features

* Make default refreshKey to be `every 10 seconds` and enable scheduled refresh in dev mode by default ([221003a](https://github.com/cube-js/cube.js/commit/221003aa73aa1ece3d649de9164a7379a4a690be))


### BREAKING CHANGES

* `every 10 seconds` refreshKey becomes a default refreshKey for all cubes.





## [0.23.15](https://github.com/cube-js/cube.js/compare/v0.23.14...v0.23.15) (2020-11-25)


### Bug Fixes

* Error: Cannot find module 'antlr4/index' ([0d2e330](https://github.com/cube-js/cube.js/commit/0d2e33040dfea3fb80df2a1af2ccff46db0f8673))





## [0.23.11](https://github.com/cube-js/cube.js/compare/v0.23.10...v0.23.11) (2020-11-13)


### Features

* **@cubejs-backend/mysql-aurora-serverless-driver:** Add a new driver to support AWS Aurora Serverless MySql ([#1333](https://github.com/cube-js/cube.js/issues/1333)) Thanks to [@kcwinner](https://github.com/kcwinner)! ([154fab1](https://github.com/cube-js/cube.js/commit/154fab1a222685e1e83d5187a4f00f745c4613a3))





## [0.23.8](https://github.com/cube-js/cube.js/compare/v0.23.7...v0.23.8) (2020-11-06)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.23.6](https://github.com/cube-js/cube.js/compare/v0.23.5...v0.23.6) (2020-11-02)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.23.3](https://github.com/cube-js/cube.js/compare/v0.23.2...v0.23.3) (2020-10-31)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





# [0.23.0](https://github.com/cube-js/cube.js/compare/v0.22.4...v0.23.0) (2020-10-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.22.3](https://github.com/cube-js/cube.js/compare/v0.22.2...v0.22.3) (2020-10-26)


### Bug Fixes

* **@cubejs-backend/schema-compiler:** Dialect for 'undefined' is not found, fix [#1247](https://github.com/cube-js/cube.js/issues/1247) ([1069b47](https://github.com/cube-js/cube.js/commit/1069b47ff4f0a9d2e398ba194fe3eef5ad39f0d2))





## [0.22.2](https://github.com/cube-js/cube.js/compare/v0.22.1...v0.22.2) (2020-10-26)


### Bug Fixes

* Dialect class isn't looked up for external drivers ([b793f4a](https://github.com/cube-js/cube.js/commit/b793f4a))





# [0.22.0](https://github.com/cube-js/cube.js/compare/v0.21.2...v0.22.0) (2020-10-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.21.1](https://github.com/cube-js/cube.js/compare/v0.21.0...v0.21.1) (2020-10-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





# [0.21.0](https://github.com/cube-js/cube.js/compare/v0.20.15...v0.21.0) (2020-10-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.20.13](https://github.com/cube-js/cube.js/compare/v0.20.12...v0.20.13) (2020-10-07)


### Bug Fixes

* **@cubejs-schema-compilter:** MSSQL rollingWindow with granularity ([#1169](https://github.com/cube-js/cube.js/issues/1169)) Thanks to @JoshMentzer! ([16e6a9e](https://github.com/cube-js/cube.js/commit/16e6a9e))





## [0.20.11](https://github.com/cube-js/cube.js/compare/v0.20.10...v0.20.11) (2020-09-28)


### Bug Fixes

* **@cubejs-backend/prestodb-driver:** Wrong OFFSET/LIMIT order ([#1135](https://github.com/cube-js/cube.js/issues/1135)) ([3b94b2c](https://github.com/cube-js/cube.js/commit/3b94b2c)), closes [#988](https://github.com/cube-js/cube.js/issues/988) [#988](https://github.com/cube-js/cube.js/issues/988) [#988](https://github.com/cube-js/cube.js/issues/988)


### Features

* Introduce Druid driver ([#1099](https://github.com/cube-js/cube.js/issues/1099)) ([2bfe20f](https://github.com/cube-js/cube.js/commit/2bfe20f))





## [0.20.9](https://github.com/cube-js/cube.js/compare/v0.20.8...v0.20.9) (2020-09-19)


### Bug Fixes

* Allow empty complex boolean filter arrays ([#1100](https://github.com/cube-js/cube.js/issues/1100)) ([80d112e](https://github.com/cube-js/cube.js/commit/80d112e))


### Features

* `sqlAlias` attribute for `preAggregations` and short format for pre-aggregation table names ([#1068](https://github.com/cube-js/cube.js/issues/1068)) ([98ffad3](https://github.com/cube-js/cube.js/commit/98ffad3)), closes [#86](https://github.com/cube-js/cube.js/issues/86) [#907](https://github.com/cube-js/cube.js/issues/907)





## [0.20.8](https://github.com/cube-js/cube.js/compare/v0.20.7...v0.20.8) (2020-09-16)


### Bug Fixes

* **@cubejs-backend/elasticsearch-driver:** Respect `ungrouped` flag ([#1098](https://github.com/cube-js/cube.js/issues/1098)) Thanks to [@vignesh-123](https://github.com/vignesh-123)! ([995b8f9](https://github.com/cube-js/cube.js/commit/995b8f9))


### Features

* refreshKey every support for CRON format interval ([#1048](https://github.com/cube-js/cube.js/issues/1048)) ([3e55f5c](https://github.com/cube-js/cube.js/commit/3e55f5c))
* Strict cube schema parsing, show duplicate property name errors ([#1095](https://github.com/cube-js/cube.js/issues/1095)) ([d4ab530](https://github.com/cube-js/cube.js/commit/d4ab530))





## [0.20.7](https://github.com/cube-js/cube.js/compare/v0.20.6...v0.20.7) (2020-09-11)


### Bug Fixes

* member-dimension query normalization for queryTransformer and additional complex boolean logic tests ([#1047](https://github.com/cube-js/cube.js/issues/1047)) ([65ef327](https://github.com/cube-js/cube.js/commit/65ef327)), closes [#1007](https://github.com/cube-js/cube.js/issues/1007)





## [0.20.6](https://github.com/cube-js/cube.js/compare/v0.20.5...v0.20.6) (2020-09-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.20.5](https://github.com/cube-js/cube.js/compare/v0.20.4...v0.20.5) (2020-09-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.20.3](https://github.com/cube-js/cube.js/compare/v0.20.2...v0.20.3) (2020-09-03)


### Features

* Complex boolean logic ([#1038](https://github.com/cube-js/cube.js/issues/1038)) ([a5b44d1](https://github.com/cube-js/cube.js/commit/a5b44d1)), closes [#259](https://github.com/cube-js/cube.js/issues/259)





## [0.20.1](https://github.com/cube-js/cube.js/compare/v0.20.0...v0.20.1) (2020-09-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





# [0.20.0](https://github.com/cube-js/cube.js/compare/v0.19.61...v0.20.0) (2020-08-26)


### Bug Fixes

* **@cubejs-backend/athena-driver:** Error: Queries of this type are not supported for incremental refreshKey ([2d3018d](https://github.com/cube-js/cube.js/commit/2d3018d)), closes [#404](https://github.com/cube-js/cube.js/issues/404)
* Check partitionGranularity requires timeDimensionReference for `originalSql` ([2a2b256](https://github.com/cube-js/cube.js/commit/2a2b256))
* **@cubejs-backend/clickhouse-driver:** allow default compound indexes: add parentheses to the pre-aggregation sql definition ([#1009](https://github.com/cube-js/cube.js/issues/1009)) Thanks to [@gudjonragnar](https://github.com/gudjonragnar)! ([6535cb6](https://github.com/cube-js/cube.js/commit/6535cb6))
* TypeError: Cannot read property '1' of undefined -- Using scheduled cube refresh endpoint not working with Athena ([ed6c9aa](https://github.com/cube-js/cube.js/commit/ed6c9aa)), closes [#1000](https://github.com/cube-js/cube.js/issues/1000)


### Features

* Dremio driver ([#1008](https://github.com/cube-js/cube.js/issues/1008)) ([617225f](https://github.com/cube-js/cube.js/commit/617225f))





## [0.19.61](https://github.com/cube-js/cube.js/compare/v0.19.60...v0.19.61) (2020-08-11)


### Bug Fixes

* readOnly originalSql pre-aggregations aren't working without writing rights ([cfa7c7d](https://github.com/cube-js/cube.js/commit/cfa7c7d))


### Features

* **mssql-driver:** add readonly aggregation for mssql sources ([#920](https://github.com/cube-js/cube.js/issues/920)) Thanks to @JoshMentzer! ([dfeccca](https://github.com/cube-js/cube.js/commit/dfeccca))





## [0.19.57](https://github.com/cube-js/cube.js/compare/v0.19.56...v0.19.57) (2020-08-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.19.56](https://github.com/cube-js/cube.js/compare/v0.19.55...v0.19.56) (2020-08-03)


### Bug Fixes

* using limit and offset together in MSSql ([9ba875c](https://github.com/cube-js/cube.js/commit/9ba875c))
* Various ClickHouse improvements ([6f40847](https://github.com/cube-js/cube.js/commit/6f40847))





## [0.19.54](https://github.com/cube-js/cube.js/compare/v0.19.53...v0.19.54) (2020-07-23)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.19.50](https://github.com/cube-js/cube.js/compare/v0.19.49...v0.19.50) (2020-07-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.19.48](https://github.com/cube-js/cube.js/compare/v0.19.47...v0.19.48) (2020-07-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.19.43](https://github.com/cube-js/cube.js/compare/v0.19.42...v0.19.43) (2020-07-04)


### Features

* Pluggable dialects support ([f786fdd](https://github.com/cube-js/cube.js/commit/f786fdd)), closes [#590](https://github.com/cube-js/cube.js/issues/590)





## [0.19.40](https://github.com/cube-js/cube.js/compare/v0.19.39...v0.19.40) (2020-06-30)


### Bug Fixes

* Querying empty Postgres table with 'time' dimension in a cube results in null value ([07d00f8](https://github.com/cube-js/cube.js/commit/07d00f8)), closes [#639](https://github.com/cube-js/cube.js/issues/639)





## [0.19.39](https://github.com/cube-js/cube.js/compare/v0.19.38...v0.19.39) (2020-06-28)


### Bug Fixes

* treat wildcard Elasticsearch select as simple asterisk select: include * as part of RE to support elasticsearch indexes ([#760](https://github.com/cube-js/cube.js/issues/760)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar) ! ([099a888](https://github.com/cube-js/cube.js/commit/099a888))


### Features

* `refreshRangeStart` and `refreshRangeEnd` pre-aggregation params ([e4d2874](https://github.com/cube-js/cube.js/commit/e4d2874))





## [0.19.38](https://github.com/cube-js/cube.js/compare/v0.19.37...v0.19.38) (2020-06-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.19.35](https://github.com/cube-js/cube.js/compare/v0.19.34...v0.19.35) (2020-06-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.19.29](https://github.com/cube-js/cube.js/compare/v0.19.28...v0.19.29) (2020-06-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler





## [0.19.24](https://github.com/cube-js/cube.js/compare/v0.19.23...v0.19.24) (2020-06-06)


### Bug Fixes

* **@cubejs-backend/elasticsearch-driver:** respect ungrouped parameter ([#684](https://github.com/cube-js/cube.js/issues/684)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar)! ([27d0d49](https://github.com/cube-js/cube.js/commit/27d0d49))
* **@cubejs-backend/schema-compiler:** TypeError: methods.filter is not a function ([25c4ef6](https://github.com/cube-js/cube.js/commit/25c4ef6))





## [0.19.23](https://github.com/cube-js/cube.js/compare/v0.19.22...v0.19.23) (2020-06-02)


### Features

* drill down queries support ([#664](https://github.com/cube-js/cube.js/issues/664)) ([7e21545](https://github.com/cube-js/cube.js/commit/7e21545)), closes [#190](https://github.com/cube-js/cube.js/issues/190)





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
