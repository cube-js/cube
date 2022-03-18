# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.29.33](https://github.com/cube-js/cube.js/compare/v0.29.32...v0.29.33) (2022-03-17)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.29.31](https://github.com/cube-js/cube.js/compare/v0.29.30...v0.29.31) (2022-03-09)


### Bug Fixes

* **athena:** Fixes export bucket location. Fixes column order. ([#4183](https://github.com/cube-js/cube.js/issues/4183)) ([abd40a7](https://github.com/cube-js/cube.js/commit/abd40a79e360cd9a9eceeb56a450102bd782f3d9))





## [0.29.29](https://github.com/cube-js/cube.js/compare/v0.29.28...v0.29.29) (2022-03-03)


### Features

* **packages:** add QuestDB driver ([#4096](https://github.com/cube-js/cube.js/issues/4096)) ([de8823b](https://github.com/cube-js/cube.js/commit/de8823b524d372dd1d9b661e643d9d4664260b58))





## [0.29.28](https://github.com/cube-js/cube.js/compare/v0.29.27...v0.29.28) (2022-02-10)


### Bug Fixes

* **@cubejs-backend/athena-driver:** Batching and export support ([#4039](https://github.com/cube-js/cube.js/issues/4039)) ([108f42a](https://github.com/cube-js/cube.js/commit/108f42afdd58ae0027b1b81730f7ca9e72ab9122))





## [0.29.25](https://github.com/cube-js/cube.js/compare/v0.29.24...v0.29.25) (2022-02-03)


### Bug Fixes

* Out of memory in case of empty table has been used to build partitioned pre-aggregation ([#4021](https://github.com/cube-js/cube.js/issues/4021)) ([314cc3c](https://github.com/cube-js/cube.js/commit/314cc3c3f47d6ba9282a1bb969c2e27bdfb58a57))





## [0.29.24](https://github.com/cube-js/cube.js/compare/v0.29.23...v0.29.24) (2022-02-01)


### Bug Fixes

* Correct error message on missing partitions ([e953296](https://github.com/cube-js/cube.js/commit/e953296ff4258023a601ea5d5ab91dc8cda4ff14))
* Remove orphaned tables on error while pre-aggregation creation ([#3996](https://github.com/cube-js/cube.js/issues/3996)) ([0548435](https://github.com/cube-js/cube.js/commit/054843533b2421d87874fa25adf3a94892f24bd4))





## [0.29.23](https://github.com/cube-js/cube.js/compare/v0.29.22...v0.29.23) (2022-01-26)


### Bug Fixes

* Cannot read property ‘last_updated_at’ of undefined ([#3980](https://github.com/cube-js/cube.js/issues/3980)) ([74d75e7](https://github.com/cube-js/cube.js/commit/74d75e743eb0549eea443e84d7278e7b8f78e6af))





## [0.29.21](https://github.com/cube-js/cube.js/compare/v0.29.20...v0.29.21) (2022-01-17)


### Features

* Surfaces lastUpdateAt for used preaggregation tables and lastRefreshTime for queries using preaggreagation tables ([#3890](https://github.com/cube-js/cube.js/issues/3890)) ([f4ae73d](https://github.com/cube-js/cube.js/commit/f4ae73d9d4fd6b84483c99e8bdd1f9588efde5a2)), closes [#3540](https://github.com/cube-js/cube.js/issues/3540)





## [0.29.20](https://github.com/cube-js/cube.js/compare/v0.29.19...v0.29.20) (2022-01-10)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.29.18](https://github.com/cube-js/cube.js/compare/v0.29.17...v0.29.18) (2022-01-09)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.29.15](https://github.com/cube-js/cube.js/compare/v0.29.14...v0.29.15) (2021-12-30)


### Features

* Introduce single unified CUBEJS_DB_QUERY_TIMEOUT env variable to set all various variables that control database query timeouts ([#3864](https://github.com/cube-js/cube.js/issues/3864)) ([33c6292](https://github.com/cube-js/cube.js/commit/33c6292059e65e293a7e3d61e1f1e0c1413eeece))





## [0.29.12](https://github.com/cube-js/cube.js/compare/v0.29.11...v0.29.12) (2021-12-29)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.29.7](https://github.com/cube-js/cube.js/compare/v0.29.6...v0.29.7) (2021-12-20)


### Bug Fixes

* Table cache incorrectly invalidated after merge multi-tenant queues by default change ([#3828](https://github.com/cube-js/cube.js/issues/3828)) ([540446a](https://github.com/cube-js/cube.js/commit/540446a76e19be3aec38b96ad81c149f567e9e40))





## [0.29.6](https://github.com/cube-js/cube.js/compare/v0.29.5...v0.29.6) (2021-12-19)


### Bug Fixes

* refresh process for `useOriginalSqlPreAggregations` is broken ([#3826](https://github.com/cube-js/cube.js/issues/3826)) ([f0bf070](https://github.com/cube-js/cube.js/commit/f0bf070462b6b69c76a5c34ee97fc5a5ec8fab15))





# [0.29.0](https://github.com/cube-js/cube.js/compare/v0.28.67...v0.29.0) (2021-12-14)


### Reverts

* Revert "BREAKING CHANGE: 0.29 (#3809)" (#3811) ([db005ed](https://github.com/cube-js/cube.js/commit/db005edc04d48e8251250ab9d0e19f496cf3b52b)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809) [#3811](https://github.com/cube-js/cube.js/issues/3811)


* BREAKING CHANGE: 0.29 (#3809) ([6f1418b](https://github.com/cube-js/cube.js/commit/6f1418b9963774844f341682e594601a56bb0084)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809)


### BREAKING CHANGES

* Drop support for Node.js 10 (12.x is a minimal version)
* Upgrade Node.js to 14 for Docker images
* Drop support for Node.js 15





## [0.28.64](https://github.com/cube-js/cube.js/compare/v0.28.63...v0.28.64) (2021-12-05)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.63](https://github.com/cube-js/cube.js/compare/v0.28.62...v0.28.63) (2021-12-03)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.61](https://github.com/cube-js/cube.js/compare/v0.28.60...v0.28.61) (2021-11-30)


### Bug Fixes

* Clarify pre-aggregation build error messages ([cf17f64](https://github.com/cube-js/cube.js/commit/cf17f64406d4068385ed611d102c4bc6ea979552)), closes [#3469](https://github.com/cube-js/cube.js/issues/3469)





## [0.28.60](https://github.com/cube-js/cube.js/compare/v0.28.59...v0.28.60) (2021-11-25)


### Bug Fixes

* **@cubejs-backend/mongobi-driver:** Show all tables if database isn't set ([6a55438](https://github.com/cube-js/cube.js/commit/6a554385c05371304755037ac17d189477164ffb))
* Empty pre-aggregation with partitionGranularity throws ParserError("Expected identifier, found: )") ([#3714](https://github.com/cube-js/cube.js/issues/3714)) ([86c6aaf](https://github.com/cube-js/cube.js/commit/86c6aaf8001d16b78f3a3029903affae4a82f41d))





## [0.28.59](https://github.com/cube-js/cube.js/compare/v0.28.58...v0.28.59) (2021-11-21)


### Bug Fixes

* Dropping orphaned tables logging messages don't have necessary t… ([#3710](https://github.com/cube-js/cube.js/issues/3710)) ([1962e0f](https://github.com/cube-js/cube.js/commit/1962e0fe6ce999879d1225cef27a24ea8cb8490a))





## [0.28.58](https://github.com/cube-js/cube.js/compare/v0.28.57...v0.28.58) (2021-11-18)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.56](https://github.com/cube-js/cube.js/compare/v0.28.55...v0.28.56) (2021-11-14)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.55](https://github.com/cube-js/cube.js/compare/v0.28.54...v0.28.55) (2021-11-12)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.52](https://github.com/cube-js/cube.js/compare/v0.28.51...v0.28.52) (2021-11-03)


### Bug Fixes

* Empty data partitioned pre-aggregations are incorrectly handled -- value provided is not in a recognized RFC2822 or ISO format ([9f3acd5](https://github.com/cube-js/cube.js/commit/9f3acd572bcd2421bf8d3581c4c6287b62e77313))
* packages/cubejs-query-orchestrator/package.json to reduce vulnerabilities ([#3281](https://github.com/cube-js/cube.js/issues/3281)) ([a6a62ea](https://github.com/cube-js/cube.js/commit/a6a62ea5832a13b519ca4455af1f317cf7af64d9))





## [0.28.50](https://github.com/cube-js/cube.js/compare/v0.28.49...v0.28.50) (2021-10-28)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.47](https://github.com/cube-js/cube.js/compare/v0.28.46...v0.28.47) (2021-10-22)


### Features

* ksql support ([#3507](https://github.com/cube-js/cube.js/issues/3507)) ([b7128d4](https://github.com/cube-js/cube.js/commit/b7128d43d2aaffdd7273555779176b3efe4e2aa6))





## [0.28.42](https://github.com/cube-js/cube.js/compare/v0.28.41...v0.28.42) (2021-10-15)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.38](https://github.com/cube-js/cube.js/compare/v0.28.37...v0.28.38) (2021-09-20)


### Bug Fixes

* **@cubejs-backend/query-orchestrator:** rollup only mode error message update ([c9c5ac0](https://github.com/cube-js/cube.js/commit/c9c5ac0ac33ce26f9505ce6683a41a9aaf742d63))





## [0.28.37](https://github.com/cube-js/cube.js/compare/v0.28.36...v0.28.37) (2021-09-17)


### Bug Fixes

* **query-orchestrator:** Wrong passing data source for partition range loader, debug API ([#3426](https://github.com/cube-js/cube.js/issues/3426)) ([dfaba5c](https://github.com/cube-js/cube.js/commit/dfaba5ca5a3f6fbb0f0e27e1133500b3e33a44b9))





## [0.28.34](https://github.com/cube-js/cube.js/compare/v0.28.33...v0.28.34) (2021-09-13)


### Features

* **bigquery-driver:** Use INFORMATION_SCHEMA.COLUMNS for introspection ([ef22c6c](https://github.com/cube-js/cube.js/commit/ef22c6c7ae6ebe28e7d9683a1d7eef0f0b426b3c))





## [0.28.33](https://github.com/cube-js/cube.js/compare/v0.28.32...v0.28.33) (2021-09-11)


### Bug Fixes

* Handle "rollup only" mode for pre-aggregation preview data query, Debug API ([#3402](https://github.com/cube-js/cube.js/issues/3402)) ([f45626a](https://github.com/cube-js/cube.js/commit/f45626a72a49004f0d348310e3e05ee068f5079a))


### Features

* Add ability to pass through generic-pool options ([#3364](https://github.com/cube-js/cube.js/issues/3364)) Thanks to @TRManderson! ([582a3e8](https://github.com/cube-js/cube.js/commit/582a3e82be8d2d676a45b1183eb35b5215fc78a9)), closes [#3340](https://github.com/cube-js/cube.js/issues/3340)





## [0.28.32](https://github.com/cube-js/cube.js/compare/v0.28.31...v0.28.32) (2021-09-06)


### Bug Fixes

* HLL Rolling window query fails ([#3380](https://github.com/cube-js/cube.js/issues/3380)) ([581a52a](https://github.com/cube-js/cube.js/commit/581a52a856aeee067bac9a680e22694bf507af04))





## [0.28.29](https://github.com/cube-js/cube.js/compare/v0.28.28...v0.28.29) (2021-08-31)


### Features

* Mixed rolling window and regular measure queries from rollup support ([#3326](https://github.com/cube-js/cube.js/issues/3326)) ([3147e33](https://github.com/cube-js/cube.js/commit/3147e339f14ede73e5b0d14d05b9dd1f8b79e7b8))





## [0.28.27](https://github.com/cube-js/cube.js/compare/v0.28.26...v0.28.27) (2021-08-25)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.24](https://github.com/cube-js/cube.js/compare/v0.28.23...v0.28.24) (2021-08-19)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.22](https://github.com/cube-js/cube.js/compare/v0.28.21...v0.28.22) (2021-08-17)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.19](https://github.com/cube-js/cube.js/compare/v0.28.18...v0.28.19) (2021-08-13)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.17](https://github.com/cube-js/cube.js/compare/v0.28.16...v0.28.17) (2021-08-11)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.14](https://github.com/cube-js/cube.js/compare/v0.28.13...v0.28.14) (2021-08-05)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.13](https://github.com/cube-js/cube.js/compare/v0.28.12...v0.28.13) (2021-08-04)


### Bug Fixes

* Load build range query only on refresh key changes ([#3184](https://github.com/cube-js/cube.js/issues/3184)) ([40c6ee0](https://github.com/cube-js/cube.js/commit/40c6ee036de73144c12072c71df10081801d4e6b))





## [0.28.11](https://github.com/cube-js/cube.js/compare/v0.28.10...v0.28.11) (2021-07-31)


### Bug Fixes

* Pre-aggregation table is not found for after it was successfully created in Refresh Worker ([334af1c](https://github.com/cube-js/cube.js/commit/334af1cc3c590e8b835958511d6ec0e17a77b0f3))





## [0.28.10](https://github.com/cube-js/cube.js/compare/v0.28.9...v0.28.10) (2021-07-30)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.9](https://github.com/cube-js/cube.js/compare/v0.28.8...v0.28.9) (2021-07-29)


### Bug Fixes

* Optimize timestamp formatting and table names loading for large partition range serving ([#3166](https://github.com/cube-js/cube.js/issues/3166)) ([e1f8dc5](https://github.com/cube-js/cube.js/commit/e1f8dc5aab469b060f0fe8c69467117171c070fd))
* **query-orchestrator:** Improved manual cancellation for preaggregation request from queue, Debug API ([#3156](https://github.com/cube-js/cube.js/issues/3156)) ([11284ce](https://github.com/cube-js/cube.js/commit/11284ce8085673486fb50d49a20ba005a45f1647))





## [0.28.8](https://github.com/cube-js/cube.js/compare/v0.28.7...v0.28.8) (2021-07-25)


### Bug Fixes

* Hide debug logs, Debug API ([#3152](https://github.com/cube-js/cube.js/issues/3152)) ([601f65f](https://github.com/cube-js/cube.js/commit/601f65f3e390e738da4f03d313935a2506a77290))





## [0.28.7](https://github.com/cube-js/cube.js/compare/v0.28.6...v0.28.7) (2021-07-25)


### Features

* Remove job from pre-aggregations queue, Debug API ([#3148](https://github.com/cube-js/cube.js/issues/3148)) ([2e244db](https://github.com/cube-js/cube.js/commit/2e244dbde868a002d5c5d4396bb5274679cf9168))





## [0.28.6](https://github.com/cube-js/cube.js/compare/v0.28.5...v0.28.6) (2021-07-22)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.28.3](https://github.com/cube-js/cube.js/compare/v0.28.2...v0.28.3) (2021-07-20)


### Bug Fixes

* **query-orchestrator:** Wrong detection for 0 (zero integer) ([#3126](https://github.com/cube-js/cube.js/issues/3126)) ([39a6b1c](https://github.com/cube-js/cube.js/commit/39a6b1c2928995de4e91cb5242dd18a964e6d616))





## [0.28.2](https://github.com/cube-js/cube.js/compare/v0.28.1...v0.28.2) (2021-07-20)


### Bug Fixes

* Use experimental flag for pre-aggregations queue events bus, debug API ([#3130](https://github.com/cube-js/cube.js/issues/3130)) ([4f141d7](https://github.com/cube-js/cube.js/commit/4f141d789cd0058b2e7c07af6a16642d6c145834))





## [0.28.1](https://github.com/cube-js/cube.js/compare/v0.28.0...v0.28.1) (2021-07-19)


### Features

* Subscribe to pre-aggregations queue events, debug API  ([#3116](https://github.com/cube-js/cube.js/issues/3116)) ([9f0e52e](https://github.com/cube-js/cube.js/commit/9f0e52e012bf94d2adb5a70a75b4b1fb151238ea))





# [0.28.0](https://github.com/cube-js/cube.js/compare/v0.27.53...v0.28.0) (2021-07-17)


### Features

* Move partition range evaluation from Schema Compiler to Query Orchestrator to allow unbounded queries on partitioned pre-aggregations ([8ea654e](https://github.com/cube-js/cube.js/commit/8ea654e93b57014fb2409e070b3a4c381985a9fd))





## [0.27.53](https://github.com/cube-js/cube.js/compare/v0.27.52...v0.27.53) (2021-07-13)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.27.51](https://github.com/cube-js/cube.js/compare/v0.27.50...v0.27.51) (2021-07-13)


### Bug Fixes

* Use orphaned timeout from query body, pre-aggregations queue, debug API ([#3088](https://github.com/cube-js/cube.js/issues/3088)) ([83e0a0a](https://github.com/cube-js/cube.js/commit/83e0a0a2d68fe94f193cfbb4fedb3f44d0b3cc27))


### Features

* Manual build pre-aggregations, getter for queue state, debug API   ([#3080](https://github.com/cube-js/cube.js/issues/3080)) ([692372e](https://github.com/cube-js/cube.js/commit/692372e1da6ffa7b7c1718d8332d80d1e2ce2b2d))





## [0.27.49](https://github.com/cube-js/cube.js/compare/v0.27.48...v0.27.49) (2021-07-08)


### Features

* Execute refreshKeys in externalDb (only for every) ([#3061](https://github.com/cube-js/cube.js/issues/3061)) ([75167a0](https://github.com/cube-js/cube.js/commit/75167a0e92028dbdd6f24aca85331b84be8ec3c3))





## [0.27.47](https://github.com/cube-js/cube.js/compare/v0.27.46...v0.27.47) (2021-07-06)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.27.46](https://github.com/cube-js/cube.js/compare/v0.27.45...v0.27.46) (2021-07-01)


### Bug Fixes

* **query-orchestrator:** Incorrect passing params for fetch preview pre-aggregation data, debug API ([#3039](https://github.com/cube-js/cube.js/issues/3039)) ([bedc064](https://github.com/cube-js/cube.js/commit/bedc064039ebb2da4f6dbb7854524351deac9bd4))
* CUBEJS_REFRESH_WORKER shouldn't enabled externalRefresh ([7b2e9ee](https://github.com/cube-js/cube.js/commit/7b2e9ee68c37efa1eeef53a4e7b5cf4a86e0b065))





## [0.27.45](https://github.com/cube-js/cube.js/compare/v0.27.44...v0.27.45) (2021-06-30)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.27.41](https://github.com/cube-js/cube.js/compare/v0.27.40...v0.27.41) (2021-06-25)


### Features

* Fetch pre-aggregation data preview by partition, debug api ([#2951](https://github.com/cube-js/cube.js/issues/2951)) ([4207f5d](https://github.com/cube-js/cube.js/commit/4207f5dea4f6c7a0237428f2d6fad468b98161a3))





## [0.27.39](https://github.com/cube-js/cube.js/compare/v0.27.38...v0.27.39) (2021-06-22)


### Bug Fixes

* Skip empty pre-aggregations sql, debug API ([#2989](https://github.com/cube-js/cube.js/issues/2989)) ([6629ca1](https://github.com/cube-js/cube.js/commit/6629ca15ae8f3e6d2efa83b019468f96a2473c18))





## [0.27.38](https://github.com/cube-js/cube.js/compare/v0.27.37...v0.27.38) (2021-06-22)


### Bug Fixes

* **query-orchestrator:** Re-use pre-aggregations cache loader for debug API ([#2981](https://github.com/cube-js/cube.js/issues/2981)) ([2a5f26e](https://github.com/cube-js/cube.js/commit/2a5f26e83c88246644151cf504624ced1ff1d431))





## [0.27.37](https://github.com/cube-js/cube.js/compare/v0.27.36...v0.27.37) (2021-06-21)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.27.35](https://github.com/cube-js/cube.js/compare/v0.27.34...v0.27.35) (2021-06-18)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.27.34](https://github.com/cube-js/cube.js/compare/v0.27.33...v0.27.34) (2021-06-15)


### Features

* **clickhouse-driver:** Migrate to TS & HydrationStream ([c9672c0](https://github.com/cube-js/cube.js/commit/c9672c0a14d141a9c3fc6ae75e4321a2211383e7))





## [0.27.33](https://github.com/cube-js/cube.js/compare/v0.27.32...v0.27.33) (2021-06-15)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.27.32](https://github.com/cube-js/cube.js/compare/v0.27.31...v0.27.32) (2021-06-12)


### Features

* **@cubejs-client/playground:** internal pre-agg warning ([#2943](https://github.com/cube-js/cube.js/issues/2943)) ([039270f](https://github.com/cube-js/cube.js/commit/039270f267774bb5a80d5434ba057164e565b08b))





## [0.27.31](https://github.com/cube-js/cube.js/compare/v0.27.30...v0.27.31) (2021-06-11)


### Bug Fixes

* Fetch all partitions, pre-aggregations Meta API ([#2944](https://github.com/cube-js/cube.js/issues/2944)) ([b5585fb](https://github.com/cube-js/cube.js/commit/b5585fbbb677b1b9e76d10719e64531286175da3))
* Make missing externalDriverFactory error message more specific ([b5480f6](https://github.com/cube-js/cube.js/commit/b5480f6b86e4d727bc027b22914508f17fd1f88c))


### Features

* Suggest export/unload for large pre-aggregations (detect via streaming) ([b20cdbc](https://github.com/cube-js/cube.js/commit/b20cdbc0b9fa98785d3ea46443492037017da12f))





## [0.27.30](https://github.com/cube-js/cube.js/compare/v0.27.29...v0.27.30) (2021-06-04)


### Bug Fixes

* **@cubejs-backend/query-orchestrator:** enum to generic type ([#2906](https://github.com/cube-js/cube.js/issues/2906)) ([1a4f745](https://github.com/cube-js/cube.js/commit/1a4f74575116ec14570f1245895fc8756a6a6ff4))
* **@cubejs-client/playground:** pre-agg status ([#2904](https://github.com/cube-js/cube.js/issues/2904)) ([b18685f](https://github.com/cube-js/cube.js/commit/b18685f55a5f2bde8060cc7345dfd38f762307e3))


### Features

* Introduce lock for dropOrphanedTables (concurrency bug) ([29509fa](https://github.com/cube-js/cube.js/commit/29509fa6714f636ba28960119138fbc20d604b4f))
* skipExternalCacheAndQueue for Cube Store ([dc6138e](https://github.com/cube-js/cube.js/commit/dc6138e27deb9c319bab2faadd4413fe318c18e2))





## [0.27.25](https://github.com/cube-js/cube.js/compare/v0.27.24...v0.27.25) (2021-06-01)


### Features

* **redshift-driver:** Support UNLOAD (direct export to S3) ([d741027](https://github.com/cube-js/cube.js/commit/d7410278d3a27e692c41b222ab0eb66f7992fe21))
* Pre-aggregations Meta API, part 2 ([#2804](https://github.com/cube-js/cube.js/issues/2804)) ([84b6e70](https://github.com/cube-js/cube.js/commit/84b6e70ed81e80cff0ba8d0dd9ad507132bb1b24))





## [0.27.22](https://github.com/cube-js/cube.js/compare/v0.27.21...v0.27.22) (2021-05-27)


### Bug Fixes

* **postgresql-driver:** Map bool/float4/float8 to generic type, use bigint for int8 ([ddc1739](https://github.com/cube-js/cube.js/commit/ddc17393dba3067ebe06248d817c8778859020a8))


### Features

* **bigquery-driver:** Migrate to TypeScript ([7c5b254](https://github.com/cube-js/cube.js/commit/7c5b25459cd3265587ddd8ed6dd23c944094254c))





## [0.27.17](https://github.com/cube-js/cube.js/compare/v0.27.16...v0.27.17) (2021-05-22)


### Bug Fixes

* Pre-aggregation table is not found if warming up same pre-aggregation with a new index ([b980744](https://github.com/cube-js/cube.js/commit/b9807446174e310cc2981af0fa1bc6b54fa1669d))


### Features

* **snowflake-driver:** Support UNLOAD to S3 ([d984f97](https://github.com/cube-js/cube.js/commit/d984f973547d1ff339dbf6ba26f0e363bddf2e98))
* Dont introspect schema, if driver can detect types ([3467b44](https://github.com/cube-js/cube.js/commit/3467b4472e800d8345260a5765542486ed93647b))
* **mysql-driver:** Support streaming ([d694c91](https://github.com/cube-js/cube.js/commit/d694c91370ceb703a50aefdd42ae3a9834e9884c))
* **postgres-driver:** Introduce streaming ([7685ffd](https://github.com/cube-js/cube.js/commit/7685ffd45640c5ac7b403487476dd799a11db5a0))
* **postgres-driver:** Migrate driver to TypeScript ([38f4adb](https://github.com/cube-js/cube.js/commit/38f4adbc7d0405b99cada80cc844011dcc504d2f))





## [0.27.16](https://github.com/cube-js/cube.js/compare/v0.27.15...v0.27.16) (2021-05-19)


### Bug Fixes

* Optimize high load Cube Store serve track: do not use Redis while fetching pre-aggregation tables ([#2776](https://github.com/cube-js/cube.js/issues/2776)) ([a3bc0b8](https://github.com/cube-js/cube.js/commit/a3bc0b8079373394e6a6a4fca12ddd46195d5b7d))





## [0.27.15](https://github.com/cube-js/cube.js/compare/v0.27.14...v0.27.15) (2021-05-18)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.27.13](https://github.com/cube-js/cube.js/compare/v0.27.12...v0.27.13) (2021-05-13)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.27.10](https://github.com/cube-js/cube.js/compare/v0.27.9...v0.27.10) (2021-05-11)


### Features

* Move External Cache And Queue serving to Cube Store ([#2702](https://github.com/cube-js/cube.js/issues/2702)) ([37e4268](https://github.com/cube-js/cube.js/commit/37e4268869a23c07f922a039873d349b733bf577))





## [0.27.5](https://github.com/cube-js/cube.js/compare/v0.27.4...v0.27.5) (2021-05-03)


### Bug Fixes

* **query-orchestrator:** tableColumnTypes - compatibility for MySQL 8 ([a886876](https://github.com/cube-js/cube.js/commit/a88687606ce54272c8742503f62bcac8d2ca0441))





## [0.27.4](https://github.com/cube-js/cube.js/compare/v0.27.3...v0.27.4) (2021-04-29)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.27.2](https://github.com/cube-js/cube.js/compare/v0.27.1...v0.27.2) (2021-04-28)


### Bug Fixes

* Disable Scheduling when querying is not ready (before connection Wizard) ([b518ccd](https://github.com/cube-js/cube.js/commit/b518ccdd66a2f51af541bc54de2df816e138bac5))





## [0.27.1](https://github.com/cube-js/cube.js/compare/v0.27.0...v0.27.1) (2021-04-27)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





# [0.27.0](https://github.com/cube-js/cube.js/compare/v0.26.104...v0.27.0) (2021-04-26)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.26.103](https://github.com/cube-js/cube.js/compare/v0.26.102...v0.26.103) (2021-04-24)


### Bug Fixes

* concurrency lock driver instance & release on error ([dd60f5d](https://github.com/cube-js/cube.js/commit/dd60f5d1be59012ed33958d5889276afcc0639af))





## [0.26.101](https://github.com/cube-js/cube.js/compare/v0.26.100...v0.26.101) (2021-04-20)


### Bug Fixes

* QueryQueue - broken queuing on any error (TimeoutError: ResourceRequest timed out) ([ebe2c71](https://github.com/cube-js/cube.js/commit/ebe2c716d4ec0aa5cbc60529567f39672c199720))





## [0.26.99](https://github.com/cube-js/cube.js/compare/v0.26.98...v0.26.99) (2021-04-16)


### Bug Fixes

* Continuous refresh during Scheduled Refresh in case of both external and internal pre-aggregations are defined for the same data source ([#2566](https://github.com/cube-js/cube.js/issues/2566)) ([dcd2894](https://github.com/cube-js/cube.js/commit/dcd2894453d7afda3ea50b2592ae6610b231f5c5))





## [0.26.95](https://github.com/cube-js/cube.js/compare/v0.26.94...v0.26.95) (2021-04-13)


### Bug Fixes

* **playground:** Test connection in Connection Wizard ([715fa1c](https://github.com/cube-js/cube.js/commit/715fa1c0ab6667017e12851cbd97e97cf6807d60))





## [0.26.90](https://github.com/cube-js/cube.js/compare/v0.26.89...v0.26.90) (2021-04-11)


### Bug Fixes

* Manage stream connection release by orchestrator instead of driver ([adf059e](https://github.com/cube-js/cube.js/commit/adf059ec52e31e3ebc055b60a1ac6236c57251f8))





## [0.26.88](https://github.com/cube-js/cube.js/compare/v0.26.87...v0.26.88) (2021-04-10)


### Features

* Mysql Cube Store streaming ingests ([#2528](https://github.com/cube-js/cube.js/issues/2528)) ([0b36a6f](https://github.com/cube-js/cube.js/commit/0b36a6faa184766873ec3792785eb1aa5ca582af))





## [0.26.87](https://github.com/cube-js/cube.js/compare/v0.26.86...v0.26.87) (2021-04-10)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.26.81](https://github.com/cube-js/cube.js/compare/v0.26.80...v0.26.81) (2021-04-07)


### Features

* Introduce databricks-jdbc-driver ([bb0b31f](https://github.com/cube-js/cube.js/commit/bb0b31fb333f2aa379f11f6733c4efc17ec12dde))





## [0.26.79](https://github.com/cube-js/cube.js/compare/v0.26.78...v0.26.79) (2021-04-06)


### Bug Fixes

* sqlAlias on non partitioned rollups ([0675925](https://github.com/cube-js/cube.js/commit/0675925efb61a6492344b28179b7647eabb01a1d))





## [0.26.77](https://github.com/cube-js/cube.js/compare/v0.26.76...v0.26.77) (2021-04-04)


### Bug Fixes

* Reduce Redis traffic for Refresh Scheduler by introducing single pre-aggregations load cache for a whole refresh cycle ([bdb69d5](https://github.com/cube-js/cube.js/commit/bdb69d55ee15e7d9ecca69a8e237d40ee666d051))





## [0.26.76](https://github.com/cube-js/cube.js/compare/v0.26.75...v0.26.76) (2021-04-03)


### Bug Fixes

* Reduce Redis traffic while querying immutable pre-aggregation partitions by introducing LRU in memory cache for refresh keys ([#2484](https://github.com/cube-js/cube.js/issues/2484)) ([76ea3c1](https://github.com/cube-js/cube.js/commit/76ea3c1a5227cfa8eda5b0ad6a09f41b71826866))





## [0.26.74](https://github.com/cube-js/cube.js/compare/v0.26.73...v0.26.74) (2021-04-01)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.26.69](https://github.com/cube-js/cube.js/compare/v0.26.68...v0.26.69) (2021-03-25)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.26.65](https://github.com/cube-js/cube.js/compare/v0.26.64...v0.26.65) (2021-03-24)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.26.54](https://github.com/cube-js/cube.js/compare/v0.26.53...v0.26.54) (2021-03-12)


### Features

* Suggest to install cubestore-driver to get external pre-agg layer ([2059e57](https://github.com/cube-js/cube.js/commit/2059e57a2aa7caf81691c083949395e9697d2bcb))





## [0.26.45](https://github.com/cube-js/cube.js/compare/v0.26.44...v0.26.45) (2021-03-04)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.26.38](https://github.com/cube-js/cube.js/compare/v0.26.37...v0.26.38) (2021-02-26)


### Bug Fixes

* Optimize compute time during pre-aggregations load checks for queries with many partitions ([b9b590b](https://github.com/cube-js/cube.js/commit/b9b590b2891674020b23bc2e56ea2ab8dc02fa10))





## [0.26.35](https://github.com/cube-js/cube.js/compare/v0.26.34...v0.26.35) (2021-02-25)


### Features

* Use Cube Store as default external storage for CUBEJS_DEV_MODE ([e526676](https://github.com/cube-js/cube.js/commit/e52667617e5e687c92d383045fb1a8d5fd19cab6))





## [0.26.25](https://github.com/cube-js/cube.js/compare/v0.26.24...v0.26.25) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.26.23](https://github.com/cube-js/cube.js/compare/v0.26.22...v0.26.23) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.26.22](https://github.com/cube-js/cube.js/compare/v0.26.21...v0.26.22) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.26.19](https://github.com/cube-js/cube.js/compare/v0.26.18...v0.26.19) (2021-02-19)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.26.16](https://github.com/cube-js/cube.js/compare/v0.26.15...v0.26.16) (2021-02-18)


### Bug Fixes

* continueWaitTimeout is ignored: expose single centralized continueWaitTimeout option ([#2120](https://github.com/cube-js/cube.js/issues/2120)) ([2735d2c](https://github.com/cube-js/cube.js/commit/2735d2c1b60c0a31e3970c65b7dd1c293351614e)), closes [#225](https://github.com/cube-js/cube.js/issues/225)





## [0.26.15](https://github.com/cube-js/cube.js/compare/v0.26.14...v0.26.15) (2021-02-16)


### Features

* support SSL ca/cert/key in .env files as multiline text ([#2072](https://github.com/cube-js/cube.js/issues/2072)) ([d9f5154](https://github.com/cube-js/cube.js/commit/d9f5154e38c14417aa332f9bcd3d38ef00553e5a))





## [0.26.13](https://github.com/cube-js/cube.js/compare/v0.26.12...v0.26.13) (2021-02-12)


### Features

* type detection - check overflows for int/bigint ([393948a](https://github.com/cube-js/cube.js/commit/393948ae5fabf1c4b46ce4ea2b2dc22e8f012ee1))





## [0.26.12](https://github.com/cube-js/cube.js/compare/v0.26.11...v0.26.12) (2021-02-11)


### Bug Fixes

* **query-orchestrator:** detect negative int/decimal as strings ([#2051](https://github.com/cube-js/cube.js/issues/2051)) ([2b8b549](https://github.com/cube-js/cube.js/commit/2b8b549b022e357d85b9f4549a4ff61c9d39fbeb))
* UnhandledPromiseRejectionWarning: Error: Continue wait. fix [#1873](https://github.com/cube-js/cube.js/issues/1873) ([7f113f6](https://github.com/cube-js/cube.js/commit/7f113f61bef5b197cf26a3948a80d052a9cda79d))





## [0.26.11](https://github.com/cube-js/cube.js/compare/v0.26.10...v0.26.11) (2021-02-10)


### Bug Fixes

* CUBEJS_SCHEDULED_REFRESH_TIMER, fix [#1972](https://github.com/cube-js/cube.js/issues/1972) ([#1975](https://github.com/cube-js/cube.js/issues/1975)) ([dac7e52](https://github.com/cube-js/cube.js/commit/dac7e52ee0d3a118c9d69c9d030e58a3c048cca1))





## [0.26.10](https://github.com/cube-js/cube.js/compare/v0.26.9...v0.26.10) (2021-02-09)


### Bug Fixes

* Using .end() without the flush parameter is deprecated and throws from v.3.0.0 ([7078f41](https://github.com/cube-js/cube.js/commit/7078f4146572a4eb447b9ed6f64e071b86e0aca2))





## [0.26.7](https://github.com/cube-js/cube.js/compare/v0.26.6...v0.26.7) (2021-02-09)


### Features

* Support for Redis Sentinel + IORedis driver. fix [#1769](https://github.com/cube-js/cube.js/issues/1769) ([a5e7972](https://github.com/cube-js/cube.js/commit/a5e7972485fa97faaf9965b9794b0cf48256f484))
* Use REDIS_URL for IORedis options (with santinels) ([988bfe5](https://github.com/cube-js/cube.js/commit/988bfe5526be3506fe7b773d247ad89b3287fad4))





## [0.26.6](https://github.com/cube-js/cube.js/compare/v0.26.5...v0.26.6) (2021-02-08)


### Features

* **@cubejs-client/playground:** Building pre-aggregations message ([#1984](https://github.com/cube-js/cube.js/issues/1984)) ([e1fff5d](https://github.com/cube-js/cube.js/commit/e1fff5de4584df1bd8ef518e2436e1dcb4962975))
* **server-core:** Correct typings for driverFactory/dialectFactory ([51fb117](https://github.com/cube-js/cube.js/commit/51fb117883d2e04c3a8fce4494ac48e0938a0097))





## [0.26.2](https://github.com/cube-js/cube.js/compare/v0.26.1...v0.26.2) (2021-02-01)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





# [0.26.0](https://github.com/cube-js/cube.js/compare/v0.25.33...v0.26.0) (2021-02-01)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.25.32](https://github.com/cube-js/cube.js/compare/v0.25.31...v0.25.32) (2021-01-29)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.25.30](https://github.com/cube-js/cube.js/compare/v0.25.29...v0.25.30) (2021-01-26)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.25.29](https://github.com/cube-js/cube.js/compare/v0.25.28...v0.25.29) (2021-01-26)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.25.26](https://github.com/cube-js/cube.js/compare/v0.25.25...v0.25.26) (2021-01-25)


### Features

* BigQuery CSV pre-aggregation download support ([#1867](https://github.com/cube-js/cube.js/issues/1867)) ([5a2ea3f](https://github.com/cube-js/cube.js/commit/5a2ea3f27058a01bf08f697495c8ccce5abf9fa2))





## [0.25.24](https://github.com/cube-js/cube.js/compare/v0.25.23...v0.25.24) (2021-01-22)


### Bug Fixes

* Non default data source cache key and table schema queries are forwarded to the default data source ([2f7c672](https://github.com/cube-js/cube.js/commit/2f7c67292468da60faea284751bf8c71d2e051f5))
* Non default data source cache key and table schema queries are forwarded to the default data source: broken test ([#1856](https://github.com/cube-js/cube.js/issues/1856)) ([8aad3f5](https://github.com/cube-js/cube.js/commit/8aad3f52f476836df4f93c266af96f30ceb57131))





## [0.25.23](https://github.com/cube-js/cube.js/compare/v0.25.22...v0.25.23) (2021-01-22)


### Bug Fixes

* Map int2/4/8 to generic int type. fix [#1796](https://github.com/cube-js/cube.js/issues/1796) ([78e20eb](https://github.com/cube-js/cube.js/commit/78e20eb304eda3086cda7dbc4ea5d33ef877facb))





## [0.25.22](https://github.com/cube-js/cube.js/compare/v0.25.21...v0.25.22) (2021-01-21)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.25.21](https://github.com/cube-js/cube.js/compare/v0.25.20...v0.25.21) (2021-01-19)


### Bug Fixes

* **@cubejs-backend/query-orchestrator:** prevent generic pool infinite loop ([#1793](https://github.com/cube-js/cube.js/issues/1793)) ([d4129c4](https://github.com/cube-js/cube.js/commit/d4129c4d71b4afa66f62ae5d9666fcd9a08d9187))





## [0.25.20](https://github.com/cube-js/cube.js/compare/v0.25.19...v0.25.20) (2021-01-15)


### Bug Fixes

* Remove unnecessary `SELECT 1` during scheduled refresh. Fixes [#1592](https://github.com/cube-js/cube.js/issues/1592) ([#1786](https://github.com/cube-js/cube.js/issues/1786)) ([66f9d91](https://github.com/cube-js/cube.js/commit/66f9d91d12b1853b69903475af8338bfa586026b))





## [0.25.18](https://github.com/cube-js/cube.js/compare/v0.25.17...v0.25.18) (2021-01-14)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.25.15](https://github.com/cube-js/cube.js/compare/v0.25.14...v0.25.15) (2021-01-12)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.25.13](https://github.com/cube-js/cube.js/compare/v0.25.12...v0.25.13) (2021-01-07)


### Bug Fixes

* Guard from `undefined` dataSource in queue key ([6ae1fd6](https://github.com/cube-js/cube.js/commit/6ae1fd60a1e67bc73c0630b7de36b598397ce22b))





## [0.25.6](https://github.com/cube-js/cube.js/compare/v0.25.5...v0.25.6) (2020-12-30)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.25.5](https://github.com/cube-js/cube.js/compare/v0.25.4...v0.25.5) (2020-12-30)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.25.4](https://github.com/cube-js/cube.js/compare/v0.25.3...v0.25.4) (2020-12-30)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.25.2](https://github.com/cube-js/cube.js/compare/v0.25.1...v0.25.2) (2020-12-27)


### Bug Fixes

* **@cubejs-backend/query-orchestrator:** Throw an exception on empty pre-agg in readOnly mode, refs [#1597](https://github.com/cube-js/cube.js/issues/1597) ([17d5fdb](https://github.com/cube-js/cube.js/commit/17d5fdb82e0ce06d55e438913e32952f32db7923))





## [0.25.1](https://github.com/cube-js/cube.js/compare/v0.25.0...v0.25.1) (2020-12-24)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





# [0.25.0](https://github.com/cube-js/cube.js/compare/v0.24.15...v0.25.0) (2020-12-21)


### Bug Fixes

* getQueryStage throws undefined is not a function ([0de1603](https://github.com/cube-js/cube.js/commit/0de1603293fc918c0da8ff8bd514b49f14de51d8))


### Features

* Allow cross data source joins ([a58336e](https://github.com/cube-js/cube.js/commit/a58336e3840f8ac02d83de43ec7661419bceb71c))
* Allow cross data source joins: Serverless support ([034cdc8](https://github.com/cube-js/cube.js/commit/034cdc8dbf8907988df0f999fd115b8acdb4990f))





## [0.24.13](https://github.com/cube-js/cube.js/compare/v0.24.12...v0.24.13) (2020-12-18)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.24.12](https://github.com/cube-js/cube.js/compare/v0.24.11...v0.24.12) (2020-12-17)


### Features

* **query-orchestrator:** detects bigint in readOnly mode, when it's Number ([a21cc10](https://github.com/cube-js/cube.js/commit/a21cc1065031b23ee7c199cb56ec039112f83770))
* Introduce health checks ([#1607](https://github.com/cube-js/cube.js/issues/1607)) ([d96c662](https://github.com/cube-js/cube.js/commit/d96c66201ca8202907af8dc563eaaf908a5ece89))





## [0.24.9](https://github.com/cube-js/cube.js/compare/v0.24.8...v0.24.9) (2020-12-16)


### Bug Fixes

* **@cubejs-backend/mysql-driver:** Revert back test on borrow with database pool error logging. ([2cdaf40](https://github.com/cube-js/cube.js/commit/2cdaf406a7d99116849f60e00e1b1bc25605e0d3))





## [0.24.8](https://github.com/cube-js/cube.js/compare/v0.24.7...v0.24.8) (2020-12-15)


### Features

* **@cubejs-backend/query-orchestrator:** Introduce AsyncRedisClient type ([728110e](https://github.com/cube-js/cube.js/commit/728110ed0ffe5697bd5e47e3920bf2e5377a0ffd))
* **@cubejs-backend/query-orchestrator:** Migrate createRedisClient to TS ([78e8422](https://github.com/cube-js/cube.js/commit/78e8422937e79457fdcec70535225bc9ccecfce8))
* **@cubejs-backend/query-orchestrator:** Move RedisPool to TS, export RedisPoolOptions ([8e8abde](https://github.com/cube-js/cube.js/commit/8e8abde85b9fa821d21f33fc286cfb2cc56891e4))
* **@cubejs-backend/query-orchestrator:** Set redis pool options from server config ([c1270d4](https://github.com/cube-js/cube.js/commit/c1270d4cfdc243b230ade0cb3a4c59171db70d20))





## [0.24.6](https://github.com/cube-js/cube.js/compare/v0.24.5...v0.24.6) (2020-12-13)


### Features

* Move index creation orchestration to the driver: allow to control drivers when to create indexes ([2a94e71](https://github.com/cube-js/cube.js/commit/2a94e710a89954ecedf4aa6f76b89578138e7aff))





## [0.24.5](https://github.com/cube-js/cube.js/compare/v0.24.4...v0.24.5) (2020-12-09)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.24.4](https://github.com/cube-js/cube.js/compare/v0.24.3...v0.24.4) (2020-12-07)


### Features

* Ability to load SSL keys from FS ([#1512](https://github.com/cube-js/cube.js/issues/1512)) ([71da5bb](https://github.com/cube-js/cube.js/commit/71da5bb529294fabd92b3a914b1e8bceb464643c))





## [0.24.3](https://github.com/cube-js/cube.js/compare/v0.24.2...v0.24.3) (2020-12-01)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.24.2](https://github.com/cube-js/cube.js/compare/v0.24.1...v0.24.2) (2020-11-27)


### Features

* **@cubejs-backend/query-orchestrator:** Initial move to TypeScript ([#1462](https://github.com/cube-js/cube.js/issues/1462)) ([101e8dc](https://github.com/cube-js/cube.js/commit/101e8dc90d4b1266c0327adb86cab3e3caa8d4d0))





# [0.24.0](https://github.com/cube-js/cube.js/compare/v0.23.15...v0.24.0) (2020-11-26)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.23.14](https://github.com/cube-js/cube.js/compare/v0.23.13...v0.23.14) (2020-11-22)


### Bug Fixes

* **@cubejs-backend/query-orchestrator:** Intermittent lags when pre-aggregation tables are refreshed ([4efe1fc](https://github.com/cube-js/cube.js/commit/4efe1fc006282d87ab2718918d1bdd174baa6be3))





## [0.23.6](https://github.com/cube-js/cube.js/compare/v0.23.5...v0.23.6) (2020-11-02)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.23.3](https://github.com/cube-js/cube.js/compare/v0.23.2...v0.23.3) (2020-10-31)


### Features

* **@cubejs-backend/query-orchestrator:** add support for MSSQL nvarchar ([#1260](https://github.com/cube-js/cube.js/issues/1260)) Thanks to @JoshMentzer! ([a9e9919](https://github.com/cube-js/cube.js/commit/a9e9919))





# [0.23.0](https://github.com/cube-js/cube.js/compare/v0.22.4...v0.23.0) (2020-10-28)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





# [0.22.0](https://github.com/cube-js/cube.js/compare/v0.21.2...v0.22.0) (2020-10-20)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.21.1](https://github.com/cube-js/cube.js/compare/v0.21.0...v0.21.1) (2020-10-15)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





# [0.21.0](https://github.com/cube-js/cube.js/compare/v0.20.15...v0.21.0) (2020-10-09)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.20.13](https://github.com/cube-js/cube.js/compare/v0.20.12...v0.20.13) (2020-10-07)


### Bug Fixes

* **@cubejs-backend/mongobi-driver:** TypeError: v.toLowerCase is not a function ([16a15cb](https://github.com/cube-js/cube.js/commit/16a15cb))





## [0.20.9](https://github.com/cube-js/cube.js/compare/v0.20.8...v0.20.9) (2020-09-19)


### Features

* `sqlAlias` attribute for `preAggregations` and short format for pre-aggregation table names ([#1068](https://github.com/cube-js/cube.js/issues/1068)) ([98ffad3](https://github.com/cube-js/cube.js/commit/98ffad3)), closes [#86](https://github.com/cube-js/cube.js/issues/86) [#907](https://github.com/cube-js/cube.js/issues/907)





# [0.20.0](https://github.com/cube-js/cube.js/compare/v0.19.61...v0.20.0) (2020-08-26)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.19.61](https://github.com/cube-js/cube.js/compare/v0.19.60...v0.19.61) (2020-08-11)


### Bug Fixes

* avoid opening connection to the source database when caching tables from external rollup db ([#929](https://github.com/cube-js/cube.js/issues/929)) Thanks to [@jcw](https://github.com/jcw)-! ([92cd0b3](https://github.com/cube-js/cube.js/commit/92cd0b3))





## [0.19.60](https://github.com/cube-js/cube.js/compare/v0.19.59...v0.19.60) (2020-08-08)


### Bug Fixes

* Intermittent errors with empty rollups or not ready metadata for Athena and MySQL: HIVE_CANNOT_OPEN_SPLIT errors. ([fa2cf45](https://github.com/cube-js/cube.js/commit/fa2cf45))





## [0.19.56](https://github.com/cube-js/cube.js/compare/v0.19.55...v0.19.56) (2020-08-03)


### Bug Fixes

* allow renewQuery in dev mode with warning ([#868](https://github.com/cube-js/cube.js/issues/868)) Thanks to [@jcw](https://github.com/jcw)-! ([dbdbb5f](https://github.com/cube-js/cube.js/commit/dbdbb5f))





## [0.19.54](https://github.com/cube-js/cube.js/compare/v0.19.53...v0.19.54) (2020-07-23)


### Bug Fixes

* Orphaned queries in Redis queue during intensive load ([101b85f](https://github.com/cube-js/cube.js/commit/101b85f))





## [0.19.53](https://github.com/cube-js/cube.js/compare/v0.19.52...v0.19.53) (2020-07-20)


### Features

* More logging info for Orphaned Queries debugging ([99bf957](https://github.com/cube-js/cube.js/commit/99bf957))





## [0.19.52](https://github.com/cube-js/cube.js/compare/v0.19.51...v0.19.52) (2020-07-18)


### Bug Fixes

* Redis driver execAsync ignores watch directives ([ac67e5b](https://github.com/cube-js/cube.js/commit/ac67e5b))





## [0.19.50](https://github.com/cube-js/cube.js/compare/v0.19.49...v0.19.50) (2020-07-16)


### Features

* Generic readOnly external rollup implementation. MongoDB support. ([79d7bfd](https://github.com/cube-js/cube.js/commit/79d7bfd)), closes [#239](https://github.com/cube-js/cube.js/issues/239)
* Rollup mode ([#843](https://github.com/cube-js/cube.js/issues/843)) Thanks to [@jcw](https://github.com/jcw)-! ([cc41f97](https://github.com/cube-js/cube.js/commit/cc41f97))





## [0.19.46](https://github.com/cube-js/cube.js/compare/v0.19.45...v0.19.46) (2020-07-06)


### Features

* Report query usage for Athena and BigQuery ([697b53f](https://github.com/cube-js/cube.js/commit/697b53f))





## [0.19.36](https://github.com/cube-js/cube.js/compare/v0.19.35...v0.19.36) (2020-06-24)


### Bug Fixes

* Avoid excessive pre-aggregation invalidation in presence of multiple structure versions ([fd5e602](https://github.com/cube-js/cube.js/commit/fd5e602))





## [0.19.17](https://github.com/cube-js/cube.js/compare/v0.19.16...v0.19.17) (2020-05-09)


### Bug Fixes

* Continue wait errors during tables fetch ([cafaa28](https://github.com/cube-js/cube.js/commit/cafaa28))





## [0.19.15](https://github.com/cube-js/cube.js/compare/v0.19.14...v0.19.15) (2020-05-04)


### Features

* More pre-aggregation info logging ([9d69f98](https://github.com/cube-js/cube.js/commit/9d69f98))





## [0.19.14](https://github.com/cube-js/cube.js/compare/v0.19.13...v0.19.14) (2020-04-24)


### Bug Fixes

* More descriptive errors for download errors ([e834aba](https://github.com/cube-js/cube.js/commit/e834aba))





## [0.19.9](https://github.com/cube-js/cube.js/compare/v0.19.8...v0.19.9) (2020-04-16)


### Features

* Allow persisting multiple pre-aggregation structure versions to support staging pre-aggregation warm-up environments and multiple timezones ([ab9539a](https://github.com/cube-js/cube.js/commit/ab9539a))





## [0.19.8](https://github.com/cube-js/cube.js/compare/v0.19.7...v0.19.8) (2020-04-15)


### Bug Fixes

* Dead queries added to queue in serverless ([eca3d0c](https://github.com/cube-js/cube.js/commit/eca3d0c))





## [0.19.7](https://github.com/cube-js/cube.js/compare/v0.19.6...v0.19.7) (2020-04-14)


### Bug Fixes

* Associate Queue storage error with requestId ([ec2750e](https://github.com/cube-js/cube.js/commit/ec2750e))





## [0.19.6](https://github.com/cube-js/cube.js/compare/v0.19.5...v0.19.6) (2020-04-14)


### Bug Fixes

* Consistent queryKey logging ([5f1a632](https://github.com/cube-js/cube.js/commit/5f1a632))





## [0.19.5](https://github.com/cube-js/cube.js/compare/v0.19.4...v0.19.5) (2020-04-13)


### Bug Fixes

* Broken query and pre-aggregation cancel ([aa82256](https://github.com/cube-js/cube.js/commit/aa82256))


### Features

* Log queue state on Waiting for query ([395c63c](https://github.com/cube-js/cube.js/commit/395c63c))





# [0.19.0](https://github.com/cube-js/cube.js/compare/v0.18.32...v0.19.0) (2020-04-09)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.18.18](https://github.com/cube-js/cube.js/compare/v0.18.17...v0.18.18) (2020-03-28)


### Features

* Executing SQL logging message that shows final SQL ([26b8758](https://github.com/cube-js/cube.js/commit/26b8758))





## [0.18.17](https://github.com/cube-js/cube.js/compare/v0.18.16...v0.18.17) (2020-03-24)


### Features

* More places to fetch `readOnly` pre-aggregations flag from ([9877037](https://github.com/cube-js/cube.js/commit/9877037))





## [0.18.13](https://github.com/cube-js/cube.js/compare/v0.18.12...v0.18.13) (2020-03-21)


### Bug Fixes

* Various cleanup errors ([538f6d0](https://github.com/cube-js/cube.js/commit/538f6d0)), closes [#525](https://github.com/cube-js/cube.js/issues/525)





## [0.18.12](https://github.com/cube-js/cube.js/compare/v0.18.11...v0.18.12) (2020-03-19)


### Features

* Add duration to error logging ([59a4255](https://github.com/cube-js/cube.js/commit/59a4255))





## [0.18.11](https://github.com/cube-js/cube.js/compare/v0.18.10...v0.18.11) (2020-03-18)


### Bug Fixes

* Orphaned pre-aggregation tables aren't dropped because LocalCacheDriver doesn't expire keys ([393af3d](https://github.com/cube-js/cube.js/commit/393af3d))





## [0.18.7](https://github.com/cube-js/cube.js/compare/v0.18.6...v0.18.7) (2020-03-17)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.18.6](https://github.com/cube-js/cube.js/compare/v0.18.5...v0.18.6) (2020-03-16)


### Bug Fixes

* Waiting for query isn't logged for Local Queue when query is already in progress ([e7be6d1](https://github.com/cube-js/cube.js/commit/e7be6d1))





## [0.18.5](https://github.com/cube-js/cube.js/compare/v0.18.4...v0.18.5) (2020-03-15)


### Bug Fixes

* `requestId` isn't propagating to all pre-aggregations messages ([650dd6e](https://github.com/cube-js/cube.js/commit/650dd6e))





## [0.18.4](https://github.com/cube-js/cube.js/compare/v0.18.3...v0.18.4) (2020-03-09)


### Features

* Use options pattern in constructor ([#468](https://github.com/cube-js/cube.js/issues/468)) Thanks to [@jcw](https://github.com/jcw)-! ([ff20167](https://github.com/cube-js/cube.js/commit/ff20167))





## [0.18.3](https://github.com/cube-js/cube.js/compare/v0.18.2...v0.18.3) (2020-03-02)


### Bug Fixes

* CUBEJS_REDIS_POOL_MAX=0 env variable setting isn't respected ([75f6889](https://github.com/cube-js/cube.js/commit/75f6889))





## [0.18.2](https://github.com/cube-js/cube.js/compare/v0.18.1...v0.18.2) (2020-03-01)


### Bug Fixes

* Limit pre-aggregations fetch table requests using queue -- handle HA for pre-aggregations ([75833b1](https://github.com/cube-js/cube.js/commit/75833b1))





## [0.18.1](https://github.com/cube-js/cube.js/compare/v0.18.0...v0.18.1) (2020-03-01)


### Bug Fixes

* Remove user facing errors for pre-aggregations refreshes ([d15c551](https://github.com/cube-js/cube.js/commit/d15c551))





# [0.18.0](https://github.com/cube-js/cube.js/compare/v0.17.10...v0.18.0) (2020-03-01)


### Bug Fixes

* Error: client.readOnly is not a function ([6069499](https://github.com/cube-js/cube.js/commit/6069499))
* Redis query queue locking redesign ([a2eb9b2](https://github.com/cube-js/cube.js/commit/a2eb9b2)), closes [#459](https://github.com/cube-js/cube.js/issues/459)
* TypeError: Cannot read property 'queryKey' of null under load ([0c996d8](https://github.com/cube-js/cube.js/commit/0c996d8))


### Features

* Redis connection pooling ([#433](https://github.com/cube-js/cube.js/issues/433)) Thanks to [@jcw](https://github.com/jcw)! ([cf133a9](https://github.com/cube-js/cube.js/commit/cf133a9)), closes [#104](https://github.com/cube-js/cube.js/issues/104)





## [0.17.10](https://github.com/cube-js/cube.js/compare/v0.17.9...v0.17.10) (2020-02-20)


### Features

* Support external rollups from readonly source ([#395](https://github.com/cube-js/cube.js/issues/395)) ([b17e841](https://github.com/cube-js/cube.js/commit/b17e841))





## [0.17.9](https://github.com/cube-js/cube.js/compare/v0.17.8...v0.17.9) (2020-02-18)


### Features

* Enhanced trace logging ([1fdd8e9](https://github.com/cube-js/cube.js/commit/1fdd8e9))





## [0.17.5](https://github.com/cube-js/cube.js/compare/v0.17.4...v0.17.5) (2020-02-07)


### Bug Fixes

* Sanity check for silent truncate name problem during pre-aggregation creation ([e7fb2f2](https://github.com/cube-js/cube.js/commit/e7fb2f2))





## [0.17.3](https://github.com/cube-js/cube.js/compare/v0.17.2...v0.17.3) (2020-02-06)


### Features

* Pre-aggregation indexes support ([d443585](https://github.com/cube-js/cube.js/commit/d443585))





# [0.17.0](https://github.com/cube-js/cube.js/compare/v0.16.0...v0.17.0) (2020-02-04)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





# [0.16.0](https://github.com/cube-js/cube.js/compare/v0.15.4...v0.16.0) (2020-02-04)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





# [0.15.0](https://github.com/cube-js/cube.js/compare/v0.14.3...v0.15.0) (2020-01-18)


### Features

* New refreshKeyRenewalThresholds and foreground renew defaults ([9fb0abb](https://github.com/cube-js/cube.js/commit/9fb0abb))
* Slow Query Warning and scheduled refresh for cube refresh keys ([8768b0e](https://github.com/cube-js/cube.js/commit/8768b0e))





# [0.14.0](https://github.com/cube-js/cube.js/compare/v0.13.12...v0.14.0) (2020-01-16)


### Bug Fixes

* Cannot read property 'requestId' of null ([d087837](https://github.com/cube-js/cube.js/commit/d087837)), closes [#347](https://github.com/cube-js/cube.js/issues/347)


### Features

* Scheduled refresh for pre-aggregations ([c87b525](https://github.com/cube-js/cube.js/commit/c87b525))
* Scheduled Refresh REST API ([472a0c3](https://github.com/cube-js/cube.js/commit/472a0c3))





## [0.13.9](https://github.com/cube-js/cube.js/compare/v0.13.8...v0.13.9) (2020-01-03)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.13.2](https://github.com/cube-js/cube.js/compare/v0.13.1...v0.13.2) (2019-12-13)


### Features

* Propagate `requestId` for trace logging ([24d7b41](https://github.com/cube-js/cube.js/commit/24d7b41))





# [0.13.0](https://github.com/cube-js/cube.js/compare/v0.12.3...v0.13.0) (2019-12-10)

**Note:** Version bump only for package @cubejs-backend/query-orchestrator





## [0.12.2](https://github.com/cube-js/cube.js/compare/v0.12.1...v0.12.2) (2019-12-02)


### Bug Fixes

* this.versionEntries typo ([#279](https://github.com/cube-js/cube.js/issues/279)) ([743f9fb](https://github.com/cube-js/cube.js/commit/743f9fb))


### Features

* support REDIS_PASSWORD env variable ([#280](https://github.com/cube-js/cube.js/issues/280)). Thanks to [@lanphan](https://github.com/lanphan)! ([5172745](https://github.com/cube-js/cube.js/commit/5172745))





## [0.12.1](https://github.com/cube-js/cube.js/compare/v0.12.0...v0.12.1) (2019-11-26)


### Features

* Show used pre-aggregations and match rollup results in Playground ([4a67346](https://github.com/cube-js/cube.js/commit/4a67346))





# [0.12.0](https://github.com/cube-js/cube.js/compare/v0.11.25...v0.12.0) (2019-11-25)


### Features

* Show `refreshKey` values in Playground ([b49e184](https://github.com/cube-js/cube.js/commit/b49e184))





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
