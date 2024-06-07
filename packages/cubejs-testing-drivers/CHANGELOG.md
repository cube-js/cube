# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.35.47](https://github.com/cube-js/cube.js/compare/v0.35.46...v0.35.47) (2024-06-07)


### Bug Fixes

* **cubesql:** Rollup doesn't work over aliased columns ([#8334](https://github.com/cube-js/cube.js/issues/8334)) ([98e7529](https://github.com/cube-js/cube.js/commit/98e7529975703f2d4b72cc8f21ce4f8c6fc4c8de))





## [0.35.46](https://github.com/cube-js/cube.js/compare/v0.35.45...v0.35.46) (2024-06-06)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.45](https://github.com/cube-js/cube.js/compare/v0.35.44...v0.35.45) (2024-06-05)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.44](https://github.com/cube-js/cube/compare/v0.35.43...v0.35.44) (2024-06-04)


### Bug Fixes

* **databricks-jdbc-driver:** Rolling window & count_distinct_approx (HLL) ([#8323](https://github.com/cube-js/cube/issues/8323)) ([5969f0d](https://github.com/cube-js/cube/commit/5969f0d788fffc1fe3492783eec4270325520d38))
* **query-orchestrator:** Range intersection for 6 digits timestamp, fix [#8320](https://github.com/cube-js/cube/issues/8320) ([#8322](https://github.com/cube-js/cube/issues/8322)) ([667e95b](https://github.com/cube-js/cube/commit/667e95bd9933d67f84409c09f61d47b28156f0c2))





## [0.35.43](https://github.com/cube-js/cube/compare/v0.35.42...v0.35.43) (2024-05-31)


### Features

* **bigquery-driver:** Use 6 digits precision for timestamps ([#8308](https://github.com/cube-js/cube/issues/8308)) ([568bfe3](https://github.com/cube-js/cube/commit/568bfe34bc6aca136b580acb8873d208b522a2f3))





## [0.35.42](https://github.com/cube-js/cube/compare/v0.35.41...v0.35.42) (2024-05-30)


### Features

* **cubesql:** Group By Rollup support ([#8281](https://github.com/cube-js/cube/issues/8281)) ([e563798](https://github.com/cube-js/cube/commit/e5637980489608e374f3b89cc219207973a08bbb))





## [0.35.41](https://github.com/cube-js/cube/compare/v0.35.40...v0.35.41) (2024-05-27)


### Features

* **databricks-driver:** Support HLL feature with export bucket ([#8301](https://github.com/cube-js/cube/issues/8301)) ([7f97af4](https://github.com/cube-js/cube/commit/7f97af42d6a6c0645bd778e94f75d62280ffeba6))





## [0.35.40](https://github.com/cube-js/cube/compare/v0.35.39...v0.35.40) (2024-05-24)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.39](https://github.com/cube-js/cube/compare/v0.35.38...v0.35.39) (2024-05-24)


### Features

* **schema-compiler:** Support multi time dimensions for rollup pre-aggregations ([#8291](https://github.com/cube-js/cube/issues/8291)) ([8b0a056](https://github.com/cube-js/cube/commit/8b0a05657c7bc7f42e0d45ecc7ce03d37e5b1e57))





## [0.35.38](https://github.com/cube-js/cube/compare/v0.35.37...v0.35.38) (2024-05-22)


### Features

* **clickhouse-driver:** SQL API PUSH down - support datetrunc, timestamp_literal ([45bc230](https://github.com/cube-js/cube/commit/45bc2306eb01e2b2cc5c204b64bfa0411ae0144b))





## [0.35.37](https://github.com/cube-js/cube/compare/v0.35.36...v0.35.37) (2024-05-20)


### Bug Fixes

* **cubesql:** Make param render respect dialect's reuse params flag ([9c91af2](https://github.com/cube-js/cube/commit/9c91af2e03c84d903ac153337cda9f53682aacf1))





## [0.35.36](https://github.com/cube-js/cube/compare/v0.35.35...v0.35.36) (2024-05-17)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.35](https://github.com/cube-js/cube.js/compare/v0.35.34...v0.35.35) (2024-05-17)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.34](https://github.com/cube-js/cube/compare/v0.35.33...v0.35.34) (2024-05-15)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.33](https://github.com/cube-js/cube.js/compare/v0.35.32...v0.35.33) (2024-05-15)


### Bug Fixes

* Mismatched input '10000'. Expecting: '?', 'ALL', <integer> for post-aggregate members in Athena ([#8262](https://github.com/cube-js/cube.js/issues/8262)) ([59834e7](https://github.com/cube-js/cube.js/commit/59834e7157bf060804470104e0a713194b811f39))





## [0.35.32](https://github.com/cube-js/cube/compare/v0.35.31...v0.35.32) (2024-05-14)


### Features

* **databricks-jdbc-driver:** Support HLL ([#8257](https://github.com/cube-js/cube/issues/8257)) ([da231ed](https://github.com/cube-js/cube/commit/da231ed48ae8386f1726710e80c3a943204f6895))





## [0.35.31](https://github.com/cube-js/cube/compare/v0.35.30...v0.35.31) (2024-05-13)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.30](https://github.com/cube-js/cube.js/compare/v0.35.29...v0.35.30) (2024-05-10)


### Bug Fixes

* Unexpected keyword WITH for rolling window measures in BigQuery ([9468f90](https://github.com/cube-js/cube.js/commit/9468f90fefdc08280e7b81b0f8e289fa041cd37d)), closes [#8193](https://github.com/cube-js/cube.js/issues/8193)





## [0.35.29](https://github.com/cube-js/cube.js/compare/v0.35.28...v0.35.29) (2024-05-03)


### Bug Fixes

* Apply time shift after timezone conversions to avoid double casts ([#8229](https://github.com/cube-js/cube.js/issues/8229)) ([651b9e0](https://github.com/cube-js/cube.js/commit/651b9e029d8b0a685883d5151a0538f2f6429da2))





## [0.35.28](https://github.com/cube-js/cube/compare/v0.35.27...v0.35.28) (2024-05-02)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.27](https://github.com/cube-js/cube/compare/v0.35.26...v0.35.27) (2024-05-02)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.26](https://github.com/cube-js/cube/compare/v0.35.25...v0.35.26) (2024-05-02)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.25](https://github.com/cube-js/cube.js/compare/v0.35.24...v0.35.25) (2024-04-29)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.24](https://github.com/cube-js/cube/compare/v0.35.23...v0.35.24) (2024-04-26)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.23](https://github.com/cube-js/cube/compare/v0.35.22...v0.35.23) (2024-04-25)


### Features

* primary and foreign keys driver queries ([#8115](https://github.com/cube-js/cube/issues/8115)) ([35bb1d4](https://github.com/cube-js/cube/commit/35bb1d435a75f53f704e9c5e33382093cbc4e115))





## [0.35.22](https://github.com/cube-js/cube/compare/v0.35.21...v0.35.22) (2024-04-22)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.21](https://github.com/cube-js/cube/compare/v0.35.20...v0.35.21) (2024-04-19)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.20](https://github.com/cube-js/cube.js/compare/v0.35.19...v0.35.20) (2024-04-18)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.19](https://github.com/cube-js/cube.js/compare/v0.35.18...v0.35.19) (2024-04-18)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.18](https://github.com/cube-js/cube.js/compare/v0.35.17...v0.35.18) (2024-04-17)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.17](https://github.com/cube-js/cube/compare/v0.35.16...v0.35.17) (2024-04-16)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.16](https://github.com/cube-js/cube.js/compare/v0.35.15...v0.35.16) (2024-04-16)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.15](https://github.com/cube-js/cube.js/compare/v0.35.14...v0.35.15) (2024-04-15)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.14](https://github.com/cube-js/cube/compare/v0.35.13...v0.35.14) (2024-04-15)


### Bug Fixes

* **server-core:** Handle schemaPath default value correctly everywhere (refix) ([#8152](https://github.com/cube-js/cube/issues/8152)) ([678f17f](https://github.com/cube-js/cube/commit/678f17f2ef5a2cf38b28e3c095a721f1cd38fb56))





## [0.35.13](https://github.com/cube-js/cube.js/compare/v0.35.12...v0.35.13) (2024-04-15)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.12](https://github.com/cube-js/cube.js/compare/v0.35.11...v0.35.12) (2024-04-12)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.11](https://github.com/cube-js/cube/compare/v0.35.10...v0.35.11) (2024-04-11)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.10](https://github.com/cube-js/cube/compare/v0.35.9...v0.35.10) (2024-04-09)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.9](https://github.com/cube-js/cube/compare/v0.35.8...v0.35.9) (2024-04-08)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.8](https://github.com/cube-js/cube/compare/v0.35.7...v0.35.8) (2024-04-05)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.7](https://github.com/cube-js/cube/compare/v0.35.6...v0.35.7) (2024-04-03)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.6](https://github.com/cube-js/cube/compare/v0.35.5...v0.35.6) (2024-04-02)


### Features

* Ungrouped query pre-aggregation support ([#8058](https://github.com/cube-js/cube/issues/8058)) ([2ca99de](https://github.com/cube-js/cube/commit/2ca99de3e7ea813bdb7ea684bed4af886bde237b))





## [0.35.5](https://github.com/cube-js/cube.js/compare/v0.35.4...v0.35.5) (2024-03-28)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.4](https://github.com/cube-js/cube.js/compare/v0.35.3...v0.35.4) (2024-03-27)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.3](https://github.com/cube-js/cube.js/compare/v0.35.2...v0.35.3) (2024-03-22)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.2](https://github.com/cube-js/cube/compare/v0.35.1...v0.35.2) (2024-03-22)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.35.1](https://github.com/cube-js/cube/compare/v0.35.0...v0.35.1) (2024-03-18)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





# [0.35.0](https://github.com/cube-js/cube/compare/v0.34.62...v0.35.0) (2024-03-14)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.62](https://github.com/cube-js/cube/compare/v0.34.61...v0.34.62) (2024-03-13)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.61](https://github.com/cube-js/cube/compare/v0.34.60...v0.34.61) (2024-03-11)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.60](https://github.com/cube-js/cube.js/compare/v0.34.59...v0.34.60) (2024-03-02)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.59](https://github.com/cube-js/cube.js/compare/v0.34.58...v0.34.59) (2024-02-28)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.58](https://github.com/cube-js/cube.js/compare/v0.34.57...v0.34.58) (2024-02-27)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.57](https://github.com/cube-js/cube.js/compare/v0.34.56...v0.34.57) (2024-02-26)


### Bug Fixes

* **cubesql:** `ungrouped` query can be routed to Cube Store ([#7810](https://github.com/cube-js/cube.js/issues/7810)) ([b122837](https://github.com/cube-js/cube.js/commit/b122837de9cd4fcaca4ddc0e7f85ff695de09483))





## [0.34.56](https://github.com/cube-js/cube.js/compare/v0.34.55...v0.34.56) (2024-02-20)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.55](https://github.com/cube-js/cube.js/compare/v0.34.54...v0.34.55) (2024-02-15)


### Bug Fixes

* **cubesql:** Quote `FROM` alias for SQL push down to avoid name clas… ([#7755](https://github.com/cube-js/cube.js/issues/7755)) ([4e2732a](https://github.com/cube-js/cube.js/commit/4e2732ae9997762a95fc946a5392b50e4dbf8622))





## [0.34.54](https://github.com/cube-js/cube.js/compare/v0.34.53...v0.34.54) (2024-02-13)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.53](https://github.com/cube-js/cube.js/compare/v0.34.52...v0.34.53) (2024-02-13)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.52](https://github.com/cube-js/cube.js/compare/v0.34.51...v0.34.52) (2024-02-13)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.51](https://github.com/cube-js/cube.js/compare/v0.34.50...v0.34.51) (2024-02-11)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.50](https://github.com/cube-js/cube/compare/v0.34.49...v0.34.50) (2024-01-31)


### Bug Fixes

* Merge streaming methods to one interface to allow SQL API use batching and Databricks batching implementation ([#7695](https://github.com/cube-js/cube/issues/7695)) ([73ad72d](https://github.com/cube-js/cube/commit/73ad72dd8a104651cf5ef23a60e8b4c116c97eed))





## [0.34.49](https://github.com/cube-js/cube.js/compare/v0.34.48...v0.34.49) (2024-01-26)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.48](https://github.com/cube-js/cube.js/compare/v0.34.47...v0.34.48) (2024-01-25)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.47](https://github.com/cube-js/cube.js/compare/v0.34.46...v0.34.47) (2024-01-23)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.46](https://github.com/cube-js/cube.js/compare/v0.34.45...v0.34.46) (2024-01-18)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.45](https://github.com/cube-js/cube.js/compare/v0.34.44...v0.34.45) (2024-01-16)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.44](https://github.com/cube-js/cube/compare/v0.34.43...v0.34.44) (2024-01-15)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.43](https://github.com/cube-js/cube/compare/v0.34.42...v0.34.43) (2024-01-11)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.42](https://github.com/cube-js/cube.js/compare/v0.34.41...v0.34.42) (2024-01-07)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.41](https://github.com/cube-js/cube.js/compare/v0.34.40...v0.34.41) (2024-01-02)


### Bug Fixes

* **databricks-jdbc-driver:** Time series queries with rolling window & time dimension ([#7564](https://github.com/cube-js/cube.js/issues/7564)) ([79d033e](https://github.com/cube-js/cube.js/commit/79d033eecc54c4ae5a4e04ed7713fb34957af091))





## [0.34.40](https://github.com/cube-js/cube.js/compare/v0.34.39...v0.34.40) (2023-12-21)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.39](https://github.com/cube-js/cube/compare/v0.34.38...v0.34.39) (2023-12-21)


### Bug Fixes

* **clickhouse-driver:** Correct parsing for DateTime('timezone') ([#7565](https://github.com/cube-js/cube/issues/7565)) ([d39e4a2](https://github.com/cube-js/cube/commit/d39e4a25f2982cd2f66434f8095905883b9815ff))





## [0.34.38](https://github.com/cube-js/cube/compare/v0.34.37...v0.34.38) (2023-12-19)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.37](https://github.com/cube-js/cube.js/compare/v0.34.36...v0.34.37) (2023-12-19)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.36](https://github.com/cube-js/cube.js/compare/v0.34.35...v0.34.36) (2023-12-16)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.35](https://github.com/cube-js/cube.js/compare/v0.34.34...v0.34.35) (2023-12-13)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.34](https://github.com/cube-js/cube/compare/v0.34.33...v0.34.34) (2023-12-12)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.33](https://github.com/cube-js/cube/compare/v0.34.32...v0.34.33) (2023-12-11)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.32](https://github.com/cube-js/cube.js/compare/v0.34.31...v0.34.32) (2023-12-07)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.31](https://github.com/cube-js/cube.js/compare/v0.34.30...v0.34.31) (2023-12-07)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.30](https://github.com/cube-js/cube.js/compare/v0.34.29...v0.34.30) (2023-12-04)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.29](https://github.com/cube-js/cube/compare/v0.34.28...v0.34.29) (2023-12-01)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.28](https://github.com/cube-js/cube/compare/v0.34.27...v0.34.28) (2023-11-30)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.27](https://github.com/cube-js/cube.js/compare/v0.34.26...v0.34.27) (2023-11-30)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.26](https://github.com/cube-js/cube/compare/v0.34.25...v0.34.26) (2023-11-28)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.25](https://github.com/cube-js/cube/compare/v0.34.24...v0.34.25) (2023-11-24)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.24](https://github.com/cube-js/cube/compare/v0.34.23...v0.34.24) (2023-11-23)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.23](https://github.com/cube-js/cube/compare/v0.34.22...v0.34.23) (2023-11-19)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.22](https://github.com/cube-js/cube.js/compare/v0.34.21...v0.34.22) (2023-11-16)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.21](https://github.com/cube-js/cube.js/compare/v0.34.20...v0.34.21) (2023-11-15)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.20](https://github.com/cube-js/cube/compare/v0.34.19...v0.34.20) (2023-11-14)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.19](https://github.com/cube-js/cube.js/compare/v0.34.18...v0.34.19) (2023-11-11)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.18](https://github.com/cube-js/cube.js/compare/v0.34.17...v0.34.18) (2023-11-10)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.17](https://github.com/cube-js/cube/compare/v0.34.16...v0.34.17) (2023-11-09)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.16](https://github.com/cube-js/cube/compare/v0.34.15...v0.34.16) (2023-11-06)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.15](https://github.com/cube-js/cube.js/compare/v0.34.14...v0.34.15) (2023-11-06)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.14](https://github.com/cube-js/cube.js/compare/v0.34.13...v0.34.14) (2023-11-05)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.13](https://github.com/cube-js/cube.js/compare/v0.34.12...v0.34.13) (2023-10-31)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.12](https://github.com/cube-js/cube/compare/v0.34.11...v0.34.12) (2023-10-30)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.11](https://github.com/cube-js/cube/compare/v0.34.10...v0.34.11) (2023-10-29)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.10](https://github.com/cube-js/cube.js/compare/v0.34.9...v0.34.10) (2023-10-27)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.9](https://github.com/cube-js/cube/compare/v0.34.8...v0.34.9) (2023-10-26)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.8](https://github.com/cube-js/cube.js/compare/v0.34.7...v0.34.8) (2023-10-25)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.7](https://github.com/cube-js/cube/compare/v0.34.6...v0.34.7) (2023-10-23)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.6](https://github.com/cube-js/cube/compare/v0.34.5...v0.34.6) (2023-10-20)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.5](https://github.com/cube-js/cube/compare/v0.34.4...v0.34.5) (2023-10-16)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.4](https://github.com/cube-js/cube/compare/v0.34.3...v0.34.4) (2023-10-14)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.3](https://github.com/cube-js/cube/compare/v0.34.2...v0.34.3) (2023-10-12)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.2](https://github.com/cube-js/cube/compare/v0.34.1...v0.34.2) (2023-10-12)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.34.1](https://github.com/cube-js/cube.js/compare/v0.34.0...v0.34.1) (2023-10-09)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





# [0.34.0](https://github.com/cube-js/cube.js/compare/v0.33.65...v0.34.0) (2023-10-03)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.65](https://github.com/cube-js/cube.js/compare/v0.33.64...v0.33.65) (2023-10-02)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.64](https://github.com/cube-js/cube.js/compare/v0.33.63...v0.33.64) (2023-09-30)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.63](https://github.com/cube-js/cube.js/compare/v0.33.62...v0.33.63) (2023-09-26)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.62](https://github.com/cube-js/cube/compare/v0.33.61...v0.33.62) (2023-09-25)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.61](https://github.com/cube-js/cube/compare/v0.33.60...v0.33.61) (2023-09-22)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.60](https://github.com/cube-js/cube.js/compare/v0.33.59...v0.33.60) (2023-09-22)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.59](https://github.com/cube-js/cube.js/compare/v0.33.58...v0.33.59) (2023-09-20)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.58](https://github.com/cube-js/cube/compare/v0.33.57...v0.33.58) (2023-09-18)


### Features

* new methods for step-by-step db schema fetching ([#7058](https://github.com/cube-js/cube/issues/7058)) ([a362c20](https://github.com/cube-js/cube/commit/a362c2042d4158ae735e9afe0cfeae15c331dc9d))





## [0.33.57](https://github.com/cube-js/cube/compare/v0.33.56...v0.33.57) (2023-09-15)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.56](https://github.com/cube-js/cube/compare/v0.33.55...v0.33.56) (2023-09-13)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.55](https://github.com/cube-js/cube/compare/v0.33.54...v0.33.55) (2023-09-12)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.54](https://github.com/cube-js/cube.js/compare/v0.33.53...v0.33.54) (2023-09-12)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.53](https://github.com/cube-js/cube.js/compare/v0.33.52...v0.33.53) (2023-09-08)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.52](https://github.com/cube-js/cube/compare/v0.33.51...v0.33.52) (2023-09-07)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.51](https://github.com/cube-js/cube/compare/v0.33.50...v0.33.51) (2023-09-06)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.50](https://github.com/cube-js/cube/compare/v0.33.49...v0.33.50) (2023-09-04)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.49](https://github.com/cube-js/cube/compare/v0.33.48...v0.33.49) (2023-08-31)


### Bug Fixes

* **databricks-driver:** Uppercase filter values doesn't match in contains filter ([#7067](https://github.com/cube-js/cube/issues/7067)) ([1e29bb3](https://github.com/cube-js/cube/commit/1e29bb396434730fb705c5406c7a7f3df91b7edf))





## [0.33.48](https://github.com/cube-js/cube/compare/v0.33.47...v0.33.48) (2023-08-23)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.47](https://github.com/cube-js/cube.js/compare/v0.33.46...v0.33.47) (2023-08-15)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.46](https://github.com/cube-js/cube.js/compare/v0.33.45...v0.33.46) (2023-08-14)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.45](https://github.com/cube-js/cube.js/compare/v0.33.44...v0.33.45) (2023-08-13)


### Features

* **cubesql:** Whole SQL query push down to data sources ([#6629](https://github.com/cube-js/cube.js/issues/6629)) ([0e8a76a](https://github.com/cube-js/cube.js/commit/0e8a76a20cb37e675997f384785dd06e09175113))





## [0.33.44](https://github.com/cube-js/cube/compare/v0.33.43...v0.33.44) (2023-08-11)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.43](https://github.com/cube-js/cube/compare/v0.33.42...v0.33.43) (2023-08-04)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.42](https://github.com/cube-js/cube/compare/v0.33.41...v0.33.42) (2023-08-03)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.41](https://github.com/cube-js/cube/compare/v0.33.40...v0.33.41) (2023-07-28)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.40](https://github.com/cube-js/cube/compare/v0.33.39...v0.33.40) (2023-07-27)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.39](https://github.com/cube-js/cube/compare/v0.33.38...v0.33.39) (2023-07-25)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.38](https://github.com/cube-js/cube/compare/v0.33.37...v0.33.38) (2023-07-21)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.37](https://github.com/cube-js/cube/compare/v0.33.36...v0.33.37) (2023-07-20)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.36](https://github.com/cube-js/cube/compare/v0.33.35...v0.33.36) (2023-07-13)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.35](https://github.com/cube-js/cube/compare/v0.33.34...v0.33.35) (2023-07-12)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.34](https://github.com/cube-js/cube/compare/v0.33.33...v0.33.34) (2023-07-12)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.33](https://github.com/cube-js/cube.js/compare/v0.33.32...v0.33.33) (2023-07-08)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.32](https://github.com/cube-js/cube.js/compare/v0.33.31...v0.33.32) (2023-07-07)


### Bug Fixes

* **databricks-jdbc-driver:** Return NULL as NULL instead of false for boolean ([#6791](https://github.com/cube-js/cube.js/issues/6791)) ([7eb02f5](https://github.com/cube-js/cube.js/commit/7eb02f569464d801ec71215503bc9b3679b5e856))





## [0.33.31](https://github.com/cube-js/cube.js/compare/v0.33.30...v0.33.31) (2023-07-01)


### Bug Fixes

* **databricks-jdbc-driver:** Return NULL decimal as NULL instead of 0 ([#6768](https://github.com/cube-js/cube.js/issues/6768)) ([c2ab19d](https://github.com/cube-js/cube.js/commit/c2ab19d86d6144e4f91f9e8fb681e17e87bfcef3))





## [0.33.29](https://github.com/cube-js/cube/compare/v0.33.28...v0.33.29) (2023-06-20)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.28](https://github.com/cube-js/cube/compare/v0.33.27...v0.33.28) (2023-06-19)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.27](https://github.com/cube-js/cube.js/compare/v0.33.26...v0.33.27) (2023-06-17)


### Bug Fixes

* Support unescaped `\\N` as NULL value for Snowflake driver ([#6735](https://github.com/cube-js/cube.js/issues/6735)) ([1f92ba6](https://github.com/cube-js/cube.js/commit/1f92ba6f5407f82703c8920b27a3a3e5a16fea41)), closes [#6693](https://github.com/cube-js/cube.js/issues/6693)





## [0.33.26](https://github.com/cube-js/cube/compare/v0.33.25...v0.33.26) (2023-06-14)


### Features

* **schema:** Initial support for jinja templates ([#6704](https://github.com/cube-js/cube/issues/6704)) ([338d1b7](https://github.com/cube-js/cube/commit/338d1b7ed03fc074c06fb028f731c9817ba8d419))





## [0.33.25](https://github.com/cube-js/cube/compare/v0.33.24...v0.33.25) (2023-06-07)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.24](https://github.com/cube-js/cube/compare/v0.33.23...v0.33.24) (2023-06-05)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.23](https://github.com/cube-js/cube/compare/v0.33.22...v0.33.23) (2023-06-01)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.22](https://github.com/cube-js/cube/compare/v0.33.21...v0.33.22) (2023-05-31)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.21](https://github.com/cube-js/cube/compare/v0.33.20...v0.33.21) (2023-05-31)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.20](https://github.com/cube-js/cube/compare/v0.33.19...v0.33.20) (2023-05-31)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.19](https://github.com/cube-js/cube/compare/v0.33.18...v0.33.19) (2023-05-30)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.18](https://github.com/cube-js/cube/compare/v0.33.17...v0.33.18) (2023-05-29)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.16](https://github.com/cube-js/cube.js/compare/v0.33.15...v0.33.16) (2023-05-28)


### Bug Fixes

* **mssql-driver:** Pre-aggregations builds hang up if over 10K of rows ([#6661](https://github.com/cube-js/cube.js/issues/6661)) ([9b20ff4](https://github.com/cube-js/cube.js/commit/9b20ff4ef78acbb65ebea80adceb227bf96b1727))





## [0.33.15](https://github.com/cube-js/cube.js/compare/v0.33.14...v0.33.15) (2023-05-26)


### Bug Fixes

* **athena-driver:** Internal: Error during planning: Coercion from [Utf8, Utf8] to the signature Exact([Utf8, Timestamp(Nanosecond, None)]) failed for athena pre-aggregations ([#6655](https://github.com/cube-js/cube.js/issues/6655)) ([46f7dbd](https://github.com/cube-js/cube.js/commit/46f7dbdeb0a9f55640d0f7afd7edb67ec101a43a))
* Put temp table strategy drop under lock to avoid missing table r… ([#6642](https://github.com/cube-js/cube.js/issues/6642)) ([05fcdca](https://github.com/cube-js/cube.js/commit/05fcdca9795bb7f049d57eb3cdaa38d098554bb8))





## [0.33.14](https://github.com/cube-js/cube/compare/v0.33.13...v0.33.14) (2023-05-25)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.13](https://github.com/cube-js/cube/compare/v0.33.12...v0.33.13) (2023-05-25)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.12](https://github.com/cube-js/cube.js/compare/v0.33.11...v0.33.12) (2023-05-22)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.11](https://github.com/cube-js/cube/compare/v0.33.10...v0.33.11) (2023-05-22)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.10](https://github.com/cube-js/cube.js/compare/v0.33.9...v0.33.10) (2023-05-19)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.9](https://github.com/cube-js/cube.js/compare/v0.33.8...v0.33.9) (2023-05-18)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.8](https://github.com/cube-js/cube/compare/v0.33.7...v0.33.8) (2023-05-17)


### Bug Fixes

* **athena-driver:** Fix partitioned pre-aggregations and column values with `,` through export bucket ([#6596](https://github.com/cube-js/cube/issues/6596)) ([1214cab](https://github.com/cube-js/cube/commit/1214cabf69f9e6216c516d05acadfe7e6178cccf))





## [0.33.7](https://github.com/cube-js/cube/compare/v0.33.6...v0.33.7) (2023-05-16)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.6](https://github.com/cube-js/cube.js/compare/v0.33.5...v0.33.6) (2023-05-13)


### Bug Fixes

* LIMIT is not enforced ([#6586](https://github.com/cube-js/cube.js/issues/6586)) ([8ca5234](https://github.com/cube-js/cube.js/commit/8ca52342944b9767f2c34591a9241bf31cf78c71))
* **snowflake-driver:** Bind variable ? not set for partitioned pre-aggregations ([#6594](https://github.com/cube-js/cube.js/issues/6594)) ([0819075](https://github.com/cube-js/cube.js/commit/081907568d97fa79f56edf1898b2845affb925cf))





## [0.33.5](https://github.com/cube-js/cube.js/compare/v0.33.4...v0.33.5) (2023-05-11)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.4](https://github.com/cube-js/cube.js/compare/v0.33.3...v0.33.4) (2023-05-07)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.3](https://github.com/cube-js/cube.js/compare/v0.33.2...v0.33.3) (2023-05-05)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.33.2](https://github.com/cube-js/cube/compare/v0.33.1...v0.33.2) (2023-05-04)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





# [0.33.0](https://github.com/cube-js/cube.js/compare/v0.32.31...v0.33.0) (2023-05-02)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.32.31](https://github.com/cube-js/cube.js/compare/v0.32.30...v0.32.31) (2023-05-02)

**Note:** Version bump only for package @cubejs-backend/testing-drivers





## [0.32.30](https://github.com/cube-js/cube.js/compare/v0.32.29...v0.32.30) (2023-04-28)


### Bug Fixes

* **snowflake-driver:** Int is exported to pre-aggregations as decimal… ([#6513](https://github.com/cube-js/cube.js/issues/6513)) ([3710b11](https://github.com/cube-js/cube.js/commit/3710b113160d4b0f53b40d6b31ae9c901aa51571))


### Features

* **bigquery-driver:** CI, read-only, streaming and unloading ([#6495](https://github.com/cube-js/cube.js/issues/6495)) ([4c07431](https://github.com/cube-js/cube.js/commit/4c07431033df7554ffb6f9d5f64eca156267b3e3))
* **playground:** cube type tag, public cubes ([#6482](https://github.com/cube-js/cube.js/issues/6482)) ([cede7a7](https://github.com/cube-js/cube.js/commit/cede7a71f7d2e8d9dc221669b6b1714ee146d8ea))





## [0.32.29](https://github.com/cube-js/cube/compare/v0.32.28...v0.32.29) (2023-04-25)


### Features

* **athena-driver:** read-only unload ([#6469](https://github.com/cube-js/cube/issues/6469)) ([d3fee7c](https://github.com/cube-js/cube/commit/d3fee7cbefe5c415573c4d2507b7e61a48f0c91a))
* **snowflake-driver:** streaming export, read-only unload ([#6452](https://github.com/cube-js/cube/issues/6452)) ([67565b9](https://github.com/cube-js/cube/commit/67565b975c16f93070de0346056c6a3865bc9fd8))





## [0.32.28](https://github.com/cube-js/cube/compare/v0.32.27...v0.32.28) (2023-04-19)

**Note:** Version bump only for package @cubejs-backend/testing-drivers
