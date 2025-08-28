# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [1.3.61](https://github.com/cube-js/cube/compare/v1.3.60...v1.3.61) (2025-08-28)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.60](https://github.com/cube-js/cube/compare/v1.3.59...v1.3.60) (2025-08-28)

### Bug Fixes

- **cubesql:** Fix columns referencing outer context in subqueries ([#9924](https://github.com/cube-js/cube/issues/9924)) ([09d3b95](https://github.com/cube-js/cube/commit/09d3b9521d62797294aa225fb5aca99a0aad0567))

## [1.3.59](https://github.com/cube-js/cube/compare/v1.3.58...v1.3.59) (2025-08-26)

### Bug Fixes

- **cubesql:** Merge subqueries with SQL push down ([#9916](https://github.com/cube-js/cube/issues/9916)) ([9a5597b](https://github.com/cube-js/cube/commit/9a5597bfb555bc8f81f851b1b6866d159cf4a304))

### Features

- **cubesql:** Report rewrite start/success events ([#9917](https://github.com/cube-js/cube/issues/9917)) ([7cc80b2](https://github.com/cube-js/cube/commit/7cc80b2e2c43ec401e95482fb9d8969ed53f458c))

## [1.3.58](https://github.com/cube-js/cube/compare/v1.3.57...v1.3.58) (2025-08-25)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.57](https://github.com/cube-js/cube/compare/v1.3.56...v1.3.57) (2025-08-22)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.56](https://github.com/cube-js/cube/compare/v1.3.55...v1.3.56) (2025-08-21)

### Features

- **cubesql:** Avoid `COUNT(*)` pushdown to joined cubes ([#9905](https://github.com/cube-js/cube/issues/9905)) ([e073a72](https://github.com/cube-js/cube/commit/e073a7217e25c3bc4b3c63145d4a22973a587491))

## [1.3.55](https://github.com/cube-js/cube/compare/v1.3.54...v1.3.55) (2025-08-19)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.54](https://github.com/cube-js/cube/compare/v1.3.53...v1.3.54) (2025-08-15)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.53](https://github.com/cube-js/cube/compare/v1.3.52...v1.3.53) (2025-08-15)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.52](https://github.com/cube-js/cube/compare/v1.3.51...v1.3.52) (2025-08-14)

### Features

- **cubesql:** Support cursors in stream mode ([#9877](https://github.com/cube-js/cube/issues/9877)) ([8ddaba5](https://github.com/cube-js/cube/commit/8ddaba5553addd2a5352fe2a54e50c236252d4d3))

## [1.3.51](https://github.com/cube-js/cube/compare/v1.3.50...v1.3.51) (2025-08-14)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.50](https://github.com/cube-js/cube/compare/v1.3.49...v1.3.50) (2025-08-13)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.49](https://github.com/cube-js/cube/compare/v1.3.48...v1.3.49) (2025-08-12)

### Bug Fixes

- **cubesql:** Improve SQL push down for Athena/Presto ([#9873](https://github.com/cube-js/cube/issues/9873)) ([893e9b3](https://github.com/cube-js/cube/commit/893e9b3e0dd200a26c8b97bb7a39532707edf632))

## [1.3.48](https://github.com/cube-js/cube/compare/v1.3.47...v1.3.48) (2025-08-09)

### Bug Fixes

- **cubesql:** Allow repeated aliases (auto-realias) ([#9863](https://github.com/cube-js/cube/issues/9863)) ([0fb183a](https://github.com/cube-js/cube/commit/0fb183a8c63f180f92913d84fa6890aec4339fcc))
- **cubesql:** Improve Trino SQL push down compatibility ([#9861](https://github.com/cube-js/cube/issues/9861)) ([9d5794e](https://github.com/cube-js/cube/commit/9d5794e9eb3610d722adc0a8ec5092140efdf0f1))
- **cubesql:** Support concatenating non-strings in SQL push down for Athena/Presto ([#9853](https://github.com/cube-js/cube/issues/9853)) ([97e54e0](https://github.com/cube-js/cube/commit/97e54e01ad78a9483736d92339769332c9934e68))

### Features

- **cubesql:** Support date type for parameter binding ([#9864](https://github.com/cube-js/cube/issues/9864)) ([5246fa0](https://github.com/cube-js/cube/commit/5246fa0836a11f38a9cc86357674c203414085dd))

## [1.3.47](https://github.com/cube-js/cube/compare/v1.3.46...v1.3.47) (2025-08-04)

### Features

- **cubesql:** Allow to bind float64 (support in pg-srv) ([#9846](https://github.com/cube-js/cube/issues/9846)) ([760640c](https://github.com/cube-js/cube/commit/760640cadfa8e6a6728608de2cc7d71431fe93ce))
- **cubesql:** Support `BETWEEN` SQL push down ([#9834](https://github.com/cube-js/cube/issues/9834)) ([195402f](https://github.com/cube-js/cube/commit/195402f76f893a0649488a373fb5b46b7f0a04b3))
- **cubesql:** Support timestamp parameter binding, fix [#9784](https://github.com/cube-js/cube/issues/9784) ([#9847](https://github.com/cube-js/cube/issues/9847)) ([8a614bb](https://github.com/cube-js/cube/commit/8a614bb715f4e7baf597a03a1134702ea6e3c286))

## [1.3.46](https://github.com/cube-js/cube/compare/v1.3.45...v1.3.46) (2025-07-31)

### Features

- **cubesql:** Add support for `current_catalog` function for postgresql protocol ([#9839](https://github.com/cube-js/cube/issues/9839)) ([95dc46e](https://github.com/cube-js/cube/commit/95dc46e08735ae0319048ef4bcd52cb4226d26f5))

## [1.3.45](https://github.com/cube-js/cube/compare/v1.3.44...v1.3.45) (2025-07-29)

### Features

- **cubesql:** Improve DataGrip compatibility ([#9825](https://github.com/cube-js/cube/issues/9825)) ([18b09ee](https://github.com/cube-js/cube/commit/18b09ee5408bd82d28eb06f6f23e7cab29a2ce54))

## [1.3.44](https://github.com/cube-js/cube/compare/v1.3.43...v1.3.44) (2025-07-28)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.43](https://github.com/cube-js/cube/compare/v1.3.42...v1.3.43) (2025-07-24)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.42](https://github.com/cube-js/cube/compare/v1.3.41...v1.3.42) (2025-07-23)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.41](https://github.com/cube-js/cube/compare/v1.3.40...v1.3.41) (2025-07-22)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.40](https://github.com/cube-js/cube/compare/v1.3.39...v1.3.40) (2025-07-20)

### Bug Fixes

- **cubesql:** Add missing pub CubeMetaNestedFolder in cube client ([#9790](https://github.com/cube-js/cube/issues/9790)) ([d78cd6b](https://github.com/cube-js/cube/commit/d78cd6b8a7ac2439ed57ce9f3390bdb6d3fc69e4))

## [1.3.39](https://github.com/cube-js/cube/compare/v1.3.38...v1.3.39) (2025-07-17)

### Bug Fixes

- **cubesql:** Fix sort push down projection on complex expressions ([#9787](https://github.com/cube-js/cube/issues/9787)) ([cd1c983](https://github.com/cube-js/cube/commit/cd1c9832386e69ab154f8d01f2bf67c63ff5c685))

## [1.3.38](https://github.com/cube-js/cube/compare/v1.3.37...v1.3.38) (2025-07-16)

### Features

- **cubesql:** Push Limit-Sort down Projection ([#9776](https://github.com/cube-js/cube/issues/9776)) ([72e6059](https://github.com/cube-js/cube/commit/72e605966100bb24d44b715d96cfb2cc4d8d793d))
- **schema-compiler,api-gateway:** Nested folders support ([#9659](https://github.com/cube-js/cube/issues/9659)) ([720f048](https://github.com/cube-js/cube/commit/720f0485c8b11f16eb99490259a881c21b845c73))

## [1.3.37](https://github.com/cube-js/cube/compare/v1.3.36...v1.3.37) (2025-07-14)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.36](https://github.com/cube-js/cube/compare/v1.3.35...v1.3.36) (2025-07-10)

### Bug Fixes

- **cubesql:** Fix adding missing columns for `ORDER BY` clause ([#9764](https://github.com/cube-js/cube/issues/9764)) ([185db54](https://github.com/cube-js/cube/commit/185db547e0c83c0e276c9a89618b4753c969dea7))
- **cubesql:** Improve DBeaver compatibility ([#9769](https://github.com/cube-js/cube/issues/9769)) ([c206c90](https://github.com/cube-js/cube/commit/c206c901e9da14a230a2b358dfc4ea577adf9f49))

## [1.3.35](https://github.com/cube-js/cube/compare/v1.3.34...v1.3.35) (2025-07-09)

### Bug Fixes

- **cubesql:** Hide security context from logs ([#9761](https://github.com/cube-js/cube/issues/9761)) ([e38c03c](https://github.com/cube-js/cube/commit/e38c03c2ba95f86965909b0f9161babd7bb52ecf))
- **cubesql:** Normalize `EXTRACT`/`DATE_TRUNC` granularities ([#9759](https://github.com/cube-js/cube/issues/9759)) ([2db54ba](https://github.com/cube-js/cube/commit/2db54ba4a70c4e96a1ffa196fe43270385a48b3c))

## [1.3.34](https://github.com/cube-js/cube/compare/v1.3.33...v1.3.34) (2025-07-04)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.33](https://github.com/cube-js/cube/compare/v1.3.32...v1.3.33) (2025-07-03)

### Features

- **cubesql:** Filter push down for date_part('year', ?col) = ?literal ([#9749](https://github.com/cube-js/cube/issues/9749)) ([f952cf7](https://github.com/cube-js/cube/commit/f952cf7e895ca5623d356bed6d104fd26b081ec9))

## [1.3.32](https://github.com/cube-js/cube/compare/v1.3.31...v1.3.32) (2025-07-03)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.31](https://github.com/cube-js/cube/compare/v1.3.30...v1.3.31) (2025-07-02)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.30](https://github.com/cube-js/cube/compare/v1.3.29...v1.3.30) (2025-07-01)

### Features

- **cubesql:** Support `AGE` function ([#9734](https://github.com/cube-js/cube/issues/9734)) ([5b2682c](https://github.com/cube-js/cube/commit/5b2682c3569933e94f56f6d998065b9063525d29))
- **cubesql:** Support `DATE_PART` with intervals ([#9740](https://github.com/cube-js/cube/issues/9740)) ([65d084d](https://github.com/cube-js/cube/commit/65d084ddd81f6cfefe836b224fb9dd7575a62756))
- **cubesql:** Support decimal math with scalar ([#9742](https://github.com/cube-js/cube/issues/9742)) ([2629d36](https://github.com/cube-js/cube/commit/2629d36572944b0b1f6194970c4a4e6132fd5a8a))

## [1.3.29](https://github.com/cube-js/cube/compare/v1.3.28...v1.3.29) (2025-07-01)

### Bug Fixes

- **cubesql:** Fix incorrect datetime parsing in filters rewrite rules ([#9732](https://github.com/cube-js/cube/issues/9732)) ([6e73860](https://github.com/cube-js/cube/commit/6e73860aa92aa9b2733a771ded59b2febf9853dd))

## [1.3.28](https://github.com/cube-js/cube/compare/v1.3.27...v1.3.28) (2025-06-30)

### Bug Fixes

- **cubesql:** Fix cube rust client schema for custom granularities with sql ([#9727](https://github.com/cube-js/cube/issues/9727)) ([2711fa6](https://github.com/cube-js/cube/commit/2711fa6a37322a645e995f17f269d9291345c78a))

## [1.3.27](https://github.com/cube-js/cube/compare/v1.3.26...v1.3.27) (2025-06-30)

### Bug Fixes

- **cubejs-schema-compiler:** Stay unchanged `__user` / `__cubejoinfield` names in aliasing ([#8303](https://github.com/cube-js/cube/issues/8303)) ([7bb4bdc](https://github.com/cube-js/cube/commit/7bb4bdc3f6b2d67a6f8263730f84fc3289b08347))
- **cubesql:** Fix incorrect underscore truncation for aliases ([#9716](https://github.com/cube-js/cube/issues/9716)) ([c16175b](https://github.com/cube-js/cube/commit/c16175bf964fbb351bede1bfe0fd13adf793e51a))

## [1.3.26](https://github.com/cube-js/cube/compare/v1.3.25...v1.3.26) (2025-06-25)

### Bug Fixes

- **cubesql:** Push down `__user` meta filter further ([#9711](https://github.com/cube-js/cube/issues/9711)) ([5dd626a](https://github.com/cube-js/cube/commit/5dd626a2471a8282dd51a9d6d03654dcf44e2f80))

## [1.3.25](https://github.com/cube-js/cube/compare/v1.3.24...v1.3.25) (2025-06-24)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.24](https://github.com/cube-js/cube/compare/v1.3.23...v1.3.24) (2025-06-24)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.23](https://github.com/cube-js/cube/compare/v1.3.22...v1.3.23) (2025-06-19)

### Bug Fixes

- **cubesql:** Split meta on `CAST` over `__user` column ([#9690](https://github.com/cube-js/cube/issues/9690)) ([1685c1b](https://github.com/cube-js/cube/commit/1685c1b6dc6331855a9cd6e41ee4c7de8e185a8e))

## [1.3.22](https://github.com/cube-js/cube/compare/v1.3.21...v1.3.22) (2025-06-18)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.21](https://github.com/cube-js/cube/compare/v1.3.20...v1.3.21) (2025-06-10)

### Bug Fixes

- Report more accurate time to APM on heavy used deployments ([#9667](https://github.com/cube-js/cube/issues/9667)) ([a900c78](https://github.com/cube-js/cube/commit/a900c787d3724ebdd241cb0e4f4562e37f81ce14))

## [1.3.20](https://github.com/cube-js/cube/compare/v1.3.19...v1.3.20) (2025-06-06)

### Bug Fixes

- **cubesql:** Fix Tableau relative dates ([#9641](https://github.com/cube-js/cube/issues/9641)) ([18ec4fc](https://github.com/cube-js/cube/commit/18ec4fc4fcd9ea94799241dc3f8ce9c7ac531b4a))

### Features

- **cubesql:** Support `PERCENTILE_CONT` SQL push down ([#8697](https://github.com/cube-js/cube/issues/8697)) ([577a09f](https://github.com/cube-js/cube/commit/577a09f498085ca5a7950467e602dee54691e88e))

## [1.3.19](https://github.com/cube-js/cube/compare/v1.3.18...v1.3.19) (2025-06-02)

### Bug Fixes

- **cubesql:** Fix "Tracker memory shrink underflow" error ([#9624](https://github.com/cube-js/cube/issues/9624)) ([d3af150](https://github.com/cube-js/cube/commit/d3af1506d845276a5b7fd97c5d8543d2cf03a1e0))
- **cubesql:** Quote subquery joins alias in SQL push down to cube ([#9629](https://github.com/cube-js/cube/issues/9629)) ([89b00cf](https://github.com/cube-js/cube/commit/89b00cf76dfbbfd06f0412d6e80178f0fdb9f46c))

### Features

- **cubesql:** Support `date_trunc != literal date` filter ([#9627](https://github.com/cube-js/cube/issues/9627)) ([2b36aae](https://github.com/cube-js/cube/commit/2b36aae5e93f88f4cca6059067bee047c32f4d24))
- **cubesql:** Support round() function with two parameters ([#9594](https://github.com/cube-js/cube/issues/9594)) ([8cd1dfe](https://github.com/cube-js/cube/commit/8cd1dfec1b18b246ed8f24f4d7c33a91556a4afa))
- Expose aliasMember for hierarchy in View ([#9636](https://github.com/cube-js/cube/issues/9636)) ([737caab](https://github.com/cube-js/cube/commit/737caabf2a43bc28ea0ad90085f44ffbaa1b292b))

## [1.3.18](https://github.com/cube-js/cube/compare/v1.3.17...v1.3.18) (2025-05-27)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.17](https://github.com/cube-js/cube/compare/v1.3.16...v1.3.17) (2025-05-22)

### Bug Fixes

- **cubesql:** Do not merge time dimension ranges in "or" filter into date range ([#9609](https://github.com/cube-js/cube/issues/9609)) ([803998f](https://github.com/cube-js/cube/commit/803998fc8e1799719542d0611c82032473409e01))

## [1.3.16](https://github.com/cube-js/cube/compare/v1.3.15...v1.3.16) (2025-05-19)

### Features

- **cubesql:** Push down `DATE_TRUNC` expressions as member expressions with granularity ([#9583](https://github.com/cube-js/cube/issues/9583)) ([b9c97cd](https://github.com/cube-js/cube/commit/b9c97cd6c169b6d359575649bd845f735ff1a516))

## [1.3.15](https://github.com/cube-js/cube/compare/v1.3.14...v1.3.15) (2025-05-15)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.14](https://github.com/cube-js/cube/compare/v1.3.13...v1.3.14) (2025-05-13)

### Features

- Rewrite joins from SQL as query-level join hints ([#9561](https://github.com/cube-js/cube/issues/9561)) ([2b2ac1c](https://github.com/cube-js/cube/commit/2b2ac1c47898f4f6bf67ebae658f90b768c63a7a))

## [1.3.13](https://github.com/cube-js/cube/compare/v1.3.12...v1.3.13) (2025-05-12)

### Features

- introduce "protocol" and "method" props for request param in checkSqlAuth ([#9525](https://github.com/cube-js/cube/issues/9525)) ([401a845](https://github.com/cube-js/cube/commit/401a84584f418e2b4bdabe13766ac213646e0924))

## [1.3.12](https://github.com/cube-js/cube/compare/v1.3.11...v1.3.12) (2025-05-08)

### Features

- **cubestore:** Add `XIRR` aggregate function to Cube Store ([#9520](https://github.com/cube-js/cube/issues/9520)) ([785142d](https://github.com/cube-js/cube/commit/785142d1c8ecc89cadaa7696c9f58b34115d929b))

## [1.3.11](https://github.com/cube-js/cube/compare/v1.3.10...v1.3.11) (2025-05-05)

### Features

- **cubesql:** Data source per member ([#9537](https://github.com/cube-js/cube/issues/9537)) ([c0be00c](https://github.com/cube-js/cube/commit/c0be00cd4e5239b52116e38e0f5bf8d846e57090))

## [1.3.10](https://github.com/cube-js/cube/compare/v1.3.9...v1.3.10) (2025-05-01)

### Bug Fixes

- **cubesql:** Disable filter pushdown over Filter(CrossJoin) ([#9474](https://github.com/cube-js/cube/issues/9474)) ([940c30f](https://github.com/cube-js/cube/commit/940c30f81c0d0f73bcc58bc80d3b673d484cc067))

### Features

- **cubesql:** SQL push down complex window expressions ([#8788](https://github.com/cube-js/cube/issues/8788)) ([2b1bb28](https://github.com/cube-js/cube/commit/2b1bb284e1413a13f96df62fe712c61aee32fd68))
- **cubesql:** Support trivial casts in member pushdown ([#9480](https://github.com/cube-js/cube/issues/9480)) ([85c27a9](https://github.com/cube-js/cube/commit/85c27a928a773245163406d0262c7a5bc69c69bb))

## [1.3.9](https://github.com/cube-js/cube/compare/v1.3.8...v1.3.9) (2025-04-28)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.8](https://github.com/cube-js/cube/compare/v1.3.7...v1.3.8) (2025-04-24)

### Features

- **cubesql:** Add `XIRR` aggregate function ([#9508](https://github.com/cube-js/cube/issues/9508)) ([c7fb71b](https://github.com/cube-js/cube/commit/c7fb71bd4023b37635bfb82c2aac523337c2f8be))

## [1.3.7](https://github.com/cube-js/cube/compare/v1.3.6...v1.3.7) (2025-04-23)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.6](https://github.com/cube-js/cube/compare/v1.3.5...v1.3.6) (2025-04-22)

### Bug Fixes

- **cubesql:** Fix SortPushDown pushing sort through joins ([#9464](https://github.com/cube-js/cube/issues/9464)) ([fed08e1](https://github.com/cube-js/cube/commit/fed08e1384cf6f666970472f5bd67feb5c86462e)), closes [/github.com/apache/datafusion/blob/7.0.0/datafusion/src/physical_plan/hash_join.rs#L282-L284](https://github.com//github.com/apache/datafusion/blob/7.0.0/datafusion/src/physical_plan/hash_join.rs/issues/L282-L284) [/github.com/apache/datafusion/blob/7.0.0/datafusion/src/physical_plan/cross_join.rs#L141-L143](https://github.com//github.com/apache/datafusion/blob/7.0.0/datafusion/src/physical_plan/cross_join.rs/issues/L141-L143)
- **cubesql:** Realias expressions when normalizing columns ([#9498](https://github.com/cube-js/cube/issues/9498)) ([32f9b79](https://github.com/cube-js/cube/commit/32f9b7993c97fc294ec92b54a587f27e4e29dda0))

## [1.3.5](/compare/v1.3.4...v1.3.5) (2025-04-17)

### Bug Fixes

- **cubesql:** Disallow mixing measure and dimension filters in a single FilterOp (#9486) 858f6d5, closes #9486

## [1.3.4](https://github.com/cube-js/cube/compare/v1.3.3...v1.3.4) (2025-04-17)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.3](https://github.com/cube-js/cube/compare/v1.3.2...v1.3.3) (2025-04-16)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.2](https://github.com/cube-js/cube/compare/v1.3.1...v1.3.2) (2025-04-16)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.3.1](https://github.com/cube-js/cube/compare/v1.3.0...v1.3.1) (2025-04-14)

**Note:** Version bump only for package @cubejs-backend/cubesql

# [1.3.0](https://github.com/cube-js/cube/compare/v1.2.33...v1.3.0) (2025-04-11)

### Features

- **cubesql:** Remove bottom-up extraction completely ([#9183](https://github.com/cube-js/cube/issues/9183)) ([c528ebe](https://github.com/cube-js/cube/commit/c528ebe6b5700cc950a26530906436bd274c1942))

## [1.2.33](https://github.com/cube-js/cube/compare/v1.2.32...v1.2.33) (2025-04-10)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.32](/compare/v1.2.31...v1.2.32) (2025-04-08)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.31](https://github.com/cube-js/cube/compare/v1.2.30...v1.2.31) (2025-04-08)

### Bug Fixes

- **schema-compiler:** Fix BigQuery DATE_ADD push down template for years/quarters/months ([#9432](https://github.com/cube-js/cube/issues/9432)) ([5845c88](https://github.com/cube-js/cube/commit/5845c88dc2d7e482d2a79eb3329ecc302655a493))

### Features

- PatchMeasure member expression ([#9218](https://github.com/cube-js/cube/issues/9218)) ([128280a](https://github.com/cube-js/cube/commit/128280ae02d053b8435388ff2a808a27b773cef1))

## [1.2.30](/compare/v1.2.29...v1.2.30) (2025-04-04)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.29](https://github.com/cube-js/cube/compare/v1.2.28...v1.2.29) (2025-04-02)

### Bug Fixes

- **cubesql:** Penalize CrossJoins in favor of wrapper ([#9414](https://github.com/cube-js/cube/issues/9414)) ([a48963d](https://github.com/cube-js/cube/commit/a48963d03cb16e0dc3d110ae398fe3b05447209d))

## [1.2.28](https://github.com/cube-js/cube/compare/v1.2.27...v1.2.28) (2025-04-01)

### Bug Fixes

- **cubesql:** Allow more filters in CubeScan before aggregation pushdown ([#9409](https://github.com/cube-js/cube/issues/9409)) ([351ac7a](https://github.com/cube-js/cube/commit/351ac7aece72e7795f570f5582250206e3c0124e))
- **schema-compiler:** Fix BigQuery DATETIME_TRUNC() week processing ([#9380](https://github.com/cube-js/cube/issues/9380)) ([6c8564f](https://github.com/cube-js/cube/commit/6c8564ffc15e5e930fa2160be642ea3f3cb7b888))

## [1.2.27](https://github.com/cube-js/cube/compare/v1.2.26...v1.2.27) (2025-03-25)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.26](https://github.com/cube-js/cube/compare/v1.2.25...v1.2.26) (2025-03-21)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.25](https://github.com/cube-js/cube/compare/v1.2.24...v1.2.25) (2025-03-20)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.24](/compare/v1.2.23...v1.2.24) (2025-03-18)

### Bug Fixes

- **cubesql:** Disable projection_push_down DF optimizer (#9356) a15442c, closes #9356

### Features

- Implement disable_post_processing in /v1/sql (#9331) c336b10, closes #9331

## [1.2.23](https://github.com/cube-js/cube/compare/v1.2.22...v1.2.23) (2025-03-17)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.22](https://github.com/cube-js/cube/compare/v1.2.21...v1.2.22) (2025-03-14)

### Bug Fixes

- **cubesql:** Functions without arguments alias as plain function name ([#9338](https://github.com/cube-js/cube/issues/9338)) ([de10c23](https://github.com/cube-js/cube/commit/de10c233bf84ef11eb0af272ea296881651dafd1))

## [1.2.21](https://github.com/cube-js/cube/compare/v1.2.20...v1.2.21) (2025-03-11)

### Bug Fixes

- **cubejs-native:** cubesql query logger span_id ([#9325](https://github.com/cube-js/cube/issues/9325)) ([568d306](https://github.com/cube-js/cube/commit/568d306a9b97672caf69543077e7863fc773af41))

### Features

- **cubesql:** Move dimensions-only projections to dimensions for push-to-Cube wrapper ([#9318](https://github.com/cube-js/cube/issues/9318)) ([ca62aa0](https://github.com/cube-js/cube/commit/ca62aa0e747b88c2754f3a758c7a959ee52b0c81))

## [1.2.20](/compare/v1.2.19...v1.2.20) (2025-03-10)

### Features

- Add SQL queries support in /v1/sql endpoint (#9301) 7eba663, closes #9301

## [1.2.19](https://github.com/cube-js/cube/compare/v1.2.18...v1.2.19) (2025-03-08)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.17](/compare/v1.2.16...v1.2.17) (2025-03-05)

### Bug Fixes

- **cubesql:** Use pushdown-pullup scheme for FilterSimplifyReplacer (#9278) ab5a64e, closes #9278

## [1.2.16](https://github.com/cube-js/cube/compare/v1.2.15...v1.2.16) (2025-03-04)

### Bug Fixes

- **cubejs-native:** sql over http drop sessions, correct error ([#9297](https://github.com/cube-js/cube/issues/9297)) ([6fad670](https://github.com/cube-js/cube/commit/6fad670c722fd91e29d950a36659ac4630cef64a))

## [1.2.15](https://github.com/cube-js/cube/compare/v1.2.14...v1.2.15) (2025-03-03)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.14](https://github.com/cube-js/cube/compare/v1.2.13...v1.2.14) (2025-02-28)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.13](https://github.com/cube-js/cube/compare/v1.2.12...v1.2.13) (2025-02-26)

### Bug Fixes

- **cubesql:** Split `__user` WHERE predicate into separate filter node ([#8812](https://github.com/cube-js/cube/issues/8812)) ([83baf7b](https://github.com/cube-js/cube/commit/83baf7bf5f83108fd6c3dd134a8739968e781f92))

## [1.2.12](https://github.com/cube-js/cube/compare/v1.2.11...v1.2.12) (2025-02-26)

### Bug Fixes

- **cubesql:** Generate typed null literals ([#9238](https://github.com/cube-js/cube/issues/9238)) ([1dfa10d](https://github.com/cube-js/cube/commit/1dfa10d7128841f24c5d94cd1c5bdd2c742ff9de))
- **cubesql:** Match CubeScan timestamp literal types to member types ([#9275](https://github.com/cube-js/cube/issues/9275)) ([4a4e82b](https://github.com/cube-js/cube/commit/4a4e82ba602fc024a262a22ac65e3fcb7a4bba5c))

## [1.2.11](https://github.com/cube-js/cube/compare/v1.2.10...v1.2.11) (2025-02-25)

### Bug Fixes

- **cubesql:** Break cost symmetry for (non)-push-to-Cube WrappedSelect ([#9155](https://github.com/cube-js/cube/issues/9155)) ([2c0e443](https://github.com/cube-js/cube/commit/2c0e443dc18379490e35a3d83b3888f66e12ade0))
- **cubesql:** Generate proper projection wrapper for duplicated members in CubeScanNode ([#9233](https://github.com/cube-js/cube/issues/9233)) ([aba6430](https://github.com/cube-js/cube/commit/aba643082acc440cf5b3fe9828c2c38ac1a833c9))

## [1.2.10](https://github.com/cube-js/cube/compare/v1.2.9...v1.2.10) (2025-02-24)

### Bug Fixes

- **schema-compiler:** Fix sql generation for rolling_window queries with multiple time dimensions ([#9124](https://github.com/cube-js/cube/issues/9124)) ([52a664e](https://github.com/cube-js/cube/commit/52a664e4d0643d78464f75cc48c4a1f686455ebe))

## [1.2.9](https://github.com/cube-js/cube/compare/v1.2.8...v1.2.9) (2025-02-21)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.8](https://github.com/cube-js/cube/compare/v1.2.7...v1.2.8) (2025-02-21)

### Features

- **cubeclient:** Add `short_title` to dimensions and measures ([#9256](https://github.com/cube-js/cube/issues/9256)) ([584b3dc](https://github.com/cube-js/cube/commit/584b3dcefedb7c01b849e7f18a59445bd3542b7e))

## [1.2.7](https://github.com/cube-js/cube/compare/v1.2.6...v1.2.7) (2025-02-20)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.6](https://github.com/cube-js/cube/compare/v1.2.5...v1.2.6) (2025-02-18)

### Bug Fixes

- **schema-compiler:** Correct models transpilation in native in multitenant environments ([#9234](https://github.com/cube-js/cube/issues/9234)) ([84f90c0](https://github.com/cube-js/cube/commit/84f90c07ee3827e6f3652dd6c9fab0993ecc8150))

### Performance Improvements

- **cubesql:** Avoid allocations in MetaContext methods ([#9228](https://github.com/cube-js/cube/issues/9228)) ([ba753d0](https://github.com/cube-js/cube/commit/ba753d0d43927b50d5cf8faf5f09de3e53bec3db))

## [1.2.4](/compare/v1.2.3...v1.2.4) (2025-02-11)

### Features

- **cubesql:** Add projection flattening rule (#9165) 8cfb253, closes #9165
- **cubesql:** Allow providing API type when getting load request meta (#9202) ae5d977, closes #9202

## [1.2.3](https://github.com/cube-js/cube/compare/v1.2.2...v1.2.3) (2025-02-06)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.2.2](https://github.com/cube-js/cube/compare/v1.2.1...v1.2.2) (2025-02-06)

**Note:** Version bump only for package @cubejs-backend/cubesql

# [1.2.0](https://github.com/cube-js/cube/compare/v1.1.18...v1.2.0) (2025-02-05)

### Bug Fixes

- **cubesql:** Avoid panics during filter rewrites ([#9166](https://github.com/cube-js/cube/issues/9166)) ([4c8de88](https://github.com/cube-js/cube/commit/4c8de882b3cc57e2b0c29c33ca4ee91377176e85))
- **databricks-jdbc-driver:** Fix extract epoch from timestamp SQL Generation ([#9160](https://github.com/cube-js/cube/issues/9160)) ([9a73857](https://github.com/cube-js/cube/commit/9a73857b4abc5691b44b8a395176a565cdbf1b2a))

### Features

- **cubeclient:** Add hierarchies to Cube meta ([#9180](https://github.com/cube-js/cube/issues/9180)) ([56dbf9e](https://github.com/cube-js/cube/commit/56dbf9edc8c257cd81f00ff119b12543652b76d0))
- **cubesql:** Add filter flattening rule ([#9148](https://github.com/cube-js/cube/issues/9148)) ([92a4b8e](https://github.com/cube-js/cube/commit/92a4b8e0e65c05f2ca0a683387d3f4434c358fc6))
- **cubesql:** Add separate ungrouped_scan flag to wrapper replacer context ([#9120](https://github.com/cube-js/cube/issues/9120)) ([50bdbe7](https://github.com/cube-js/cube/commit/50bdbe7f52f653680e32d26c96c41f10e459c341))
- Support complex join conditions for grouped joins ([#9157](https://github.com/cube-js/cube/issues/9157)) ([28c1e3b](https://github.com/cube-js/cube/commit/28c1e3bba7a100f3152bfdefd86197e818fac941))

## [1.1.17](https://github.com/cube-js/cube/compare/v1.1.16...v1.1.17) (2025-01-27)

### Features

- **api-gateway:** Async native query results transformations ([#8961](https://github.com/cube-js/cube/issues/8961)) ([3822107](https://github.com/cube-js/cube/commit/382210716fc3c9ed459c5b45a8a52e766ff7d7cf))
- **cubesql:** Support %s in format ([#9129](https://github.com/cube-js/cube/issues/9129)) ([be140ec](https://github.com/cube-js/cube/commit/be140ecdd0425f6ea3420520649f68ff81ae46f1)), closes [#9126](https://github.com/cube-js/cube/issues/9126)

## [1.1.16](https://github.com/cube-js/cube/compare/v1.1.15...v1.1.16) (2025-01-22)

### Bug Fixes

- **cubesql:** Add forgotten Distinct in ast_size_outside_wrapper cost component ([#8882](https://github.com/cube-js/cube/issues/8882)) ([a64272e](https://github.com/cube-js/cube/commit/a64272e376d67431c7f1c057b56b04bf10c59967))
- **cubesql:** Fix condition for joining two date range filters ([#9113](https://github.com/cube-js/cube/issues/9113)) ([39190e0](https://github.com/cube-js/cube/commit/39190e075671bf1adcf8334c513b70130c67cf64))
- **cubesql:** Pass proper in_projection flag in non-trivial wrapper pull up rule ([#9097](https://github.com/cube-js/cube/issues/9097)) ([8f0758e](https://github.com/cube-js/cube/commit/8f0758e2b29a502a0048d6264ae30cf02d7340d7))
- typo for DBeaver in comments change DBEver to DBeaver ([#9092](https://github.com/cube-js/cube/issues/9092)) ([aab9e8f](https://github.com/cube-js/cube/commit/aab9e8f844244247d21fac427ae36ee25aa24ae1))

### Features

- **cubesql:** Implement format and col_description ([#9072](https://github.com/cube-js/cube/issues/9072)) ([bde6eea](https://github.com/cube-js/cube/commit/bde6eea73f35a768f532c5a7dfd20c0533720238)), closes [#8947](https://github.com/cube-js/cube/issues/8947) [#8926](https://github.com/cube-js/cube/issues/8926)
- Initial support for grouped join pushdown ([#9032](https://github.com/cube-js/cube/issues/9032)) ([2f11d20](https://github.com/cube-js/cube/commit/2f11d2050ab1e2fc7f0a37012d5d45592f01938e))

### Performance Improvements

- **cubesql:** Improve rules loading perf ([#9014](https://github.com/cube-js/cube/issues/9014)) ([4cef4f0](https://github.com/cube-js/cube/commit/4cef4f00337bd7d5c8921301ca1c18bc2e1a437d))

## [1.1.15](https://github.com/cube-js/cube/compare/v1.1.14...v1.1.15) (2025-01-13)

### Bug Fixes

- **cubesql:** Add folders to Cube Metadata ([#9089](https://github.com/cube-js/cube/issues/9089)) ([8d714d6](https://github.com/cube-js/cube/commit/8d714d6637862b36024aee5c3857267c0c167dbf))

## [1.1.14](https://github.com/cube-js/cube/compare/v1.1.13...v1.1.14) (2025-01-09)

### Bug Fixes

- **cubesql:** add title field to Dimension Metadata ([#9084](https://github.com/cube-js/cube/issues/9084)) ([9653a23](https://github.com/cube-js/cube/commit/9653a23d9f477b391627755a533b2d6e8eae5656))

## [1.1.12](https://github.com/cube-js/cube/compare/v1.1.11...v1.1.12) (2025-01-09)

### Features

- **cubesql:** Penalize zero members in wrapper ([#8927](https://github.com/cube-js/cube/issues/8927)) ([171ea35](https://github.com/cube-js/cube/commit/171ea351e739f705ddbf0d803a34b944cb8c9da5))

## [1.1.10](https://github.com/cube-js/cube/compare/v1.1.9...v1.1.10) (2024-12-16)

### Features

- **cubesql:** Basic VALUES support in rewrite engine ([#9041](https://github.com/cube-js/cube/issues/9041)) ([368671f](https://github.com/cube-js/cube/commit/368671fd1b53b2ed5ad8df6af113492982f23c0c))

## [1.1.9](https://github.com/cube-js/cube/compare/v1.1.8...v1.1.9) (2024-12-08)

### Bug Fixes

- **cubesql:** Allow aggregation pushdown only for unlimited CubeScan ([#8929](https://github.com/cube-js/cube/issues/8929)) ([5b10a68](https://github.com/cube-js/cube/commit/5b10a68b4aca8e5050291fa3ca85dd5f3edc6614))

## [1.1.8](https://github.com/cube-js/cube/compare/v1.1.7...v1.1.8) (2024-12-05)

### Bug Fixes

- **cubesql:** fix unhandled timestamp unwrapping in df/transform_response ([#8952](https://github.com/cube-js/cube/issues/8952)) ([4ea0740](https://github.com/cube-js/cube/commit/4ea0740a4001767ab1863c21c062a7e1487fc4e6))

## [1.1.7](https://github.com/cube-js/cube/compare/v1.1.6...v1.1.7) (2024-11-20)

### Bug Fixes

- **cubesql:** Support explicit UTC as timezone in pushdown SQL generation ([#8971](https://github.com/cube-js/cube/issues/8971)) ([85eaa29](https://github.com/cube-js/cube/commit/85eaa29a3e8df520fbdc8b2df0ece4a131c39cdc)), closes [/github.com/cube-js/arrow-datafusion/blob/dcf3e4aa26fd112043ef26fa4a78db5dbd443c86/datafusion/physical-expr/src/datetime_expressions.rs#L357-L367](https://github.com//github.com/cube-js/arrow-datafusion/blob/dcf3e4aa26fd112043ef26fa4a78db5dbd443c86/datafusion/physical-expr/src/datetime_expressions.rs/issues/L357-L367)

## [1.1.5](https://github.com/cube-js/cube/compare/v1.1.4...v1.1.5) (2024-11-13)

### Features

- **cubesql:** Initial SQL pushdown flattening ([#8888](https://github.com/cube-js/cube/issues/8888)) ([211d1c1](https://github.com/cube-js/cube/commit/211d1c1053dba7ed69c82f489a433602a66d78e7))

## [1.1.4](https://github.com/cube-js/cube/compare/v1.1.3...v1.1.4) (2024-11-12)

### Bug Fixes

- **cubesql:** Add checks that projection/filters/fetch in TableScan is empty ([#8883](https://github.com/cube-js/cube/issues/8883)) ([a7bab04](https://github.com/cube-js/cube/commit/a7bab04c2b533d95a9ac2f65fb851b9efa9afde1))
- **cubesql:** Pass null_equals_null through egraph ([#8776](https://github.com/cube-js/cube/issues/8776)) ([e02f612](https://github.com/cube-js/cube/commit/e02f6126cd274f7a6f212b497b720fbed6dc1131))

### Features

- **api-gateway:** Meta - expose aliasMember for members in View ([#8945](https://github.com/cube-js/cube/issues/8945)) ([c127f36](https://github.com/cube-js/cube/commit/c127f36a124edb24ba31c043760b2883485b79f1))

## [1.1.3](https://github.com/cube-js/cube/compare/v1.1.2...v1.1.3) (2024-11-08)

### Bug Fixes

- **cubesql:** Don't show meta OLAP queries in query history ([#8336](https://github.com/cube-js/cube/issues/8336)) ([78a5fc3](https://github.com/cube-js/cube/commit/78a5fc332e5e224ba9a00bba8deacf0417edbd34))
- **cubesql:** Fix `NULLS FIRST`/`LAST` SQL push down for several dialects ([#8895](https://github.com/cube-js/cube/issues/8895)) ([61c5ac6](https://github.com/cube-js/cube/commit/61c5ac618c9b68cf1185625d77420ed4a2c5da54))

## [1.1.2](https://github.com/cube-js/cube/compare/v1.1.1...v1.1.2) (2024-11-01)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.1.1](https://github.com/cube-js/cube/compare/v1.1.0...v1.1.1) (2024-10-31)

**Note:** Version bump only for package @cubejs-backend/cubesql

# [1.1.0](https://github.com/cube-js/cube/compare/v1.0.4...v1.1.0) (2024-10-24)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.0.4](https://github.com/cube-js/cube/compare/v1.0.3...v1.0.4) (2024-10-23)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.0.3](https://github.com/cube-js/cube/compare/v1.0.2...v1.0.3) (2024-10-22)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [1.0.2](https://github.com/cube-js/cube/compare/v1.0.1...v1.0.2) (2024-10-21)

### Features

- **cubesql:** Top-down extractor for rewrites ([#8694](https://github.com/cube-js/cube/issues/8694)) ([e8fe6db](https://github.com/cube-js/cube/commit/e8fe6db3b382bab91d9b2e2b46886095b5f8b2e6))
- OpenAPI - declare meta field ([#8840](https://github.com/cube-js/cube/issues/8840)) ([55b8f63](https://github.com/cube-js/cube/commit/55b8f637ce721e1145050699671051ea524f5b19))
- OpenAPI - declare type field for Cube ([#8837](https://github.com/cube-js/cube/issues/8837)) ([578b90c](https://github.com/cube-js/cube/commit/578b90c9c89926d333498cfe6f2f155579688cb0))

## [1.0.1](https://github.com/cube-js/cube/compare/v1.0.0...v1.0.1) (2024-10-16)

### Features

- **cubesql:** QueryRouter - remove some MySQL functionality ([#8818](https://github.com/cube-js/cube/issues/8818)) ([5935964](https://github.com/cube-js/cube/commit/59359641cab81bdea55db83075a1e009b52087a7))

# [1.0.0](https://github.com/cube-js/cube/compare/v0.36.11...v1.0.0) (2024-10-15)

### Features

- Enable `CUBESQL_SQL_PUSH_DOWN` by default ([#8814](https://github.com/cube-js/cube/issues/8814)) ([e1a8e8d](https://github.com/cube-js/cube/commit/e1a8e8d124ee80839193a363c939152392095ee8))

### BREAKING CHANGES

- Enabling `CUBESQL_SQL_PUSH_DOWN` is backward incompatible to many default behaviors of SQL API

## [0.36.9](https://github.com/cube-js/cube/compare/v0.36.8...v0.36.9) (2024-10-14)

### Bug Fixes

- **cubesql:** Ignore `__user IS NOT NULL` filter ([#8796](https://github.com/cube-js/cube/issues/8796)) ([c1e542a](https://github.com/cube-js/cube/commit/c1e542a7f4f6e22edeff335846c2e1c53c95116b))

## [0.36.8](https://github.com/cube-js/cube/compare/v0.36.7...v0.36.8) (2024-10-11)

### Bug Fixes

- Render LIMIT 0 and OFFSET 0 properly in SQL templates ([#8781](https://github.com/cube-js/cube/issues/8781)) ([6b17731](https://github.com/cube-js/cube/commit/6b17731f84aa494de820dce791e3120e4282bc37))

## [0.36.7](https://github.com/cube-js/cube/compare/v0.36.6...v0.36.7) (2024-10-08)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.36.2](https://github.com/cube-js/cube/compare/v0.36.1...v0.36.2) (2024-09-18)

### Bug Fixes

- **cubesql:** Support new QuickSight meta queries ([148e4cf](https://github.com/cube-js/cube/commit/148e4cf32890872d8be41b127a05df1aca68179c))

### Features

- **cubesql:** Support `[I]LIKE ... ESCAPE ...` SQL push down ([2bda0dd](https://github.com/cube-js/cube/commit/2bda0dd968944e777c5b89b2587a620c448dba10))

# [0.36.0](https://github.com/cube-js/cube/compare/v0.35.81...v0.36.0) (2024-09-13)

- feat(cubesql)!: Enable CUBESQL_SQL_NO_IMPLICIT_ORDER by default ([f22e1ef](https://github.com/cube-js/cube/commit/f22e1efaef6cb81ce920aac0e85abc0eebc94bf9))

### Features

- **cubesql:** Support `information_schema.sql_implementation_info` meta table ([841f59a](https://github.com/cube-js/cube/commit/841f59a5f4155482cca188464fea89d5b1dc55b6))

### BREAKING CHANGES

- It's started to be true. it means that SQL API will not add ordering to queries that doesn't specify ORDER BY, previusly it was true only for ungrouped queries

## [0.35.81](https://github.com/cube-js/cube/compare/v0.35.80...v0.35.81) (2024-09-12)

### Bug Fixes

- **cubesql:** Use load meta with user change for SQL generation calls ([#8693](https://github.com/cube-js/cube/issues/8693)) ([0f7bb3d](https://github.com/cube-js/cube/commit/0f7bb3d3a96447a69835e3c591ebaf67592c3eed))

## [0.35.80](https://github.com/cube-js/cube/compare/v0.35.79...v0.35.80) (2024-09-09)

### Features

- **cubesql:** Fill pg_description table with cube and members descriptions ([#8618](https://github.com/cube-js/cube/issues/8618)) ([2288c18](https://github.com/cube-js/cube/commit/2288c18bf30d1f3a3299b235fe9b4405d2cb7463))
- **cubesql:** Support join with type coercion ([#8608](https://github.com/cube-js/cube/issues/8608)) ([46b3a36](https://github.com/cube-js/cube/commit/46b3a36936f0f00805144714f0dd87a3c50a5e0a))

## [0.35.79](https://github.com/cube-js/cube/compare/v0.35.78...v0.35.79) (2024-09-04)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.35.78](https://github.com/cube-js/cube/compare/v0.35.77...v0.35.78) (2024-08-27)

### Bug Fixes

- **cubesql:** Don't clone AST on pre-planning step ([#8644](https://github.com/cube-js/cube/issues/8644)) ([03277b0](https://github.com/cube-js/cube/commit/03277b0e42afcc31fd7b1822aa9570a88f78e788))
- **cubesql:** Fix non-injective functions split rules, make them variadic ([#8563](https://github.com/cube-js/cube/issues/8563)) ([ed33403](https://github.com/cube-js/cube/commit/ed33403eee334642f1ccaf2db075035e3b877368))
- **cubesql:** Normalize incoming date_part and date_trunc tokens ([#8583](https://github.com/cube-js/cube/issues/8583)) ([f985265](https://github.com/cube-js/cube/commit/f9852650cb3b61dd52386f5cc6a1cec6a5752588))

### Features

- **cubejs-api-gateway:** Add description to V1CubeMeta, V1CubeMetaDimension and V1CubeMetaMeasure in OpenAPI ([#8597](https://github.com/cube-js/cube/issues/8597)) ([1afa934](https://github.com/cube-js/cube/commit/1afa934b1db7379a87ee913816e9ce855783d2bb))
- **cubesql:** Introduce max sessions limit ([#8616](https://github.com/cube-js/cube/issues/8616)) ([dfcb596](https://github.com/cube-js/cube/commit/dfcb5966e76a27fd847e2457bf4af2e1c32b21ac))
- **cubesql:** Upgrade serde, serde_json - performance boost ([#8636](https://github.com/cube-js/cube/issues/8636)) ([b4754db](https://github.com/cube-js/cube/commit/b4754dbd7898d928844558adece42668fe6e728f))

## [0.35.77](https://github.com/cube-js/cube/compare/v0.35.76...v0.35.77) (2024-08-26)

### Features

- **cubesql:** CubeScan - don't clone strings for non stream response ([#8633](https://github.com/cube-js/cube/issues/8633)) ([df364be](https://github.com/cube-js/cube/commit/df364beae38badbeeb27488a847a34c4431457e8))

## [0.35.76](https://github.com/cube-js/cube/compare/v0.35.75...v0.35.76) (2024-08-24)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.35.74](https://github.com/cube-js/cube.js/compare/v0.35.73...v0.35.74) (2024-08-22)

### Features

- **cubesql:** Add pg_class self-reference ([#8603](https://github.com/cube-js/cube.js/issues/8603)) ([05ea7e2](https://github.com/cube-js/cube.js/commit/05ea7e25014b7ac72e32fa0391ce3ea99ab3e01f))

## [0.35.73](https://github.com/cube-js/cube/compare/v0.35.72...v0.35.73) (2024-08-21)

### Bug Fixes

- **cubesql:** Reduce memory usage while converting to DataFrame ([#8598](https://github.com/cube-js/cube/issues/8598)) ([604085e](https://github.com/cube-js/cube/commit/604085e5a2066414eb91128ae020b6e4b92b449f))
- **cubesql:** Use date_part => date_part + date_trunc split only with appropriate date_part argument ([#8552](https://github.com/cube-js/cube/issues/8552)) ([9387072](https://github.com/cube-js/cube/commit/93870720ac872ab06266d4a65a0f9b4aec384f6a))

## [0.35.72](https://github.com/cube-js/cube/compare/v0.35.71...v0.35.72) (2024-08-16)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.35.71](https://github.com/cube-js/cube/compare/v0.35.70...v0.35.71) (2024-08-15)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.35.70](https://github.com/cube-js/cube/compare/v0.35.69...v0.35.70) (2024-08-14)

### Bug Fixes

- **cubesql:** Don't push down aggregate to grouped query with filters ([df3334c](https://github.com/cube-js/cube/commit/df3334ce6e97e82c4773de5129a615ab5a3d05d8))

## [0.35.69](https://github.com/cube-js/cube/compare/v0.35.68...v0.35.69) (2024-08-12)

### Bug Fixes

- **cubesql:** Split PowerBI count distinct expression ([6a518d3](https://github.com/cube-js/cube/commit/6a518d32c70f8e7da5433d77c4476072829a3ed9))

## [0.35.68](https://github.com/cube-js/cube/compare/v0.35.67...v0.35.68) (2024-08-12)

### Features

- **cubesql:** Support variable number of scalar function arguments in split rewrites ([#8534](https://github.com/cube-js/cube/issues/8534)) ([2300fe8](https://github.com/cube-js/cube/commit/2300fe816ce385dc583355e0d9f18dab90150730))

## [0.35.67](https://github.com/cube-js/cube/compare/v0.35.66...v0.35.67) (2024-08-07)

### Features

- **cubesql:** Support push down cast type templates ([556ca7c](https://github.com/cube-js/cube/commit/556ca7c67b280b18221cb6748cfe20f841b1a7b9))

## [0.35.66](https://github.com/cube-js/cube/compare/v0.35.65...v0.35.66) (2024-08-06)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.35.65](https://github.com/cube-js/cube/compare/v0.35.64...v0.35.65) (2024-07-26)

### Features

- **native:** Add datetime types support to generate_series() UDTF ([#8511](https://github.com/cube-js/cube/issues/8511)) ([99b3c65](https://github.com/cube-js/cube/commit/99b3c654382de0b750b6204286dc3ddf5dc7a63e))

## [0.35.64](https://github.com/cube-js/cube/compare/v0.35.63...v0.35.64) (2024-07-24)

### Bug Fixes

- **native:** Fix build failures caused by new ShutdownMode param ([#8514](https://github.com/cube-js/cube/issues/8514)) ([80d10fd](https://github.com/cube-js/cube/commit/80d10fda26bc693a9c4e2510283cbc6ca06eb4a5))

### Features

- Smart and Semi-Fast shutdown ([#8411](https://github.com/cube-js/cube/issues/8411)) ([0bc8e6f](https://github.com/cube-js/cube/commit/0bc8e6f8f98e3ca68e42d0d1817bf46ef6732b22))

### Performance Improvements

- **cubesql:** More complete usage of member_name_to_expr caching ([#8497](https://github.com/cube-js/cube/issues/8497)) ([3f369e3](https://github.com/cube-js/cube/commit/3f369e3720e01772e5c64a37d533cb197f0a26e3))

## [0.35.63](https://github.com/cube-js/cube/compare/v0.35.62...v0.35.63) (2024-07-24)

### Bug Fixes

- **cubesql:** Apply `IN` as `=` transformation with push down disabled ([152fca0](https://github.com/cube-js/cube/commit/152fca0edad8f384a0a4c4f8f833bcd1a3e950ed))

### Features

- **cubesql:** Support float\*interval and interval/float ([#8496](https://github.com/cube-js/cube/issues/8496)) ([300f8fc](https://github.com/cube-js/cube/commit/300f8fc3224ebc0500f33e13e337bcb44cecce7c))

## [0.35.62](https://github.com/cube-js/cube/compare/v0.35.61...v0.35.62) (2024-07-22)

### Features

- **native:** Initial support for native api-gateway ([#8472](https://github.com/cube-js/cube/issues/8472)) ([d917d6f](https://github.com/cube-js/cube/commit/d917d6fd422090cc78fc30125731d147a091de6c))
- **rust/cubeclient:** Upgrade reqwest to 0.12.5 (hyper 1) ([#8498](https://github.com/cube-js/cube/issues/8498)) ([f77c3aa](https://github.com/cube-js/cube/commit/f77c3aad67566568117f2c1d42859e2fd53a13d2))

### Performance Improvements

- **cubesql:** Replaced LogicalPlanData::find_member with a caching version ([#8469](https://github.com/cube-js/cube/issues/8469)) ([858d965](https://github.com/cube-js/cube/commit/858d965a42b30e446ae7fd19899cfd9b078ee63f))

## [0.35.61](https://github.com/cube-js/cube/compare/v0.35.60...v0.35.61) (2024-07-19)

### Bug Fixes

- **cubesql:** Transform `IN` filter with one value to `=` with all expressions ([671e067](https://github.com/cube-js/cube/commit/671e067d25521deadf1502d0dec21da2df5c8e3b))

### Features

- **cubesql:** Support `Null` input type in `SUM` and `AVG` functions ([5ce589a](https://github.com/cube-js/cube/commit/5ce589a1e7b3f8e3f850fdc9abadb26e212f2da4))

## [0.35.60](https://github.com/cube-js/cube/compare/v0.35.59...v0.35.60) (2024-07-17)

### Features

- **cubesql:** Upgrade rust to nightly-2024-07-15 ([#8473](https://github.com/cube-js/cube/issues/8473)) ([6a6a7fe](https://github.com/cube-js/cube/commit/6a6a7fe694c13d04bf048434df92e20ad920aed5))
- **cubesql:** Use lld linker for linux ([#8439](https://github.com/cube-js/cube/issues/8439)) ([a2fb38b](https://github.com/cube-js/cube/commit/a2fb38b410382abdd925d42e10673e9192f0f880))

## [0.35.59](https://github.com/cube-js/cube/compare/v0.35.58...v0.35.59) (2024-07-13)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.35.58](https://github.com/cube-js/cube/compare/v0.35.57...v0.35.58) (2024-07-10)

### Bug Fixes

- **cubesql:** Fix incorrect `dateRange` when filtering over `date_trunc`'d column in outer query ([90ce14e](https://github.com/cube-js/cube/commit/90ce14e327046a8526012183d040bad0b73cc7cf))

## [0.35.57](https://github.com/cube-js/cube.js/compare/v0.35.56...v0.35.57) (2024-07-05)

### Bug Fixes

- **cubesql:** Don't put aggregate functions in `Projection` on split ([4ff4086](https://github.com/cube-js/cube.js/commit/4ff4086da869a97412a518b9b4572ccc5dcbae7c))
- **cubesql:** Make CAST(COUNT(\*) as float) work properly in some situations ([#8423](https://github.com/cube-js/cube.js/issues/8423)) ([3ff5c9f](https://github.com/cube-js/cube.js/commit/3ff5c9fc2349fa35644040787173a6c97d93341f))

### Features

- **cubesql:** Remove implicit order by clause ([#8380](https://github.com/cube-js/cube.js/issues/8380)) ([3e7d325](https://github.com/cube-js/cube.js/commit/3e7d325aadff1a572953d843394f55577aef0357))

## [0.35.56](https://github.com/cube-js/cube/compare/v0.35.55...v0.35.56) (2024-07-03)

### Bug Fixes

- **cubesql:** Correctly type `pg_index` fields ([edadaf1](https://github.com/cube-js/cube/commit/edadaf13eeb05c51529fcf10cafd7028454d73fe))
- **cubesql:** Realias outer projection on flatten ([8cbcd7b](https://github.com/cube-js/cube/commit/8cbcd7b038a39b77f8b0309dd959f9a4edaddea3))

### Features

- Make graceful shutdown add fatal messages in postgres ([8fe1af2](https://github.com/cube-js/cube/commit/8fe1af223a79253b9beaab923d29e96d13f67cdf))

## [0.35.54](https://github.com/cube-js/cube.js/compare/v0.35.53...v0.35.54) (2024-06-26)

### Features

- **cubesql:** `Interval(MonthDayNano)` multiplication and decomposition ([576f7f7](https://github.com/cube-js/cube.js/commit/576f7f7de9190a4d466a58f619cb5cf8d6bc6a59))

## [0.35.53](https://github.com/cube-js/cube/compare/v0.35.52...v0.35.53) (2024-06-26)

### Features

- **cubesql:** Decimal128 i.e. NUMERIC(_,_) literal generation for SQL push down ([43e29f9](https://github.com/cube-js/cube/commit/43e29f9f49a683d148b31ea1bcccd28a7c99034e))

## [0.35.51](https://github.com/cube-js/cube/compare/v0.35.50...v0.35.51) (2024-06-20)

### Bug Fixes

- **cubesql:** Support `CAST` projection split ([bcbe47b](https://github.com/cube-js/cube/commit/bcbe47b498ee47d563c3354d52db5bca886b0b65))

### Features

- **cubesql:** support timestamp subtract date in SQL API, tests ([#8372](https://github.com/cube-js/cube/issues/8372)) ([1db6a5c](https://github.com/cube-js/cube/commit/1db6a5c9bc912e52e5874a1ad01a8636523cc2ab))

### Performance Improvements

- **cubesql:** Make incremental rule scheduler distinguish between generation 0 and 1 ([719d8cf](https://github.com/cube-js/cube/commit/719d8cf862be7743355224f7f1737e5ea7a8c788))

## [0.35.50](https://github.com/cube-js/cube/compare/v0.35.49...v0.35.50) (2024-06-17)

### Bug Fixes

- **cubesql:** Proper interval dataframe values and better formatting ([57e1d74](https://github.com/cube-js/cube/commit/57e1d74bfb374eb4334489b1a0156e032077577b))

## [0.35.49](https://github.com/cube-js/cube/compare/v0.35.48...v0.35.49) (2024-06-14)

### Features

- **cubesql:** Implement timestamp subtraction and epoch extraction from intervals ([1239e15](https://github.com/cube-js/cube/commit/1239e159fcb774b161f43fb7b515acb8346204fa))

## [0.35.48](https://github.com/cube-js/cube/compare/v0.35.47...v0.35.48) (2024-06-14)

### Bug Fixes

- **cubesql:** Rollup don't work over subquery with asterisk projection ([#8354](https://github.com/cube-js/cube/issues/8354)) ([0bc0306](https://github.com/cube-js/cube/commit/0bc03066ac4bedec4ee2b85d381e36bb13e2edef))

### Features

- **cubesql:** support `GREATEST`/`LEAST` SQL functions ([#8325](https://github.com/cube-js/cube/issues/8325)) ([c13a28e](https://github.com/cube-js/cube/commit/c13a28e21514c0e06e41c0ed14e97621bd777cf7))

### Performance Improvements

- **cubesql:** Use an incremental rule scheduler for egg ([5892df7](https://github.com/cube-js/cube/commit/5892df749edb0f290488a196804a5a060e5ea387))

## [0.35.47](https://github.com/cube-js/cube.js/compare/v0.35.46...v0.35.47) (2024-06-07)

### Bug Fixes

- **cubesql:** Rollup doesn't work over aliased columns ([#8334](https://github.com/cube-js/cube.js/issues/8334)) ([98e7529](https://github.com/cube-js/cube.js/commit/98e7529975703f2d4b72cc8f21ce4f8c6fc4c8de))

## [0.35.46](https://github.com/cube-js/cube.js/compare/v0.35.45...v0.35.46) (2024-06-06)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.35.45](https://github.com/cube-js/cube.js/compare/v0.35.44...v0.35.45) (2024-06-05)

### Bug Fixes

- **cubesql:** Don't duplicate time dimensions ([577220d](https://github.com/cube-js/cube.js/commit/577220ddb88c098dd3142e518fcdb73eb7513b74))
- **cubesql:** Support `DATE_TRUNC` equals literal string ([69ba0ee](https://github.com/cube-js/cube.js/commit/69ba0eeb9724ee4105ad0d0ff39eedf92dbc715f))

### Performance Improvements

- **cubesql:** Improve rewrite engine performance ([4f78b8a](https://github.com/cube-js/cube.js/commit/4f78b8a9b75672227e7114be6a524a1b1ced8ff6))

## [0.35.44](https://github.com/cube-js/cube/compare/v0.35.43...v0.35.44) (2024-06-04)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.35.43](https://github.com/cube-js/cube/compare/v0.35.42...v0.35.43) (2024-05-31)

### Features

- sql api over http ([#8254](https://github.com/cube-js/cube/issues/8254)) ([4c18954](https://github.com/cube-js/cube/commit/4c18954452bf14268a2f368960a72fec45f274dc))

## [0.35.42](https://github.com/cube-js/cube/compare/v0.35.41...v0.35.42) (2024-05-30)

### Features

- **cubesql:** Group By Rollup support ([#8281](https://github.com/cube-js/cube/issues/8281)) ([e563798](https://github.com/cube-js/cube/commit/e5637980489608e374f3b89cc219207973a08bbb))

## [0.35.40](https://github.com/cube-js/cube/compare/v0.35.39...v0.35.40) (2024-05-24)

### Bug Fixes

- **cubesql:** Match `inDateRange` as time dimension date range in `AND` ([64f176a](https://github.com/cube-js/cube/commit/64f176ab5afc4b0771e45206bff5d6649ca8d672))

### Features

- **cubesql:** Support `DATE_TRUNC` InList filter ([48e7ad4](https://github.com/cube-js/cube/commit/48e7ad4826f6d279570badae13c93b80c28c8094))

## [0.35.39](https://github.com/cube-js/cube/compare/v0.35.38...v0.35.39) (2024-05-24)

### Bug Fixes

- **cubesql:** Fix time dimension range filter chaining with `OR` operator ([757c4c5](https://github.com/cube-js/cube/commit/757c4c51f608c34b8a857ef7a98229bb3f7b04bb))

### Features

- **cubesql:** Flatten list expression rewrites to improve performance ([96c1549](https://github.com/cube-js/cube/commit/96c1549e1dc2d51f94aacd8fd1b95baef31cc0b1))

## [0.35.37](https://github.com/cube-js/cube/compare/v0.35.36...v0.35.37) (2024-05-20)

### Bug Fixes

- **cubesql:** Make param render respect dialect's reuse params flag ([9c91af2](https://github.com/cube-js/cube/commit/9c91af2e03c84d903ac153337cda9f53682aacf1))

## [0.35.36](https://github.com/cube-js/cube/compare/v0.35.35...v0.35.36) (2024-05-17)

### Bug Fixes

- **cubesql:** Do not generate large graphs for large `IN` filters ([f29c9e4](https://github.com/cube-js/cube/commit/f29c9e481ffa65361a804f6ff1e5bf685cb8d0d5))

## [0.35.35](https://github.com/cube-js/cube.js/compare/v0.35.34...v0.35.35) (2024-05-17)

### Bug Fixes

- **cubesql:** Remove incorrect LOWER match rules and fallback to SQL pushdown ([#8246](https://github.com/cube-js/cube.js/issues/8246)) ([6c39f37](https://github.com/cube-js/cube.js/commit/6c39f376a649eeb7d9c171f1805033b020ef58a7))
- **cubesql:** Remove prefix underscore from aliases ([#8266](https://github.com/cube-js/cube.js/issues/8266)) ([24e8977](https://github.com/cube-js/cube.js/commit/24e89779fc4833a8c6b14dc1496d95271c18ebdc))
- **schema-compiler:** Fix failing of `WHERE FALSE` queries ([#8265](https://github.com/cube-js/cube.js/issues/8265)) ([e63b4ab](https://github.com/cube-js/cube.js/commit/e63b4ab701cf44162d5e51d65b8d38e812f9085e))

### Features

- **cubesql:** Send expressions as objects ([#8216](https://github.com/cube-js/cube.js/issues/8216)) ([4deee84](https://github.com/cube-js/cube.js/commit/4deee845a51e3999f3a75b121e14b570a124ad3a))

## [0.35.33](https://github.com/cube-js/cube.js/compare/v0.35.32...v0.35.33) (2024-05-15)

### Features

- **cubesql:** Rewrites for pushdown of subqueries with empty source ([#8188](https://github.com/cube-js/cube.js/issues/8188)) ([86a58a5](https://github.com/cube-js/cube.js/commit/86a58a5f3368a509debfb3f2ba4c83001377127c))

## [0.35.32](https://github.com/cube-js/cube/compare/v0.35.31...v0.35.32) (2024-05-14)

### Features

- **cubesql:** Flatten IN lists expressions to improve performance ([#8235](https://github.com/cube-js/cube/issues/8235)) ([66aa01d](https://github.com/cube-js/cube/commit/66aa01d4f8f888993e68a0967e664b9dcfcc51e7))

## [0.35.30](https://github.com/cube-js/cube.js/compare/v0.35.29...v0.35.30) (2024-05-10)

### Bug Fixes

- **cubesql:** Add alias to rebased window expressions ([990a767](https://github.com/cube-js/cube.js/commit/990a767e3b2f32c0846907b1dfcff232227b4cbc))
- **cubesql:** Reuse query params in push down ([b849f34](https://github.com/cube-js/cube.js/commit/b849f34c001b9a94ac5aed1edacce906fd02f33c))

### Features

- **cubesql:** Support window frame SQL push down ([5469dbc](https://github.com/cube-js/cube.js/commit/5469dbc14c4ae15d9e1047fca90ab5cc4268f047))

## [0.35.25](https://github.com/cube-js/cube.js/compare/v0.35.24...v0.35.25) (2024-04-29)

### Bug Fixes

- **cubesql:** Disallow ematching cycles ([4902c6d](https://github.com/cube-js/cube.js/commit/4902c6d3f882a0ee27f62d63d8b0fcda3669a0b1))

## [0.35.24](https://github.com/cube-js/cube/compare/v0.35.23...v0.35.24) (2024-04-26)

### Bug Fixes

- **cubesql:** Fix `date_trunc` over column offset in filters ([1a602be](https://github.com/cube-js/cube/commit/1a602bea8fc17cc258c6d125ea8e7904b33454ff))

## [0.35.23](https://github.com/cube-js/cube/compare/v0.35.22...v0.35.23) (2024-04-25)

### Features

- **cubesql:** In subquery rewrite ([#8162](https://github.com/cube-js/cube/issues/8162)) ([d17c2a7](https://github.com/cube-js/cube/commit/d17c2a7d58beada203009c5d624974d3a68c6af8))

## [0.35.19](https://github.com/cube-js/cube.js/compare/v0.35.18...v0.35.19) (2024-04-18)

### Features

- **cubesql:** Update egg and remove some clones ([#8164](https://github.com/cube-js/cube.js/issues/8164)) ([fe20d35](https://github.com/cube-js/cube.js/commit/fe20d3575abc7105f827b979266e147299553aee))

## [0.35.14](https://github.com/cube-js/cube/compare/v0.35.13...v0.35.14) (2024-04-15)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.35.11](https://github.com/cube-js/cube/compare/v0.35.10...v0.35.11) (2024-04-11)

### Bug Fixes

- **cubesql:** Support latest Metabase, fix [#8105](https://github.com/cube-js/cube/issues/8105) ([#8130](https://github.com/cube-js/cube/issues/8130)) ([c82bb10](https://github.com/cube-js/cube/commit/c82bb10ed383f9c0cec04def8ac7dc53283f7254))

## [0.35.10](https://github.com/cube-js/cube/compare/v0.35.9...v0.35.10) (2024-04-09)

### Bug Fixes

- **cubesql:** has_schema_privilege - compatiblity with PostgreSQL ([#8098](https://github.com/cube-js/cube/issues/8098)) ([42586cf](https://github.com/cube-js/cube/commit/42586cfae7f55c998f0034cd3c816f59ba4116df))

## [0.35.6](https://github.com/cube-js/cube/compare/v0.35.5...v0.35.6) (2024-04-02)

### Bug Fixes

- **cubesql:** Remove binary expression associative rewrites ([19f6245](https://github.com/cube-js/cube/commit/19f62450f9fcf23b5484620783275564f2eedca4))

## [0.35.5](https://github.com/cube-js/cube.js/compare/v0.35.4...v0.35.5) (2024-03-28)

### Bug Fixes

- **cubesql:** SQL push down of ORDER BY causes Invalid query format: "order[0][0]" with value fails to match the required pattern: /^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/ ([#8032](https://github.com/cube-js/cube.js/issues/8032)) ([0681725](https://github.com/cube-js/cube.js/commit/0681725c921ea62f7ef813562be0202e93928889))

## [0.35.4](https://github.com/cube-js/cube.js/compare/v0.35.3...v0.35.4) (2024-03-27)

### Bug Fixes

- **cubesql:** Flatten aliasing breaks wrapped select `ORDER BY` expressions ([#8004](https://github.com/cube-js/cube.js/issues/8004)) ([eea88ec](https://github.com/cube-js/cube.js/commit/eea88ecaf3267091d1fbdd761db0451775b9fbe9))

### Features

- **cubesql:** Support `DISTINCT` push down ([fc79188](https://github.com/cube-js/cube.js/commit/fc79188b5a205d29d9acd3669677cc829b0a9013))

## [0.35.2](https://github.com/cube-js/cube/compare/v0.35.1...v0.35.2) (2024-03-22)

### Bug Fixes

- **cubesql:** Fix timestamp parsing format string ([938b13c](https://github.com/cube-js/cube/commit/938b13c16706243efdb38f67e687c9778b8ca5f9))

### Features

- **cubesql:** Flatten rules to allow multiple level transformation queries be executed by split and SQL push down ([#7979](https://github.com/cube-js/cube/issues/7979)) ([f078adc](https://github.com/cube-js/cube/commit/f078adc38ce3eab39763253d137f291ee7625bf1))
- **cubesql:** Parse timestamp strings as `Date32` ([4ba5a80](https://github.com/cube-js/cube/commit/4ba5a80c6736bcd7e19afc88692c7b6713286850))
- **cubesql:** Support `a ^ b` exponentiation ([914b058](https://github.com/cube-js/cube/commit/914b05893a66269ef41fae3d5b81f903ad1a75e7))

## [0.35.1](https://github.com/cube-js/cube/compare/v0.35.0...v0.35.1) (2024-03-18)

**Note:** Version bump only for package @cubejs-backend/cubesql

# [0.35.0](https://github.com/cube-js/cube/compare/v0.34.62...v0.35.0) (2024-03-14)

### Bug Fixes

- **cubesql:** Don't ignore second filter over same time dimension ([03e7f3a](https://github.com/cube-js/cube/commit/03e7f3a6e68d911368fdba2a79387cbea995cae5))

## [0.34.62](https://github.com/cube-js/cube/compare/v0.34.61...v0.34.62) (2024-03-13)

### Bug Fixes

- **cubesql:** Fix push down column remapping ([8221a53](https://github.com/cube-js/cube/commit/8221a53a6f679ea9dcfce83df948173ccc89c5f0))

## [0.34.61](https://github.com/cube-js/cube/compare/v0.34.60...v0.34.61) (2024-03-11)

### Bug Fixes

- **cubesql:** Error out when temporary table already exists ([1a0a324](https://github.com/cube-js/cube/commit/1a0a324274071b9dd9a667505c86698870e236b0))
- **cubesql:** Fix push down `CASE` with expr ([f1d1242](https://github.com/cube-js/cube/commit/f1d12428870cd6e1a159bd559a3402bb6be778aa))
- **cubesql:** Fix push down column remapping ([c7736cc](https://github.com/cube-js/cube/commit/c7736ccb2c3e1fd809d98adf58bad012fc38c8b7))
- **cubesql:** Trim ".0" postfix when converting `Float to `Utf8` ([3131f94](https://github.com/cube-js/cube/commit/3131f94def0a73b29fd1638a40672377339dd7d3))

### Features

- **cubesql:** Support `FETCH ... n ROWS ONLY` ([53b0c14](https://github.com/cube-js/cube/commit/53b0c149c7e348cfb4b36890d72c5090762e717b))

### Reverts

- Revert "fix(cubesql): Fix push down column remapping" (#7895) ([dec8901](https://github.com/cube-js/cube/commit/dec8901493814f3028e38e0ed17f5868e6ecd7a5)), closes [#7895](https://github.com/cube-js/cube/issues/7895)

## [0.34.60](https://github.com/cube-js/cube.js/compare/v0.34.59...v0.34.60) (2024-03-02)

### Bug Fixes

- **cubesql:** Allow different `Timestamp` types in `DATEDIFF` ([de9ef08](https://github.com/cube-js/cube.js/commit/de9ef081cae21f551a38219bb64749e08e7ca6fc))
- **cubesql:** Prioritize ungrouped aggregate scans over ungrouped projection scans so most of the members can be pushed down without wrapping ([#7865](https://github.com/cube-js/cube.js/issues/7865)) ([addde0d](https://github.com/cube-js/cube.js/commit/addde0d373f01e69485b8a0333850917ffad9a2d))
- **cubesql:** Remove excessive limit on inner wrapped queries ([#7864](https://github.com/cube-js/cube.js/issues/7864)) ([b97268f](https://github.com/cube-js/cube.js/commit/b97268fe5caf55c5b7806c597b9f7b75410f6ba4))

### Features

- **cubesql:** In subquery support ([#7851](https://github.com/cube-js/cube.js/issues/7851)) ([8e2a3ec](https://github.com/cube-js/cube.js/commit/8e2a3ecc348c4ab9e6a5ab038c46fcf7f4c3dfcc))
- **cubesql:** Support split and SUM(1) ([16e2ee0](https://github.com/cube-js/cube.js/commit/16e2ee0d290c502f796891e137556aad2275e52d))

## [0.34.59](https://github.com/cube-js/cube.js/compare/v0.34.58...v0.34.59) (2024-02-28)

### Bug Fixes

- **cubesql:** Replace only simple ungrouped measures in projections to avoid aggregate over aggregate statements ([#7852](https://github.com/cube-js/cube.js/issues/7852)) ([fa2a89b](https://github.com/cube-js/cube.js/commit/fa2a89b89f91c8eba175130fca33975200690288))

## [0.34.58](https://github.com/cube-js/cube.js/compare/v0.34.57...v0.34.58) (2024-02-27)

### Bug Fixes

- **cubesql:** Ambiguous reference in case of same two 16 char prefixes are in query ([5051f66](https://github.com/cube-js/cube.js/commit/5051f663b4142735ee1cc455936300f765de62a7))

## [0.34.57](https://github.com/cube-js/cube.js/compare/v0.34.56...v0.34.57) (2024-02-26)

### Bug Fixes

- **cubesql:** Can't find rewrite due to AST node limit reached for remaining non-equivalent date and timestamp constant folding rules ([b79f697](https://github.com/cube-js/cube.js/commit/b79f69739e2b8cb74cfc86db72fa7157dd723960))
- **cubesql:** Support `Z` postfix in `Timestamp` response transformation ([c013c91](https://github.com/cube-js/cube.js/commit/c013c913bbeb8dabacc508240ab94956f1172a8b))
- **cubesql:** Timestamp equals filter support ([754a0df](https://github.com/cube-js/cube.js/commit/754a0df3a20cae5269e961649a01fb5047906645))

### Features

- **cubesql:** `WHERE` SQL push down ([#7808](https://github.com/cube-js/cube.js/issues/7808)) ([98b5709](https://github.com/cube-js/cube.js/commit/98b570946905586f16a502f83b0a1cf8e4aa92a6))
- **cubesql:** Allow replacement of aggregation functions in SQL push down ([#7811](https://github.com/cube-js/cube.js/issues/7811)) ([97fa757](https://github.com/cube-js/cube.js/commit/97fa757a0d22e6d7d9432d686005765b28271f7c))
- **cubesql:** Support placeholders in `OFFSET`, `FETCH ...` ([60aad90](https://github.com/cube-js/cube.js/commit/60aad90a237800f4471bb4efa10ec590b50e19fe))
- **cubesql:** Support temporary tables ([7022611](https://github.com/cube-js/cube.js/commit/702261156fc3748fc7d3103f28bd4f4648fd4e0b))

## [0.34.56](https://github.com/cube-js/cube.js/compare/v0.34.55...v0.34.56) (2024-02-20)

### Bug Fixes

- **cubesql:** Allow `NULL` values in `CASE` ([a97acdc](https://github.com/cube-js/cube.js/commit/a97acdc996dd68a0e2c00c155dfe30a863440ecc))

## [0.34.55](https://github.com/cube-js/cube.js/compare/v0.34.54...v0.34.55) (2024-02-15)

### Bug Fixes

- **cubesql:** Quote `FROM` alias for SQL push down to avoid name clas… ([#7755](https://github.com/cube-js/cube.js/issues/7755)) ([4e2732a](https://github.com/cube-js/cube.js/commit/4e2732ae9997762a95fc946a5392b50e4dbf8622))

### Features

- **cubesql:** Strings with Unicode Escapes ([#7756](https://github.com/cube-js/cube.js/issues/7756)) ([49acad5](https://github.com/cube-js/cube.js/commit/49acad51e6a2b7ffa9ec0b584aaaa6e54f4f1434))

## [0.34.52](https://github.com/cube-js/cube.js/compare/v0.34.51...v0.34.52) (2024-02-13)

### Bug Fixes

- **cubesql:** Support new Metabase meta queries ([0bc09fd](https://github.com/cube-js/cube.js/commit/0bc09fdbefaf8b4e184f4c0803919103789acb0a))

### Features

- **cubesql:** Always Prefer SQL push down over aggregation in Datafusion ([#7751](https://github.com/cube-js/cube.js/issues/7751)) ([b4b0f05](https://github.com/cube-js/cube.js/commit/b4b0f05d16eb08f2be3831a5d48468be8d8b9d76))
- **cubesql:** EXTRACT(EPOCH, ...) support ([#7734](https://github.com/cube-js/cube.js/issues/7734)) ([b4deacd](https://github.com/cube-js/cube.js/commit/b4deacddf0a2e06b0b1f4216ca735a41e52724e2))
- **cubesql:** Support `TimestampNanosecond` in `CASE` ([69aed08](https://github.com/cube-js/cube.js/commit/69aed0875afb3b2d56176f4bdfdee7b1acd17ce9))
- **cubesql:** Support Athena OFFSET and LIMIT push down ([00c2a6b](https://github.com/cube-js/cube.js/commit/00c2a6b3cc88fde5d65d02549d3458818e4a8e42))

## [0.34.51](https://github.com/cube-js/cube.js/compare/v0.34.50...v0.34.51) (2024-02-11)

### Bug Fixes

- **cubesql:** Enable constant folding for unary minus exprs ([704d05a](https://github.com/cube-js/cube.js/commit/704d05aecafada282a6bcbd57bd0519bb5a12aa5))
- **cubesql:** Fix `CASE` type with `NULL` values ([2b7cc30](https://github.com/cube-js/cube.js/commit/2b7cc304b101330ca072fa0c4a3a6e9ae9efa2a5))
- **cubesql:** Stabilize split operations for SQL push down ([#7725](https://github.com/cube-js/cube.js/issues/7725)) ([6241e5e](https://github.com/cube-js/cube.js/commit/6241e5e9335148947a687f7e3d6b56929ba46c36))

### Features

- **cubesql:** Extend `DATEDIFF` push down support ([ecaaf1c](https://github.com/cube-js/cube.js/commit/ecaaf1ca5566ff7cc27289468440e6a902a09609))

## [0.34.48](https://github.com/cube-js/cube.js/compare/v0.34.47...v0.34.48) (2024-01-25)

### Bug Fixes

- **cubesql:** Fix unary minus operator precedence ([d5a935a](https://github.com/cube-js/cube.js/commit/d5a935ac3bb16c1dda6c30982cdc9ef787a24967))
- **cubesql:** Segment mixed with a filter and a date range filter may affect push down of `inDateRange` filter to time dimension ([#7684](https://github.com/cube-js/cube.js/issues/7684)) ([f29a7be](https://github.com/cube-js/cube.js/commit/f29a7be8379097b8de657ebc2e46f40bae3ccce9))
- **cubesql:** Support Sigma Sunday week granularity ([3d492eb](https://github.com/cube-js/cube.js/commit/3d492eb5feb84503a1bffda7481ed8b562939e44))

### Features

- **cubesql:** Support KPI chart in Thoughtspot ([dbab39e](https://github.com/cube-js/cube.js/commit/dbab39e63a1c752a56a2cb06169a479a3e9cb11e))
- **cubesql:** Support unwrapping BINARY expr from SUM(<expr>) ([#7683](https://github.com/cube-js/cube.js/issues/7683)) ([ce93cc7](https://github.com/cube-js/cube.js/commit/ce93cc7a0f667409d725b34913405f18d18f629b))

## [0.34.47](https://github.com/cube-js/cube.js/compare/v0.34.46...v0.34.47) (2024-01-23)

### Features

- **cubesql:** TO_CHAR - support more formats, correct NULL handling ([#7671](https://github.com/cube-js/cube.js/issues/7671)) ([2d1e2d2](https://github.com/cube-js/cube.js/commit/2d1e2d216c99af68a2d5cf1b2acd2f5e2a623323))

## [0.34.46](https://github.com/cube-js/cube.js/compare/v0.34.45...v0.34.46) (2024-01-18)

### Features

- **cubesql:** Cache plan rewrites with and without replaced parameters ([#7670](https://github.com/cube-js/cube.js/issues/7670)) ([c360d3c](https://github.com/cube-js/cube.js/commit/c360d3c9da61b45f8215cc17db098cfa0a74c899))

## [0.34.45](https://github.com/cube-js/cube.js/compare/v0.34.44...v0.34.45) (2024-01-16)

### Features

- **cubesql:** Query rewrite cache ([#7647](https://github.com/cube-js/cube.js/issues/7647)) ([79888af](https://github.com/cube-js/cube.js/commit/79888afc3823a3ef29ba76c440828c8c5d719ae4))

## [0.34.42](https://github.com/cube-js/cube.js/compare/v0.34.41...v0.34.42) (2024-01-07)

### Features

- **cubesql:** Compiler cache for rewrite rules ([#7604](https://github.com/cube-js/cube.js/issues/7604)) ([995889f](https://github.com/cube-js/cube.js/commit/995889fb7722cda3bf839095949d6d71693dd329))

## [0.34.41](https://github.com/cube-js/cube.js/compare/v0.34.40...v0.34.41) (2024-01-02)

### Bug Fixes

- **cubesql:** Enable `Visitor` on `GROUP BY` expressions ([#7575](https://github.com/cube-js/cube.js/issues/7575)) ([bcc1a89](https://github.com/cube-js/cube.js/commit/bcc1a8911fe99f33b0a82e865597dec38101ecad))

### Features

- **cubesql:** Support Domo data queries ([#7509](https://github.com/cube-js/cube.js/issues/7509)) ([6d644dc](https://github.com/cube-js/cube.js/commit/6d644dc5265245b8581eb2c2e3b75f5d6d9f929c))

## [0.34.40](https://github.com/cube-js/cube.js/compare/v0.34.39...v0.34.40) (2023-12-21)

### Features

- **cubesql:** Do not run split re-aggregate for trivial push down to improve wide table queries ([#7567](https://github.com/cube-js/cube.js/issues/7567)) ([8dbf879](https://github.com/cube-js/cube.js/commit/8dbf87986cd58f4860d647d5a0bb33e64a229db1))

## [0.34.37](https://github.com/cube-js/cube.js/compare/v0.34.36...v0.34.37) (2023-12-19)

### Features

- **cubesql:** Avoid pushing split down for trivial selects to optimi… ([#7556](https://github.com/cube-js/cube.js/issues/7556)) ([2bf86e5](https://github.com/cube-js/cube.js/commit/2bf86e5a70810f5f081a527d6e7b70c8020673aa))

## [0.34.36](https://github.com/cube-js/cube.js/compare/v0.34.35...v0.34.36) (2023-12-16)

### Bug Fixes

- **cubesql:** Improve performance for wide table querying ([#7534](https://github.com/cube-js/cube.js/issues/7534)) ([0f877d4](https://github.com/cube-js/cube.js/commit/0f877d41f08aeb1ebc9b22e9b38da931152435d2))

## [0.34.35](https://github.com/cube-js/cube.js/compare/v0.34.34...v0.34.35) (2023-12-13)

### Bug Fixes

- **cubesql:** Support Sigma Computing table schema sync ([d87bd19](https://github.com/cube-js/cube.js/commit/d87bd19384e25a161fb2424b3b6c01da675de04e))

### Features

- **cubesql:** Additional trace event logging for SQL API ([#7524](https://github.com/cube-js/cube.js/issues/7524)) ([6b700cd](https://github.com/cube-js/cube.js/commit/6b700cd493b16d4450ce1efaa449207836a47592))

## [0.34.31](https://github.com/cube-js/cube.js/compare/v0.34.30...v0.34.31) (2023-12-07)

### Bug Fixes

- **cubesql:** Avoid constant folding for current_date() function duri… ([#7498](https://github.com/cube-js/cube.js/issues/7498)) ([e86f4be](https://github.com/cube-js/cube.js/commit/e86f4be42a6e48a115c2765e0cda84fbf1cc56e7))

### Features

- **cubesql:** Support `Utf8 * Interval` expression ([ea1fa9c](https://github.com/cube-js/cube.js/commit/ea1fa9ca6e04cf12b4c334b5702d7a5a33f0c364))

## [0.34.27](https://github.com/cube-js/cube.js/compare/v0.34.26...v0.34.27) (2023-11-30)

### Features

- **cubesql:** Provide password supplied by Postgres connection as a 3rd argument of `check_sql_auth()` ([#7471](https://github.com/cube-js/cube.js/issues/7471)) ([ee3c19f](https://github.com/cube-js/cube.js/commit/ee3c19f8d467056c90ee407b3ac386dc1892b678)), closes [#5430](https://github.com/cube-js/cube.js/issues/5430)

## [0.34.26](https://github.com/cube-js/cube/compare/v0.34.25...v0.34.26) (2023-11-28)

### Bug Fixes

- **cubesql:** Missing template backslash escaping ([#7465](https://github.com/cube-js/cube/issues/7465)) ([4a08de5](https://github.com/cube-js/cube/commit/4a08de5791f7353b925c60ee84d2654e95e7967a))

### Features

- **cubesql:** Support SQL push down for several functions ([79e5ac8](https://github.com/cube-js/cube/commit/79e5ac8e998005ebf8b5f72ccf1d63f425f6003c))

## [0.34.23](https://github.com/cube-js/cube/compare/v0.34.22...v0.34.23) (2023-11-19)

### Features

- **cubesql:** Support `-` (unary minus) SQL push down ([a0a2e12](https://github.com/cube-js/cube/commit/a0a2e129e4cf3264df75bbdf53a962a892e4e9c2))
- **cubesql:** Support `NOT` SQL push down ([#7422](https://github.com/cube-js/cube/issues/7422)) ([7b1ff0d](https://github.com/cube-js/cube/commit/7b1ff0d897ec9a5cfffba1e09444ff7baa8bea5b))

## [0.34.22](https://github.com/cube-js/cube.js/compare/v0.34.21...v0.34.22) (2023-11-16)

### Bug Fixes

- **cubesql:** Window PARTITION BY, ORDER BY queries fail for SQL push down ([62b359f](https://github.com/cube-js/cube.js/commit/62b359f2d33d0c8fd59aa570e7e3a83718a3f7e8))

### Features

- **cubesql:** Ambiguous column references for SQL push down ([c5f1648](https://github.com/cube-js/cube.js/commit/c5f16485f2b7324f5e8c5ce3642ec9e9d29de534))

## [0.34.21](https://github.com/cube-js/cube.js/compare/v0.34.20...v0.34.21) (2023-11-15)

### Features

- **cubesql:** SQL push down support for synthetic fields ([#7418](https://github.com/cube-js/cube.js/issues/7418)) ([d2bdc1b](https://github.com/cube-js/cube.js/commit/d2bdc1bedbb89ffee14d3bda1c8045b833076e35))
- **cubesql:** Support SQL push down for more functions ([#7406](https://github.com/cube-js/cube.js/issues/7406)) ([b1606da](https://github.com/cube-js/cube.js/commit/b1606daba70ab92952b1cbbacd94dd7294b17ad5))

## [0.34.20](https://github.com/cube-js/cube/compare/v0.34.19...v0.34.20) (2023-11-14)

### Features

- **cubesql:** Support `[NOT] IN` SQL push down ([c64994a](https://github.com/cube-js/cube/commit/c64994ac26e1174ce121c79af46fa6a62747b7e9))

## [0.34.19](https://github.com/cube-js/cube.js/compare/v0.34.18...v0.34.19) (2023-11-11)

### Features

- **cubesql:** SQL push down support for window functions ([#7403](https://github.com/cube-js/cube.js/issues/7403)) ([b1da6c0](https://github.com/cube-js/cube.js/commit/b1da6c0e38e3b586c3d4b1ddf9c00be57065d960))

## [0.34.14](https://github.com/cube-js/cube.js/compare/v0.34.13...v0.34.14) (2023-11-05)

### Features

- **cubesql:** SQL push down for several ANSI SQL functions ([ac2bf15](https://github.com/cube-js/cube.js/commit/ac2bf15954e6b143b9014ff4b8f72c6098253c82))
- **cubesql:** SQL push down support for `IS NULL` and `IS NOT NULL` expressions ([9b3c27d](https://github.com/cube-js/cube.js/commit/9b3c27d502adbcda8a98a4de486a9d0baf4307aa))

## [0.34.13](https://github.com/cube-js/cube.js/compare/v0.34.12...v0.34.13) (2023-10-31)

### Bug Fixes

- **cubesql:** SQL push down for limit and offset for ungrouped queries ([67da8c3](https://github.com/cube-js/cube.js/commit/67da8c31463d81e0f84ed1430a1c2d848f910f66))

## [0.34.11](https://github.com/cube-js/cube/compare/v0.34.10...v0.34.11) (2023-10-29)

### Features

- **cubesql:** Allow changing current user through `SET user = ?` ([#7350](https://github.com/cube-js/cube/issues/7350)) ([2c9c8d6](https://github.com/cube-js/cube/commit/2c9c8d68d1bd76b005ebf863b6899ea463d59aae))

## [0.34.10](https://github.com/cube-js/cube.js/compare/v0.34.9...v0.34.10) (2023-10-27)

### Features

- **cubesql:** Introduce `CUBESQL_DISABLE_STRICT_AGG_TYPE_MATCH` to avoid aggregation type checking during querying ([#7316](https://github.com/cube-js/cube.js/issues/7316)) ([61089f0](https://github.com/cube-js/cube.js/commit/61089f056af44f69f79420ce9767e4bb68030f26))

## [0.34.8](https://github.com/cube-js/cube.js/compare/v0.34.7...v0.34.8) (2023-10-25)

### Bug Fixes

- **cubesql:** column does not exist in case of ORDER BY is called on CASE column ([8ec80e8](https://github.com/cube-js/cube.js/commit/8ec80e80ed1501bf369e29c68c1e3fc8f894693f))

### Features

- **cubesql:** Aggregation over dimensions support ([#7290](https://github.com/cube-js/cube.js/issues/7290)) ([745ae38](https://github.com/cube-js/cube.js/commit/745ae38554571b6890be7db5b1e1b5dc4c51324b))

## [0.34.6](https://github.com/cube-js/cube/compare/v0.34.5...v0.34.6) (2023-10-20)

### Bug Fixes

- **native:** Init logger earlier without javascript side (silient errors) ([#7228](https://github.com/cube-js/cube/issues/7228)) ([1f6d49d](https://github.com/cube-js/cube/commit/1f6d49dbd0aa2e792db8c3bcefd54a8a434b4000))

## [0.34.1](https://github.com/cube-js/cube.js/compare/v0.34.0...v0.34.1) (2023-10-09)

### Bug Fixes

- **cubesql:** Support unaliased columns for SQL push down ([#7199](https://github.com/cube-js/cube.js/issues/7199)) ([e92e15c](https://github.com/cube-js/cube.js/commit/e92e15c81205f9a05f3df88513d4b934fa039886))

### Features

- **cubesql:** Support `SET ROLE name` ([d02077e](https://github.com/cube-js/cube.js/commit/d02077ed44f8641850bf74f200c91cb6864fddbe))

# [0.34.0](https://github.com/cube-js/cube.js/compare/v0.33.65...v0.34.0) (2023-10-03)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.33.63](https://github.com/cube-js/cube.js/compare/v0.33.62...v0.33.63) (2023-09-26)

### Features

- **cubesql:** Tableau Standard Gregorian missing date groupings support through SQL push down and some other functions([#7172](https://github.com/cube-js/cube.js/issues/7172)) ([1339f57](https://github.com/cube-js/cube.js/commit/1339f577badf94aab02483e3431f614b1fe61302))

## [0.33.62](https://github.com/cube-js/cube/compare/v0.33.61...v0.33.62) (2023-09-25)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.33.60](https://github.com/cube-js/cube.js/compare/v0.33.59...v0.33.60) (2023-09-22)

### Bug Fixes

- **cubesql:** `dataRange` filter isn't being push down to time dimension in case of other filters are used ([a4edfae](https://github.com/cube-js/cube.js/commit/a4edfae654f99b0d5a1227dfc569e8f2231e9697)), closes [#6312](https://github.com/cube-js/cube.js/issues/6312)

## [0.33.59](https://github.com/cube-js/cube.js/compare/v0.33.58...v0.33.59) (2023-09-20)

### Features

- **cubesql:** `EXTRACT` SQL push down ([#7151](https://github.com/cube-js/cube.js/issues/7151)) ([e30c4da](https://github.com/cube-js/cube.js/commit/e30c4da555adfc1515fcc9e0253bc89ca8adc58b))
- **cubesql:** Add ability to filter dates inclusive of date being passed in when using `<=` or `>=` ([#7041](https://github.com/cube-js/cube.js/issues/7041)) Thanks [@darapuk](https://github.com/darapuk) ! ([6b9ae70](https://github.com/cube-js/cube.js/commit/6b9ae703b113a01fa4e6de54b5597475aed0b85d))

## [0.33.57](https://github.com/cube-js/cube/compare/v0.33.56...v0.33.57) (2023-09-15)

### Bug Fixes

- **cubesql:** ORDER BY references outer alias instead of inner expression for SQL push down ([778e16c](https://github.com/cube-js/cube/commit/778e16c52319e5ba6d8a786de1e5ff95036b4461))

## [0.33.54](https://github.com/cube-js/cube.js/compare/v0.33.53...v0.33.54) (2023-09-12)

### Features

- **cubesql:** `ORDER BY` SQL push down support ([#7115](https://github.com/cube-js/cube.js/issues/7115)) ([49ea3cf](https://github.com/cube-js/cube.js/commit/49ea3cf0721f30da142fb021f860cc56b0a85ab6))

## [0.33.53](https://github.com/cube-js/cube.js/compare/v0.33.52...v0.33.53) (2023-09-08)

### Features

- **cubesql:** Ungrouped SQL push down ([#7102](https://github.com/cube-js/cube.js/issues/7102)) ([4c7fde5](https://github.com/cube-js/cube.js/commit/4c7fde5a96a5db0978b72d0887e533450123e9f7))

## [0.33.51](https://github.com/cube-js/cube/compare/v0.33.50...v0.33.51) (2023-09-06)

### Features

- **cubesql:** Support `inet_server_addr` stub ([9ecb180](https://github.com/cube-js/cube/commit/9ecb180add83a06f5689f530df561f79d441311f))

## [0.33.50](https://github.com/cube-js/cube/compare/v0.33.49...v0.33.50) (2023-09-04)

### Features

- **cubesql:** Support `Date32` to `Timestamp` coercion ([54bdfee](https://github.com/cube-js/cube/commit/54bdfeec01ce9bbdd78b022fcf02687a9dcf0793))

## [0.33.49](https://github.com/cube-js/cube/compare/v0.33.48...v0.33.49) (2023-08-31)

### Features

- **cubesql:** Support multiple values in `SET` ([41af344](https://github.com/cube-js/cube/commit/41af3441bbb1502950a3e34905070f055c783f29))

## [0.33.48](https://github.com/cube-js/cube/compare/v0.33.47...v0.33.48) (2023-08-23)

### Features

- **cubesql:** Ungrouped queries support ([#7056](https://github.com/cube-js/cube/issues/7056)) ([1b5c161](https://github.com/cube-js/cube/commit/1b5c161655de0b055bf55dcd399cd6cde199ef46))

## [0.33.47](https://github.com/cube-js/cube.js/compare/v0.33.46...v0.33.47) (2023-08-15)

### Features

- **cubesql:** Support LIMIT for SQL push down ([1b5c19f](https://github.com/cube-js/cube.js/commit/1b5c19f03331ca4174b37614b8b41cdafc211ad7))

## [0.33.46](https://github.com/cube-js/cube.js/compare/v0.33.45...v0.33.46) (2023-08-14)

### Features

- **cubesql:** Initial SQL push down support for BigQuery, Clickhouse and MySQL ([38467ab](https://github.com/cube-js/cube.js/commit/38467ab7de64803cd51acf4d5fc696938e52f778))

## [0.33.45](https://github.com/cube-js/cube.js/compare/v0.33.44...v0.33.45) (2023-08-13)

### Features

- **cubesql:** CASE WHEN SQL push down ([#7029](https://github.com/cube-js/cube.js/issues/7029)) ([80e4a60](https://github.com/cube-js/cube.js/commit/80e4a609cdb983db0a600d0fff0fd5bfe31652ed))
- **cubesql:** Whole SQL query push down to data sources ([#6629](https://github.com/cube-js/cube.js/issues/6629)) ([0e8a76a](https://github.com/cube-js/cube.js/commit/0e8a76a20cb37e675997f384785dd06e09175113))

## [0.33.28](https://github.com/cube-js/cube/compare/v0.33.27...v0.33.28) (2023-06-19)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.33.26](https://github.com/cube-js/cube/compare/v0.33.25...v0.33.26) (2023-06-14)

### Features

- **schema:** Initial support for jinja templates ([#6704](https://github.com/cube-js/cube/issues/6704)) ([338d1b7](https://github.com/cube-js/cube/commit/338d1b7ed03fc074c06fb028f731c9817ba8d419))

## [0.33.24](https://github.com/cube-js/cube/compare/v0.33.23...v0.33.24) (2023-06-05)

### Features

- **cubesql:** Support `CURRENT_DATE` scalar function ([ec928a6](https://github.com/cube-js/cube/commit/ec928a67f05ed91517b556b701581d8d04370cc7))

## [0.33.23](https://github.com/cube-js/cube/compare/v0.33.22...v0.33.23) (2023-06-01)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.33.11](https://github.com/cube-js/cube/compare/v0.33.10...v0.33.11) (2023-05-22)

### Features

- Improve LangChain support ([15e51bc](https://github.com/cube-js/cube/commit/15e51bcb19ce22c38b71f4685484295fc637e44c))

## [0.33.6](https://github.com/cube-js/cube.js/compare/v0.33.5...v0.33.6) (2023-05-13)

### Bug Fixes

- **cubesql:** Improve NULL comparison with int/bool ([de1be39](https://github.com/cube-js/cube.js/commit/de1be39d07ceec5d87d6c7aef8adb65fbe246ee3))

## [0.33.5](https://github.com/cube-js/cube.js/compare/v0.33.4...v0.33.5) (2023-05-11)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.33.2](https://github.com/cube-js/cube/compare/v0.33.1...v0.33.2) (2023-05-04)

**Note:** Version bump only for package @cubejs-backend/cubesql

# [0.33.0](https://github.com/cube-js/cube.js/compare/v0.32.31...v0.33.0) (2023-05-02)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.32.31](https://github.com/cube-js/cube.js/compare/v0.32.30...v0.32.31) (2023-05-02)

### Features

- **cubesql:** psqlodbc driver support ([fcfc7ea](https://github.com/cube-js/cube.js/commit/fcfc7ea9f8bfd027aa549d139b9ed12b773594ec))

## [0.32.30](https://github.com/cube-js/cube.js/compare/v0.32.29...v0.32.30) (2023-04-28)

### Bug Fixes

- **cubesql:** Resolve Grafana introspection issues ([db32377](https://github.com/cube-js/cube.js/commit/db32377f6e6d45c8c16b12ee7e51fdf1e9687fc9))

## [0.32.28](https://github.com/cube-js/cube/compare/v0.32.27...v0.32.28) (2023-04-19)

### Features

- **cubesql:** Support new Thoughtspot introspection ([a04c83a](https://github.com/cube-js/cube/commit/a04c83a070ddbd2924528ce5e7ecf10c7b4f235c))
- **cubesql:** Support psql's `\list` command ([0b30def](https://github.com/cube-js/cube/commit/0b30def71aa5bb53dbc59ddb6a4b63bd59eda95a))

## [0.32.19](https://github.com/cube-js/cube/compare/v0.32.18...v0.32.19) (2023-04-03)

### Features

- **cubesql:** Support `date_trunc = literal date` filter ([#6376](https://github.com/cube-js/cube/issues/6376)) ([0ef53cb](https://github.com/cube-js/cube/commit/0ef53cb978e8995185a731985944b08f1f24949e))

## [0.32.17](https://github.com/cube-js/cube/compare/v0.32.16...v0.32.17) (2023-03-29)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.32.15](https://github.com/cube-js/cube.js/compare/v0.32.14...v0.32.15) (2023-03-24)

### Bug Fixes

- **cubesql:** Allow any aggregation for number measure as it can be a wildcard ([48f8828](https://github.com/cube-js/cube.js/commit/48f882867b020ca4a0d058ff147e61ef9bea9555))

## [0.32.12](https://github.com/cube-js/cube.js/compare/v0.32.11...v0.32.12) (2023-03-22)

### Bug Fixes

- **cubesql:** Support quicksight AVG Rebase window exprs: Physical plan does not support logical expression SUM(x) PARTITION BY ([#6328](https://github.com/cube-js/cube.js/issues/6328)) ([5a5d7e4](https://github.com/cube-js/cube.js/commit/5a5d7e497f05c69541e04df0a464c85eb9a5f506))

## [0.32.11](https://github.com/cube-js/cube.js/compare/v0.32.10...v0.32.11) (2023-03-21)

### Bug Fixes

- **cubesql:** Ignore timestamps which can't be represented as nanoseconds instead of failing ([e393b06](https://github.com/cube-js/cube.js/commit/e393b0601fb663b03158ea03143a30eb0086ebbf))
- **cubesql:** Quicksight AVG measures support ([#6323](https://github.com/cube-js/cube.js/issues/6323)) ([ada0afd](https://github.com/cube-js/cube.js/commit/ada0afd17b42a54fbecac69b849abf40158991c1))

## [0.32.9](https://github.com/cube-js/cube.js/compare/v0.32.8...v0.32.9) (2023-03-18)

### Bug Fixes

- **cubesql:** Unexpected response from Cube, Field "count" doesn't exist in row ([6bdc91d](https://github.com/cube-js/cube.js/commit/6bdc91d3eaf51dcb25e7321d03b485147511f049))

## [0.32.8](https://github.com/cube-js/cube.js/compare/v0.32.7...v0.32.8) (2023-03-17)

### Bug Fixes

- **cubesql:** Catch error on TcpStream.peer_addr() ([#6300](https://github.com/cube-js/cube.js/issues/6300)) ([d74a1f0](https://github.com/cube-js/cube.js/commit/d74a1f059ce7f1baa9dad5f216e013a3f0f1bc45))
- **cubesql:** Use writable streams with plain objects instead of JSON.stringify pipe for streaming capability ([#6306](https://github.com/cube-js/cube.js/issues/6306)) ([a9b19fa](https://github.com/cube-js/cube.js/commit/a9b19fa1a1a9c2f0710c8058ed797a4b7a48ed7e))

## [0.32.1](https://github.com/cube-js/cube.js/compare/v0.32.0...v0.32.1) (2023-03-03)

### Bug Fixes

- **cubesql:** Replace stream buffering with async implementation ([#6127](https://github.com/cube-js/cube.js/issues/6127)) ([5186d30](https://github.com/cube-js/cube.js/commit/5186d308cedf103b08c8a8140de84984839c710a))

# [0.32.0](https://github.com/cube-js/cube.js/compare/v0.31.69...v0.32.0) (2023-03-02)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.31.64](https://github.com/cube-js/cube.js/compare/v0.31.63...v0.31.64) (2023-02-21)

### Features

- **cubesql:** Remove unexpected clone (reduce memory consumption) ([#6185](https://github.com/cube-js/cube.js/issues/6185)) ([904556b](https://github.com/cube-js/cube.js/commit/904556b83e724e6b55e65afd9dbd077bb6c9ea99))

## [0.31.63](https://github.com/cube-js/cube.js/compare/v0.31.62...v0.31.63) (2023-02-20)

### Bug Fixes

- **cubesql:** `CAST(column AS DATE)` to DateTrunc day ([8f6fbe2](https://github.com/cube-js/cube.js/commit/8f6fbe274fa659870f9a736f4ac0c8e8406c64d0))

## [0.31.60](https://github.com/cube-js/cube.js/compare/v0.31.59...v0.31.60) (2023-02-10)

### Features

- **cubesql:** Redesign member pushdown to support more advanced join… ([#6122](https://github.com/cube-js/cube.js/issues/6122)) ([3bb85e4](https://github.com/cube-js/cube.js/commit/3bb85e492056d73c28b3d006a95e0f9765e6e026))

## [0.31.59](https://github.com/cube-js/cube.js/compare/v0.31.58...v0.31.59) (2023-02-06)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.31.58](https://github.com/cube-js/cube.js/compare/v0.31.57...v0.31.58) (2023-02-02)

### Features

- **cubesql:** Improve catching of panic's reason ([#6107](https://github.com/cube-js/cube.js/issues/6107)) ([c8cf300](https://github.com/cube-js/cube.js/commit/c8cf3007b5bcb4f0362e5e3721eccadf69bcea62))

## [0.31.56](https://github.com/cube-js/cube.js/compare/v0.31.55...v0.31.56) (2023-01-31)

### Bug Fixes

- **cubesql:** Allow Thoughtspot `EXTRACT YEAR AS date` ([22d0ad9](https://github.com/cube-js/cube.js/commit/22d0ad967380b4ece695b567e77a216a16b3bf17))

## [0.31.55](https://github.com/cube-js/cube.js/compare/v0.31.54...v0.31.55) (2023-01-26)

### Bug Fixes

- **cubesql:** Correct Thoughtspot day in quarter offset ([d62079e](https://github.com/cube-js/cube.js/commit/d62079eaadaaa81d9b1e45580b27d7597192263e))

## [0.31.50](https://github.com/cube-js/cube.js/compare/v0.31.49...v0.31.50) (2023-01-21)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.31.48](https://github.com/cube-js/cube.js/compare/v0.31.47...v0.31.48) (2023-01-20)

### Features

- **cubesql:** Postgres protocol - stream support ([#6025](https://github.com/cube-js/cube.js/issues/6025)) ([d5786df](https://github.com/cube-js/cube.js/commit/d5786df63a1f48dec2697a8bb5e8c017c1b13ae4))
- **cubesql:** Streams - cancel query and drop conection handling ([8c585f2](https://github.com/cube-js/cube.js/commit/8c585f24003c768300a31e0ed6774a3a724e54fa))

## [0.31.45](https://github.com/cube-js/cube.js/compare/v0.31.44...v0.31.45) (2023-01-16)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.31.41](https://github.com/cube-js/cube.js/compare/v0.31.40...v0.31.41) (2023-01-13)

### Features

- streaming capabilities ([#5995](https://github.com/cube-js/cube.js/issues/5995)) ([d336c4e](https://github.com/cube-js/cube.js/commit/d336c4eaa3547422484bb003df19dfd4c7be5f96))

## [0.31.40](https://github.com/cube-js/cube.js/compare/v0.31.39...v0.31.40) (2023-01-12)

### Features

- **cubesql:** Support `date_trunc` over column filter ([c9e71e6](https://github.com/cube-js/cube.js/commit/c9e71e6a13f41e2af388b7f91043e4118ba91f40))

## [0.31.39](https://github.com/cube-js/cube.js/compare/v0.31.38...v0.31.39) (2023-01-12)

### Bug Fixes

- **cubesql:** Query cancellation for simple query protocol ([#5987](https://github.com/cube-js/cube.js/issues/5987)) ([aae758f](https://github.com/cube-js/cube.js/commit/aae758f83d45a2572caddfc5f85663e059406c78))

## [0.31.38](https://github.com/cube-js/cube.js/compare/v0.31.37...v0.31.38) (2023-01-11)

### Features

- **cubesql:** Improve memory usage in writting for pg-wire ([#4870](https://github.com/cube-js/cube.js/issues/4870)) ([401fbcf](https://github.com/cube-js/cube.js/commit/401fbcfa1e11a36d65555f7848280f5e60801808))

## [0.31.35](https://github.com/cube-js/cube.js/compare/v0.31.34...v0.31.35) (2023-01-07)

### Features

- **cubesql:** Support `NULLIF` in projection ([129fc58](https://github.com/cube-js/cube.js/commit/129fc580579062be73d362cfa829e3af82f37ad0))
- **cubesql:** Support Thoughtspot starts/ends LIKE exprs ([e6798cc](https://github.com/cube-js/cube.js/commit/e6798cca8f9de33badf34b9cd64c41a2a7e6ce88))

## [0.31.33](https://github.com/cube-js/cube.js/compare/v0.31.32...v0.31.33) (2023-01-03)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.31.32](https://github.com/cube-js/cube.js/compare/v0.31.31...v0.31.32) (2022-12-28)

### Features

- **cubesql:** Allow postprocessing with JOIN below Cube query limit ([56f5399](https://github.com/cube-js/cube.js/commit/56f5399fb37dbb7b388951b1db0be21dda2a94d4))
- **cubesql:** Support `LEFT`, `RIGHT` in projection ([282ad3a](https://github.com/cube-js/cube.js/commit/282ad3ab3106d81a1361f94a499cfc7dc716f3e6))

## [0.31.31](https://github.com/cube-js/cube.js/compare/v0.31.30...v0.31.31) (2022-12-23)

### Bug Fixes

- **cubesql:** Improve Thoughtspot `WHERE IN` support ([6212efe](https://github.com/cube-js/cube.js/commit/6212efe428e504cd8c06797dfaa2b81783b80777))
- **cubesql:** Support Thoughtspot DATEADD queries ([58b5669](https://github.com/cube-js/cube.js/commit/58b566903685ac3d14e78d61dc38c38c43aa5c3c))

## [0.31.30](https://github.com/cube-js/cube.js/compare/v0.31.29...v0.31.30) (2022-12-22)

### Bug Fixes

- **cubesql:** Improve Thoughtspot compatibility ([4d6511a](https://github.com/cube-js/cube.js/commit/4d6511a3d18ea877f06775c1aae154b5665feda0))

## [0.31.28](https://github.com/cube-js/cube.js/compare/v0.31.27...v0.31.28) (2022-12-16)

### Features

- Support `string`, `time` and `boolean` measures ([#5842](https://github.com/cube-js/cube.js/issues/5842)) ([4543ede](https://github.com/cube-js/cube.js/commit/4543edefe5b2432c90bb8530bc6a3c24c5548de3))

## [0.31.25](https://github.com/cube-js/cube.js/compare/v0.31.24...v0.31.25) (2022-12-10)

### Bug Fixes

- **cubesql:** normalize column names in filter node ([#5788](https://github.com/cube-js/cube.js/issues/5788)) ([28aa008](https://github.com/cube-js/cube.js/commit/28aa008d8060173b2af2052577afdc26cc32c36d))

## [0.31.24](https://github.com/cube-js/cube.js/compare/v0.31.23...v0.31.24) (2022-12-09)

### Bug Fixes

- **cubesql:** Support `CAST` in `HAVING` clause ([17ba3e2](https://github.com/cube-js/cube.js/commit/17ba3e212fb801fbffec99ef043b443f6f2a698f))

## [0.31.23](https://github.com/cube-js/cube.js/compare/v0.31.22...v0.31.23) (2022-12-09)

### Features

- **cubesql:** Support `CASE` statements in cube projection ([e7ae68c](https://github.com/cube-js/cube.js/commit/e7ae68c1afcb1152c0248f61ee355f0b30cc9b73))

## [0.31.22](https://github.com/cube-js/cube.js/compare/v0.31.21...v0.31.22) (2022-12-07)

### Bug Fixes

- **cubesql:** Metabase - auto-generated charts for cubes containing string dimensions ([#5728](https://github.com/cube-js/cube.js/issues/5728)) ([72be686](https://github.com/cube-js/cube.js/commit/72be68671faaa4c938374f95cb8cb81578ef4fdb))

## [0.31.20](https://github.com/cube-js/cube.js/compare/v0.31.19...v0.31.20) (2022-12-02)

### Bug Fixes

- **cubesql:** Fix escape symbols in `LIKE` expressions ([5f3cd50](https://github.com/cube-js/cube.js/commit/5f3cd50ea311900adc27ba2b30c72a05a3453a1d))

### Features

- **cubesql:** Support Thoughtspot include filter search ([745fe5d](https://github.com/cube-js/cube.js/commit/745fe5d2806b4c6c9e76d6061aa038892ec7438f))
- **cubesql:** Support ThoughtSpot search filters ([ee0fde4](https://github.com/cube-js/cube.js/commit/ee0fde4798894c619f63cfd87cfc118c7ff1fc78))

## [0.31.18](https://github.com/cube-js/cube.js/compare/v0.31.17...v0.31.18) (2022-11-28)

### Bug Fixes

- **cubesql:** Prevent infinite limit push down ([f26d40a](https://github.com/cube-js/cube.js/commit/f26d40a3b91a2651811cfa622424c6ed2e44dfef))
- **cubesql:** Push down projection to CubeScan with literals ([207616d](https://github.com/cube-js/cube.js/commit/207616d8e51649a78b854b02fc83611472aea715))

### Features

- **cubesql:** Sigma Computing date filters ([404e3f4](https://github.com/cube-js/cube.js/commit/404e3f42ac02be8c0c9eaacb6cf81bb516616001))

## [0.31.14](https://github.com/cube-js/cube.js/compare/v0.31.13...v0.31.14) (2022-11-14)

### Bug Fixes

- **cubesql:** Allow referencing CTEs in UNION ([7f5bc83](https://github.com/cube-js/cube.js/commit/7f5bc8316d6119ffe1703bc1bb41e54586f9d19b))
- **cubesql:** Keep CubeScan literal values relation ([6d3856a](https://github.com/cube-js/cube.js/commit/6d3856acdfeea8d93866a1f36513016a5a04b2e8))

### Features

- **cubesql:** Join Cubes ([#5585](https://github.com/cube-js/cube.js/issues/5585)) ([c687e42](https://github.com/cube-js/cube.js/commit/c687e42f9280f611152f7c154fdf136e6d9ce402))

## [0.31.12](https://github.com/cube-js/cube.js/compare/v0.31.11...v0.31.12) (2022-11-05)

### Features

- **cubesql:** Support Skyvia date granularities ([df69d93](https://github.com/cube-js/cube.js/commit/df69d93e3f0c016d4767e0509ca523b60bc74099))

## [0.31.10](https://github.com/cube-js/cube.js/compare/v0.31.9...v0.31.10) (2022-11-01)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.31.8](https://github.com/cube-js/cube.js/compare/v0.31.7...v0.31.8) (2022-10-30)

### Bug Fixes

- **cubesql:** Count measure type changed from u64 to i64 ([#5535](https://github.com/cube-js/cube.js/issues/5535)) ([f568851](https://github.com/cube-js/cube.js/commit/f568851948cff16ddc53a974d46a77a8698dbdf1))

### Features

- **cubesql:** Support `BOOL_AND`, `BOOL_OR` aggregate functions ([#5533](https://github.com/cube-js/cube.js/issues/5533)) ([a2e6e38](https://github.com/cube-js/cube.js/commit/a2e6e386557bfbf43b2a4907c1fa3aef07ea90f2))
- **cubesql:** Support Sigma Computing number filters ([f2f2abd](https://github.com/cube-js/cube.js/commit/f2f2abdbdafdd4669e1bd223b5b2f50a24c42b86))
- **cubesql:** Thoughspot - count distinct with year and month ([#5450](https://github.com/cube-js/cube.js/issues/5450)) ([d44baad](https://github.com/cube-js/cube.js/commit/d44baad34dab8dbf70aa7c9b011dfe17f93b1375))

## [0.31.7](https://github.com/cube-js/cube.js/compare/v0.31.6...v0.31.7) (2022-10-27)

### Features

- **cubesql:** Support `is null`, `is not null` in SELECT ([d499c47](https://github.com/cube-js/cube.js/commit/d499c47c6cec4ac7b72d26418598bfbe515c6c62))
- **cubesql:** Support Sigma Computing string filters ([d18b971](https://github.com/cube-js/cube.js/commit/d18b9712c4eae1ced2b8f94681e1676d854e99a5))

## [0.31.5](https://github.com/cube-js/cube.js/compare/v0.31.4...v0.31.5) (2022-10-20)

### Bug Fixes

- **cubesql:** Use real database name in Postgres meta layer ([031a90f](https://github.com/cube-js/cube.js/commit/031a90fac068ce35f18823a034f65b486153c726))

### Features

- **cubesql:** Allow `EXTRACT` field to be parsed as a string ([cad1e0b](https://github.com/cube-js/cube.js/commit/cad1e0b4452233866f87d3856823ef81ca117444))

## [0.31.2](https://github.com/cube-js/cube.js/compare/v0.31.1...v0.31.2) (2022-10-08)

### Bug Fixes

- **cubesql:** Handle Panic on simple query executiony ([#5394](https://github.com/cube-js/cube.js/issues/5394)) ([84dc442](https://github.com/cube-js/cube.js/commit/84dc442eb1c42bc3c7b7b03fe365c7c0a948e328))

### Features

- **cubesql:** Support boolean decoding in pg-wire ([#5436](https://github.com/cube-js/cube.js/issues/5436)) ([4fd2ee6](https://github.com/cube-js/cube.js/commit/4fd2ee6cd238161f889896739a00f09e6dc11651))

# [0.31.0](https://github.com/cube-js/cube.js/compare/v0.30.75...v0.31.0) (2022-10-03)

### Bug Fixes

- **cubesql:** Allow derived tables to have a dot in column name ([#5391](https://github.com/cube-js/cube.js/issues/5391)) ([f83009c](https://github.com/cube-js/cube.js/commit/f83009cf193a6313296dddffa45bffa08ec01725))
- **cubesql:** cast strings to timestamp ([#5331](https://github.com/cube-js/cube.js/issues/5331)) ([a706258](https://github.com/cube-js/cube.js/commit/a706258f85faa3f99150127a2c78f885e99e3aaf))
- **cubesql:** Metabase - substring \_\_user ([#5328](https://github.com/cube-js/cube.js/issues/5328)) ([a25c8bf](https://github.com/cube-js/cube.js/commit/a25c8bf3ddad9c589918b91f05df440eb31a2ad4))
- **cubesql:** udf format_type prepared statement fix ([#5260](https://github.com/cube-js/cube.js/issues/5260)) ([307ed1b](https://github.com/cube-js/cube.js/commit/307ed1b6cc9b242e76d48241cb871b36f571f91e))
- **cubesql:** WHERE Lower / Upper in list with multiply items ([#5376](https://github.com/cube-js/cube.js/issues/5376)) ([2269b2b](https://github.com/cube-js/cube.js/commit/2269b2bb41293107f8e8fca118218c56bf3eca53))

### Features

- **cubesql:** Add `float8`, `bool` casts ([b345ade](https://github.com/cube-js/cube.js/commit/b345ade898d6a0ec14e320d66129e985244cddb4))
- **cubesql:** Allow `char_length` function to be used with cubes ([e99344f](https://github.com/cube-js/cube.js/commit/e99344f4e056ef6698f5d92c9e8b79801871a199))
- **cubesql:** Allow filter by exact year (Tableau) ([#5367](https://github.com/cube-js/cube.js/issues/5367)) ([c31e59d](https://github.com/cube-js/cube.js/commit/c31e59d4763e0dd45e96b8e39eb9bcf914370eae))
- **cubesql:** Holistics - in dates list filter ([#5333](https://github.com/cube-js/cube.js/issues/5333)) ([94b6509](https://github.com/cube-js/cube.js/commit/94b650928a81be9ea203e50612ea194d9558b298))
- **cubesql:** Holistics - support range of charts ([#5325](https://github.com/cube-js/cube.js/issues/5325)) ([d16b4c2](https://github.com/cube-js/cube.js/commit/d16b4c2dc0a582d8e28e48a1e5fae3ff2fe7b0de))
- **cubesql:** Support `date_trunc` over column filter with `<=` ([b30d239](https://github.com/cube-js/cube.js/commit/b30d239ae4e00d8d547f0aa65b324f1f0d3af3f1))
- **cubesql:** Support joins with distinct ([#5340](https://github.com/cube-js/cube.js/issues/5340)) ([da4304f](https://github.com/cube-js/cube.js/commit/da4304fef51e33d9c29627d9da92925569943083))

## [0.30.75](https://github.com/cube-js/cube.js/compare/v0.30.74...v0.30.75) (2022-09-22)

### Bug Fixes

- **cubesql:** Allow interval sum chaining ([eabbdc2](https://github.com/cube-js/cube.js/commit/eabbdc27b2a4cd38b4b722ad0c63e2d698868742))

## [0.30.74](https://github.com/cube-js/cube.js/compare/v0.30.73...v0.30.74) (2022-09-20)

### Features

- **cubesql:** Support LOWER(?column) IN (?literal) ([#5319](https://github.com/cube-js/cube.js/issues/5319)) ([2e85182](https://github.com/cube-js/cube.js/commit/2e85182c8863d5aaeda07157fade2c00fa27f4e5))

## [0.30.73](https://github.com/cube-js/cube.js/compare/v0.30.72...v0.30.73) (2022-09-19)

### Features

- **cubesql:** Increase limits for statements/portals/cursors ([#5146](https://github.com/cube-js/cube.js/issues/5146)) ([363b42d](https://github.com/cube-js/cube.js/commit/363b42dd4f48cbef31b1832906ae4069023643ca))

## [0.30.72](https://github.com/cube-js/cube.js/compare/v0.30.71...v0.30.72) (2022-09-18)

### Features

- **cubesql:** starts_with, ends_with, LOWER(?column) = ?literal ([#5310](https://github.com/cube-js/cube.js/issues/5310)) ([321b74f](https://github.com/cube-js/cube.js/commit/321b74f03cc5e929ca18c69d15a0734cfa6613f6))

## [0.30.71](https://github.com/cube-js/cube.js/compare/v0.30.70...v0.30.71) (2022-09-16)

### Features

- **cubesql:** Holistics - string not contains filter ([#5307](https://github.com/cube-js/cube.js/issues/5307)) ([3e563db](https://github.com/cube-js/cube.js/commit/3e563db34b60bc19016b4e3769d96bcdf5a4e42b))
- **cubesql:** Support filtering date within one granularity unit ([427e846](https://github.com/cube-js/cube.js/commit/427e8460e749c1a32f4f9166e19621bc11bee61c))
- **cubesql:** Support startsWith/endsWidth filters (QuickSight) ([#5302](https://github.com/cube-js/cube.js/issues/5302)) ([867279a](https://github.com/cube-js/cube.js/commit/867279abe91b10f61fefaae2cc2578180e1c2f1f))

## [0.30.69](https://github.com/cube-js/cube.js/compare/v0.30.68...v0.30.69) (2022-09-13)

### Bug Fixes

- **cubesql:** LOWER(\_\_user) IN (?literal) (ThoughtSpot) ([#5292](https://github.com/cube-js/cube.js/issues/5292)) ([0565a16](https://github.com/cube-js/cube.js/commit/0565a16c62cad0c854c59ea4e8f8a6c918883d67))

### Features

- **cubesql:** Holistics - support range of charts ([#5281](https://github.com/cube-js/cube.js/issues/5281)) ([f52c682](https://github.com/cube-js/cube.js/commit/f52c6827b3fc29f0588d62974eef6323ff32acae))
- **cubesql:** Support `pg_catalog.pg_stats` meta layer table ([f2a1da2](https://github.com/cube-js/cube.js/commit/f2a1da2666852d33c7583cecf696a6a130b00a99))

## [0.30.68](https://github.com/cube-js/cube.js/compare/v0.30.67...v0.30.68) (2022-09-09)

### Features

- **cubesql:** Support IN for \_\_user (ThoughtSpot) ([#5269](https://github.com/cube-js/cube.js/issues/5269)) ([d9aaefc](https://github.com/cube-js/cube.js/commit/d9aaefc65c14bffe87a0676a4e6222d08caf538d))
- **cubesql:** Support interval multiplication ([bb2e82a](https://github.com/cube-js/cube.js/commit/bb2e82ac46a877f8c75996b42bd73bc5c35102ef))

## [0.30.67](https://github.com/cube-js/cube.js/compare/v0.30.66...v0.30.67) (2022-09-09)

### Bug Fixes

- **cubesql:** Show `MEASURE()` instead of `NUMBER()` in measure aggregation type doesn't match error. ([#5268](https://github.com/cube-js/cube.js/issues/5268)) ([a76059e](https://github.com/cube-js/cube.js/commit/a76059eff93c0098ef812f9d09fe996489126cd5))

### Features

- **cubesql:** Holistics - GROUP BY dates support ([#5264](https://github.com/cube-js/cube.js/issues/5264)) ([7217950](https://github.com/cube-js/cube.js/commit/7217950c9f0954e23f37efd3609e5eff4d125620))

## [0.30.65](https://github.com/cube-js/cube.js/compare/v0.30.64...v0.30.65) (2022-09-07)

### Features

- **cubesql:** Holistics - support in subquery introspection query ([#5248](https://github.com/cube-js/cube.js/issues/5248)) ([977a251](https://github.com/cube-js/cube.js/commit/977a251fa869344dedebc1315635f2ff9e7de07b))
- **cubesql:** Holistics - support left join introspection query ([#5249](https://github.com/cube-js/cube.js/issues/5249)) ([455d31f](https://github.com/cube-js/cube.js/commit/455d31f531c783c5f621303a1fa4bce01e1fac61))

## [0.30.64](https://github.com/cube-js/cube.js/compare/v0.30.63...v0.30.64) (2022-09-07)

### Bug Fixes

- **cubesql:** select column with the same name as table ([#5235](https://github.com/cube-js/cube.js/issues/5235)) ([1a20f6f](https://github.com/cube-js/cube.js/commit/1a20f6fe2772d36f693821f19ab43e830c198651))

### Features

- **cubesql:** Holistics - support schema privilege query ([#5240](https://github.com/cube-js/cube.js/issues/5240)) ([ae59ddf](https://github.com/cube-js/cube.js/commit/ae59ddffc9fa5b47d00efca6f03f11e5533e5b89))
- **cubesql:** Support nullif with scalars ([#5241](https://github.com/cube-js/cube.js/issues/5241)) ([138dcae](https://github.com/cube-js/cube.js/commit/138dcae3ab9c763f4446f684da101421219abe5b))
- **cubesql:** Support yearly granularity (ThoughtSpot) ([#5236](https://github.com/cube-js/cube.js/issues/5236)) ([416ddd8](https://github.com/cube-js/cube.js/commit/416ddd87042c7fb805b7b9c3c4b6a0bb53552236))

## [0.30.62](https://github.com/cube-js/cube.js/compare/v0.30.61...v0.30.62) (2022-09-02)

### Features

- **cubesql:** Superset - serverside paging ([#5204](https://github.com/cube-js/cube.js/issues/5204)) ([dfd695d](https://github.com/cube-js/cube.js/commit/dfd695d2f09ea00b4a0b8ef816a597b5c3986ce6))
- **cubesql:** Support dow granularity (ThoughtSpot) ([#5172](https://github.com/cube-js/cube.js/issues/5172)) ([0919e40](https://github.com/cube-js/cube.js/commit/0919e40c9283db45b3856fcf81993d985cbfc7ac))
- **cubesql:** Support doy granularity (ThoughtSpot) ([#5232](https://github.com/cube-js/cube.js/issues/5232)) ([be26775](https://github.com/cube-js/cube.js/commit/be2677535ae1b7bd231530c3bb9266016f9b4c8b))

## [0.30.61](https://github.com/cube-js/cube.js/compare/v0.30.60...v0.30.61) (2022-09-01)

### Features

- **cubesql:** Eliminate literal filter (true or true = true) ([#5142](https://github.com/cube-js/cube.js/issues/5142)) ([7a6f8f9](https://github.com/cube-js/cube.js/commit/7a6f8f9ae91cd314f5c2699dadbd2c5b79c1e73e))
- **cubesql:** Improve support (formats) for TO_TIMESTAMP function ([#5218](https://github.com/cube-js/cube.js/issues/5218)) ([044c3e1](https://github.com/cube-js/cube.js/commit/044c3e1585479b59c391d31c9783ee46908bbcc3))
- **cubesql:** Push down limit through projection ([#5206](https://github.com/cube-js/cube.js/issues/5206)) ([3c6ff7d](https://github.com/cube-js/cube.js/commit/3c6ff7d6eddc925567234a5ec94606eb09970b33))
- **cubesql:** Support `LOCALTIMESTAMP` ([0089a65](https://github.com/cube-js/cube.js/commit/0089a65ae86019df159c0d9dbe5323ebc38c7172))

## [0.30.59](https://github.com/cube-js/cube.js/compare/v0.30.58...v0.30.59) (2022-08-26)

### Bug Fixes

- **cubesql:** Persist dbname from connection for pg-wire ([#5165](https://github.com/cube-js/cube.js/issues/5165)) ([6bdf5df](https://github.com/cube-js/cube.js/commit/6bdf5df270d00840b1c49e0733c1a5cf8bbc18e3))

## [0.30.58](https://github.com/cube-js/cube.js/compare/v0.30.57...v0.30.58) (2022-08-25)

### Features

- **cubesql:** Support qtr granularity in DateTrunc for analytics queries ([#5159](https://github.com/cube-js/cube.js/issues/5159)) ([ce13846](https://github.com/cube-js/cube.js/commit/ce1384631f73a6405fd3c82502f0cbb24154259e))

## [0.30.57](https://github.com/cube-js/cube.js/compare/v0.30.56...v0.30.57) (2022-08-25)

### Features

- **cubesql:** DiscardAll in QE - clear prepared statements ([b6fb724](https://github.com/cube-js/cube.js/commit/b6fb72407d3c414175b1831d4d07ae0cb6f8ed53))
- **cubesql:** Support new Superset version ([#5154](https://github.com/cube-js/cube.js/issues/5154)) ([148a062](https://github.com/cube-js/cube.js/commit/148a062530cb399cb96da84821928f5e39e871ce))
- **cubesql:** Support pg_catalog.pg_prepared_statements table ([e03e557](https://github.com/cube-js/cube.js/commit/e03e55709598cc5b0d8efcfec281be76c7caa351))

## [0.30.56](https://github.com/cube-js/cube.js/compare/v0.30.55...v0.30.56) (2022-08-23)

### Bug Fixes

- **cubesql:** array_upper && array_lower UDFs return type fix ([#5136](https://github.com/cube-js/cube.js/issues/5136)) ([9451a86](https://github.com/cube-js/cube.js/commit/9451a86f853c87aef0992c568f5c0a44a0b8610d))
- **cubesql:** Normalize error messsage ([ac00acb](https://github.com/cube-js/cube.js/commit/ac00acbfa71285d5eb423edf42a5a45eb3792c63))

### Features

- Support usage of CTE (with realiasing) ([e64db05](https://github.com/cube-js/cube.js/commit/e64db05c7084568acb1a28b9dada56dd75d35bba))
- **cubesql:** Disable optimizers for analytics queries ([b710c95](https://github.com/cube-js/cube.js/commit/b710c95529fba4ccf853d00436c8ba6ce48a818b))

## [0.30.54](https://github.com/cube-js/cube.js/compare/v0.30.53...v0.30.54) (2022-08-19)

### Features

- **cubesql:** Catch panic on Portal (DF.stream) - return error to the client ([a80cdc7](https://github.com/cube-js/cube.js/commit/a80cdc7a8ed9c66d1ad8d5c7e261e23b10d6d5d0))

## [0.30.52](https://github.com/cube-js/cube.js/compare/v0.30.51...v0.30.52) (2022-08-18)

### Bug Fixes

- **cubesql:** SUM(CAST(rows.col AS Decimal(38, 10))) expression can't be coerced in Power BI ([#5107](https://github.com/cube-js/cube.js/issues/5107)) ([0037fb4](https://github.com/cube-js/cube.js/commit/0037fb416c68b47e055e846d724ce276b1675879))
- **cubesql:** Type coercion for CASE WHEN THEN ([88b124d](https://github.com/cube-js/cube.js/commit/88b124d2d0549e4d55678fddef73f4a5796c4ada))

### Features

- **cubesql:** Support Redshift connection (ThoughtSpot) ([b244d59](https://github.com/cube-js/cube.js/commit/b244d595487503c3597dc63b6aedca85170e7424))

## [0.30.48](https://github.com/cube-js/cube.js/compare/v0.30.47...v0.30.48) (2022-08-14)

### Features

- **cubesql:** Cubes JOIN support ([#5099](https://github.com/cube-js/cube.js/issues/5099)) ([4995476](https://github.com/cube-js/cube.js/commit/4995476e974d4f5ea732e24de6e19fdcd3e308a2))

## [0.30.47](https://github.com/cube-js/cube.js/compare/v0.30.46...v0.30.47) (2022-08-12)

### Features

- **cubesql:** Datastudio - string startWith filter support ([#5093](https://github.com/cube-js/cube.js/issues/5093)) ([3c21986](https://github.com/cube-js/cube.js/commit/3c21986044732c218ff0c04798cd3bc2fbc6b43c))
- **cubesql:** Metabase v0.44 support ([#5097](https://github.com/cube-js/cube.js/issues/5097)) ([1b2f53b](https://github.com/cube-js/cube.js/commit/1b2f53b8bbff655fa418763534e0ac88f896afcf))
- **cubesql:** Support COALESCE function ([199c775](https://github.com/cube-js/cube.js/commit/199c775d607a70d26c9afa473f397b0f3d1c6e20))
- **cubesql:** Support REGEXP_SUBSTR function (Redshift) ([#5090](https://github.com/cube-js/cube.js/issues/5090)) ([3c9f024](https://github.com/cube-js/cube.js/commit/3c9f024226f6e29f2bedabe7fb88d3fb124e55c7))

## [0.30.46](https://github.com/cube-js/cube.js/compare/v0.30.45...v0.30.46) (2022-08-10)

### Features

- **cubesql:** Datastudio - aggr by month and day support ([#5025](https://github.com/cube-js/cube.js/issues/5025)) ([da3ed59](https://github.com/cube-js/cube.js/commit/da3ed59910b968cf0523f49eea7758f33d427b3e))
- **cubesql:** Datastudio - between dates filter support ([#5022](https://github.com/cube-js/cube.js/issues/5022)) ([20f7d64](https://github.com/cube-js/cube.js/commit/20f7d649574a522c380bfd069667f929855bd6d1))
- **cubesql:** Datastudio - Min/Max datetime aggregation support ([#5021](https://github.com/cube-js/cube.js/issues/5021)) ([7cf1f75](https://github.com/cube-js/cube.js/commit/7cf1f7520956304a74e79b7acc54b61f907d0706))
- **cubesql:** Support DEALLOCATE in pg-wire ([06b6476](https://github.com/cube-js/cube.js/commit/06b647687afa37fcca075e018910049ad0ac0883))

## [0.30.45](https://github.com/cube-js/cube.js/compare/v0.30.44...v0.30.45) (2022-08-05)

### Features

- **cubesql:** Support binary bitwise operators (>>, <<) ([7363879](https://github.com/cube-js/cube.js/commit/7363879184395b3c499f9b678da7152362226ea0))
- **cubesql:** Support svv_tables table (Redshift) ([#5060](https://github.com/cube-js/cube.js/issues/5060)) ([d3ed3ac](https://github.com/cube-js/cube.js/commit/d3ed3aca798d41fe4e1919c9fde2f7610435168c))

## [0.30.44](https://github.com/cube-js/cube.js/compare/v0.30.43...v0.30.44) (2022-08-01)

### Bug Fixes

- **cubesql:** Ignore IO's UnexpectedEof|BrokenPipe on handling error ([98deb73](https://github.com/cube-js/cube.js/commit/98deb7362bf772816af88173e6669bf486c328a9))

## [0.30.43](https://github.com/cube-js/cube.js/compare/v0.30.42...v0.30.43) (2022-07-28)

### Bug Fixes

- **cubesq:** Ignore BrokenPipe/UnexpectedEOF as error in pg-wire ([4ec01d2](https://github.com/cube-js/cube.js/commit/4ec01d269f2216f74b841ebe2fd96d3b8597fdcc))

### Features

- **cubesql:** Security Context switching (Row Access) ([731e1ab](https://github.com/cube-js/cube.js/commit/731e1ab6d9362fb9a1857f5276e22a565f79781c))

## [0.30.42](https://github.com/cube-js/cube.js/compare/v0.30.41...v0.30.42) (2022-07-27)

### Features

- **cubesql:** Metabase - support Summarize by week of year ([#5000](https://github.com/cube-js/cube.js/issues/5000)) ([37589a9](https://github.com/cube-js/cube.js/commit/37589a9e58b0c8f14922041432647b814759f22a))

## [0.30.38](https://github.com/cube-js/cube.js/compare/v0.30.37...v0.30.38) (2022-07-25)

### Features

- **cubesql:** Define standard_conforming_strings (SQLAlchemy compatibility) ([8fbc046](https://github.com/cube-js/cube.js/commit/8fbc0467c2e3e37fa9c4b320630dc9200884f3ee)), closes [#L2994](https://github.com/cube-js/cube.js/issues/L2994)
- **cubesql:** Support Cast(expr as Regclass) ([e3cafe4](https://github.com/cube-js/cube.js/commit/e3cafe4a0a291d61545e8855425b8755f3629a4e))
- **cubesql:** Support for new introspection query in SQLAlchemy ([0dbc9e6](https://github.com/cube-js/cube.js/commit/0dbc9e6551016d12155bba27a57b9a17e13dbd02)), closes [#L3381](https://github.com/cube-js/cube.js/issues/L3381)
- **cubesql:** Support pg_catalog.pg_sequence table ([fe057bf](https://github.com/cube-js/cube.js/commit/fe057bf256b8744a9c3f407908808cefa6cd6d8c))

## [0.30.37](https://github.com/cube-js/cube.js/compare/v0.30.36...v0.30.37) (2022-07-20)

### Bug Fixes

- **cubesql:** Correct UDTF behavior with no batch sections ([f52c89a](https://github.com/cube-js/cube.js/commit/f52c89a1baedd9e5a259b663f9427e02ade9fb10))

### Features

- **cubesql:** Add `pg_constraint` pg_type ([e9beb5f](https://github.com/cube-js/cube.js/commit/e9beb5fd875e8f2d181aec45035849e503a61e6b))

## [0.30.36](https://github.com/cube-js/cube.js/compare/v0.30.35...v0.30.36) (2022-07-18)

### Features

- **cubesql:** Metabase - support between numbers queries ([#4916](https://github.com/cube-js/cube.js/issues/4916)) ([52a34fd](https://github.com/cube-js/cube.js/commit/52a34fd563aed43448908e9c5efb3fd55d82de74))
- **cubesql:** Metabase - support Summarize's Bins ([#4926](https://github.com/cube-js/cube.js/issues/4926)) ([8fcdf1a](https://github.com/cube-js/cube.js/commit/8fcdf1a0d1730dbab2871ed6acb99d5added8df1))
- **cubesql:** Metabase string contains / not contains filters ([#4922](https://github.com/cube-js/cube.js/issues/4922)) ([e5abc09](https://github.com/cube-js/cube.js/commit/e5abc09747c5b7d1855b373236b8c682ce278710))
- **cubesql:** Support `has_schema_privilege` UDF ([7ba3148](https://github.com/cube-js/cube.js/commit/7ba3148532568da23934e60abf5919f0e85c8956))
- **cubesql:** Support `pg_catalog.pg_statio_user_tables` meta table ([a4d9050](https://github.com/cube-js/cube.js/commit/a4d9050f02b14a04374520256dabee29e5a4c226))
- **cubesql:** Support `pg_total_relation_size` UDF ([cfca8ee](https://github.com/cube-js/cube.js/commit/cfca8eeec3bd83ba343dbeadfb2ad063d8058ec7))
- **cubesql:** Support minus, multiply, division for binary expression in projection ([#4899](https://github.com/cube-js/cube.js/issues/4899)) ([1fc653b](https://github.com/cube-js/cube.js/commit/1fc653bd6cd83d6d023b51cf4141f9649e6b00da))

## [0.30.35](https://github.com/cube-js/cube.js/compare/v0.30.34...v0.30.35) (2022-07-14)

### Bug Fixes

- **cubesql:** Binary operations with dates and intervals ([#4908](https://github.com/cube-js/cube.js/issues/4908)) ([a2a0cba](https://github.com/cube-js/cube.js/commit/a2a0cba9c3ea0507bd81c684d120d897579f2b90))

### Features

- **cubesql:** Metabase - datetime filters with 'starting from' flag support ([#4882](https://github.com/cube-js/cube.js/issues/4882)) ([4cc01f1](https://github.com/cube-js/cube.js/commit/4cc01f1750b141ad851081efafb1833133420885))
- **cubesql:** Support `PREPARE` queries in pg-wire ([#4906](https://github.com/cube-js/cube.js/issues/4906)) ([2e2ae63](https://github.com/cube-js/cube.js/commit/2e2ae6347692ae5ae77fcf6f921c97b5c5bd10f1))

## [0.30.34](https://github.com/cube-js/cube.js/compare/v0.30.33...v0.30.34) (2022-07-12)

### Features

- **cubesql:** Metabase - BETWEEN filters support ([#4852](https://github.com/cube-js/cube.js/issues/4852)) ([b191120](https://github.com/cube-js/cube.js/commit/b19112079f0f9a51d6703e37afaa121d09ce31e4))
- **cubesql:** Metabase - filters with relative dates support ([#4851](https://github.com/cube-js/cube.js/issues/4851)) ([423be2f](https://github.com/cube-js/cube.js/commit/423be2f33d40ccd5681c47201586ac93944ac9dd))
- **cubesql:** Support Extract(DAY/DOW), Binary (?expr + ?literal_expr) for rewriting (Metabase) ([#4887](https://github.com/cube-js/cube.js/issues/4887)) ([2565705](https://github.com/cube-js/cube.js/commit/2565705fcff6a3d3dc4ff5ac2dcd819d8ad040db))
- **cubesql:** Support Substring for rewriting (Metabase) ([#4881](https://github.com/cube-js/cube.js/issues/4881)) ([8fadebd](https://github.com/cube-js/cube.js/commit/8fadebd7670e9f461a16e51e5114812933722ddd))

## [0.30.32](https://github.com/cube-js/cube.js/compare/v0.30.31...v0.30.32) (2022-07-07)

### Bug Fixes

- **cubesql:** Correct portal pagination (use PortalSuspended) in pg-wire ([#4872](https://github.com/cube-js/cube.js/issues/4872)) ([63aad19](https://github.com/cube-js/cube.js/commit/63aad191ea2be58291b0ce8709e1352a62cbd8a4))

### Features

- **cubesql:** Support grant tables (columns, tables) ([a3d9493](https://github.com/cube-js/cube.js/commit/a3d949324e1ac879606b45faed4812b30b07173b))

## [0.30.31](https://github.com/cube-js/cube.js/compare/v0.30.30...v0.30.31) (2022-07-07)

### Features

- **cubesql:** Initial support for canceling queries in pg-wire ([#4847](https://github.com/cube-js/cube.js/issues/4847)) ([bce0f99](https://github.com/cube-js/cube.js/commit/bce0f994d59a48f221cce3d21e3c2f3244e5f3a1))

## [0.30.30](https://github.com/cube-js/cube.js/compare/v0.30.29...v0.30.30) (2022-07-05)

### Bug Fixes

- **cubesql:** Invalid argument error: all columns in a record batch must have the same length ([895f8cf](https://github.com/cube-js/cube.js/commit/895f8cf301a951907aa4cd3ea190ea1cfeb3be73))

### Features

- **cubesql:** Superset ILIKE support for Search all filter options feature ([2532040](https://github.com/cube-js/cube.js/commit/2532040792faa9ed0a151d85cead1c1bd425d3ce))
- **cubesql:** Support for metabase literal queries ([#4843](https://github.com/cube-js/cube.js/issues/4843)) ([6d45d55](https://github.com/cube-js/cube.js/commit/6d45d558e0c58c37c515d07cae367eed5624cb3a))
- **cubesql:** Support Interval type for pg-wire ([4c8a82c](https://github.com/cube-js/cube.js/commit/4c8a82caf3b64c295bf7606e6a694f6cda50491c))

## [0.30.29](https://github.com/cube-js/cube.js/compare/v0.30.28...v0.30.29) (2022-07-01)

### Bug Fixes

- **cubesql:** Can't find rewrite due to timeout reached for bigger ORDER BY queries ([b765838](https://github.com/cube-js/cube.js/commit/b765838ff6c27ae34272feec00f1b60e7932b2c7))

### Features

- **cubesql:** Initial support for DBeaver ([#4831](https://github.com/cube-js/cube.js/issues/4831)) ([0a63152](https://github.com/cube-js/cube.js/commit/0a6315210fd7115f4649ec12a68a2d9b1479a23f))
- **cubesql:** Send parameters at once (initial handshake) for pg-wire ([#4812](https://github.com/cube-js/cube.js/issues/4812)) ([645253f](https://github.com/cube-js/cube.js/commit/645253f9b21ef08f7fc908e6577878f97b3ef6b0))
- **cubesql:** Support Date type in pg-wire (Date32, Date64) ([d0d08cf](https://github.com/cube-js/cube.js/commit/d0d08cf8ee848903a3b49849cace34046371a90f))
- **pg-srv:** Introduce ToProtocolValue trait (encoding) ([#4818](https://github.com/cube-js/cube.js/issues/4818)) ([4e35aee](https://github.com/cube-js/cube.js/commit/4e35aeec993cdeecab9d64fdb0392c33c35913e4))

## [0.30.28](https://github.com/cube-js/cube.js/compare/v0.30.27...v0.30.28) (2022-06-27)

### Bug Fixes

- **cubesql:** Correct sync behaviour for extended query in pg-wire ([#4815](https://github.com/cube-js/cube.js/issues/4815)) ([ee1362f](https://github.com/cube-js/cube.js/commit/ee1362f19fe1c36569109fc474c86f7ac9292ee5))

## [0.30.27](https://github.com/cube-js/cube.js/compare/v0.30.26...v0.30.27) (2022-06-24)

### Bug Fixes

- **cubesql:** Correct TransactionStatus for Sync in pg-wire ([90c6265](https://github.com/cube-js/cube.js/commit/90c62658fe076060161e6384e0b3dcc8e7e94dd4))
- **cubesql:** Return error on execute for unknown portal in pg-wire ([0b87261](https://github.com/cube-js/cube.js/commit/0b872614f30f5fd9b22c88916ad4edba604f8d02))
- **cubesql:** thread 'tokio-runtime-worker' panicked at 'called `Option::unwrap()` on a `None` value' in case of MEASURE() called on a dimension ([5d62c5a](https://github.com/cube-js/cube.js/commit/5d62c5af1562696ccb192c800ed2047b8345f8f8))

### Features

- **cubesql:** Metabase interval date range filter support ([#4763](https://github.com/cube-js/cube.js/issues/4763)) ([221715a](https://github.com/cube-js/cube.js/commit/221715adee2876585c639e8918dc0f171ad91a86))
- **cubesql:** Support Numeric type (text + binary) in pg-wire ([db7ec5c](https://github.com/cube-js/cube.js/commit/db7ec5c2d0a726b99daf014a70cdee8c15d3721b))
- **cubesql:** Workaround for Metabase introspection query ([ee7b3cf](https://github.com/cube-js/cube.js/commit/ee7b3cfd7401882bf802d668e5709e4f02c64be3))

## [0.30.26](https://github.com/cube-js/cube.js/compare/v0.30.25...v0.30.26) (2022-06-20)

### Features

- **cubesql:** Correct implementation for placeholder binder/finder in pg-wire ([fa018bd](https://github.com/cube-js/cube.js/commit/fa018bd62fd0d7f66c8aa0b68b43cc37d73d65ac))
- **cubesql:** Replace timestamptz CAST with timestamp ([9e7c1bd](https://github.com/cube-js/cube.js/commit/9e7c1bd69adae367a65f77339087194e7e1bc5fe))
- **cubesql:** Support Int8 for Bind + binary in pg-wire ([f28fbd5](https://github.com/cube-js/cube.js/commit/f28fbd5049e0a72adbb0f078e45728d60b481ca2))
- **cubesql:** Support placeholders in `WITH` and `LIMIT` ([#4768](https://github.com/cube-js/cube.js/issues/4768)) ([d444c0f](https://github.com/cube-js/cube.js/commit/d444c0fda31b3cdf824e85c3f03d76d8a3f47211))
- **cubesql:** Workaround CTEs with subqueries (Sigma) ([#4767](https://github.com/cube-js/cube.js/issues/4767)) ([d99a02f](https://github.com/cube-js/cube.js/commit/d99a02f508418c9a054977572da0985f627acfc3))

## [0.30.25](https://github.com/cube-js/cube.js/compare/v0.30.24...v0.30.25) (2022-06-16)

### Features

- logging cubesql queries errors ([#4550](https://github.com/cube-js/cube.js/issues/4550)) ([10021c3](https://github.com/cube-js/cube.js/commit/10021c34f28348183fd30584d8bb97a97103b91e))
- **cubesql:** PowerBI support for wrapped queries ([#4752](https://github.com/cube-js/cube.js/issues/4752)) ([fc129d4](https://github.com/cube-js/cube.js/commit/fc129d4364ea89ea32aa903cda9499133959fdbe))

## [0.30.20](https://github.com/cube-js/cube.js/compare/v0.30.19...v0.30.20) (2022-06-11)

### Bug Fixes

- **cubesql:** Send `Empty Query` message on empty query ([88e966d](https://github.com/cube-js/cube.js/commit/88e966d12e31e6277ac02bf9a1b44cd7c8722311))

### Features

- **cubesql:** Support pg_catalog.pg_roles table ([eed0727](https://github.com/cube-js/cube.js/commit/eed0727fe70b9fddfaddf8c32821fc721c911ae8))
- **cubesql:** Support pg_my_temp_schema, pg_is_other_temp_schema UDFs ([c843491](https://github.com/cube-js/cube.js/commit/c843491c834204231424a21bc8a89b18336cc68a))

## [0.30.18](https://github.com/cube-js/cube.js/compare/v0.30.17...v0.30.18) (2022-06-10)

### Bug Fixes

- **cubesql:** Simple query: fetch in pg-wire (ODBC) ([fc7c0e0](https://github.com/cube-js/cube.js/commit/fc7c0e0f46000c68a64a1c6d1c635a56ab84d51e))

## [0.30.17](https://github.com/cube-js/cube.js/compare/v0.30.16...v0.30.17) (2022-06-09)

### Bug Fixes

- **cubesql:** Simple query: commit/rollback in pg-wire ([#4743](https://github.com/cube-js/cube.js/issues/4743)) ([3e03870](https://github.com/cube-js/cube.js/commit/3e03870545fb916d434a610d69c0a56a597d7e70))

### Features

- **cubesql:** Add Postgres `pg_database` meta layer table ([64e65eb](https://github.com/cube-js/cube.js/commit/64e65eb622241f77c150dbfca687186d05f3432e))
- **cubesql:** add support public compount identifier in filters ([#4742](https://github.com/cube-js/cube.js/issues/4742)) ([74aaef6](https://github.com/cube-js/cube.js/commit/74aaef6c114e5ab4918d54c215b0ed05c27999a4))
- **cubesql:** Workarounds for Tableau Desktop (ODBC) ([951c4b5](https://github.com/cube-js/cube.js/commit/951c4b5c807c28818b76aa6fc26880b4654bff0a))

## [0.30.16](https://github.com/cube-js/cube.js/compare/v0.30.15...v0.30.16) (2022-06-08)

### Bug Fixes

- **cubesql:** Allow binary encoding for all types in pg-wire ([d456745](https://github.com/cube-js/cube.js/commit/d4567451c40c168076ef86ed055052f5490723c4))
- **cubesql:** TIMESTAMP/TZ was wrong in some BIs (pg-wire) ([dfdb5ff](https://github.com/cube-js/cube.js/commit/dfdb5ffe611d0978258a5ae3eb3354366cd1f346))

## [0.30.14](https://github.com/cube-js/cube.js/compare/v0.30.13...v0.30.14) (2022-06-06)

### Features

- **cubesql:** Auto-closing hold cursos on transaction end (simple query) ([79725ec](https://github.com/cube-js/cube.js/commit/79725ec3b02abde6d9cf3f5d3e45e60518a8386f))
- **cubesql:** cast DECIMAL with default precision and scale ([#4709](https://github.com/cube-js/cube.js/issues/4709)) ([771d179](https://github.com/cube-js/cube.js/commit/771d1797f4084fff68f0291c55a37b25b32fb5e2))
- **cubesql:** Support CAST for name, int2/4/8 ([#4711](https://github.com/cube-js/cube.js/issues/4711)) ([36fe891](https://github.com/cube-js/cube.js/commit/36fe891fd102c165eb28b0c5561151934751f143))
- **cubesql:** Support CLOSE [name | ALL] (cursors) for pg-wire ([#4712](https://github.com/cube-js/cube.js/issues/4712)) ([91048bd](https://github.com/cube-js/cube.js/commit/91048bd48ddf755b436f41a1bdfff8b24d4bf5f5))
- **cubesql:** Support Metabase pg_type introspection query ([2401dbf](https://github.com/cube-js/cube.js/commit/2401dbf9e5b5de75c5f7cf31e3586135a0a016e5))

## [0.30.13](https://github.com/cube-js/cube.js/compare/v0.30.12...v0.30.13) (2022-06-05)

### Features

- **cubesql:** PowerBI is not empty filter ([e31ffdc](https://github.com/cube-js/cube.js/commit/e31ffdcd762236fb54d454ede7e892acb54bdcee))

## [0.30.11](https://github.com/cube-js/cube.js/compare/v0.30.10...v0.30.11) (2022-06-03)

### Bug Fixes

- **cubesql:** array_lower, array_upper - correct behaviour ([#4677](https://github.com/cube-js/cube.js/issues/4677)) ([a3f29d4](https://github.com/cube-js/cube.js/commit/a3f29d4df9fc85e53406101bb73b7a7281a60846))

### Features

- **cubesql:** Add `pg_catalog.pg_matviews` meta layer table ([2fbc5f4](https://github.com/cube-js/cube.js/commit/2fbc5f43de312a85967dbd8be79bd92ee04141a7))
- **cubesql:** PowerBI contains filter support ([#4646](https://github.com/cube-js/cube.js/issues/4646)) ([3cbd753](https://github.com/cube-js/cube.js/commit/3cbd753b47dc1a20f3fede11bf0c01b784504869))
- **cubesql:** Support `[NOT] ILIKE` operator ([96b05c8](https://github.com/cube-js/cube.js/commit/96b05c843588aa96a935e5491667d77b3f456b82))
- **cubesql:** Support ArrayIndex for scalars ([419689e](https://github.com/cube-js/cube.js/commit/419689e7d341e287596455d5c94b8225d627798b))

## [0.30.10](https://github.com/cube-js/cube.js/compare/v0.30.9...v0.30.10) (2022-06-01)

### Bug Fixes

- **cubesql:** Handle `Flush` pg-wire message ([f779e75](https://github.com/cube-js/cube.js/commit/f779e75fb4e6ba5a12d7b751c5f53313d33753bc))
- **cubesql:** Store description on Portal in Finished state ([f5f6566](https://github.com/cube-js/cube.js/commit/f5f65663cb01fdbc222e38ec8d3fb6813d5466ae))

### Features

- **cubesql:** information_schema.constraint_column_usage meta table ([1fe8312](https://github.com/cube-js/cube.js/commit/1fe83127b2601bf7ef9f3b63ff63b4026958e8c8))
- **cubesql:** information_schema.views meta table ([490d721](https://github.com/cube-js/cube.js/commit/490d721b4bcd90ae059996bdac177d707935f58e))
- **cubesql:** Support ANY expressions ([77e0672](https://github.com/cube-js/cube.js/commit/77e06727a2a4039d7297538d9bace9498a0fc1a2))
- **cubesql:** Support current_database(), current_schema(), current_user for pg-wire ([a18f68c](https://github.com/cube-js/cube.js/commit/a18f68c8a6538c38c8985b512996a9fec2292da2))
- **cubesql:** Support string for NULLIF (metabase pg_class query) ([#4638](https://github.com/cube-js/cube.js/issues/4638)) ([ef962e7](https://github.com/cube-js/cube.js/commit/ef962e71fe9955c359a044ea83736cac1748c4a4))
- Initial support for FETCH/DECLARE (cursors) for simple query in pg-wire ([#4601](https://github.com/cube-js/cube.js/issues/4601)) ([b160773](https://github.com/cube-js/cube.js/commit/b160773d9a208c2b794a34e6e36f4ce73a83a53e))

## [0.30.9](https://github.com/cube-js/cube.js/compare/v0.30.8...v0.30.9) (2022-05-31)

### Bug Fixes

- **cubesql:** Allow `CASE` with `pg_attribute.atttypmod` offset ([fc09160](https://github.com/cube-js/cube.js/commit/fc091609e6f3512d5a078501279e8b9064048b54))

### Features

- **cubesql:** Support comparison between strings and booleans ([#4618](https://github.com/cube-js/cube.js/issues/4618)) ([e4352c3](https://github.com/cube-js/cube.js/commit/e4352c3930e6c948e98bae764920f5d6e21103e8))

## [0.30.8](https://github.com/cube-js/cube.js/compare/v0.30.7...v0.30.8) (2022-05-30)

### Bug Fixes

- **cubesql:** Empty results on `JOIN` with `AND` + `OR` in `WHERE` ([#4608](https://github.com/cube-js/cube.js/issues/4608)) ([96c2f15](https://github.com/cube-js/cube.js/commit/96c2f157f03b95106b509b677fc3d4d6af36b0a2))
- **cubesql:** fix log error standalone ([#4606](https://github.com/cube-js/cube.js/issues/4606)) ([3e3e010](https://github.com/cube-js/cube.js/commit/3e3e010403dc83ca34f7b2ca95c7b46a2a2f1e2d))

### Features

- **cubesql:** Allow `::information_schema.cardinal_number` casting ([b198fb3](https://github.com/cube-js/cube.js/commit/b198fb3a70b3d075ccdfaff638dc8f36e6530944))
- **cubesql:** excel subquery column with same name ([#4602](https://github.com/cube-js/cube.js/issues/4602)) ([ea3a0bc](https://github.com/cube-js/cube.js/commit/ea3a0bc4a944cd724672056f5885110c7cee90cd))
- **cubesql:** PowerBI basic queries support ([455ae07](https://github.com/cube-js/cube.js/commit/455ae076880f305ed73d1d217a87f908837070f5))
- **cubesql:** Support array_upper, array_lower UDFs ([5a3b6bb](https://github.com/cube-js/cube.js/commit/5a3b6bb31c5af920c706b56a8e3c5046f272f8ca))
- **cubesql:** Support to_char UDF ([#4600](https://github.com/cube-js/cube.js/issues/4600)) ([48077a9](https://github.com/cube-js/cube.js/commit/48077a95fccf48309085e6f1f9b2652c581ab3a3))

## [0.30.7](https://github.com/cube-js/cube.js/compare/v0.30.6...v0.30.7) (2022-05-26)

### Bug Fixes

- **cubesql:** Correct command completion for SET in pg-wire ([ab42e54](https://github.com/cube-js/cube.js/commit/ab42e54b2c49aea63d4db75e9332655159fa73e6))

### Features

- **cubesql:** Support escaped string literals, E'str' ([ef9700d](https://github.com/cube-js/cube.js/commit/ef9700d8f7a1ccd0a31aeece70fdcecee092eb9f))
- **cubesql:** Support multiple stmts for simple query in pg-wire ([0f645cb](https://github.com/cube-js/cube.js/commit/0f645cbd0a4bf25d0a03a14d366607ae716fc792))

## [0.30.6](https://github.com/cube-js/cube.js/compare/v0.30.5...v0.30.6) (2022-05-24)

### Bug Fixes

- **cubesql:** Normalize column names for joins and aliased columns ([7faadc9](https://github.com/cube-js/cube.js/commit/7faadc9c96d4cb80b7318a1955cd01e854ca2272))

### Features

- **cubesql:** Support `_pg_truetypid`, `_pg_truetypmod` UDFs ([1436a76](https://github.com/cube-js/cube.js/commit/1436a76c71e7cec8a62149def9fc2de39a48acef))

## [0.30.4](https://github.com/cube-js/cube.js/compare/v0.30.3...v0.30.4) (2022-05-20)

### Bug Fixes

- **cubesql:** Skip returning of schema for special queries in pg-wire ([479ec78](https://github.com/cube-js/cube.js/commit/479ec78836cc095dda8c3725e1378b9f60f56233))
- **cubesql:** Wrong format in RowDescription, support i16/i32/f32 ([0c52cd6](https://github.com/cube-js/cube.js/commit/0c52cd6180e7cf43aeb735ec901da07508ff4598))

### Features

- **cubesql:** Allow ::oid casting ([bb31838](https://github.com/cube-js/cube.js/commit/bb318383028ce9557ccd45ae03cd33f05705bff2))
- **cubesql:** Initial support for type receivers ([452f504](https://github.com/cube-js/cube.js/commit/452f504b7c57d6c669de2eabba935c7a398aa7d2))
- **cubesql:** Support ||, correct schema/catalog/ordinal_position ([6d6cbf5](https://github.com/cube-js/cube.js/commit/6d6cbf5ee743e527d8b9f64008cdc0d12103abf6))
- **cubesql:** Support DISCARD [ALL | PLANS | SEQUENCES | TEMPORARY |… ([#4560](https://github.com/cube-js/cube.js/issues/4560)) ([390c764](https://github.com/cube-js/cube.js/commit/390c764a98fb58fc294cdfe08ed224f2318e1b31))
- **cubesql:** Support IS TRUE|FALSE ([4d227b1](https://github.com/cube-js/cube.js/commit/4d227b11cbe93352d735c81b50da79f256266bb9))

## [0.30.3](https://github.com/cube-js/cube.js/compare/v0.30.2...v0.30.3) (2022-05-17)

### Bug Fixes

- **cubesql:** Add support for all types to `pg_catalog.format_type` UDF ([c49c55a](https://github.com/cube-js/cube.js/commit/c49c55a213efba8da49f2e53cc36a8c8fd9cd64e))
- **cubesql:** Coerce empty subquery result to `NULL` ([e59d2fb](https://github.com/cube-js/cube.js/commit/e59d2fb367f99deea3463316d87ee9eb5ae59463))
- **cubesql:** Fix several UDFs to return correct row amount ([f1e0223](https://github.com/cube-js/cube.js/commit/f1e02239962965f6d246eed53a81c756cbc3a24d))

### Features

- **cubesql:** Ignore `pg_catalog` schema for UDFs ([ab2a0da](https://github.com/cube-js/cube.js/commit/ab2a0da0cdf2ec3cd9974dcb1a532c2ccfad4851))

## [0.30.2](https://github.com/cube-js/cube.js/compare/v0.30.1...v0.30.2) (2022-05-16)

### Features

- **cubesql:** Superset Postgres protocol support ([#4535](https://github.com/cube-js/cube.js/issues/4535)) ([394248f](https://github.com/cube-js/cube.js/commit/394248fa8a10dfd568721405e4a8f392d236d551))

## [0.30.1](https://github.com/cube-js/cube.js/compare/v0.30.0...v0.30.1) (2022-05-14)

### Features

- **cubesql:** Add CUBEJS_PG_SQL_PORT env support and SQL API reference docs ([#4531](https://github.com/cube-js/cube.js/issues/4531)) ([de60d71](https://github.com/cube-js/cube.js/commit/de60d71c360be47e3231e7eafa349b9a0fddd244))
- **cubesql:** Provide specific error messages for not matched expressions ([e035780](https://github.com/cube-js/cube.js/commit/e0357801bd39269585dd31d6ad932b32287a05af))
- **cubesql:** Support `quarter` field in `date_part` SQL function ([7fdf4ac](https://github.com/cube-js/cube.js/commit/7fdf4acf6ce60387d3fa716c572e1611a77c205b))

# [0.30.0](https://github.com/cube-js/cube.js/compare/v0.29.57...v0.30.0) (2022-05-11)

### Features

- **cubesql:** Support dynamic key in ArrayIndex expression ([#4504](https://github.com/cube-js/cube.js/issues/4504)) ([115dd55](https://github.com/cube-js/cube.js/commit/115dd55ed390b8617d592add832b1aefde636265))

## [0.29.57](https://github.com/cube-js/cube.js/compare/v0.29.56...v0.29.57) (2022-05-11)

### Bug Fixes

- **cubesql:** Fix format_type udf usage with tables ([a49b2b4](https://github.com/cube-js/cube.js/commit/a49b2b44c10e4da42443cfd948404d2bc60671ec))
- **cubesql:** Reject `SELECT INTO` queries gracefully ([8b67ff7](https://github.com/cube-js/cube.js/commit/8b67ff7d0de1c5ca0b3852a342931d348ec2422c))

## [0.29.56](https://github.com/cube-js/cube.js/compare/v0.29.55...v0.29.56) (2022-05-06)

### Features

- **cubesql:** Correct support for regclass in CAST expr ([#4499](https://github.com/cube-js/cube.js/issues/4499)) ([cdab58a](https://github.com/cube-js/cube.js/commit/cdab58abeb4251c45e0365ff2a8584c9094f6d4d))
- **cubesql:** More descriptive error messages ([812db77](https://github.com/cube-js/cube.js/commit/812db772a651e0df1f7bc0d1dba97192c65ea834))
- **cubesql:** Partial support for Tableau's table_cat query ([#4466](https://github.com/cube-js/cube.js/issues/4466)) ([f1956d3](https://github.com/cube-js/cube.js/commit/f1956d3240bf067e1ecbee0997303ae76ab3fcaa))
- **cubesql:** Support pg_catalog.pg_enum postgres table ([2db445a](https://github.com/cube-js/cube.js/commit/2db445a120832390dd2577192597e30768b29918))
- **cubesql:** Support pg_get_constraintdef UDF ([#4487](https://github.com/cube-js/cube.js/issues/4487)) ([7a3018d](https://github.com/cube-js/cube.js/commit/7a3018d24326124b5e9257264a11fa09bc565f57))
- **cubesql:** Support pg_type_is_visible postgres udf ([47fc285](https://github.com/cube-js/cube.js/commit/47fc285c07c9633bee2482ef246f5436dd79dff3))

## [0.29.55](https://github.com/cube-js/cube.js/compare/v0.29.54...v0.29.55) (2022-05-04)

### Bug Fixes

- **cubesql:** Correct handling for boolean type ([cff6c8b](https://github.com/cube-js/cube.js/commit/cff6c8b4d69c9b8bade7ce1e6e2f2502a44f3918))
- **cubesql:** Tableau new regclass query fast fix ([2a7ff1e](https://github.com/cube-js/cube.js/commit/2a7ff1e20fc79dccd9cff94e6225d657569ed06e))

### Features

- **cubesql:** Tableau cubes without count measure support ([931e2f5](https://github.com/cube-js/cube.js/commit/931e2f5fb5fa29b19347b7858a8b4f892162f169))

## [0.29.54](https://github.com/cube-js/cube.js/compare/v0.29.53...v0.29.54) (2022-05-03)

### Bug Fixes

- **cubesql:** Using same alias on column yields Option.unwrap() panic ([a674c5f](https://github.com/cube-js/cube.js/commit/a674c5f98f8c643ed407fcf1cac528c797c43746))

### Features

- **cubesql:** Tableau boolean filters support ([33aa5f1](https://github.com/cube-js/cube.js/commit/33aa5f138b44ccf60afc6e562b9bf71c2fe6257c))
- **cubesql:** Tableau cast projection queries support ([71ec644](https://github.com/cube-js/cube.js/commit/71ec64444e182a0a1c92818d655b40f78e463684))
- **cubesql:** Tableau contains support ([71dcad0](https://github.com/cube-js/cube.js/commit/71dcad091dc8e60958c717bd01e07db050abf8af))
- **cubesql:** Tableau min max number dimension support ([2abe13e](https://github.com/cube-js/cube.js/commit/2abe13e3155ad03ec3837da38bd465fbee0eb2f9))
- **cubesql:** Tableau not null filter support ([d48d0e0](https://github.com/cube-js/cube.js/commit/d48d0e03d05559413ddcff0ce980f6cf96cd24bc))
- **cubesql:** Tableau week support ([6d987ea](https://github.com/cube-js/cube.js/commit/6d987ea6062a90843084b72b254d068f46e26601))

## [0.29.53](https://github.com/cube-js/cube.js/compare/v0.29.52...v0.29.53) (2022-04-29)

### Bug Fixes

- **cubesql:** fix pg_constraint confkey type ([#4462](https://github.com/cube-js/cube.js/issues/4462)) ([82c25fd](https://github.com/cube-js/cube.js/commit/82c25fd98961a4130607cd1b93049d9b6f3093e7))

### Features

- **cubesql:** Aggregate aggregate split to support Tableau extract date part queries ([532b4ee](https://github.com/cube-js/cube.js/commit/532b4eece185dce8bfd5de46325105b45d50f621))
- **cubesql:** Projection aggregate split to support Tableau casts ([#4435](https://github.com/cube-js/cube.js/issues/4435)) ([1550774](https://github.com/cube-js/cube.js/commit/1550774acf2dd208d7222bb7b4742dcc64ca4b89))
- **cubesql:** Support for pg_get_userbyid, pg_table_is_visible UDFs ([64f8885](https://github.com/cube-js/cube.js/commit/64f8885806d9034cb55b828d37193d5540829a6a))
- **cubesql:** Support generate_subscripts UDTF ([a29551a](https://github.com/cube-js/cube.js/commit/a29551a402f323541a1b10523f3478f9ae284989))
- **cubesql:** Support get_expr query for Pg/Tableau ([#4421](https://github.com/cube-js/cube.js/issues/4421)) ([4d4918f](https://github.com/cube-js/cube.js/commit/4d4918fd9ff73c4d642416c74d720e5a85e2a87a))
- **cubesql:** Support information_schema.\_pg_expandarray postgres UDTF ([#4439](https://github.com/cube-js/cube.js/issues/4439)) ([1af4290](https://github.com/cube-js/cube.js/commit/1af4290a9d35a67e62c21acc3edc0536ce15c694))
- **cubesql:** Support pg_catalog.pg_am table ([24b231d](https://github.com/cube-js/cube.js/commit/24b231d45d355c0c01425157a41db1f7ac65b80a))
- **cubesql:** Support Timestamp, TimestampTZ for pg-wire ([0b38b3d](https://github.com/cube-js/cube.js/commit/0b38b3d594999bf5f165295ba9643998004beb81))
- **cubesql:** Support unnest UDTF ([110bdf8](https://github.com/cube-js/cube.js/commit/110bdf8de390bf82c604aeab0dacafaae4b0eda8))
- **cubesql:** Tableau default having support ([4d432c0](https://github.com/cube-js/cube.js/commit/4d432c0b12d2ed75488d723304aa999554f7ee54))
- **cubesql:** Tableau Min, Max timestamp queries support ([48ee34e](https://github.com/cube-js/cube.js/commit/48ee34efb9c7a1a3feaae8fa0e091a84c18b4736))
- **cubesql:** Tableau range of dates support ([ef56133](https://github.com/cube-js/cube.js/commit/ef5613307996cf5b3973af366f625ca78bcb2dbd))
- **cubesql:** Tableau relative date range support ([87a3817](https://github.com/cube-js/cube.js/commit/87a381705dcfaa3e3c3841bdb66b2b6f0535d8ca))
- **cubesql:** Unwrap filter casts for Tableau ([0a39420](https://github.com/cube-js/cube.js/commit/0a3942038d12a357d9af13941311af7cbcc87830))

## [0.29.51](https://github.com/cube-js/cube.js/compare/v0.29.50...v0.29.51) (2022-04-22)

### Bug Fixes

- **cubesql:** Bool encoding for text format in pg-wire ([7faf34b](https://github.com/cube-js/cube.js/commit/7faf34b4dee421202528aa2e9985acbfcc8da6b9))
- **cubesql:** current_schema() UDF ([69a75dc](https://github.com/cube-js/cube.js/commit/69a75dc3fe29be97eecf2f0eeb97a642a2328212))
- **cubesql:** Proper handling for Postgresql table reference ([35f5635](https://github.com/cube-js/cube.js/commit/35f56350f39f22665e71fa53a1e6fc5d7bb02262))

### Features

- **cubesql:** Correlated subqueries support for introspection queries ([#4408](https://github.com/cube-js/cube.js/issues/4408)) ([1f02b2c](https://github.com/cube-js/cube.js/commit/1f02b2c363becb046ae5b94833a46a7091e572ad))
- **cubesql:** Implement rewrites for SELECT \* FROM WHERE 1=0 ([#4427](https://github.com/cube-js/cube.js/issues/4427)) ([0c9abd1](https://github.com/cube-js/cube.js/commit/0c9abd1bde7c5492c42340f75e020dc09228908b))
- **cubesql:** Support arrays in pg-wire ([b7925ba](https://github.com/cube-js/cube.js/commit/b7925ba703d245115321fb6b399eb71efec71cab))
- **cubesql:** Support generate_series UDTF ([#4416](https://github.com/cube-js/cube.js/issues/4416)) ([3321925](https://github.com/cube-js/cube.js/commit/33219254319b13e2d7ef97fd81eedb01a198123c))
- **cubesql:** Support GetIndexedFieldExpr rewrites ([#4424](https://github.com/cube-js/cube.js/issues/4424)) ([8dca8b5](https://github.com/cube-js/cube.js/commit/8dca8b50ea67f2e5e562e1bb69b5375de55a3b48))
- **cubesql:** Support information_schema.\_pg_datetime_precision UDF ([4d20ee6](https://github.com/cube-js/cube.js/commit/4d20ee61410d439fc17ddc204afb9e855705c7b7))
- **cubesql:** Support information_schema.\_pg_numeric_precision UDF ([6fc6c0a](https://github.com/cube-js/cube.js/commit/6fc6c0a22c57f4fa38ace1f5183e2d9e3eb7afde))
- **cubesql:** Support information_schema.\_pg_numeric_scale UDF ([398d1db](https://github.com/cube-js/cube.js/commit/398d1dba9736ff1059bc328c3ab881cdb9ad1650))
- **cubesql:** Support lc_collate for PostgreSQL ([120ce31](https://github.com/cube-js/cube.js/commit/120ce3145447e1df034ac412fa20471ff674c893))
- **cubesql:** Support NoData response for empty response in pg-wire ([6711c8a](https://github.com/cube-js/cube.js/commit/6711c8aa39bbcff0e36db763c4c7f1a37b838a5c))
- **cubesql:** Support pg_get_expr UDF ([#4425](https://github.com/cube-js/cube.js/issues/4425)) ([2b51d70](https://github.com/cube-js/cube.js/commit/2b51d70e4aafb5e2531df2a39293803cbf33b195))
- **cubesql:** Support pg_get_userbyid UDF ([c6efef8](https://github.com/cube-js/cube.js/commit/c6efef83736f2b1733f40f07f315d51574f6d371))
- **cubesql:** Use proper command completion tags for pg-wire ([3e777ec](https://github.com/cube-js/cube.js/commit/3e777ec2926d3e6c2f174ff94b6ac50ae5e2593a))

## [0.29.50](https://github.com/cube-js/cube.js/compare/v0.29.49...v0.29.50) (2022-04-18)

### Features

- **cubesql:** Initial support for Binary format in pg-wire ([a36845c](https://github.com/cube-js/cube.js/commit/a36845c5edcb6bd77172de2cebcd67a700df5224))
- **cubesql:** Support Describe(Portal) for pg-wire ([34cf111](https://github.com/cube-js/cube.js/commit/34cf111249c3fede986c3633fe8d5f0cade3ed91))
- **cubesql:** Support pg_depend postgres table ([ceb35d4](https://github.com/cube-js/cube.js/commit/ceb35d4825cd2ac76ad191eef950ab3be126c3de))

## [0.29.48](https://github.com/cube-js/cube.js/compare/v0.29.47...v0.29.48) (2022-04-14)

### Bug Fixes

- **cubesql:** Support pg_catalog.format_type through fully qualified name ([9eafae0](https://github.com/cube-js/cube.js/commit/9eafae0c4eafc2ad1d8517be9dbf292c1650c64a))

### Features

- **cubesql:** Initial support for prepared statements in pg-wire ([#4244](https://github.com/cube-js/cube.js/issues/4244)) ([912b52a](https://github.com/cube-js/cube.js/commit/912b52a5cb8d72820c68843e15a2ef83233b952f))
- **cubesql:** Postgres Apache Superset connection flow support ([ab256d9](https://github.com/cube-js/cube.js/commit/ab256d9fc31fd4d2bc08c969b374cec449e34bae))

## [0.29.47](https://github.com/cube-js/cube.js/compare/v0.29.46...v0.29.47) (2022-04-12)

### Bug Fixes

- **cubesql:** Correct MySQL types in response headers ([#4362](https://github.com/cube-js/cube.js/issues/4362)) ([c507f82](https://github.com/cube-js/cube.js/commit/c507f82fbdd92363d27c4b3c8b41957bd62a3d87))
- **cubesql:** Special handling for bool as string ([3ba27bf](https://github.com/cube-js/cube.js/commit/3ba27bf7ee91aef69eb75c33580abf18b21bd29e))
- **cubesql:** Support boolean (ColumnType) for MySQL protocol ([23f8367](https://github.com/cube-js/cube.js/commit/23f8367f6657b8d7f31e4e34a4547d30c3c34c79))

## [0.29.46](https://github.com/cube-js/cube.js/compare/v0.29.45...v0.29.46) (2022-04-11)

### Bug Fixes

- **cubesql:** Rewrite engine decimal measure support ([8a0fa98](https://github.com/cube-js/cube.js/commit/8a0fa981b87b67281867c6073903fa9bb6826570))

### Features

- **cubesql:** Support format_type UDF for Postgres ([#4325](https://github.com/cube-js/cube.js/issues/4325)) ([8b972ca](https://github.com/cube-js/cube.js/commit/8b972ca9bfd46cc8d43a93ce04e696624838fbde))

## [0.29.45](https://github.com/cube-js/cube.js/compare/v0.29.44...v0.29.45) (2022-04-09)

### Bug Fixes

- **cubesql:** Rewrite engine datafusion after rebase regressions: mismatched to_day_interval signature, projection aliases, order by date. ([8310f7e](https://github.com/cube-js/cube.js/commit/8310f7e1d4b7c2c28b6d2e7f0fb683114c837282))

## [0.29.44](https://github.com/cube-js/cube.js/compare/v0.29.43...v0.29.44) (2022-04-07)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.29.43](https://github.com/cube-js/cube.js/compare/v0.29.42...v0.29.43) (2022-04-07)

### Bug Fixes

- **cubesql:** Rewrites don't respect projection column order ([cfe35a7](https://github.com/cube-js/cube.js/commit/cfe35a7b65390db43f1e7c68ac54c82c2ec8af49))

### Features

- **cubesql:** Rewrite engine error handling ([3fba823](https://github.com/cube-js/cube.js/commit/3fba823bc561d7a985c89c4cf437a6595ef88a7c))
- **cubesql:** Upgrade rust to 1.61.0-nightly (2022-02-22) ([c836065](https://github.com/cube-js/cube.js/commit/c8360658ccb8e5e3e6cfcd62da2d156b44ee8456))

## [0.29.42](https://github.com/cube-js/cube.js/compare/v0.29.41...v0.29.42) (2022-04-04)

### Bug Fixes

- **cubesql:** Allow quoted variables with SHOW <variable> syntax ([#4313](https://github.com/cube-js/cube.js/issues/4313)) ([3eece0e](https://github.com/cube-js/cube.js/commit/3eece0e70817b2b72406b146a95a5757cdfb994c))

### Features

- **cubesql:** Rewrite engine segments support ([48b0767](https://github.com/cube-js/cube.js/commit/48b0767aa880a2373d6a6fa15b2e0a1815bb1865))

## [0.29.40](https://github.com/cube-js/cube.js/compare/v0.29.39...v0.29.40) (2022-04-03)

### Bug Fixes

- **cubesql:** Table columns should take precedence over projection to mimic MySQL and Postgres behavior ([60d6e45](https://github.com/cube-js/cube.js/commit/60d6e4511267b132df204fe7b637953d81e5f980))

## [0.29.38](https://github.com/cube-js/cube.js/compare/v0.29.37...v0.29.38) (2022-04-01)

### Bug Fixes

- **cubesql:** Deallocate statement on specific command in MySQL protocol ([ab3f36c](https://github.com/cube-js/cube.js/commit/ab3f36c182f603f348115463926e1cbe8ee40fd6))
- **cubesql:** Enable EXPLAIN support for postgres ([c0244d1](https://github.com/cube-js/cube.js/commit/c0244d10bd43647c6e6f562be3e30ec3d1ae66d9))

### Features

- **cubesql:** Initial support for current_schemas() postgres function ([e0907ff](https://github.com/cube-js/cube.js/commit/e0907ffa0a03b985ac2fe1cb9d592a47a6141d20))
- **cubesql:** Postgres pg_catalog.pg_class MetaLayer table ([#4287](https://github.com/cube-js/cube.js/issues/4287)) ([d70da08](https://github.com/cube-js/cube.js/commit/d70da08a345f857b09e60333d324e722e6619684))
- **cubesql:** Support binding values for prepared statements (MySQL only) ([ad26dc5](https://github.com/cube-js/cube.js/commit/ad26dc55274cc060300525bfd6238df82ffd782c))
- **cubesql:** Support current_schema() postgres function ([44b64ce](https://github.com/cube-js/cube.js/commit/44b64ce97d46f94eff057d0f7fd182e969bbbea0))
- **cubesql:** Support information_schema.character_sets (postgres) table ([1804b79](https://github.com/cube-js/cube.js/commit/1804b792381b044a5ee41b60cd72d4d02c1cf945))
- **cubesql:** Support information_schema.key_column_usage (postgres) table ([84cf2c1](https://github.com/cube-js/cube.js/commit/84cf2c1791f56dee0f240faa23e70ae4dd4e013a))
- **cubesql:** Support information_schema.referential_constraints (postgres) table ([eeb42be](https://github.com/cube-js/cube.js/commit/eeb42bec4d8635a647c638857bfa12f5f3518f1c))
- **cubesql:** Support information_schema.table_constraints (postgres) table ([2d6bfee](https://github.com/cube-js/cube.js/commit/2d6bfee1ee8d7bdc9750f32e85ec6f027ef988f0))
- **cubesql:** Support pg_catalog.pg_description and pg_catalog.pg_constraint MetaLayer tables ([#4292](https://github.com/cube-js/cube.js/issues/4292)) ([0ea9699](https://github.com/cube-js/cube.js/commit/0ea969930c5821589033595cb749d8acd64d991b))
- MySQL SET variables / Postgres SHOW SET variables ([#4266](https://github.com/cube-js/cube.js/issues/4266)) ([88ec3cc](https://github.com/cube-js/cube.js/commit/88ec3ccdf6582129d164bfcd3b0486b7f90cd923))
- **cubesql:** Postgres pg_catalog.pg_proc MetaLayer table ([#4289](https://github.com/cube-js/cube.js/issues/4289)) ([b3613d0](https://github.com/cube-js/cube.js/commit/b3613d08e4b906f61f5433c3a40a03eab1f0b297))
- **cubesql:** Support pg_catalog.pg_attrdef table ([d6aae8d](https://github.com/cube-js/cube.js/commit/d6aae8da32de339fefc232ba83818a15c4b01872))
- **cubesql:** Support pg_catalog.pg_attribute table ([d5f7d0c](https://github.com/cube-js/cube.js/commit/d5f7d0cab2df0b4b5eb6e1308b4347552ff3494d))
- **cubesql:** Support pg_catalog.pg_index table ([a621532](https://github.com/cube-js/cube.js/commit/a6215327a61439eb4293b7182de148ec425716fe))

## [0.29.37](https://github.com/cube-js/cube.js/compare/v0.29.36...v0.29.37) (2022-03-29)

### Bug Fixes

- **cubesql:** Dropping session on close for pg-wire ([#4280](https://github.com/cube-js/cube.js/issues/4280)) ([c4442be](https://github.com/cube-js/cube.js/commit/c4442be153160b864fafd34a4f0769dce9117fa4))
- **cubesql:** Rewrite engine can't parse `db` prefixed table names ([b7d9382](https://github.com/cube-js/cube.js/commit/b7d93827750b8d72e871abd527f1c0a649e5e6c2))
- **cubesql:** Rewrite engine: support for stacked time series charts ([c1add2c](https://github.com/cube-js/cube.js/commit/c1add2c9c52d1cd884dfefe4db978a853a76c83e))

### Features

- **cubesql:** Global Meta Tables ([88db9ea](https://github.com/cube-js/cube.js/commit/88db9eab3854a89cd93cfdce3a9fad9a180f3b45))
- **cubesql:** Global Meta Tables - add tests ([42e9517](https://github.com/cube-js/cube.js/commit/42e9517d1ac1673622bb9b03352af94f8ec968ba))
- **cubesql:** Global Meta Tables - cargo fmt ([c8336d9](https://github.com/cube-js/cube.js/commit/c8336d92a7867fd2b78e1542d61b42519fa9e3f2))
- **cubesql:** Support pg_catalog.pg_range table ([625c03a](https://github.com/cube-js/cube.js/commit/625c03ae965b2730d924812a7d16aec3fbdf5369))

## [0.29.36](https://github.com/cube-js/cube.js/compare/v0.29.35...v0.29.36) (2022-03-27)

### Features

- **cubesql:** Improve Postgres, MySQL meta layer ([#4228](https://github.com/cube-js/cube.js/issues/4228)) ([5c8d002](https://github.com/cube-js/cube.js/commit/5c8d002d1efc8cb6a57849389b87e7cb4ec187f0))
- **cubesql:** Rewrite engine first steps ([#4132](https://github.com/cube-js/cube.js/issues/4132)) ([84c51ed](https://github.com/cube-js/cube.js/commit/84c51eda4bf989a46f95fe683ea2732814dde28f))
- **cubesql:** Support pg_catalog.pg_namespace table ([66e41da](https://github.com/cube-js/cube.js/commit/66e41dacdaf3d0dc24f866c3d29ddb76b79a292a))
- **cubesql:** Support pg_catalog.pg_type table ([d792bb9](https://github.com/cube-js/cube.js/commit/d792bb9949b48e0dceb3aa5d02500258533cfb66))

## [0.29.35](https://github.com/cube-js/cube.js/compare/v0.29.34...v0.29.35) (2022-03-24)

### Bug Fixes

- **cubesql:** Fix decoding for messages without body in pg-wire protocol ([f7aa6ed](https://github.com/cube-js/cube.js/commit/f7aa6ed5438888edce2b413529d79996a912aac3))
- **cubesql:** Specify required parameters on startup for pg-wire ([b79088b](https://github.com/cube-js/cube.js/commit/b79088b01d328082378c6c66d5ca103997955e42))

### Features

- **cubesql:** Split variables to session / server for MySQL ([#4255](https://github.com/cube-js/cube.js/issues/4255)) ([f78b539](https://github.com/cube-js/cube.js/commit/f78b5396e217d9fdf6cf970bd837f767c5b8a2f5))

## [0.29.34](https://github.com/cube-js/cube.js/compare/v0.29.33...v0.29.34) (2022-03-21)

### Bug Fixes

- **cubesql:** Disable MySQL specific functions/statements for pg-wire protocol ([#4222](https://github.com/cube-js/cube.js/issues/4222)) ([21f6cde](https://github.com/cube-js/cube.js/commit/21f6cde31537e515daedd7266e958e7b259f0ace))

### Features

- **cubesql:** Correct response for SslRequest in pg-wire ([#4238](https://github.com/cube-js/cube.js/issues/4238)) ([bd1468a](https://github.com/cube-js/cube.js/commit/bd1468aa0a5851c9bcddb81dfd0d1da5c080972f))

## [0.29.33](https://github.com/cube-js/cube.js/compare/v0.29.32...v0.29.33) (2022-03-17)

### Bug Fixes

- **cubesql:** Add numeric_scale field for information_schema.columns ([2e2877a](https://github.com/cube-js/cube.js/commit/2e2877ab8a1d144f529661481b7fc6ddef7d3c85))

### Features

- **cubesql:** Enable PostgresServer via env variable ([39b6528](https://github.com/cube-js/cube.js/commit/39b6528d91b569bdae90362e1a693404a4eef958))
- **cubesql:** Initial support for pg-wire protocol ([1b87c8c](https://github.com/cube-js/cube.js/commit/1b87c8cc67055ab0be0c208505d2bd50b7abffc8))
- **cubesql:** Support meta layer and dialect for Postgres service ([#4215](https://github.com/cube-js/cube.js/issues/4215)) ([46af90d](https://github.com/cube-js/cube.js/commit/46af90d6d41d147b33f9e9eed24e830857243967))
- **cubesql:** Support PLAIN authentication method to pg-wire ([#4229](https://github.com/cube-js/cube.js/issues/4229)) ([c4fbd8c](https://github.com/cube-js/cube.js/commit/c4fbd8c9f12ffed396754712f912868f147c697a))
- **cubesql:** Support SHOW processlist ([0194098](https://github.com/cube-js/cube.js/commit/0194098af10e77c84ef141dc372f3abc46b3b514))

## [0.29.32](https://github.com/cube-js/cube.js/compare/v0.29.31...v0.29.32) (2022-03-10)

### Features

- **cubesql:** Support information_schema.processlist ([#4185](https://github.com/cube-js/cube.js/issues/4185)) ([4179fb0](https://github.com/cube-js/cube.js/commit/4179fb006104275ba0d7074d681cd937efb0a8fc))

## [0.29.28](https://github.com/cube-js/cube.js/compare/v0.29.27...v0.29.28) (2022-02-10)

### Bug Fixes

- **cubesql:** Allow to pass measure as an argument in COUNT function ([#4063](https://github.com/cube-js/cube.js/issues/4063)) ([c48c7ea](https://github.com/cube-js/cube.js/commit/c48c7ea1c86a64463a84a9ffc1c06aa605c6331c))

## [0.29.27](https://github.com/cube-js/cube.js/compare/v0.29.26...v0.29.27) (2022-02-09)

### Bug Fixes

- **cubesql:** Unique filtering for measures/dimensions/segments in Request ([552c87b](https://github.com/cube-js/cube.js/commit/552c87bf38479133e2c8dac20ac1c29eb034c762))

### Features

- **cubesql:** Move execution to Query Engine ([2d84b6b](https://github.com/cube-js/cube.js/commit/2d84b6b98fc03d84f858bd152f2359232e9ea8ed))

## [0.29.26](https://github.com/cube-js/cube.js/compare/v0.29.25...v0.29.26) (2022-02-07)

### Bug Fixes

- **cubesql:** Ignore case sensitive search for usage of identifiers ([a50f8a2](https://github.com/cube-js/cube.js/commit/a50f8a25e8064f98eb7931c643d2ce67be340ad0))

### Features

- **cubesql:** Support information_schema.COLLATIONS table ([#4018](https://github.com/cube-js/cube.js/issues/4018)) ([262314d](https://github.com/cube-js/cube.js/commit/262314dd939b57851c264f038e4f032d8b98bab8))
- **cubesql:** Support prepared statements in MySQL protocol ([#4005](https://github.com/cube-js/cube.js/issues/4005)) ([6b2f61c](https://github.com/cube-js/cube.js/commit/6b2f61cafbcf4758bba1d16a344871a84d0767f3))
- **cubesql:** Support SHOW COLLATION ([#4025](https://github.com/cube-js/cube.js/issues/4025)) ([95b5d0e](https://github.com/cube-js/cube.js/commit/95b5d0ee8af9054c64e5dac50a89db7bb6d8a5fc))

## [0.29.24](https://github.com/cube-js/cube.js/compare/v0.29.23...v0.29.24) (2022-02-01)

### Bug Fixes

- **cubesql:** Ignore @@ global prefix for system defined variables ([80caef0](https://github.com/cube-js/cube.js/commit/80caef0f2eb145a4405d8edcb4d650179b22c593))

### Features

- **cubesql:** Support binary expression for measures ([#4009](https://github.com/cube-js/cube.js/issues/4009)) ([475a614](https://github.com/cube-js/cube.js/commit/475a6148aa8d87183e7680888d27737c0290e401))
- **cubesql:** Support COUNT(1) ([#4004](https://github.com/cube-js/cube.js/issues/4004)) ([df33d89](https://github.com/cube-js/cube.js/commit/df33d89b1a19c452b1a97b49e640c4ed1a53e1ad))
- **cubesql:** Support SHOW COLUMNS ([#3995](https://github.com/cube-js/cube.js/issues/3995)) ([bbf7e6c](https://github.com/cube-js/cube.js/commit/bbf7e6c232d9c91ccd9421f01f4fdda07ef82998))
- **cubesql:** Support SHOW TABLES via QE ([#4001](https://github.com/cube-js/cube.js/issues/4001)) ([bac2aaa](https://github.com/cube-js/cube.js/commit/bac2aaae130c0863790b2178884a157dcdf0c55d))
- **cubesql:** Support USE 'db' (success reply) ([bd945fb](https://github.com/cube-js/cube.js/commit/bd945fbc12a9250a90f240127ad1ac9910011a01))

## [0.29.23](https://github.com/cube-js/cube.js/compare/v0.29.22...v0.29.23) (2022-01-26)

### Features

- **cubesql:** Setup more system variables ([97fe231](https://github.com/cube-js/cube.js/commit/97fe231b36d1d2497e0a913b2ae35f3f41f98e53))

## [0.29.22](https://github.com/cube-js/cube.js/compare/v0.29.21...v0.29.22) (2022-01-21)

### Features

- **cubesql:** Execute SHOW VARIABLES [LIKE 'pattern'] via QE instead of hardcoding ([#3960](https://github.com/cube-js/cube.js/issues/3960)) ([48c0d77](https://github.com/cube-js/cube.js/commit/48c0d774b8c206dee3f2280fadcbeb832d695dc9))

## [0.29.21](https://github.com/cube-js/cube.js/compare/v0.29.20...v0.29.21) (2022-01-17)

### Features

- **cubesql:** Improve error messages ([#3829](https://github.com/cube-js/cube.js/issues/3829)) ([8293e52](https://github.com/cube-js/cube.js/commit/8293e52a4a509e8559949d8af6446ef8a04e33f5))

## [0.29.20](https://github.com/cube-js/cube.js/compare/v0.29.19...v0.29.20) (2022-01-10)

### Bug Fixes

- **cubesql:** Alias binding problem with escapes (<expr> as '') ([8b2c002](https://github.com/cube-js/cube.js/commit/8b2c002537e5151b51328e041828153ac77bf231))

## [0.29.18](https://github.com/cube-js/cube.js/compare/v0.29.17...v0.29.18) (2022-01-09)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.29.15](https://github.com/cube-js/cube.js/compare/v0.29.14...v0.29.15) (2021-12-30)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.29.12](https://github.com/cube-js/cube.js/compare/v0.29.11...v0.29.12) (2021-12-29)

### Features

- **cubesql:** Ignore KILL statement without error ([20590f3](https://github.com/cube-js/cube.js/commit/20590f39bc1931f5b23b14d81aa48562e373c95b))

## [0.29.11](https://github.com/cube-js/cube.js/compare/v0.29.10...v0.29.11) (2021-12-24)

**Note:** Version bump only for package @cubejs-backend/cubesql

## [0.29.10](https://github.com/cube-js/cube.js/compare/v0.29.9...v0.29.10) (2021-12-22)

**Note:** Version bump only for package @cubejs-backend/cubesql
