# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [1.3.61](https://github.com/cube-js/cube/compare/v1.3.60...v1.3.61) (2025-08-28)

### Features

- **schema-compiler:** Support shorthand for userAttributes properties in models ([#9921](https://github.com/cube-js/cube/issues/9921)) ([6086a65](https://github.com/cube-js/cube/commit/6086a6565872361d1952fb5cb395d5f1faaa9968))
- **tesseract:** Full key aggregate and logical plan refactoring ([#9807](https://github.com/cube-js/cube/issues/9807)) ([da4d7aa](https://github.com/cube-js/cube/commit/da4d7aa8ce230dd625467810051a8389962adf9a))

## [1.3.60](https://github.com/cube-js/cube/compare/v1.3.59...v1.3.60) (2025-08-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.59](https://github.com/cube-js/cube/compare/v1.3.58...v1.3.59) (2025-08-26)

### Bug Fixes

- **schema-compiler:** Fix incorrect backAlias members collection for FILTER_PARAMS members ([#9919](https://github.com/cube-js/cube/issues/9919)) ([91115d7](https://github.com/cube-js/cube/commit/91115d7344920ed13eb9ef1b9f522c9ee67fe8af))

## [1.3.58](https://github.com/cube-js/cube/compare/v1.3.57...v1.3.58) (2025-08-25)

### Bug Fixes

- **schema-compiler:** Use join tree for pre-agg matching ([#9597](https://github.com/cube-js/cube/issues/9597)) ([c0341ba](https://github.com/cube-js/cube/commit/c0341ba94c5aae9c5ffef178bd6ccca27735db46))

## [1.3.57](https://github.com/cube-js/cube/compare/v1.3.56...v1.3.57) (2025-08-22)

### Features

- **schema-compiler:** groups support ([#9903](https://github.com/cube-js/cube/issues/9903)) ([8a13051](https://github.com/cube-js/cube/commit/8a13051d7845d85450569c14f971169cd5ad389e))

## [1.3.56](https://github.com/cube-js/cube/compare/v1.3.55...v1.3.56) (2025-08-21)

### Bug Fixes

- **tesseract:** Param casting in Presto ([#9908](https://github.com/cube-js/cube/issues/9908)) ([346d300](https://github.com/cube-js/cube/commit/346d300f4d4ac0c231229a5883c2cf9cec746035))

### Features

- **cubesql:** Avoid `COUNT(*)` pushdown to joined cubes ([#9905](https://github.com/cube-js/cube/issues/9905)) ([e073a72](https://github.com/cube-js/cube/commit/e073a7217e25c3bc4b3c63145d4a22973a587491))

### Performance Improvements

- **schema-compiler:** Reduce JS compilation memory usage 3x-5x times. ([#9897](https://github.com/cube-js/cube/issues/9897)) ([3f1ff57](https://github.com/cube-js/cube/commit/3f1ff57589bc206bdf3cd30fed270343ef68a284))

## [1.3.55](https://github.com/cube-js/cube/compare/v1.3.54...v1.3.55) (2025-08-19)

### Bug Fixes

- **schema-compiler:** Fix joins getter in cube symbols ([#9904](https://github.com/cube-js/cube/issues/9904)) ([8012341](https://github.com/cube-js/cube/commit/8012341f4e0699c0eb3bb3a7e3eb68052295f191))

## [1.3.54](https://github.com/cube-js/cube/compare/v1.3.53...v1.3.54) (2025-08-15)

### Features

- **schema-compiler:** Reduce memory usage after compilation is done ([#9890](https://github.com/cube-js/cube/issues/9890)) ([812efec](https://github.com/cube-js/cube/commit/812efec7afa071b77d4da0a39fcbf3987b7f0464))

## [1.3.53](https://github.com/cube-js/cube/compare/v1.3.52...v1.3.53) (2025-08-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.52](https://github.com/cube-js/cube/compare/v1.3.51...v1.3.52) (2025-08-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.51](https://github.com/cube-js/cube/compare/v1.3.50...v1.3.51) (2025-08-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.50](https://github.com/cube-js/cube/compare/v1.3.49...v1.3.50) (2025-08-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.49](https://github.com/cube-js/cube/compare/v1.3.48...v1.3.49) (2025-08-12)

### Bug Fixes

- **cubesql:** Improve SQL push down for Athena/Presto ([#9873](https://github.com/cube-js/cube/issues/9873)) ([893e9b3](https://github.com/cube-js/cube/commit/893e9b3e0dd200a26c8b97bb7a39532707edf632))

## [1.3.48](https://github.com/cube-js/cube/compare/v1.3.47...v1.3.48) (2025-08-09)

### Bug Fixes

- access_policy is not applied for JS models ([#9865](https://github.com/cube-js/cube/issues/9865)) ([89c78ec](https://github.com/cube-js/cube/commit/89c78ec921d12e14aefa89193df544eadc221cfb))
- **cubesql:** Improve Trino SQL push down compatibility ([#9861](https://github.com/cube-js/cube/issues/9861)) ([9d5794e](https://github.com/cube-js/cube/commit/9d5794e9eb3610d722adc0a8ec5092140efdf0f1))
- **cubesql:** Support concatenating non-strings in SQL push down for Athena/Presto ([#9853](https://github.com/cube-js/cube/issues/9853)) ([97e54e0](https://github.com/cube-js/cube/commit/97e54e01ad78a9483736d92339769332c9934e68))

## [1.3.47](https://github.com/cube-js/cube/compare/v1.3.46...v1.3.47) (2025-08-04)

### Features

- **cubesql:** Support `BETWEEN` SQL push down ([#9834](https://github.com/cube-js/cube/issues/9834)) ([195402f](https://github.com/cube-js/cube/commit/195402f76f893a0649488a373fb5b46b7f0a04b3))

## [1.3.46](https://github.com/cube-js/cube/compare/v1.3.45...v1.3.46) (2025-07-31)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.45](https://github.com/cube-js/cube/compare/v1.3.44...v1.3.45) (2025-07-29)

### Features

- **clickhouse-driver:** Upgrade @clickhouse/client from 1.7.0 to 1.12.0 ([#9829](https://github.com/cube-js/cube/issues/9829)) ([80c281f](https://github.com/cube-js/cube/commit/80c281f330b16fd27a7b61d9ff030ce4fcf92b13))

## [1.3.44](https://github.com/cube-js/cube/compare/v1.3.43...v1.3.44) (2025-07-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.43](https://github.com/cube-js/cube/compare/v1.3.42...v1.3.43) (2025-07-24)

### Bug Fixes

- **snowflake-driver:** Set date/timestamp format for exporting data to CSV export bucket ([#9810](https://github.com/cube-js/cube/issues/9810)) ([4c0fef7](https://github.com/cube-js/cube/commit/4c0fef7b068893322f05310b028059ea3dad8986))

## [1.3.42](https://github.com/cube-js/cube/compare/v1.3.41...v1.3.42) (2025-07-23)

### Features

- **tesseract:** Lambda rollup support ([#9806](https://github.com/cube-js/cube/issues/9806)) ([eb56169](https://github.com/cube-js/cube/commit/eb56169f5e88424b95b12dda8535cab383ecc541))

## [1.3.41](https://github.com/cube-js/cube/compare/v1.3.40...v1.3.41) (2025-07-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.40](https://github.com/cube-js/cube/compare/v1.3.39...v1.3.40) (2025-07-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.39](https://github.com/cube-js/cube/compare/v1.3.38...v1.3.39) (2025-07-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.38](https://github.com/cube-js/cube/compare/v1.3.37...v1.3.38) (2025-07-16)

### Bug Fixes

- **schema-compiler:** Fix BigQuery convertTz implementation ([#9782](https://github.com/cube-js/cube/issues/9782)) ([75f4813](https://github.com/cube-js/cube/commit/75f48139abccc341398980c7b9abfd78bc7d21aa))

### Features

- **schema-compiler,api-gateway:** Nested folders support ([#9659](https://github.com/cube-js/cube/issues/9659)) ([720f048](https://github.com/cube-js/cube/commit/720f0485c8b11f16eb99490259a881c21b845c73))
- **tesseract:** Allow named calendar timeshifts for common intervals ([#9777](https://github.com/cube-js/cube/issues/9777)) ([a5f8a2e](https://github.com/cube-js/cube/commit/a5f8a2e0d93bf5de0291389d846660f6491651fe))

## [1.3.37](https://github.com/cube-js/cube/compare/v1.3.36...v1.3.37) (2025-07-14)

### Bug Fixes

- **tesseract:** Fix infinite loops during calendar time dimension processing ([#9772](https://github.com/cube-js/cube/issues/9772)) ([3f16a39](https://github.com/cube-js/cube/commit/3f16a39c27ac7da2d5671031cb1745e0053b4a42))

### Features

- **tesseract:** Introduce named multistage time shifts for use with calendars ([#9773](https://github.com/cube-js/cube/issues/9773)) ([e033aaf](https://github.com/cube-js/cube/commit/e033aaf34af65192f981b942d834e33b1e61b851))

## [1.3.36](https://github.com/cube-js/cube/compare/v1.3.35...v1.3.36) (2025-07-10)

### Features

- **tesseract:** Rolling window with custom granularities without date range ([#9762](https://github.com/cube-js/cube/issues/9762)) ([d81c57d](https://github.com/cube-js/cube/commit/d81c57db80e96a211c1d12a67b06cefd868e111e))
- **tesseract:** Rollup Join support ([#9745](https://github.com/cube-js/cube/issues/9745)) ([05bb520](https://github.com/cube-js/cube/commit/05bb52091ad2d3ac09303c0fa50603cd24c99d61))
- **tesseract:** Support for time shifts over calendar cubes dimensions ([#9758](https://github.com/cube-js/cube/issues/9758)) ([70dfa52](https://github.com/cube-js/cube/commit/70dfa52a2aa0cafd80cdf21229d70795392009ec))

## [1.3.35](https://github.com/cube-js/cube/compare/v1.3.34...v1.3.35) (2025-07-09)

### Bug Fixes

- **schema-compiler:** Avoid ambiguous dimension column mappings for ClickHouse queries ([#9674](https://github.com/cube-js/cube/issues/9674)) ([ea2a59f](https://github.com/cube-js/cube/commit/ea2a59fa708e8739f1c6052ac9896f0d01819470))

## [1.3.34](https://github.com/cube-js/cube/compare/v1.3.33...v1.3.34) (2025-07-04)

### Bug Fixes

- **schema-compiler:** Fix pre-agg partitions creation for Ksql ([#9750](https://github.com/cube-js/cube/issues/9750)) ([f08f19b](https://github.com/cube-js/cube/commit/f08f19b2a78105bd5fcc06de3d156767eb483908))

## [1.3.33](https://github.com/cube-js/cube/compare/v1.3.32...v1.3.33) (2025-07-03)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.32](https://github.com/cube-js/cube/compare/v1.3.31...v1.3.32) (2025-07-03)

### Features

- **tesseract:** MultiStage rolling window ([#9747](https://github.com/cube-js/cube/issues/9747)) ([4f5e5dc](https://github.com/cube-js/cube/commit/4f5e5dcd47e93be237e856d672e7df3c3a2f3d6f))

## [1.3.31](https://github.com/cube-js/cube/compare/v1.3.30...v1.3.31) (2025-07-02)

### Bug Fixes

- **schema-compiler:** Fix date filter truncation for rolling window queries ([#9743](https://github.com/cube-js/cube/issues/9743)) ([f045af9](https://github.com/cube-js/cube/commit/f045af91086dafad80fe363a4ddba6f6411892f7))

### Features

- **tesseract:** Support custom granularities in to_date rolling windows ([#9739](https://github.com/cube-js/cube/issues/9739)) ([a8b611c](https://github.com/cube-js/cube/commit/a8b611c877dfa87edc14652af6d3ebeafc071bd2))

## [1.3.30](https://github.com/cube-js/cube/compare/v1.3.29...v1.3.30) (2025-07-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.29](https://github.com/cube-js/cube/compare/v1.3.28...v1.3.29) (2025-07-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.28](https://github.com/cube-js/cube/compare/v1.3.27...v1.3.28) (2025-06-30)

### Bug Fixes

- **tesseract:** MultiStage group_by and reduce_by now work with views ([#9710](https://github.com/cube-js/cube/issues/9710)) ([f17f49a](https://github.com/cube-js/cube/commit/f17f49aba5a0e84361147514e6e45d708569dba3))

## [1.3.27](https://github.com/cube-js/cube/compare/v1.3.26...v1.3.27) (2025-06-30)

### Bug Fixes

- **cubejs-schema-compiler:** Stay unchanged `__user` / `__cubejoinfield` names in aliasing ([#8303](https://github.com/cube-js/cube/issues/8303)) ([7bb4bdc](https://github.com/cube-js/cube/commit/7bb4bdc3f6b2d67a6f8263730f84fc3289b08347))
- **schema-compiler:** Fix BigQuery rolling window time series queries ([#9718](https://github.com/cube-js/cube/issues/9718)) ([1f6cf8f](https://github.com/cube-js/cube/commit/1f6cf8fe44c7cd802ef47785a34e06c23fb18829))
- **schema-compiler:** Use member expression definition as measure key ([#9154](https://github.com/cube-js/cube/issues/9154)) ([e478d0e](https://github.com/cube-js/cube/commit/e478d0ea20e33f383b624a4dbf4cff14359336f8))
- **tesseracrt:** Fix filter params casting for BigQuery dialect ([#9720](https://github.com/cube-js/cube/issues/9720)) ([7b48c27](https://github.com/cube-js/cube/commit/7b48c275ecb4b18ed6262f23866760f46420e099))

### Features

- **tesseract:** Support calendar cubes and custom sql granularities ([#9698](https://github.com/cube-js/cube/issues/9698)) ([40b3708](https://github.com/cube-js/cube/commit/40b3708ce053fd5ea1664bea70cdc613cf343810))

## [1.3.26](https://github.com/cube-js/cube/compare/v1.3.25...v1.3.26) (2025-06-25)

### Bug Fixes

- **schema-compiler:** Fix incorrect truncated time dimensions over time series queries for BigQuery ([#9615](https://github.com/cube-js/cube/issues/9615)) ([b075966](https://github.com/cube-js/cube/commit/b075966a6882c70a8f21652fca83cca74611e632))

## [1.3.25](https://github.com/cube-js/cube/compare/v1.3.24...v1.3.25) (2025-06-24)

### Features

- **tesseract:** Athena support ([#9707](https://github.com/cube-js/cube/issues/9707)) ([a35a477](https://github.com/cube-js/cube/commit/a35a47785fbdc98ed1f9153df3fcdda28d5a7dd0))

## [1.3.24](https://github.com/cube-js/cube/compare/v1.3.23...v1.3.24) (2025-06-24)

### Bug Fixes

- **schema-compiler:** Correct join hints collection for transitive joins ([#9671](https://github.com/cube-js/cube/issues/9671)) ([f60b4aa](https://github.com/cube-js/cube/commit/f60b4aa99285fb5e0c8a80f28837cbbdb5d634dd))
- **schema-compiler:** Fix BigQuery queries datetime/timestamp comparisons ([#9683](https://github.com/cube-js/cube/issues/9683)) ([525932c](https://github.com/cube-js/cube/commit/525932cb9d28d333a94c678f389334832f3a3ba8))

### Features

- **schema-compiler:** Allow to specify td with granularity in REST API query order section ([#9630](https://github.com/cube-js/cube/issues/9630)) ([ba13bd3](https://github.com/cube-js/cube/commit/ba13bd369607f4672802933c015e82169e3f6876))

## [1.3.23](https://github.com/cube-js/cube/compare/v1.3.22...v1.3.23) (2025-06-19)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.22](https://github.com/cube-js/cube/compare/v1.3.21...v1.3.22) (2025-06-18)

### Bug Fixes

- **cubejs-cli:** Fix validate command ([#9666](https://github.com/cube-js/cube/issues/9666)) ([b2bc99f](https://github.com/cube-js/cube/commit/b2bc99f3f29a8ba3ad1b07ded6379881668f596a))
- **schema-compiler:** Case insensitive filter for ClickHouse ([#9373](https://github.com/cube-js/cube/issues/9373)) ([273d277](https://github.com/cube-js/cube/commit/273d277e1058feff36796c48cf0fb315a8211ced))
- **schema-compiler:** Fix Access Policy inheritance ([#9648](https://github.com/cube-js/cube/issues/9648)) ([896af5e](https://github.com/cube-js/cube/commit/896af5eaeccec00c88463fa518e98bf374acdc9b))
- **tesseract:** Fix rolling window external pre-aggregation ([#9625](https://github.com/cube-js/cube/issues/9625)) ([aae3b05](https://github.com/cube-js/cube/commit/aae3b05f49222009f57e407c52d7288bb33b9b8a))
- **tesseract:** Fix rolling window with few time dimensions, filter_group in segments and member expressions ([#9673](https://github.com/cube-js/cube/issues/9673)) ([98d334b](https://github.com/cube-js/cube/commit/98d334bb8ee4debe49b428c92581f63596f3f56c))

### Features

- **schema-compiler:** Add support for time dimensions with granularities in multi-stage measures add_group_by ([#9657](https://github.com/cube-js/cube/issues/9657)) ([6700b43](https://github.com/cube-js/cube/commit/6700b432cc22d71b4b8ef650e835ba0cb33cf91c))

## [1.3.21](https://github.com/cube-js/cube/compare/v1.3.20...v1.3.21) (2025-06-10)

### Bug Fixes

- **schema-compiler:** Fix pre-aggregation for time dimension matching ([#9669](https://github.com/cube-js/cube/issues/9669)) ([0914e1e](https://github.com/cube-js/cube/commit/0914e1ed89b7d1cf8f3bf8dc19858aeaf4b779a7))

## [1.3.20](https://github.com/cube-js/cube/compare/v1.3.19...v1.3.20) (2025-06-06)

### Bug Fixes

- **schema-compiler:** Support for `export function xxx()` during transpilation ([#9645](https://github.com/cube-js/cube/issues/9645)) ([9bcb5a1](https://github.com/cube-js/cube/commit/9bcb5a16ae0c3f1b36d80ce4fcdfab20586b0b8a))

### Features

- **cubesql:** Support `PERCENTILE_CONT` SQL push down ([#8697](https://github.com/cube-js/cube/issues/8697)) ([577a09f](https://github.com/cube-js/cube/commit/577a09f498085ca5a7950467e602dee54691e88e))
- **trino-driver:** Add special testConnection for Trino ([#9634](https://github.com/cube-js/cube/issues/9634)) ([ae10a76](https://github.com/cube-js/cube/commit/ae10a767636322a7fcff21ca6b648b4a0374aad2))

## [1.3.19](https://github.com/cube-js/cube/compare/v1.3.18...v1.3.19) (2025-06-02)

### Features

- Expose aliasMember for hierarchy in View ([#9636](https://github.com/cube-js/cube/issues/9636)) ([737caab](https://github.com/cube-js/cube/commit/737caabf2a43bc28ea0ad90085f44ffbaa1b292b))
- **schema-compiler:** add quarter granularity support in SqliteQuery using CASE expression ([#9633](https://github.com/cube-js/cube/issues/9633)) ([c7ae936](https://github.com/cube-js/cube/commit/c7ae9365eaf333e995d2536101ebe7dec1daf16a))

## [1.3.18](https://github.com/cube-js/cube/compare/v1.3.17...v1.3.18) (2025-05-27)

### Bug Fixes

- **schema-compiler:** exclude time dimensions w/o granularities from select list in base queries ([#9614](https://github.com/cube-js/cube/issues/9614)) ([c9ebfbc](https://github.com/cube-js/cube/commit/c9ebfbca8a791ba917b20c691476a336f92374c7))

## [1.3.17](https://github.com/cube-js/cube/compare/v1.3.16...v1.3.17) (2025-05-22)

### Bug Fixes

- **schema-compiler:** Avoid mutating context on first occasion of TD with granularity ([#9592](https://github.com/cube-js/cube/issues/9592)) ([93027d8](https://github.com/cube-js/cube/commit/93027d8bcb7f0e76d25679aeccad446ee9d265ad))
- **schema-compiler:** Fix rolling window queries with expressions from SQL API ([#9603](https://github.com/cube-js/cube/issues/9603)) ([43f47d8](https://github.com/cube-js/cube/commit/43f47d890a5c17416dd818b1712d54cb958ee95c))

## [1.3.16](https://github.com/cube-js/cube/compare/v1.3.15...v1.3.16) (2025-05-19)

### Bug Fixes

- **schema-compiler:** Collect join hints from subquery join conditions ([#9554](https://github.com/cube-js/cube/issues/9554)) ([cbf0bfd](https://github.com/cube-js/cube/commit/cbf0bfddafc8629ce7d09d9b73d8b60da6d7bafb))

### Features

- **tesseract:** Initial BigQuery support ([#9577](https://github.com/cube-js/cube/issues/9577)) ([60ad2f0](https://github.com/cube-js/cube/commit/60ad2f034d760220c81f7ff9794a388ba5dfbdc5))

## [1.3.15](https://github.com/cube-js/cube/compare/v1.3.14...v1.3.15) (2025-05-15)

### Bug Fixes

- **schema-compiler:** Fix timeshift measure queries coming from SQL API ([#9570](https://github.com/cube-js/cube/issues/9570)) ([d94afeb](https://github.com/cube-js/cube/commit/d94afebbbca9de2eb17fdcfcdb43fa9e64ad1735))

## [1.3.14](https://github.com/cube-js/cube/compare/v1.3.13...v1.3.14) (2025-05-13)

### Bug Fixes

- **schema-compiler:** Fix view queries for measure with single timeshift reference without time dimension ([#9565](https://github.com/cube-js/cube/issues/9565)) ([d3c28d4](https://github.com/cube-js/cube/commit/d3c28d48b470d7ee052107ed10d4bce6a324eef2))

### Features

- Rewrite joins from SQL as query-level join hints ([#9561](https://github.com/cube-js/cube/issues/9561)) ([2b2ac1c](https://github.com/cube-js/cube/commit/2b2ac1c47898f4f6bf67ebae658f90b768c63a7a))

## [1.3.13](https://github.com/cube-js/cube/compare/v1.3.12...v1.3.13) (2025-05-12)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.12](https://github.com/cube-js/cube/compare/v1.3.11...v1.3.12) (2025-05-08)

### Bug Fixes

- **query-orchestrator:** Correct local date parsing for partition start/end queries ([#9543](https://github.com/cube-js/cube/issues/9543)) ([20dd8ad](https://github.com/cube-js/cube/commit/20dd8ad10f6ad0fc2538aecd0ff82cd43f6ff680))
- **schema-compiler:** Fix filtering by time measure ([#9544](https://github.com/cube-js/cube/issues/9544)) ([00a589f](https://github.com/cube-js/cube/commit/00a589fa3fb324ee3dde36fead0de7ced3d19e5b))
- **schema-compiler:** Fix rendered references for preaggregations with join paths ([#9528](https://github.com/cube-js/cube/issues/9528)) ([98ef928](https://github.com/cube-js/cube/commit/98ef928ece7385c2b41a359dfe0ebcc78dfaf8ee))
- **schema-compiler:** Fix view queries with timeShift members ([#9556](https://github.com/cube-js/cube/issues/9556)) ([ef3a04b](https://github.com/cube-js/cube/commit/ef3a04b28c9e0b804a2a24fb4046e3d4755e8abe))
- **schema-compiler:** Reject pre-agg if measure is unmultiplied in query but multiplied in pre-agg ([#9541](https://github.com/cube-js/cube/issues/9541)) ([f094d09](https://github.com/cube-js/cube/commit/f094d09e25b606001cbb699b82427dc43f19a6dc))

### Features

- **schema-compiler:** Allow one measure timeShift without time dimension ([#9545](https://github.com/cube-js/cube/issues/9545)) ([320484f](https://github.com/cube-js/cube/commit/320484fc03f0c49efe0ada2ac9d3dce443011ab9))
- **tesseract:** Basic pre-aggregations support ([#9434](https://github.com/cube-js/cube/issues/9434)) ([1deddcc](https://github.com/cube-js/cube/commit/1deddcc4e37795bf2f775770798acefdbeb24c71))

## [1.3.11](https://github.com/cube-js/cube/compare/v1.3.10...v1.3.11) (2025-05-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.10](https://github.com/cube-js/cube/compare/v1.3.9...v1.3.10) (2025-05-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.9](https://github.com/cube-js/cube/compare/v1.3.8...v1.3.9) (2025-04-28)

### Bug Fixes

- **schema-compiler:** Fix non-match for exact custom granularity pre-aggregation ([#9517](https://github.com/cube-js/cube/issues/9517)) ([313a606](https://github.com/cube-js/cube/commit/313a606e1aedb7dda31a88ea78e71e106a2fa884))

### Features

- **schema-compiler:** Support adding pre-aggregations in Rollup Designer for YAML-based models ([#9500](https://github.com/cube-js/cube/issues/9500)) ([1cfe4c2](https://github.com/cube-js/cube/commit/1cfe4c2dd1cc0fe738590abb39fe2485899fc51e))
- **schema-compiler:** Support view extends ([#9507](https://github.com/cube-js/cube/issues/9507)) ([e68530a](https://github.com/cube-js/cube/commit/e68530a51dd9fa58dd85dab0d593a034000a6ed3))

## [1.3.8](https://github.com/cube-js/cube/compare/v1.3.7...v1.3.8) (2025-04-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.7](https://github.com/cube-js/cube/compare/v1.3.6...v1.3.7) (2025-04-23)

### Bug Fixes

- **schema-compiler:** Fix not working timeshift in views ([#9504](https://github.com/cube-js/cube/issues/9504)) ([9319b0d](https://github.com/cube-js/cube/commit/9319b0df50a4026ceb91732de123a21e8d788b39))

### Features

- **schema-compiler:** Custom granularities support for Amazon Athena/Presto dialect ([#9472](https://github.com/cube-js/cube/issues/9472)) ([4c1520e](https://github.com/cube-js/cube/commit/4c1520e90207469cb4d6be41b28e1c06bb2d0646))
- **schema-compiler:** Support custom granularities with Year-Month intervals for AWS Redshift dialect ([#9489](https://github.com/cube-js/cube/issues/9489)) ([2fc53f2](https://github.com/cube-js/cube/commit/2fc53f2d10e94ba6ac1b40bfce956167b89b8caf))
- **schema-compiler:** Support overriding `title`, `description`, `meta`, and `format` on view members ([#9496](https://github.com/cube-js/cube/issues/9496)) ([e0179c5](https://github.com/cube-js/cube/commit/e0179c5644952c6ebe41ab4f1f12784491036bd0))

## [1.3.6](https://github.com/cube-js/cube/compare/v1.3.5...v1.3.6) (2025-04-22)

### Bug Fixes

- **schema-compiler:** Use join paths from pre-aggregation declaration instead of building join tree from scratch ([#9471](https://github.com/cube-js/cube/issues/9471)) ([cd7e9fd](https://github.com/cube-js/cube/commit/cd7e9fddca8fa1c31335adbba6e79c4032841cbb))

## [1.3.5](https://github.com/cube-js/cube/compare/v1.3.4...v1.3.5) (2025-04-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.4](https://github.com/cube-js/cube/compare/v1.3.3...v1.3.4) (2025-04-17)

### Bug Fixes

- **schema-compiler:** Make RefreshKeySql timezone-aware ([#9479](https://github.com/cube-js/cube/issues/9479)) ([fa2adb9](https://github.com/cube-js/cube/commit/fa2adb92e6fd87223f551b61d75f367dc8ea2524))

## [1.3.3](https://github.com/cube-js/cube/compare/v1.3.2...v1.3.3) (2025-04-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.3.2](https://github.com/cube-js/cube/compare/v1.3.1...v1.3.2) (2025-04-16)

### Bug Fixes

- **schema-compiler:** Allow calling cube functions in yaml python blocks ([#9473](https://github.com/cube-js/cube/issues/9473)) ([61ef9da](https://github.com/cube-js/cube/commit/61ef9da243e8219b8ba3ab7ccbf9f9e0403cba04))
- **schema-compiler:** Allow escaping of curly braces in yaml models ([#9469](https://github.com/cube-js/cube/issues/9469)) ([0a8cf07](https://github.com/cube-js/cube/commit/0a8cf07f20351a0e591b658f02f243510bf78eae))

## [1.3.1](https://github.com/cube-js/cube/compare/v1.3.0...v1.3.1) (2025-04-14)

### Bug Fixes

- **schema-compiler:** Fix incorrect transpilation of yaml models ([#9465](https://github.com/cube-js/cube/issues/9465)) ([e732fa2](https://github.com/cube-js/cube/commit/e732fa2b8ead0ca605512611eb371b94e0238dec))

# [1.3.0](https://github.com/cube-js/cube/compare/v1.2.33...v1.3.0) (2025-04-11)

### Bug Fixes

- **schema-compiler:** Do not drop join hints when view member is not itself owned ([#9444](https://github.com/cube-js/cube/issues/9444)) ([b5ddd11](https://github.com/cube-js/cube/commit/b5ddd115a8ad066bc4399fc2608d1d99d3d863dc))
- **schema-compiler:** Fix cubes inheritance ([#9386](https://github.com/cube-js/cube/issues/9386)) ([409d74d](https://github.com/cube-js/cube/commit/409d74db8c5ac5a314e1e3e7b872586f538713d1))
- **schema-compiler:** Fix incorrect native bulk transpilation if there are no JS files ([#9453](https://github.com/cube-js/cube/issues/9453)) ([610820f](https://github.com/cube-js/cube/commit/610820fe15701ad0971b4d317bb112ec7765bb9b))

## [1.2.33](https://github.com/cube-js/cube/compare/v1.2.32...v1.2.33) (2025-04-10)

### Bug Fixes

- **schema-compiler:** ExportNamedDeclaration for variables object ([#9390](https://github.com/cube-js/cube/issues/9390)) ([b16e8dd](https://github.com/cube-js/cube/commit/b16e8dd5d37f96349b78796e525fb432ad2f8dcc))

### Features

- **schema-compiler:** Implement bulk processing for js transpilation in native ([#9427](https://github.com/cube-js/cube/issues/9427)) ([c274b2a](https://github.com/cube-js/cube/commit/c274b2ad7c99f7b794b543d820f4f086cc0e35bf))

## [1.2.32](https://github.com/cube-js/cube/compare/v1.2.31...v1.2.32) (2025-04-08)

### Bug Fixes

- **schema-compiler:** Use join paths from pre-aggregation declaration instead of building join tree from scratch ([#9431](https://github.com/cube-js/cube/issues/9431)) ([aea3a6c](https://github.com/cube-js/cube/commit/aea3a6c02886f495b00751e5e12dcd0ef3099441))
- **tesseract:** Fix issues with member expressions over multi stage ([#9416](https://github.com/cube-js/cube/issues/9416)) ([14ae6a4](https://github.com/cube-js/cube/commit/14ae6a46ca215da8be8c4f5fb12b8f3c01948639))

## [1.2.31](https://github.com/cube-js/cube/compare/v1.2.30...v1.2.31) (2025-04-08)

### Bug Fixes

- **schema-compiler:** Fix BigQuery DATE_ADD push down template for years/quarters/months ([#9432](https://github.com/cube-js/cube/issues/9432)) ([5845c88](https://github.com/cube-js/cube/commit/5845c88dc2d7e482d2a79eb3329ecc302655a493))

### Features

- PatchMeasure member expression ([#9218](https://github.com/cube-js/cube/issues/9218)) ([128280a](https://github.com/cube-js/cube/commit/128280ae02d053b8435388ff2a808a27b773cef1))
- **schema-compiler:** Use LRUCache for js data models compiled vm.Scripts ([#9424](https://github.com/cube-js/cube/issues/9424)) ([e923a0f](https://github.com/cube-js/cube/commit/e923a0fb38f1cdc5bca1256bf868efca46e13cca))

## [1.2.30](https://github.com/cube-js/cube/compare/v1.2.29...v1.2.30) (2025-04-04)

### Bug Fixes

- **tesseract:** Fix for multi fact queries ([#9421](https://github.com/cube-js/cube/issues/9421)) ([5547f50](https://github.com/cube-js/cube/commit/5547f5046c961966b582f76faf1377393ec8557a))

### Features

- **schema-compiler:** generate join sql using dimension refs when po… ([#9413](https://github.com/cube-js/cube/issues/9413)) ([8fc4f7c](https://github.com/cube-js/cube/commit/8fc4f7c4125889fcc24306256665d61ac30198c3))

## [1.2.29](https://github.com/cube-js/cube/compare/v1.2.28...v1.2.29) (2025-04-02)

### Bug Fixes

- **schema-compiler:** Respect ungrouped alias in measures ([#9411](https://github.com/cube-js/cube/issues/9411)) ([f0829d7](https://github.com/cube-js/cube/commit/f0829d730c284a1e53c833cd53ccf8d37b08e095))
- **schema-compiler:** Use segments as-is in rollupPreAggregation ([#9391](https://github.com/cube-js/cube/issues/9391)) ([cb7b643](https://github.com/cube-js/cube/commit/cb7b6438da199daf0b07fb2dd6f5087ed259f71f))

## [1.2.28](https://github.com/cube-js/cube/compare/v1.2.27...v1.2.28) (2025-04-01)

### Bug Fixes

- **schema-compiler:** Fix BigQuery DATETIME_TRUNC() week processing ([#9380](https://github.com/cube-js/cube/issues/9380)) ([6c8564f](https://github.com/cube-js/cube/commit/6c8564ffc15e5e930fa2160be642ea3f3cb7b888))
- **schema-compiler:** Fix date alignment for week-based custom granularities ([#9405](https://github.com/cube-js/cube/issues/9405)) ([8b6c490](https://github.com/cube-js/cube/commit/8b6c4906e2c49fb2c04abc381f6fdb3105309eaf))
- **schema-compiler:** Fix model validation for aliased included members for views ([#9398](https://github.com/cube-js/cube/issues/9398)) ([f5e69f7](https://github.com/cube-js/cube/commit/f5e69f76e7268812aa776a20588875d85077b3bf))
- **schema-compiler:** Skip syntax check for transpiled files ([#9395](https://github.com/cube-js/cube/issues/9395)) ([c624e88](https://github.com/cube-js/cube/commit/c624e88d4793c917bb43c3099a7c9d128b517b8a))
- **tesseract:** Support rolling window with multiple granularities ([#9382](https://github.com/cube-js/cube/issues/9382)) ([6ff9331](https://github.com/cube-js/cube/commit/6ff9331729af59e45385608057fa0e99688cfcbe))

### Features

- **tesseract:** Custom granularities support ([#9400](https://github.com/cube-js/cube/issues/9400)) ([dc93e40](https://github.com/cube-js/cube/commit/dc93e4022d4c2741d0aafcdb1007a69c6181f232))
- **tesseract:** Support for rolling window without date range ([#9364](https://github.com/cube-js/cube/issues/9364)) ([fd94b02](https://github.com/cube-js/cube/commit/fd94b02b8bba286a67264057f9259769215239a9))

## [1.2.27](https://github.com/cube-js/cube/compare/v1.2.26...v1.2.27) (2025-03-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.26](https://github.com/cube-js/cube/compare/v1.2.25...v1.2.26) (2025-03-21)

### Bug Fixes

- **schema-compiler:** Fix doubled calls to convertTz() for compound time dimensions with custom granularities ([#9369](https://github.com/cube-js/cube/issues/9369)) ([0dc8320](https://github.com/cube-js/cube/commit/0dc83209ee8d4d38ebaa6be6c48a07e146c4a7b7))

## [1.2.25](https://github.com/cube-js/cube/compare/v1.2.24...v1.2.25) (2025-03-20)

### Bug Fixes

- **schema-compiler:** Handle measures with dimension-only member expressions ([#9335](https://github.com/cube-js/cube/issues/9335)) ([2f7a128](https://github.com/cube-js/cube/commit/2f7a1288108ea3a62b295c08dcf756bfec76d784))
- **schema-compiler:** support unary operators in DAP filters ([#9366](https://github.com/cube-js/cube/issues/9366)) ([fff8af2](https://github.com/cube-js/cube/commit/fff8af2bfa25098e8ed20b4e4848f3926bba70b8))

### Features

- **server-core,api-gateway:** Allow manual rebuilding pre-aggregation partitions within date Range ([#9342](https://github.com/cube-js/cube/issues/9342)) ([b5701e3](https://github.com/cube-js/cube/commit/b5701e35a92769e23acdbb2fe72549731cd21aba))

## [1.2.24](https://github.com/cube-js/cube/compare/v1.2.23...v1.2.24) (2025-03-18)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.23](https://github.com/cube-js/cube/compare/v1.2.22...v1.2.23) (2025-03-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.22](https://github.com/cube-js/cube/compare/v1.2.21...v1.2.22) (2025-03-14)

### Features

- **tesseract:** Segments and MemberExpressions segments support ([#9336](https://github.com/cube-js/cube/issues/9336)) ([b2c03e7](https://github.com/cube-js/cube/commit/b2c03e7644f954ba29201b4c98de35ded7eebe79))

## [1.2.21](https://github.com/cube-js/cube/compare/v1.2.20...v1.2.21) (2025-03-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.20](https://github.com/cube-js/cube/compare/v1.2.19...v1.2.20) (2025-03-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.19](https://github.com/cube-js/cube/compare/v1.2.18...v1.2.19) (2025-03-08)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.18](https://github.com/cube-js/cube/compare/v1.2.17...v1.2.18) (2025-03-06)

### Bug Fixes

- **query-orchestrator:** Fix improper pre-aggregation buildRange construction for non UTC timezones ([#9284](https://github.com/cube-js/cube/issues/9284)) ([ef12d8d](https://github.com/cube-js/cube/commit/ef12d8d02702df7dcc1e6531c1b0aee6afa576ef))
- **tesseract:** Don't generate COALESCE with single argument ([#9300](https://github.com/cube-js/cube/issues/9300)) ([3174be1](https://github.com/cube-js/cube/commit/3174be17cd1d9ba6aa1c0ec3ccbd0c5426342dc0))
- **tesseract:** fix wrong default alias for symbols with digits in name ([#9299](https://github.com/cube-js/cube/issues/9299)) ([329d228](https://github.com/cube-js/cube/commit/329d2289e115a5f71714755323a8b8a92a260519))

## [1.2.17](https://github.com/cube-js/cube/compare/v1.2.16...v1.2.17) (2025-03-05)

### Bug Fixes

- **schema-compiler:** Fix ORDER BY clause generation for queries with td with filters ([#9296](https://github.com/cube-js/cube/issues/9296)) ([1c8ce4f](https://github.com/cube-js/cube/commit/1c8ce4f475a0327fd4dc8fedf24ac9979aa2bffd))

## [1.2.16](https://github.com/cube-js/cube/compare/v1.2.15...v1.2.16) (2025-03-04)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.15](https://github.com/cube-js/cube/compare/v1.2.14...v1.2.15) (2025-03-03)

### Features

- **schema-compiler:** auto include hierarchy dimensions ([#9288](https://github.com/cube-js/cube/issues/9288)) ([162c5b4](https://github.com/cube-js/cube/commit/162c5b48f6ef489bf9f4bb95a1d4bca168b836c9))
- **tesseract:** Pre-aggregation planning fallback ([#9230](https://github.com/cube-js/cube/issues/9230)) ([08650e2](https://github.com/cube-js/cube/commit/08650e241e2aa85705e3c68e4993098267f89c82))

## [1.2.14](https://github.com/cube-js/cube/compare/v1.2.13...v1.2.14) (2025-02-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.13](https://github.com/cube-js/cube/compare/v1.2.12...v1.2.13) (2025-02-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.12](https://github.com/cube-js/cube/compare/v1.2.11...v1.2.12) (2025-02-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.11](https://github.com/cube-js/cube/compare/v1.2.10...v1.2.11) (2025-02-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.10](https://github.com/cube-js/cube/compare/v1.2.9...v1.2.10) (2025-02-24)

### Bug Fixes

- **schema-compiler:** Fix sql generation for rolling_window queries with multiple time dimensions ([#9124](https://github.com/cube-js/cube/issues/9124)) ([52a664e](https://github.com/cube-js/cube/commit/52a664e4d0643d78464f75cc48c4a1f686455ebe))

## [1.2.9](https://github.com/cube-js/cube/compare/v1.2.8...v1.2.9) (2025-02-21)

### Bug Fixes

- **schema-compiler:** Fix allowNonStrictDateRangeMatch pre-agg match for dateRange queries without granularities ([#9258](https://github.com/cube-js/cube/issues/9258)) ([00fe682](https://github.com/cube-js/cube/commit/00fe6825418e7ad6681b0856fee11175dfcc649e))

## [1.2.8](https://github.com/cube-js/cube/compare/v1.2.7...v1.2.8) (2025-02-21)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.7](https://github.com/cube-js/cube/compare/v1.2.6...v1.2.7) (2025-02-20)

### Bug Fixes

- **schema-compiler:** hierarchies to respect prefix value ([#9239](https://github.com/cube-js/cube/issues/9239)) ([19d1b33](https://github.com/cube-js/cube/commit/19d1b33b7df6a98198aadfa25e290fd33b89ad33))

## [1.2.6](https://github.com/cube-js/cube/compare/v1.2.5...v1.2.6) (2025-02-18)

### Bug Fixes

- **schema-compiler:** Correct models transpilation in native in multitenant environments ([#9234](https://github.com/cube-js/cube/issues/9234)) ([84f90c0](https://github.com/cube-js/cube/commit/84f90c07ee3827e6f3652dd6c9fab0993ecc8150))

### Features

- **schema-compiler:** Boost models transpilation 10-13x times (using SWC instead of Babel) ([#9225](https://github.com/cube-js/cube/issues/9225)) ([2dd9a4a](https://github.com/cube-js/cube/commit/2dd9a4a5ba0f178e304befc476609fc30ada8975))

## [1.2.5](https://github.com/cube-js/cube/compare/v1.2.4...v1.2.5) (2025-02-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.4](https://github.com/cube-js/cube/compare/v1.2.3...v1.2.4) (2025-02-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.3](https://github.com/cube-js/cube/compare/v1.2.2...v1.2.3) (2025-02-06)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.2.2](https://github.com/cube-js/cube/compare/v1.2.1...v1.2.2) (2025-02-06)

### Bug Fixes

- **schema-compiler:** Fix queries with time dimensions without granularities don't hit pre-aggregations with allow_non_strict_date_range_match=true ([#9102](https://github.com/cube-js/cube/issues/9102)) ([eff0736](https://github.com/cube-js/cube/commit/eff073629261c687416a92b8876e46a522bdd1d8))

## [1.2.1](https://github.com/cube-js/cube/compare/v1.2.0...v1.2.1) (2025-02-06)

### Features

- **schema-compiler:** Move transpiling to worker threads (under the flag) ([#9188](https://github.com/cube-js/cube/issues/9188)) ([7451452](https://github.com/cube-js/cube/commit/745145283b42c891e0e063c4cad1e2102c71616a))
- **tesseract:** Subqueries support ([#9108](https://github.com/cube-js/cube/issues/9108)) ([0dbf3bb](https://github.com/cube-js/cube/commit/0dbf3bb866b731f97b5588b0e620c3be7cbf9e5b))

# [1.2.0](https://github.com/cube-js/cube/compare/v1.1.18...v1.2.0) (2025-02-05)

### Bug Fixes

- **schema-compiler:** make sure view members are resolvable in DAP ([#9059](https://github.com/cube-js/cube/issues/9059)) ([e7b992e](https://github.com/cube-js/cube/commit/e7b992effb6ef90883d059280270c92d903607d4))

### Features

- Support complex join conditions for grouped joins ([#9157](https://github.com/cube-js/cube/issues/9157)) ([28c1e3b](https://github.com/cube-js/cube/commit/28c1e3bba7a100f3152bfdefd86197e818fac941))

## [1.1.17](https://github.com/cube-js/cube/compare/v1.1.16...v1.1.17) (2025-01-27)

### Features

- **schema-compiler:** Add flag for using named timezones in MySQL Query class ([#9111](https://github.com/cube-js/cube/issues/9111)) ([5a540db](https://github.com/cube-js/cube/commit/5a540db9228dcbb88c434123f13291202f6da9be))

## [1.1.16](https://github.com/cube-js/cube/compare/v1.1.15...v1.1.16) (2025-01-22)

### Bug Fixes

- **schema-compiler:** hierarchies on extended cubes ([#9100](https://github.com/cube-js/cube/issues/9100)) ([c8f24f7](https://github.com/cube-js/cube/commit/c8f24f70131f0dc7138769837f8effa812998169))
- **schema-compiler:** update hierarchies handling to use object pattern ([#9121](https://github.com/cube-js/cube/issues/9121)) ([42cbc8e](https://github.com/cube-js/cube/commit/42cbc8e51fd38b6ca7ade546d638408c06aeafb2))

### Features

- Initial support for grouped join pushdown ([#9032](https://github.com/cube-js/cube/issues/9032)) ([2f11d20](https://github.com/cube-js/cube/commit/2f11d2050ab1e2fc7f0a37012d5d45592f01938e))

## [1.1.15](https://github.com/cube-js/cube/compare/v1.1.14...v1.1.15) (2025-01-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.1.14](https://github.com/cube-js/cube/compare/v1.1.13...v1.1.14) (2025-01-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.1.13](https://github.com/cube-js/cube/compare/v1.1.12...v1.1.13) (2025-01-09)

### Bug Fixes

- **schema-compiler:** Handle member expressions in keyDimensions ([#9083](https://github.com/cube-js/cube/issues/9083)) ([056a1d8](https://github.com/cube-js/cube/commit/056a1d88b4fd8055575dbb07967bc64b7065ce86))

## [1.1.12](https://github.com/cube-js/cube/compare/v1.1.11...v1.1.12) (2025-01-09)

### Bug Fixes

- **schema-compiler:** fix time dimension granularity origin formatting in local timezone ([#9071](https://github.com/cube-js/cube/issues/9071)) ([c97526f](https://github.com/cube-js/cube/commit/c97526ff9145195786d4ec9617bcf298567bce7e))

### Features

- **cubesql:** Penalize zero members in wrapper ([#8927](https://github.com/cube-js/cube/issues/8927)) ([171ea35](https://github.com/cube-js/cube/commit/171ea351e739f705ddbf0d803a34b944cb8c9da5))
- **tesseract:** Make measure an entry point for multi-fact join for now and support template for params ([#9053](https://github.com/cube-js/cube/issues/9053)) ([cb507c1](https://github.com/cube-js/cube/commit/cb507c1ab345828435d192d1f783998bbc5130b3))

## [1.1.11](https://github.com/cube-js/cube/compare/v1.1.10...v1.1.11) (2024-12-16)

### Bug Fixes

- TypeError: Cannot read properties of undefined (reading 'joins') ([14adaeb](https://github.com/cube-js/cube/commit/14adaebdd1c3d398bcd2997012da070999e47d9d))

## [1.1.10](https://github.com/cube-js/cube/compare/v1.1.9...v1.1.10) (2024-12-16)

### Bug Fixes

- **schema-compiler:** join relationship aliases ([ad4e8e3](https://github.com/cube-js/cube/commit/ad4e8e3872307ab77e035709e5208b0191f87f5b))

### Features

- **tesseract:** Support multiple join paths within single query ([#9047](https://github.com/cube-js/cube/issues/9047)) ([b62446e](https://github.com/cube-js/cube/commit/b62446e3c3893068f8dd8aa32d7204ea06a16f98))

## [1.1.9](https://github.com/cube-js/cube/compare/v1.1.8...v1.1.9) (2024-12-08)

### Features

- **schema-compiler:** folders support, hierarchies improvements ([#9018](https://github.com/cube-js/cube/issues/9018)) ([2012281](https://github.com/cube-js/cube/commit/20122810f508d978921b56f8d9b8a4e479290d56))

## [1.1.8](https://github.com/cube-js/cube/compare/v1.1.7...v1.1.8) (2024-12-05)

### Bug Fixes

- **schema-compiler:** fix FILTER_PARAMS to populate set and notSet filters in cube's SQL query ([#8937](https://github.com/cube-js/cube/issues/8937)) ([6d82f18](https://github.com/cube-js/cube/commit/6d82f18c59f92cae2bb94fe754aa3c0431c24949)), closes [#9009](https://github.com/cube-js/cube/issues/9009) [#8793](https://github.com/cube-js/cube/issues/8793)
- **schema-compiler:** Fix incorrect sql generation for view queries referencing measures from joined cubes ([#8991](https://github.com/cube-js/cube/issues/8991)) ([cd7490e](https://github.com/cube-js/cube/commit/cd7490ef0d5bf3fa1b81b7bf1d820b12a04b1e99))
- **schema-compiler:** Fix ungrouped cumulative queries incorrect sql generation ([#8981](https://github.com/cube-js/cube/issues/8981)) ([82ba01a](https://github.com/cube-js/cube/commit/82ba01ae3695dc7469e76dd8bced1c532bea0865))
- **schema-compiler:** Undefined columns for lambda pre-aggregation queries referencing dimensions from joined cubes ([#8946](https://github.com/cube-js/cube/issues/8946)) ([41b49f4](https://github.com/cube-js/cube/commit/41b49f4fc00f30da7c30d59f522970a24b0e1426))
- **schema-compiler:** use query timezone for time granularity origin ([#8936](https://github.com/cube-js/cube/issues/8936)) ([1beba2d](https://github.com/cube-js/cube/commit/1beba2d3fa6e9a7d95c8c18b448e42fd1562a7e6))

### Features

- **sqlplanner:** Extract alias logic from the symbols ([#8919](https://github.com/cube-js/cube/issues/8919)) ([d1b07f1](https://github.com/cube-js/cube/commit/d1b07f1029aa50bd391ba9e52bd1b575c3659da9))

## [1.1.7](https://github.com/cube-js/cube/compare/v1.1.6...v1.1.7) (2024-11-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.1.6](https://github.com/cube-js/cube/compare/v1.1.5...v1.1.6) (2024-11-17)

### Bug Fixes

- **schema-compiler:** Time dimension filter for lambda cubes ([#8957](https://github.com/cube-js/cube/issues/8957)) ([eca7e4d](https://github.com/cube-js/cube/commit/eca7e4daa47056f137a3bc0e5735214dba8c8869))

## [1.1.5](https://github.com/cube-js/cube/compare/v1.1.4...v1.1.5) (2024-11-13)

### Bug Fixes

- **schema-compiler:** fix Maximum call stack size exceeded if FILTER_PARAMS are used inside dimensions/measures ([#8867](https://github.com/cube-js/cube/issues/8867)) ([8e7c5c7](https://github.com/cube-js/cube/commit/8e7c5c7d0af0e672f23c8efdab0117121838fa3f))

## [1.1.4](https://github.com/cube-js/cube/compare/v1.1.3...v1.1.4) (2024-11-12)

### Features

- **api-gateway:** Meta - expose aliasMember for members in View ([#8945](https://github.com/cube-js/cube/issues/8945)) ([c127f36](https://github.com/cube-js/cube/commit/c127f36a124edb24ba31c043760b2883485b79f1))

## [1.1.3](https://github.com/cube-js/cube/compare/v1.1.2...v1.1.3) (2024-11-08)

### Bug Fixes

- **cubesql:** Fix `NULLS FIRST`/`LAST` SQL push down for several dialects ([#8895](https://github.com/cube-js/cube/issues/8895)) ([61c5ac6](https://github.com/cube-js/cube/commit/61c5ac618c9b68cf1185625d77420ed4a2c5da54))

## [1.1.2](https://github.com/cube-js/cube/compare/v1.1.1...v1.1.2) (2024-11-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.1.1](https://github.com/cube-js/cube/compare/v1.1.0...v1.1.1) (2024-10-31)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [1.1.0](https://github.com/cube-js/cube/compare/v1.0.4...v1.1.0) (2024-10-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.0.4](https://github.com/cube-js/cube/compare/v1.0.3...v1.0.4) (2024-10-23)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.0.3](https://github.com/cube-js/cube/compare/v1.0.2...v1.0.3) (2024-10-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [1.0.2](https://github.com/cube-js/cube/compare/v1.0.1...v1.0.2) (2024-10-21)

### Features

- **sqlplanner:** Base multi staging support ([#8832](https://github.com/cube-js/cube/issues/8832)) ([20ed3e0](https://github.com/cube-js/cube/commit/20ed3e0ac23d3664372b9ee4b5450b0c35f04e91))

## [1.0.1](https://github.com/cube-js/cube/compare/v1.0.0...v1.0.1) (2024-10-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [1.0.0](https://github.com/cube-js/cube/compare/v0.36.11...v1.0.0) (2024-10-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.36.11](https://github.com/cube-js/cube/compare/v0.36.10...v0.36.11) (2024-10-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.36.10](https://github.com/cube-js/cube/compare/v0.36.9...v0.36.10) (2024-10-14)

### Features

- New style RBAC framework ([#8766](https://github.com/cube-js/cube/issues/8766)) ([ad81453](https://github.com/cube-js/cube/commit/ad81453757788784c785c7a75202f94a74ab4c26))

## [0.36.9](https://github.com/cube-js/cube/compare/v0.36.8...v0.36.9) (2024-10-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.36.8](https://github.com/cube-js/cube/compare/v0.36.7...v0.36.8) (2024-10-11)

### Bug Fixes

- Render LIMIT 0 and OFFSET 0 properly in SQL templates ([#8781](https://github.com/cube-js/cube/issues/8781)) ([6b17731](https://github.com/cube-js/cube/commit/6b17731f84aa494de820dce791e3120e4282bc37))

### Features

- **prestodb-driver:** Added support export bucket, gcs only ([#8797](https://github.com/cube-js/cube/issues/8797)) ([8c7dcbb](https://github.com/cube-js/cube/commit/8c7dcbbd88de9f977eb75cf92c9b8decf0218643))
- Renaming of Post Aggregate into Multi Stage ([#8798](https://github.com/cube-js/cube/issues/8798)) ([bd42c44](https://github.com/cube-js/cube/commit/bd42c44c48f571bdf6a5f097854f3a022e85278a))

## [0.36.7](https://github.com/cube-js/cube/compare/v0.36.6...v0.36.7) (2024-10-08)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.36.6](https://github.com/cube-js/cube/compare/v0.36.5...v0.36.6) (2024-10-03)

### Bug Fixes

- **schema-compiler:** Support minutes and seconds for cubestore format interval ([#8773](https://github.com/cube-js/cube/issues/8773)) ([a7a515d](https://github.com/cube-js/cube/commit/a7a515d6d419dfc576b74968cb40b7cf5d9af751))

## [0.36.5](https://github.com/cube-js/cube/compare/v0.36.4...v0.36.5) (2024-10-02)

### Bug Fixes

- **schema-compiler:** fix FILTER_PARAMS flow in pre aggregations filtering ([#8761](https://github.com/cube-js/cube/issues/8761)) ([41ed9cc](https://github.com/cube-js/cube/commit/41ed9cc2e19f2ab68cdb76e5b6bedcddd4fd13f0))

### Features

- **cubesqlplanner:** Base joins support ([#8656](https://github.com/cube-js/cube/issues/8656)) ([a2e604e](https://github.com/cube-js/cube/commit/a2e604eef41d3b3f079f1a8e49d95b1b1d6ce846))

## [0.36.4](https://github.com/cube-js/cube/compare/v0.36.3...v0.36.4) (2024-09-27)

### Bug Fixes

- **schema-compiler:** Warn when `COUNT(*)` is used with a cube/view where `count` is not defined ([#8667](https://github.com/cube-js/cube/issues/8667)) ([4739d94](https://github.com/cube-js/cube/commit/4739d946ca0c6358210fdc81cc2a1906fcffc7f8))

### Features

- **schema-compiler:** expose custom granularities details via meta API and query annotation ([#8740](https://github.com/cube-js/cube/issues/8740)) ([c58e97a](https://github.com/cube-js/cube/commit/c58e97ac1a268845c32f8d5f35ac292324f5fd28))

## [0.36.3](https://github.com/cube-js/cube/compare/v0.36.2...v0.36.3) (2024-09-26)

### Bug Fixes

- **schema-compiler:** fix FILTER_PARAMS propagation from view to cube's SQL query ([#8721](https://github.com/cube-js/cube/issues/8721)) ([ec2c2ec](https://github.com/cube-js/cube/commit/ec2c2ec4d057dd1b29748d2b3847d7b60f96d5c8))

## [0.36.2](https://github.com/cube-js/cube/compare/v0.36.1...v0.36.2) (2024-09-18)

### Bug Fixes

- **schema-compiler:** correct origin timestamp initialization in Granularity instance ([#8710](https://github.com/cube-js/cube/issues/8710)) ([11e941d](https://github.com/cube-js/cube/commit/11e941d822b60063d46d706432e552ad200983dd))

### Features

- **cubesql:** Support `[I]LIKE ... ESCAPE ...` SQL push down ([2bda0dd](https://github.com/cube-js/cube/commit/2bda0dd968944e777c5b89b2587a620c448dba10))
- **schema-compiler:** exact match preagg with custom granularity without the need for allow_non_strict_date_range_match option ([#8712](https://github.com/cube-js/cube/issues/8712)) ([47c4d78](https://github.com/cube-js/cube/commit/47c4d78d6334ec8a279b42a7c0940ac778654dc8))
- **schema-compiler:** expose custom granularities via meta API ([#8703](https://github.com/cube-js/cube/issues/8703)) ([4875b8e](https://github.com/cube-js/cube/commit/4875b8eb1c2b9c807156725e77d834edcf7b2632))

## [0.36.1](https://github.com/cube-js/cube/compare/v0.36.0...v0.36.1) (2024-09-16)

### Bug Fixes

- **schema-compiler:** Add missing “quarter” time unit to granularity definitions ([#8708](https://github.com/cube-js/cube/issues/8708)) ([e8d81f2](https://github.com/cube-js/cube/commit/e8d81f2e04ce61215b1185d9adc4320233bc34da))

### Features

- **schema-compiler:** Reference granularities in a proxy dimension ([#8664](https://github.com/cube-js/cube/issues/8664)) ([b7674f3](https://github.com/cube-js/cube/commit/b7674f35aa0a22efe46b0142bd0a42eeb39fc02a))

# [0.36.0](https://github.com/cube-js/cube/compare/v0.35.81...v0.36.0) (2024-09-13)

- chore!: Remove support for USER_CONTEXT (#8705) ([8a796f8](https://github.com/cube-js/cube/commit/8a796f838a0b638fda079377b42d2cbf86474315)), closes [#8705](https://github.com/cube-js/cube/issues/8705)
- chore!: Support for Node.js 16 was removed ([8b83021](https://github.com/cube-js/cube/commit/8b830214ab3d16ebfadc65cb9587a08b0496fb93))

### Features

- **schema-compiler:** custom granularity support ([#8537](https://github.com/cube-js/cube/issues/8537)) ([2109849](https://github.com/cube-js/cube/commit/21098494353b9b6274b534b6c656154cb8451c7b))
- **schema-compiler:** Support date_bin function in Cube Store queries ([#8684](https://github.com/cube-js/cube/issues/8684)) ([f7c07a7](https://github.com/cube-js/cube/commit/f7c07a7572c95de26db81308194577b32e289e53))

### BREAKING CHANGES

- This functionality was deprecated starting from v0.26.0. Please migrate to SECURITY_CONTEXT.
- Node.js is EOL, it was deprecated in v0.35.0

## [0.35.81](https://github.com/cube-js/cube/compare/v0.35.80...v0.35.81) (2024-09-12)

### Bug Fixes

- **api-gateway:** fixes an issue where queries to get the total count of results were incorrectly applying sorting from the original query and also were getting default ordering applied when the query ordering was stripped out ([#8060](https://github.com/cube-js/cube/issues/8060)) Thanks [@rdwoodring](https://github.com/rdwoodring)! ([863f370](https://github.com/cube-js/cube/commit/863f3709e97c904f1c800ad98889dc272dbfddbd))

### Features

- ksql and rollup pre-aggregations ([#8619](https://github.com/cube-js/cube/issues/8619)) ([cdfbd1e](https://github.com/cube-js/cube/commit/cdfbd1e21ffcf111e40c525f8a391cc0dcee3c11))

## [0.35.80](https://github.com/cube-js/cube/compare/v0.35.79...v0.35.80) (2024-09-09)

### Bug Fixes

- **schema-compiler:** propagate FILTER_PARAMS from view to inner cube's SELECT ([#8466](https://github.com/cube-js/cube/issues/8466)) ([c0466fd](https://github.com/cube-js/cube/commit/c0466fde9b7a3834159d7ec592362edcab6d9795))

## [0.35.79](https://github.com/cube-js/cube/compare/v0.35.78...v0.35.79) (2024-09-04)

### Bug Fixes

- **schema-compiler:** correct string casting for BigQuery ([#8651](https://github.com/cube-js/cube/issues/8651)) ([f6ac9c2](https://github.com/cube-js/cube/commit/f6ac9c2255091b27fc4ac5c2434bf7de7248afb2))
- **schema-compiler:** correct string casting for MySQL ([#8650](https://github.com/cube-js/cube/issues/8650)) ([9408bb4](https://github.com/cube-js/cube/commit/9408bb43a7a63134894f0a7c4e1883e267b051a1))
- **schema-compiler:** correct string concatenation and casting for MS SQL ([#8649](https://github.com/cube-js/cube/issues/8649)) ([0e9f9f1](https://github.com/cube-js/cube/commit/0e9f9f1f475245bd2c41fa05e6fd8a090b07bf99))
- **schema-compiler:** Support `CURRENT_DATE` SQL push down for BigQuery ([9aebd6b](https://github.com/cube-js/cube/commit/9aebd6ba94bdc67c1b83e0b34d769db4088b41d2))

## [0.35.78](https://github.com/cube-js/cube/compare/v0.35.77...v0.35.78) (2024-08-27)

### Features

- **cubejs-api-gateway:** Add description to V1CubeMeta, V1CubeMetaDimension and V1CubeMetaMeasure in OpenAPI ([#8597](https://github.com/cube-js/cube/issues/8597)) ([1afa934](https://github.com/cube-js/cube/commit/1afa934b1db7379a87ee913816e9ce855783d2bb))
- **schema-compiler:** detect boolean columns for schema generation ([#8637](https://github.com/cube-js/cube/issues/8637)) ([178d98f](https://github.com/cube-js/cube/commit/178d98fbc35b1744d0e83e0e4103ce0465ca2244))

## [0.35.77](https://github.com/cube-js/cube/compare/v0.35.76...v0.35.77) (2024-08-26)

### Bug Fixes

- Row deduplication query isn't triggered in views if multiple entry join path is used ([#8631](https://github.com/cube-js/cube/issues/8631)) ([1668175](https://github.com/cube-js/cube/commit/16681754acfd23f1005838f83e8333ac1da1c6c3))
- View builds incorrect join graph if join paths partially shared ([#8632](https://github.com/cube-js/cube/issues/8632)) Thanks [@barakcoh](https://github.com/barakcoh) for the fix hint! ([5ca76db](https://github.com/cube-js/cube/commit/5ca76dbd66c8b0227d3107a1b1c3f1f9316dbd86)), closes [#8499](https://github.com/cube-js/cube/issues/8499)

## [0.35.76](https://github.com/cube-js/cube/compare/v0.35.75...v0.35.76) (2024-08-24)

### Bug Fixes

- Invalid interval when querying multiple negative interval windows within Cube Store ([#8626](https://github.com/cube-js/cube/issues/8626)) ([716b26a](https://github.com/cube-js/cube/commit/716b26a7fa5c43d1b7970461c5007c872b0ca184))

## [0.35.75](https://github.com/cube-js/cube/compare/v0.35.74...v0.35.75) (2024-08-22)

### Bug Fixes

- Internal: ParserError("Expected AND, found: INTERVAL") in case of trailing and leading window parts are defined for pre-aggregated rolling window measure ([#8611](https://github.com/cube-js/cube/issues/8611)) ([18d0620](https://github.com/cube-js/cube/commit/18d0620e69c9a43a9d0e0d1634132959e40f6860))

## [0.35.74](https://github.com/cube-js/cube/compare/v0.35.73...v0.35.74) (2024-08-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.73](https://github.com/cube-js/cube/compare/v0.35.72...v0.35.73) (2024-08-21)

### Features

- **cubejs-schema-compiler:** Support FILTER_GROUP in YAML models ([#8574](https://github.com/cube-js/cube/issues/8574)) ([f3b8b19](https://github.com/cube-js/cube/commit/f3b8b19daff7f1b8dcfd73abf0d1f20e74e5a847))

## [0.35.72](https://github.com/cube-js/cube/compare/v0.35.71...v0.35.72) (2024-08-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.71](https://github.com/cube-js/cube/compare/v0.35.70...v0.35.71) (2024-08-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.70](https://github.com/cube-js/cube/compare/v0.35.69...v0.35.70) (2024-08-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.69](https://github.com/cube-js/cube/compare/v0.35.68...v0.35.69) (2024-08-12)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.68](https://github.com/cube-js/cube/compare/v0.35.67...v0.35.68) (2024-08-12)

### Bug Fixes

- **schema-compiler:** incorrect sql for query with same dimension with different granularities ([#8564](https://github.com/cube-js/cube/issues/8564)) ([b8ec20e](https://github.com/cube-js/cube/commit/b8ec20e33b0c3a44ae81fba35252986737fcef1e))

## [0.35.67](https://github.com/cube-js/cube/compare/v0.35.66...v0.35.67) (2024-08-07)

### Features

- **cubesqlplanner:** Native SQL planner implementation first steps ([#8506](https://github.com/cube-js/cube/issues/8506)) ([fab9c48](https://github.com/cube-js/cube/commit/fab9c4801de41a0d3d05331c5ca85c3e0cdd6266))
- **cubesql:** Support push down cast type templates ([556ca7c](https://github.com/cube-js/cube/commit/556ca7c67b280b18221cb6748cfe20f841b1a7b9))

## [0.35.66](https://github.com/cube-js/cube/compare/v0.35.65...v0.35.66) (2024-08-06)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.65](https://github.com/cube-js/cube/compare/v0.35.64...v0.35.65) (2024-07-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.64](https://github.com/cube-js/cube/compare/v0.35.63...v0.35.64) (2024-07-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.63](https://github.com/cube-js/cube/compare/v0.35.62...v0.35.63) (2024-07-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.62](https://github.com/cube-js/cube/compare/v0.35.61...v0.35.62) (2024-07-22)

### Bug Fixes

- **schema-compiler:** Replace all toDateTime with toDateTime64 in ClickHouseQuery adapter to handle dates before 1970 ([#8390](https://github.com/cube-js/cube/issues/8390)) Thanks @Ilex4524 ! )j ([f6cff1a](https://github.com/cube-js/cube/commit/f6cff1a8a8dc510ad365ff53c02b1b15a8ba70f2))

## [0.35.61](https://github.com/cube-js/cube/compare/v0.35.60...v0.35.61) (2024-07-19)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.60](https://github.com/cube-js/cube/compare/v0.35.59...v0.35.60) (2024-07-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.59](https://github.com/cube-js/cube/compare/v0.35.58...v0.35.59) (2024-07-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.58](https://github.com/cube-js/cube/compare/v0.35.57...v0.35.58) (2024-07-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.57](https://github.com/cube-js/cube/compare/v0.35.56...v0.35.57) (2024-07-05)

### Features

- **cubesql:** Remove implicit order by clause ([#8380](https://github.com/cube-js/cube/issues/8380)) ([3e7d325](https://github.com/cube-js/cube/commit/3e7d325aadff1a572953d843394f55577aef0357))

## [0.35.56](https://github.com/cube-js/cube/compare/v0.35.55...v0.35.56) (2024-07-03)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.54](https://github.com/cube-js/cube/compare/v0.35.53...v0.35.54) (2024-06-26)

### Features

- `to_date` rolling window type syntax ([#8399](https://github.com/cube-js/cube/issues/8399)) ([eb7755d](https://github.com/cube-js/cube/commit/eb7755d997f67722daf200040e4e0f3c2814132a))
- **cubesql:** `Interval(MonthDayNano)` multiplication and decomposition ([576f7f7](https://github.com/cube-js/cube/commit/576f7f7de9190a4d466a58f619cb5cf8d6bc6a59))

## [0.35.53](https://github.com/cube-js/cube/compare/v0.35.52...v0.35.53) (2024-06-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.52](https://github.com/cube-js/cube/compare/v0.35.51...v0.35.52) (2024-06-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.51](https://github.com/cube-js/cube/compare/v0.35.50...v0.35.51) (2024-06-20)

### Bug Fixes

- **cube:** Rebuild of PreAggregations with aggregating indexes ([#8326](https://github.com/cube-js/cube/issues/8326)) ([cc1de4f](https://github.com/cube-js/cube/commit/cc1de4f3f71bb32f71e88a6db36a59c5cc8f9571))
- **schema-compiler:** add escape sequence to Snowflake QueryFilter ([13d10f6](https://github.com/cube-js/cube/commit/13d10f69bfe1569a61ab091ca917a6dbcab8644a))

## [0.35.50](https://github.com/cube-js/cube/compare/v0.35.49...v0.35.50) (2024-06-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.49](https://github.com/cube-js/cube/compare/v0.35.48...v0.35.49) (2024-06-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.48](https://github.com/cube-js/cube/compare/v0.35.47...v0.35.48) (2024-06-14)

### Features

- **cubesql:** support `GREATEST`/`LEAST` SQL functions ([#8325](https://github.com/cube-js/cube/issues/8325)) ([c13a28e](https://github.com/cube-js/cube/commit/c13a28e21514c0e06e41c0ed14e97621bd777cf7))

## [0.35.47](https://github.com/cube-js/cube/compare/v0.35.46...v0.35.47) (2024-06-07)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.46](https://github.com/cube-js/cube/compare/v0.35.45...v0.35.46) (2024-06-06)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.45](https://github.com/cube-js/cube/compare/v0.35.44...v0.35.45) (2024-06-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.44](https://github.com/cube-js/cube/compare/v0.35.43...v0.35.44) (2024-06-04)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.43](https://github.com/cube-js/cube/compare/v0.35.42...v0.35.43) (2024-05-31)

### Features

- **bigquery-driver:** Use 6 digits precision for timestamps ([#8308](https://github.com/cube-js/cube/issues/8308)) ([568bfe3](https://github.com/cube-js/cube/commit/568bfe34bc6aca136b580acb8873d208b522a2f3))

## [0.35.42](https://github.com/cube-js/cube/compare/v0.35.41...v0.35.42) (2024-05-30)

### Features

- **cubesql:** Group By Rollup support ([#8281](https://github.com/cube-js/cube/issues/8281)) ([e563798](https://github.com/cube-js/cube/commit/e5637980489608e374f3b89cc219207973a08bbb))

## [0.35.40](https://github.com/cube-js/cube/compare/v0.35.39...v0.35.40) (2024-05-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.39](https://github.com/cube-js/cube/compare/v0.35.38...v0.35.39) (2024-05-24)

### Features

- **schema-compiler:** Support multi time dimensions for rollup pre-aggregations ([#8291](https://github.com/cube-js/cube/issues/8291)) ([8b0a056](https://github.com/cube-js/cube/commit/8b0a05657c7bc7f42e0d45ecc7ce03d37e5b1e57))

## [0.35.38](https://github.com/cube-js/cube/compare/v0.35.37...v0.35.38) (2024-05-22)

### Features

- **clickhouse-driver:** SQL API PUSH down - support datetrunc, timestamp_literal ([45bc230](https://github.com/cube-js/cube/commit/45bc2306eb01e2b2cc5c204b64bfa0411ae0144b))
- **clickhouse-driver:** Support countDistinctApprox ([4d36279](https://github.com/cube-js/cube/commit/4d3627996e8cdaa9d77711f9ea21edabc5d3b8d0))

## [0.35.37](https://github.com/cube-js/cube/compare/v0.35.36...v0.35.37) (2024-05-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.36](https://github.com/cube-js/cube/compare/v0.35.35...v0.35.36) (2024-05-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.35](https://github.com/cube-js/cube/compare/v0.35.34...v0.35.35) (2024-05-17)

### Bug Fixes

- **schema-compiler:** Fix failing of `WHERE FALSE` queries ([#8265](https://github.com/cube-js/cube/issues/8265)) ([e63b4ab](https://github.com/cube-js/cube/commit/e63b4ab701cf44162d5e51d65b8d38e812f9085e))

## [0.35.34](https://github.com/cube-js/cube/compare/v0.35.33...v0.35.34) (2024-05-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.33](https://github.com/cube-js/cube/compare/v0.35.32...v0.35.33) (2024-05-15)

### Bug Fixes

- Mismatched input '10000'. Expecting: '?', 'ALL', <integer> for post-aggregate members in Athena ([#8262](https://github.com/cube-js/cube/issues/8262)) ([59834e7](https://github.com/cube-js/cube/commit/59834e7157bf060804470104e0a713194b811f39))

### Features

- **cubesql:** Rewrites for pushdown of subqueries with empty source ([#8188](https://github.com/cube-js/cube/issues/8188)) ([86a58a5](https://github.com/cube-js/cube/commit/86a58a5f3368a509debfb3f2ba4c83001377127c))

## [0.35.32](https://github.com/cube-js/cube/compare/v0.35.31...v0.35.32) (2024-05-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.31](https://github.com/cube-js/cube/compare/v0.35.30...v0.35.31) (2024-05-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.30](https://github.com/cube-js/cube/compare/v0.35.29...v0.35.30) (2024-05-10)

### Bug Fixes

- **cubesql:** Reuse query params in push down ([b849f34](https://github.com/cube-js/cube/commit/b849f34c001b9a94ac5aed1edacce906fd02f33c))
- **schema-compiler:** Convert timezone for time dimensions in measures ([0bea221](https://github.com/cube-js/cube/commit/0bea221e5fafdf8c7ca3c8cb3cfa6a233e5fc8a0))
- **schema-compiler:** respect view prefix and member alias in hierarc… ([#8231](https://github.com/cube-js/cube/issues/8231)) ([59fddea](https://github.com/cube-js/cube/commit/59fddea82e29d5651a5bade751b76afbca9d52da))
- Unexpected keyword WITH for rolling window measures in BigQuery ([9468f90](https://github.com/cube-js/cube/commit/9468f90fefdc08280e7b81b0f8e289fa041cd37d)), closes [#8193](https://github.com/cube-js/cube/issues/8193)
- Unrecognized name when regular measures are selected with multiplied ones ([617fd11](https://github.com/cube-js/cube/commit/617fd11a527a0be619450698f868858306e1daf8)), closes [#8206](https://github.com/cube-js/cube/issues/8206)

### Features

- **cubesql:** Support window frame SQL push down ([5469dbc](https://github.com/cube-js/cube/commit/5469dbc14c4ae15d9e1047fca90ab5cc4268f047))

## [0.35.29](https://github.com/cube-js/cube/compare/v0.35.28...v0.35.29) (2024-05-03)

### Bug Fixes

- Apply time shift after timezone conversions to avoid double casts ([#8229](https://github.com/cube-js/cube/issues/8229)) ([651b9e0](https://github.com/cube-js/cube/commit/651b9e029d8b0a685883d5151a0538f2f6429da2))
- **schema-compiler:** support string measures in views ([#8194](https://github.com/cube-js/cube/issues/8194)) ([fadfd1f](https://github.com/cube-js/cube/commit/fadfd1f557a16bc7f4e6c8c0daeca7c6ac1bf237))

## [0.35.28](https://github.com/cube-js/cube/compare/v0.35.27...v0.35.28) (2024-05-02)

### Bug Fixes

- **schema-compiler:** views sorting ([#8228](https://github.com/cube-js/cube/issues/8228)) ([50b459a](https://github.com/cube-js/cube/commit/50b459a6135cb27bb7e52f04639c3a0ad6c89b97))

## [0.35.27](https://github.com/cube-js/cube/compare/v0.35.26...v0.35.27) (2024-05-02)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.26](https://github.com/cube-js/cube/compare/v0.35.25...v0.35.26) (2024-05-02)

### Features

- **schema-compiler:** hierarchies ([#8102](https://github.com/cube-js/cube/issues/8102)) ([c98683f](https://github.com/cube-js/cube/commit/c98683f07a286c6f4cf71c656e3e336295a0cc78))
- **schema-compiler:** Upgrade cron-parser to 4.9, fix [#8203](https://github.com/cube-js/cube/issues/8203) ([#8214](https://github.com/cube-js/cube/issues/8214)) ([f643c4b](https://github.com/cube-js/cube/commit/f643c4ba55e5f69c161dd687c79d7ce359fcf8e5))
- Support post-aggregate time shifts for prior period lookup support ([#8215](https://github.com/cube-js/cube/issues/8215)) ([a9f55d8](https://github.com/cube-js/cube/commit/a9f55d80ee36be6a197cdfed6e1d202a52c1a39d))

## [0.35.25](https://github.com/cube-js/cube/compare/v0.35.24...v0.35.25) (2024-04-29)

### Features

- Rolling YTD, QTD and MTD measure support ([#8196](https://github.com/cube-js/cube/issues/8196)) ([bfc0708](https://github.com/cube-js/cube/commit/bfc0708502f78902e481608b4f83c2eee6182636))

## [0.35.24](https://github.com/cube-js/cube/compare/v0.35.23...v0.35.24) (2024-04-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.23](https://github.com/cube-js/cube/compare/v0.35.22...v0.35.23) (2024-04-25)

### Features

- **cubesql:** In subquery rewrite ([#8162](https://github.com/cube-js/cube/issues/8162)) ([d17c2a7](https://github.com/cube-js/cube/commit/d17c2a7d58beada203009c5d624974d3a68c6af8))
- primary and foreign keys driver queries ([#8115](https://github.com/cube-js/cube/issues/8115)) ([35bb1d4](https://github.com/cube-js/cube/commit/35bb1d435a75f53f704e9c5e33382093cbc4e115))

## [0.35.22](https://github.com/cube-js/cube/compare/v0.35.21...v0.35.22) (2024-04-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.21](https://github.com/cube-js/cube/compare/v0.35.20...v0.35.21) (2024-04-19)

### Bug Fixes

- Logical boolean measure filter for post aggregate measures support ([92cee95](https://github.com/cube-js/cube/commit/92cee954b1252b0234e3008fb74d88d7b1c3c47f))

## [0.35.20](https://github.com/cube-js/cube/compare/v0.35.19...v0.35.20) (2024-04-18)

### Features

- Allow all calculated measure types in post aggregate measures ([66f3acc](https://github.com/cube-js/cube/commit/66f3accd2f4b84ccdc41ece68da5c0c73998df29))
- Post aggregate raw dimension with date range support ([23411cf](https://github.com/cube-js/cube/commit/23411cfe8152687b6a8ed02c073b151217c60e6a))

## [0.35.19](https://github.com/cube-js/cube/compare/v0.35.18...v0.35.19) (2024-04-18)

### Features

- Post aggregate measure filter support ([120f22b](https://github.com/cube-js/cube/commit/120f22b4a52e5328ebfeea60e90b2b1aa1839feb))

## [0.35.18](https://github.com/cube-js/cube/compare/v0.35.17...v0.35.18) (2024-04-17)

### Features

- Post aggregate no granularity time dimension support ([f7a99f6](https://github.com/cube-js/cube/commit/f7a99f6e3f61b766e1581f2af574dc5d81f58101))
- Post aggregate view support ([#8161](https://github.com/cube-js/cube/issues/8161)) ([acb507d](https://github.com/cube-js/cube/commit/acb507d21d29b25592050bcf49c09fb9d88a7d93))

## [0.35.17](https://github.com/cube-js/cube/compare/v0.35.16...v0.35.17) (2024-04-16)

### Bug Fixes

- ungrouped not getting applied for MSSQL ([#7997](https://github.com/cube-js/cube/issues/7997)) Thanks @UsmanYasin ! ([3fac97c](https://github.com/cube-js/cube/commit/3fac97cc4c72dc78b8016014cfa536f54c1be9d8))

## [0.35.16](https://github.com/cube-js/cube/compare/v0.35.15...v0.35.16) (2024-04-16)

### Features

- Post aggregate time dimensions support ([4071e8a](https://github.com/cube-js/cube/commit/4071e8a112823150e98703a2face4ccb98fd9e86))

## [0.35.15](https://github.com/cube-js/cube/compare/v0.35.14...v0.35.15) (2024-04-15)

### Features

- Post aggregate filter support ([49131ea](https://github.com/cube-js/cube/commit/49131ea8adb025b974ce6a4a596d6637df47bdea))

## [0.35.14](https://github.com/cube-js/cube/compare/v0.35.13...v0.35.14) (2024-04-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.13](https://github.com/cube-js/cube/compare/v0.35.12...v0.35.13) (2024-04-15)

### Features

- Post aggregate measure support ([#8145](https://github.com/cube-js/cube/issues/8145)) ([2648a26](https://github.com/cube-js/cube/commit/2648a26a6149c31615d5df105575e5c30117be6f))

## [0.35.11](https://github.com/cube-js/cube/compare/v0.35.10...v0.35.11) (2024-04-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.10](https://github.com/cube-js/cube/compare/v0.35.9...v0.35.10) (2024-04-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.9](https://github.com/cube-js/cube/compare/v0.35.8...v0.35.9) (2024-04-08)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.8](https://github.com/cube-js/cube/compare/v0.35.7...v0.35.8) (2024-04-05)

### Bug Fixes

- **schema-compiler:** quote case sensitive idents ([#8063](https://github.com/cube-js/cube/issues/8063)) ([6273532](https://github.com/cube-js/cube/commit/6273532611385d794257e7123d747f01b1c1c436))

## [0.35.7](https://github.com/cube-js/cube/compare/v0.35.6...v0.35.7) (2024-04-03)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.35.6](https://github.com/cube-js/cube/compare/v0.35.5...v0.35.6) (2024-04-02)

### Features

- Ungrouped query pre-aggregation support ([#8058](https://github.com/cube-js/cube/issues/8058)) ([2ca99de](https://github.com/cube-js/cube/commit/2ca99de3e7ea813bdb7ea684bed4af886bde237b))

## [0.35.5](https://github.com/cube-js/cube/compare/v0.35.4...v0.35.5) (2024-03-28)

### Bug Fixes

- **cubesql:** SQL push down of ORDER BY causes Invalid query format: "order[0][0]" with value fails to match the required pattern: /^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/ ([#8032](https://github.com/cube-js/cube/issues/8032)) ([0681725](https://github.com/cube-js/cube/commit/0681725c921ea62f7ef813562be0202e93928889))
- Multiplied count measure can be used as additive measure in pre-… ([#8033](https://github.com/cube-js/cube/issues/8033)) ([f36959c](https://github.com/cube-js/cube/commit/f36959c297bd637c5332ba171a9049c3448b3522))
- **schema-compiler:** Upgrade babel/traverse to 7.24 (GHSA-67hx-6x53-jw92) ([#8002](https://github.com/cube-js/cube/issues/8002)) ([9848e51](https://github.com/cube-js/cube/commit/9848e51a2a5420bc7649775951ea122c03aa52e9))

## [0.35.4](https://github.com/cube-js/cube/compare/v0.35.3...v0.35.4) (2024-03-27)

### Features

- **cubesql:** Support `DISTINCT` push down ([fc79188](https://github.com/cube-js/cube/commit/fc79188b5a205d29d9acd3669677cc829b0a9013))

## [0.35.3](https://github.com/cube-js/cube/compare/v0.35.2...v0.35.3) (2024-03-22)

### Bug Fixes

- Render all distinct counts as `1` for ungrouped queries to not fail ratio measures ([#7987](https://github.com/cube-js/cube/issues/7987)) ([f756e4b](https://github.com/cube-js/cube/commit/f756e4bd7d48a0e60d505ac0de50ae1469637011))

## [0.35.2](https://github.com/cube-js/cube/compare/v0.35.1...v0.35.2) (2024-03-22)

### Reverts

- Revert "feat(schema-compiler): composite key generation (#7929)" ([61e4c45](https://github.com/cube-js/cube/commit/61e4c45024bdce7799a2161608b1fdc97e9bd9e8)), closes [#7929](https://github.com/cube-js/cube/issues/7929)

## [0.35.1](https://github.com/cube-js/cube/compare/v0.35.0...v0.35.1) (2024-03-18)

### Features

- **mssql-driver:** Upgrade mssql to 10.0.2 (Node.js 17+ compatiblity), fix [#7951](https://github.com/cube-js/cube/issues/7951) ([7adaea6](https://github.com/cube-js/cube/commit/7adaea6522e47035e67596b4dc0ab15e87312706))
- **schema-compiler:** composite key generation ([#7929](https://github.com/cube-js/cube/issues/7929)) ([5a27200](https://github.com/cube-js/cube/commit/5a27200252cf837fc34ef38b320216d2e37ffa22))

# [0.35.0](https://github.com/cube-js/cube/compare/v0.34.62...v0.35.0) (2024-03-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.62](https://github.com/cube-js/cube/compare/v0.34.61...v0.34.62) (2024-03-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.61](https://github.com/cube-js/cube/compare/v0.34.60...v0.34.61) (2024-03-11)

### Bug Fixes

- **cubesql:** Fix push down `CASE` with expr ([f1d1242](https://github.com/cube-js/cube/commit/f1d12428870cd6e1a159bd559a3402bb6be778aa))
- **cubesql:** Partition pruning doesn't work when view uses proxy member ([#7866](https://github.com/cube-js/cube/issues/7866)) ([c02a07f](https://github.com/cube-js/cube/commit/c02a07f99141bcd549ffbbcaa37e95d138ff72a2)), closes [#6623](https://github.com/cube-js/cube/issues/6623)
- Wrong sql generation when multiple primary keys used ([#6689](https://github.com/cube-js/cube/issues/6689)) Thanks [@alexanderlz](https://github.com/alexanderlz) ! ([2540a22](https://github.com/cube-js/cube/commit/2540a224f406b8fd3c09183b826fb899dad7a915))

### Features

- **clickhouse-driver:** Support export bucket for S3 (write only) ([#7849](https://github.com/cube-js/cube/issues/7849)) ([db7e2c1](https://github.com/cube-js/cube/commit/db7e2c10c97dc4b81d136bb1205931ea5eab2918))

## [0.34.60](https://github.com/cube-js/cube/compare/v0.34.59...v0.34.60) (2024-03-02)

### Bug Fixes

- **cubesql:** Incorrect CASE WHEN generated during ungrouped filtered count ([#7859](https://github.com/cube-js/cube/issues/7859)) ([f970937](https://github.com/cube-js/cube/commit/f9709376e5dadfbcbffe9a65dfa9f0ea16df7bab)), closes [#7858](https://github.com/cube-js/cube/issues/7858)
- **cubesql:** Remove excessive limit on inner wrapped queries ([#7864](https://github.com/cube-js/cube/issues/7864)) ([b97268f](https://github.com/cube-js/cube/commit/b97268fe5caf55c5b7806c597b9f7b75410f6ba4))

## [0.34.59](https://github.com/cube-js/cube/compare/v0.34.58...v0.34.59) (2024-02-28)

### Bug Fixes

- Ungrouped filtered count should contain `CASE WHEN` statement bu… ([#7853](https://github.com/cube-js/cube/issues/7853)) ([8c37a40](https://github.com/cube-js/cube/commit/8c37a407b9d8c8f97dab51c9e0e97067c91ff90e))

## [0.34.58](https://github.com/cube-js/cube/compare/v0.34.57...v0.34.58) (2024-02-27)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.57](https://github.com/cube-js/cube/compare/v0.34.56...v0.34.57) (2024-02-26)

### Bug Fixes

- **cubesql:** `ungrouped` query can be routed to Cube Store ([#7810](https://github.com/cube-js/cube/issues/7810)) ([b122837](https://github.com/cube-js/cube/commit/b122837de9cd4fcaca4ddc0e7f85ff695de09483))

### Features

- **cubesql:** `WHERE` SQL push down ([#7808](https://github.com/cube-js/cube/issues/7808)) ([98b5709](https://github.com/cube-js/cube/commit/98b570946905586f16a502f83b0a1cf8e4aa92a6))

## [0.34.56](https://github.com/cube-js/cube/compare/v0.34.55...v0.34.56) (2024-02-20)

### Bug Fixes

- Gracefully handle logical expressions in FILTER_PARAMS and introduce FILTER_GROUP to allow use FILTER_PARAMS for logical expressions ([#7766](https://github.com/cube-js/cube/issues/7766)) ([736070d](https://github.com/cube-js/cube/commit/736070d5a33c25d212420a61da6c98a4b4d31188))

## [0.34.55](https://github.com/cube-js/cube/compare/v0.34.54...v0.34.55) (2024-02-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.54](https://github.com/cube-js/cube/compare/v0.34.53...v0.34.54) (2024-02-13)

### Bug Fixes

- **cubesql:** Cannot read properties of undefined (reading 'preAggregation') with enabled SQL push down ([ac2cc17](https://github.com/cube-js/cube/commit/ac2cc177fc5d53a213603efd849acb2b4abf7a48))

## [0.34.53](https://github.com/cube-js/cube/compare/v0.34.52...v0.34.53) (2024-02-13)

### Bug Fixes

- **cubesql:** SQL push down can be incorrectly routed to Cube Store pre-aggregations ([#7752](https://github.com/cube-js/cube/issues/7752)) ([1a6451c](https://github.com/cube-js/cube/commit/1a6451c2c0bef6f5efbba2870ec0a11238b1addb))

## [0.34.52](https://github.com/cube-js/cube/compare/v0.34.51...v0.34.52) (2024-02-13)

### Features

- **cubesql:** Support Athena OFFSET and LIMIT push down ([00c2a6b](https://github.com/cube-js/cube/commit/00c2a6b3cc88fde5d65d02549d3458818e4a8e42))

## [0.34.51](https://github.com/cube-js/cube/compare/v0.34.50...v0.34.51) (2024-02-11)

### Features

- **cubesql:** Extend `DATEDIFF` push down support ([ecaaf1c](https://github.com/cube-js/cube/commit/ecaaf1ca5566ff7cc27289468440e6a902a09609))

## [0.34.50](https://github.com/cube-js/cube/compare/v0.34.49...v0.34.50) (2024-01-31)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.49](https://github.com/cube-js/cube/compare/v0.34.48...v0.34.49) (2024-01-26)

### Bug Fixes

- Revert drillMembers include in views as it's a breaking change. Need to re-introduce it through evaluation on cube level and validating drillMembers are included in view ([#7692](https://github.com/cube-js/cube/issues/7692)) ([6cbbaa0](https://github.com/cube-js/cube/commit/6cbbaa009c87eef4250cebca79de3c9cbb2ace11))

## [0.34.48](https://github.com/cube-js/cube/compare/v0.34.47...v0.34.48) (2024-01-25)

### Bug Fixes

- Join order is incorrect for snowflake schema views with defined join paths ([#7689](https://github.com/cube-js/cube/issues/7689)) ([454d456](https://github.com/cube-js/cube/commit/454d4563120f591445c219c42dd5bd2fd937f7a6)), closes [#7663](https://github.com/cube-js/cube/issues/7663)

### Features

- View members inherit title, format and drillMembers attribute ([#7617](https://github.com/cube-js/cube/issues/7617)) Thanks @CallumWalterWhite ! ([302a756](https://github.com/cube-js/cube/commit/302a7560fd65e1437249701e1fc42b32da5df8fa))

## [0.34.47](https://github.com/cube-js/cube/compare/v0.34.46...v0.34.47) (2024-01-23)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.46](https://github.com/cube-js/cube/compare/v0.34.45...v0.34.46) (2024-01-18)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.45](https://github.com/cube-js/cube/compare/v0.34.44...v0.34.45) (2024-01-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.44](https://github.com/cube-js/cube/compare/v0.34.43...v0.34.44) (2024-01-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.43](https://github.com/cube-js/cube/compare/v0.34.42...v0.34.43) (2024-01-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.42](https://github.com/cube-js/cube/compare/v0.34.41...v0.34.42) (2024-01-07)

### Features

- **cubesql:** Compiler cache for rewrite rules ([#7604](https://github.com/cube-js/cube/issues/7604)) ([995889f](https://github.com/cube-js/cube/commit/995889fb7722cda3bf839095949d6d71693dd329))

## [0.34.41](https://github.com/cube-js/cube/compare/v0.34.40...v0.34.41) (2024-01-02)

### Features

- **cubesql:** Support Domo data queries ([#7509](https://github.com/cube-js/cube/issues/7509)) ([6d644dc](https://github.com/cube-js/cube/commit/6d644dc5265245b8581eb2c2e3b75f5d6d9f929c))

## [0.34.40](https://github.com/cube-js/cube/compare/v0.34.39...v0.34.40) (2023-12-21)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.39](https://github.com/cube-js/cube/compare/v0.34.38...v0.34.39) (2023-12-21)

### Bug Fixes

- **base-driver:** Support parsing intervals with month|quarter|year granularity ([#7561](https://github.com/cube-js/cube/issues/7561)) ([24c850c](https://github.com/cube-js/cube/commit/24c850ccb74b08f74b5314ed97f872b95cb46b43))

## [0.34.38](https://github.com/cube-js/cube/compare/v0.34.37...v0.34.38) (2023-12-19)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.37](https://github.com/cube-js/cube/compare/v0.34.36...v0.34.37) (2023-12-19)

### Bug Fixes

- **clickhouse-driver:** Initial support for DateTime64, fix [#7537](https://github.com/cube-js/cube/issues/7537) ([#7538](https://github.com/cube-js/cube/issues/7538)) ([401e9e1](https://github.com/cube-js/cube/commit/401e9e1b9c07e115804a1f84fade2bb82b55ca29))
- Internal: Error during planning: No field named for pre-aggregat… ([#7554](https://github.com/cube-js/cube/issues/7554)) ([412213c](https://github.com/cube-js/cube/commit/412213cbec40748d0de6f54731686c3d0b263e5c))

## [0.34.36](https://github.com/cube-js/cube/compare/v0.34.35...v0.34.36) (2023-12-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.35](https://github.com/cube-js/cube/compare/v0.34.34...v0.34.35) (2023-12-13)

### Features

- Add meta property for cube definition ([#7327](https://github.com/cube-js/cube/issues/7327)) Thanks [@mharrisb1](https://github.com/mharrisb1) ! ([0ea12c4](https://github.com/cube-js/cube/commit/0ea12c440b3b3fb8280063a1da7d7f9e6b9b6754))

## [0.34.33](https://github.com/cube-js/cube/compare/v0.34.32...v0.34.33) (2023-12-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.32](https://github.com/cube-js/cube/compare/v0.34.31...v0.34.32) (2023-12-07)

### Features

- **cubesql:** Add CURRENT_DATE function definitions for Postgres and Snowflake so those can be pushed down ([ac1ca88](https://github.com/cube-js/cube/commit/ac1ca889add970dd05820b8f077edb968c985994))

## [0.34.31](https://github.com/cube-js/cube/compare/v0.34.30...v0.34.31) (2023-12-07)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.30](https://github.com/cube-js/cube/compare/v0.34.29...v0.34.30) (2023-12-04)

### Bug Fixes

- View tries to join cube twice in case there's join ambiguity ([#7487](https://github.com/cube-js/cube/issues/7487)) ([66f7c17](https://github.com/cube-js/cube/commit/66f7c17a19fb594a8c71ff6d449f25a5a9e727aa))

## [0.34.29](https://github.com/cube-js/cube/compare/v0.34.28...v0.34.29) (2023-12-01)

### Bug Fixes

- **schema-compiler:** td.isDateOperator is not a function ([#7483](https://github.com/cube-js/cube/issues/7483)) ([31c272a](https://github.com/cube-js/cube/commit/31c272ae929bc8e7f7972e46666f84bf8509cc3f))

## [0.34.27](https://github.com/cube-js/cube/compare/v0.34.26...v0.34.27) (2023-11-30)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.26](https://github.com/cube-js/cube/compare/v0.34.25...v0.34.26) (2023-11-28)

### Bug Fixes

- **schema-compiler:** Incorrect excludes resolution, fix [#7100](https://github.com/cube-js/cube/issues/7100) ([#7468](https://github.com/cube-js/cube/issues/7468)) ([f89ef8d](https://github.com/cube-js/cube/commit/f89ef8d638fafa760ea6415bc670c0c3939ea06b))
- **schema-compiler:** Missing pre-aggregation filter for partitioned pre-aggregation via view, fix [#6623](https://github.com/cube-js/cube/issues/6623) ([#7454](https://github.com/cube-js/cube/issues/7454)) ([567b92f](https://github.com/cube-js/cube/commit/567b92fe716bcc11b19d42815699b476339ef201))

### Features

- **cubesql:** Support SQL push down for several functions ([79e5ac8](https://github.com/cube-js/cube/commit/79e5ac8e998005ebf8b5f72ccf1d63f425f6003c))

## [0.34.25](https://github.com/cube-js/cube/compare/v0.34.24...v0.34.25) (2023-11-24)

### Bug Fixes

- **mongobi-driver:** Correct time zone conversion for minutes, fix [#7152](https://github.com/cube-js/cube/issues/7152) ([#7456](https://github.com/cube-js/cube/issues/7456)) ([5ae2009](https://github.com/cube-js/cube/commit/5ae2009a8231432020e09fb6dfeaa8f40ef152fa))

## [0.34.24](https://github.com/cube-js/cube/compare/v0.34.23...v0.34.24) (2023-11-23)

### Features

- **schema-compiler:** add dimension primaryKey property to transform result ([#7443](https://github.com/cube-js/cube/issues/7443)) ([baabd41](https://github.com/cube-js/cube/commit/baabd41fed8bd1476d0489dc97c3f78c9106a63b))

## [0.34.23](https://github.com/cube-js/cube/compare/v0.34.22...v0.34.23) (2023-11-19)

### Bug Fixes

- Match additive pre-aggregations with missing members for views and proxy members ([#7433](https://github.com/cube-js/cube/issues/7433)) ([e236079](https://github.com/cube-js/cube/commit/e236079ad0ecef2b43f285f4a5154bcd8cb8a9e2))

### Features

- **cubesql:** Support `-` (unary minus) SQL push down ([a0a2e12](https://github.com/cube-js/cube/commit/a0a2e129e4cf3264df75bbdf53a962a892e4e9c2))
- **cubesql:** Support `NOT` SQL push down ([#7422](https://github.com/cube-js/cube/issues/7422)) ([7b1ff0d](https://github.com/cube-js/cube/commit/7b1ff0d897ec9a5cfffba1e09444ff7baa8bea5b))

## [0.34.22](https://github.com/cube-js/cube/compare/v0.34.21...v0.34.22) (2023-11-16)

### Bug Fixes

- **cubesql:** Window PARTITION BY, ORDER BY queries fail for SQL push down ([62b359f](https://github.com/cube-js/cube/commit/62b359f2d33d0c8fd59aa570e7e3a83718a3f7e8))

## [0.34.21](https://github.com/cube-js/cube/compare/v0.34.20...v0.34.21) (2023-11-15)

### Features

- **cubesql:** Support SQL push down for more functions ([#7406](https://github.com/cube-js/cube/issues/7406)) ([b1606da](https://github.com/cube-js/cube/commit/b1606daba70ab92952b1cbbacd94dd7294b17ad5))

## [0.34.20](https://github.com/cube-js/cube/compare/v0.34.19...v0.34.20) (2023-11-14)

### Bug Fixes

- Expose `isVisible` and `public` properties in meta consistently ([35ca1d0](https://github.com/cube-js/cube/commit/35ca1d0c104ea6111d4db3b8649cd52a5fabcf79))

### Features

- **cubesql:** Support `[NOT] IN` SQL push down ([c64994a](https://github.com/cube-js/cube/commit/c64994ac26e1174ce121c79af46fa6a62747b7e9))

## [0.34.19](https://github.com/cube-js/cube/compare/v0.34.18...v0.34.19) (2023-11-11)

### Features

- **cubesql:** SQL push down support for window functions ([#7403](https://github.com/cube-js/cube/issues/7403)) ([b1da6c0](https://github.com/cube-js/cube/commit/b1da6c0e38e3b586c3d4b1ddf9c00be57065d960))

## [0.34.18](https://github.com/cube-js/cube/compare/v0.34.17...v0.34.18) (2023-11-10)

### Bug Fixes

- Non-additive pre-aggregations based on proxy dimension doesn't match anymore ([#7396](https://github.com/cube-js/cube/issues/7396)) ([910a49d](https://github.com/cube-js/cube/commit/910a49d87ebfade22ab5ac2463eca9b43f2d5742))

## [0.34.17](https://github.com/cube-js/cube/compare/v0.34.16...v0.34.17) (2023-11-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.16](https://github.com/cube-js/cube/compare/v0.34.15...v0.34.16) (2023-11-06)

### Features

- **cubesql:** Enable `COVAR` aggr functions push down ([89d841a](https://github.com/cube-js/cube/commit/89d841a049c0064b902bf3cdc631cb056984ddd4))
- **native:** Jinja - async render ([#7309](https://github.com/cube-js/cube/issues/7309)) ([cd1019c](https://github.com/cube-js/cube/commit/cd1019c9fa904ba76b334e941726ff871d2e3a44))

## [0.34.15](https://github.com/cube-js/cube/compare/v0.34.14...v0.34.15) (2023-11-06)

### Bug Fixes

- Single value non-additive pre-aggregation match in views ([9666f24](https://github.com/cube-js/cube/commit/9666f24650e93ad6fd6990f6b82fb957e26c2885))

## [0.34.14](https://github.com/cube-js/cube/compare/v0.34.13...v0.34.14) (2023-11-05)

### Bug Fixes

- Views with proxy dimensions and non-additive measures don't not match pre-aggregations ([#7374](https://github.com/cube-js/cube/issues/7374)) ([7189720](https://github.com/cube-js/cube/commit/718972090612f31326bad84fcf792d674ea622a9)), closes [#7099](https://github.com/cube-js/cube/issues/7099)

### Features

- **cubesql:** SQL push down for several ANSI SQL functions ([ac2bf15](https://github.com/cube-js/cube/commit/ac2bf15954e6b143b9014ff4b8f72c6098253c82))

## [0.34.13](https://github.com/cube-js/cube/compare/v0.34.12...v0.34.13) (2023-10-31)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.12](https://github.com/cube-js/cube/compare/v0.34.11...v0.34.12) (2023-10-30)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.11](https://github.com/cube-js/cube/compare/v0.34.10...v0.34.11) (2023-10-29)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.10](https://github.com/cube-js/cube/compare/v0.34.9...v0.34.10) (2023-10-27)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.9](https://github.com/cube-js/cube/compare/v0.34.8...v0.34.9) (2023-10-26)

### Features

- **native:** Jinja - support fields reflection for PyObject ([#7312](https://github.com/cube-js/cube/issues/7312)) ([e2569a9](https://github.com/cube-js/cube/commit/e2569a9407f86380831d410be8df0e9ef27d5b40))
- Split view support ([#7308](https://github.com/cube-js/cube/issues/7308)) ([1e722dd](https://github.com/cube-js/cube/commit/1e722dd0cc1a29b6e6554b855b7899abe5746a07))

## [0.34.8](https://github.com/cube-js/cube/compare/v0.34.7...v0.34.8) (2023-10-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.7](https://github.com/cube-js/cube/compare/v0.34.6...v0.34.7) (2023-10-23)

### Features

- **native:** Jinja - passing filters via TemplateContext from Python ([#7284](https://github.com/cube-js/cube/issues/7284)) ([1e92352](https://github.com/cube-js/cube/commit/1e9235293f3843caacc1d20fc458e0184e933c40))

## [0.34.6](https://github.com/cube-js/cube/compare/v0.34.5...v0.34.6) (2023-10-20)

### Features

- **native:** Jinja - passing variables via TemplateContext from Python ([#7280](https://github.com/cube-js/cube/issues/7280)) ([e3dec88](https://github.com/cube-js/cube/commit/e3dec888870f6c37da2957ff49988e5769e37f3d))

## [0.34.5](https://github.com/cube-js/cube/compare/v0.34.4...v0.34.5) (2023-10-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.4](https://github.com/cube-js/cube/compare/v0.34.3...v0.34.4) (2023-10-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.3](https://github.com/cube-js/cube/compare/v0.34.2...v0.34.3) (2023-10-12)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.34.2](https://github.com/cube-js/cube/compare/v0.34.1...v0.34.2) (2023-10-12)

### Features

- **native:** Support jinja inside yml in fallback build (without python) ([#7203](https://github.com/cube-js/cube/issues/7203)) ([e3e7e0e](https://github.com/cube-js/cube/commit/e3e7e0e987d1f0b433873779063fa709c0b43eeb))

## [0.34.1](https://github.com/cube-js/cube/compare/v0.34.0...v0.34.1) (2023-10-09)

### Bug Fixes

- More descriptive error message for incorrect `incremental: true` non-partitioned setting ([7e12e4e](https://github.com/cube-js/cube/commit/7e12e4e812b9aee3dab69ba8b86709f889f1a5a0))

# [0.34.0](https://github.com/cube-js/cube/compare/v0.33.65...v0.34.0) (2023-10-03)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.65](https://github.com/cube-js/cube/compare/v0.33.64...v0.33.65) (2023-10-02)

### Features

- **python:** Evaluate Jinja templates inside `.yml` and `.yaml` ([#7182](https://github.com/cube-js/cube/issues/7182)) ([69e9437](https://github.com/cube-js/cube/commit/69e9437deccc2f16442a39ceb95d1583bcea195f))

## [0.33.64](https://github.com/cube-js/cube/compare/v0.33.63...v0.33.64) (2023-09-30)

### Bug Fixes

- SQL push down expressions incorrectly cache used cube names ([c5f0b03](https://github.com/cube-js/cube/commit/c5f0b03e2582be6969ae2de8d1e7dfc6190141c4))

### Features

- **native:** More concise module structure ([#7180](https://github.com/cube-js/cube/issues/7180)) ([e2a80bf](https://github.com/cube-js/cube/commit/e2a80bfa0515696391f5aa69c299038f6e52c405))

## [0.33.63](https://github.com/cube-js/cube/compare/v0.33.62...v0.33.63) (2023-09-26)

### Features

- **cubesql:** Tableau Standard Gregorian missing date groupings support through SQL push down and some other functions([#7172](https://github.com/cube-js/cube/issues/7172)) ([1339f57](https://github.com/cube-js/cube/commit/1339f577badf94aab02483e3431f614b1fe61302))

## [0.33.62](https://github.com/cube-js/cube/compare/v0.33.61...v0.33.62) (2023-09-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.61](https://github.com/cube-js/cube/compare/v0.33.60...v0.33.61) (2023-09-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.60](https://github.com/cube-js/cube/compare/v0.33.59...v0.33.60) (2023-09-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.59](https://github.com/cube-js/cube/compare/v0.33.58...v0.33.59) (2023-09-20)

### Features

- **cubesql:** `EXTRACT` SQL push down ([#7151](https://github.com/cube-js/cube/issues/7151)) ([e30c4da](https://github.com/cube-js/cube/commit/e30c4da555adfc1515fcc9e0253bc89ca8adc58b))
- **cubesql:** Add ability to filter dates inclusive of date being passed in when using `<=` or `>=` ([#7041](https://github.com/cube-js/cube/issues/7041)) Thanks [@darapuk](https://github.com/darapuk) ! ([6b9ae70](https://github.com/cube-js/cube/commit/6b9ae703b113a01fa4e6de54b5597475aed0b85d))

## [0.33.58](https://github.com/cube-js/cube/compare/v0.33.57...v0.33.58) (2023-09-18)

### Bug Fixes

- **schema-compiler:** YAML - crash on empty string/null, fix [#7126](https://github.com/cube-js/cube/issues/7126) ([#7141](https://github.com/cube-js/cube/issues/7141)) ([73f5ca7](https://github.com/cube-js/cube/commit/73f5ca74a6b6d7ae66b5576c6c4446fda6b1a5de))
- **schema-compiler:** YAML - crash on unammed measure/dimension/join, refs [#7033](https://github.com/cube-js/cube/issues/7033) ([#7142](https://github.com/cube-js/cube/issues/7142)) ([5c6a065](https://github.com/cube-js/cube/commit/5c6a065b5755d9a991bc89beeb15243b119511cb))

## [0.33.57](https://github.com/cube-js/cube/compare/v0.33.56...v0.33.57) (2023-09-15)

### Features

- **cubesql:** `CONCAT` function support for SQL push down ([da24afe](https://github.com/cube-js/cube/commit/da24afebc844f6f983d6f4df76f2367168667036))
- **cubesql:** DATE_TRUNC support for SQL push down ([#7132](https://github.com/cube-js/cube/issues/7132)) ([ae80eb1](https://github.com/cube-js/cube/commit/ae80eb1e45857cde14ec0ff9c15547f917d6fdd2))

## [0.33.56](https://github.com/cube-js/cube/compare/v0.33.55...v0.33.56) (2023-09-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.55](https://github.com/cube-js/cube/compare/v0.33.54...v0.33.55) (2023-09-12)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.54](https://github.com/cube-js/cube/compare/v0.33.53...v0.33.54) (2023-09-12)

### Features

- **cubesql:** `ORDER BY` SQL push down support ([#7115](https://github.com/cube-js/cube/issues/7115)) ([49ea3cf](https://github.com/cube-js/cube/commit/49ea3cf0721f30da142fb021f860cc56b0a85ab6))

## [0.33.53](https://github.com/cube-js/cube/compare/v0.33.52...v0.33.53) (2023-09-08)

### Features

- **cubesql:** Ungrouped SQL push down ([#7102](https://github.com/cube-js/cube/issues/7102)) ([4c7fde5](https://github.com/cube-js/cube/commit/4c7fde5a96a5db0978b72d0887e533450123e9f7))

## [0.33.51](https://github.com/cube-js/cube/compare/v0.33.50...v0.33.51) (2023-09-06)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.50](https://github.com/cube-js/cube/compare/v0.33.49...v0.33.50) (2023-09-04)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.49](https://github.com/cube-js/cube/compare/v0.33.48...v0.33.49) (2023-08-31)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.48](https://github.com/cube-js/cube/compare/v0.33.47...v0.33.48) (2023-08-23)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.47](https://github.com/cube-js/cube/compare/v0.33.46...v0.33.47) (2023-08-15)

### Features

- **cubesql:** Support LIMIT for SQL push down ([1b5c19f](https://github.com/cube-js/cube/commit/1b5c19f03331ca4174b37614b8b41cdafc211ad7))

## [0.33.46](https://github.com/cube-js/cube/compare/v0.33.45...v0.33.46) (2023-08-14)

### Features

- **cubesql:** Initial SQL push down support for BigQuery, Clickhouse and MySQL ([38467ab](https://github.com/cube-js/cube/commit/38467ab7de64803cd51acf4d5fc696938e52f778))

## [0.33.45](https://github.com/cube-js/cube/compare/v0.33.44...v0.33.45) (2023-08-13)

### Features

- **cubesql:** CASE WHEN SQL push down ([#7029](https://github.com/cube-js/cube/issues/7029)) ([80e4a60](https://github.com/cube-js/cube/commit/80e4a609cdb983db0a600d0fff0fd5bfe31652ed))
- **cubesql:** Whole SQL query push down to data sources ([#6629](https://github.com/cube-js/cube/issues/6629)) ([0e8a76a](https://github.com/cube-js/cube/commit/0e8a76a20cb37e675997f384785dd06e09175113))

## [0.33.44](https://github.com/cube-js/cube/compare/v0.33.43...v0.33.44) (2023-08-11)

### Bug Fixes

- **cubejs-schema-compiler:** cube level public should take precedence over dimension level public ([#7005](https://github.com/cube-js/cube/issues/7005)) ([2161227](https://github.com/cube-js/cube/commit/216122708a2fa2732b3b4755e81f3eccde21f070))

### Features

- **schema-compiler:** Support .yaml.jinja extension, fix [#7018](https://github.com/cube-js/cube/issues/7018) ([#7023](https://github.com/cube-js/cube/issues/7023)) ([3f910fb](https://github.com/cube-js/cube/commit/3f910fb90d8b280e71867ff451f4433dec9a4efa))

## [0.33.43](https://github.com/cube-js/cube/compare/v0.33.42...v0.33.43) (2023-08-04)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.42](https://github.com/cube-js/cube/compare/v0.33.41...v0.33.42) (2023-08-03)

### Bug Fixes

- **mssql-driver:** Fix group by clause for queries with dimensions and cumulative measures in MS SQL ([#6526](https://github.com/cube-js/cube/issues/6526)) Thanks [@mfclarke-cnx](https://github.com/mfclarke-cnx)! ([e0ed3a2](https://github.com/cube-js/cube/commit/e0ed3a27c9decf00c00568ebb01b6689f410a44d))

### Features

- **schema-compiler:** allow passing a catalog ([#6964](https://github.com/cube-js/cube/issues/6964)) ([680f545](https://github.com/cube-js/cube/commit/680f54561a53d4c8b2108204f191ae9a3c22156e))

## [0.33.41](https://github.com/cube-js/cube/compare/v0.33.40...v0.33.41) (2023-07-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.40](https://github.com/cube-js/cube/compare/v0.33.39...v0.33.40) (2023-07-27)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.39](https://github.com/cube-js/cube/compare/v0.33.38...v0.33.39) (2023-07-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.38](https://github.com/cube-js/cube/compare/v0.33.37...v0.33.38) (2023-07-21)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.37](https://github.com/cube-js/cube/compare/v0.33.36...v0.33.37) (2023-07-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.36](https://github.com/cube-js/cube/compare/v0.33.35...v0.33.36) (2023-07-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.34](https://github.com/cube-js/cube/compare/v0.33.33...v0.33.34) (2023-07-12)

### Features

- **schema:** Support python [@context](https://github.com/context)\_func for jinja templates ([#6729](https://github.com/cube-js/cube/issues/6729)) ([2f2be58](https://github.com/cube-js/cube/commit/2f2be58d49e4baf2b389a94ba2c782f8579022ab))

## [0.33.33](https://github.com/cube-js/cube/compare/v0.33.32...v0.33.33) (2023-07-08)

### Features

- **schema:** Support multitenancy for jinja templates ([#6793](https://github.com/cube-js/cube/issues/6793)) ([fb59e59](https://github.com/cube-js/cube/commit/fb59e590b09e13bfaecb794a9db7d0b861f38d3f))

## [0.33.32](https://github.com/cube-js/cube/compare/v0.33.31...v0.33.32) (2023-07-07)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.31](https://github.com/cube-js/cube/compare/v0.33.30...v0.33.31) (2023-07-01)

### Bug Fixes

- **databricks-jdbc-driver:** Return NULL decimal as NULL instead of 0 ([#6768](https://github.com/cube-js/cube/issues/6768)) ([c2ab19d](https://github.com/cube-js/cube/commit/c2ab19d86d6144e4f91f9e8fb681e17e87bfcef3))

## [0.33.29](https://github.com/cube-js/cube/compare/v0.33.28...v0.33.29) (2023-06-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.28](https://github.com/cube-js/cube/compare/v0.33.27...v0.33.28) (2023-06-19)

### Bug Fixes

- **schema:** Load jinja engine only when jinja templates exist ([#6741](https://github.com/cube-js/cube/issues/6741)) ([75469a6](https://github.com/cube-js/cube/commit/75469a67b58d0a730b29b57d4f05a4049462bf10))

## [0.33.27](https://github.com/cube-js/cube/compare/v0.33.26...v0.33.27) (2023-06-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.26](https://github.com/cube-js/cube/compare/v0.33.25...v0.33.26) (2023-06-14)

### Features

- **schema:** Initial support for jinja templates ([#6704](https://github.com/cube-js/cube/issues/6704)) ([338d1b7](https://github.com/cube-js/cube/commit/338d1b7ed03fc074c06fb028f731c9817ba8d419))

## [0.33.25](https://github.com/cube-js/cube/compare/v0.33.24...v0.33.25) (2023-06-07)

### Bug Fixes

- schema generation joins and quotes ([#6695](https://github.com/cube-js/cube/issues/6695)) ([85dd2b5](https://github.com/cube-js/cube/commit/85dd2b55daba9a0fed3c13131d9c8acae600e136))
- Support indexes in pre-aggs in yaml ([#6587](https://github.com/cube-js/cube/issues/6587)) ([eaec97d](https://github.com/cube-js/cube/commit/eaec97d99a386e39be937ae8770d422512b482ff))

## [0.33.24](https://github.com/cube-js/cube/compare/v0.33.23...v0.33.24) (2023-06-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.21](https://github.com/cube-js/cube/compare/v0.33.20...v0.33.21) (2023-05-31)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.20](https://github.com/cube-js/cube/compare/v0.33.19...v0.33.20) (2023-05-31)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.19](https://github.com/cube-js/cube/compare/v0.33.18...v0.33.19) (2023-05-30)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.18](https://github.com/cube-js/cube/compare/v0.33.17...v0.33.18) (2023-05-29)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.15](https://github.com/cube-js/cube/compare/v0.33.14...v0.33.15) (2023-05-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.13](https://github.com/cube-js/cube/compare/v0.33.12...v0.33.13) (2023-05-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.11](https://github.com/cube-js/cube/compare/v0.33.10...v0.33.11) (2023-05-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.9](https://github.com/cube-js/cube/compare/v0.33.8...v0.33.9) (2023-05-18)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.8](https://github.com/cube-js/cube/compare/v0.33.7...v0.33.8) (2023-05-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.7](https://github.com/cube-js/cube/compare/v0.33.6...v0.33.7) (2023-05-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.33.6](https://github.com/cube-js/cube/compare/v0.33.5...v0.33.6) (2023-05-13)

### Bug Fixes

- LIMIT is not enforced ([#6586](https://github.com/cube-js/cube/issues/6586)) ([8ca5234](https://github.com/cube-js/cube/commit/8ca52342944b9767f2c34591a9241bf31cf78c71))

## [0.33.5](https://github.com/cube-js/cube/compare/v0.33.4...v0.33.5) (2023-05-11)

### Bug Fixes

- fix includes all for views ([#6559](https://github.com/cube-js/cube/issues/6559)) ([f62044a](https://github.com/cube-js/cube/commit/f62044aa7b60f6a2eeafc60e5649629aadc287a2))
- yml formatter escapse double quotes ([341e115](https://github.com/cube-js/cube/commit/341e115ef7fd6475f3d947fb0aa878addee86eb9))

## [0.33.4](https://github.com/cube-js/cube/compare/v0.33.3...v0.33.4) (2023-05-07)

### Bug Fixes

- join_path for JS schemas and \* includes with prefix ([#6555](https://github.com/cube-js/cube/issues/6555)) ([c76bbef](https://github.com/cube-js/cube/commit/c76bbef36e5b6594bcb79174ceb672d8e2b870ff))

## [0.33.2](https://github.com/cube-js/cube/compare/v0.33.1...v0.33.2) (2023-05-04)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [0.33.0](https://github.com/cube-js/cube/compare/v0.32.31...v0.33.0) (2023-05-02)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.31](https://github.com/cube-js/cube/compare/v0.32.30...v0.32.31) (2023-05-02)

### Features

- Support snake case security_context in COMPILE_CONTEXT ([#6519](https://github.com/cube-js/cube/issues/6519)) ([feb3fe3](https://github.com/cube-js/cube/commit/feb3fe389462fc4cf3947f8ecf2146c3fe102b9e))

## [0.32.30](https://github.com/cube-js/cube/compare/v0.32.29...v0.32.30) (2023-04-28)

### Bug Fixes

- model style guide, views folder ([#6493](https://github.com/cube-js/cube/issues/6493)) ([1fa7f99](https://github.com/cube-js/cube/commit/1fa7f994551e05fd2ef417bd2ee7f472881bc2bc))

### Features

- **playground:** cube type tag, public cubes ([#6482](https://github.com/cube-js/cube/issues/6482)) ([cede7a7](https://github.com/cube-js/cube/commit/cede7a71f7d2e8d9dc221669b6b1714ee146d8ea))

## [0.32.29](https://github.com/cube-js/cube/compare/v0.32.28...v0.32.29) (2023-04-25)

### Bug Fixes

- **schema-compiler:** yml backtick escaping ([#6478](https://github.com/cube-js/cube/issues/6478)) ([39a9ac0](https://github.com/cube-js/cube/commit/39a9ac0e9a641efeeca0e99d286962aa007551cd))

### Features

- **schema-compiler:** Support public/shown for segments ([#6491](https://github.com/cube-js/cube/issues/6491)) ([df6160c](https://github.com/cube-js/cube/commit/df6160cc62f8ab2fccbfb85f2916040aec218fc3))

## [0.32.28](https://github.com/cube-js/cube/compare/v0.32.27...v0.32.28) (2023-04-19)

### Bug Fixes

- **schema-compiler:** Incorrect camelize for meta ([#6444](https://github.com/cube-js/cube/issues/6444)) ([c82844e](https://github.com/cube-js/cube/commit/c82844e130b31722fa338022b740ac7285c01190))
- segment not found for path if rollup with segment is used within `rollupJoin` ([6a3ca80](https://github.com/cube-js/cube/commit/6a3ca8032151d530a30bab88c024c451d4141c01))

### Features

- using snake case for model files, model templates updates ([#6447](https://github.com/cube-js/cube/issues/6447)) ([37e3180](https://github.com/cube-js/cube/commit/37e3180d4218b342ee0c2fe0ef90464aa6633b54))

## [0.32.27](https://github.com/cube-js/cube/compare/v0.32.26...v0.32.27) (2023-04-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.26](https://github.com/cube-js/cube/compare/v0.32.25...v0.32.26) (2023-04-13)

### Bug Fixes

- **cli:** Validate command - allow node require ([#6416](https://github.com/cube-js/cube/issues/6416)) ([ead63d8](https://github.com/cube-js/cube/commit/ead63d823253e8cd42e9353a09d4481e5e889d0b))

### Features

- **schema-compiler:** JS - support snake_case for any properties ([#6396](https://github.com/cube-js/cube/issues/6396)) ([f1b4c0b](https://github.com/cube-js/cube/commit/f1b4c0bfb2e8b8ad20853af6232388c6d764a780))

## [0.32.25](https://github.com/cube-js/cube/compare/v0.32.24...v0.32.25) (2023-04-12)

### Bug Fixes

- **schema-compiler:** Yaml - crash on fully commented file ([#6413](https://github.com/cube-js/cube/issues/6413)) ([29d2eae](https://github.com/cube-js/cube/commit/29d2eaed8c2d8fe7575be732bc21acbf8dadafbb))

### Features

- **duckdb-driver:** Support countDistinctApprox ([#6409](https://github.com/cube-js/cube/issues/6409)) ([6b1fadb](https://github.com/cube-js/cube/commit/6b1fadb0954ce349a55b88a158d236c2ca31ecfc))

## [0.32.23](https://github.com/cube-js/cube/compare/v0.32.22...v0.32.23) (2023-04-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.22](https://github.com/cube-js/cube/compare/v0.32.21...v0.32.22) (2023-04-10)

### Bug Fixes

- Correct the syntax error in the ClickHouse cast method ([#6154](https://github.com/cube-js/cube/issues/6154)) Thanks [@fxx-real](https://github.com/fxx-real)! ([b5ccf18](https://github.com/cube-js/cube/commit/b5ccf18cd56ee9cc9d8c1c9bb5ffd519c00d0f3c))

### Features

- **schema-compiler:** Yaml - report error on misconfiguration (object instead of array) ([#6407](https://github.com/cube-js/cube/issues/6407)) ([29e3778](https://github.com/cube-js/cube/commit/29e37780c9c4f320c868880266dc73f55840987f))

## [0.32.21](https://github.com/cube-js/cube/compare/v0.32.20...v0.32.21) (2023-04-06)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.20](https://github.com/cube-js/cube/compare/v0.32.19...v0.32.20) (2023-04-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.19](https://github.com/cube-js/cube/compare/v0.32.18...v0.32.19) (2023-04-03)

### Features

- **schema-compiler:** Support new aliases for join.relationship ([#6367](https://github.com/cube-js/cube/issues/6367)) ([34a18d8](https://github.com/cube-js/cube/commit/34a18d8936d6e3e27d0dcba2456dbac3f03b0d76))

## [0.32.18](https://github.com/cube-js/cube/compare/v0.32.17...v0.32.18) (2023-04-02)

### Features

- Cube based includes and meta exposure ([#6380](https://github.com/cube-js/cube/issues/6380)) ([ead4ac0](https://github.com/cube-js/cube/commit/ead4ac0b3b78c074f29fcdec3f86282bce42c45a))
- **schema-compiler:** Introduce public field (alias for shown) ([#6361](https://github.com/cube-js/cube/issues/6361)) ([6d0c66a](https://github.com/cube-js/cube/commit/6d0c66aad43d51dd23cc3b0cbd6ba3454581afd6))

## [0.32.17](https://github.com/cube-js/cube/compare/v0.32.16...v0.32.17) (2023-03-29)

### Features

- **schema-compiler:** Introduce support for sqlTable (syntactic sugar) ([#6360](https://github.com/cube-js/cube/issues/6360)) ([c73b368](https://github.com/cube-js/cube/commit/c73b368ca20735c63d01d2364b1c2f26d3b24cc5))

## [0.32.16](https://github.com/cube-js/cube/compare/v0.32.15...v0.32.16) (2023-03-27)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.15](https://github.com/cube-js/cube/compare/v0.32.14...v0.32.15) (2023-03-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.14](https://github.com/cube-js/cube/compare/v0.32.13...v0.32.14) (2023-03-23)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.13](https://github.com/cube-js/cube/compare/v0.32.12...v0.32.13) (2023-03-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.12](https://github.com/cube-js/cube/compare/v0.32.11...v0.32.12) (2023-03-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.10](https://github.com/cube-js/cube.js/compare/v0.32.9...v0.32.10) (2023-03-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.8](https://github.com/cube-js/cube.js/compare/v0.32.7...v0.32.8) (2023-03-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.7](https://github.com/cube-js/cube.js/compare/v0.32.6...v0.32.7) (2023-03-14)

### Bug Fixes

- Cross data source `rollupLambda` uses incorrect refreshKey ([#6288](https://github.com/cube-js/cube.js/issues/6288)) ([f87be98](https://github.com/cube-js/cube.js/commit/f87be98c68bcdcb65bd317b845571ad186586bd1))

## [0.32.6](https://github.com/cube-js/cube.js/compare/v0.32.5...v0.32.6) (2023-03-14)

### Features

- **schema-compiler:** quarter granularity ([#6285](https://github.com/cube-js/cube.js/issues/6285)) ([22f6102](https://github.com/cube-js/cube.js/commit/22f6102e137940b6882d55f560d826feceaf470d))

## [0.32.5](https://github.com/cube-js/cube.js/compare/v0.32.4...v0.32.5) (2023-03-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.4](https://github.com/cube-js/cube.js/compare/v0.32.3...v0.32.4) (2023-03-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.3](https://github.com/cube-js/cube.js/compare/v0.32.2...v0.32.3) (2023-03-07)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.32.2](https://github.com/cube-js/cube.js/compare/v0.32.1...v0.32.2) (2023-03-07)

### Bug Fixes

- Replace deprecated `@hapi/joi` with `joi` ([#6223](https://github.com/cube-js/cube.js/issues/6223)) Thanks [@hehex9](https://github.com/hehex9) ! ([ccbcc50](https://github.com/cube-js/cube.js/commit/ccbcc501dc91ef68ca49ddced79316425ae8f215))

### Features

- Support rollupLambda across different cubes and data sources ([#6245](https://github.com/cube-js/cube.js/issues/6245)) ([fa284a4](https://github.com/cube-js/cube.js/commit/fa284a4e04ac0f80e9e06fb1dc48b5c07204c5a4))

## [0.32.1](https://github.com/cube-js/cube.js/compare/v0.32.0...v0.32.1) (2023-03-03)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [0.32.0](https://github.com/cube-js/cube.js/compare/v0.31.69...v0.32.0) (2023-03-02)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.69](https://github.com/cube-js/cube.js/compare/v0.31.68...v0.31.69) (2023-03-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.68](https://github.com/cube-js/cube.js/compare/v0.31.67...v0.31.68) (2023-02-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.67](https://github.com/cube-js/cube.js/compare/v0.31.66...v0.31.67) (2023-02-27)

### Bug Fixes

- **schema-compiler:** skip empty YAML files ([b068f7f](https://github.com/cube-js/cube.js/commit/b068f7f4f44c8bede3d45aac8df4bc8b4f38912e))

### Features

- **cube-cli:** Schema validation command ([#6208](https://github.com/cube-js/cube.js/issues/6208)) ([1fc6490](https://github.com/cube-js/cube.js/commit/1fc64906e5e628437fb58d35feea8ab3aa5bfe06))

## [0.31.66](https://github.com/cube-js/cube.js/compare/v0.31.65...v0.31.66) (2023-02-27)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.65](https://github.com/cube-js/cube.js/compare/v0.31.64...v0.31.65) (2023-02-23)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.64](https://github.com/cube-js/cube.js/compare/v0.31.63...v0.31.64) (2023-02-21)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.63](https://github.com/cube-js/cube.js/compare/v0.31.62...v0.31.63) (2023-02-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.62](https://github.com/cube-js/cube.js/compare/v0.31.61...v0.31.62) (2023-02-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.60](https://github.com/cube-js/cube.js/compare/v0.31.59...v0.31.60) (2023-02-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.59](https://github.com/cube-js/cube.js/compare/v0.31.58...v0.31.59) (2023-02-06)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.58](https://github.com/cube-js/cube.js/compare/v0.31.57...v0.31.58) (2023-02-02)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.57](https://github.com/cube-js/cube.js/compare/v0.31.56...v0.31.57) (2023-02-02)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.56](https://github.com/cube-js/cube.js/compare/v0.31.55...v0.31.56) (2023-01-31)

### Bug Fixes

- (dimensions.type.ownedByCube = true) is not allowed if dynamic schemas outside of schema folder are used ([88b09bd](https://github.com/cube-js/cube.js/commit/88b09bd14e13a05599c9f433063832a17664ed92))

## [0.31.55](https://github.com/cube-js/cube.js/compare/v0.31.54...v0.31.55) (2023-01-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.54](https://github.com/cube-js/cube.js/compare/v0.31.53...v0.31.54) (2023-01-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.53](https://github.com/cube-js/cube.js/compare/v0.31.52...v0.31.53) (2023-01-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.52](https://github.com/cube-js/cube.js/compare/v0.31.51...v0.31.52) (2023-01-23)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.51](https://github.com/cube-js/cube.js/compare/v0.31.50...v0.31.51) (2023-01-21)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.50](https://github.com/cube-js/cube.js/compare/v0.31.49...v0.31.50) (2023-01-21)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.49](https://github.com/cube-js/cube.js/compare/v0.31.48...v0.31.49) (2023-01-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.48](https://github.com/cube-js/cube.js/compare/v0.31.47...v0.31.48) (2023-01-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.47](https://github.com/cube-js/cube.js/compare/v0.31.46...v0.31.47) (2023-01-18)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.46](https://github.com/cube-js/cube.js/compare/v0.31.45...v0.31.46) (2023-01-18)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.45](https://github.com/cube-js/cube.js/compare/v0.31.44...v0.31.45) (2023-01-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.44](https://github.com/cube-js/cube.js/compare/v0.31.43...v0.31.44) (2023-01-16)

### Bug Fixes

- Rollups are sorted by name in `rollups` params of pre-aggregations ([#6011](https://github.com/cube-js/cube.js/issues/6011)) ([235ec70](https://github.com/cube-js/cube.js/commit/235ec70c296846f02e337acc4be90b4863556d15))

## [0.31.43](https://github.com/cube-js/cube.js/compare/v0.31.42...v0.31.43) (2023-01-16)

### Features

- Pre-aggregations API for `rollupLambda` support ([#6009](https://github.com/cube-js/cube.js/issues/6009)) ([b90b3f2](https://github.com/cube-js/cube.js/commit/b90b3f2b5c991a0046220d7821ce74fc11ddbb85))

## [0.31.42](https://github.com/cube-js/cube.js/compare/v0.31.41...v0.31.42) (2023-01-15)

### Features

- Multiple rollups in `rollupLambda` support ([#6008](https://github.com/cube-js/cube.js/issues/6008)) ([84fff0d](https://github.com/cube-js/cube.js/commit/84fff0d7745ccf657bb8eca0c5cc426c82ffd516))

## [0.31.41](https://github.com/cube-js/cube.js/compare/v0.31.40...v0.31.41) (2023-01-13)

### Features

- streaming capabilities ([#5995](https://github.com/cube-js/cube.js/issues/5995)) ([d336c4e](https://github.com/cube-js/cube.js/commit/d336c4eaa3547422484bb003df19dfd4c7be5f96))

## [0.31.40](https://github.com/cube-js/cube.js/compare/v0.31.39...v0.31.40) (2023-01-12)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.39](https://github.com/cube-js/cube.js/compare/v0.31.38...v0.31.39) (2023-01-12)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.38](https://github.com/cube-js/cube.js/compare/v0.31.37...v0.31.38) (2023-01-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.37](https://github.com/cube-js/cube.js/compare/v0.31.36...v0.31.37) (2023-01-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.36](https://github.com/cube-js/cube.js/compare/v0.31.35...v0.31.36) (2023-01-08)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.35](https://github.com/cube-js/cube.js/compare/v0.31.34...v0.31.35) (2023-01-07)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.34](https://github.com/cube-js/cube.js/compare/v0.31.33...v0.31.34) (2023-01-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.33](https://github.com/cube-js/cube.js/compare/v0.31.32...v0.31.33) (2023-01-03)

### Features

- Introduce CubeStoreCacheDriver ([#5511](https://github.com/cube-js/cube.js/issues/5511)) ([fb39776](https://github.com/cube-js/cube.js/commit/fb3977631adb126183d5d2775c70821c6c099897))

## [0.31.32](https://github.com/cube-js/cube.js/compare/v0.31.31...v0.31.32) (2022-12-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.31](https://github.com/cube-js/cube.js/compare/v0.31.30...v0.31.31) (2022-12-23)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.30](https://github.com/cube-js/cube.js/compare/v0.31.29...v0.31.30) (2022-12-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.29](https://github.com/cube-js/cube.js/compare/v0.31.28...v0.31.29) (2022-12-18)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.28](https://github.com/cube-js/cube.js/compare/v0.31.27...v0.31.28) (2022-12-16)

### Features

- Support `string`, `time` and `boolean` measures ([#5842](https://github.com/cube-js/cube.js/issues/5842)) ([4543ede](https://github.com/cube-js/cube.js/commit/4543edefe5b2432c90bb8530bc6a3c24c5548de3))

## [0.31.26](https://github.com/cube-js/cube.js/compare/v0.31.25...v0.31.26) (2022-12-13)

### Bug Fixes

- **trino-driver:** Timezone issue fix ([#5731](https://github.com/cube-js/cube.js/issues/5731)) Thanks [@yuraborue](https://github.com/yuraborue) ! ([70df903](https://github.com/cube-js/cube.js/commit/70df9034f4246be06956cbf1dc69de709d4d3df8))

## [0.31.23](https://github.com/cube-js/cube.js/compare/v0.31.22...v0.31.23) (2022-12-09)

### Features

- query limits ([#5763](https://github.com/cube-js/cube.js/issues/5763)) ([4ec172b](https://github.com/cube-js/cube.js/commit/4ec172b3dd27193d145afcdb8d9bf7ef1bd7505d))

## [0.31.21](https://github.com/cube-js/cube.js/compare/v0.31.20...v0.31.21) (2022-12-06)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.19](https://github.com/cube-js/cube.js/compare/v0.31.18...v0.31.19) (2022-11-29)

### Bug Fixes

- packages/cubejs-schema-compiler/package.json to reduce vulnerabilities ([#5359](https://github.com/cube-js/cube.js/issues/5359)) ([cd81554](https://github.com/cube-js/cube.js/commit/cd81554079533eae2c35ce142b26ad862b6e250d))

## [0.31.18](https://github.com/cube-js/cube.js/compare/v0.31.17...v0.31.18) (2022-11-28)

### Features

- yaml schema generation ([#5676](https://github.com/cube-js/cube.js/issues/5676)) ([fc0b810](https://github.com/cube-js/cube.js/commit/fc0b81016e5c66156d06f6834187d3323e9e8ca8))

## [0.31.16](https://github.com/cube-js/cube.js/compare/v0.31.15...v0.31.16) (2022-11-23)

### Features

- **ksql-driver:** Support offset earliest, replays and per partition streaming ([#5663](https://github.com/cube-js/cube.js/issues/5663)) ([3a79d02](https://github.com/cube-js/cube.js/commit/3a79d02c9794b5e437a52f9f0be72eb82a92805a))

## [0.31.15](https://github.com/cube-js/cube.js/compare/v0.31.14...v0.31.15) (2022-11-17)

### Bug Fixes

- `extends` YAML support ([982885e](https://github.com/cube-js/cube.js/commit/982885e8e41b351e27919551688f50f7e5af3a5a))

### Features

- Replace YAML parser to provide more meaningful parse errors ([9984066](https://github.com/cube-js/cube.js/commit/99840665ee31aa8f14cf9c83b19d4a4cbc3a978a))
- Support snake case in YAML relationship field ([f20fe6b](https://github.com/cube-js/cube.js/commit/f20fe6baa43142589a5a85be5af1daefb56acd8b))

## [0.31.13](https://github.com/cube-js/cube.js/compare/v0.31.12...v0.31.13) (2022-11-08)

### Features

- notStartsWith/notEndsWith filters support ([#5579](https://github.com/cube-js/cube.js/issues/5579)) ([8765833](https://github.com/cube-js/cube.js/commit/87658333df0194db07c3ce0ae6f94a292f8bd592))
- YAML snake case and `.yaml` extension support ([#5578](https://github.com/cube-js/cube.js/issues/5578)) ([c8af286](https://github.com/cube-js/cube.js/commit/c8af2864af2dcc532abdc3629cf58893d874c190))

## [0.31.12](https://github.com/cube-js/cube.js/compare/v0.31.11...v0.31.12) (2022-11-05)

### Bug Fixes

- Cannot read property 'apply' of undefined on missed dimension sql ([#5559](https://github.com/cube-js/cube.js/issues/5559)) ([6f85096](https://github.com/cube-js/cube.js/commit/6f850967ba834bbdd87284fa957859d66d19344a))
- No column found in case non equals filter query incorrectly matched against rollup with no dimensions ([#5552](https://github.com/cube-js/cube.js/issues/5552)) ([73b3203](https://github.com/cube-js/cube.js/commit/73b3203925bf9d8221001f730bb23272dd4e47e6))
- YAML filter support ([fb6fade](https://github.com/cube-js/cube.js/commit/fb6fade4bfa5b0fb2e3b6895e378aef97ea26f95))

## [0.31.11](https://github.com/cube-js/cube.js/compare/v0.31.10...v0.31.11) (2022-11-02)

### Features

- **cubestore:** Sealing partition ([#5523](https://github.com/cube-js/cube.js/issues/5523)) ([70ee72c](https://github.com/cube-js/cube.js/commit/70ee72cca5b9a77bf14994f05b0e77148089362c))

## [0.31.9](https://github.com/cube-js/cube.js/compare/v0.31.8...v0.31.9) (2022-11-01)

### Bug Fixes

- You requested hidden member in case of rolling window measure is being used in a view ([#5546](https://github.com/cube-js/cube.js/issues/5546)) ([c00ea43](https://github.com/cube-js/cube.js/commit/c00ea432021a02638361b278f8eb24801a7a867b))

## [0.31.8](https://github.com/cube-js/cube.js/compare/v0.31.7...v0.31.8) (2022-10-30)

### Features

- YAML support ([#5539](https://github.com/cube-js/cube.js/issues/5539)) ([29c19db](https://github.com/cube-js/cube.js/commit/29c19db9315ef7ad40350150f74f9519d6ff4a98))

## [0.31.7](https://github.com/cube-js/cube.js/compare/v0.31.6...v0.31.7) (2022-10-27)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.5](https://github.com/cube-js/cube.js/compare/v0.31.4...v0.31.5) (2022-10-20)

### Features

- pre-aggregations build jobs API endpoint ([#5251](https://github.com/cube-js/cube.js/issues/5251)) ([4aee3ef](https://github.com/cube-js/cube.js/commit/4aee3efba9c14b4698c81707d92af11c98978c81))

## [0.31.4](https://github.com/cube-js/cube.js/compare/v0.31.3...v0.31.4) (2022-10-13)

### Features

- `shown` flag for cubes and views ([#5455](https://github.com/cube-js/cube.js/issues/5455)) ([e7ef446](https://github.com/cube-js/cube.js/commit/e7ef4467adfc581e07c14b315a43c0eb4d1e8c11))

## [0.31.3](https://github.com/cube-js/cube.js/compare/v0.31.2...v0.31.3) (2022-10-08)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.31.2](https://github.com/cube-js/cube.js/compare/v0.31.1...v0.31.2) (2022-10-08)

### Features

- Includes and Excludes directives for Cube Views ([#5437](https://github.com/cube-js/cube.js/issues/5437)) ([7c35604](https://github.com/cube-js/cube.js/commit/7c356049b56a0ea58a4ad8f2628ffaff2ac307d4))

## [0.31.1](https://github.com/cube-js/cube.js/compare/v0.31.0...v0.31.1) (2022-10-04)

### Bug Fixes

- Cube not found for path due to FILTER_PARAMS are used in members ([#5417](https://github.com/cube-js/cube.js/issues/5417)) ([bfe76bf](https://github.com/cube-js/cube.js/commit/bfe76bfbf3bd5f51a408088f8f5fefb35e17e22f))
- **extensions:** fix SELECT handling in the Funnels extensions ([#5397](https://github.com/cube-js/cube.js/issues/5397)) ([041f591](https://github.com/cube-js/cube.js/commit/041f591482cc75d840310a9b7592158c1b33fa37))

# [0.31.0](https://github.com/cube-js/cube.js/compare/v0.30.75...v0.31.0) (2022-10-03)

- feat!: Cube Views implementation (#5278) ([9937356](https://github.com/cube-js/cube.js/commit/99373563b610d1f15dfc44fafac0c329c1dc9a0d)), closes [#5278](https://github.com/cube-js/cube.js/issues/5278)

### Bug Fixes

- **schema-compiler:** throw an error for the empty pre-aggs ([#5392](https://github.com/cube-js/cube.js/issues/5392)) ([4afd604](https://github.com/cube-js/cube.js/commit/4afd6041dae8175fb8d292e9ee0db15969239c81))

### BREAKING CHANGES

- The logic of how cubes are included in joins has been changed. There are multiple member reference constructs that are now forbidden and should be rewritten. You can't reference foreign cubes anymore to define members inside other cubes: `${ForeignCube}.foo`. `foo` member should be defined in `ForeignCube` and referenced as `${ForeignCube.foo}`. You also can't mix references and members without `CUBE` self-reference. For example `${ForeignCube.foo} + bar` is invalid and `${ForeignCube.foo} + ${CUBE}.bar` should be used instead. If not fixed, it'll lead to missing tables in the `FROM` clause.

## [0.30.75](https://github.com/cube-js/cube.js/compare/v0.30.74...v0.30.75) (2022-09-22)

### Bug Fixes

- Invalid identifier day for month `lambdaRollup` ([#5338](https://github.com/cube-js/cube.js/issues/5338)) ([bacc643](https://github.com/cube-js/cube.js/commit/bacc64309b09c99cddc0a57bfcca94ee01dd5877))

## [0.30.73](https://github.com/cube-js/cube.js/compare/v0.30.72...v0.30.73) (2022-09-19)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.72](https://github.com/cube-js/cube.js/compare/v0.30.71...v0.30.72) (2022-09-18)

### Features

- Introduce `rollupLambda` rollup type ([#5315](https://github.com/cube-js/cube.js/issues/5315)) ([6fd5ee4](https://github.com/cube-js/cube.js/commit/6fd5ee4ed3a7fe98f55ce2f3dc900be1f089e590))

## [0.30.70](https://github.com/cube-js/cube.js/compare/v0.30.69...v0.30.70) (2022-09-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.69](https://github.com/cube-js/cube.js/compare/v0.30.68...v0.30.69) (2022-09-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.67](https://github.com/cube-js/cube.js/compare/v0.30.66...v0.30.67) (2022-09-09)

### Bug Fixes

- **schema-compiler:** messed order in sub-query aggregate ([#5257](https://github.com/cube-js/cube.js/issues/5257)) ([a6ad9f6](https://github.com/cube-js/cube.js/commit/a6ad9f63fd9db502d1e59aa4fe30dbe5c34c364d))

## [0.30.63](https://github.com/cube-js/cube.js/compare/v0.30.62...v0.30.63) (2022-09-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.61](https://github.com/cube-js/cube.js/compare/v0.30.60...v0.30.61) (2022-09-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.60](https://github.com/cube-js/cube.js/compare/v0.30.59...v0.30.60) (2022-08-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.57](https://github.com/cube-js/cube.js/compare/v0.30.56...v0.30.57) (2022-08-25)

### Features

- **cubestore:** Aggregating index in pre-aggregations ([#5016](https://github.com/cube-js/cube.js/issues/5016)) ([0c3caca](https://github.com/cube-js/cube.js/commit/0c3caca7ac6bfe12fb95a15bb1bc538d1363ecdf))

## [0.30.53](https://github.com/cube-js/cube.js/compare/v0.30.52...v0.30.53) (2022-08-18)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.51](https://github.com/cube-js/cube.js/compare/v0.30.50...v0.30.51) (2022-08-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.50](https://github.com/cube-js/cube.js/compare/v0.30.49...v0.30.50) (2022-08-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.47](https://github.com/cube-js/cube.js/compare/v0.30.46...v0.30.47) (2022-08-12)

### Features

- **cubejs:** LambdaView: hybrid query of source tables and pre-aggregation tables. ([#4718](https://github.com/cube-js/cube.js/issues/4718)) ([4ae826b](https://github.com/cube-js/cube.js/commit/4ae826b4d27afbfce366830150e130f29c7fcbbf))

## [0.30.46](https://github.com/cube-js/cube.js/compare/v0.30.45...v0.30.46) (2022-08-10)

### Features

- **drivers:** Bootstraps CrateDB driver ([#4929](https://github.com/cube-js/cube.js/issues/4929)) ([db87b8f](https://github.com/cube-js/cube.js/commit/db87b8f18686607498467c6ff0f71abcd1e38c5d))
- **schema-compiler:** aggregated sub query group by clause ([#5078](https://github.com/cube-js/cube.js/issues/5078)) ([473398d](https://github.com/cube-js/cube.js/commit/473398d0a4a983730aba115766afc53f2dd829a6))

## [0.30.45](https://github.com/cube-js/cube.js/compare/v0.30.44...v0.30.45) (2022-08-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.43](https://github.com/cube-js/cube.js/compare/v0.30.42...v0.30.43) (2022-07-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.39](https://github.com/cube-js/cube.js/compare/v0.30.38...v0.30.39) (2022-07-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.34](https://github.com/cube-js/cube.js/compare/v0.30.33...v0.30.34) (2022-07-12)

### Bug Fixes

- Cache allDefinitions Object.assign invocations to optimize highly dynamic schema SQL execution ([#4894](https://github.com/cube-js/cube.js/issues/4894)) ([ad3fea8](https://github.com/cube-js/cube.js/commit/ad3fea8610aaee7aae33623821a2aa3a3416a62a))

## [0.30.30](https://github.com/cube-js/cube.js/compare/v0.30.29...v0.30.30) (2022-07-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.28](https://github.com/cube-js/cube.js/compare/v0.30.27...v0.30.28) (2022-06-27)

### Bug Fixes

- More explanatory error message on failed rollupJoin match ([5808e37](https://github.com/cube-js/cube.js/commit/5808e375baf31259d8d070f3d5ff5b2a4faf5db4)), closes [#4746](https://github.com/cube-js/cube.js/issues/4746)

## [0.30.25](https://github.com/cube-js/cube.js/compare/v0.30.24...v0.30.25) (2022-06-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.20](https://github.com/cube-js/cube.js/compare/v0.30.19...v0.30.20) (2022-06-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.8](https://github.com/cube-js/cube.js/compare/v0.30.7...v0.30.8) (2022-05-30)

### Features

- **schema-compiler:** allowNonStrictDateRangeMatch flag support for the pre-aggregations with time dimension ([#4582](https://github.com/cube-js/cube.js/issues/4582)) ([31d9fae](https://github.com/cube-js/cube.js/commit/31d9faea762f2103a19e6d3062e272b9dbc88dc8))

## [0.30.7](https://github.com/cube-js/cube.js/compare/v0.30.6...v0.30.7) (2022-05-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.6](https://github.com/cube-js/cube.js/compare/v0.30.5...v0.30.6) (2022-05-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.30.5](https://github.com/cube-js/cube.js/compare/v0.30.4...v0.30.5) (2022-05-23)

### Bug Fixes

- **schema-compiler:** exclude non-included dimensions from drillMembers ([#4569](https://github.com/cube-js/cube.js/issues/4569)) ([c1f12d1](https://github.com/cube-js/cube.js/commit/c1f12d182a3f7434a296a97b640b19b705dba921))

## [0.30.4](https://github.com/cube-js/cube.js/compare/v0.30.3...v0.30.4) (2022-05-20)

### Features

- Download streaming `select * from table` originalSql pre-aggregations directly ([#4537](https://github.com/cube-js/cube.js/issues/4537)) ([a62d81a](https://github.com/cube-js/cube.js/commit/a62d81abe67fc94ca57cabf914cc55800fc89d96))

## [0.30.1](https://github.com/cube-js/cube.js/compare/v0.30.0...v0.30.1) (2022-05-14)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [0.30.0](https://github.com/cube-js/cube.js/compare/v0.29.57...v0.30.0) (2022-05-11)

### Features

- **streamlined-config:** CUBEJS_EXTERNAL_DEFAULT and CUBEJS_SCHEDULED_REFRESH_DEFAULT defaults changed to "true" ([#4367](https://github.com/cube-js/cube.js/issues/4367)) ([d52adaf](https://github.com/cube-js/cube.js/commit/d52adaf9d7e95d9892348c8a2fbc971c4652dae3))

## [0.29.57](https://github.com/cube-js/cube.js/compare/v0.29.56...v0.29.57) (2022-05-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.54](https://github.com/cube-js/cube.js/compare/v0.29.53...v0.29.54) (2022-05-03)

### Bug Fixes

- Prestodb timestamp cast to work in prestodb and athena ([#4419](https://github.com/cube-js/cube.js/issues/4419)) Thanks [@apzeb](https://github.com/apzeb) ! ([8f8f61a](https://github.com/cube-js/cube.js/commit/8f8f61a7186aeb63876bfcb8d440a7c35e27e0b3)), closes [#4221](https://github.com/cube-js/cube.js/issues/4221)

### Features

- **cubejs:** rollupJoin between multiple databases ([#4371](https://github.com/cube-js/cube.js/issues/4371)) ([6cd77d5](https://github.com/cube-js/cube.js/commit/6cd77d542ec8af570f556a4cefd1710ab2e5f508))

## [0.29.53](https://github.com/cube-js/cube.js/compare/v0.29.52...v0.29.53) (2022-04-29)

### Features

- **packages:** Materialize driver ([#4320](https://github.com/cube-js/cube.js/issues/4320)) ([d40d13b](https://github.com/cube-js/cube.js/commit/d40d13b0a80cd65b27337e87a523314af585dbc6))

## [0.29.52](https://github.com/cube-js/cube.js/compare/v0.29.51...v0.29.52) (2022-04-23)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.51](https://github.com/cube-js/cube.js/compare/v0.29.50...v0.29.51) (2022-04-22)

### Features

- **query-language:** "startsWith", "endsWith" filters support ([#4128](https://github.com/cube-js/cube.js/issues/4128)) ([e8c72d6](https://github.com/cube-js/cube.js/commit/e8c72d630eecd930a8fd36fc52f9b594a45d59c0))

## [0.29.48](https://github.com/cube-js/cube.js/compare/v0.29.47...v0.29.48) (2022-04-14)

### Features

- **query-language:** "total" flag support ([#4134](https://github.com/cube-js/cube.js/issues/4134)) ([51aef5e](https://github.com/cube-js/cube.js/commit/51aef5ede6e9b0c0e0e8749119e98102f168b8ca))
- Support Compound Primary Keys ([#4370](https://github.com/cube-js/cube.js/issues/4370)) Thanks [@rccoe](https://github.com/rccoe)! ([0e3983c](https://github.com/cube-js/cube.js/commit/0e3983ccb30a3f6815ba5ee28aa09807e55fd2a3)), closes [#4364](https://github.com/cube-js/cube.js/issues/4364)

## [0.29.42](https://github.com/cube-js/cube.js/compare/v0.29.41...v0.29.42) (2022-04-04)

### Bug Fixes

- **playground:** rollup designer count distinct warning ([#4309](https://github.com/cube-js/cube.js/issues/4309)) ([add2dd3](https://github.com/cube-js/cube.js/commit/add2dd3106f7fd9deb3ea88fa74467ccd1b9244f))

## [0.29.37](https://github.com/cube-js/cube.js/compare/v0.29.36...v0.29.37) (2022-03-29)

### Features

- **@cubejs-backend/schema-compiler:** Allow filtering with or/and in FILTER_PARAMS ([#4253](https://github.com/cube-js/cube.js/issues/4253)) Thanks [@tchell](https://github.com/tchell)! ([cb1446c](https://github.com/cube-js/cube.js/commit/cb1446c4e788bcb43bdc90fc0a48ffc49fd0d891))

## [0.29.35](https://github.com/cube-js/cube.js/compare/v0.29.34...v0.29.35) (2022-03-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.33](https://github.com/cube-js/cube.js/compare/v0.29.32...v0.29.33) (2022-03-17)

### Features

- **playground:** non-additive measures message ([#4236](https://github.com/cube-js/cube.js/issues/4236)) ([ae18bbc](https://github.com/cube-js/cube.js/commit/ae18bbcb9030d0eef03c74410c25902602ec6d43))

## [0.29.31](https://github.com/cube-js/cube.js/compare/v0.29.30...v0.29.31) (2022-03-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.29](https://github.com/cube-js/cube.js/compare/v0.29.28...v0.29.29) (2022-03-03)

### Bug Fixes

- **@cubejs-backend/schema-compiler:** Add strictness to booleans ([#4157](https://github.com/cube-js/cube.js/issues/4157)) Thanks [@zpencerq](https://github.com/zpencerq)! ([e918837](https://github.com/cube-js/cube.js/commit/e918837ec8c5eb7965620f43408a92f4c2b2bec5))

## [0.29.28](https://github.com/cube-js/cube.js/compare/v0.29.27...v0.29.28) (2022-02-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.27](https://github.com/cube-js/cube.js/compare/v0.29.26...v0.29.27) (2022-02-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.26](https://github.com/cube-js/cube.js/compare/v0.29.25...v0.29.26) (2022-02-07)

### Bug Fixes

- Use prototype name matching instead of classes to allow exact version mismatches for AbstractExtension ([75545e8](https://github.com/cube-js/cube.js/commit/75545e8ffbf88fd693e4c8c4afd78d86830925b2))

## [0.29.25](https://github.com/cube-js/cube.js/compare/v0.29.24...v0.29.25) (2022-02-03)

### Features

- Load metrics from DBT project ([#4000](https://github.com/cube-js/cube.js/issues/4000)) ([2975d84](https://github.com/cube-js/cube.js/commit/2975d84cd2a2d3bba3c31a7744ab5a5fb3789b6e))

## [0.29.24](https://github.com/cube-js/cube.js/compare/v0.29.23...v0.29.24) (2022-02-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.23](https://github.com/cube-js/cube.js/compare/v0.29.22...v0.29.23) (2022-01-26)

### Bug Fixes

- Error: column does not exist during in case of subQuery for rolling window measure ([6084407](https://github.com/cube-js/cube.js/commit/6084407cb7cad3f0d239959b072f4fa011aa29a4))

## [0.29.22](https://github.com/cube-js/cube.js/compare/v0.29.21...v0.29.22) (2022-01-21)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.21](https://github.com/cube-js/cube.js/compare/v0.29.20...v0.29.21) (2022-01-17)

### Features

- **@cubejs-backend/schema-compiler:** extend the schema generation API ([#3936](https://github.com/cube-js/cube.js/issues/3936)) ([48b2335](https://github.com/cube-js/cube.js/commit/48b2335c7d9810dc433fd8c76f4b3ec8a7b83442))

## [0.29.20](https://github.com/cube-js/cube.js/compare/v0.29.19...v0.29.20) (2022-01-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.18](https://github.com/cube-js/cube.js/compare/v0.29.17...v0.29.18) (2022-01-09)

### Bug Fixes

- **schema-compiler:** Handle null Values in Security Context ([#3868](https://github.com/cube-js/cube.js/issues/3868)) Thanks [@joealden](https://github.com/joealden) ! ([739367b](https://github.com/cube-js/cube.js/commit/739367bd50c1863ee8bae17741c0591065ea47a1))

## [0.29.17](https://github.com/cube-js/cube.js/compare/v0.29.16...v0.29.17) (2022-01-05)

### Bug Fixes

- Do not instantiate SqlParser if rewriteQueries is false to save cache memory ([00a239f](https://github.com/cube-js/cube.js/commit/00a239fae57ae9448337bd816b2ae5ca17f15230))

## [0.29.16](https://github.com/cube-js/cube.js/compare/v0.29.15...v0.29.16) (2022-01-05)

### Bug Fixes

- `refreshKey` is evaluated ten times more frequently if `sql` and `every` are simultaneously defined ([#3873](https://github.com/cube-js/cube.js/issues/3873)) ([c93ae12](https://github.com/cube-js/cube.js/commit/c93ae127b4d0ce2c214d3665f8d86f6ade5cce6f))

## [0.29.15](https://github.com/cube-js/cube.js/compare/v0.29.14...v0.29.15) (2021-12-30)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.12](https://github.com/cube-js/cube.js/compare/v0.29.11...v0.29.12) (2021-12-29)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.7](https://github.com/cube-js/cube.js/compare/v0.29.6...v0.29.7) (2021-12-20)

### Bug Fixes

- Table cache incorrectly invalidated after merge multi-tenant queues by default change ([#3828](https://github.com/cube-js/cube.js/issues/3828)) ([540446a](https://github.com/cube-js/cube.js/commit/540446a76e19be3aec38b96ad81c149f567e9e40))

## [0.29.6](https://github.com/cube-js/cube.js/compare/v0.29.5...v0.29.6) (2021-12-19)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.29.4](https://github.com/cube-js/cube.js/compare/v0.29.3...v0.29.4) (2021-12-16)

### Bug Fixes

- Validate `contextToAppId` is in place when COMPILE_CONTEXT is used ([54a8b84](https://github.com/cube-js/cube.js/commit/54a8b843eca965c6a098118da04ec480ea06fa76))

# [0.29.0](https://github.com/cube-js/cube.js/compare/v0.28.67...v0.29.0) (2021-12-14)

### Reverts

- Revert "BREAKING CHANGE: 0.29 (#3809)" (#3811) ([db005ed](https://github.com/cube-js/cube.js/commit/db005edc04d48e8251250ab9d0e19f496cf3b52b)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809) [#3811](https://github.com/cube-js/cube.js/issues/3811)

- BREAKING CHANGE: 0.29 (#3809) ([6f1418b](https://github.com/cube-js/cube.js/commit/6f1418b9963774844f341682e594601a56bb0084)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809)

### BREAKING CHANGES

- Drop support for Node.js 10 (12.x is a minimal version)
- Upgrade Node.js to 14 for Docker images
- Drop support for Node.js 15

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

- **@cubejs-backend/clickhouse-driver:** clickhouse joins full key query aggregate fails ([#3600](https://github.com/cube-js/cube.js/issues/3600)) Thanks [@antnmxmv](https://github.com/antnmxmv)! ([c6451cd](https://github.com/cube-js/cube.js/commit/c6451cdef8498d4d645167ed3da7cf2f599a2e5b)), closes [#3534](https://github.com/cube-js/cube.js/issues/3534)

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

- Use BaseQuery#evaluateSql() for evaluate refresh range references, pre-aggregations debug API ([#3352](https://github.com/cube-js/cube.js/issues/3352)) ([ea81650](https://github.com/cube-js/cube.js/commit/ea816509ee9c07707bb46fc8fac83e55c52aaf00))
- **@cubejs-backend/ksql-driver:** Scaffolding for empty schema generates empty prefix ([091e45c](https://github.com/cube-js/cube.js/commit/091e45c66b712491699856d0a203e442bdfbd888))

## [0.28.47](https://github.com/cube-js/cube.js/compare/v0.28.46...v0.28.47) (2021-10-22)

### Features

- ksql support ([#3507](https://github.com/cube-js/cube.js/issues/3507)) ([b7128d4](https://github.com/cube-js/cube.js/commit/b7128d43d2aaffdd7273555779176b3efe4e2aa6))

## [0.28.46](https://github.com/cube-js/cube.js/compare/v0.28.45...v0.28.46) (2021-10-20)

### Bug Fixes

- update error message for join across data sources ([#3435](https://github.com/cube-js/cube.js/issues/3435)) ([5ad72cc](https://github.com/cube-js/cube.js/commit/5ad72ccf0f6bbba3c362b04427b172210f0b8ada))
- **@cubejs-backend/snowflake-driver:** escape date_from and date_to in generated series SQL ([#3542](https://github.com/cube-js/cube.js/issues/3542)) Thanks to [@zpencerq](https://github.com/zpencerq) ! ([858b7fa](https://github.com/cube-js/cube.js/commit/858b7fa2dbc5ed08350a6f875189f6f608c6d55c)), closes [#3215](https://github.com/cube-js/cube.js/issues/3215)
- **schema-compiler:** assign isVisible to segments ([#3484](https://github.com/cube-js/cube.js/issues/3484)) Thanks to [@piktur](https://github.com/piktur)! ([53fdf27](https://github.com/cube-js/cube.js/commit/53fdf27bb522608f36343c4a14ef32bf64c43200))

## [0.28.42](https://github.com/cube-js/cube.js/compare/v0.28.41...v0.28.42) (2021-10-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.28.41](https://github.com/cube-js/cube.js/compare/v0.28.40...v0.28.41) (2021-10-12)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.28.40](https://github.com/cube-js/cube.js/compare/v0.28.39...v0.28.40) (2021-09-30)

### Bug Fixes

- Count distinct by week matches daily rollup in case of data range is daily ([#3490](https://github.com/cube-js/cube.js/issues/3490)) ([2401418](https://github.com/cube-js/cube.js/commit/24014188d63baa6f178242e738f72790369234a9))
- **@cubejs-backend/schema-compiler:** check segments when matching pre-aggregations ([#3494](https://github.com/cube-js/cube.js/issues/3494)) ([9357484](https://github.com/cube-js/cube.js/commit/9357484cdc924046b7371c7b701d307b1df84089))
- **@cubejs-backend/schema-compiler:** CubePropContextTranspiler expli… ([#3461](https://github.com/cube-js/cube.js/issues/3461)) ([2ae7f1d](https://github.com/cube-js/cube.js/commit/2ae7f1d51b0caca0fe9755a98463a6898960a11f))
- **@cubejs-backend/schema-compiler:** match query with no dimensions … ([#3472](https://github.com/cube-js/cube.js/issues/3472)) ([2a5dd4c](https://github.com/cube-js/cube.js/commit/2a5dd4cbe4805d9dea8f5f8e630bf613198195a3))

## [0.28.38](https://github.com/cube-js/cube.js/compare/v0.28.37...v0.28.38) (2021-09-20)

### Bug Fixes

- **@cubejs-backend/schema-compiler:** CubeValidator human readable error messages ([#3425](https://github.com/cube-js/cube.js/issues/3425)) ([22db0a6](https://github.com/cube-js/cube.js/commit/22db0a6b607b7ef1d2cbe399415ac64c639325f5))

## [0.28.37](https://github.com/cube-js/cube.js/compare/v0.28.36...v0.28.37) (2021-09-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.28.35](https://github.com/cube-js/cube.js/compare/v0.28.34...v0.28.35) (2021-09-13)

### Bug Fixes

- **gateway:** hidden members filtering ([#3384](https://github.com/cube-js/cube.js/issues/3384)) ([43ac8c3](https://github.com/cube-js/cube.js/commit/43ac8c30247944543f917cd893787ee8485fc985))

## [0.28.34](https://github.com/cube-js/cube.js/compare/v0.28.33...v0.28.34) (2021-09-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.28.33](https://github.com/cube-js/cube.js/compare/v0.28.32...v0.28.33) (2021-09-11)

### Bug Fixes

- `updateWindow` validation isn't consistent with `refreshKey` interval parsing -- allow `s` at the end of time interval ([#3403](https://github.com/cube-js/cube.js/issues/3403)) ([57559e7](https://github.com/cube-js/cube.js/commit/57559e7841657e1900a30522ce5a178d091b6474))

## [0.28.32](https://github.com/cube-js/cube.js/compare/v0.28.31...v0.28.32) (2021-09-06)

### Bug Fixes

- HLL Rolling window query fails ([#3380](https://github.com/cube-js/cube.js/issues/3380)) ([581a52a](https://github.com/cube-js/cube.js/commit/581a52a856aeee067bac9a680e22694bf507af04))

## [0.28.29](https://github.com/cube-js/cube.js/compare/v0.28.28...v0.28.29) (2021-08-31)

### Features

- Mixed rolling window and regular measure queries from rollup support ([#3326](https://github.com/cube-js/cube.js/issues/3326)) ([3147e33](https://github.com/cube-js/cube.js/commit/3147e339f14ede73e5b0d14d05b9dd1f8b79e7b8))
- Support multi-value filtering on same column through FILTER_PARAMS ([#2854](https://github.com/cube-js/cube.js/issues/2854)) Thanks to [@omab](https://github.com/omab)! ([efc5745](https://github.com/cube-js/cube.js/commit/efc57452af44ee31092b8dfbb33a7ba23e86bba5))

## [0.28.28](https://github.com/cube-js/cube.js/compare/v0.28.27...v0.28.28) (2021-08-26)

### Bug Fixes

- **playground:** reset state on query change ([#3321](https://github.com/cube-js/cube.js/issues/3321)) ([9c0d4fe](https://github.com/cube-js/cube.js/commit/9c0d4fe647f3f90a4d1a65782f5625469079b579))

## [0.28.27](https://github.com/cube-js/cube.js/compare/v0.28.26...v0.28.27) (2021-08-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.28.25](https://github.com/cube-js/cube.js/compare/v0.28.24...v0.28.25) (2021-08-20)

### Features

- **@cubejs-backend/dremio-driver:** support quarter granularity ([b193b04](https://github.com/cube-js/cube.js/commit/b193b048db4f104683ca5dc518b09647a8435a4e))
- **@cubejs-backend/mysql-driver:** Support quarter granularity ([#3289](https://github.com/cube-js/cube.js/issues/3289)) ([6922e5d](https://github.com/cube-js/cube.js/commit/6922e5da50d2056c00a2ca248665e133a6de28be))

## [0.28.24](https://github.com/cube-js/cube.js/compare/v0.28.23...v0.28.24) (2021-08-19)

### Features

- Added Quarter to the timeDimensions of ([3f62b2c](https://github.com/cube-js/cube.js/commit/3f62b2c125b2b7b752e370b65be4c89a0c65a623))
- Support quarter granularity ([4ad1356](https://github.com/cube-js/cube.js/commit/4ad1356ac2d2c4d479c25e60519b0f7b4c1605bb))

## [0.28.22](https://github.com/cube-js/cube.js/compare/v0.28.21...v0.28.22) (2021-08-17)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.28.21](https://github.com/cube-js/cube.js/compare/v0.28.20...v0.28.21) (2021-08-16)

### Bug Fixes

- Pre-aggregations should have default refreshKey of every 1 hour if it doesn't set for Cube ([#3259](https://github.com/cube-js/cube.js/issues/3259)) ([bc472aa](https://github.com/cube-js/cube.js/commit/bc472aac1a666c84ed9e7e00b2d4e9018a6b5719))

## [0.28.19](https://github.com/cube-js/cube.js/compare/v0.28.18...v0.28.19) (2021-08-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.28.17](https://github.com/cube-js/cube.js/compare/v0.28.16...v0.28.17) (2021-08-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.28.14](https://github.com/cube-js/cube.js/compare/v0.28.13...v0.28.14) (2021-08-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.28.13](https://github.com/cube-js/cube.js/compare/v0.28.12...v0.28.13) (2021-08-04)

### Bug Fixes

- Support rolling `countDistinctApprox` rollups ([#3185](https://github.com/cube-js/cube.js/issues/3185)) ([e731992](https://github.com/cube-js/cube.js/commit/e731992b351f68f1ee249c9412f679b1903a6f28))

## [0.28.11](https://github.com/cube-js/cube.js/compare/v0.28.10...v0.28.11) (2021-07-31)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.28.10](https://github.com/cube-js/cube.js/compare/v0.28.9...v0.28.10) (2021-07-30)

### Features

- Rolling window rollup support ([#3151](https://github.com/cube-js/cube.js/issues/3151)) ([109ab5b](https://github.com/cube-js/cube.js/commit/109ab5bec32255244412a24bf75402abd1cbfe49))

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

- Support every for refreshKey with SQL ([63cd8f4](https://github.com/cube-js/cube.js/commit/63cd8f4673f9312f9b685352c18bb3ed01a40e6c))

## [0.28.1](https://github.com/cube-js/cube.js/compare/v0.28.0...v0.28.1) (2021-07-19)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [0.28.0](https://github.com/cube-js/cube.js/compare/v0.27.53...v0.28.0) (2021-07-17)

### Features

- Move partition range evaluation from Schema Compiler to Query Orchestrator to allow unbounded queries on partitioned pre-aggregations ([8ea654e](https://github.com/cube-js/cube.js/commit/8ea654e93b57014fb2409e070b3a4c381985a9fd))

## [0.27.53](https://github.com/cube-js/cube.js/compare/v0.27.52...v0.27.53) (2021-07-13)

### Features

- **@cubejs-client/playground:** save pre-aggregations from the Rollup Designer ([#3096](https://github.com/cube-js/cube.js/issues/3096)) ([866f949](https://github.com/cube-js/cube.js/commit/866f949f2fc05e189a30b943a963aa7a3f697c81))

## [0.27.49](https://github.com/cube-js/cube.js/compare/v0.27.48...v0.27.49) (2021-07-08)

### Features

- Execute refreshKeys in externalDb (only for every) ([#3061](https://github.com/cube-js/cube.js/issues/3061)) ([75167a0](https://github.com/cube-js/cube.js/commit/75167a0e92028dbdd6f24aca85331b84be8ec3c3))

## [0.27.47](https://github.com/cube-js/cube.js/compare/v0.27.46...v0.27.47) (2021-07-06)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.27.46](https://github.com/cube-js/cube.js/compare/v0.27.45...v0.27.46) (2021-07-01)

### Features

- Rename refreshRangeStart/End to buildRangeStart/End ([232d117](https://github.com/cube-js/cube.js/commit/232d1179623b567b96b026ce35522b177bcafce5))

## [0.27.45](https://github.com/cube-js/cube.js/compare/v0.27.44...v0.27.45) (2021-06-30)

### Bug Fixes

- Unexpected refresh value for refreshKey (earlier then expected) ([#3031](https://github.com/cube-js/cube.js/issues/3031)) ([55f75ac](https://github.com/cube-js/cube.js/commit/55f75ac95c93ab07b5e04158236b2356c2482f2c))

## [0.27.44](https://github.com/cube-js/cube.js/compare/v0.27.43...v0.27.44) (2021-06-29)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.27.41](https://github.com/cube-js/cube.js/compare/v0.27.40...v0.27.41) (2021-06-25)

### Bug Fixes

- Use timeDimension without s on the end ([#2997](https://github.com/cube-js/cube.js/issues/2997)) ([5313836](https://github.com/cube-js/cube.js/commit/531383699d888efbea87a8eec27e839cc6142f41))

### Features

- Fetch pre-aggregation data preview by partition, debug api ([#2951](https://github.com/cube-js/cube.js/issues/2951)) ([4207f5d](https://github.com/cube-js/cube.js/commit/4207f5dea4f6c7a0237428f2d6fad468b98161a3))

## [0.27.40](https://github.com/cube-js/cube.js/compare/v0.27.39...v0.27.40) (2021-06-23)

### Features

- **mssql-driver:** Use DATETIME2 for timeStampCast ([ed13768](https://github.com/cube-js/cube.js/commit/ed13768d842392491a2545ac2f465d24e43986ba))

## [0.27.37](https://github.com/cube-js/cube.js/compare/v0.27.36...v0.27.37) (2021-06-21)

### Bug Fixes

- **mssql-driver:** Use DATETIME2 type in dateTimeCast ([#2962](https://github.com/cube-js/cube.js/issues/2962)) ([c8563ab](https://github.com/cube-js/cube.js/commit/c8563abca030bf43c3ef5f72ab00e294dcac5cd0))

### Features

- Remove support for view (dead code) ([de41702](https://github.com/cube-js/cube.js/commit/de41702492342f379d4098c065cbf6a61e0c5314))
- Support schema without references postfix ([22388cc](https://github.com/cube-js/cube.js/commit/22388cce0773aa57ac8888507904f6c2bd15f5ed))

## [0.27.35](https://github.com/cube-js/cube.js/compare/v0.27.34...v0.27.35) (2021-06-18)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.27.33](https://github.com/cube-js/cube.js/compare/v0.27.32...v0.27.33) (2021-06-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.27.31](https://github.com/cube-js/cube.js/compare/v0.27.30...v0.27.31) (2021-06-11)

### Features

- Write preAggregations block in schema generation ([2c1e150](https://github.com/cube-js/cube.js/commit/2c1e150ce70787381a34b22da32bf5b1e9e26bc6))

## [0.27.30](https://github.com/cube-js/cube.js/compare/v0.27.29...v0.27.30) (2021-06-04)

### Bug Fixes

- **@cubejs-client/playground:** pre-agg status ([#2904](https://github.com/cube-js/cube.js/issues/2904)) ([b18685f](https://github.com/cube-js/cube.js/commit/b18685f55a5f2bde8060cc7345dfd38f762307e3))
- pass timezone to pre-aggregation description ([#2884](https://github.com/cube-js/cube.js/issues/2884)) ([9cca41e](https://github.com/cube-js/cube.js/commit/9cca41ee18ee6bb0dbd0d6abe6f778d467a9a240))

### Features

- Make scheduledRefresh true by default (preview period) ([f3e648c](https://github.com/cube-js/cube.js/commit/f3e648c7a3d05bfe4719a8f820794f11611fb8c7))

## [0.27.29](https://github.com/cube-js/cube.js/compare/v0.27.27...v0.27.29) (2021-06-02)

### Bug Fixes

- Resolve refresh key sql for pre-aggregations meta api ([#2881](https://github.com/cube-js/cube.js/issues/2881)) ([55383b6](https://github.com/cube-js/cube.js/commit/55383b6d1d9755ded6e8815fc04ba3070e675199))

### Features

- **snowflake-driver:** Support HLL ([7b57840](https://github.com/cube-js/cube.js/commit/7b578401a5271a2cbe43266f0190b786b2191aaf))

## [0.27.28](https://github.com/cube-js/cube.js/compare/v0.27.27...v0.27.28) (2021-06-02)

### Bug Fixes

- Resolve refresh key sql for pre-aggregations meta api ([#2881](https://github.com/cube-js/cube.js/issues/2881)) ([55383b6](https://github.com/cube-js/cube.js/commit/55383b6d1d9755ded6e8815fc04ba3070e675199))

### Features

- **snowflake-driver:** Support HLL ([7b57840](https://github.com/cube-js/cube.js/commit/7b578401a5271a2cbe43266f0190b786b2191aaf))

## [0.27.25](https://github.com/cube-js/cube.js/compare/v0.27.24...v0.27.25) (2021-06-01)

### Features

- Pre-aggregations Meta API, part 2 ([#2804](https://github.com/cube-js/cube.js/issues/2804)) ([84b6e70](https://github.com/cube-js/cube.js/commit/84b6e70ed81e80cff0ba8d0dd9ad507132bb1b24))

## [0.27.22](https://github.com/cube-js/cube.js/compare/v0.27.21...v0.27.22) (2021-05-27)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.27.19](https://github.com/cube-js/cube.js/compare/v0.27.18...v0.27.19) (2021-05-24)

### Features

- Make rollup default for preAggregation.type ([4875fa1](https://github.com/cube-js/cube.js/commit/4875fa1be360372e7ed9dcaf9ded8b28bb348e2f))
- Pre-aggregations Meta API, part 1 ([#2801](https://github.com/cube-js/cube.js/issues/2801)) ([2245a77](https://github.com/cube-js/cube.js/commit/2245a7774666a3a8bd36703b2b4001b20789b943))

## [0.27.17](https://github.com/cube-js/cube.js/compare/v0.27.16...v0.27.17) (2021-05-22)

### Features

- Dont introspect schema, if driver can detect types ([3467b44](https://github.com/cube-js/cube.js/commit/3467b4472e800d8345260a5765542486ed93647b))

## [0.27.15](https://github.com/cube-js/cube.js/compare/v0.27.14...v0.27.15) (2021-05-18)

### Features

- Enable external pre-aggregations by default for new users ([22de035](https://github.com/cube-js/cube.js/commit/22de0358ec35017c45e6a716faaacf176c49c652))

## [0.27.14](https://github.com/cube-js/cube.js/compare/v0.27.13...v0.27.14) (2021-05-13)

### Bug Fixes

- **schema-compiler:** Time-series query with minute/second granularity ([c4a6044](https://github.com/cube-js/cube.js/commit/c4a6044702df39629044802b0b5d9e1636cc99d0))

## [0.27.13](https://github.com/cube-js/cube.js/compare/v0.27.12...v0.27.13) (2021-05-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.27.11](https://github.com/cube-js/cube.js/compare/v0.27.10...v0.27.11) (2021-05-12)

### Bug Fixes

- **clickhouse-driver:** Support ungrouped query, fix [#2717](https://github.com/cube-js/cube.js/issues/2717) ([#2719](https://github.com/cube-js/cube.js/issues/2719)) ([82efc98](https://github.com/cube-js/cube.js/commit/82efc987c574960c4989b41b92776ba928a2f22b))

## [0.27.10](https://github.com/cube-js/cube.js/compare/v0.27.9...v0.27.10) (2021-05-11)

### Bug Fixes

- titleize case ([6acc100](https://github.com/cube-js/cube.js/commit/6acc100a6d50bf6970010843d0472e981399a602))

## [0.27.9](https://github.com/cube-js/cube.js/compare/v0.27.8...v0.27.9) (2021-05-11)

### Bug Fixes

- **@cubejs-backend/schema-compiler:** titleize fix ([#2695](https://github.com/cube-js/cube.js/issues/2695)) ([d997f49](https://github.com/cube-js/cube.js/commit/d997f4978de9f4b25b606ba6c17b225f3e6a6cb1))

## [0.27.7](https://github.com/cube-js/cube.js/compare/v0.27.6...v0.27.7) (2021-05-04)

### Bug Fixes

- TypeError: moment is not a function ([39662e4](https://github.com/cube-js/cube.js/commit/39662e468ed001a5b472a2c09fc9fa2d48af03d2))

## [0.27.5](https://github.com/cube-js/cube.js/compare/v0.27.4...v0.27.5) (2021-05-03)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.27.4](https://github.com/cube-js/cube.js/compare/v0.27.3...v0.27.4) (2021-04-29)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.27.2](https://github.com/cube-js/cube.js/compare/v0.27.1...v0.27.2) (2021-04-28)

### Bug Fixes

- Move Prettier & Jest to dev dep (reduce size) ([da59584](https://github.com/cube-js/cube.js/commit/da5958426b701b8a81506e8d74070b2977e3df56))

## [0.27.1](https://github.com/cube-js/cube.js/compare/v0.27.0...v0.27.1) (2021-04-27)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [0.27.0](https://github.com/cube-js/cube.js/compare/v0.26.104...v0.27.0) (2021-04-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.104](https://github.com/cube-js/cube.js/compare/v0.26.103...v0.26.104) (2021-04-26)

### Bug Fixes

- Original SQL table is not found when `useOriginalSqlPreAggregations` used together with `CUBE.sql()` reference ([#2603](https://github.com/cube-js/cube.js/issues/2603)) ([5fd8e42](https://github.com/cube-js/cube.js/commit/5fd8e42cbe361a66cf0ffe6542478b6beaad86c5))

## [0.26.103](https://github.com/cube-js/cube.js/compare/v0.26.102...v0.26.103) (2021-04-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.95](https://github.com/cube-js/cube.js/compare/v0.26.94...v0.26.95) (2021-04-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.87](https://github.com/cube-js/cube.js/compare/v0.26.86...v0.26.87) (2021-04-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.81](https://github.com/cube-js/cube.js/compare/v0.26.80...v0.26.81) (2021-04-07)

### Features

- Introduce databricks-jdbc-driver ([bb0b31f](https://github.com/cube-js/cube.js/commit/bb0b31fb333f2aa379f11f6733c4efc17ec12dde))

## [0.26.79](https://github.com/cube-js/cube.js/compare/v0.26.78...v0.26.79) (2021-04-06)

### Bug Fixes

- sqlAlias on non partitioned rollups ([0675925](https://github.com/cube-js/cube.js/commit/0675925efb61a6492344b28179b7647eabb01a1d))

## [0.26.74](https://github.com/cube-js/cube.js/compare/v0.26.73...v0.26.74) (2021-04-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.69](https://github.com/cube-js/cube.js/compare/v0.26.68...v0.26.69) (2021-03-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.67](https://github.com/cube-js/cube.js/compare/v0.26.66...v0.26.67) (2021-03-24)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.65](https://github.com/cube-js/cube.js/compare/v0.26.64...v0.26.65) (2021-03-24)

### Bug Fixes

- Allow using sub query dimensions in join conditions ([#2419](https://github.com/cube-js/cube.js/issues/2419)) ([496a075](https://github.com/cube-js/cube.js/commit/496a0755308f376666e732ea09a4c7816682cfb0))

## [0.26.56](https://github.com/cube-js/cube.js/compare/v0.26.55...v0.26.56) (2021-03-13)

### Bug Fixes

- Expected one parameter but nothing found ([#2362](https://github.com/cube-js/cube.js/issues/2362)) ([ce490d2](https://github.com/cube-js/cube.js/commit/ce490d2de60c200832966824e6b0300ba91cde41))

## [0.26.54](https://github.com/cube-js/cube.js/compare/v0.26.53...v0.26.54) (2021-03-12)

### Features

- Suggest to use rollUp & pre-agg for to join across data sources ([2cf1a63](https://github.com/cube-js/cube.js/commit/2cf1a630a9abaa7248526c284441e65212e82259))

## [0.26.49](https://github.com/cube-js/cube.js/compare/v0.26.48...v0.26.49) (2021-03-05)

### Features

- **elasticsearch-driver:** Support for elastic.co & improve docs ([#2240](https://github.com/cube-js/cube.js/issues/2240)) ([d8557f6](https://github.com/cube-js/cube.js/commit/d8557f6487ea98c19c055cc94b94b284dd273835))

## [0.26.45](https://github.com/cube-js/cube.js/compare/v0.26.44...v0.26.45) (2021-03-04)

### Bug Fixes

- **@cubejs-schema-compiler:** addInterval / subtractInterval for Mssql ([#2239](https://github.com/cube-js/cube.js/issues/2239)) Thanks to [@florian-fischer-swarm](https://github.com/florian-fischer-swarm)! ([0930e15](https://github.com/cube-js/cube.js/commit/0930e1526612b92db2d192e4444a2c2a1d2d15ce)), closes [#2237](https://github.com/cube-js/cube.js/issues/2237)
- **schema-compiler:** Lock antlr4ts to 0.5.0-alpha.4, fix [#2264](https://github.com/cube-js/cube.js/issues/2264) ([37b3a0d](https://github.com/cube-js/cube.js/commit/37b3a0d61433ae1b3e41c1264298d1409b7f95b7))

## [0.26.44](https://github.com/cube-js/cube.js/compare/v0.26.43...v0.26.44) (2021-03-02)

### Bug Fixes

- **schema-compiler:** @types/ramda is a dev dependecy, dont ship it ([0a87d11](https://github.com/cube-js/cube.js/commit/0a87d1152f454e0e4d9c30c3295ee975dd493d0d))

## [0.26.35](https://github.com/cube-js/cube.js/compare/v0.26.34...v0.26.35) (2021-02-25)

### Features

- Use Cube Store as default external storage for CUBEJS_DEV_MODE ([e526676](https://github.com/cube-js/cube.js/commit/e52667617e5e687c92d383045fb1a8d5fd19cab6))

## [0.26.25](https://github.com/cube-js/cube.js/compare/v0.26.24...v0.26.25) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.23](https://github.com/cube-js/cube.js/compare/v0.26.22...v0.26.23) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.22](https://github.com/cube-js/cube.js/compare/v0.26.21...v0.26.22) (2021-02-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.19](https://github.com/cube-js/cube.js/compare/v0.26.18...v0.26.19) (2021-02-19)

### Bug Fixes

- **@cubejs-schema-compilter:** MSSQL remove order by from subqueries ([75c1903](https://github.com/cube-js/cube.js/commit/75c19035e2732adfb7c4711197bba57245e9673e))

## [0.26.16](https://github.com/cube-js/cube.js/compare/v0.26.15...v0.26.16) (2021-02-18)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.15](https://github.com/cube-js/cube.js/compare/v0.26.14...v0.26.15) (2021-02-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.13](https://github.com/cube-js/cube.js/compare/v0.26.12...v0.26.13) (2021-02-12)

### Features

- **schema-compiler:** Generate parser by antlr4ts ([d8e68c7](https://github.com/cube-js/cube.js/commit/d8e68c77f4649ddc056322f2848c769e5311c6b1))
- **schema-compiler:** Wrap new generated parser. fix [#1798](https://github.com/cube-js/cube.js/issues/1798) ([c5fde21](https://github.com/cube-js/cube.js/commit/c5fde21cb4bbcd675a4eeb735cd0c48d7a3ade6d))
- Support for extra params in generating schema for tables. ([#1990](https://github.com/cube-js/cube.js/issues/1990)) ([a9b3df2](https://github.com/cube-js/cube.js/commit/a9b3df222f8eaca86724ed2e1c24c348b38f718c))

## [0.26.11](https://github.com/cube-js/cube.js/compare/v0.26.10...v0.26.11) (2021-02-10)

### Bug Fixes

- CUBEJS_SCHEDULED_REFRESH_TIMER, fix [#1972](https://github.com/cube-js/cube.js/issues/1972) ([#1975](https://github.com/cube-js/cube.js/issues/1975)) ([dac7e52](https://github.com/cube-js/cube.js/commit/dac7e52ee0d3a118c9d69c9d030e58a3c048cca1))

## [0.26.10](https://github.com/cube-js/cube.js/compare/v0.26.9...v0.26.10) (2021-02-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.26.6](https://github.com/cube-js/cube.js/compare/v0.26.5...v0.26.6) (2021-02-08)

### Bug Fixes

- **sqlite-driver:** Use workaround for FLOOR ([#1931](https://github.com/cube-js/cube.js/issues/1931)) ([fe64feb](https://github.com/cube-js/cube.js/commit/fe64febd1b970c4b8396d05a859f16b3d9e5a8a8))

# [0.26.0](https://github.com/cube-js/cube.js/compare/v0.25.33...v0.26.0) (2021-02-01)

### Features

- Storing userContext inside payload.u is deprecated, moved to root ([559bd87](https://github.com/cube-js/cube.js/commit/559bd8757d9754ab486eed88d1fdb0c280b82dc9))
- USER_CONTEXT -> SECURITY_CONTEXT, authInfo -> securityInfo ([fa5d17c](https://github.com/cube-js/cube.js/commit/fa5d17c0bb703b087f442c41a5bf0a3dca1c5faa))

## [0.25.33](https://github.com/cube-js/cube.js/compare/v0.25.32...v0.25.33) (2021-01-30)

### Bug Fixes

- Use local dates for pre-aggregations to avoid timezone shift discrepancies on DST timezones for timezone unaware databases like MySQL ([#1941](https://github.com/cube-js/cube.js/issues/1941)) ([f138e6f](https://github.com/cube-js/cube.js/commit/f138e6fa3d97492c34527d0f04917e78c374eb57))
- **schema-compiler:** Wrong dayOffset in refreshKey for not UTC computers ([#1938](https://github.com/cube-js/cube.js/issues/1938)) ([5fe3431](https://github.com/cube-js/cube.js/commit/5fe3431a8f7320555fc3dba101c72547a0f41dac))

## [0.25.23](https://github.com/cube-js/cube.js/compare/v0.25.22...v0.25.23) (2021-01-22)

### Features

- **schema-compiler:** Move some parts to TS ([2ad0e2e](https://github.com/cube-js/cube.js/commit/2ad0e2e377fce52f4967fc73ae2486d4365f3ac4))

## [0.25.21](https://github.com/cube-js/cube.js/compare/v0.25.20...v0.25.21) (2021-01-19)

### Features

- **schema-compiler:** Initial support for TS ([5926067](https://github.com/cube-js/cube.js/commit/5926067bf5314c7cbddfe59f26dd0ae3b8b60293))

## [0.25.19](https://github.com/cube-js/cube.js/compare/v0.25.18...v0.25.19) (2021-01-14)

### Bug Fixes

- Do not renew historical refresh keys during scheduled refresh ([e5fbb12](https://github.com/cube-js/cube.js/commit/e5fbb120d5e848468999de59ba536b95be2e67e9))

## [0.25.2](https://github.com/cube-js/cube.js/compare/v0.25.1...v0.25.2) (2020-12-27)

### Bug Fixes

- **@cubejs-backend/schema-compiler:** MySQL double timezone conversion ([e5f1490](https://github.com/cube-js/cube.js/commit/e5f1490a897df4f0eac062dfabbc20aca2ea2f5b))

## [0.25.1](https://github.com/cube-js/cube.js/compare/v0.25.0...v0.25.1) (2020-12-24)

### Bug Fixes

- **@cubejs-backend/schema-compiler:** Better error message for join member resolutions ([30cc3ab](https://github.com/cube-js/cube.js/commit/30cc3abc4e8c91e8d95b8794f892e1d1f2152798))
- **@cubejs-backend/schema-compiler:** Error: TypeError: R.eq is not a function -- existing joins in rollup support ([5f62aae](https://github.com/cube-js/cube.js/commit/5f62aaee88b7ecc281437601410b10ef04d7bbf3))

# [0.25.0](https://github.com/cube-js/cube.js/compare/v0.24.15...v0.25.0) (2020-12-21)

### Features

- Allow cross data source joins ([a58336e](https://github.com/cube-js/cube.js/commit/a58336e3840f8ac02d83de43ec7661419bceb71c))

## [0.24.15](https://github.com/cube-js/cube.js/compare/v0.24.14...v0.24.15) (2020-12-20)

### Features

- Allow joins between data sources for external queries ([1dbfe2c](https://github.com/cube-js/cube.js/commit/1dbfe2cdc1b1904ce8567a7599b24e660c5047f3))

## [0.24.14](https://github.com/cube-js/cube.js/compare/v0.24.13...v0.24.14) (2020-12-19)

### Bug Fixes

- Rollup match results for rollupJoin ([0279b13](https://github.com/cube-js/cube.js/commit/0279b13a8696643ad95c374062ea059cea3b890b))

## [0.24.13](https://github.com/cube-js/cube.js/compare/v0.24.12...v0.24.13) (2020-12-18)

### Features

- Rollup join implementation ([#1637](https://github.com/cube-js/cube.js/issues/1637)) ([bffd220](https://github.com/cube-js/cube.js/commit/bffd22095f58369f3d52474283951b4844657f2b))

## [0.24.8](https://github.com/cube-js/cube.js/compare/v0.24.7...v0.24.8) (2020-12-15)

### Bug Fixes

- **@cubejs-backend/schema-compiler:** CubeCheckDuplicatePropTranspiler - dont crash on not StringLiterals ([#1582](https://github.com/cube-js/cube.js/issues/1582)) ([a705a2e](https://github.com/cube-js/cube.js/commit/a705a2ed6885d5c08e654945682054a1421dfb51))

## [0.24.5](https://github.com/cube-js/cube.js/compare/v0.24.4...v0.24.5) (2020-12-09)

### Features

- **@cubejs-backend/mysql-driver:** CAST all time dimensions with granularities to DATETIME in order to provide typing for rollup downloads. Add mediumtext and mediumint generic type conversions. ([3d8cb37](https://github.com/cube-js/cube.js/commit/3d8cb37d03716cd2768a0986643495e4a844cb8d))

## [0.24.4](https://github.com/cube-js/cube.js/compare/v0.24.3...v0.24.4) (2020-12-07)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.24.1](https://github.com/cube-js/cube.js/compare/v0.24.0...v0.24.1) (2020-11-27)

### Bug Fixes

- Specifying `dateRange` in time dimension should produce same result as `inDateRange` in filter ([a7603d7](https://github.com/cube-js/cube.js/commit/a7603d724732a51301227f68c39ba699333c0e06)), closes [#962](https://github.com/cube-js/cube.js/issues/962)

# [0.24.0](https://github.com/cube-js/cube.js/compare/v0.23.15...v0.24.0) (2020-11-26)

### Bug Fixes

- Error: Type must be provided for null values. -- `null` parameter values are passed to BigQuery when used for dimensions that contain `?` ([6417e7d](https://github.com/cube-js/cube.js/commit/6417e7d120a95c4792557a4c4a0d6abb7c483db9))

### Features

- Make default refreshKey to be `every 10 seconds` and enable scheduled refresh in dev mode by default ([221003a](https://github.com/cube-js/cube.js/commit/221003aa73aa1ece3d649de9164a7379a4a690be))

### BREAKING CHANGES

- `every 10 seconds` refreshKey becomes a default refreshKey for all cubes.

## [0.23.15](https://github.com/cube-js/cube.js/compare/v0.23.14...v0.23.15) (2020-11-25)

### Bug Fixes

- Error: Cannot find module 'antlr4/index' ([0d2e330](https://github.com/cube-js/cube.js/commit/0d2e33040dfea3fb80df2a1af2ccff46db0f8673))

## [0.23.11](https://github.com/cube-js/cube.js/compare/v0.23.10...v0.23.11) (2020-11-13)

### Features

- **@cubejs-backend/mysql-aurora-serverless-driver:** Add a new driver to support AWS Aurora Serverless MySql ([#1333](https://github.com/cube-js/cube.js/issues/1333)) Thanks to [@kcwinner](https://github.com/kcwinner)! ([154fab1](https://github.com/cube-js/cube.js/commit/154fab1a222685e1e83d5187a4f00f745c4613a3))

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

- **@cubejs-backend/schema-compiler:** Dialect for 'undefined' is not found, fix [#1247](https://github.com/cube-js/cube.js/issues/1247) ([1069b47](https://github.com/cube-js/cube.js/commit/1069b47ff4f0a9d2e398ba194fe3eef5ad39f0d2))

## [0.22.2](https://github.com/cube-js/cube.js/compare/v0.22.1...v0.22.2) (2020-10-26)

### Bug Fixes

- Dialect class isn't looked up for external drivers ([b793f4a](https://github.com/cube-js/cube.js/commit/b793f4a))

# [0.22.0](https://github.com/cube-js/cube.js/compare/v0.21.2...v0.22.0) (2020-10-20)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.21.1](https://github.com/cube-js/cube.js/compare/v0.21.0...v0.21.1) (2020-10-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [0.21.0](https://github.com/cube-js/cube.js/compare/v0.20.15...v0.21.0) (2020-10-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.20.13](https://github.com/cube-js/cube.js/compare/v0.20.12...v0.20.13) (2020-10-07)

### Bug Fixes

- **@cubejs-schema-compilter:** MSSQL rollingWindow with granularity ([#1169](https://github.com/cube-js/cube.js/issues/1169)) Thanks to @JoshMentzer! ([16e6a9e](https://github.com/cube-js/cube.js/commit/16e6a9e))

## [0.20.11](https://github.com/cube-js/cube.js/compare/v0.20.10...v0.20.11) (2020-09-28)

### Bug Fixes

- **@cubejs-backend/prestodb-driver:** Wrong OFFSET/LIMIT order ([#1135](https://github.com/cube-js/cube.js/issues/1135)) ([3b94b2c](https://github.com/cube-js/cube.js/commit/3b94b2c)), closes [#988](https://github.com/cube-js/cube.js/issues/988) [#988](https://github.com/cube-js/cube.js/issues/988) [#988](https://github.com/cube-js/cube.js/issues/988)

### Features

- Introduce Druid driver ([#1099](https://github.com/cube-js/cube.js/issues/1099)) ([2bfe20f](https://github.com/cube-js/cube.js/commit/2bfe20f))

## [0.20.9](https://github.com/cube-js/cube.js/compare/v0.20.8...v0.20.9) (2020-09-19)

### Bug Fixes

- Allow empty complex boolean filter arrays ([#1100](https://github.com/cube-js/cube.js/issues/1100)) ([80d112e](https://github.com/cube-js/cube.js/commit/80d112e))

### Features

- `sqlAlias` attribute for `preAggregations` and short format for pre-aggregation table names ([#1068](https://github.com/cube-js/cube.js/issues/1068)) ([98ffad3](https://github.com/cube-js/cube.js/commit/98ffad3)), closes [#86](https://github.com/cube-js/cube.js/issues/86) [#907](https://github.com/cube-js/cube.js/issues/907)

## [0.20.8](https://github.com/cube-js/cube.js/compare/v0.20.7...v0.20.8) (2020-09-16)

### Bug Fixes

- **@cubejs-backend/elasticsearch-driver:** Respect `ungrouped` flag ([#1098](https://github.com/cube-js/cube.js/issues/1098)) Thanks to [@vignesh-123](https://github.com/vignesh-123)! ([995b8f9](https://github.com/cube-js/cube.js/commit/995b8f9))

### Features

- refreshKey every support for CRON format interval ([#1048](https://github.com/cube-js/cube.js/issues/1048)) ([3e55f5c](https://github.com/cube-js/cube.js/commit/3e55f5c))
- Strict cube schema parsing, show duplicate property name errors ([#1095](https://github.com/cube-js/cube.js/issues/1095)) ([d4ab530](https://github.com/cube-js/cube.js/commit/d4ab530))

## [0.20.7](https://github.com/cube-js/cube.js/compare/v0.20.6...v0.20.7) (2020-09-11)

### Bug Fixes

- member-dimension query normalization for queryTransformer and additional complex boolean logic tests ([#1047](https://github.com/cube-js/cube.js/issues/1047)) ([65ef327](https://github.com/cube-js/cube.js/commit/65ef327)), closes [#1007](https://github.com/cube-js/cube.js/issues/1007)

## [0.20.6](https://github.com/cube-js/cube.js/compare/v0.20.5...v0.20.6) (2020-09-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.20.5](https://github.com/cube-js/cube.js/compare/v0.20.4...v0.20.5) (2020-09-10)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.20.3](https://github.com/cube-js/cube.js/compare/v0.20.2...v0.20.3) (2020-09-03)

### Features

- Complex boolean logic ([#1038](https://github.com/cube-js/cube.js/issues/1038)) ([a5b44d1](https://github.com/cube-js/cube.js/commit/a5b44d1)), closes [#259](https://github.com/cube-js/cube.js/issues/259)

## [0.20.1](https://github.com/cube-js/cube.js/compare/v0.20.0...v0.20.1) (2020-09-01)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [0.20.0](https://github.com/cube-js/cube.js/compare/v0.19.61...v0.20.0) (2020-08-26)

### Bug Fixes

- **@cubejs-backend/athena-driver:** Error: Queries of this type are not supported for incremental refreshKey ([2d3018d](https://github.com/cube-js/cube.js/commit/2d3018d)), closes [#404](https://github.com/cube-js/cube.js/issues/404)
- Check partitionGranularity requires timeDimensionReference for `originalSql` ([2a2b256](https://github.com/cube-js/cube.js/commit/2a2b256))
- **@cubejs-backend/clickhouse-driver:** allow default compound indexes: add parentheses to the pre-aggregation sql definition ([#1009](https://github.com/cube-js/cube.js/issues/1009)) Thanks to [@gudjonragnar](https://github.com/gudjonragnar)! ([6535cb6](https://github.com/cube-js/cube.js/commit/6535cb6))
- TypeError: Cannot read property '1' of undefined -- Using scheduled cube refresh endpoint not working with Athena ([ed6c9aa](https://github.com/cube-js/cube.js/commit/ed6c9aa)), closes [#1000](https://github.com/cube-js/cube.js/issues/1000)

### Features

- Dremio driver ([#1008](https://github.com/cube-js/cube.js/issues/1008)) ([617225f](https://github.com/cube-js/cube.js/commit/617225f))

## [0.19.61](https://github.com/cube-js/cube.js/compare/v0.19.60...v0.19.61) (2020-08-11)

### Bug Fixes

- readOnly originalSql pre-aggregations aren't working without writing rights ([cfa7c7d](https://github.com/cube-js/cube.js/commit/cfa7c7d))

### Features

- **mssql-driver:** add readonly aggregation for mssql sources ([#920](https://github.com/cube-js/cube.js/issues/920)) Thanks to @JoshMentzer! ([dfeccca](https://github.com/cube-js/cube.js/commit/dfeccca))

## [0.19.57](https://github.com/cube-js/cube.js/compare/v0.19.56...v0.19.57) (2020-08-05)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.19.56](https://github.com/cube-js/cube.js/compare/v0.19.55...v0.19.56) (2020-08-03)

### Bug Fixes

- using limit and offset together in MSSql ([9ba875c](https://github.com/cube-js/cube.js/commit/9ba875c))
- Various ClickHouse improvements ([6f40847](https://github.com/cube-js/cube.js/commit/6f40847))

## [0.19.54](https://github.com/cube-js/cube.js/compare/v0.19.53...v0.19.54) (2020-07-23)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.19.50](https://github.com/cube-js/cube.js/compare/v0.19.49...v0.19.50) (2020-07-16)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.19.48](https://github.com/cube-js/cube.js/compare/v0.19.47...v0.19.48) (2020-07-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.19.43](https://github.com/cube-js/cube.js/compare/v0.19.42...v0.19.43) (2020-07-04)

### Features

- Pluggable dialects support ([f786fdd](https://github.com/cube-js/cube.js/commit/f786fdd)), closes [#590](https://github.com/cube-js/cube.js/issues/590)

## [0.19.40](https://github.com/cube-js/cube.js/compare/v0.19.39...v0.19.40) (2020-06-30)

### Bug Fixes

- Querying empty Postgres table with 'time' dimension in a cube results in null value ([07d00f8](https://github.com/cube-js/cube.js/commit/07d00f8)), closes [#639](https://github.com/cube-js/cube.js/issues/639)

## [0.19.39](https://github.com/cube-js/cube.js/compare/v0.19.38...v0.19.39) (2020-06-28)

### Bug Fixes

- treat wildcard Elasticsearch select as simple asterisk select: include \* as part of RE to support elasticsearch indexes ([#760](https://github.com/cube-js/cube.js/issues/760)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar) ! ([099a888](https://github.com/cube-js/cube.js/commit/099a888))

### Features

- `refreshRangeStart` and `refreshRangeEnd` pre-aggregation params ([e4d2874](https://github.com/cube-js/cube.js/commit/e4d2874))

## [0.19.38](https://github.com/cube-js/cube.js/compare/v0.19.37...v0.19.38) (2020-06-28)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.19.35](https://github.com/cube-js/cube.js/compare/v0.19.34...v0.19.35) (2020-06-22)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.19.29](https://github.com/cube-js/cube.js/compare/v0.19.28...v0.19.29) (2020-06-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.19.24](https://github.com/cube-js/cube.js/compare/v0.19.23...v0.19.24) (2020-06-06)

### Bug Fixes

- **@cubejs-backend/elasticsearch-driver:** respect ungrouped parameter ([#684](https://github.com/cube-js/cube.js/issues/684)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar)! ([27d0d49](https://github.com/cube-js/cube.js/commit/27d0d49))
- **@cubejs-backend/schema-compiler:** TypeError: methods.filter is not a function ([25c4ef6](https://github.com/cube-js/cube.js/commit/25c4ef6))

## [0.19.23](https://github.com/cube-js/cube.js/compare/v0.19.22...v0.19.23) (2020-06-02)

### Features

- drill down queries support ([#664](https://github.com/cube-js/cube.js/issues/664)) ([7e21545](https://github.com/cube-js/cube.js/commit/7e21545)), closes [#190](https://github.com/cube-js/cube.js/issues/190)

## [0.19.19](https://github.com/cube-js/cube.js/compare/v0.19.18...v0.19.19) (2020-05-15)

### Features

- ability to add custom meta data for measures, dimensions and segments ([#641](https://github.com/cube-js/cube.js/issues/641)) ([88d5c9b](https://github.com/cube-js/cube.js/commit/88d5c9b)), closes [#625](https://github.com/cube-js/cube.js/issues/625)

## [0.19.18](https://github.com/cube-js/cube.js/compare/v0.19.17...v0.19.18) (2020-05-11)

### Bug Fixes

- Offset doesn't affect actual queries ([1feaa38](https://github.com/cube-js/cube.js/commit/1feaa38)), closes [#636](https://github.com/cube-js/cube.js/issues/636)

## [0.19.14](https://github.com/cube-js/cube.js/compare/v0.19.13...v0.19.14) (2020-04-24)

### Features

- Postgres HLL improvements: always round to int ([#611](https://github.com/cube-js/cube.js/issues/611)) Thanks to [@milanbella](https://github.com/milanbella)! ([680a613](https://github.com/cube-js/cube.js/commit/680a613))

## [0.19.13](https://github.com/cube-js/cube.js/compare/v0.19.12...v0.19.13) (2020-04-21)

### Features

- Postgres Citus Data HLL plugin implementation ([#601](https://github.com/cube-js/cube.js/issues/601)) Thanks to [@milanbella](https://github.com/milanbella) ! ([be85ac6](https://github.com/cube-js/cube.js/commit/be85ac6)), closes [#563](https://github.com/cube-js/cube.js/issues/563)

## [0.19.11](https://github.com/cube-js/cube.js/compare/v0.19.10...v0.19.11) (2020-04-20)

### Bug Fixes

- Strict date range and rollup granularity alignment check ([deb62b6](https://github.com/cube-js/cube.js/commit/deb62b6)), closes [#103](https://github.com/cube-js/cube.js/issues/103)

## [0.19.10](https://github.com/cube-js/cube.js/compare/v0.19.9...v0.19.10) (2020-04-18)

### Bug Fixes

- Recursive pre-aggregation description generation: support propagateFiltersToSubQuery with partitioned originalSql ([6a2b9dd](https://github.com/cube-js/cube.js/commit/6a2b9dd))

## [0.19.1](https://github.com/cube-js/cube.js/compare/v0.19.0...v0.19.1) (2020-04-11)

### Bug Fixes

- TypeError: Cannot read property 'path' of undefined -- Case when partitioned originalSql is resolved for query without time dimension and incremental refreshKey is used ([ca0f1f6](https://github.com/cube-js/cube.js/commit/ca0f1f6))

### Features

- Renamed OpenDistro to AWSElasticSearch. Added `elasticsearch` dialect ([#577](https://github.com/cube-js/cube.js/issues/577)) Thanks to [@chad-codeworkshop](https://github.com/chad-codeworkshop)! ([a4e41cb](https://github.com/cube-js/cube.js/commit/a4e41cb))

# [0.19.0](https://github.com/cube-js/cube.js/compare/v0.18.32...v0.19.0) (2020-04-09)

### Features

- Multi-level query structures in-memory caching ([38aa32d](https://github.com/cube-js/cube.js/commit/38aa32d))

## [0.18.31](https://github.com/cube-js/cube.js/compare/v0.18.30...v0.18.31) (2020-04-07)

### Bug Fixes

- Rewrite converts left outer to inner join due to filtering in where: ensure `OR` is supported ([93a1250](https://github.com/cube-js/cube.js/commit/93a1250))

## [0.18.30](https://github.com/cube-js/cube.js/compare/v0.18.29...v0.18.30) (2020-04-04)

### Bug Fixes

- Rewrite converts left outer to inner join due to filtering in where ([2034d37](https://github.com/cube-js/cube.js/commit/2034d37))

### Features

- Native X-Pack SQL ElasticSearch Driver ([#551](https://github.com/cube-js/cube.js/issues/551)) ([efde731](https://github.com/cube-js/cube.js/commit/efde731))

## [0.18.29](https://github.com/cube-js/cube.js/compare/v0.18.28...v0.18.29) (2020-04-04)

### Features

- Hour partition granularity support ([5f78974](https://github.com/cube-js/cube.js/commit/5f78974))
- Rewrite SQL for more places ([2412821](https://github.com/cube-js/cube.js/commit/2412821))

## [0.18.28](https://github.com/cube-js/cube.js/compare/v0.18.27...v0.18.28) (2020-04-03)

### Bug Fixes

- TypeError: date.match is not a function at BaseTimeDimension.formatFromDate ([7379b84](https://github.com/cube-js/cube.js/commit/7379b84))

## [0.18.26](https://github.com/cube-js/cube.js/compare/v0.18.25...v0.18.26) (2020-04-03)

### Bug Fixes

- `AND 1 = 1` case ([cd189d5](https://github.com/cube-js/cube.js/commit/cd189d5))

## [0.18.25](https://github.com/cube-js/cube.js/compare/v0.18.24...v0.18.25) (2020-04-02)

### Bug Fixes

- TypeError: Cannot read property \'replace\' of null for `scheduledRefresh: true` ([28e45c0](https://github.com/cube-js/cube.js/commit/28e45c0)), closes [#558](https://github.com/cube-js/cube.js/issues/558)

### Features

- Basic query rewrites ([af07865](https://github.com/cube-js/cube.js/commit/af07865))

## [0.18.24](https://github.com/cube-js/cube.js/compare/v0.18.23...v0.18.24) (2020-04-01)

### Bug Fixes

- TypeError: Cannot read property \'replace\' of null for `scheduledRefresh: true` ([ea88edf](https://github.com/cube-js/cube.js/commit/ea88edf))

## [0.18.23](https://github.com/cube-js/cube.js/compare/v0.18.22...v0.18.23) (2020-03-30)

### Bug Fixes

- Cannot read property 'timeDimensions' of null -- originalSql scheduledRefresh support ([e7667a5](https://github.com/cube-js/cube.js/commit/e7667a5))
- minute requests incorrectly snapped to daily partitions ([8fd7876](https://github.com/cube-js/cube.js/commit/8fd7876))

## [0.18.19](https://github.com/cube-js/cube.js/compare/v0.18.18...v0.18.19) (2020-03-29)

### Bug Fixes

- Empty default `originalSql` refreshKey ([dd8536b](https://github.com/cube-js/cube.js/commit/dd8536b))
- incorrect WHERE for refreshKey every ([bf8b648](https://github.com/cube-js/cube.js/commit/bf8b648))
- Return single table for one partition queries ([54083ef](https://github.com/cube-js/cube.js/commit/54083ef))

### Features

- `propagateFiltersToSubQuery` flag ([6b253c0](https://github.com/cube-js/cube.js/commit/6b253c0))
- Partitioned `originalSql` support ([133857e](https://github.com/cube-js/cube.js/commit/133857e))

## [0.18.17](https://github.com/cube-js/cube.js/compare/v0.18.16...v0.18.17) (2020-03-24)

### Bug Fixes

- Unknown function NOW for Snowflake -- Incorrect now timestamp implementation ([036f68a](https://github.com/cube-js/cube.js/commit/036f68a)), closes [#537](https://github.com/cube-js/cube.js/issues/537)

## [0.18.16](https://github.com/cube-js/cube.js/compare/v0.18.15...v0.18.16) (2020-03-24)

### Features

- Log canUseTransformedQuery ([5b2ab90](https://github.com/cube-js/cube.js/commit/5b2ab90))

## [0.18.14](https://github.com/cube-js/cube.js/compare/v0.18.13...v0.18.14) (2020-03-24)

### Bug Fixes

- MySQL segment references support ([be42298](https://github.com/cube-js/cube.js/commit/be42298))

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

- Handle multiple occurrences in the first event of a funnel: conversion percent discrepancies. ([0989482](https://github.com/cube-js/cube.js/commit/0989482))

### Features

- COMPILE_CONTEXT and async driverFactory support ([160f931](https://github.com/cube-js/cube.js/commit/160f931))

## [0.17.10](https://github.com/cube-js/cube.js/compare/v0.17.9...v0.17.10) (2020-02-20)

### Features

- Support external rollups from readonly source ([#395](https://github.com/cube-js/cube.js/issues/395)) ([b17e841](https://github.com/cube-js/cube.js/commit/b17e841))

## [0.17.9](https://github.com/cube-js/cube.js/compare/v0.17.8...v0.17.9) (2020-02-18)

### Features

- Extend meta response with aggregation type ([#394](https://github.com/cube-js/cube.js/issues/394)) Thanks to [@pyrooka](https://github.com/pyrooka)! ([06eed0b](https://github.com/cube-js/cube.js/commit/06eed0b))

## [0.17.8](https://github.com/cube-js/cube.js/compare/v0.17.7...v0.17.8) (2020-02-14)

### Bug Fixes

- Wrong interval functions for BigQuery ([#367](https://github.com/cube-js/cube.js/issues/367)) Thanks to [@lvauvillier](https://github.com/lvauvillier)! ([0e09d4d](https://github.com/cube-js/cube.js/commit/0e09d4d))

### Features

- Athena HLL support ([45c7b83](https://github.com/cube-js/cube.js/commit/45c7b83))

## [0.17.7](https://github.com/cube-js/cube.js/compare/v0.17.6...v0.17.7) (2020-02-12)

### Bug Fixes

- Invalid Date: Incorrect MySQL minutes granularity ([dc553b9](https://github.com/cube-js/cube.js/commit/dc553b9))

## [0.17.6](https://github.com/cube-js/cube.js/compare/v0.17.5...v0.17.6) (2020-02-10)

### Bug Fixes

- `sqlAlias` isn't used for pre-aggregation table names ([b757175](https://github.com/cube-js/cube.js/commit/b757175))
- Multiplied measures rollup select case and leaf measure additive exact match ([c897dec](https://github.com/cube-js/cube.js/commit/c897dec))

## [0.17.3](https://github.com/cube-js/cube.js/compare/v0.17.2...v0.17.3) (2020-02-06)

### Features

- Pre-aggregation indexes support ([d443585](https://github.com/cube-js/cube.js/commit/d443585))

## [0.17.2](https://github.com/cube-js/cube.js/compare/v0.17.1...v0.17.2) (2020-02-04)

### Bug Fixes

- Funnel step names cannot contain spaces ([aff1891](https://github.com/cube-js/cube.js/commit/aff1891)), closes [#359](https://github.com/cube-js/cube.js/issues/359)

# [0.17.0](https://github.com/cube-js/cube.js/compare/v0.16.0...v0.17.0) (2020-02-04)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [0.16.0](https://github.com/cube-js/cube.js/compare/v0.15.4...v0.16.0) (2020-02-04)

### Features

- Allow `null` filter values ([9e339f7](https://github.com/cube-js/cube.js/commit/9e339f7)), closes [#362](https://github.com/cube-js/cube.js/issues/362)

## [0.15.1](https://github.com/cube-js/cube.js/compare/v0.15.0...v0.15.1) (2020-01-21)

### Features

- `updateWindow` property for incremental partitioned rollup refreshKey ([09c0a86](https://github.com/cube-js/cube.js/commit/09c0a86))

# [0.15.0](https://github.com/cube-js/cube.js/compare/v0.14.3...v0.15.0) (2020-01-18)

### Bug Fixes

- "Illegal input character" when using originalSql pre-aggregation with BigQuery and USER_CONTEXT ([904cf17](https://github.com/cube-js/cube.js/commit/904cf17)), closes [#197](https://github.com/cube-js/cube.js/issues/197)

### Features

- `dynRef` for dynamic member referencing ([41b644c](https://github.com/cube-js/cube.js/commit/41b644c))
- New refreshKeyRenewalThresholds and foreground renew defaults ([9fb0abb](https://github.com/cube-js/cube.js/commit/9fb0abb))
- Slow Query Warning and scheduled refresh for cube refresh keys ([8768b0e](https://github.com/cube-js/cube.js/commit/8768b0e))

## [0.14.3](https://github.com/cube-js/cube.js/compare/v0.14.2...v0.14.3) (2020-01-17)

### Bug Fixes

- originalSql pre-aggregations with FILTER_PARAMS params mismatch ([f4ee7b6](https://github.com/cube-js/cube.js/commit/f4ee7b6))

### Features

- RefreshKeys helper extension of popular implementations ([f2000c0](https://github.com/cube-js/cube.js/commit/f2000c0))

## [0.14.2](https://github.com/cube-js/cube.js/compare/v0.14.1...v0.14.2) (2020-01-17)

### Bug Fixes

- TypeError: Cannot read property 'evaluateSymbolSqlWithContext' of undefined ([125afd7](https://github.com/cube-js/cube.js/commit/125afd7))

## [0.14.1](https://github.com/cube-js/cube.js/compare/v0.14.0...v0.14.1) (2020-01-17)

### Features

- Default refreshKey implementations for mutable and immutable pre-aggregations. ([bef0626](https://github.com/cube-js/cube.js/commit/bef0626))

# [0.14.0](https://github.com/cube-js/cube.js/compare/v0.13.12...v0.14.0) (2020-01-16)

### Bug Fixes

- Time dimension can't be selected twice within same query with and without granularity ([aa65129](https://github.com/cube-js/cube.js/commit/aa65129))

### Features

- Scheduled refresh for pre-aggregations ([c87b525](https://github.com/cube-js/cube.js/commit/c87b525))

## [0.13.7](https://github.com/cube-js/cube.js/compare/v0.13.6...v0.13.7) (2019-12-31)

### Bug Fixes

- ER_TRUNCATED_WRONG_VALUE: Truncated incorrect datetime value ([fcbbe84](https://github.com/cube-js/cube.js/commit/fcbbe84)), closes [#309](https://github.com/cube-js/cube.js/issues/309)
- **client-core:** Uncaught TypeError: cubejs is not a function ([b5c32cd](https://github.com/cube-js/cube.js/commit/b5c32cd))

## [0.13.5](https://github.com/cube-js/cube.js/compare/v0.13.4...v0.13.5) (2019-12-17)

### Features

- Elasticsearch driver preview ([d6a6a07](https://github.com/cube-js/cube.js/commit/d6a6a07))

## [0.13.3](https://github.com/cube-js/cube.js/compare/v0.13.2...v0.13.3) (2019-12-16)

### Features

- **sqlite-driver:** Pre-aggregations support ([5ffb3d2](https://github.com/cube-js/cube.js/commit/5ffb3d2))

# [0.13.0](https://github.com/cube-js/cube.js/compare/v0.12.3...v0.13.0) (2019-12-10)

### Bug Fixes

- cube validation from updating BasePreAggregation ([#285](https://github.com/cube-js/cube.js/issues/285)). Thanks to [@ferrants](https://github.com/ferrants)! ([f4bda4e](https://github.com/cube-js/cube.js/commit/f4bda4e))

### Features

- Minute and second granularities support ([34c5d4c](https://github.com/cube-js/cube.js/commit/34c5d4c))
- Sqlite driver implementation ([f9b43d3](https://github.com/cube-js/cube.js/commit/f9b43d3))

## [0.12.1](https://github.com/cube-js/cube.js/compare/v0.12.0...v0.12.1) (2019-11-26)

### Features

- Show used pre-aggregations and match rollup results in Playground ([4a67346](https://github.com/cube-js/cube.js/commit/4a67346))

# [0.12.0](https://github.com/cube-js/cube.js/compare/v0.11.25...v0.12.0) (2019-11-25)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.11.21](https://github.com/cube-js/cube.js/compare/v0.11.20...v0.11.21) (2019-11-20)

### Features

- **schema-compiler:** Upgrade babel and support `objectRestSpread` for schema generation ([ac97c44](https://github.com/cube-js/cube.js/commit/ac97c44))

## [0.11.20](https://github.com/cube-js/cube.js/compare/v0.11.19...v0.11.20) (2019-11-18)

### Features

- support for pre-aggregation time hierarchies ([#258](https://github.com/cube-js/cube.js/issues/258)) Thanks to @Justin-ZS! ([ea78c84](https://github.com/cube-js/cube.js/commit/ea78c84)), closes [#246](https://github.com/cube-js/cube.js/issues/246)
- per cube `dataSource` support ([6dc3fef](https://github.com/cube-js/cube.js/commit/6dc3fef))

## [0.11.19](https://github.com/cube-js/cube.js/compare/v0.11.18...v0.11.19) (2019-11-16)

### Bug Fixes

- Merge back `sqlAlias` support ([80b312f](https://github.com/cube-js/cube.js/commit/80b312f))

## [0.11.18](https://github.com/cube-js/cube.js/compare/v0.11.17...v0.11.18) (2019-11-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.11.16](https://github.com/statsbotco/cubejs-client/compare/v0.11.15...v0.11.16) (2019-11-04)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.11.11](https://github.com/statsbotco/cubejs-client/compare/v0.11.10...v0.11.11) (2019-10-26)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.11.7](https://github.com/statsbotco/cubejs-client/compare/v0.11.6...v0.11.7) (2019-10-22)

### Features

- dynamic case label ([#236](https://github.com/statsbotco/cubejs-client/issues/236)) ([1a82605](https://github.com/statsbotco/cubejs-client/commit/1a82605)), closes [#235](https://github.com/statsbotco/cubejs-client/issues/235)

# [0.11.0](https://github.com/statsbotco/cubejs-client/compare/v0.10.62...v0.11.0) (2019-10-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.10.62](https://github.com/statsbotco/cubejs-client/compare/v0.10.61...v0.10.62) (2019-10-11)

### Features

- `ungrouped` queries support ([c6ac873](https://github.com/statsbotco/cubejs-client/commit/c6ac873))

## [0.10.61](https://github.com/statsbotco/cubejs-client/compare/v0.10.60...v0.10.61) (2019-10-10)

### Features

- **schema-compiler:** Allow access raw data in `USER_CONTEXT` using `unsafeValue()` method ([52ef146](https://github.com/statsbotco/cubejs-client/commit/52ef146))

## [0.10.59](https://github.com/statsbotco/cubejs-client/compare/v0.10.58...v0.10.59) (2019-10-08)

### Bug Fixes

- Rolling window returns dates in incorrect time zone for Postgres ([71a3baa](https://github.com/statsbotco/cubejs-client/commit/71a3baa)), closes [#216](https://github.com/statsbotco/cubejs-client/issues/216)

## [0.10.41](https://github.com/statsbotco/cubejs-client/compare/v0.10.40...v0.10.41) (2019-09-13)

### Features

- Provide date filter with hourly granularity ([e423d82](https://github.com/statsbotco/cubejs-client/commit/e423d82)), closes [#179](https://github.com/statsbotco/cubejs-client/issues/179)

## [0.10.39](https://github.com/statsbotco/cubejs-client/compare/v0.10.38...v0.10.39) (2019-09-09)

### Bug Fixes

- Requiring local node files is restricted: adding test for relative path resolvers ([f328d07](https://github.com/statsbotco/cubejs-client/commit/f328d07))

## [0.10.38](https://github.com/statsbotco/cubejs-client/compare/v0.10.37...v0.10.38) (2019-09-09)

### Bug Fixes

- Requiring local node files is restricted ([ba3c390](https://github.com/statsbotco/cubejs-client/commit/ba3c390))

## [0.10.36](https://github.com/statsbotco/cubejs-client/compare/v0.10.35...v0.10.36) (2019-09-09)

### Bug Fixes

- all queries forwarded to external DB instead of original one for zero pre-aggregation query ([2c230f4](https://github.com/statsbotco/cubejs-client/commit/2c230f4))

## [0.10.35](https://github.com/statsbotco/cubejs-client/compare/v0.10.34...v0.10.35) (2019-09-09)

### Features

- `originalSql` external pre-aggregations support ([0db2282](https://github.com/statsbotco/cubejs-client/commit/0db2282))

## [0.10.30](https://github.com/statsbotco/cubejs-client/compare/v0.10.29...v0.10.30) (2019-08-26)

### Bug Fixes

- Athena doesn't support `_` in contains filter ([d330be4](https://github.com/statsbotco/cubejs-client/commit/d330be4))

## [0.10.29](https://github.com/statsbotco/cubejs-client/compare/v0.10.28...v0.10.29) (2019-08-21)

### Bug Fixes

- MS SQL segment pre-aggregations support ([f8e37bf](https://github.com/statsbotco/cubejs-client/commit/f8e37bf)), closes [#186](https://github.com/statsbotco/cubejs-client/issues/186)

## [0.10.24](https://github.com/statsbotco/cubejs-client/compare/v0.10.23...v0.10.24) (2019-08-16)

### Bug Fixes

- MS SQL has unusual CTAS syntax ([1a00e4a](https://github.com/statsbotco/cubejs-client/commit/1a00e4a)), closes [#185](https://github.com/statsbotco/cubejs-client/issues/185)

## [0.10.23](https://github.com/statsbotco/cubejs-client/compare/v0.10.22...v0.10.23) (2019-08-14)

### Bug Fixes

- Unexpected string literal Bigquery ([8768895](https://github.com/statsbotco/cubejs-client/commit/8768895)), closes [#182](https://github.com/statsbotco/cubejs-client/issues/182)

## [0.10.21](https://github.com/statsbotco/cubejs-client/compare/v0.10.20...v0.10.21) (2019-08-05)

### Features

- Offset pagination support ([7fb1715](https://github.com/statsbotco/cubejs-client/commit/7fb1715)), closes [#117](https://github.com/statsbotco/cubejs-client/issues/117)

## [0.10.18](https://github.com/statsbotco/cubejs-client/compare/v0.10.17...v0.10.18) (2019-07-31)

### Bug Fixes

- BigQuery external rollup compatibility: use `__` separator for member aliases. Fix missed override. ([c1eb113](https://github.com/statsbotco/cubejs-client/commit/c1eb113))

## [0.10.17](https://github.com/statsbotco/cubejs-client/compare/v0.10.16...v0.10.17) (2019-07-31)

### Bug Fixes

- BigQuery external rollup compatibility: use `__` separator for member aliases. Fix all tests. ([723359c](https://github.com/statsbotco/cubejs-client/commit/723359c))
- Moved joi dependency to it's new availability ([#171](https://github.com/statsbotco/cubejs-client/issues/171)) ([1c20838](https://github.com/statsbotco/cubejs-client/commit/1c20838))

## [0.10.16](https://github.com/statsbotco/cubejs-client/compare/v0.10.15...v0.10.16) (2019-07-20)

### Bug Fixes

- Added correct string concat for Mysql. ([#162](https://github.com/statsbotco/cubejs-client/issues/162)) ([287411b](https://github.com/statsbotco/cubejs-client/commit/287411b))
- remove redundant hacks: primaryKey filter for method dimensionColumns ([#161](https://github.com/statsbotco/cubejs-client/issues/161)) ([f910a56](https://github.com/statsbotco/cubejs-client/commit/f910a56))

### Features

- BigQuery external rollup support ([10c635c](https://github.com/statsbotco/cubejs-client/commit/10c635c))

## [0.10.15](https://github.com/statsbotco/cubejs-client/compare/v0.10.14...v0.10.15) (2019-07-13)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.10.14](https://github.com/statsbotco/cubejs-client/compare/v0.10.13...v0.10.14) (2019-07-13)

### Features

- Oracle driver ([#160](https://github.com/statsbotco/cubejs-client/issues/160)) ([854ebff](https://github.com/statsbotco/cubejs-client/commit/854ebff))

## [0.10.9](https://github.com/statsbotco/cubejs-client/compare/v0.10.8...v0.10.9) (2019-06-30)

### Bug Fixes

- Syntax error during parsing: Unexpected token, expected: escaping back ticks ([9638a1a](https://github.com/statsbotco/cubejs-client/commit/9638a1a))

## [0.10.4](https://github.com/statsbotco/cubejs-client/compare/v0.10.3...v0.10.4) (2019-06-26)

### Features

- More descriptive error for SyntaxError ([f6d12d3](https://github.com/statsbotco/cubejs-client/commit/f6d12d3))

# [0.10.0](https://github.com/statsbotco/cubejs-client/compare/v0.9.24...v0.10.0) (2019-06-21)

### Features

- **schema-compiler:** `asyncModules` and Node.js `require()`: support loading cube definitions from DB and other async sources ([397cceb](https://github.com/statsbotco/cubejs-client/commit/397cceb)), closes [#141](https://github.com/statsbotco/cubejs-client/issues/141)

## [0.9.23](https://github.com/statsbotco/cubejs-client/compare/v0.9.22...v0.9.23) (2019-06-17)

### Bug Fixes

- **hive:** Fix count when id is not defined ([5a5fffd](https://github.com/statsbotco/cubejs-client/commit/5a5fffd))

## [0.9.21](https://github.com/statsbotco/cubejs-client/compare/v0.9.20...v0.9.21) (2019-06-16)

### Features

- Hive dialect for simple queries ([30d4a30](https://github.com/statsbotco/cubejs-client/commit/30d4a30))

## [0.9.19](https://github.com/statsbotco/cubejs-client/compare/v0.9.18...v0.9.19) (2019-06-13)

### Bug Fixes

- Handle rollingWindow queries without dateRange: TypeError: Cannot read property '0' of undefined at BaseTimeDimension.dateFromFormatted ([409a238](https://github.com/statsbotco/cubejs-client/commit/409a238))
- More descriptive SyntaxError messages ([acd17ad](https://github.com/statsbotco/cubejs-client/commit/acd17ad))

## [0.9.17](https://github.com/statsbotco/cubejs-client/compare/v0.9.16...v0.9.17) (2019-06-11)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.9.16](https://github.com/statsbotco/cubejs-client/compare/v0.9.15...v0.9.16) (2019-06-10)

### Bug Fixes

- force escape cubeAlias to work with restricted column names such as "case" ([#128](https://github.com/statsbotco/cubejs-client/issues/128)) ([b8a59da](https://github.com/statsbotco/cubejs-client/commit/b8a59da))

## [0.9.15](https://github.com/statsbotco/cubejs-client/compare/v0.9.14...v0.9.15) (2019-06-07)

### Bug Fixes

- **schema-compiler:** subquery in FROM must have an alias -- fix Redshift rollingWindow ([70b752f](https://github.com/statsbotco/cubejs-client/commit/70b752f))

## [0.9.13](https://github.com/statsbotco/cubejs-client/compare/v0.9.12...v0.9.13) (2019-06-06)

### Bug Fixes

- Schema generation with joins having case sensitive table and column names ([#124](https://github.com/statsbotco/cubejs-client/issues/124)) ([c7b706a](https://github.com/statsbotco/cubejs-client/commit/c7b706a)), closes [#120](https://github.com/statsbotco/cubejs-client/issues/120) [#120](https://github.com/statsbotco/cubejs-client/issues/120)

## [0.9.12](https://github.com/statsbotco/cubejs-client/compare/v0.9.11...v0.9.12) (2019-06-03)

### Bug Fixes

- **schema-compiler:** cast parameters for IN filters ([28f3e48](https://github.com/statsbotco/cubejs-client/commit/28f3e48)), closes [#119](https://github.com/statsbotco/cubejs-client/issues/119)

## [0.9.11](https://github.com/statsbotco/cubejs-client/compare/v0.9.10...v0.9.11) (2019-05-31)

### Bug Fixes

- **schema-compiler:** TypeError: Cannot read property 'filterToWhere' of undefined ([6b399ea](https://github.com/statsbotco/cubejs-client/commit/6b399ea))

## [0.9.6](https://github.com/statsbotco/cubejs-client/compare/v0.9.5...v0.9.6) (2019-05-24)

### Bug Fixes

- contains filter does not work with MS SQL Server database ([35210f6](https://github.com/statsbotco/cubejs-client/commit/35210f6)), closes [#113](https://github.com/statsbotco/cubejs-client/issues/113)

# [0.9.0](https://github.com/statsbotco/cubejs-client/compare/v0.8.7...v0.9.0) (2019-05-11)

### Features

- External rollup implementation ([d22a809](https://github.com/statsbotco/cubejs-client/commit/d22a809))

## [0.8.4](https://github.com/statsbotco/cubejs-client/compare/v0.8.3...v0.8.4) (2019-05-02)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.8.3](https://github.com/statsbotco/cubejs-client/compare/v0.8.2...v0.8.3) (2019-05-01)

### Features

- clickhouse dialect implementation ([#98](https://github.com/statsbotco/cubejs-client/issues/98)) ([7236e29](https://github.com/statsbotco/cubejs-client/commit/7236e29)), closes [#93](https://github.com/statsbotco/cubejs-client/issues/93)

## [0.8.1](https://github.com/statsbotco/cubejs-client/compare/v0.8.0...v0.8.1) (2019-04-30)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [0.8.0](https://github.com/statsbotco/cubejs-client/compare/v0.7.10...v0.8.0) (2019-04-29)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

## [0.7.9](https://github.com/statsbotco/cubejs-client/compare/v0.7.8...v0.7.9) (2019-04-24)

### Features

- **schema-compiler:** Allow to pass functions to USER_CONTEXT ([b489090](https://github.com/statsbotco/cubejs-client/commit/b489090)), closes [#88](https://github.com/statsbotco/cubejs-client/issues/88)

## [0.7.6](https://github.com/statsbotco/cubejs-client/compare/v0.7.5...v0.7.6) (2019-04-23)

### Features

- **schema-compiler:** Athena rollingWindow support ([f112c69](https://github.com/statsbotco/cubejs-client/commit/f112c69))

## [0.7.5](https://github.com/statsbotco/cubejs-client/compare/v0.7.4...v0.7.5) (2019-04-18)

### Bug Fixes

- **schema-compiler:** Athena, Mysql and BigQuery doesn't respect multiple contains filter ([0a8f324](https://github.com/statsbotco/cubejs-client/commit/0a8f324))

# [0.7.0](https://github.com/statsbotco/cubejs-client/compare/v0.6.2...v0.7.0) (2019-04-15)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [0.6.0](https://github.com/statsbotco/cubejs-client/compare/v0.5.2...v0.6.0) (2019-04-09)

**Note:** Version bump only for package @cubejs-backend/schema-compiler

# [0.5.0](https://github.com/statsbotco/cubejs-client/compare/v0.4.6...v0.5.0) (2019-04-01)

### Bug Fixes

- **schema-compiler:** joi@10.6.0 upgrade to joi@14.3.1 ([#59](https://github.com/statsbotco/cubejs-client/issues/59)) ([f035531](https://github.com/statsbotco/cubejs-client/commit/f035531))
- mongobi issue with parsing schema file with nested fields ([eaf1631](https://github.com/statsbotco/cubejs-client/commit/eaf1631)), closes [#55](https://github.com/statsbotco/cubejs-client/issues/55)

## [0.4.4](https://github.com/statsbotco/cubejs-client/compare/v0.4.3...v0.4.4) (2019-03-17)

### Bug Fixes

- Postgres doesn't show any data for queries with time dimension. ([e95e6fe](https://github.com/statsbotco/cubejs-client/commit/e95e6fe))

## [0.4.3](https://github.com/statsbotco/cubejs-client/compare/v0.4.2...v0.4.3) (2019-03-15)

### Bug Fixes

- **mongobi-driver:** implement `convert_tz` as a simple hour shift ([c97e451](https://github.com/statsbotco/cubejs-client/commit/c97e451)), closes [#50](https://github.com/statsbotco/cubejs-client/issues/50)

# [0.4.0](https://github.com/statsbotco/cubejs-client/compare/v0.3.5-alpha.0...v0.4.0) (2019-03-13)

### Features

- Add MongoBI connector and schema adapter support ([3ebbbf0](https://github.com/statsbotco/cubejs-client/commit/3ebbbf0))
