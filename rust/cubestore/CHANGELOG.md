# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.29.33](https://github.com/cube-js/cube.js/compare/v0.29.32...v0.29.33) (2022-03-17)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.29.30](https://github.com/cube-js/cube.js/compare/v0.29.29...v0.29.30) (2022-03-04)


### Bug Fixes

* **@cubejs-backend/cubestore-driver:** Empty tables in Cube Store if index is being used ([#4170](https://github.com/cube-js/cube.js/issues/4170)) ([2585c12](https://github.com/cube-js/cube.js/commit/2585c124f5ba3bc843e19a7f8177c8dbb35ad1cc))





## [0.29.29](https://github.com/cube-js/cube.js/compare/v0.29.28...v0.29.29) (2022-03-03)


### Bug Fixes

* **cubestore:** Add list file size validation after file upload to check upload consistency ([#4093](https://github.com/cube-js/cube.js/issues/4093)) ([1c62859](https://github.com/cube-js/cube.js/commit/1c62859747b2773b418b756178e03e685381ca82))
* **cubestore:** Deactivate tables on data corruption to allow refresh worker to reconcile failing partitions ([#4092](https://github.com/cube-js/cube.js/issues/4092)) ([2c3c83a](https://github.com/cube-js/cube.js/commit/2c3c83a97c2dfd6e6f6d5dcd3acc4afe5fda294f))
* **cubestore:** Do not spawn select workers for router nodes ([8c07bba](https://github.com/cube-js/cube.js/commit/8c07bbab4efdd3a3d929726870193401f1d42479))
* **cubestore:** Do not warmup chunks on table creation to avoid stuck on warmup of unpartitioned chunks situation ([4c27d51](https://github.com/cube-js/cube.js/commit/4c27d51a0e01b26cd7d9447454a45da25e9ce4a7))
* **cubestore:** Jobs are fetched only once 5 seconds if there's a queue ([dee115f](https://github.com/cube-js/cube.js/commit/dee115f39375ebae478dfa023c6e33e511b310b9))
* **cubestore:** Leading decimal zeros are truncated during formatting ([a97f34b](https://github.com/cube-js/cube.js/commit/a97f34b0adb088223234ae187192e6be2b483cd4))
* **cubestore:** Postpone deletion of partitions and chunks after metastore log commits to avoid missing files on sudden metastore loss ([#4094](https://github.com/cube-js/cube.js/issues/4094)) ([493c53e](https://github.com/cube-js/cube.js/commit/493c53e97f225c086524e59297b10c5a8ef4646b))


### Features

* **cubestore:** Decimal partition pruning ([#4089](https://github.com/cube-js/cube.js/issues/4089)) ([c00efad](https://github.com/cube-js/cube.js/commit/c00efadfa3a841bd9bb5707fd5e98904ca9112bc))
* **cubestore:** Introduce CUBESTORE_EVENT_LOOP_WORKER_THREADS to allow set tokio worker threads explicitly ([9349a11](https://github.com/cube-js/cube.js/commit/9349a112a795787b749f88e4179cfc8ae56575e1))
* **cubestore:** Repartition single chunks instead of partition as a whole to speed up ingestion of big tables ([#4125](https://github.com/cube-js/cube.js/issues/4125)) ([af65cdd](https://github.com/cube-js/cube.js/commit/af65cddb0728c6d101bc076b7af88db1b684cc9e))
* Unwinds CubeStore select worker panics to provide descriptive error messages ([#4097](https://github.com/cube-js/cube.js/issues/4097)) ([6e21434](https://github.com/cube-js/cube.js/commit/6e214345fe12d55534174d80a05a18597ffdd17a))





## [0.29.28](https://github.com/cube-js/cube.js/compare/v0.29.27...v0.29.28) (2022-02-10)


### Bug Fixes

* **@cubejs-backend/athena-driver:** Batching and export support ([#4039](https://github.com/cube-js/cube.js/issues/4039)) ([108f42a](https://github.com/cube-js/cube.js/commit/108f42afdd58ae0027b1b81730f7ca9e72ab9122))





## [0.29.27](https://github.com/cube-js/cube.js/compare/v0.29.26...v0.29.27) (2022-02-09)


### Bug Fixes

* **cubestore:** Ensure file size matching during downloads to localize any remote fs consistency issues ([#4054](https://github.com/cube-js/cube.js/issues/4054)) ([38fdf35](https://github.com/cube-js/cube.js/commit/38fdf3514eb5420bd6176dfae9cb4b6aa9ec6b5a))
* **cubestore:** Schema type mismatch when in memory chunks are queried ([#4024](https://github.com/cube-js/cube.js/issues/4024)) ([614809b](https://github.com/cube-js/cube.js/commit/614809b7db2e15ec65b671752da7a27474abf8b7))





## [0.29.25](https://github.com/cube-js/cube.js/compare/v0.29.24...v0.29.25) (2022-02-03)


### Bug Fixes

* **cubestore:** Decimals without integral part are ignoring sign during to_string() ([b02b1a6](https://github.com/cube-js/cube.js/commit/b02b1a6b9ffe101869e22e4e65065f017f929a30))


### Features

* **cubestore:** Support quarter granularity for date_trunc fn ([#4011](https://github.com/cube-js/cube.js/issues/4011)) ([404482d](https://github.com/cube-js/cube.js/commit/404482def1f6ea7324d329a1943e6c8270518203))





## [0.29.23](https://github.com/cube-js/cube.js/compare/v0.29.22...v0.29.23) (2022-01-26)


### Bug Fixes

* Provide more readable message for CSV parsing error ([0b9a3f8](https://github.com/cube-js/cube.js/commit/0b9a3f897a88dce2ad4387990bd64e9e06624839))





## [0.29.21](https://github.com/cube-js/cube.js/compare/v0.29.20...v0.29.21) (2022-01-17)


### Features

* **cubestore:** Bump Clang to 12 ([8a16102](https://github.com/cube-js/cube.js/commit/8a161023a183447a45dabc59cc256fc01322ff45))
* **cubestore:** Use OpenSSL 1.1.1l ([1e18bec](https://github.com/cube-js/cube.js/commit/1e18bec92be6756139387b0e9fef17c7c2cd388d))





## [0.29.20](https://github.com/cube-js/cube.js/compare/v0.29.19...v0.29.20) (2022-01-10)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.29.18](https://github.com/cube-js/cube.js/compare/v0.29.17...v0.29.18) (2022-01-09)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.29.15](https://github.com/cube-js/cube.js/compare/v0.29.14...v0.29.15) (2021-12-30)


### Features

* **cubestore:** Mark linux GNU as supported on ARM64 (post installer) ([3b385e5](https://github.com/cube-js/cube.js/commit/3b385e54d9f193559e5416a61349c707b40f5653))
* **native/cubesql:** Build for ARM64 linux GNU ([5351c41](https://github.com/cube-js/cube.js/commit/5351c41110d1940956b242e85b879db6f3622d21))





## [0.29.12](https://github.com/cube-js/cube.js/compare/v0.29.11...v0.29.12) (2021-12-29)


### Bug Fixes

* **cubestore:** Do not fail scheduler loop on lagged broadcast receive ([11a2a67](https://github.com/cube-js/cube.js/commit/11a2a67bc733a76ab6a01ca1a4c3853e5d15ea4e))





## [0.29.11](https://github.com/cube-js/cube.js/compare/v0.29.10...v0.29.11) (2021-12-24)


### Bug Fixes

* **cubestore:** Respect pending chunks for compaction ([ac23554](https://github.com/cube-js/cube.js/commit/ac235545a0516a27009035b72defaa338a2fc5d3))


### Features

* **cubestore:** Build binary for aarch64-unknown-linux-gnu ([#3844](https://github.com/cube-js/cube.js/issues/3844)) ([38b8054](https://github.com/cube-js/cube.js/commit/38b8054308353bb11a023d6c47b05761e7bc7535))





## [0.29.10](https://github.com/cube-js/cube.js/compare/v0.29.9...v0.29.10) (2021-12-22)


### Bug Fixes

* **cubestore:** Do not show errors for not uploaded chunks scheduled for removal ([ca94fb2](https://github.com/cube-js/cube.js/commit/ca94fb284d9d70753099a448b59b60690a229d93))
* **cubestore:** Re-parent chunks on compaction instead of repartitioning ([cb6b9d5](https://github.com/cube-js/cube.js/commit/cb6b9d536182ff7843ecf7726db886fb3f90511c))





## [0.29.9](https://github.com/cube-js/cube.js/compare/v0.29.8...v0.29.9) (2021-12-22)


### Features

* **cubestore:** Introduce table partition_split_threshold to allow large scale partitions ([#3837](https://github.com/cube-js/cube.js/issues/3837)) ([2bdebce](https://github.com/cube-js/cube.js/commit/2bdebced3b4c0901216557dc5729f8be1b739854))





## [0.29.8](https://github.com/cube-js/cube.js/compare/v0.29.7...v0.29.8) (2021-12-21)


### Bug Fixes

* **cubestore:** Reduce excessive startup memory usage ([4f0dfc8](https://github.com/cube-js/cube.js/commit/4f0dfc816edd41f8c1986e7665a1f318b8fe4b70))


### Features

* **cubesql:** Improve selection finder for ORDER BY ([d28897b](https://github.com/cube-js/cube.js/commit/d28897b67d233f04f2d0d22adf28f191b3320ebf))
* **cubesql:** Introduce information_schema.key_column_usage ([922b6e2](https://github.com/cube-js/cube.js/commit/922b6e2641198499eb46467085f5d32a3e4a65f6))
* **cubesql:** Introduce information_schema.referential_constraints ([cdfdcd7](https://github.com/cube-js/cube.js/commit/cdfdcd771d124fbc97358895d78c2d9770abab5c))
* **cubesql:** Introduce information_schema.schemata ([3035231](https://github.com/cube-js/cube.js/commit/303523185c65c6cdc60e1a0cb5cabf00fd2315b9))
* **cubesql:** Rewrite general planner to pass restrictions for QE ([28e127b](https://github.com/cube-js/cube.js/commit/28e127bc6f2ec6ccb24e9c5942b2b97a2593c12f))





## [0.29.5](https://github.com/cube-js/cube.js/compare/v0.29.4...v0.29.5) (2021-12-17)


### Features

* **cubesql:** Support CompoundIdentifier in compiling ([030c981](https://github.com/cube-js/cube.js/commit/030c98150a228c2c5e80c2530266509e864ed3c9))
* **cubesql:** Support DATE with compound identifier ([fa959d8](https://github.com/cube-js/cube.js/commit/fa959d89406ab84d6764e0cc035b819b2f7dae21))
* **cubesql:** Support DATE, DATE_ADD, NOW fuunctions & Intervals ([a71340c](https://github.com/cube-js/cube.js/commit/a71340c56d58377eb384a02a252bd3064c74595f))
* **cubesql:** Support hours interval ([b2d4b53](https://github.com/cube-js/cube.js/commit/b2d4b53642abbd587cd207f6f29021c2fdb74a57))





## [0.29.4](https://github.com/cube-js/cube.js/compare/v0.29.3...v0.29.4) (2021-12-16)


### Bug Fixes

* **cubesql:** IF function, support array & scalar ([1b04ad1](https://github.com/cube-js/cube.js/commit/1b04ad1b0873a689414edbfcce5f6436d651f55e))
* **cubesql:** LIKE '%(%)%' ([c75efaa](https://github.com/cube-js/cube.js/commit/c75efaa18566b8e4bf8e2448c9f9066ef2dc815c))
* **cubesql:** Substr with negative count should return empty string (not an error) ([197b9e5](https://github.com/cube-js/cube.js/commit/197b9e5edbdabfc8167ef55d643240b6a597dad4))


### Features

* **cubesql:** Support LOCATE function ([9692ae3](https://github.com/cube-js/cube.js/commit/9692ae3fa741c600e095a727c726353236c40aa7))
* **cubesql:** Support SUBSTRING with commans syntax ([ffb0a6b](https://github.com/cube-js/cube.js/commit/ffb0a6b321cfdc767ff6b0f7b93313cd3aee5c42))
* **cubesql:** Support UCASE function ([8853ec6](https://github.com/cube-js/cube.js/commit/8853ec6a194a43bf71f2e28344d80637c798f8ed))





# [0.29.0](https://github.com/cube-js/cube.js/compare/v0.28.67...v0.29.0) (2021-12-14)


### Reverts

* Revert "BREAKING CHANGE: 0.29 (#3809)" (#3811) ([db005ed](https://github.com/cube-js/cube.js/commit/db005edc04d48e8251250ab9d0e19f496cf3b52b)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809) [#3811](https://github.com/cube-js/cube.js/issues/3811)


* BREAKING CHANGE: 0.29 (#3809) ([6f1418b](https://github.com/cube-js/cube.js/commit/6f1418b9963774844f341682e594601a56bb0084)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809)


### BREAKING CHANGES

* Drop support for Node.js 10 (12.x is a minimal version)
* Upgrade Node.js to 14 for Docker images
* Drop support for Node.js 15





## [0.28.66](https://github.com/cube-js/cube.js/compare/v0.28.65...v0.28.66) (2021-12-14)


### Bug Fixes

* **cubestore:** Cleanup non active never written partitions as part of delete middle man ([#3802](https://github.com/cube-js/cube.js/issues/3802)) ([7b31c2f](https://github.com/cube-js/cube.js/commit/7b31c2fc47e568b9ad2bdd2e891202d413c7d795))
* **cubestore:** Drop created but not written partitions during reconciliation ([98326f1](https://github.com/cube-js/cube.js/commit/98326f118f33f95dc926e5c7277fa9522e636475))
* **cubestore:** GCTask queue holds a lot of delete middle man partition jobs so it looks like a memory leak ([d3ffb07](https://github.com/cube-js/cube.js/commit/d3ffb077a3b70d4843771bebedd5659d47f115f3))
* **cubestore:** Limit chunk count in a single repartition hop to avoid repartition timeouts ([67ca6c8](https://github.com/cube-js/cube.js/commit/67ca6c81a838848dfec411f704d0883dc9971cf5))





## [0.28.65](https://github.com/cube-js/cube.js/compare/v0.28.64...v0.28.65) (2021-12-10)


### Bug Fixes

* **cubesql:** Special NULL handling for LEAST function ([edb4b02](https://github.com/cube-js/cube.js/commit/edb4b02fe7de249f4d107b5556e3afef433a8e82))
* **cubestore:** Introduce network protocol hand shakes to avoid corrupted messages deserialization ([aac0a5b](https://github.com/cube-js/cube.js/commit/aac0a5b1a89ffb8607460b48cfbdc6be16c0b199))


### Features

* **cubesql:** Introduce convert_tz fn (stub) ([1f08272](https://github.com/cube-js/cube.js/commit/1f08272bca28a79c46e8b28fbe43847b276958fb))
* **cubesql:** Introduce support for least function ([434084e](https://github.com/cube-js/cube.js/commit/434084ed8ea427d5afd2a999951cfacadfbb40d2))
* **cubesql:** Introduce time_format fn (stub) ([9c9b217](https://github.com/cube-js/cube.js/commit/9c9b21728a7fe192f8452c4959482b473979f2f2))
* **cubesql:** Introduce timediff fn (stub) ([29dfb97](https://github.com/cube-js/cube.js/commit/29dfb9716298c5a579c0ffba6742e13a29325670))
* **cubesql:** Support compound identifier in ORDER BY ([6d08ba8](https://github.com/cube-js/cube.js/commit/6d08ba89216f8c856ef10dfe7ac9366c544eea3d))
* **cubesql:** Support performance_schema.session_variables & global_variables ([a807858](https://github.com/cube-js/cube.js/commit/a807858cdc3d55790be75165d8b1c14045229d80))
* **cubesql:** Support type coercion for IF function ([3b3f48c](https://github.com/cube-js/cube.js/commit/3b3f48c15166f6e172fa9b3bffe1ea6b5448bad4))
* **cubestore:** Sort NULLS LAST by default ([#3785](https://github.com/cube-js/cube.js/issues/3785)) ([02744e8](https://github.com/cube-js/cube.js/commit/02744e8c6d33363c6383aa0bf38733074c8e7d09))





## [0.28.64](https://github.com/cube-js/cube.js/compare/v0.28.63...v0.28.64) (2021-12-05)


### Bug Fixes

* **cubestore:** Support `\N` as null value during CSV imports ([fbba787](https://github.com/cube-js/cube.js/commit/fbba7874ed7d572d7656818f7305bb9e31210559))





## [0.28.63](https://github.com/cube-js/cube.js/compare/v0.28.62...v0.28.63) (2021-12-03)


### Bug Fixes

* **cubesql:** Crash with WHEN ELSE IF ([7eeadf5](https://github.com/cube-js/cube.js/commit/7eeadf54433c3cbb8f5f501cacbd84d2766be52b))
* **cubesql:** Information_schema.COLUMNS - correct DATA_TYPE fields ([337d1d1](https://github.com/cube-js/cube.js/commit/337d1d1e74d52fa58685107c4217a0987e203cba))
* **cubesql:** Initial support for compound identifiers ([e95fdb6](https://github.com/cube-js/cube.js/commit/e95fdb69760e999b0961b13471c464ea8489520c))


### Features

* **cubesql:** Implement IF function ([0e08399](https://github.com/cube-js/cube.js/commit/0e083999559293798ae7d2c45c635ef1120721de))
* **cubesql:** Implement INFORMATION_SCHEMA.COLUMNS (compatibility with MySQL) ([f37e625](https://github.com/cube-js/cube.js/commit/f37e625694e1ac2c7ba8441549dc2f0771c6c306))
* **cubesql:** Implement INFORMATION_SCHEMA.TABLES (compatibility with MySQL) ([ed0e774](https://github.com/cube-js/cube.js/commit/ed0e774d5868031cc05e8746c5b2377bfc5ea454))
* **cubesql:** Initial support for information_schema.statistics ([e478baa](https://github.com/cube-js/cube.js/commit/e478baad7d8ea340141ed868677eae3d2c09eb44))
* **cubesql:** WHERE 1 <> 1 LIMIT 0 - (metabase introspection) ([431b1e9](https://github.com/cube-js/cube.js/commit/431b1e9873bf08a2440af7cb8140be2bccc0ec00))





## [0.28.62](https://github.com/cube-js/cube.js/compare/v0.28.61...v0.28.62) (2021-12-02)


### Features

* **cubesql:** Specify transaction_isolation, transaction_read_only ([81a8f2d](https://github.com/cube-js/cube.js/commit/81a8f2d7c791938f01b56572b757edf1630b724e))
* **cubesql:** Support ISNULL ([f0a4b62](https://github.com/cube-js/cube.js/commit/f0a4b62f4bd2a1ba2caf37c764b117b352a2f2b3))





## [0.28.61](https://github.com/cube-js/cube.js/compare/v0.28.60...v0.28.61) (2021-11-30)


### Bug Fixes

* **cubesql:** Ignore SET NAMES on AST level ([495f245](https://github.com/cube-js/cube.js/commit/495f245b2652f7bf688c9f8d253c7cdf4b96edcc))
* **cubestore:** Internal: Execution error: Internal: Arrow error: Invalid argument error: number of columns(4) must match number of fields(5) in schema for streaming tables ([#3737](https://github.com/cube-js/cube.js/issues/3737)) ([d35cc1f](https://github.com/cube-js/cube.js/commit/d35cc1f2e55f89811e41774b56c2c7631083fa4b))
* **cubestore:** Support escaping sequence for ILIKE ([#3744](https://github.com/cube-js/cube.js/issues/3744)) ([fbe7376](https://github.com/cube-js/cube.js/commit/fbe73767852135e52bb61b794191b5e0c652c15f)), closes [#3742](https://github.com/cube-js/cube.js/issues/3742)


### Features

* **cubesql:** Return Status:SERVER_SESSION_STATE_CHANGED on SET operator ([6f7adf8](https://github.com/cube-js/cube.js/commit/6f7adf8abfc276386eff007f5fded08bfb0559da))
* **cubesql:** Skip SET {GLOBAL|SESSION} TRANSACTION isolation on AST level ([3afe2b1](https://github.com/cube-js/cube.js/commit/3afe2b1741f7d37a0976a57b3a905f4760f6833c))
* **cubesql:** Specify max_allowed_packet, auto_increment_increment ([dd4a22d](https://github.com/cube-js/cube.js/commit/dd4a22d713181df3e5862fd457bff32221f8eaa1))
* **cubesql:** Support specifying ColumnFlags in QE ([4170b27](https://github.com/cube-js/cube.js/commit/4170b273bb1f2743cf68af54ba365feb6ad81b7c))
* **cubestore:** Minio support ([#3738](https://github.com/cube-js/cube.js/issues/3738)) ([c857562](https://github.com/cube-js/cube.js/commit/c857562fdf1dac6644039fee646a3be1177591f1)), closes [#3510](https://github.com/cube-js/cube.js/issues/3510)





## [0.28.60](https://github.com/cube-js/cube.js/compare/v0.28.59...v0.28.60) (2021-11-25)


### Bug Fixes

* **cubesql:** MySQL CLI connection error with COM_FIELD_LIST ([#3728](https://github.com/cube-js/cube.js/issues/3728)) ([aef1401](https://github.com/cube-js/cube.js/commit/aef14014fce87b8b47dc41ead066a7647f6fe225))
* **cubesql:** Pass selection for server variables to QE ([#3724](https://github.com/cube-js/cube.js/issues/3724)) ([4c66581](https://github.com/cube-js/cube.js/commit/4c66581c00e01c905f0d631aeeada43a4b75ad67))
* **native:** Return promise for registerInterface ([be97a84](https://github.com/cube-js/cube.js/commit/be97a8433dd0d2e400734c6ab3d3110d59ad4d1a))


### Features

* **cubesql:** Enable unicode_expression (required for LEFT) ([4059a17](https://github.com/cube-js/cube.js/commit/4059a1785fece8293461323ac1de5940a8b9876d))
* **cubesql:** Support insrt function ([61bdc99](https://github.com/cube-js/cube.js/commit/61bdc991ef634b39c2684e3923f63cb83f5b598c))





## [0.28.59](https://github.com/cube-js/cube.js/compare/v0.28.58...v0.28.59) (2021-11-21)


### Bug Fixes

* **cubestore:** Ensure strict meta store consistency with single thre… ([#3696](https://github.com/cube-js/cube.js/issues/3696)) ([135bc3f](https://github.com/cube-js/cube.js/commit/135bc3feeb3db8dc0547954a50d1c3ec27195d40))
* **cubestore:** unexpected value Int(42) for type String ([e6eab32](https://github.com/cube-js/cube.js/commit/e6eab32495be386bed80d73545a960a75322e0da))


### Features

* **cubesql:** Strict check for dates in binary expr ([3919c42](https://github.com/cube-js/cube.js/commit/3919c4229b3983592cc3ae3b7d3c6b7e966ef3ad))





## [0.28.58](https://github.com/cube-js/cube.js/compare/v0.28.57...v0.28.58) (2021-11-18)


### Bug Fixes

* **cubesql:** Parsing error with single comment line ([a7697c1](https://github.com/cube-js/cube.js/commit/a7697c1751a5e0003fa85ed83be033d6db651d7b))
* **cubestore:** Error during planning: The field has has qualifier for UNION query with IN ([#3697](https://github.com/cube-js/cube.js/issues/3697)) ([01a6d6f](https://github.com/cube-js/cube.js/commit/01a6d6f121e7800a70c055c0e616eda03b642267)), closes [#3693](https://github.com/cube-js/cube.js/issues/3693)


### Features

* **cubesql:** Casting to date on compare with time dimension ([8bc7b26](https://github.com/cube-js/cube.js/commit/8bc7b26833b38d200e4ea6bb9bf717cfec6da9db))
* **cubesql:** Strict check for dates in BETWEEN operator ([329da03](https://github.com/cube-js/cube.js/commit/329da03e9c17018d1c8356b888f1ade33150eca0))
* **cubesql:** Support DATE_TRUNC(string, column) ([#3677](https://github.com/cube-js/cube.js/issues/3677)) ([e4e9b4e](https://github.com/cube-js/cube.js/commit/e4e9b4e6de3de6e59322e3a78e1a26573d7b84dd))





## [0.28.57](https://github.com/cube-js/cube.js/compare/v0.28.56...v0.28.57) (2021-11-16)


### Bug Fixes

* **cubesql:** Support identifier escaping in h/m/s granularaties ([1641b69](https://github.com/cube-js/cube.js/commit/1641b698ef106489e93804bdcf364e863d7ce072))


### Features

* **cubesql:** Initial support for INFORMATION_SCHEMA ([d1fac9e](https://github.com/cube-js/cube.js/commit/d1fac9e75cb01cbf6a1207b6e69a999e9d755d1e))
* **cubesql:** Support schema() ([3af3c84](https://github.com/cube-js/cube.js/commit/3af3c841f3cf4beb6950c83a12c86fe2320cd0bc))
* **cubesql:** Support SHOW WARNINGS ([73d91c0](https://github.com/cube-js/cube.js/commit/73d91c0f6db1d1b7d0945b15cc93cb349b26f573))
* **cubesql:** Support USER(), CURRENT_USER() ([8a848aa](https://github.com/cube-js/cube.js/commit/8a848aa872fc5d34456b3ef73e72480c9c0914c0))





## [0.28.56](https://github.com/cube-js/cube.js/compare/v0.28.55...v0.28.56) (2021-11-14)


### Bug Fixes

* **cubestore:** Drop not ready tables 30 minutes after creation to avoid metastore bloating ([e775682](https://github.com/cube-js/cube.js/commit/e775682464402e194be5cf1e22eba6880747644f))
* **cubestore:** Invalidate tables cache only on table changing operations to reduce write lock contention ([28549b8](https://github.com/cube-js/cube.js/commit/28549b8906a7da446d409a9550d414ea3afe7025))
* **cubestore:** Replace all_rows access with scans to optimize allocations ([ab985c8](https://github.com/cube-js/cube.js/commit/ab985c89b16e9bb5786be8224d444c58819288d9))





## [0.28.55](https://github.com/cube-js/cube.js/compare/v0.28.54...v0.28.55) (2021-11-12)


### Bug Fixes

* **cubestore:** Do not fail `swap_chunks` with assert -- allow to gracefully capture error ([8c8b6eb](https://github.com/cube-js/cube.js/commit/8c8b6ebe328c609ba782495415d2ff3562fe31f8))
* **cubestore:** Do not fail on repartition of empty chunks ([41b3054](https://github.com/cube-js/cube.js/commit/41b30549b9c3ec2cca0899f1f778c8334168b94b))
* **cubestore:** fix float comparisons causing unsorted data in merges ([c5b5d2c](https://github.com/cube-js/cube.js/commit/c5b5d2c2936f8974cbea6fe083ba38ad6caed793))
* **cubestore:** speed up HLL++ merges, up to 180x in some cases ([24ecbc3](https://github.com/cube-js/cube.js/commit/24ecbc38462071fa0acb465261471e868bf6e1c4))
* **cubestore:** system.tables can affect table visibility cache by including non ready tables. Ignore delete middle man reconciliation errors. ([dce711f](https://github.com/cube-js/cube.js/commit/dce711ff73460ee8ef9893ef4e6cc17273f55f7b))
* **cubestore:** Timeout for create table finalization ([d715345](https://github.com/cube-js/cube.js/commit/d7153456a7c2e6d5cae035d83ea55ba488455cb7))


### Features

* Introduce checkSqlAuth (auth hook for SQL API) ([3191b73](https://github.com/cube-js/cube.js/commit/3191b73816cd63d242349041c54a7037e9027c1a))





## [0.28.54](https://github.com/cube-js/cube.js/compare/v0.28.53...v0.28.54) (2021-11-09)


### Bug Fixes

* **cubestore:** Introduce file suffixes to avoid parquet write clashes in case of meta store recovery ([#3639](https://github.com/cube-js/cube.js/issues/3639)) ([4d01e8b](https://github.com/cube-js/cube.js/commit/4d01e8b99b9cc5496d3e6f587b3502efe0fc4584))
* **cubestore:** Row with id is not found for PartitionRocksTable. Repartition not active partitions on reconcile. Do not drop orphaned jobs that weren't scheduled. Repartition only limited amount of rows during single repartition job. ([#3636](https://github.com/cube-js/cube.js/issues/3636)) ([55bbc60](https://github.com/cube-js/cube.js/commit/55bbc606ff0a013917731ec939cfda5927413925))


### Features

* **cubestore:** System tables and commands for debugging ([#3638](https://github.com/cube-js/cube.js/issues/3638)) ([22650a1](https://github.com/cube-js/cube.js/commit/22650a1f8e5c3bae85c735fbe9f31632610f567f))





## [0.28.53](https://github.com/cube-js/cube.js/compare/v0.28.52...v0.28.53) (2021-11-04)


### Features

* **cubesql:** Real connection_id ([24d9804](https://github.com/cube-js/cube.js/commit/24d98041b4752f15156b9062dad98c801761ab0f))
* **cubesql:** Specify MySQL version as 8.0.25 in protocol ([eb7e73e](https://github.com/cube-js/cube.js/commit/eb7e73eac5819f8549f51e841f2f4fdc90ba7f32))





## [0.28.52](https://github.com/cube-js/cube.js/compare/v0.28.51...v0.28.52) (2021-11-03)


### Bug Fixes

* **cubesql:** MYSQL_TYPE_STRING for Booleans was null ([fcdd8f5](https://github.com/cube-js/cube.js/commit/fcdd8f57c34766f3d9d3827795142474a3489422))


### Features

* **cubeclient:** Granularity is an optional field ([c381570](https://github.com/cube-js/cube.js/commit/c381570b786d27c49deb701c43858cd6e2facf02))
* **cubesql:** Execute SHOW DATABASES from AST ([cd2b4ac](https://github.com/cube-js/cube.js/commit/cd2b4acac41db5ced6d706c4acc6dcf46f9179ac))
* **cubesql:** Improve filter pushing (dateRange -> timeDimension) and segment ([8d7ea9b](https://github.com/cube-js/cube.js/commit/8d7ea9b076c26d6576474d6122dbffedeacd6e8e))
* **cubestore:** partitioned indexes for faster joins ([8ca605f](https://github.com/cube-js/cube.js/commit/8ca605f8cf2e0a2bf6cc08755f74ff4f8c096cb0))





## [0.28.51](https://github.com/cube-js/cube.js/compare/v0.28.50...v0.28.51) (2021-10-30)


### Features

* **cubesql:** Skip SET  = <expr> ([616023a](https://github.com/cube-js/cube.js/commit/616023a433cdf49fe76fc175b7c24abe267ea5f2))
* **cubesql:** Support db(), version() via QE ([5a289e1](https://github.com/cube-js/cube.js/commit/5a289e15f0c689ac3277edbb9c50bb11f34abdcc))
* **cubesql:** Support system variables ([#3592](https://github.com/cube-js/cube.js/issues/3592)) ([d2bd1fa](https://github.com/cube-js/cube.js/commit/d2bd1fab4674105e777b799db580d608b2c17caf))
* **cubesql:** Use real Query Engine for simple queries ([cc907d3](https://github.com/cube-js/cube.js/commit/cc907d3e2b35462a789427e084989c2ee4a693db))





## [0.28.50](https://github.com/cube-js/cube.js/compare/v0.28.49...v0.28.50) (2021-10-28)


### Bug Fixes

* **cubestore:** Added CUBESTORE_META_BIND_ADDR and CUBESTORE_WORKER_BIND_ADDR to allow for IPv6 binds ([435f8fc](https://github.com/cube-js/cube.js/commit/435f8fcf1d02bc6da5caaae223e9c64cd2e7e8be))
* **cubestore:** assertion failed: `(left == right)` in case of trying to access table streaming table right after creation ([c900d51](https://github.com/cube-js/cube.js/commit/c900d517d4a2105a202faf79585c37ec6d56a298))
* **native:** Correct logging level for native module ([c1a8439](https://github.com/cube-js/cube.js/commit/c1a843909d6681c718e3634f60684705cdc32f29))


### Features

* **native:** Simplify filters while converting to JSON RPC ([acab66a](https://github.com/cube-js/cube.js/commit/acab66a54e4cb7ca8a64717392f0dddc362f6057))
* Validate return type for dbType/driverFactory/externalDriverFactory in runtime ([#2657](https://github.com/cube-js/cube.js/issues/2657)) ([10e269f](https://github.com/cube-js/cube.js/commit/10e269f9febe26902838a2d7fa611a0f1d375d3e))





## [0.28.49](https://github.com/cube-js/cube.js/compare/v0.28.48...v0.28.49) (2021-10-23)


### Bug Fixes

* **cubesql:** Correct LE (<), GT (>) handling for DateTime filtering ([55e805a](https://github.com/cube-js/cube.js/commit/55e805a9e1fbfd462d3ce49eccd14ad815ac8c26))





## [0.28.48](https://github.com/cube-js/cube.js/compare/v0.28.47...v0.28.48) (2021-10-22)


### Features

* **cubesql:** EXPLAIN <stmt> (debug info) ([7f0b57f](https://github.com/cube-js/cube.js/commit/7f0b57f1ed593ad51df7647aeeb9ee25055edfa6))





## [0.28.47](https://github.com/cube-js/cube.js/compare/v0.28.46...v0.28.47) (2021-10-22)


### Features

* ksql support ([#3507](https://github.com/cube-js/cube.js/issues/3507)) ([b7128d4](https://github.com/cube-js/cube.js/commit/b7128d43d2aaffdd7273555779176b3efe4e2aa6))
* **cubesql:** Simplify root AND in where ([a417d4b](https://github.com/cube-js/cube.js/commit/a417d4b9166d1ac00346ea41323d6bd6e0e4e222))
* **cubesql:** Support SHOW DATABASES (alias) ([f1c4d3f](https://github.com/cube-js/cube.js/commit/f1c4d3f922fd36cb3b0af25b44301a26b801602f))





## [0.28.46](https://github.com/cube-js/cube.js/compare/v0.28.45...v0.28.46) (2021-10-20)


### Bug Fixes

* **native:** Catch errors in authentication handshake (msql_srv) ([#3560](https://github.com/cube-js/cube.js/issues/3560)) ([9012399](https://github.com/cube-js/cube.js/commit/90123990fa5713fc1351ba0540776a9f7cd78dce))





## [0.28.42](https://github.com/cube-js/cube.js/compare/v0.28.41...v0.28.42) (2021-10-15)


### Bug Fixes

* **cubesql:** SET NAMES utf8mb4 ([9229123](https://github.com/cube-js/cube.js/commit/9229123b8160eefe47f071063a455eb854199ebf))


### Features

* Integrate SQL Connector to Cube.js ([#3544](https://github.com/cube-js/cube.js/issues/3544)) ([f90de4c](https://github.com/cube-js/cube.js/commit/f90de4c9283178962f501826a8a64abb674c37d1))





## [0.28.41](https://github.com/cube-js/cube.js/compare/v0.28.40...v0.28.41) (2021-10-12)


### Bug Fixes

* **cubestore:** fix parquet statistics for string columns ([565465a](https://github.com/cube-js/cube.js/commit/565465a02328875340d63046245637a3544ce2f1))


### Features

* Introduce cubeclient (rust client) ([ff44347](https://github.com/cube-js/cube.js/commit/ff443477925e9948b9b6e190370696e1d8375ee0))
* Introduce SQL Connector for Cube.js 🚀  ([#3527](https://github.com/cube-js/cube.js/issues/3527)) ([7d97398](https://github.com/cube-js/cube.js/commit/7d97398bc11b64c1c77463030263316fad1da27a))





## [0.28.39](https://github.com/cube-js/cube.js/compare/v0.28.38...v0.28.39) (2021-09-22)


### Bug Fixes

* **cubestore:** fix string-to-timestamp conversion ([654e81d](https://github.com/cube-js/cube.js/commit/654e81d42ef90bdcfffbe5c9760aa231facf8a43))
* **cubestore:** invalid data after compaction of binary columns ([064a9f4](https://github.com/cube-js/cube.js/commit/064a9f46995ddbf35fefa3c25f5c3a6e47d96c1a))





## [0.28.38](https://github.com/cube-js/cube.js/compare/v0.28.37...v0.28.38) (2021-09-20)


### Bug Fixes

* **cubestore:** improve diagnostics on invalid configurations ([95f3810](https://github.com/cube-js/cube.js/commit/95f3810c28d777455c7c180b91f13c4fadc623de))





## [0.28.37](https://github.com/cube-js/cube.js/compare/v0.28.36...v0.28.37) (2021-09-17)


### Features

* **cubestore:** ILIKE operator ([6a3fe64](https://github.com/cube-js/cube.js/commit/6a3fe647fb5f93932521591b6a7c572b88758bfe))





## [0.28.36](https://github.com/cube-js/cube.js/compare/v0.28.35...v0.28.36) (2021-09-14)


### Features

* **cubestore:** support reading of postgres-hll sketches ([72c38ba](https://github.com/cube-js/cube.js/commit/72c38badec1ee2ffc64218299653af1897042671))





## [0.28.32](https://github.com/cube-js/cube.js/compare/v0.28.31...v0.28.32) (2021-09-06)


### Bug Fixes

* **cubestore:** 'unsorted data in merge' ([f4fad69](https://github.com/cube-js/cube.js/commit/f4fad697332de369292c30087e74c2a5af2723b7))
* **cubestore:** do not log AWS credentials, close [#3366](https://github.com/cube-js/cube.js/issues/3366) ([9aae6e5](https://github.com/cube-js/cube.js/commit/9aae6e585e87b39714d2273e9406913d1f3a8566))





## [0.28.31](https://github.com/cube-js/cube.js/compare/v0.28.30...v0.28.31) (2021-09-02)


### Bug Fixes

* **cubestore:** fix crash on 'unexpected accumulator state List([NULL])' ([cbc0d52](https://github.com/cube-js/cube.js/commit/cbc0d5255d89481a1e88dacbf3b0dd03dc189839))





## [0.28.29](https://github.com/cube-js/cube.js/compare/v0.28.28...v0.28.29) (2021-08-31)


### Features

* Mixed rolling window and regular measure queries from rollup support ([#3326](https://github.com/cube-js/cube.js/issues/3326)) ([3147e33](https://github.com/cube-js/cube.js/commit/3147e339f14ede73e5b0d14d05b9dd1f8b79e7b8))





## [0.28.28](https://github.com/cube-js/cube.js/compare/v0.28.27...v0.28.28) (2021-08-26)


### Bug Fixes

* **cubestore:** 'unsorted data' assertion with high-precision timestamps ([58a8cb4](https://github.com/cube-js/cube.js/commit/58a8cb453953d1b7b51f95b85364b708a5e0aa8c))


### Features

* **cubestore:** readiness and liveness probes ([888b0f1](https://github.com/cube-js/cube.js/commit/888b0f1b1b3fc50fe8d1dacd8718167ec2a69057))





## [0.28.26](https://github.com/cube-js/cube.js/compare/v0.28.25...v0.28.26) (2021-08-24)


### Bug Fixes

* **cubestore:** "Unsupported Encoding DELTA_BYTE_ARRAY" ([29fcd40](https://github.com/cube-js/cube.js/commit/29fcd407e6d00e8d2080224cc2a86befb8cbeeac))


### Features

* **cubestore:** SQL extension for rolling window queries ([88a91e7](https://github.com/cube-js/cube.js/commit/88a91e74682ed4b65eea227db415c7a4845805cf))





## [0.28.25](https://github.com/cube-js/cube.js/compare/v0.28.24...v0.28.25) (2021-08-20)


### Bug Fixes

* **cubestore:** do not keep zombie child processes ([bfe3483](https://github.com/cube-js/cube.js/commit/bfe34839bcc1382b0a207995c06890adeedf38e7))





## [0.28.24](https://github.com/cube-js/cube.js/compare/v0.28.23...v0.28.24) (2021-08-19)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.28.22](https://github.com/cube-js/cube.js/compare/v0.28.21...v0.28.22) (2021-08-17)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.28.19](https://github.com/cube-js/cube.js/compare/v0.28.18...v0.28.19) (2021-08-13)


### Bug Fixes

* **cubestore:** add equality comparison between bool and int, fix [#3154](https://github.com/cube-js/cube.js/issues/3154) ([b3dc224](https://github.com/cube-js/cube.js/commit/b3dc2249af8fe397371213f933aab77fa12828e9))





## [0.28.18](https://github.com/cube-js/cube.js/compare/v0.28.17...v0.28.18) (2021-08-12)


### Bug Fixes

* **cubestore:** update datafusion to a new version ([ee80b3a](https://github.com/cube-js/cube.js/commit/ee80b3a2d16138768200e72cb7431fb067398ee8))





## [0.28.17](https://github.com/cube-js/cube.js/compare/v0.28.16...v0.28.17) (2021-08-11)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.28.14](https://github.com/cube-js/cube.js/compare/v0.28.13...v0.28.14) (2021-08-05)


### Bug Fixes

* **cubestore:** proper support for nulls in group by ([922138d](https://github.com/cube-js/cube.js/commit/922138d0c0e294f67aef133b42aeee85070e7a9a))





## [0.28.13](https://github.com/cube-js/cube.js/compare/v0.28.12...v0.28.13) (2021-08-04)


### Bug Fixes

* **cubestore:** improve errors for env var parse failures ([dbedd4e](https://github.com/cube-js/cube.js/commit/dbedd4e9103b4ce3d22c86ede4d1dc8b56f64f24))





## [0.28.10](https://github.com/cube-js/cube.js/compare/v0.28.9...v0.28.10) (2021-07-30)


### Bug Fixes

* **cubestore:** date_add and date_sub on columnar data ([418c017](https://github.com/cube-js/cube.js/commit/418c017461c88fec0e4e28e1f1a64d97a0765718))





## [0.28.9](https://github.com/cube-js/cube.js/compare/v0.28.8...v0.28.9) (2021-07-29)


### Features

* **cubestore:** add `date_sub` function ([3bf2520](https://github.com/cube-js/cube.js/commit/3bf25203db8e0ebde00c224fd9462a3a2e54bee6))





## [0.28.6](https://github.com/cube-js/cube.js/compare/v0.28.5...v0.28.6) (2021-07-22)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.28.5](https://github.com/cube-js/cube.js/compare/v0.28.4...v0.28.5) (2021-07-21)


### Bug Fixes

* **cubestore:** only pick index with exact column order ([f873a0c](https://github.com/cube-js/cube.js/commit/f873a0c2ba31dbc2ca80d4cdb0d5151a39f9e912))





## [0.28.3](https://github.com/cube-js/cube.js/compare/v0.28.2...v0.28.3) (2021-07-20)


### Bug Fixes

* **cubestore:** Installer (bad path) ([fe3458f](https://github.com/cube-js/cube.js/commit/fe3458ff492c1104c8719ec0b90a5c3b5e93a588))





## [0.28.2](https://github.com/cube-js/cube.js/compare/v0.28.1...v0.28.2) (2021-07-20)


### Bug Fixes

* **cubestore:** allow to specify join columns in any order, fix [#2987](https://github.com/cube-js/cube.js/issues/2987) ([b59aaab](https://github.com/cube-js/cube.js/commit/b59aaabc765c123dfa680e1866f79e6225219c76))
* Close Cube Store process on process exit ([#3082](https://github.com/cube-js/cube.js/issues/3082)) ([f22f71a](https://github.com/cube-js/cube.js/commit/f22f71a4fe2240a9db58c035cb87d1b0d47e5b72))





# [0.28.0](https://github.com/cube-js/cube.js/compare/v0.27.53...v0.28.0) (2021-07-17)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.27.52](https://github.com/cube-js/cube.js/compare/v0.27.51...v0.27.52) (2021-07-13)


### Bug Fixes

* **cubestore:** crash on count(distinct ...) ([516924d](https://github.com/cube-js/cube.js/commit/516924d2615ff201a7b92d88e723acb0527c0b94))





## [0.27.50](https://github.com/cube-js/cube.js/compare/v0.27.49...v0.27.50) (2021-07-12)


### Features

* **cubestore:** Introduce support for DATE_ADD ([#3085](https://github.com/cube-js/cube.js/issues/3085)) ([071d7b4](https://github.com/cube-js/cube.js/commit/071d7b430566b0f42e2fc209b1888f9b4b9bb4e7))





## [0.27.48](https://github.com/cube-js/cube.js/compare/v0.27.47...v0.27.48) (2021-07-08)


### Bug Fixes

* **cubestore:** fix panic 'Unexpected accumulator state List([NULL])' ([cfe8647](https://github.com/cube-js/cube.js/commit/cfe8647d9ffd03dfde3d0fc028249a3c43ecb527))





## [0.27.47](https://github.com/cube-js/cube.js/compare/v0.27.46...v0.27.47) (2021-07-06)


### Features

* **cubestore:** add some configuration variables ([23e26fa](https://github.com/cube-js/cube.js/commit/23e26fae914f2dd20c82bc61ae3836b4a384b1cf))





## [0.27.46](https://github.com/cube-js/cube.js/compare/v0.27.45...v0.27.46) (2021-07-01)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.27.45](https://github.com/cube-js/cube.js/compare/v0.27.44...v0.27.45) (2021-06-30)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.27.44](https://github.com/cube-js/cube.js/compare/v0.27.43...v0.27.44) (2021-06-29)


### Bug Fixes

* **cubestore:** do not store error results in cache ([636ccec](https://github.com/cube-js/cube.js/commit/636ccec1c7a5d831cea1beee2275c335b5f62b8f))
* **cubestore:** merge operation on unsorted data ([7b6c67d](https://github.com/cube-js/cube.js/commit/7b6c67d2e5f7ab93612de096cc25723ab10cec0a))





## [0.27.42](https://github.com/cube-js/cube.js/compare/v0.27.41...v0.27.42) (2021-06-25)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.27.41](https://github.com/cube-js/cube.js/compare/v0.27.40...v0.27.41) (2021-06-25)


### Features

* **cubestore:** debug data dumps for select queries ([b08617f](https://github.com/cube-js/cube.js/commit/b08617f59c835819319133ee676b62a078788845))





## [0.27.40](https://github.com/cube-js/cube.js/compare/v0.27.39...v0.27.40) (2021-06-23)


### Bug Fixes

* **cubestore:** refresh AWS credentials on timer, fix [#2946](https://github.com/cube-js/cube.js/issues/2946) ([23dee35](https://github.com/cube-js/cube.js/commit/23dee354573668f11553227ca50b5cf0b283d84a))


### Features

* **cubestore:** add now() and unix_timestamp() scalar function ([b40f3a8](https://github.com/cube-js/cube.js/commit/b40f3a896a1c65aece8c200733dd5cb8fe67d7be))





## [0.27.37](https://github.com/cube-js/cube.js/compare/v0.27.36...v0.27.37) (2021-06-21)


### Bug Fixes

* **cubestore:** fix 'Failed to write RLE run' ([845094b](https://github.com/cube-js/cube.js/commit/845094b0bd96f7d10ba7bb9faa84b95404b9527b))





## [0.27.35](https://github.com/cube-js/cube.js/compare/v0.27.34...v0.27.35) (2021-06-18)


### Bug Fixes

* **cubestore:** do not spam logs when no statsd server is listening ([44b8cad](https://github.com/cube-js/cube.js/commit/44b8cad96cdbb84dd830d255392152d3f20a4e23))
* **cubestore:** fix assertion failure (unsorted inputs to merge sort) ([534da14](https://github.com/cube-js/cube.js/commit/534da146a472e2d04cd1e97c0ab9825f02623551))
* **cubestore:** send at most one request per worker ([17e504a](https://github.com/cube-js/cube.js/commit/17e504a62c21961724dc2a17bf8a9a480cf3cf23))





## [0.27.33](https://github.com/cube-js/cube.js/compare/v0.27.32...v0.27.33) (2021-06-15)


### Bug Fixes

* **cubestore:** fix crash (merge not supported on Float64) ([78e6d36](https://github.com/cube-js/cube.js/commit/78e6d36ad289ab2cc778e0fdc6c1dedd4de4c8e7))





## [0.27.31](https://github.com/cube-js/cube.js/compare/v0.27.30...v0.27.31) (2021-06-11)


### Bug Fixes

* **cubestore:** finalize GCP configuration variable names ([116ddc5](https://github.com/cube-js/cube.js/commit/116ddc5f019715308b1a640e5e88b278b07ced3d))
* **cubestore:** optimize access to table metainformation ([e727c8b](https://github.com/cube-js/cube.js/commit/e727c8b9b223c96199e2bbfdef2cd29a4457be86))
* **cubestore:** remove backtraces from error messages ([89a2e28](https://github.com/cube-js/cube.js/commit/89a2e28c3c6aea29fdf7b628174b3e03071515f6))


### Features

* **cubestore:** Bump rocksdb for bindgen -> libloading (compatiblity aaarch64) ([a09d399](https://github.com/cube-js/cube.js/commit/a09d3998e296f0ebc2183abda28b30ab945aa4d7))





## [0.27.30](https://github.com/cube-js/cube.js/compare/v0.27.29...v0.27.30) (2021-06-04)


### Features

* **cross:** Upgrade, use llvm/clang 9 ([f046839](https://github.com/cube-js/cube.js/commit/f0468398ee1890e0a7bff8b42da975029341ada2))
* **cubestore:** combine query results on worker ([d76c9fd](https://github.com/cube-js/cube.js/commit/d76c9fdfc54feede8f2e4e6a6f62aaacd4f8f8f9))
* **cubestore:** support the 'coalesce' function, fix [#2887](https://github.com/cube-js/cube.js/issues/2887) ([017fd4b](https://github.com/cube-js/cube.js/commit/017fd4b0be6d3c236a85782e59933932f0a0a7cf))
* **cubestore:** Use NPM's proxy settings in post-installer ([0b4daec](https://github.com/cube-js/cube.js/commit/0b4daec01eb41ff67daf97918df664a3dbab300a))





## [0.27.29](https://github.com/cube-js/cube.js/compare/v0.27.27...v0.27.29) (2021-06-02)


### Bug Fixes

* **cubestore:** clean up metastore if create table fails, fix [#2880](https://github.com/cube-js/cube.js/issues/2880) ([f2d5b1a](https://github.com/cube-js/cube.js/commit/f2d5b1af2d2bd8d3444eb70464f8e5bcd3511dab))
* **cubestore:** fix encoding of sparse HLL imported from Snowflake ([5ca48f4](https://github.com/cube-js/cube.js/commit/5ca48f4384e59ac56bbdfe85644f1730d5cbe011))


### Features

* **cubestore:** cubestore-specific env vars for GCS configuration ([6760c0e](https://github.com/cube-js/cube.js/commit/6760c0e0c8706332890f50c3eb85d8e0def5d8f5))





## [0.27.28](https://github.com/cube-js/cube.js/compare/v0.27.27...v0.27.28) (2021-06-02)


### Bug Fixes

* **cubestore:** clean up metastore if create table fails, fix [#2880](https://github.com/cube-js/cube.js/issues/2880) ([f2d5b1a](https://github.com/cube-js/cube.js/commit/f2d5b1af2d2bd8d3444eb70464f8e5bcd3511dab))
* **cubestore:** fix encoding of sparse HLL imported from Snowflake ([5ca48f4](https://github.com/cube-js/cube.js/commit/5ca48f4384e59ac56bbdfe85644f1730d5cbe011))


### Features

* **cubestore:** cubestore-specific env vars for GCS configuration ([6760c0e](https://github.com/cube-js/cube.js/commit/6760c0e0c8706332890f50c3eb85d8e0def5d8f5))





## [0.27.27](https://github.com/cube-js/cube.js/compare/v0.27.26...v0.27.27) (2021-06-01)


### Bug Fixes

* **cubestore:** panic on compaction of decimals [#2868](https://github.com/cube-js/cube.js/issues/2868) ([a4eef83](https://github.com/cube-js/cube.js/commit/a4eef83602734b1e6c59e0666822f9c80eed3a90))





## [0.27.25](https://github.com/cube-js/cube.js/compare/v0.27.24...v0.27.25) (2021-06-01)


### Bug Fixes

* **cubestore:** allow decimal and float type in index keys ([32d2f69](https://github.com/cube-js/cube.js/commit/32d2f691d7fba73bd36a8cfd75ed1ecebb046ef3))
* **cubestore:** Uncompress files with .gz ([5f8062a](https://github.com/cube-js/cube.js/commit/5f8062a272ed3d01d61c0de7847bd31363079d92))


### Features

* **cubestore:** Support import of Snowflake HLL ([61324e0](https://github.com/cube-js/cube.js/commit/61324e0784314bb7c6db67a45d5da35bdc5fee26))





## [0.27.24](https://github.com/cube-js/cube.js/compare/v0.27.23...v0.27.24) (2021-05-29)


### Bug Fixes

* **cubestore:** Invalid cross-device link (os error 18) during streaming CREATE TABLE ([942f6d0](https://github.com/cube-js/cube.js/commit/942f6d0b1ee7635bf15c17b8d69467385fba4747))





## [0.27.23](https://github.com/cube-js/cube.js/compare/v0.27.22...v0.27.23) (2021-05-27)


### Bug Fixes

* **cubestore:** do not resolve aliases in having clause ([caca792](https://github.com/cube-js/cube.js/commit/caca79226d69ff2ca29b731eac9074137bdbb780))





## [0.27.22](https://github.com/cube-js/cube.js/compare/v0.27.21...v0.27.22) (2021-05-27)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.27.20](https://github.com/cube-js/cube.js/compare/v0.27.19...v0.27.20) (2021-05-25)


### Features

* **cubestore:** support aliases for right side of joins ([42a7d41](https://github.com/cube-js/cube.js/commit/42a7d4120af19e45751d683389d8003691a807e7))
* **cubestore:** support SQL for rolling window queries ([03ff70a](https://github.com/cube-js/cube.js/commit/03ff70ac9be7805b9332d3382707b42ebf625be9))





## [0.27.17](https://github.com/cube-js/cube.js/compare/v0.27.16...v0.27.17) (2021-05-22)


### Bug Fixes

* **cubestore:** fix [#2748](https://github.com/cube-js/cube.js/issues/2748), a crash in partition filter ([f6f0992](https://github.com/cube-js/cube.js/commit/f6f09923c065ac6d2b97c5cbbe42958e51dab251))
* **cubestore:** improve partition filter accuracy ([ef93d26](https://github.com/cube-js/cube.js/commit/ef93d26bfdb889c34fd267db599cbe071925d507))





## [0.27.16](https://github.com/cube-js/cube.js/compare/v0.27.15...v0.27.16) (2021-05-19)


### Features

* **cubestore:** Allow to query tables only when they imported and ready ([#2775](https://github.com/cube-js/cube.js/issues/2775)) ([02cf69a](https://github.com/cube-js/cube.js/commit/02cf69ac9477d87e898a34ac1b8acad829dd120e))
* **cubestore:** update datafusion and sqlparser-rs ([a020c07](https://github.com/cube-js/cube.js/commit/a020c070cea053983ccd28c5d47e3f805364b713))





## [0.27.15](https://github.com/cube-js/cube.js/compare/v0.27.14...v0.27.15) (2021-05-18)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.27.13](https://github.com/cube-js/cube.js/compare/v0.27.12...v0.27.13) (2021-05-13)


### Features

* **cubestore:** Use MSVC build for Windows ([d472bcd](https://github.com/cube-js/cube.js/commit/d472bcdbd2c19beb433f79fab8d6a7abc23c8c05))





## [0.27.11](https://github.com/cube-js/cube.js/compare/v0.27.10...v0.27.11) (2021-05-12)


### Bug Fixes

* **cubestore:** do not stop startup warmup on errors ([90350a3](https://github.com/cube-js/cube.js/commit/90350a34d7c7519d052174432fdcb5a3c07e4359))


### Features

* **cubestore:** import separate CSV files in parallel ([ca896b3](https://github.com/cube-js/cube.js/commit/ca896b3aa3e54d923a3054f55aaf7d4b5735a64d))





## [0.27.10](https://github.com/cube-js/cube.js/compare/v0.27.9...v0.27.10) (2021-05-11)


### Features

* Move External Cache And Queue serving to Cube Store ([#2702](https://github.com/cube-js/cube.js/issues/2702)) ([37e4268](https://github.com/cube-js/cube.js/commit/37e4268869a23c07f922a039873d349b733bf577))





## [0.27.5](https://github.com/cube-js/cube.js/compare/v0.27.4...v0.27.5) (2021-05-03)


### Bug Fixes

* **cubestore:** create `metastore-current` atomically, do not send content-length to GCS ([a2a68a0](https://github.com/cube-js/cube.js/commit/a2a68a04ab89d4df30236fa175ddc1abde79503d))
* **cubestore:** support OFFSET clause ([30b7b68](https://github.com/cube-js/cube.js/commit/30b7b68647496c995c68bbcf7a6b98ebce213783))





## [0.27.4](https://github.com/cube-js/cube.js/compare/v0.27.3...v0.27.4) (2021-04-29)


### Bug Fixes

* **cubestore:** make top-k scan less batches ([486ee32](https://github.com/cube-js/cube.js/commit/486ee328f7625fd9fb2c490ec68e1fcd2c4c8a50))





## [0.27.2](https://github.com/cube-js/cube.js/compare/v0.27.1...v0.27.2) (2021-04-28)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.27.1](https://github.com/cube-js/cube.js/compare/v0.27.0...v0.27.1) (2021-04-27)


### Bug Fixes

* **cubestore:** simplify `trim_alloc` handling ([aa8e721](https://github.com/cube-js/cube.js/commit/aa8e721fb295e6748f220cb70dd1f318f0d113f8))





# [0.27.0](https://github.com/cube-js/cube.js/compare/v0.26.104...v0.27.0) (2021-04-26)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.26.103](https://github.com/cube-js/cube.js/compare/v0.26.102...v0.26.103) (2021-04-24)


### Bug Fixes

* **cubestore:** deploy datafusion fix, add test for failing top-k ([59bc127](https://github.com/cube-js/cube.js/commit/59bc127e401a03364622b9257e48db47b496caae))
* **cubestore:** fix error on binary results of CASE ([72634e9](https://github.com/cube-js/cube.js/commit/72634e9d4e5a2cf66595895513f721032a64a0a5))





## [0.26.102](https://github.com/cube-js/cube.js/compare/v0.26.101...v0.26.102) (2021-04-22)


### Bug Fixes

* **cubestore:** download data files on worker startup ([0a6caba](https://github.com/cube-js/cube.js/commit/0a6cabad2cec32ba25d995d99f65f9c1f874895b))
* **cubestore:** download only relevant partitions on workers ([7adfd62](https://github.com/cube-js/cube.js/commit/7adfd62b220ef2194a77d82101f93831a8e02c20))





## [0.26.98](https://github.com/cube-js/cube.js/compare/v0.26.97...v0.26.98) (2021-04-15)


### Bug Fixes

* **cubestore:** allow to disable top-k with env var ([9c2838a](https://github.com/cube-js/cube.js/commit/9c2838aecf2980fa3c076aa812f12fef05924344)), closes [#2559](https://github.com/cube-js/cube.js/issues/2559)
* **cubestore:** re-enable streaming for top-k ([c21b5f7](https://github.com/cube-js/cube.js/commit/c21b5f7690d5de7570034449d24a7842dfd097c6))





## [0.26.95](https://github.com/cube-js/cube.js/compare/v0.26.94...v0.26.95) (2021-04-13)


### Bug Fixes

* post-install compatibility with yarn ([4641e81](https://github.com/cube-js/cube.js/commit/4641e814909a807ecf49e838e6dc471db6920392))





## [0.26.92](https://github.com/cube-js/cube.js/compare/v0.26.91...v0.26.92) (2021-04-12)


### Bug Fixes

* **cubestore:** temporarily disable streaming in top-k ([ff629d5](https://github.com/cube-js/cube.js/commit/ff629d51790bb54f719b38448acfbd7fb1eba67c))
* post installers compatiblity with Windows [#2520](https://github.com/cube-js/cube.js/issues/2520) ([7e9bd7c](https://github.com/cube-js/cube.js/commit/7e9bd7c86df1032d53e752654fe4a446951480bb))





## [0.26.90](https://github.com/cube-js/cube.js/compare/v0.26.89...v0.26.90) (2021-04-11)


### Bug Fixes

* **cubestore:** Empty files on temp upload ([893e467](https://github.com/cube-js/cube.js/commit/893e467dcbb1461a3f769709197838773b9eccc0))





## [0.26.89](https://github.com/cube-js/cube.js/compare/v0.26.88...v0.26.89) (2021-04-10)


### Bug Fixes

* **cubestore:** File not found for S3 during uploads ([a1b0087](https://github.com/cube-js/cube.js/commit/a1b00876c64e3206a9e0cbfa39f0440a865125a2))
* **cubestore:** Return http errors as JSON ([fb52f7d](https://github.com/cube-js/cube.js/commit/fb52f7dc647840747b86640c1466bbce78cc3817))





## [0.26.88](https://github.com/cube-js/cube.js/compare/v0.26.87...v0.26.88) (2021-04-10)


### Features

* Mysql Cube Store streaming ingests ([#2528](https://github.com/cube-js/cube.js/issues/2528)) ([0b36a6f](https://github.com/cube-js/cube.js/commit/0b36a6faa184766873ec3792785eb1aa5ca582af))





## [0.26.87](https://github.com/cube-js/cube.js/compare/v0.26.86...v0.26.87) (2021-04-10)


### Bug Fixes

* **cubestore:** Something wrong with downloading Cube Store before running it. ([208dd31](https://github.com/cube-js/cube.js/commit/208dd31f20aa64a5c79e143d40055ac2658d0745))





## [0.26.86](https://github.com/cube-js/cube.js/compare/v0.26.85...v0.26.86) (2021-04-09)


### Bug Fixes

* **cubestore:** installer - compability with windows. fix [#2520](https://github.com/cube-js/cube.js/issues/2520) ([e05db81](https://github.com/cube-js/cube.js/commit/e05db81cc7b885046b08b2a0f034e472e22c8b3e))





## [0.26.85](https://github.com/cube-js/cube.js/compare/v0.26.84...v0.26.85) (2021-04-09)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.26.84](https://github.com/cube-js/cube.js/compare/v0.26.83...v0.26.84) (2021-04-09)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.26.81](https://github.com/cube-js/cube.js/compare/v0.26.80...v0.26.81) (2021-04-07)


### Features

* Introduce databricks-jdbc-driver ([bb0b31f](https://github.com/cube-js/cube.js/commit/bb0b31fb333f2aa379f11f6733c4efc17ec12dde))





## [0.26.79](https://github.com/cube-js/cube.js/compare/v0.26.78...v0.26.79) (2021-04-06)


### Bug Fixes

* **cubestore:** reduce serialization time for record batches ([cea5fd2](https://github.com/cube-js/cube.js/commit/cea5fd21c721b0252b3a068e8f324100ebfff546))
* **cubestore:** stream results for topk ([d2c7485](https://github.com/cube-js/cube.js/commit/d2c7485807cd20d15f8da333fcf31035dab0d529))





## [0.26.74](https://github.com/cube-js/cube.js/compare/v0.26.73...v0.26.74) (2021-04-01)


### Features

* **cubestore:** top-k query planning and execution ([#2464](https://github.com/cube-js/cube.js/issues/2464)) ([3607a3a](https://github.com/cube-js/cube.js/commit/3607a3a69537feb815de470cf0a2ec9dde351ae8))





## [0.26.72](https://github.com/cube-js/cube.js/compare/v0.26.71...v0.26.72) (2021-03-29)


### Bug Fixes

* **cubestore:** Detect gnu libc without warning ([03e01e5](https://github.com/cube-js/cube.js/commit/03e01e5a30f88acfd61b4285461b25c26ef9ecfe))





## [0.26.71](https://github.com/cube-js/cube.js/compare/v0.26.70...v0.26.71) (2021-03-26)


### Bug Fixes

* **cubestore:** Remove tracing from logs ([10a264c](https://github.com/cube-js/cube.js/commit/10a264c1261bad9ae3f04753ac8c49dfe30efa63))





## [0.26.69](https://github.com/cube-js/cube.js/compare/v0.26.68...v0.26.69) (2021-03-25)


### Features

* Introduce @cubejs-backend/maven ([#2432](https://github.com/cube-js/cube.js/issues/2432)) ([6dc6034](https://github.com/cube-js/cube.js/commit/6dc6034c3cdcc8e2c2b0568c218228a18b64f44b))





## [0.26.68](https://github.com/cube-js/cube.js/compare/v0.26.67...v0.26.68) (2021-03-25)


### Bug Fixes

* **cubestore:** make get active partitions a read operation ([#2416](https://github.com/cube-js/cube.js/issues/2416)) ([a1981f3](https://github.com/cube-js/cube.js/commit/a1981f3eadeb7359ab5cabdedf7ee2e5cfe9cc00))





## [0.26.66](https://github.com/cube-js/cube.js/compare/v0.26.65...v0.26.66) (2021-03-24)


### Bug Fixes

* **cubestore:** choose inplace aggregate in more cases ([#2402](https://github.com/cube-js/cube.js/issues/2402)) ([9ab6559](https://github.com/cube-js/cube.js/commit/9ab65599ea2a900bf63c4cb5e0a2544e5766822f))


### Features

* **cubestore:** add 'no upload' mode ([#2405](https://github.com/cube-js/cube.js/issues/2405)) ([38999b0](https://github.com/cube-js/cube.js/commit/38999b05a41849cae690b8900319340a99177fdb))





## [0.26.65](https://github.com/cube-js/cube.js/compare/v0.26.64...v0.26.65) (2021-03-24)


### Bug Fixes

* Warning/skip Cube Store on unsupported platforms ([c187e11](https://github.com/cube-js/cube.js/commit/c187e119b8747e1f6bb3fe2bd84f66ae3822ac7d))
* **cubestore:** use less read and write locks during planning ([#2420](https://github.com/cube-js/cube.js/issues/2420)) ([2d5d963](https://github.com/cube-js/cube.js/commit/2d5d96343dd2ef9204cb68c7a3e897dd28fa0d52))





## [0.26.64](https://github.com/cube-js/cube.js/compare/v0.26.63...v0.26.64) (2021-03-22)


### Bug Fixes

* **cubestore-driver:** Download x86-darwin for arm64-apple (for Rosetta2) ([562ea1a](https://github.com/cube-js/cube.js/commit/562ea1aa2dd34dc6b282ad8b4216be6c09b4240e))





## [0.26.63](https://github.com/cube-js/cube.js/compare/v0.26.62...v0.26.63) (2021-03-22)


### Bug Fixes

* **cubestore:** Http message processing isn't forked ([844dab2](https://github.com/cube-js/cube.js/commit/844dab24114508c0c6ddbe068aa81d0f609250be))
* **cubestore:** Introduce meta store lock acquire timeouts to avoid deadlocks ([24b87e4](https://github.com/cube-js/cube.js/commit/24b87e41e172ab04d02e65f2343b928e3806e6bd))
* **cubestore:** Narrow check point lock life time ([b8e9003](https://github.com/cube-js/cube.js/commit/b8e9003a243d17e6ce5fa2ea8eabbf097cb42835))
* **cubestore:** Remove upstream directory when runs locally ([d5975f1](https://github.com/cube-js/cube.js/commit/d5975f13d34c46c03224d584995de6862e82f7ef))


### Features

* **cubestore:** Make query planning for indices explicit ([#2400](https://github.com/cube-js/cube.js/issues/2400)) ([a3e6c5c](https://github.com/cube-js/cube.js/commit/a3e6c5ce98974ffcb0280295e1c6182c1a46a1f4))





## [0.26.60](https://github.com/cube-js/cube.js/compare/v0.26.59...v0.26.60) (2021-03-16)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.26.58](https://github.com/cube-js/cube.js/compare/v0.26.56...v0.26.58) (2021-03-14)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.26.57](https://github.com/cube-js/cube.js/compare/v0.26.56...v0.26.57) (2021-03-14)

**Note:** Version bump only for package @cubejs-backend/cubestore





## [0.26.56](https://github.com/cube-js/cube.js/compare/v0.26.55...v0.26.56) (2021-03-13)


### Features

* **cubestore:** Tracing support ([be5ab9b](https://github.com/cube-js/cube.js/commit/be5ab9b66d2bdc65962b0e04622d1db1f8608791))





## [0.26.54](https://github.com/cube-js/cube.js/compare/v0.26.53...v0.26.54) (2021-03-12)


### Bug Fixes

* **cubestore:** fix crash on empty sort order, temporarily disable full hash aggregate optimization ([#2348](https://github.com/cube-js/cube.js/issues/2348)) ([7dfd51a](https://github.com/cube-js/cube.js/commit/7dfd51a633f1f39e95bf908164a0abc4feeab37d))





## [0.26.53](https://github.com/cube-js/cube.js/compare/v0.26.52...v0.26.53) (2021-03-11)


### Bug Fixes

* **cubestore:** fix crash on empty results from workers ([9efb2a4](https://github.com/cube-js/cube.js/commit/9efb2a46ef57d4d3d5bef91f61ba7848568e1154))
* **cubestore:** Malloc trim inside worker processes ([9962fa1](https://github.com/cube-js/cube.js/commit/9962fa1259c85826abe4527f47518e826a0bec94))
* **cubestore:** Node.js 10 support, switched to cli-progress ([032a6ab](https://github.com/cube-js/cube.js/commit/032a6abe25028c09a2947e36a58ffd94d4334dca))
* **cubestore:** update arrow, fix test merge sort over unions ([#2326](https://github.com/cube-js/cube.js/issues/2326)) ([2c02d8f](https://github.com/cube-js/cube.js/commit/2c02d8f9599e3e7131ada82bcd714d814ebd100f))
* **cubestore:** use merge sort exec when aggregations are required ([#2330](https://github.com/cube-js/cube.js/issues/2330)) ([9a4603a](https://github.com/cube-js/cube.js/commit/9a4603a857c55b868fe20e8d45536d1f1188cf44))


### Features

* **cubestore:** Support boolean expressions in partition filters ([#2322](https://github.com/cube-js/cube.js/issues/2322)) ([6fa38d3](https://github.com/cube-js/cube.js/commit/6fa38d39caa0a65beda64c1fce4ccbbff8b101da))





## [0.26.52](https://github.com/cube-js/cube.js/compare/v0.26.51...v0.26.52) (2021-03-07)


### Bug Fixes

* **@cubejs-backend/cubestore-driver:** Error: connect ECONNREFUSED 127.0.0.1:3030 ([74f4683](https://github.com/cube-js/cube.js/commit/74f468362b34f0decac67e48f52d3756ba4dc647))





## [0.26.50](https://github.com/cube-js/cube.js/compare/v0.26.49...v0.26.50) (2021-03-07)


### Bug Fixes

* **cubestore:** Group by without aggregates returns empty results ([82902dd](https://github.com/cube-js/cube.js/commit/82902ddb894dc0a0d30e88bde33b0308136789b9))





## [0.26.49](https://github.com/cube-js/cube.js/compare/v0.26.48...v0.26.49) (2021-03-05)


### Bug Fixes

* **cubestore:** fully execute a single-node query on a worker ([#2288](https://github.com/cube-js/cube.js/issues/2288)) ([00156d0](https://github.com/cube-js/cube.js/commit/00156d03b38becbb472f0b93bfb1617506caa941))
* **cubestore:** Merge aggregate performance improvements ([a0dbb1a](https://github.com/cube-js/cube.js/commit/a0dbb1ab492f5da40216435b8bf9b98f1ffda5e5))
* **cubestore:** update arrow, provide hints for default index of CubeTableExec ([#2304](https://github.com/cube-js/cube.js/issues/2304)) ([e27b8a4](https://github.com/cube-js/cube.js/commit/e27b8a4bb9b35b77625103a72a73f98ccca225e0))


### Features

* **cubestore:** Merge aggregate ([#2297](https://github.com/cube-js/cube.js/issues/2297)) ([31ebbbc](https://github.com/cube-js/cube.js/commit/31ebbbcb8a1ca2bc145b55fac00838cdeca0ea87))





## [0.26.48](https://github.com/cube-js/cube.js/compare/v0.26.47...v0.26.48) (2021-03-04)


### Bug Fixes

* **cubestore:** publish issue ([5bd1c3b](https://github.com/cube-js/cube.js/commit/5bd1c3bb74d49a4f6c363f18c6b5bb4822a543cc))





## [0.26.47](https://github.com/cube-js/cube.js/compare/v0.26.46...v0.26.47) (2021-03-04)


### Bug Fixes

* **cubestore:** post-install - compatbility with non bash env ([4b0c9ef](https://github.com/cube-js/cube.js/commit/4b0c9ef19b20d4cbfaee63337b7a0025bb31e6e9))





## [0.26.45](https://github.com/cube-js/cube.js/compare/v0.26.44...v0.26.45) (2021-03-04)


### Bug Fixes

* **cubestore:** attempt to exit gracefully on sigint (ctrl+c) ([#2255](https://github.com/cube-js/cube.js/issues/2255)) ([2b006f8](https://github.com/cube-js/cube.js/commit/2b006f80428a7202da06a9bded1b42c3d2753ced))


### Features

* **cubestore:** Extract transport to separate service ([#2236](https://github.com/cube-js/cube.js/issues/2236)) ([921786b](https://github.com/cube-js/cube.js/commit/921786b8a80bc0b2ed3e50d798a0c5bab435ec5c))





## [0.26.43](https://github.com/cube-js/cube.js/compare/v0.26.42...v0.26.43) (2021-03-02)


### Bug Fixes

* **cubestore:** post-install - right path ([fc77c8f](https://github.com/cube-js/cube.js/commit/fc77c8f1672a36205dadd90e2f33cfdf89eb330c))


### Features

* **cli:** Install Cube Store driver ([6153add](https://github.com/cube-js/cube.js/commit/6153add82ebb6abdd29424d773f8f3256ae3508e))





## [0.26.42](https://github.com/cube-js/cube.js/compare/v0.26.41...v0.26.42) (2021-03-02)


### Bug Fixes

* **cubestore:** allow to prune partitions with unbounded min or max ([#2213](https://github.com/cube-js/cube.js/issues/2213)) ([1649c09](https://github.com/cube-js/cube.js/commit/1649c094e24f9cdd00bbaef9693e9e623cbdd523))


### Features

* **cubestore:** post-install - improvements ([871cadb](https://github.com/cube-js/cube.js/commit/871cadb57109b79f1f792bc2843983aa0712e648))





## [0.26.40](https://github.com/cube-js/cube.js/compare/v0.26.39...v0.26.40) (2021-03-01)


### Bug Fixes

* **cubestore:** CubeStoreHandler - startup timeout, delay execution before start ([db9a8bd](https://github.com/cube-js/cube.js/commit/db9a8bd19f2b892151dcc051e962d9c5bedb4669))





## [0.26.39](https://github.com/cube-js/cube.js/compare/v0.26.38...v0.26.39) (2021-02-28)


### Bug Fixes

* **cubestore:** Malloc trim is broken for docker ([#2223](https://github.com/cube-js/cube.js/issues/2223)) ([5702cc4](https://github.com/cube-js/cube.js/commit/5702cc432c63d8db19a45d0938f4bbc073d05542))
* **cubestore:** use `spawn_blocking` on potentially expensive operations ([#2219](https://github.com/cube-js/cube.js/issues/2219)) ([a0f92e3](https://github.com/cube-js/cube.js/commit/a0f92e378f3c9531d4112f69a997ba32b8d09187))


### Features

* **cubestore:** Bump OpenSSL to 1.1.1h ([a1d091e](https://github.com/cube-js/cube.js/commit/a1d091e411933ed68ee823e31d5bce8703c83d06))
* **cubestore:** Web Socket transport ([#2227](https://github.com/cube-js/cube.js/issues/2227)) ([8821b9e](https://github.com/cube-js/cube.js/commit/8821b9e1378c17c54441bfb54a9ab387ae1e7044))
* Use single instance for Cube Store handler ([#2229](https://github.com/cube-js/cube.js/issues/2229)) ([35c140c](https://github.com/cube-js/cube.js/commit/35c140cac864b5b588fa88e90fec3d8b7de6acda))





## [0.26.38](https://github.com/cube-js/cube.js/compare/v0.26.37...v0.26.38) (2021-02-26)


### Bug Fixes

* **cubestore:** Reduce too verbose logging on slow queries ([1d62a47](https://github.com/cube-js/cube.js/commit/1d62a470bacf6b254b5f04f80ad44f24e84d6fb7))





## [0.26.36](https://github.com/cube-js/cube.js/compare/v0.26.35...v0.26.36) (2021-02-25)


### Bug Fixes

* **cubestore:** speed up ingestion ([#2205](https://github.com/cube-js/cube.js/issues/2205)) ([22685ea](https://github.com/cube-js/cube.js/commit/22685ea2d313893479ee9eaf88073158b0059c91))





## [0.26.35](https://github.com/cube-js/cube.js/compare/v0.26.34...v0.26.35) (2021-02-25)


### Features

* Use Cube Store as default external storage for CUBEJS_DEV_MODE ([e526676](https://github.com/cube-js/cube.js/commit/e52667617e5e687c92d383045fb1a8d5fd19cab6))





## [0.26.34](https://github.com/cube-js/cube.js/compare/v0.26.33...v0.26.34) (2021-02-25)


### Features

* **cubestore:** speed up import with faster timestamp parsing ([#2203](https://github.com/cube-js/cube.js/issues/2203)) ([18958aa](https://github.com/cube-js/cube.js/commit/18958aab4597930111211de3e8497040bce9432e))





## [0.26.33](https://github.com/cube-js/cube.js/compare/v0.26.32...v0.26.33) (2021-02-24)


### Bug Fixes

* **docker:** Move back to scretch + build linux (gnu) via cross ([4e48acc](https://github.com/cube-js/cube.js/commit/4e48acc626abce800be27b234651cc22778e1b9a))


### Features

* **cubestore:** Wait for processing loops and MySQL password support ([#2186](https://github.com/cube-js/cube.js/issues/2186)) ([f3649f5](https://github.com/cube-js/cube.js/commit/f3649f536ef7d645c686c0a5c30ca2e570790d73))





## [0.26.32](https://github.com/cube-js/cube.js/compare/v0.26.31...v0.26.32) (2021-02-24)


### Features

* **cubestore:** installer - detect musl + support windows ([9af0d34](https://github.com/cube-js/cube.js/commit/9af0d34512ef01c108ce843929009316eed51f4b))





## [0.26.31](https://github.com/cube-js/cube.js/compare/v0.26.30...v0.26.31) (2021-02-23)


### Features

* **cubestore:** Build binary for Linux (Musl) :feelsgood: ([594956c](https://github.com/cube-js/cube.js/commit/594956c9ec685d8939bfae0221c8ad6537194ab1))
* **cubestore:** Build binary for Windows :neckbeard: ([3e64d03](https://github.com/cube-js/cube.js/commit/3e64d0362f392a0461c9dc31ea7aac1d1ac0f901))





## [0.26.29](https://github.com/cube-js/cube.js/compare/v0.26.28...v0.26.29) (2021-02-22)


### Bug Fixes

* **cubestore:** switch from string to float in table value ([#2175](https://github.com/cube-js/cube.js/issues/2175)) ([05dc7d2](https://github.com/cube-js/cube.js/commit/05dc7d2174bee767ecff26acc6d4047a82f5f70d))


### Features

* **cubestore:** Ability to control process in Node.js ([f45e875](https://github.com/cube-js/cube.js/commit/f45e87560139beff1fc013f4a82b4b6a16799c1e))
* **cubestore:** installer - extract on fly ([290e264](https://github.com/cube-js/cube.js/commit/290e264a935a81a3c8181ec9a79730bf580232be))





## [0.26.27](https://github.com/cube-js/cube.js/compare/v0.26.26...v0.26.27) (2021-02-20)


### Bug Fixes

* **cubestore:** Check CUBESTORE_SKIP_POST_INSTALL before calling script ([fd2cebb](https://github.com/cube-js/cube.js/commit/fd2cebb8d4e0c91b22ffcd3f78ad15db225e6fad))





## [0.26.26](https://github.com/cube-js/cube.js/compare/v0.26.25...v0.26.26) (2021-02-20)


### Bug Fixes

* docker build ([8661acd](https://github.com/cube-js/cube.js/commit/8661acdff2b88eabeb855b25e8395815c9ecfa26))





## [0.26.24](https://github.com/cube-js/cube.js/compare/v0.26.23...v0.26.24) (2021-02-20)


### Features

* **cubestore:** Improve installer error reporting ([76dd651](https://github.com/cube-js/cube.js/commit/76dd6515fec8e809cd3b188e4c1c437707ff79a4))





## [0.26.23](https://github.com/cube-js/cube.js/compare/v0.26.22...v0.26.23) (2021-02-20)


### Features

* **cubestore:** Download binary from GitHub release. ([#2167](https://github.com/cube-js/cube.js/issues/2167)) ([9f90d2b](https://github.com/cube-js/cube.js/commit/9f90d2b27e480231c119af7b4e7039d0659b7b75))





## [0.26.22](https://github.com/cube-js/cube.js/compare/v0.26.21...v0.26.22) (2021-02-20)


### Features

* **cubestore:** Return success for create table only after table has been warmed up ([991a538](https://github.com/cube-js/cube.js/commit/991a538968b59104729a95a5be5d6b55a0aa6dcc))





## [0.26.21](https://github.com/cube-js/cube.js/compare/v0.26.20...v0.26.21) (2021-02-19)


### Bug Fixes

* **cubestore:** Cleanup memory after selects as well ([d9fd460](https://github.com/cube-js/cube.js/commit/d9fd46004caabc68145fa916a30b22b08c486a29))





## [0.26.20](https://github.com/cube-js/cube.js/compare/v0.26.19...v0.26.20) (2021-02-19)


### Bug Fixes

* **cubestore:** publish package ([60496e5](https://github.com/cube-js/cube.js/commit/60496e52e63e12b96bf750b468dc02686ddcdf5e))





## [0.26.19](https://github.com/cube-js/cube.js/compare/v0.26.18...v0.26.19) (2021-02-19)

**Note:** Version bump only for package @cubejs-backend/cubestore
