# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [1.3.61](https://github.com/cube-js/cube/compare/v1.3.60...v1.3.61) (2025-08-28)

### Bug Fixes

- **api-gateway:** Remove stack information from error responses when not in dev mode to prevent leaking internals in prod ([#9862](https://github.com/cube-js/cube/issues/9862)) ([2e0b239](https://github.com/cube-js/cube/commit/2e0b239690137040d80191c05b2d0aa0bc04f277))

### Features

- **schema-compiler:** Support shorthand for userAttributes properties in models ([#9921](https://github.com/cube-js/cube/issues/9921)) ([6086a65](https://github.com/cube-js/cube/commit/6086a6565872361d1952fb5cb395d5f1faaa9968))
- **tesseract:** Full key aggregate and logical plan refactoring ([#9807](https://github.com/cube-js/cube/issues/9807)) ([da4d7aa](https://github.com/cube-js/cube/commit/da4d7aa8ce230dd625467810051a8389962adf9a))

## [1.3.60](https://github.com/cube-js/cube/compare/v1.3.59...v1.3.60) (2025-08-28)

### Bug Fixes

- **cubesql:** Fix columns referencing outer context in subqueries ([#9924](https://github.com/cube-js/cube/issues/9924)) ([09d3b95](https://github.com/cube-js/cube/commit/09d3b9521d62797294aa225fb5aca99a0aad0567))

## [1.3.59](https://github.com/cube-js/cube/compare/v1.3.58...v1.3.59) (2025-08-26)

### Bug Fixes

- **cubesql:** Merge subqueries with SQL push down ([#9916](https://github.com/cube-js/cube/issues/9916)) ([9a5597b](https://github.com/cube-js/cube/commit/9a5597bfb555bc8f81f851b1b6866d159cf4a304))
- **redshift-driver:** Use proper query for table column types that respects fetchColumnsByOrdinalPosition ([#9915](https://github.com/cube-js/cube/issues/9915)) ([2e97019](https://github.com/cube-js/cube/commit/2e97019b272ad6d991733e8fa8becdf248256f6c))
- **schema-compiler:** Fix incorrect backAlias members collection for FILTER_PARAMS members ([#9919](https://github.com/cube-js/cube/issues/9919)) ([91115d7](https://github.com/cube-js/cube/commit/91115d7344920ed13eb9ef1b9f522c9ee67fe8af))

### Features

- **cubesql:** Report rewrite start/success events ([#9917](https://github.com/cube-js/cube/issues/9917)) ([7cc80b2](https://github.com/cube-js/cube/commit/7cc80b2e2c43ec401e95482fb9d8969ed53f458c))

## [1.3.58](https://github.com/cube-js/cube/compare/v1.3.57...v1.3.58) (2025-08-25)

### Bug Fixes

- **schema-compiler:** Use join tree for pre-agg matching ([#9597](https://github.com/cube-js/cube/issues/9597)) ([c0341ba](https://github.com/cube-js/cube/commit/c0341ba94c5aae9c5ffef178bd6ccca27735db46))

## [1.3.57](https://github.com/cube-js/cube/compare/v1.3.56...v1.3.57) (2025-08-22)

### Features

- **schema-compiler:** groups support ([#9903](https://github.com/cube-js/cube/issues/9903)) ([8a13051](https://github.com/cube-js/cube/commit/8a13051d7845d85450569c14f971169cd5ad389e))

## [1.3.56](https://github.com/cube-js/cube/compare/v1.3.55...v1.3.56) (2025-08-21)

### Bug Fixes

- **cubejs-playground:** meta update propagation ([#9895](https://github.com/cube-js/cube/issues/9895)) ([2b64bc1](https://github.com/cube-js/cube/commit/2b64bc1fa0c692cc8a8341ce945d11e2009593d6))
- **tesseract:** Param casting in Presto ([#9908](https://github.com/cube-js/cube/issues/9908)) ([346d300](https://github.com/cube-js/cube/commit/346d300f4d4ac0c231229a5883c2cf9cec746035))

### Features

- **cubesql:** Avoid `COUNT(*)` pushdown to joined cubes ([#9905](https://github.com/cube-js/cube/issues/9905)) ([e073a72](https://github.com/cube-js/cube/commit/e073a7217e25c3bc4b3c63145d4a22973a587491))

### Performance Improvements

- **schema-compiler:** Reduce JS compilation memory usage 3x-5x times. ([#9897](https://github.com/cube-js/cube/issues/9897)) ([3f1ff57](https://github.com/cube-js/cube/commit/3f1ff57589bc206bdf3cd30fed270343ef68a284))

## [1.3.55](https://github.com/cube-js/cube/compare/v1.3.54...v1.3.55) (2025-08-19)

### Bug Fixes

- **schema-compiler:** Fix joins getter in cube symbols ([#9904](https://github.com/cube-js/cube/issues/9904)) ([8012341](https://github.com/cube-js/cube/commit/8012341f4e0699c0eb3bb3a7e3eb68052295f191))

### Performance Improvements

- **cubestore:** Reduce memory allocations with pinned slices ([#9901](https://github.com/cube-js/cube/issues/9901)) ([6caf7cb](https://github.com/cube-js/cube/commit/6caf7cb13176f01c683f3c00c782a5cb74b86188))

## [1.3.54](https://github.com/cube-js/cube/compare/v1.3.53...v1.3.54) (2025-08-15)

### Features

- **schema-compiler:** Reduce memory usage after compilation is done ([#9890](https://github.com/cube-js/cube/issues/9890)) ([812efec](https://github.com/cube-js/cube/commit/812efec7afa071b77d4da0a39fcbf3987b7f0464))
- **snowflake-driver:** Add queryTag connection parameter ([#9889](https://github.com/cube-js/cube/issues/9889)) ([d0554d6](https://github.com/cube-js/cube/commit/d0554d634ddd815bef1d2f0f33ebf15a885ccd05))

## [1.3.53](https://github.com/cube-js/cube/compare/v1.3.52...v1.3.53) (2025-08-15)

### Features

- **athena-driver:** export env variables for IAM assume role auth ([#9882](https://github.com/cube-js/cube/issues/9882)) ([3c6fc89](https://github.com/cube-js/cube/commit/3c6fc89dcabe651d83599e7568f571c841ee8bbb))
- **client-core:** introduce cubesql method ([#9884](https://github.com/cube-js/cube/issues/9884)) ([423aadc](https://github.com/cube-js/cube/commit/423aadc94a2c2d2006f2887e7d0e0fc1d3682ee5))

## [1.3.52](https://github.com/cube-js/cube/compare/v1.3.51...v1.3.52) (2025-08-14)

### Bug Fixes

- **api-gateway:** Handle array format for joins in /meta?extended endpoint ([#9881](https://github.com/cube-js/cube/issues/9881)) ([e1a2cfa](https://github.com/cube-js/cube/commit/e1a2cfa883ed95adae572914f5d1c8dad8cb5947)), closes [#9800](https://github.com/cube-js/cube/issues/9800)

### Features

- **cubesql:** Support cursors in stream mode ([#9877](https://github.com/cube-js/cube/issues/9877)) ([8ddaba5](https://github.com/cube-js/cube/commit/8ddaba5553addd2a5352fe2a54e50c236252d4d3))
- **snowflake-driver:** Upgrade Snowflake Node.js driver to 2.2.0 ([#9880](https://github.com/cube-js/cube/issues/9880)) ([3956b71](https://github.com/cube-js/cube/commit/3956b71259633c60ba95c8c0e979ee49c6d7b6ee))

## [1.3.51](https://github.com/cube-js/cube/compare/v1.3.50...v1.3.51) (2025-08-14)

### Performance Improvements

- Debounce information schema queries to Cube Store (1.8x) ([#9879](https://github.com/cube-js/cube/issues/9879)) ([6fe6b09](https://github.com/cube-js/cube/commit/6fe6b09ed933660ab73009a2f30b9baf0f147570))

## [1.3.50](https://github.com/cube-js/cube/compare/v1.3.49...v1.3.50) (2025-08-13)

### Bug Fixes

- **client-core:** Remove default value in table pivot ([#9869](https://github.com/cube-js/cube/issues/9869)) ([692c4ae](https://github.com/cube-js/cube/commit/692c4ae8ccd6ff1e1a26bddd5d8e65f81c4f3488))

### Features

- **cubestore:** Support EXPLAIN for meta queries ([#9876](https://github.com/cube-js/cube/issues/9876)) ([619eb90](https://github.com/cube-js/cube/commit/619eb901544ae08d0fbf5fccdcff546a06cac6cf))
- **tesseract:** Support time series queries in Databricks ([#9871](https://github.com/cube-js/cube/issues/9871)) ([b238a29](https://github.com/cube-js/cube/commit/b238a29f0637054386a84a8d6f72cc63949eb7d0))

## [1.3.49](https://github.com/cube-js/cube/compare/v1.3.48...v1.3.49) (2025-08-12)

### Bug Fixes

- **cubesql:** Improve SQL push down for Athena/Presto ([#9873](https://github.com/cube-js/cube/issues/9873)) ([893e9b3](https://github.com/cube-js/cube/commit/893e9b3e0dd200a26c8b97bb7a39532707edf632))

### Features

- **docker:** Security upgrade Node.js from 22.16.0 to 22.18.0 ([#9854](https://github.com/cube-js/cube/issues/9854)) ([78ec2a5](https://github.com/cube-js/cube/commit/78ec2a59e332dbe5f06ba67efbe0d56ed39b064c))

## [1.3.48](https://github.com/cube-js/cube/compare/v1.3.47...v1.3.48) (2025-08-09)

### Bug Fixes

- access_policy is not applied for JS models ([#9865](https://github.com/cube-js/cube/issues/9865)) ([89c78ec](https://github.com/cube-js/cube/commit/89c78ec921d12e14aefa89193df544eadc221cfb))
- **cubesql:** Allow repeated aliases (auto-realias) ([#9863](https://github.com/cube-js/cube/issues/9863)) ([0fb183a](https://github.com/cube-js/cube/commit/0fb183a8c63f180f92913d84fa6890aec4339fcc))
- **cubesql:** Improve Trino SQL push down compatibility ([#9861](https://github.com/cube-js/cube/issues/9861)) ([9d5794e](https://github.com/cube-js/cube/commit/9d5794e9eb3610d722adc0a8ec5092140efdf0f1))
- **cubesql:** Support concatenating non-strings in SQL push down for Athena/Presto ([#9853](https://github.com/cube-js/cube/issues/9853)) ([97e54e0](https://github.com/cube-js/cube/commit/97e54e01ad78a9483736d92339769332c9934e68))

### Features

- **cubesql:** Support date type for parameter binding ([#9864](https://github.com/cube-js/cube/issues/9864)) ([5246fa0](https://github.com/cube-js/cube/commit/5246fa0836a11f38a9cc86357674c203414085dd))
- **cubestore:** Upgrade rust to nightly-2025-08-01 ([#9858](https://github.com/cube-js/cube/issues/9858)) ([54b1553](https://github.com/cube-js/cube/commit/54b1553e04e674a6bc342dfd6d757a0136d67425))

### Performance Improvements

- **cubestore:** Reduce allocations in info schema tables ([#9855](https://github.com/cube-js/cube/issues/9855)) ([3cc34ff](https://github.com/cube-js/cube/commit/3cc34ff9e11b5dc1fe94463f82d587f5db8a76ac))

## [1.3.47](https://github.com/cube-js/cube/compare/v1.3.46...v1.3.47) (2025-08-04)

### Features

- **cubesql:** Allow to bind float64 (support in pg-srv) ([#9846](https://github.com/cube-js/cube/issues/9846)) ([760640c](https://github.com/cube-js/cube/commit/760640cadfa8e6a6728608de2cc7d71431fe93ce))
- **cubesql:** Support `BETWEEN` SQL push down ([#9834](https://github.com/cube-js/cube/issues/9834)) ([195402f](https://github.com/cube-js/cube/commit/195402f76f893a0649488a373fb5b46b7f0a04b3))
- **cubesql:** Support timestamp parameter binding, fix [#9784](https://github.com/cube-js/cube/issues/9784) ([#9847](https://github.com/cube-js/cube/issues/9847)) ([8a614bb](https://github.com/cube-js/cube/commit/8a614bb715f4e7baf597a03a1134702ea6e3c286))

## [1.3.46](https://github.com/cube-js/cube/compare/v1.3.45...v1.3.46) (2025-07-31)

### Bug Fixes

- **clickhouse-driver:** Parse Error: Header overflow due to X-ClickHouse-Progress ([#9842](https://github.com/cube-js/cube/issues/9842)) ([e7db3fe](https://github.com/cube-js/cube/commit/e7db3fe369824e6960cb59ec48f77fed2eddcbd2))

### Features

- add datasource schema read methods ([#9818](https://github.com/cube-js/cube/issues/9818)) ([8fd6dcc](https://github.com/cube-js/cube/commit/8fd6dcc2ec69fa37f79d22c78d2a9e50160d62df))
- **cubesql:** Add support for `current_catalog` function for postgresql protocol ([#9839](https://github.com/cube-js/cube/issues/9839)) ([95dc46e](https://github.com/cube-js/cube/commit/95dc46e08735ae0319048ef4bcd52cb4226d26f5))

## [1.3.45](https://github.com/cube-js/cube/compare/v1.3.44...v1.3.45) (2025-07-29)

### Bug Fixes

- **api-gateway:** Fix member sql extraction in meta?extended ([#9826](https://github.com/cube-js/cube/issues/9826)) ([4647ea3](https://github.com/cube-js/cube/commit/4647ea3fb23e5d3b5cfd8b06555c3d6e47e55076))
- **prestodb/trino-driver:** Specify correct timeouts for query execution ([#9827](https://github.com/cube-js/cube/issues/9827)) ([394eb84](https://github.com/cube-js/cube/commit/394eb84910160e86e4634df6bcc78c7ac803803b))

### Features

- **clickhouse-driver:** Upgrade @clickhouse/client from 1.7.0 to 1.12.0 ([#9829](https://github.com/cube-js/cube/issues/9829)) ([80c281f](https://github.com/cube-js/cube/commit/80c281f330b16fd27a7b61d9ff030ce4fcf92b13))
- **cubesql:** Improve DataGrip compatibility ([#9825](https://github.com/cube-js/cube/issues/9825)) ([18b09ee](https://github.com/cube-js/cube/commit/18b09ee5408bd82d28eb06f6f23e7cab29a2ce54))
- **query-ochestrator:** Reduce number of cache set for used flag ([#9828](https://github.com/cube-js/cube/issues/9828)) ([b9ff371](https://github.com/cube-js/cube/commit/b9ff3714ecdbe058f941ee79a3e6e6e0687e1005))

## [1.3.44](https://github.com/cube-js/cube/compare/v1.3.43...v1.3.44) (2025-07-28)

### Bug Fixes

- **base-driver:** Support empty credentials for gcs ([#9820](https://github.com/cube-js/cube/issues/9820)) ([9fb6711](https://github.com/cube-js/cube/commit/9fb67114f7d8df9f00ddf3ce9fcd2de3c17543df))
- **query-orchestrator:** QueryQueue - update heartbeat by queue id ([#9817](https://github.com/cube-js/cube/issues/9817)) ([c388350](https://github.com/cube-js/cube/commit/c38835023f5a4b164a2aa503b2eab0df5f899b00))

### Features

- **partners:** rename link ([#9822](https://github.com/cube-js/cube/issues/9822)) ([58cc4ac](https://github.com/cube-js/cube/commit/58cc4ac956d7af9979718b9060c949e3de7850f8))

## [1.3.43](https://github.com/cube-js/cube/compare/v1.3.42...v1.3.43) (2025-07-24)

### Bug Fixes

- **server-core:** Fix getCompilersInstances to not return compiler promise ([#9814](https://github.com/cube-js/cube/issues/9814)) ([5622a73](https://github.com/cube-js/cube/commit/5622a732c6e6a25f9bda35a68e2faf61a3ae04e3))
- **snowflake-driver:** Set date/timestamp format for exporting data to CSV export bucket ([#9810](https://github.com/cube-js/cube/issues/9810)) ([4c0fef7](https://github.com/cube-js/cube/commit/4c0fef7b068893322f05310b028059ea3dad8986))

## [1.3.42](https://github.com/cube-js/cube/compare/v1.3.41...v1.3.42) (2025-07-23)

### Bug Fixes

- **query-orchestrator:** Reduce number of refresh key queries ([#9809](https://github.com/cube-js/cube/issues/9809)) ([a6b68f8](https://github.com/cube-js/cube/commit/a6b68f84fac89d2fc710aa3ccd1ef10b741e1fa5))

### Features

- **tesseract:** Lambda rollup support ([#9806](https://github.com/cube-js/cube/issues/9806)) ([eb56169](https://github.com/cube-js/cube/commit/eb56169f5e88424b95b12dda8535cab383ecc541))

## [1.3.41](https://github.com/cube-js/cube/compare/v1.3.40...v1.3.41) (2025-07-22)

### Features

- **cubestore:** Upgrade rocksdb to 7.10.2 from 7.9.2 ([#9802](https://github.com/cube-js/cube/issues/9802)) ([585bb1e](https://github.com/cube-js/cube/commit/585bb1e2c4992b13e1919623a6fcf38ed00d96af))

## [1.3.40](https://github.com/cube-js/cube/compare/v1.3.39...v1.3.40) (2025-07-20)

### Bug Fixes

- **cubesql:** Add missing pub CubeMetaNestedFolder in cube client ([#9790](https://github.com/cube-js/cube/issues/9790)) ([d78cd6b](https://github.com/cube-js/cube/commit/d78cd6b8a7ac2439ed57ce9f3390bdb6d3fc69e4))
- **docs:** fix a broken menu entry in docs ([#9793](https://github.com/cube-js/cube/issues/9793)) ([bf30940](https://github.com/cube-js/cube/commit/bf30940e75a29a838375659ea12653ca3dfd204c))
- **native:** Handle null correctly as JsNull, not as JsString ([#9717](https://github.com/cube-js/cube/issues/9717)) ([0523f7a](https://github.com/cube-js/cube/commit/0523f7a823ffbd878292f6e422989e625985a31f))
- Send column schema and empty data for empty /cubesql result sets instead of empty response ([#9798](https://github.com/cube-js/cube/issues/9798)) ([113f45b](https://github.com/cube-js/cube/commit/113f45b7a4417712ff57aab96e8b690c2c82de68))

## [1.3.39](https://github.com/cube-js/cube/compare/v1.3.38...v1.3.39) (2025-07-17)

### Bug Fixes

- **cubesql:** Fix sort push down projection on complex expressions ([#9787](https://github.com/cube-js/cube/issues/9787)) ([cd1c983](https://github.com/cube-js/cube/commit/cd1c9832386e69ab154f8d01f2bf67c63ff5c685))

## [1.3.38](https://github.com/cube-js/cube/compare/v1.3.37...v1.3.38) (2025-07-16)

### Bug Fixes

- **cubesql:** Propagate errors from SqlAuthService to the user ([#9665](https://github.com/cube-js/cube/issues/9665)) ([3037ada](https://github.com/cube-js/cube/commit/3037adaa55dcb20ac6a3b3064de4292fc454fdc2))
- **docs:** fix AWS deployment doc redirect ([#9786](https://github.com/cube-js/cube/issues/9786)) ([93c83f8](https://github.com/cube-js/cube/commit/93c83f889c147d28c2bd7ea276572687be61777e))
- **schema-compiler:** Fix BigQuery convertTz implementation ([#9782](https://github.com/cube-js/cube/issues/9782)) ([75f4813](https://github.com/cube-js/cube/commit/75f48139abccc341398980c7b9abfd78bc7d21aa))

### Features

- **cubesql:** Push Limit-Sort down Projection ([#9776](https://github.com/cube-js/cube/issues/9776)) ([72e6059](https://github.com/cube-js/cube/commit/72e605966100bb24d44b715d96cfb2cc4d8d793d))
- **schema-compiler,api-gateway:** Nested folders support ([#9659](https://github.com/cube-js/cube/issues/9659)) ([720f048](https://github.com/cube-js/cube/commit/720f0485c8b11f16eb99490259a881c21b845c73))
- **tesseract:** Allow named calendar timeshifts for common intervals ([#9777](https://github.com/cube-js/cube/issues/9777)) ([a5f8a2e](https://github.com/cube-js/cube/commit/a5f8a2e0d93bf5de0291389d846660f6491651fe))

## [1.3.37](https://github.com/cube-js/cube/compare/v1.3.36...v1.3.37) (2025-07-14)

### Bug Fixes

- **tesseract:** Fix infinite loops during calendar time dimension processing ([#9772](https://github.com/cube-js/cube/issues/9772)) ([3f16a39](https://github.com/cube-js/cube/commit/3f16a39c27ac7da2d5671031cb1745e0053b4a42))

### Features

- **tesseract:** Introduce named multistage time shifts for use with calendars ([#9773](https://github.com/cube-js/cube/issues/9773)) ([e033aaf](https://github.com/cube-js/cube/commit/e033aaf34af65192f981b942d834e33b1e61b851))

## [1.3.36](https://github.com/cube-js/cube/compare/v1.3.35...v1.3.36) (2025-07-10)

### Bug Fixes

- **cubesql:** Fix adding missing columns for `ORDER BY` clause ([#9764](https://github.com/cube-js/cube/issues/9764)) ([185db54](https://github.com/cube-js/cube/commit/185db547e0c83c0e276c9a89618b4753c969dea7))
- **cubesql:** Improve DBeaver compatibility ([#9769](https://github.com/cube-js/cube/issues/9769)) ([c206c90](https://github.com/cube-js/cube/commit/c206c901e9da14a230a2b358dfc4ea577adf9f49))
- **cubestore:** Avoid bug in topk planning when projection column order is permuted ([#9765](https://github.com/cube-js/cube/issues/9765)) ([49a58ff](https://github.com/cube-js/cube/commit/49a58fffb8ffa064debe9b4cfecc078e5da01004))
- **cubestore:** Make projection_above_limit optimization behave deter… ([#9766](https://github.com/cube-js/cube/issues/9766)) ([08f21b6](https://github.com/cube-js/cube/commit/08f21b67dbf6f05739822409ef0c48265bc0d429))

### Features

- **tesseract:** Rolling window with custom granularities without date range ([#9762](https://github.com/cube-js/cube/issues/9762)) ([d81c57d](https://github.com/cube-js/cube/commit/d81c57db80e96a211c1d12a67b06cefd868e111e))
- **tesseract:** Rollup Join support ([#9745](https://github.com/cube-js/cube/issues/9745)) ([05bb520](https://github.com/cube-js/cube/commit/05bb52091ad2d3ac09303c0fa50603cd24c99d61))
- **tesseract:** Support for time shifts over calendar cubes dimensions ([#9758](https://github.com/cube-js/cube/issues/9758)) ([70dfa52](https://github.com/cube-js/cube/commit/70dfa52a2aa0cafd80cdf21229d70795392009ec))

## [1.3.35](https://github.com/cube-js/cube/compare/v1.3.34...v1.3.35) (2025-07-09)

### Bug Fixes

- **api-gateway:** Update shown: true to public : true in error messages ([#9756](https://github.com/cube-js/cube/issues/9756)) ([4dc44bb](https://github.com/cube-js/cube/commit/4dc44bbd30ea31153191a562ecd4ffa950dc9eec))
- **cubesql:** Hide security context from logs ([#9761](https://github.com/cube-js/cube/issues/9761)) ([e38c03c](https://github.com/cube-js/cube/commit/e38c03c2ba95f86965909b0f9161babd7bb52ecf))
- **cubesql:** Normalize `EXTRACT`/`DATE_TRUNC` granularities ([#9759](https://github.com/cube-js/cube/issues/9759)) ([2db54ba](https://github.com/cube-js/cube/commit/2db54ba4a70c4e96a1ffa196fe43270385a48b3c))
- **schema-compiler:** Avoid ambiguous dimension column mappings for ClickHouse queries ([#9674](https://github.com/cube-js/cube/issues/9674)) ([ea2a59f](https://github.com/cube-js/cube/commit/ea2a59fa708e8739f1c6052ac9896f0d01819470))

### Features

- **snowflake-driver:** Introduce CUBEJS_DB_SNOWFLAKE_OAUTH_TOKEN env var / config option ([#9752](https://github.com/cube-js/cube/issues/9752)) ([1c66d3e](https://github.com/cube-js/cube/commit/1c66d3eeb8e138e13bef4411f60513de97dc15d4))

## [1.3.34](https://github.com/cube-js/cube/compare/v1.3.33...v1.3.34) (2025-07-04)

### Bug Fixes

- **schema-compiler:** Fix pre-agg partitions creation for Ksql ([#9750](https://github.com/cube-js/cube/issues/9750)) ([f08f19b](https://github.com/cube-js/cube/commit/f08f19b2a78105bd5fcc06de3d156767eb483908))

## [1.3.33](https://github.com/cube-js/cube/compare/v1.3.32...v1.3.33) (2025-07-03)

### Features

- **cubesql:** Filter push down for date_part('year', ?col) = ?literal ([#9749](https://github.com/cube-js/cube/issues/9749)) ([f952cf7](https://github.com/cube-js/cube/commit/f952cf7e895ca5623d356bed6d104fd26b081ec9))

## [1.3.32](https://github.com/cube-js/cube/compare/v1.3.31...v1.3.32) (2025-07-03)

### Features

- **tesseract:** MultiStage rolling window ([#9747](https://github.com/cube-js/cube/issues/9747)) ([4f5e5dc](https://github.com/cube-js/cube/commit/4f5e5dcd47e93be237e856d672e7df3c3a2f3d6f))

## [1.3.31](https://github.com/cube-js/cube/compare/v1.3.30...v1.3.31) (2025-07-02)

### Bug Fixes

- **clickhouse-driver:** Fix headers check ([#9746](https://github.com/cube-js/cube/issues/9746)) ([b643b0c](https://github.com/cube-js/cube/commit/b643b0cbf85e9090008e63f3fb516bcf302b4967))
- **schema-compiler:** Fix date filter truncation for rolling window queries ([#9743](https://github.com/cube-js/cube/issues/9743)) ([f045af9](https://github.com/cube-js/cube/commit/f045af91086dafad80fe363a4ddba6f6411892f7))

### Features

- **cubestore:** Use seperate working loop for queue ([#6243](https://github.com/cube-js/cube/issues/6243)) ([f961f97](https://github.com/cube-js/cube/commit/f961f97328efbd6535cbcaf510c38904984b0970))
- **tesseract:** Support custom granularities in to_date rolling windows ([#9739](https://github.com/cube-js/cube/issues/9739)) ([a8b611c](https://github.com/cube-js/cube/commit/a8b611c877dfa87edc14652af6d3ebeafc071bd2))

## [1.3.30](https://github.com/cube-js/cube/compare/v1.3.29...v1.3.30) (2025-07-01)

### Bug Fixes

- **tesseract:** Fix join condition for toDate Rolling Window ([#9738](https://github.com/cube-js/cube/issues/9738)) ([dae44a4](https://github.com/cube-js/cube/commit/dae44a471a27f771b4908274ba0990502e0c8982))

### Features

- **cubesql:** Support `AGE` function ([#9734](https://github.com/cube-js/cube/issues/9734)) ([5b2682c](https://github.com/cube-js/cube/commit/5b2682c3569933e94f56f6d998065b9063525d29))
- **cubesql:** Support `DATE_PART` with intervals ([#9740](https://github.com/cube-js/cube/issues/9740)) ([65d084d](https://github.com/cube-js/cube/commit/65d084ddd81f6cfefe836b224fb9dd7575a62756))
- **cubesql:** Support decimal math with scalar ([#9742](https://github.com/cube-js/cube/issues/9742)) ([2629d36](https://github.com/cube-js/cube/commit/2629d36572944b0b1f6194970c4a4e6132fd5a8a))

## [1.3.29](https://github.com/cube-js/cube/compare/v1.3.28...v1.3.29) (2025-07-01)

### Bug Fixes

- **cubesql:** Fix incorrect datetime parsing in filters rewrite rules ([#9732](https://github.com/cube-js/cube/issues/9732)) ([6e73860](https://github.com/cube-js/cube/commit/6e73860aa92aa9b2733a771ded59b2febf9853dd))

### Features

- **athena-driver:** Add option for providing default database to use ([#9735](https://github.com/cube-js/cube/issues/9735)) ([834d381](https://github.com/cube-js/cube/commit/834d3815fe6c5228ea5f1ea768027309957524d1))
- **cubestore:** Rockstore - optimize index scanning ([#9728](https://github.com/cube-js/cube/issues/9728)) ([7987b75](https://github.com/cube-js/cube/commit/7987b757f9bc674fc3973493b3707865093fcf0a))
- **prestodb-driver:** Support s3 export bucket ([#9736](https://github.com/cube-js/cube/issues/9736)) ([31c6629](https://github.com/cube-js/cube/commit/31c6629c50eb487879254b1f31b55489a7fe82fb))

## [1.3.28](https://github.com/cube-js/cube/compare/v1.3.27...v1.3.28) (2025-06-30)

### Bug Fixes

- **cubesql:** Fix cube rust client schema for custom granularities with sql ([#9727](https://github.com/cube-js/cube/issues/9727)) ([2711fa6](https://github.com/cube-js/cube/commit/2711fa6a37322a645e995f17f269d9291345c78a))
- **tesseract:** MultiStage group_by and reduce_by now work with views ([#9710](https://github.com/cube-js/cube/issues/9710)) ([f17f49a](https://github.com/cube-js/cube/commit/f17f49aba5a0e84361147514e6e45d708569dba3))

## [1.3.27](https://github.com/cube-js/cube/compare/v1.3.26...v1.3.27) (2025-06-30)

### Bug Fixes

- **cubejs-schema-compiler:** Stay unchanged `__user` / `__cubejoinfield` names in aliasing ([#8303](https://github.com/cube-js/cube/issues/8303)) ([7bb4bdc](https://github.com/cube-js/cube/commit/7bb4bdc3f6b2d67a6f8263730f84fc3289b08347))
- **cubesql:** Fix incorrect underscore truncation for aliases ([#9716](https://github.com/cube-js/cube/issues/9716)) ([c16175b](https://github.com/cube-js/cube/commit/c16175bf964fbb351bede1bfe0fd13adf793e51a))
- **cubestore:** Cache - minimize possible bad effect of pro active eviction ([#9692](https://github.com/cube-js/cube/issues/9692)) ([22407d7](https://github.com/cube-js/cube/commit/22407d7c2043b90e0c55bb51fdb97de01153c300))
- **schema-compiler:** Fix BigQuery rolling window time series queries ([#9718](https://github.com/cube-js/cube/issues/9718)) ([1f6cf8f](https://github.com/cube-js/cube/commit/1f6cf8fe44c7cd802ef47785a34e06c23fb18829))
- **schema-compiler:** Use member expression definition as measure key ([#9154](https://github.com/cube-js/cube/issues/9154)) ([e478d0e](https://github.com/cube-js/cube/commit/e478d0ea20e33f383b624a4dbf4cff14359336f8))
- **tesseracrt:** Fix filter params casting for BigQuery dialect ([#9720](https://github.com/cube-js/cube/issues/9720)) ([7b48c27](https://github.com/cube-js/cube/commit/7b48c275ecb4b18ed6262f23866760f46420e099))

### Features

- **tesseract:** Support calendar cubes and custom sql granularities ([#9698](https://github.com/cube-js/cube/issues/9698)) ([40b3708](https://github.com/cube-js/cube/commit/40b3708ce053fd5ea1664bea70cdc613cf343810))

## [1.3.26](https://github.com/cube-js/cube/compare/v1.3.25...v1.3.26) (2025-06-25)

### Bug Fixes

- **cubesql:** Push down `__user` meta filter further ([#9711](https://github.com/cube-js/cube/issues/9711)) ([5dd626a](https://github.com/cube-js/cube/commit/5dd626a2471a8282dd51a9d6d03654dcf44e2f80))
- **schema-compiler:** Fix incorrect truncated time dimensions over time series queries for BigQuery ([#9615](https://github.com/cube-js/cube/issues/9615)) ([b075966](https://github.com/cube-js/cube/commit/b075966a6882c70a8f21652fca83cca74611e632))

## [1.3.25](https://github.com/cube-js/cube/compare/v1.3.24...v1.3.25) (2025-06-24)

### Features

- **tesseract:** Athena support ([#9707](https://github.com/cube-js/cube/issues/9707)) ([a35a477](https://github.com/cube-js/cube/commit/a35a47785fbdc98ed1f9153df3fcdda28d5a7dd0))

## [1.3.24](https://github.com/cube-js/cube/compare/v1.3.23...v1.3.24) (2025-06-24)

### Bug Fixes

- **docs:** fix typo in Cube principal name in AWS docs ([#9697](https://github.com/cube-js/cube/issues/9697)) ([b4a9597](https://github.com/cube-js/cube/commit/b4a9597d48f0d63e53b8c9d6f98e98b0c25233f4))
- **schema-compiler:** Correct join hints collection for transitive joins ([#9671](https://github.com/cube-js/cube/issues/9671)) ([f60b4aa](https://github.com/cube-js/cube/commit/f60b4aa99285fb5e0c8a80f28837cbbdb5d634dd))
- **schema-compiler:** Fix BigQuery queries datetime/timestamp comparisons ([#9683](https://github.com/cube-js/cube/issues/9683)) ([525932c](https://github.com/cube-js/cube/commit/525932cb9d28d333a94c678f389334832f3a3ba8))

### Features

- **schema-compiler:** Allow to specify td with granularity in REST API query order section ([#9630](https://github.com/cube-js/cube/issues/9630)) ([ba13bd3](https://github.com/cube-js/cube/commit/ba13bd369607f4672802933c015e82169e3f6876))

## [1.3.23](https://github.com/cube-js/cube/compare/v1.3.22...v1.3.23) (2025-06-19)

### Bug Fixes

- **api-gateway:** Fix request validation (offset & limit must be numbers strictly) ([#9686](https://github.com/cube-js/cube/issues/9686)) ([b94470f](https://github.com/cube-js/cube/commit/b94470f07d2bd56f6396fe7fa269837c973af59d))
- **backend-native:** Use the lowest granularity for data-transform td with granularity fallback ([#9640](https://github.com/cube-js/cube/issues/9640)) ([59210da](https://github.com/cube-js/cube/commit/59210da8c3edabaced373b83679bac17b0843ef4))
- **cubesql:** Split meta on `CAST` over `__user` column ([#9690](https://github.com/cube-js/cube/issues/9690)) ([1685c1b](https://github.com/cube-js/cube/commit/1685c1b6dc6331855a9cd6e41ee4c7de8e185a8e))

### Features

- **server:** Add server config for headers and keep alive timeouts ([#9309](https://github.com/cube-js/cube/issues/9309)) ([8fd2c42](https://github.com/cube-js/cube/commit/8fd2c42c708c0be6863197849cfd3f10b7e51215))

## [1.3.22](https://github.com/cube-js/cube/compare/v1.3.21...v1.3.22) (2025-06-18)

### Bug Fixes

- **client-core:** Fix for the issue with Generated SQL tab in playground ([#9675](https://github.com/cube-js/cube/issues/9675)) ([17570d4](https://github.com/cube-js/cube/commit/17570d42a70292a58baba963fd3f8106816a2824))
- **cubejs-cli:** Fix validate command ([#9666](https://github.com/cube-js/cube/issues/9666)) ([b2bc99f](https://github.com/cube-js/cube/commit/b2bc99f3f29a8ba3ad1b07ded6379881668f596a))
- **cubeorchestrator:** Fix serialization of link type format for dimension ([#9649](https://github.com/cube-js/cube/issues/9649)) ([267ce43](https://github.com/cube-js/cube/commit/267ce4374a549b970cef399743a0009f3deb4a35))
- **questdb-driver:** Fix invalid QuestDB timestamp floor year unit ([#9678](https://github.com/cube-js/cube/issues/9678)) ([33012b1](https://github.com/cube-js/cube/commit/33012b1d20a54d63c24f20f7538d2bf504fd24ef))
- **schema-compiler:** Case insensitive filter for ClickHouse ([#9373](https://github.com/cube-js/cube/issues/9373)) ([273d277](https://github.com/cube-js/cube/commit/273d277e1058feff36796c48cf0fb315a8211ced))
- **schema-compiler:** Fix Access Policy inheritance ([#9648](https://github.com/cube-js/cube/issues/9648)) ([896af5e](https://github.com/cube-js/cube/commit/896af5eaeccec00c88463fa518e98bf374acdc9b))
- **tesseract:** Fix rolling window external pre-aggregation ([#9625](https://github.com/cube-js/cube/issues/9625)) ([aae3b05](https://github.com/cube-js/cube/commit/aae3b05f49222009f57e407c52d7288bb33b9b8a))
- **tesseract:** Fix rolling window with few time dimensions, filter_group in segments and member expressions ([#9673](https://github.com/cube-js/cube/issues/9673)) ([98d334b](https://github.com/cube-js/cube/commit/98d334bb8ee4debe49b428c92581f63596f3f56c))
- **tesseract:** Fix typo in interval ([#9680](https://github.com/cube-js/cube/issues/9680)) ([9b75d99](https://github.com/cube-js/cube/commit/9b75d99a08abfd54a075e61a3040e14c034a5169))
- **tesseract:** Handle JS exceptions in Rust with safe call ([#9677](https://github.com/cube-js/cube/issues/9677)) ([bb6d655](https://github.com/cube-js/cube/commit/bb6d6557b7c39267660dd3ae59ff341881c41a4b))

### Features

- **duckdb-driver:** Add support for using default credential provider chain for duckdb s3 access ([#9679](https://github.com/cube-js/cube/issues/9679)) ([89f54e9](https://github.com/cube-js/cube/commit/89f54e91af72e5d671268472d3ff04ebb841d1ed))
- **prestodb-driver, trino-driver:** Support dbUseSelectTestConnection flag ([#9663](https://github.com/cube-js/cube/issues/9663)) ([97b6bb4](https://github.com/cube-js/cube/commit/97b6bb43b9f3dd7209a8aa164680be76dcfc9f45))
- **schema-compiler:** Add support for time dimensions with granularities in multi-stage measures add_group_by ([#9657](https://github.com/cube-js/cube/issues/9657)) ([6700b43](https://github.com/cube-js/cube/commit/6700b432cc22d71b4b8ef650e835ba0cb33cf91c))

## [1.3.21](https://github.com/cube-js/cube/compare/v1.3.20...v1.3.21) (2025-06-10)

### Bug Fixes

- **client-vue3:** Prevent heuristic call when initial query is empty in computed property validateQuery ([#9656](https://github.com/cube-js/cube/issues/9656)) ([622b266](https://github.com/cube-js/cube/commit/622b26601e6bde3f15fb1cfc5ff53daff5cb6ed9))
- Report more accurate time to APM on heavy used deployments ([#9667](https://github.com/cube-js/cube/issues/9667)) ([a900c78](https://github.com/cube-js/cube/commit/a900c787d3724ebdd241cb0e4f4562e37f81ce14))
- **schema-compiler:** Fix pre-aggregation for time dimension matching ([#9669](https://github.com/cube-js/cube/issues/9669)) ([0914e1e](https://github.com/cube-js/cube/commit/0914e1ed89b7d1cf8f3bf8dc19858aeaf4b779a7))

## [1.3.20](https://github.com/cube-js/cube/compare/v1.3.19...v1.3.20) (2025-06-06)

### Bug Fixes

- **cubesql:** Fix Tableau relative dates ([#9641](https://github.com/cube-js/cube/issues/9641)) ([18ec4fc](https://github.com/cube-js/cube/commit/18ec4fc4fcd9ea94799241dc3f8ce9c7ac531b4a))
- **cubestore:** Delete old metastore snapshots in batches, after updating metastore-current ([#9647](https://github.com/cube-js/cube/issues/9647)) ([1104bde](https://github.com/cube-js/cube/commit/1104bde3307b4e5fd941c731305d8faa67ac803f))
- **schema-compiler:** Support for `export function xxx()` during transpilation ([#9645](https://github.com/cube-js/cube/issues/9645)) ([9bcb5a1](https://github.com/cube-js/cube/commit/9bcb5a16ae0c3f1b36d80ce4fcdfab20586b0b8a))
- **tesseract:** Removed unnecessary pushdown of measure filters in multistage queries ([#9650](https://github.com/cube-js/cube/issues/9650)) ([265580f](https://github.com/cube-js/cube/commit/265580f59437ff762118e79e469e157da548d4aa))

### Features

- **cubesql:** Support `PERCENTILE_CONT` SQL push down ([#8697](https://github.com/cube-js/cube/issues/8697)) ([577a09f](https://github.com/cube-js/cube/commit/577a09f498085ca5a7950467e602dee54691e88e))
- **databricks-jdbc-driver:** Support M2M OAuth Authentication ([#9651](https://github.com/cube-js/cube/issues/9651)) ([71c1022](https://github.com/cube-js/cube/commit/71c10226f7c797e8b63df248fc704f2a2f1b7452))
- **prestodb-driver, trino-driver:** Support custom auth headers (JWT) ([#9660](https://github.com/cube-js/cube/issues/9660)) ([3219695](https://github.com/cube-js/cube/commit/32196950d2c54ce482f686ae61c978a0c375e2f8))
- **trino-driver:** Add special testConnection for Trino ([#9634](https://github.com/cube-js/cube/issues/9634)) ([ae10a76](https://github.com/cube-js/cube/commit/ae10a767636322a7fcff21ca6b648b4a0374aad2))

## [1.3.19](https://github.com/cube-js/cube/compare/v1.3.18...v1.3.19) (2025-06-02)

### Bug Fixes

- **client-vue3:** Avoid Maximum recursive updates exceeded ([5d5f7da](https://github.com/cube-js/cube/commit/5d5f7da1dd7ad69b0db6ce9f33973efcc08879c5))
- **cubesql:** Fix "Tracker memory shrink underflow" error ([#9624](https://github.com/cube-js/cube/issues/9624)) ([d3af150](https://github.com/cube-js/cube/commit/d3af1506d845276a5b7fd97c5d8543d2cf03a1e0))
- **cubesql:** Quote subquery joins alias in SQL push down to cube ([#9629](https://github.com/cube-js/cube/issues/9629)) ([89b00cf](https://github.com/cube-js/cube/commit/89b00cf76dfbbfd06f0412d6e80178f0fdb9f46c))

### Features

- **cubesql:** Support `date_trunc != literal date` filter ([#9627](https://github.com/cube-js/cube/issues/9627)) ([2b36aae](https://github.com/cube-js/cube/commit/2b36aae5e93f88f4cca6059067bee047c32f4d24))
- **cubesql:** Support round() function with two parameters ([#9594](https://github.com/cube-js/cube/issues/9594)) ([8cd1dfe](https://github.com/cube-js/cube/commit/8cd1dfec1b18b246ed8f24f4d7c33a91556a4afa))
- Expose aliasMember for hierarchy in View ([#9636](https://github.com/cube-js/cube/issues/9636)) ([737caab](https://github.com/cube-js/cube/commit/737caabf2a43bc28ea0ad90085f44ffbaa1b292b))
- **schema-compiler:** add quarter granularity support in SqliteQuery using CASE expression ([#9633](https://github.com/cube-js/cube/issues/9633)) ([c7ae936](https://github.com/cube-js/cube/commit/c7ae9365eaf333e995d2536101ebe7dec1daf16a))

## [1.3.18](https://github.com/cube-js/cube/compare/v1.3.17...v1.3.18) (2025-05-27)

### Bug Fixes

- **client:** Update main fields in package.json to cjs files, thanks @Graphmaxer ([#9620](https://github.com/cube-js/cube/issues/9620)) ([06f82e5](https://github.com/cube-js/cube/commit/06f82e5a68beb5471d676734cabefa23aeb8a701))
- **schema-compiler:** exclude time dimensions w/o granularities from select list in base queries ([#9614](https://github.com/cube-js/cube/issues/9614)) ([c9ebfbc](https://github.com/cube-js/cube/commit/c9ebfbca8a791ba917b20c691476a336f92374c7))

### Features

- **docker:** Security upgrade node from 22.14.0 to 22.16.0 ([#9602](https://github.com/cube-js/cube/issues/9602)) ([efbb709](https://github.com/cube-js/cube/commit/efbb709b6ac7d9cbb7a0403d9036ed1fc3d80b49))

## [1.3.17](https://github.com/cube-js/cube/compare/v1.3.16...v1.3.17) (2025-05-22)

### Bug Fixes

- **cubesql:** Do not merge time dimension ranges in "or" filter into date range ([#9609](https://github.com/cube-js/cube/issues/9609)) ([803998f](https://github.com/cube-js/cube/commit/803998fc8e1799719542d0611c82032473409e01))
- **cubestore:** Avoid empty result set with LocalDirRemoteFs:list_with_metadata on Windows ([#9598](https://github.com/cube-js/cube/issues/9598)) ([7b4e4ad](https://github.com/cube-js/cube/commit/7b4e4ad151274a16cd34154f1cad5bd048884bde))
- **schema-compiler:** Avoid mutating context on first occasion of TD with granularity ([#9592](https://github.com/cube-js/cube/issues/9592)) ([93027d8](https://github.com/cube-js/cube/commit/93027d8bcb7f0e76d25679aeccad446ee9d265ad))
- **schema-compiler:** Fix rolling window queries with expressions from SQL API ([#9603](https://github.com/cube-js/cube/issues/9603)) ([43f47d8](https://github.com/cube-js/cube/commit/43f47d890a5c17416dd818b1712d54cb958ee95c))

### Features

- **snowflake-driver:** Add support for export Buckets with paths ([#9587](https://github.com/cube-js/cube/issues/9587)) ([e1f22da](https://github.com/cube-js/cube/commit/e1f22daac005170f354a4f69ecdfa27273350d87))

## [1.3.16](https://github.com/cube-js/cube/compare/v1.3.15...v1.3.16) (2025-05-19)

### Bug Fixes

- **backend-native:** Fix load response serialization (that breaks cubeApi further processing) ([#9575](https://github.com/cube-js/cube/issues/9575)) ([4b87403](https://github.com/cube-js/cube/commit/4b8740318300584809ff70e280a41dc981be948e))
- **schema-compiler:** Collect join hints from subquery join conditions ([#9554](https://github.com/cube-js/cube/issues/9554)) ([cbf0bfd](https://github.com/cube-js/cube/commit/cbf0bfddafc8629ce7d09d9b73d8b60da6d7bafb))

### Features

- **clickhouse-driver:** Add support for S3 Bucket with paths ([#9585](https://github.com/cube-js/cube/issues/9585)) ([2e47147](https://github.com/cube-js/cube/commit/2e4714721ef166b76432bfdd374db57bff886b7b))
- **client-core:** Add signal option to support abort fetch ([#9539](https://github.com/cube-js/cube/issues/9539)) ([2536dfc](https://github.com/cube-js/cube/commit/2536dfcd993055ad1cf94ae698265cd58b6be59f))
- **cubesql:** Push down `DATE_TRUNC` expressions as member expressions with granularity ([#9583](https://github.com/cube-js/cube/issues/9583)) ([b9c97cd](https://github.com/cube-js/cube/commit/b9c97cd6c169b6d359575649bd845f735ff1a516))
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

### Features

- introduce "protocol" and "method" props for request param in checkSqlAuth ([#9525](https://github.com/cube-js/cube/issues/9525)) ([401a845](https://github.com/cube-js/cube/commit/401a84584f418e2b4bdabe13766ac213646e0924))

## [1.3.12](https://github.com/cube-js/cube/compare/v1.3.11...v1.3.12) (2025-05-08)

### Bug Fixes

- **query-orchestrator:** Correct local date parsing for partition start/end queries ([#9543](https://github.com/cube-js/cube/issues/9543)) ([20dd8ad](https://github.com/cube-js/cube/commit/20dd8ad10f6ad0fc2538aecd0ff82cd43f6ff680))
- **schema-compiler:** Fix filtering by time measure ([#9544](https://github.com/cube-js/cube/issues/9544)) ([00a589f](https://github.com/cube-js/cube/commit/00a589fa3fb324ee3dde36fead0de7ced3d19e5b))
- **schema-compiler:** Fix rendered references for preaggregations with join paths ([#9528](https://github.com/cube-js/cube/issues/9528)) ([98ef928](https://github.com/cube-js/cube/commit/98ef928ece7385c2b41a359dfe0ebcc78dfaf8ee))
- **schema-compiler:** Fix view queries with timeShift members ([#9556](https://github.com/cube-js/cube/issues/9556)) ([ef3a04b](https://github.com/cube-js/cube/commit/ef3a04b28c9e0b804a2a24fb4046e3d4755e8abe))
- **schema-compiler:** Reject pre-agg if measure is unmultiplied in query but multiplied in pre-agg ([#9541](https://github.com/cube-js/cube/issues/9541)) ([f094d09](https://github.com/cube-js/cube/commit/f094d09e25b606001cbb699b82427dc43f19a6dc))
- Throw a user-friendly error on an empty cube.py file ([#9550](https://github.com/cube-js/cube/issues/9550)) ([d69222c](https://github.com/cube-js/cube/commit/d69222cecdf4b3bdfebfaaadc18bbd665d005757))

### Features

- **cubestore:** Add `XIRR` aggregate function to Cube Store ([#9520](https://github.com/cube-js/cube/issues/9520)) ([785142d](https://github.com/cube-js/cube/commit/785142d1c8ecc89cadaa7696c9f58b34115d929b))
- **schema-compiler:** Allow one measure timeShift without time dimension ([#9545](https://github.com/cube-js/cube/issues/9545)) ([320484f](https://github.com/cube-js/cube/commit/320484fc03f0c49efe0ada2ac9d3dce443011ab9))
- **tesseract:** Basic pre-aggregations support ([#9434](https://github.com/cube-js/cube/issues/9434)) ([1deddcc](https://github.com/cube-js/cube/commit/1deddcc4e37795bf2f775770798acefdbeb24c71))

## [1.3.11](https://github.com/cube-js/cube/compare/v1.3.10...v1.3.11) (2025-05-05)

### Features

- **cubesql:** Data source per member ([#9537](https://github.com/cube-js/cube/issues/9537)) ([c0be00c](https://github.com/cube-js/cube/commit/c0be00cd4e5239b52116e38e0f5bf8d846e57090))
- **snowflake-driver:** Upgrade snowflake-sdk from 2.0.3 to 2.0.4 ([#9527](https://github.com/cube-js/cube/issues/9527)) ([8cc6346](https://github.com/cube-js/cube/commit/8cc6346a0daaf35706155671f34bf2cb387b6ad7))

## [1.3.10](https://github.com/cube-js/cube/compare/v1.3.9...v1.3.10) (2025-05-01)

### Bug Fixes

- **cubesql:** Disable filter pushdown over Filter(CrossJoin) ([#9474](https://github.com/cube-js/cube/issues/9474)) ([940c30f](https://github.com/cube-js/cube/commit/940c30f81c0d0f73bcc58bc80d3b673d484cc067))

### Features

- **backend-native:** Allow importing modules from python files within `cube.py` and `globals.py` ([#9490](https://github.com/cube-js/cube/issues/9490)) ([b8ebf15](https://github.com/cube-js/cube/commit/b8ebf15294faa118303112a2d94990dac884e9dd))
- **cubesql:** SQL push down complex window expressions ([#8788](https://github.com/cube-js/cube/issues/8788)) ([2b1bb28](https://github.com/cube-js/cube/commit/2b1bb284e1413a13f96df62fe712c61aee32fd68))
- **cubesql:** Support trivial casts in member pushdown ([#9480](https://github.com/cube-js/cube/issues/9480)) ([85c27a9](https://github.com/cube-js/cube/commit/85c27a928a773245163406d0262c7a5bc69c69bb))
- **databricks-jdbc-driver:** Implement connection checking without waking up SQL warehouse ([#9529](https://github.com/cube-js/cube/issues/9529)) ([044341e](https://github.com/cube-js/cube/commit/044341ee1812c3a2c95c57fb20a2efeaa4e8d803))

## [1.3.9](https://github.com/cube-js/cube/compare/v1.3.8...v1.3.9) (2025-04-28)

### Bug Fixes

- **schema-compiler:** Fix non-match for exact custom granularity pre-aggregation ([#9517](https://github.com/cube-js/cube/issues/9517)) ([313a606](https://github.com/cube-js/cube/commit/313a606e1aedb7dda31a88ea78e71e106a2fa884))

### Features

- **schema-compiler:** Support adding pre-aggregations in Rollup Designer for YAML-based models ([#9500](https://github.com/cube-js/cube/issues/9500)) ([1cfe4c2](https://github.com/cube-js/cube/commit/1cfe4c2dd1cc0fe738590abb39fe2485899fc51e))
- **schema-compiler:** Support view extends ([#9507](https://github.com/cube-js/cube/issues/9507)) ([e68530a](https://github.com/cube-js/cube/commit/e68530a51dd9fa58dd85dab0d593a034000a6ed3))
- **tesseract:** Logical plan ([#9497](https://github.com/cube-js/cube/issues/9497)) ([4098c28](https://github.com/cube-js/cube/commit/4098c284f6bebf65125fffd293dd8e6db05fc4ae))

## [1.3.8](https://github.com/cube-js/cube/compare/v1.3.7...v1.3.8) (2025-04-24)

### Features

- **cubesql:** Add `XIRR` aggregate function ([#9508](https://github.com/cube-js/cube/issues/9508)) ([c7fb71b](https://github.com/cube-js/cube/commit/c7fb71bd4023b37635bfb82c2aac523337c2f8be))

## [1.3.7](https://github.com/cube-js/cube/compare/v1.3.6...v1.3.7) (2025-04-23)

### Bug Fixes

- **schema-compiler:** Fix not working timeshift in views ([#9504](https://github.com/cube-js/cube/issues/9504)) ([9319b0d](https://github.com/cube-js/cube/commit/9319b0df50a4026ceb91732de123a21e8d788b39))

### Features

- **schema-compiler:** Custom granularities support for Amazon Athena/Presto dialect ([#9472](https://github.com/cube-js/cube/issues/9472)) ([4c1520e](https://github.com/cube-js/cube/commit/4c1520e90207469cb4d6be41b28e1c06bb2d0646))
- **schema-compiler:** Support custom granularities with Year-Month intervals for AWS Redshift dialect ([#9489](https://github.com/cube-js/cube/issues/9489)) ([2fc53f2](https://github.com/cube-js/cube/commit/2fc53f2d10e94ba6ac1b40bfce956167b89b8caf))
- **schema-compiler:** Support overriding `title`, `description`, `meta`, and `format` on view members ([#9496](https://github.com/cube-js/cube/issues/9496)) ([e0179c5](https://github.com/cube-js/cube/commit/e0179c5644952c6ebe41ab4f1f12784491036bd0))

## [1.3.6](https://github.com/cube-js/cube/compare/v1.3.5...v1.3.6) (2025-04-22)

### Bug Fixes

- **cubesql:** Fix SortPushDown pushing sort through joins ([#9464](https://github.com/cube-js/cube/issues/9464)) ([fed08e1](https://github.com/cube-js/cube/commit/fed08e1384cf6f666970472f5bd67feb5c86462e)), closes [/github.com/apache/datafusion/blob/7.0.0/datafusion/src/physical_plan/hash_join.rs#L282-L284](https://github.com//github.com/apache/datafusion/blob/7.0.0/datafusion/src/physical_plan/hash_join.rs/issues/L282-L284) [/github.com/apache/datafusion/blob/7.0.0/datafusion/src/physical_plan/cross_join.rs#L141-L143](https://github.com//github.com/apache/datafusion/blob/7.0.0/datafusion/src/physical_plan/cross_join.rs/issues/L141-L143)
- **cubesql:** Realias expressions when normalizing columns ([#9498](https://github.com/cube-js/cube/issues/9498)) ([32f9b79](https://github.com/cube-js/cube/commit/32f9b7993c97fc294ec92b54a587f27e4e29dda0))
- **schema-compiler:** Use join paths from pre-aggregation declaration instead of building join tree from scratch ([#9471](https://github.com/cube-js/cube/issues/9471)) ([cd7e9fd](https://github.com/cube-js/cube/commit/cd7e9fddca8fa1c31335adbba6e79c4032841cbb))

### Features

- **mssql-driver:** Return numeric result values as strings ([#9485](https://github.com/cube-js/cube/issues/9485)) ([52da601](https://github.com/cube-js/cube/commit/52da6010792985b610087040733f6da650369c1a))

## [1.3.5](https://github.com/cube-js/cube/compare/v1.3.4...v1.3.5) (2025-04-17)

### Bug Fixes

- **cubesql:** Disallow mixing measure and dimension filters in a single FilterOp ([#9486](https://github.com/cube-js/cube/issues/9486)) ([858f6d5](https://github.com/cube-js/cube/commit/858f6d55df7f4167c4eccce7b2825f36f74a8820))

## [1.3.4](https://github.com/cube-js/cube/compare/v1.3.3...v1.3.4) (2025-04-17)

### Bug Fixes

- **schema-compiler:** Make RefreshKeySql timezone-aware ([#9479](https://github.com/cube-js/cube/issues/9479)) ([fa2adb9](https://github.com/cube-js/cube/commit/fa2adb92e6fd87223f551b61d75f367dc8ea2524))

## [1.3.3](https://github.com/cube-js/cube/compare/v1.3.2...v1.3.3) (2025-04-16)

### Features

- **client-core:** Add fetch timeout and network retry options ([#9484](https://github.com/cube-js/cube/issues/9484)) ([b8de1a4](https://github.com/cube-js/cube/commit/b8de1a4869fde24fb98203b6039ec970f8258812))

## [1.3.2](https://github.com/cube-js/cube/compare/v1.3.1...v1.3.2) (2025-04-16)

### Bug Fixes

- **schema-compiler:** Allow calling cube functions in yaml python blocks ([#9473](https://github.com/cube-js/cube/issues/9473)) ([61ef9da](https://github.com/cube-js/cube/commit/61ef9da243e8219b8ba3ab7ccbf9f9e0403cba04))
- **schema-compiler:** Allow escaping of curly braces in yaml models ([#9469](https://github.com/cube-js/cube/issues/9469)) ([0a8cf07](https://github.com/cube-js/cube/commit/0a8cf07f20351a0e591b658f02f243510bf78eae))

### Features

- **duckdb-driver:** Add support for installing and loading DuckDB Community Extensions ([#9169](https://github.com/cube-js/cube/issues/9169)) ([c97f99a](https://github.com/cube-js/cube/commit/c97f99a27f2c95b1a58ea4863dfe0c4227e4c42d))

## [1.3.1](https://github.com/cube-js/cube/compare/v1.3.0...v1.3.1) (2025-04-14)

### Bug Fixes

- **schema-compiler:** Fix incorrect transpilation of yaml models ([#9465](https://github.com/cube-js/cube/issues/9465)) ([e732fa2](https://github.com/cube-js/cube/commit/e732fa2b8ead0ca605512611eb371b94e0238dec))

# [1.3.0](https://github.com/cube-js/cube/compare/v1.2.33...v1.3.0) (2025-04-11)

### Bug Fixes

- **schema-compiler:** Do not drop join hints when view member is not itself owned ([#9444](https://github.com/cube-js/cube/issues/9444)) ([b5ddd11](https://github.com/cube-js/cube/commit/b5ddd115a8ad066bc4399fc2608d1d99d3d863dc))
- **schema-compiler:** Fix cubes inheritance ([#9386](https://github.com/cube-js/cube/issues/9386)) ([409d74d](https://github.com/cube-js/cube/commit/409d74db8c5ac5a314e1e3e7b872586f538713d1))
- **schema-compiler:** Fix incorrect native bulk transpilation if there are no JS files ([#9453](https://github.com/cube-js/cube/issues/9453)) ([610820f](https://github.com/cube-js/cube/commit/610820fe15701ad0971b4d317bb112ec7765bb9b))
- **server:** Fix incorrect native result wrapper processing via webSockets ([#9458](https://github.com/cube-js/cube/issues/9458)) ([a4e76c4](https://github.com/cube-js/cube/commit/a4e76c4568bce4c63e5f131f3c7bf800f1cd45fc))

### Features

- **cubesql:** Remove bottom-up extraction completely ([#9183](https://github.com/cube-js/cube/issues/9183)) ([c528ebe](https://github.com/cube-js/cube/commit/c528ebe6b5700cc950a26530906436bd274c1942))
- **databricks-jdbc-driver:** Add export bucket support for Google Cloud Storage ([#9445](https://github.com/cube-js/cube/issues/9445)) ([a247bf9](https://github.com/cube-js/cube/commit/a247bf9ce073aea69ffb592e933fd365eee70fe1))
- **databricks-jdbc-driver:** Switch to the latest OSS Databricks JDBC driver ([#9450](https://github.com/cube-js/cube/issues/9450)) ([819edde](https://github.com/cube-js/cube/commit/819eddedc6024963b836d8090f6ca1c3ae1b5966))

## [1.2.33](https://github.com/cube-js/cube/compare/v1.2.32...v1.2.33) (2025-04-10)

### Bug Fixes

- **cubeorchestrator:** Fix incorrect null-values parsing from CubeStore Result ([#9443](https://github.com/cube-js/cube/issues/9443)) ([fcb5711](https://github.com/cube-js/cube/commit/fcb5711c824d062ea8cb55b6ef24c81f291f886a))
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
- **server-core:** Use LRU for OrchestratorStorage ([#9425](https://github.com/cube-js/cube/issues/9425)) ([ede37f8](https://github.com/cube-js/cube/commit/ede37f81efe7e23430be1f65f008f509365d94e6))

## [1.2.30](https://github.com/cube-js/cube/compare/v1.2.29...v1.2.30) (2025-04-04)

### Bug Fixes

- **tesseract:** Fix for multi fact queries ([#9421](https://github.com/cube-js/cube/issues/9421)) ([5547f50](https://github.com/cube-js/cube/commit/5547f5046c961966b582f76faf1377393ec8557a))

### Features

- **schema-compiler:** generate join sql using dimension refs when po… ([#9413](https://github.com/cube-js/cube/issues/9413)) ([8fc4f7c](https://github.com/cube-js/cube/commit/8fc4f7c4125889fcc24306256665d61ac30198c3))

## [1.2.29](https://github.com/cube-js/cube/compare/v1.2.28...v1.2.29) (2025-04-02)

### Bug Fixes

- **cubesql:** Penalize CrossJoins in favor of wrapper ([#9414](https://github.com/cube-js/cube/issues/9414)) ([a48963d](https://github.com/cube-js/cube/commit/a48963d03cb16e0dc3d110ae398fe3b05447209d))
- **schema-compiler:** Respect ungrouped alias in measures ([#9411](https://github.com/cube-js/cube/issues/9411)) ([f0829d7](https://github.com/cube-js/cube/commit/f0829d730c284a1e53c833cd53ccf8d37b08e095))
- **schema-compiler:** Use segments as-is in rollupPreAggregation ([#9391](https://github.com/cube-js/cube/issues/9391)) ([cb7b643](https://github.com/cube-js/cube/commit/cb7b6438da199daf0b07fb2dd6f5087ed259f71f))

## [1.2.28](https://github.com/cube-js/cube/compare/v1.2.27...v1.2.28) (2025-04-01)

### Bug Fixes

- **cubesql:** Allow more filters in CubeScan before aggregation pushdown ([#9409](https://github.com/cube-js/cube/issues/9409)) ([351ac7a](https://github.com/cube-js/cube/commit/351ac7aece72e7795f570f5582250206e3c0124e))
- **schema-compiler:** Fix BigQuery DATETIME_TRUNC() week processing ([#9380](https://github.com/cube-js/cube/issues/9380)) ([6c8564f](https://github.com/cube-js/cube/commit/6c8564ffc15e5e930fa2160be642ea3f3cb7b888))
- **schema-compiler:** Fix date alignment for week-based custom granularities ([#9405](https://github.com/cube-js/cube/issues/9405)) ([8b6c490](https://github.com/cube-js/cube/commit/8b6c4906e2c49fb2c04abc381f6fdb3105309eaf))
- **schema-compiler:** Fix model validation for aliased included members for views ([#9398](https://github.com/cube-js/cube/issues/9398)) ([f5e69f7](https://github.com/cube-js/cube/commit/f5e69f76e7268812aa776a20588875d85077b3bf))
- **schema-compiler:** Skip syntax check for transpiled files ([#9395](https://github.com/cube-js/cube/issues/9395)) ([c624e88](https://github.com/cube-js/cube/commit/c624e88d4793c917bb43c3099a7c9d128b517b8a))
- **tesseract:** Support rolling window with multiple granularities ([#9382](https://github.com/cube-js/cube/issues/9382)) ([6ff9331](https://github.com/cube-js/cube/commit/6ff9331729af59e45385608057fa0e99688cfcbe))

### Features

- **clickhouse-driver:** Allow to enable compression, thanks @Graphmaxer ([#9341](https://github.com/cube-js/cube/issues/9341)) ([78ac7b2](https://github.com/cube-js/cube/commit/78ac7b2d93896bae0903ba5f8e4f055165791962))
- **databricks-jdbc-driver:** Switch to the latest OSS Databricks JDBC driver ([#9376](https://github.com/cube-js/cube/issues/9376)) ([29fdd61](https://github.com/cube-js/cube/commit/29fdd615f7eb70690f5fc85897e0c93a5a0cc29a))
- **tesseract:** Custom granularities support ([#9400](https://github.com/cube-js/cube/issues/9400)) ([dc93e40](https://github.com/cube-js/cube/commit/dc93e4022d4c2741d0aafcdb1007a69c6181f232))
- **tesseract:** Support for rolling window without date range ([#9364](https://github.com/cube-js/cube/issues/9364)) ([fd94b02](https://github.com/cube-js/cube/commit/fd94b02b8bba286a67264057f9259769215239a9))

## [1.2.27](https://github.com/cube-js/cube/compare/v1.2.26...v1.2.27) (2025-03-25)

### Features

- **snowflake-driver:** Ability to use encrypted private keys for auth ([#9371](https://github.com/cube-js/cube/issues/9371)) ([ab54e28](https://github.com/cube-js/cube/commit/ab54e2817dc4e14f0e1ea3b1257b3a08072c954c))

## [1.2.26](https://github.com/cube-js/cube/compare/v1.2.25...v1.2.26) (2025-03-21)

### Bug Fixes

- **databricks-jdbc-driver:** Allow paths in s3/azure export buckets ([#9365](https://github.com/cube-js/cube/issues/9365)) ([f133737](https://github.com/cube-js/cube/commit/f1337379c977f920bc6c1864138b23e10616007d))
- **redshift-driver:** Use Redshift-specific schema query ([#9363](https://github.com/cube-js/cube/issues/9363)) ([beaf8eb](https://github.com/cube-js/cube/commit/beaf8eba22ac1657dc682ff04f7c670a8824d0ea)), closes [#3876](https://github.com/cube-js/cube/issues/3876)
- **schema-compiler:** Fix doubled calls to convertTz() for compound time dimensions with custom granularities ([#9369](https://github.com/cube-js/cube/issues/9369)) ([0dc8320](https://github.com/cube-js/cube/commit/0dc83209ee8d4d38ebaa6be6c48a07e146c4a7b7))

## [1.2.25](https://github.com/cube-js/cube/compare/v1.2.24...v1.2.25) (2025-03-20)

### Bug Fixes

- **backend-native:** Fix result wrapper parsing ([#9361](https://github.com/cube-js/cube/issues/9361)) ([1b70644](https://github.com/cube-js/cube/commit/1b70644ef81e97864447d57e4b627f7f28d8be34))
- **schema-compiler:** Handle measures with dimension-only member expressions ([#9335](https://github.com/cube-js/cube/issues/9335)) ([2f7a128](https://github.com/cube-js/cube/commit/2f7a1288108ea3a62b295c08dcf756bfec76d784))
- **schema-compiler:** support unary operators in DAP filters ([#9366](https://github.com/cube-js/cube/issues/9366)) ([fff8af2](https://github.com/cube-js/cube/commit/fff8af2bfa25098e8ed20b4e4848f3926bba70b8))

### Features

- **duckdb-driver:** Fix numeric filter comparisons in DuckDB ([#9328](https://github.com/cube-js/cube/issues/9328)) ([969508d](https://github.com/cube-js/cube/commit/969508dd33984d0928f514ff07edbc50a0c949e3)), closes [#9281](https://github.com/cube-js/cube/issues/9281) [#9281](https://github.com/cube-js/cube/issues/9281)
- **server-core,api-gateway:** Allow manual rebuilding pre-aggregation partitions within date Range ([#9342](https://github.com/cube-js/cube/issues/9342)) ([b5701e3](https://github.com/cube-js/cube/commit/b5701e35a92769e23acdbb2fe72549731cd21aba))

## [1.2.24](https://github.com/cube-js/cube/compare/v1.2.23...v1.2.24) (2025-03-18)

### Bug Fixes

- **cubesql:** Disable projection_push_down DF optimizer ([#9356](https://github.com/cube-js/cube/issues/9356)) ([a15442c](https://github.com/cube-js/cube/commit/a15442ca30071bcefe44bf49ddfe1d7f7fb870bd))

### Features

- Implement disable_post_processing in /v1/sql ([#9331](https://github.com/cube-js/cube/issues/9331)) ([c336b10](https://github.com/cube-js/cube/commit/c336b10c6fb90ee2fd3b2852da1671e7a35980bc))

## [1.2.23](https://github.com/cube-js/cube/compare/v1.2.22...v1.2.23) (2025-03-17)

### Features

- **docker:** Security upgrade node from 20.17.0 to 20.19.0 ([#9350](https://github.com/cube-js/cube/issues/9350)) ([6cacff9](https://github.com/cube-js/cube/commit/6cacff9129afa423b87fac783723a886c69878cd))

## [1.2.22](https://github.com/cube-js/cube/compare/v1.2.21...v1.2.22) (2025-03-14)

### Bug Fixes

- **cubesql:** Functions without arguments alias as plain function name ([#9338](https://github.com/cube-js/cube/issues/9338)) ([de10c23](https://github.com/cube-js/cube/commit/de10c233bf84ef11eb0af272ea296881651dafd1))

### Features

- **tesseract:** Segments and MemberExpressions segments support ([#9336](https://github.com/cube-js/cube/issues/9336)) ([b2c03e7](https://github.com/cube-js/cube/commit/b2c03e7644f954ba29201b4c98de35ded7eebe79))

## [1.2.21](https://github.com/cube-js/cube/compare/v1.2.20...v1.2.21) (2025-03-11)

### Bug Fixes

- **cubejs-native:** cubesql query logger span_id ([#9325](https://github.com/cube-js/cube/issues/9325)) ([568d306](https://github.com/cube-js/cube/commit/568d306a9b97672caf69543077e7863fc773af41))

### Features

- **cubesql:** Move dimensions-only projections to dimensions for push-to-Cube wrapper ([#9318](https://github.com/cube-js/cube/issues/9318)) ([ca62aa0](https://github.com/cube-js/cube/commit/ca62aa0e747b88c2754f3a758c7a959ee52b0c81))

## [1.2.20](https://github.com/cube-js/cube/compare/v1.2.19...v1.2.20) (2025-03-10)

### Bug Fixes

- Allow config http protocol to be passed to clickhouse driver. ([#9195](https://github.com/cube-js/cube/issues/9195)) ([0c15b01](https://github.com/cube-js/cube/commit/0c15b016359037281da856d72846c19b1bfb39aa))

### Features

- Add SQL queries support in /v1/sql endpoint ([#9301](https://github.com/cube-js/cube/issues/9301)) ([7eba663](https://github.com/cube-js/cube/commit/7eba6639164ed3cffef4c6b68f5bc8d9a7cc740e))

## [1.2.19](https://github.com/cube-js/cube/compare/v1.2.18...v1.2.19) (2025-03-08)

### Features

- **pinot-driver:** Add enableNullHandling to query options using env var ([#9310](https://github.com/cube-js/cube/issues/9310)) ([df763cc](https://github.com/cube-js/cube/commit/df763cc9a375eb656d6ac4dc10adbd9a68082a0e))

## [1.2.18](https://github.com/cube-js/cube/compare/v1.2.17...v1.2.18) (2025-03-06)

### Bug Fixes

- **cubesql:** Calculate proper limit and offset for CubeScan in nested limits case ([#8924](https://github.com/cube-js/cube/issues/8924)) ([0e95f18](https://github.com/cube-js/cube/commit/0e95f18bede4d2c79c8c3b729004715aa8aa7a58))
- **cubesql:** Make cube join check stricter ([#9043](https://github.com/cube-js/cube/issues/9043)) ([feaf03b](https://github.com/cube-js/cube/commit/feaf03b5a0199abda5e6d1f41ae5487c509a1276))
- **query-orchestrator:** Fix improper pre-aggregation buildRange construction for non UTC timezones ([#9284](https://github.com/cube-js/cube/issues/9284)) ([ef12d8d](https://github.com/cube-js/cube/commit/ef12d8d02702df7dcc1e6531c1b0aee6afa576ef))
- **tesseract:** Don't generate COALESCE with single argument ([#9300](https://github.com/cube-js/cube/issues/9300)) ([3174be1](https://github.com/cube-js/cube/commit/3174be17cd1d9ba6aa1c0ec3ccbd0c5426342dc0))
- **tesseract:** fix wrong default alias for symbols with digits in name ([#9299](https://github.com/cube-js/cube/issues/9299)) ([329d228](https://github.com/cube-js/cube/commit/329d2289e115a5f71714755323a8b8a92a260519))

### Features

- **cubesql:** Support multiple columns on each side in ungrouped-grouped join condition ([#9282](https://github.com/cube-js/cube/issues/9282)) ([e25d5c1](https://github.com/cube-js/cube/commit/e25d5c1ce686e743c67fce45ae596270c9e1ddbe))

## [1.2.17](https://github.com/cube-js/cube/compare/v1.2.16...v1.2.17) (2025-03-05)

### Bug Fixes

- **api-gateway:** Support proxy when fetching jwk for token validation ([#9286](https://github.com/cube-js/cube/issues/9286)) ([60870c8](https://github.com/cube-js/cube/commit/60870c83d704fdbdb496300023daf188a53990f9))
- **cubesql:** Use pushdown-pullup scheme for FilterSimplifyReplacer ([#9278](https://github.com/cube-js/cube/issues/9278)) ([ab5a64e](https://github.com/cube-js/cube/commit/ab5a64e9cc1b0db5223720f35a26a57d23877332))
- **docker:** apt-get install ca-certificates in latest and local Dockerfiles ([1fca9ee](https://github.com/cube-js/cube/commit/1fca9ee3e9fdac4a6f0dcfad832afc7a5b398080))
- **query-orchestrator:** Fix dropping temp tables during pre-agg creation ([#9295](https://github.com/cube-js/cube/issues/9295)) ([eb3d980](https://github.com/cube-js/cube/commit/eb3d980aee42b3077148cc93c56034b0471d2a33))
- **schema-compiler:** Fix ORDER BY clause generation for queries with td with filters ([#9296](https://github.com/cube-js/cube/issues/9296)) ([1c8ce4f](https://github.com/cube-js/cube/commit/1c8ce4f475a0327fd4dc8fedf24ac9979aa2bffd))

### Features

- **duckdb-driver:** Add `databasePath` and `motherDuckToken` config options ([6f43138](https://github.com/cube-js/cube/commit/6f431388d0c207923ea990bab55c22c79d217e5b))

## [1.2.16](https://github.com/cube-js/cube/compare/v1.2.15...v1.2.16) (2025-03-04)

### Bug Fixes

- **cubejs-native:** sql over http drop sessions, correct error ([#9297](https://github.com/cube-js/cube/issues/9297)) ([6fad670](https://github.com/cube-js/cube/commit/6fad670c722fd91e29d950a36659ac4630cef64a))

## [1.2.15](https://github.com/cube-js/cube/compare/v1.2.14...v1.2.15) (2025-03-03)

### Bug Fixes

- **cubejs-client-ngx:** Configure package-level Lerna publish directory for Angular ([#9189](https://github.com/cube-js/cube/issues/9189)) ([ef799f5](https://github.com/cube-js/cube/commit/ef799f58c21a760a7c51e03803343568634620ee))

### Features

- **schema-compiler:** auto include hierarchy dimensions ([#9288](https://github.com/cube-js/cube/issues/9288)) ([162c5b4](https://github.com/cube-js/cube/commit/162c5b48f6ef489bf9f4bb95a1d4bca168b836c9))
- **tesseract:** Pre-aggregation planning fallback ([#9230](https://github.com/cube-js/cube/issues/9230)) ([08650e2](https://github.com/cube-js/cube/commit/08650e241e2aa85705e3c68e4993098267f89c82))

## [1.2.14](https://github.com/cube-js/cube/compare/v1.2.13...v1.2.14) (2025-02-28)

### Features

- **server-core:** add fastReloadEnabled option to CompilerApi ([#9289](https://github.com/cube-js/cube/issues/9289)) ([122a3a3](https://github.com/cube-js/cube/commit/122a3a30e526f122d2bf2c0661f2b32fa070e824))

## [1.2.13](https://github.com/cube-js/cube/compare/v1.2.12...v1.2.13) (2025-02-26)

### Bug Fixes

- **cubesql:** Split `__user` WHERE predicate into separate filter node ([#8812](https://github.com/cube-js/cube/issues/8812)) ([83baf7b](https://github.com/cube-js/cube/commit/83baf7bf5f83108fd6c3dd134a8739968e781f92))

## [1.2.12](https://github.com/cube-js/cube/compare/v1.2.11...v1.2.12) (2025-02-26)

### Bug Fixes

- **api-gateway:** data scope check ([#9264](https://github.com/cube-js/cube/issues/9264)) ([75095e1](https://github.com/cube-js/cube/commit/75095e1f86839a3f459d1050226b319d3ed2410f))
- **cubesql:** Generate typed null literals ([#9238](https://github.com/cube-js/cube/issues/9238)) ([1dfa10d](https://github.com/cube-js/cube/commit/1dfa10d7128841f24c5d94cd1c5bdd2c742ff9de))
- **cubesql:** Match CubeScan timestamp literal types to member types ([#9275](https://github.com/cube-js/cube/issues/9275)) ([4a4e82b](https://github.com/cube-js/cube/commit/4a4e82ba602fc024a262a22ac65e3fcb7a4bba5c))
- **native:** Jinja - pass kwargs correctly into Python ([#9276](https://github.com/cube-js/cube/issues/9276)) ([9d1c3f8](https://github.com/cube-js/cube/commit/9d1c3f8cfef5fd9383ee5fa69e79328b5c739231))
- **server-core:** Handle empty query in getSqlGenerator ([#9270](https://github.com/cube-js/cube/issues/9270)) ([350a438](https://github.com/cube-js/cube/commit/350a438d33d19ce77f33feb4f30910e3087d36e6))

## [1.2.11](https://github.com/cube-js/cube/compare/v1.2.10...v1.2.11) (2025-02-25)

### Bug Fixes

- **cubesql:** Break cost symmetry for (non)-push-to-Cube WrappedSelect ([#9155](https://github.com/cube-js/cube/issues/9155)) ([2c0e443](https://github.com/cube-js/cube/commit/2c0e443dc18379490e35a3d83b3888f66e12ade0))
- **cubesql:** Generate proper projection wrapper for duplicated members in CubeScanNode ([#9233](https://github.com/cube-js/cube/issues/9233)) ([aba6430](https://github.com/cube-js/cube/commit/aba643082acc440cf5b3fe9828c2c38ac1a833c9))

## [1.2.10](https://github.com/cube-js/cube/compare/v1.2.9...v1.2.10) (2025-02-24)

### Bug Fixes

- **redshift-driver:** Fix empty tableColumnTypes for external (Spectrum) tables ([#9251](https://github.com/cube-js/cube/issues/9251)) ([fa318a1](https://github.com/cube-js/cube/commit/fa318a144ed6ef484e2ac101cd71ab6ab6ca7abf))
- **schema-compiler:** Fix sql generation for rolling_window queries with multiple time dimensions ([#9124](https://github.com/cube-js/cube/issues/9124)) ([52a664e](https://github.com/cube-js/cube/commit/52a664e4d0643d78464f75cc48c4a1f686455ebe))

### Features

- **cubejs-server-core:** support proxy in http agent transport ([#9263](https://github.com/cube-js/cube/issues/9263)) ([89c74ce](https://github.com/cube-js/cube/commit/89c74cec2be96f63f51990b5eec59096749e967b))

## [1.2.9](https://github.com/cube-js/cube/compare/v1.2.8...v1.2.9) (2025-02-21)

### Bug Fixes

- **schema-compiler:** Fix allowNonStrictDateRangeMatch pre-agg match for dateRange queries without granularities ([#9258](https://github.com/cube-js/cube/issues/9258)) ([00fe682](https://github.com/cube-js/cube/commit/00fe6825418e7ad6681b0856fee11175dfcc649e))

## [1.2.8](https://github.com/cube-js/cube/compare/v1.2.7...v1.2.8) (2025-02-21)

### Bug Fixes

- **backend-native:** Handle closed channel on Rust side ([#9242](https://github.com/cube-js/cube/issues/9242)) ([1203291](https://github.com/cube-js/cube/commit/120329146046f2bffc0b8ff252dcee1cc285d1ef))

### Features

- **cubeclient:** Add `short_title` to dimensions and measures ([#9256](https://github.com/cube-js/cube/issues/9256)) ([584b3dc](https://github.com/cube-js/cube/commit/584b3dcefedb7c01b849e7f18a59445bd3542b7e))

## [1.2.7](https://github.com/cube-js/cube/compare/v1.2.6...v1.2.7) (2025-02-20)

### Bug Fixes

- **schema-compiler:** hierarchies to respect prefix value ([#9239](https://github.com/cube-js/cube/issues/9239)) ([19d1b33](https://github.com/cube-js/cube/commit/19d1b33b7df6a98198aadfa25e290fd33b89ad33))

### Reverts

- **schema-compiler:** Revert breaking changes in models transpilation ([#9240](https://github.com/cube-js/cube/issues/9240)) ([28bca42](https://github.com/cube-js/cube/commit/28bca42a1bb92fff5895ddfe4946601c0ebc2746))

## [1.2.6](https://github.com/cube-js/cube/compare/v1.2.5...v1.2.6) (2025-02-18)

### Bug Fixes

- **api-gateway:** Fix CompareDateRange queries native transformations ([#9232](https://github.com/cube-js/cube/issues/9232)) ([9c6153d](https://github.com/cube-js/cube/commit/9c6153d1e958e42162acb5af5964e9792a7f0433))
- **schema-compiler:** Correct models transpilation in native in multitenant environments ([#9234](https://github.com/cube-js/cube/issues/9234)) ([84f90c0](https://github.com/cube-js/cube/commit/84f90c07ee3827e6f3652dd6c9fab0993ecc8150))

### Features

- **schema-compiler:** Boost models transpilation 10-13x times (using SWC instead of Babel) ([#9225](https://github.com/cube-js/cube/issues/9225)) ([2dd9a4a](https://github.com/cube-js/cube/commit/2dd9a4a5ba0f178e304befc476609fc30ada8975))

### Performance Improvements

- **cubesql:** Avoid allocations in MetaContext methods ([#9228](https://github.com/cube-js/cube/issues/9228)) ([ba753d0](https://github.com/cube-js/cube/commit/ba753d0d43927b50d5cf8faf5f09de3e53bec3db))

## [1.2.5](https://github.com/cube-js/cube/compare/v1.2.4...v1.2.5) (2025-02-13)

### Bug Fixes

- **cubesql:** Fix SELECT DISTINCT on pushdown ([#9144](https://github.com/cube-js/cube/issues/9144)) ([6483f66](https://github.com/cube-js/cube/commit/6483f66c826ad9a638af8770850219d923b3e7d9))

### Features

- Upgrade rust to 1.84.1 (stable) ([#9222](https://github.com/cube-js/cube/issues/9222)) ([cfc95b5](https://github.com/cube-js/cube/commit/cfc95b5e5601166d2a0aa388a211d4be66630cd8))

## [1.2.4](https://github.com/cube-js/cube/compare/v1.2.3...v1.2.4) (2025-02-11)

### Bug Fixes

- **cubejs-playground:** update query builder ([#9201](https://github.com/cube-js/cube/issues/9201)) ([f8e523b](https://github.com/cube-js/cube/commit/f8e523bbf815b2fb3fb8eb036d04cc3bf4db206d))

### Features

- **cubesql:** Add projection flattening rule ([#9165](https://github.com/cube-js/cube/issues/9165)) ([8cfb253](https://github.com/cube-js/cube/commit/8cfb2530264b856bf128d391829b18566d8dc057))
- **cubesql:** Allow providing API type when getting load request meta ([#9202](https://github.com/cube-js/cube/issues/9202)) ([ae5d977](https://github.com/cube-js/cube/commit/ae5d9770a1c00326c446e7ec40bd3f13ca0ce8f4))
- **server-core:** Introduce CUBEJS_REFRESH_WORKER_CONCURRENCY env and update default concurrency settings for drivers ([#9168](https://github.com/cube-js/cube/issues/9168)) ([7ef6282](https://github.com/cube-js/cube/commit/7ef628273905d47996b108862a52dde89b9525e3))

## [1.2.3](https://github.com/cube-js/cube/compare/v1.2.2...v1.2.3) (2025-02-06)

### Bug Fixes

- **cubejs-backend-native:** support context_to_cube_store_router_id ([#9200](https://github.com/cube-js/cube/issues/9200)) ([f93edd3](https://github.com/cube-js/cube/commit/f93edd3713c58ad62ca8daee034639b0f10a0282))

## [1.2.2](https://github.com/cube-js/cube/compare/v1.2.1...v1.2.2) (2025-02-06)

### Bug Fixes

- **dremio-driver:** Correct dremio date interval functions ([#9193](https://github.com/cube-js/cube/issues/9193)) ([7cf2287](https://github.com/cube-js/cube/commit/7cf2287e165beff948ddd88eb47bd0048812aa9b))
- **schema-compiler:** Fix queries with time dimensions without granularities don't hit pre-aggregations with allow_non_strict_date_range_match=true ([#9102](https://github.com/cube-js/cube/issues/9102)) ([eff0736](https://github.com/cube-js/cube/commit/eff073629261c687416a92b8876e46a522bdd1d8))
- **testing:** avoid cypress crashes ([#9196](https://github.com/cube-js/cube/issues/9196)) ([364d181](https://github.com/cube-js/cube/commit/364d1812d5b3aff85bfbfbb82f6d8d2217d8a4ab))

### Features

- **cubejs-server-core:** add contextToCubeStoreRouterId config option ([#9197](https://github.com/cube-js/cube/issues/9197)) ([6ee0e41](https://github.com/cube-js/cube/commit/6ee0e416761799acd064ad88241519ad2e1992f4))

## [1.2.1](https://github.com/cube-js/cube/compare/v1.2.0...v1.2.1) (2025-02-06)

### Features

- **schema-compiler:** Move transpiling to worker threads (under the flag) ([#9188](https://github.com/cube-js/cube/issues/9188)) ([7451452](https://github.com/cube-js/cube/commit/745145283b42c891e0e063c4cad1e2102c71616a))
- **tesseract:** Subqueries support ([#9108](https://github.com/cube-js/cube/issues/9108)) ([0dbf3bb](https://github.com/cube-js/cube/commit/0dbf3bb866b731f97b5588b0e620c3be7cbf9e5b))

# [1.2.0](https://github.com/cube-js/cube/compare/v1.1.18...v1.2.0) (2025-02-05)

### Bug Fixes

- **@cubejs-client/ngx:** Update APF configuration and build settings for Angular 12+ compatibility ([#9152](https://github.com/cube-js/cube/issues/9152)) Thanks to @HaidarZ! ([57bcbc4](https://github.com/cube-js/cube/commit/57bcbc482002aaa025ba5eab8960ecc5784faa55))
- **cubejs-playground:** update query builder ([#9175](https://github.com/cube-js/cube/issues/9175)) ([4e9ed0e](https://github.com/cube-js/cube/commit/4e9ed0eae46c4abb1b84e6423bbc94ad93da37c3))
- **cubejs-playground:** update query builder ([#9184](https://github.com/cube-js/cube/issues/9184)) ([247ac0c](https://github.com/cube-js/cube/commit/247ac0c4028aef5295e88cd501323b94c5a4eaab))
- **cubesql:** Avoid panics during filter rewrites ([#9166](https://github.com/cube-js/cube/issues/9166)) ([4c8de88](https://github.com/cube-js/cube/commit/4c8de882b3cc57e2b0c29c33ca4ee91377176e85))
- **databricks-jdbc-driver:** Fix extract epoch from timestamp SQL Generation ([#9160](https://github.com/cube-js/cube/issues/9160)) ([9a73857](https://github.com/cube-js/cube/commit/9a73857b4abc5691b44b8a395176a565cdbf1b2a))
- **schema-compiler:** make sure view members are resolvable in DAP ([#9059](https://github.com/cube-js/cube/issues/9059)) ([e7b992e](https://github.com/cube-js/cube/commit/e7b992effb6ef90883d059280270c92d903607d4))

### Features

- **cubeclient:** Add hierarchies to Cube meta ([#9180](https://github.com/cube-js/cube/issues/9180)) ([56dbf9e](https://github.com/cube-js/cube/commit/56dbf9edc8c257cd81f00ff119b12543652b76d0))
- **cubesql:** Add filter flattening rule ([#9148](https://github.com/cube-js/cube/issues/9148)) ([92a4b8e](https://github.com/cube-js/cube/commit/92a4b8e0e65c05f2ca0a683387d3f4434c358fc6))
- **cubesql:** Add separate ungrouped_scan flag to wrapper replacer context ([#9120](https://github.com/cube-js/cube/issues/9120)) ([50bdbe7](https://github.com/cube-js/cube/commit/50bdbe7f52f653680e32d26c96c41f10e459c341))
- **snowflake-driver:** set QUOTED_IDENTIFIERS_IGNORE_CASE=FALSE by default ([#9172](https://github.com/cube-js/cube/issues/9172)) ([164b783](https://github.com/cube-js/cube/commit/164b783843efa93ac8ca422634e5b6daccd91883))
- Support complex join conditions for grouped joins ([#9157](https://github.com/cube-js/cube/issues/9157)) ([28c1e3b](https://github.com/cube-js/cube/commit/28c1e3bba7a100f3152bfdefd86197e818fac941))

## [1.1.18](https://github.com/cube-js/cube/compare/v1.1.17...v1.1.18) (2025-01-27)

### Bug Fixes

- **api-gateway:** Fix graphql query result processing via native results ([#9146](https://github.com/cube-js/cube/issues/9146)) ([7974eac](https://github.com/cube-js/cube/commit/7974eac4f32d5c81fea6266224821424f67b5ead))

## [1.1.17](https://github.com/cube-js/cube/compare/v1.1.16...v1.1.17) (2025-01-27)

### Bug Fixes

- **presto-driver:** optimize testConnection() to issue get nodes() instead of heavy show catalogs ([#9109](https://github.com/cube-js/cube/issues/9109)) ([6d6df53](https://github.com/cube-js/cube/commit/6d6df532679514e8d57b2d801d7b7820e31946cf))
- **schema-compiler:** Move create table name check to underlying driver for Postgres, MySQL, Oracle ([#9112](https://github.com/cube-js/cube/issues/9112)) ([5fb81cc](https://github.com/cube-js/cube/commit/5fb81ccce7229c6e08a11f21957af2aa8929ad06))

### Features

- **api-gateway:** Async native query results transformations ([#8961](https://github.com/cube-js/cube/issues/8961)) ([3822107](https://github.com/cube-js/cube/commit/382210716fc3c9ed459c5b45a8a52e766ff7d7cf))
- **cubesql:** Support %s in format ([#9129](https://github.com/cube-js/cube/issues/9129)) ([be140ec](https://github.com/cube-js/cube/commit/be140ecdd0425f6ea3420520649f68ff81ae46f1)), closes [#9126](https://github.com/cube-js/cube/issues/9126)
- **databricks-driver:** Enable Azure AD authentication via Client Secret ([#9104](https://github.com/cube-js/cube/issues/9104)) ([f121e4d](https://github.com/cube-js/cube/commit/f121e4da3406af4b6dae9985e632cfd7849fc180))
- **schema-compiler:** Add flag for using named timezones in MySQL Query class ([#9111](https://github.com/cube-js/cube/issues/9111)) ([5a540db](https://github.com/cube-js/cube/commit/5a540db9228dcbb88c434123f13291202f6da9be))
- **snowflake-driver:** Add ignore case statements flag ([#9131](https://github.com/cube-js/cube/issues/9131)) ([db3b0fd](https://github.com/cube-js/cube/commit/db3b0fd6147f2b0889432eaf8b3f6d45a50461ec))

## [1.1.16](https://github.com/cube-js/cube/compare/v1.1.15...v1.1.16) (2025-01-22)

### Bug Fixes

- **api-gateway:** Allow querying time dimensions with custom granularity ([#9068](https://github.com/cube-js/cube/issues/9068)) ([80f10ce](https://github.com/cube-js/cube/commit/80f10ce7ec93377e056b4802da7cec30dfcb88a3))
- **client-ws-transport:** Flush send queue in close() to avoid race ([#9101](https://github.com/cube-js/cube/issues/9101)) ([d9bc147](https://github.com/cube-js/cube/commit/d9bc147a6ae1deb5e554d76c23a8ad0c3f0225a5))
- **cubejs-client-react:** useCubeQuery initial loading state ([#9117](https://github.com/cube-js/cube/issues/9117)) ([81f9b58](https://github.com/cube-js/cube/commit/81f9b58c88a966e459fc15105a9f2d191a18bf22))
- **cubesql:** Add forgotten Distinct in ast_size_outside_wrapper cost component ([#8882](https://github.com/cube-js/cube/issues/8882)) ([a64272e](https://github.com/cube-js/cube/commit/a64272e376d67431c7f1c057b56b04bf10c59967))
- **cubesql:** Fix condition for joining two date range filters ([#9113](https://github.com/cube-js/cube/issues/9113)) ([39190e0](https://github.com/cube-js/cube/commit/39190e075671bf1adcf8334c513b70130c67cf64))
- **cubesql:** Pass proper in_projection flag in non-trivial wrapper pull up rule ([#9097](https://github.com/cube-js/cube/issues/9097)) ([8f0758e](https://github.com/cube-js/cube/commit/8f0758e2b29a502a0048d6264ae30cf02d7340d7))
- **druid-driver:** Fix case sensitivity in like flow ([#8658](https://github.com/cube-js/cube/issues/8658)) ([6d75c60](https://github.com/cube-js/cube/commit/6d75c609cd9336b711e2539c255dba706442981c))
- **gateway:** dryRun to return pre-rewritten queries ([#9091](https://github.com/cube-js/cube/issues/9091)) ([f48495c](https://github.com/cube-js/cube/commit/f48495c10dca26b3c52acba6a66013a5246f37c0))
- Install python3 executable for node-gyp ([#8998](https://github.com/cube-js/cube/issues/8998)) ([9700dc8](https://github.com/cube-js/cube/commit/9700dc83838479ed9453c9e10e6f1cec3ad4cdd6))
- **schema-compiler:** hierarchies on extended cubes ([#9100](https://github.com/cube-js/cube/issues/9100)) ([c8f24f7](https://github.com/cube-js/cube/commit/c8f24f70131f0dc7138769837f8effa812998169))
- **schema-compiler:** update hierarchies handling to use object pattern ([#9121](https://github.com/cube-js/cube/issues/9121)) ([42cbc8e](https://github.com/cube-js/cube/commit/42cbc8e51fd38b6ca7ade546d638408c06aeafb2))
- typo for DBeaver in comments change DBEver to DBeaver ([#9092](https://github.com/cube-js/cube/issues/9092)) ([aab9e8f](https://github.com/cube-js/cube/commit/aab9e8f844244247d21fac427ae36ee25aa24ae1))

### Features

- **cli:** Deploy, added --replace-env flag ([#9095](https://github.com/cube-js/cube/issues/9095)) ([80591ea](https://github.com/cube-js/cube/commit/80591ea454f9ca4183967e344c01c616577ab294))
- **cubejs-playground:** update query builder ([#9118](https://github.com/cube-js/cube/issues/9118)) ([7ad8936](https://github.com/cube-js/cube/commit/7ad8936fbe6a174a13121b49f2f75fa0e56310ef))
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

## [1.1.13](https://github.com/cube-js/cube/compare/v1.1.12...v1.1.13) (2025-01-09)

### Bug Fixes

- **clickhouse-driver:** Support overriding Username & Password from Driver Config ([#9085](https://github.com/cube-js/cube/issues/9085)) ([c5aa6cc](https://github.com/cube-js/cube/commit/c5aa6cc4aca0c22dacd24253fe116f8619f96601))
- **cubejs-cli:** Fix content type and body after switching from request to node-fetch ([#9088](https://github.com/cube-js/cube/issues/9088)) ([bf332ef](https://github.com/cube-js/cube/commit/bf332ef6761e8354115cc485d7d762abc9268181))
- **schema-compiler:** Handle member expressions in keyDimensions ([#9083](https://github.com/cube-js/cube/issues/9083)) ([056a1d8](https://github.com/cube-js/cube/commit/056a1d88b4fd8055575dbb07967bc64b7065ce86))

## [1.1.12](https://github.com/cube-js/cube/compare/v1.1.11...v1.1.12) (2025-01-09)

### Bug Fixes

- **api-gateway:** fix DAP RLS issue with denied queries and python conf ([#9054](https://github.com/cube-js/cube/issues/9054)) ([e661d2a](https://github.com/cube-js/cube/commit/e661d2af432f57f1109a3a24f91ea0ef0fe2af6c))
- **backend-native:** Fix request sequence span ids ([#9077](https://github.com/cube-js/cube/issues/9077)) ([d48ef99](https://github.com/cube-js/cube/commit/d48ef99548d49ae0e9aa25ae052b3c432bd752f6))
- **backend-native:** Pass req.securityContext to Python config ([#9049](https://github.com/cube-js/cube/issues/9049)) ([95021f2](https://github.com/cube-js/cube/commit/95021f277967ad88e783f766b2e546e3e6ebebf2))
- **cubejs-client-core:** hierarchy typings ([#9042](https://github.com/cube-js/cube/issues/9042)) ([7c38845](https://github.com/cube-js/cube/commit/7c38845388fde0939423f0c35cf65cf1d67c1c36))
- **dremio-driver:** Fix stale version of testing-shared dependency ([#9079](https://github.com/cube-js/cube/issues/9079)) ([ee826e1](https://github.com/cube-js/cube/commit/ee826e10c4e1a078083d32580a5d63bcdeb16628))
- **schema-compiler:** fix time dimension granularity origin formatting in local timezone ([#9071](https://github.com/cube-js/cube/issues/9071)) ([c97526f](https://github.com/cube-js/cube/commit/c97526ff9145195786d4ec9617bcf298567bce7e))
- **tesseract:** sqlAlias support ([f254f64](https://github.com/cube-js/cube/commit/f254f641f6f084f4f6dfc1a5d0d90e0c6f8be9cf))

### Features

- **cubejs-client-core:** Fill missing dates with custom measure value ([#8843](https://github.com/cube-js/cube/issues/8843)) ([ed3f6c9](https://github.com/cube-js/cube/commit/ed3f6c9b00f48889843080e800e4321d939ce5ec))
- **cubesql:** Penalize zero members in wrapper ([#8927](https://github.com/cube-js/cube/issues/8927)) ([171ea35](https://github.com/cube-js/cube/commit/171ea351e739f705ddbf0d803a34b944cb8c9da5))
- **duckdb-driver:** Add support for installing and loading DuckDB Extensions. ([#8744](https://github.com/cube-js/cube/issues/8744)) ([0e6ecd9](https://github.com/cube-js/cube/commit/0e6ecd925fc342bd4ac692e3ee364f29ee6b20df))
- **server-core:** Support for scheduledRefreshTimeZones as function, passing securityContext ([#9002](https://github.com/cube-js/cube/issues/9002)) ([10e47fc](https://github.com/cube-js/cube/commit/10e47fc5472a3532a8f40f6f980f9802536a39de))
- **tesseract:** Make measure an entry point for multi-fact join for now and support template for params ([#9053](https://github.com/cube-js/cube/issues/9053)) ([cb507c1](https://github.com/cube-js/cube/commit/cb507c1ab345828435d192d1f783998bbc5130b3))
- **vertica-driver:** Introduce VerticaDriver ([#9081](https://github.com/cube-js/cube/issues/9081)) ([c43340d](https://github.com/cube-js/cube/commit/c43340d9cc7d06d7caea4803b4d6e00a3e82acdf)), closes [#2](https://github.com/cube-js/cube/issues/2) [#5](https://github.com/cube-js/cube/issues/5) [#6](https://github.com/cube-js/cube/issues/6)

## [1.1.11](https://github.com/cube-js/cube/compare/v1.1.10...v1.1.11) (2024-12-16)

### Bug Fixes

- TypeError: Cannot read properties of undefined (reading 'joins') ([14adaeb](https://github.com/cube-js/cube/commit/14adaebdd1c3d398bcd2997012da070999e47d9d))

## [1.1.10](https://github.com/cube-js/cube/compare/v1.1.9...v1.1.10) (2024-12-16)

### Bug Fixes

- **api-gateway:** allow switch sql user when the new user is the same ([#9037](https://github.com/cube-js/cube/issues/9037)) ([a69c28f](https://github.com/cube-js/cube/commit/a69c28f524fa0625b825b98a38e7f5a211a98f74))
- **api-gateway:** make sure DAP works sql pushdown ([#9021](https://github.com/cube-js/cube/issues/9021)) ([23695b2](https://github.com/cube-js/cube/commit/23695b2b5e886b5b7daf8b3f74003bb04e5b2e0b))
- **cubestore:** Allow create an index from expressions ([#9006](https://github.com/cube-js/cube/issues/9006)) ([222cab8](https://github.com/cube-js/cube/commit/222cab897c289bfc929f217483e4905204bac12f))
- **schema-compiler:** fix DAP with query_rewrite and python config ([#9033](https://github.com/cube-js/cube/issues/9033)) ([849790f](https://github.com/cube-js/cube/commit/849790f965dd0d9fddba11e3d8d124b84397ca9b))
- **schema-compiler:** join relationship aliases ([ad4e8e3](https://github.com/cube-js/cube/commit/ad4e8e3872307ab77e035709e5208b0191f87f5b))

### Features

- **cubesql:** Basic VALUES support in rewrite engine ([#9041](https://github.com/cube-js/cube/issues/9041)) ([368671f](https://github.com/cube-js/cube/commit/368671fd1b53b2ed5ad8df6af113492982f23c0c))
- **dremio-driver:** Add Dremio Cloud Support ([#8956](https://github.com/cube-js/cube/issues/8956)) ([d2c2fcd](https://github.com/cube-js/cube/commit/d2c2fcdaf8944ea7dd27e73b63c0b151c317022e))
- **tesseract:** Support multiple join paths within single query ([#9047](https://github.com/cube-js/cube/issues/9047)) ([b62446e](https://github.com/cube-js/cube/commit/b62446e3c3893068f8dd8aa32d7204ea06a16f98))

## [1.1.9](https://github.com/cube-js/cube/compare/v1.1.8...v1.1.9) (2024-12-08)

### Bug Fixes

- **cubesql:** Allow aggregation pushdown only for unlimited CubeScan ([#8929](https://github.com/cube-js/cube/issues/8929)) ([5b10a68](https://github.com/cube-js/cube/commit/5b10a68b4aca8e5050291fa3ca85dd5f3edc6614))
- **ksql-driver:** Select queries for ksql allowed only from Cube Store. In order to query ksql create pre-aggregation first if Kafka download isn't enabled ([0c2f701](https://github.com/cube-js/cube/commit/0c2f7015d7b2949009fd4aa5c0974547f7463748))

### Features

- **schema-compiler:** folders support, hierarchies improvements ([#9018](https://github.com/cube-js/cube/issues/9018)) ([2012281](https://github.com/cube-js/cube/commit/20122810f508d978921b56f8d9b8a4e479290d56))

## [1.1.8](https://github.com/cube-js/cube/compare/v1.1.7...v1.1.8) (2024-12-05)

### Bug Fixes

- **clickhouse-driver:** Replace error cause with string formatting ([#8977](https://github.com/cube-js/cube/issues/8977)) ([e948856](https://github.com/cube-js/cube/commit/e94885674cf046e9b7188c1b0a6dc4420415a234))
- **clickhouse-driver:** Tune ClickHouse errors: remove SQL, catch ping failure ([#8983](https://github.com/cube-js/cube/issues/8983)) ([811a7ce](https://github.com/cube-js/cube/commit/811a7ce2e5957de5e619ab9d22342580f22dc366))
- **cubesql:** fix unhandled timestamp unwrapping in df/transform_response ([#8952](https://github.com/cube-js/cube/issues/8952)) ([4ea0740](https://github.com/cube-js/cube/commit/4ea0740a4001767ab1863c21c062a7e1487fc4e6))
- **cubestore:** Disable writing cachestore logs by default to reduce use of disk space ([#9003](https://github.com/cube-js/cube/issues/9003)) ([7ccd607](https://github.com/cube-js/cube/commit/7ccd607027d948e867578767a8c3d54604da68da))
- **dev_env_setup:** Fix incorrect arguments processing and make console output more user-friendly ([#8999](https://github.com/cube-js/cube/issues/8999)) ([ead97b4](https://github.com/cube-js/cube/commit/ead97b41130a87fa6cb963790fad8e7ddac6cdce))
- **dev_env_setup:** use relative packages everywhere ([#8910](https://github.com/cube-js/cube/issues/8910)) ([ae823c9](https://github.com/cube-js/cube/commit/ae823c9bb09c893b5601d94fea4dfb33eaf426d7))
- ensure public is in sync with isVisible when using DAP ([#8980](https://github.com/cube-js/cube/issues/8980)) ([a3ed6ea](https://github.com/cube-js/cube/commit/a3ed6ea0d88e644cb4e7506e857a9d4e29c86759))
- fix data access policies condition logic ([#8976](https://github.com/cube-js/cube/issues/8976)) ([47e7897](https://github.com/cube-js/cube/commit/47e7897f7d16b9e24f350234c093d81f0b599dca))
- **ksql-driver:** Kafka, list of brokers ([#9009](https://github.com/cube-js/cube/issues/9009)) ([31d4b46](https://github.com/cube-js/cube/commit/31d4b46a0f84e1095924219f4c34eb56b0efc23c))
- **schema-compiler:** fix FILTER_PARAMS to populate set and notSet filters in cube's SQL query ([#8937](https://github.com/cube-js/cube/issues/8937)) ([6d82f18](https://github.com/cube-js/cube/commit/6d82f18c59f92cae2bb94fe754aa3c0431c24949)), closes [#9009](https://github.com/cube-js/cube/issues/9009) [#8793](https://github.com/cube-js/cube/issues/8793)
- **schema-compiler:** Fix incorrect sql generation for view queries referencing measures from joined cubes ([#8991](https://github.com/cube-js/cube/issues/8991)) ([cd7490e](https://github.com/cube-js/cube/commit/cd7490ef0d5bf3fa1b81b7bf1d820b12a04b1e99))
- **schema-compiler:** Fix ungrouped cumulative queries incorrect sql generation ([#8981](https://github.com/cube-js/cube/issues/8981)) ([82ba01a](https://github.com/cube-js/cube/commit/82ba01ae3695dc7469e76dd8bced1c532bea0865))
- **schema-compiler:** Undefined columns for lambda pre-aggregation queries referencing dimensions from joined cubes ([#8946](https://github.com/cube-js/cube/issues/8946)) ([41b49f4](https://github.com/cube-js/cube/commit/41b49f4fc00f30da7c30d59f522970a24b0e1426))
- **schema-compiler:** use query timezone for time granularity origin ([#8936](https://github.com/cube-js/cube/issues/8936)) ([1beba2d](https://github.com/cube-js/cube/commit/1beba2d3fa6e9a7d95c8c18b448e42fd1562a7e6))
- **server-core:** fix improper requestId retrieval in DAP ([#9017](https://github.com/cube-js/cube/issues/9017)) ([4e366d3](https://github.com/cube-js/cube/commit/4e366d3375cbc8f2d8fc731a644b2310a41edfc0))
- **server-core:** fix member level access policy evaluation ([#8992](https://github.com/cube-js/cube/issues/8992)) ([ff89d65](https://github.com/cube-js/cube/commit/ff89d65e74894a5310fa391db50d0147a9510946))

### Features

- **cubejs-api-gateway:** alternative auth headers ([#8987](https://github.com/cube-js/cube/issues/8987)) ([eb33d1a](https://github.com/cube-js/cube/commit/eb33d1aa6614945f4a73e6c1ef2200abc1917220))
- **databricks-driver:** Support for intervals and CURRENT_DATE for sql push down ([#9000](https://github.com/cube-js/cube/issues/9000)) ([f61afc3](https://github.com/cube-js/cube/commit/f61afc306b225768f741171a8be9c4c82ec83145))
- **duckdb-driver:** remove unnecessary installing and loading HttpFS extension ([#8375](https://github.com/cube-js/cube/issues/8375)) ([0aee4fc](https://github.com/cube-js/cube/commit/0aee4fc9b8563e663030cc2aa0f8805114e746e6))
- **firebolt:** Automatically start the engine after connection ([#9001](https://github.com/cube-js/cube/issues/9001)) ([8f45afa](https://github.com/cube-js/cube/commit/8f45afa28d568ad48b52b44a167f945b61a6e5eb))
- **pinot-driver:** add optional oAuth headers ([#8953](https://github.com/cube-js/cube/issues/8953)) ([7b1f797](https://github.com/cube-js/cube/commit/7b1f7975102d204c8ca5cf08d91acb9352d63a08))
- **sqlplanner:** Extract alias logic from the symbols ([#8919](https://github.com/cube-js/cube/issues/8919)) ([d1b07f1](https://github.com/cube-js/cube/commit/d1b07f1029aa50bd391ba9e52bd1b575c3659da9))

### Performance Improvements

- **cubestore:** ProjectionAboveLimit query optimization ([#8984](https://github.com/cube-js/cube/issues/8984)) ([8a53481](https://github.com/cube-js/cube/commit/8a534817b48b3646439ce2a60dc357ce98e529f2))
- **cubestore:** Update DataFusion pointer to new GroupsAccumulator changes ([#8985](https://github.com/cube-js/cube/issues/8985)) ([4613628](https://github.com/cube-js/cube/commit/46136282d02573d420afc40fdb3767c9d037c067))

## [1.1.7](https://github.com/cube-js/cube/compare/v1.1.6...v1.1.7) (2024-11-20)

### Bug Fixes

- **cubesql:** Support explicit UTC as timezone in pushdown SQL generation ([#8971](https://github.com/cube-js/cube/issues/8971)) ([85eaa29](https://github.com/cube-js/cube/commit/85eaa29a3e8df520fbdc8b2df0ece4a131c39cdc)), closes [/github.com/cube-js/arrow-datafusion/blob/dcf3e4aa26fd112043ef26fa4a78db5dbd443c86/datafusion/physical-expr/src/datetime_expressions.rs#L357-L367](https://github.com//github.com/cube-js/arrow-datafusion/blob/dcf3e4aa26fd112043ef26fa4a78db5dbd443c86/datafusion/physical-expr/src/datetime_expressions.rs/issues/L357-L367)
- **databricks-driver:** fix databricks bucket URL parsing. Now driver supports both (legacy and modern urls) ([#8968](https://github.com/cube-js/cube/issues/8968)) ([137de67](https://github.com/cube-js/cube/commit/137de6725b8150cb418b03aa59443525039380c0))
- **jdbc-driver:** Log errors from connection pool factory ([#8903](https://github.com/cube-js/cube/issues/8903)) ([cfdc2a2](https://github.com/cube-js/cube/commit/cfdc2a28cd9706dc91557f499e3412bab9ff9a07))

### Features

- **clickhouse-driver:** Switch from apla-clickhouse to @clickhouse/client ([#8928](https://github.com/cube-js/cube/issues/8928)) ([e25e65f](https://github.com/cube-js/cube/commit/e25e65fd578bef099c351393ae32da751be351dc))

## [1.1.6](https://github.com/cube-js/cube/compare/v1.1.5...v1.1.6) (2024-11-17)

### Bug Fixes

- **playground:** fix version check for yaml model generation ([#8944](https://github.com/cube-js/cube/issues/8944)) ([ab8ad2b](https://github.com/cube-js/cube/commit/ab8ad2bb0be886934f21c3ddd3be7a370b192a45))
- **schema-compiler:** Time dimension filter for lambda cubes ([#8957](https://github.com/cube-js/cube/issues/8957)) ([eca7e4d](https://github.com/cube-js/cube/commit/eca7e4daa47056f137a3bc0e5735214dba8c8869))

## [1.1.5](https://github.com/cube-js/cube/compare/v1.1.4...v1.1.5) (2024-11-13)

### Bug Fixes

- **schema-compiler:** fix Maximum call stack size exceeded if FILTER_PARAMS are used inside dimensions/measures ([#8867](https://github.com/cube-js/cube/issues/8867)) ([8e7c5c7](https://github.com/cube-js/cube/commit/8e7c5c7d0af0e672f23c8efdab0117121838fa3f))

### Features

- **cubesql:** Initial SQL pushdown flattening ([#8888](https://github.com/cube-js/cube/issues/8888)) ([211d1c1](https://github.com/cube-js/cube/commit/211d1c1053dba7ed69c82f489a433602a66d78e7))
- **cubestore:** Build standalone aarch64-apple-darwin, thanks [@vieira](https://github.com/vieira) ([#8950](https://github.com/cube-js/cube/issues/8950)) ([737fb60](https://github.com/cube-js/cube/commit/737fb60f68c5207001fd982d37413949a7412e28)), closes [#8949](https://github.com/cube-js/cube/issues/8949)

## [1.1.4](https://github.com/cube-js/cube/compare/v1.1.3...v1.1.4) (2024-11-12)

### Bug Fixes

- **cubesql:** Add checks that projection/filters/fetch in TableScan is empty ([#8883](https://github.com/cube-js/cube/issues/8883)) ([a7bab04](https://github.com/cube-js/cube/commit/a7bab04c2b533d95a9ac2f65fb851b9efa9afde1))
- **cubesql:** Pass null_equals_null through egraph ([#8776](https://github.com/cube-js/cube/issues/8776)) ([e02f612](https://github.com/cube-js/cube/commit/e02f6126cd274f7a6f212b497b720fbed6dc1131))
- **schema-compiler:** set missed CUBESQL_SQL_PUSH_DOWN to true by default in convertTzForRawTimeDimension flag ([#8931](https://github.com/cube-js/cube/issues/8931)) ([9482807](https://github.com/cube-js/cube/commit/9482807cdff86949b9142771b9389ee59bd2aae3))

### Features

- **api-gateway:** Meta - expose aliasMember for members in View ([#8945](https://github.com/cube-js/cube/issues/8945)) ([c127f36](https://github.com/cube-js/cube/commit/c127f36a124edb24ba31c043760b2883485b79f1))

## [1.1.3](https://github.com/cube-js/cube/compare/v1.1.2...v1.1.3) (2024-11-08)

### Bug Fixes

- **cubesql:** Don't show meta OLAP queries in query history ([#8336](https://github.com/cube-js/cube/issues/8336)) ([78a5fc3](https://github.com/cube-js/cube/commit/78a5fc332e5e224ba9a00bba8deacf0417edbd34))
- **cubesql:** Fix `NULLS FIRST`/`LAST` SQL push down for several dialects ([#8895](https://github.com/cube-js/cube/issues/8895)) ([61c5ac6](https://github.com/cube-js/cube/commit/61c5ac618c9b68cf1185625d77420ed4a2c5da54))
- **fileRepository:** create repositoryPath if not exists ([#8909](https://github.com/cube-js/cube/issues/8909)) ([4153e4d](https://github.com/cube-js/cube/commit/4153e4df4a9e2bb2abe0516eac45711088f3a8d0))
- patch isVisible when applying member level access policies ([#8921](https://github.com/cube-js/cube/issues/8921)) ([2bb1d21](https://github.com/cube-js/cube/commit/2bb1d2165aa4d9a0ad9a39f936fe6cd2d08253f1))

### Reverts

- Revert "chore: release script - push tag and commit manually (#8897)" ([d7dc5fe](https://github.com/cube-js/cube/commit/d7dc5fe901def8fb7aae776c9a8bdac8d01bc0d9)), closes [#8897](https://github.com/cube-js/cube/issues/8897)

## [1.1.2](https://github.com/cube-js/cube/compare/v1.1.1...v1.1.2) (2024-11-01)

### Features

- **snowflake-driver:** host env variable ([#8898](https://github.com/cube-js/cube/issues/8898)) ([26c765b](https://github.com/cube-js/cube/commit/26c765b6e02fd7ebddd7d387f705fc7da19921c0))

## [1.1.1](https://github.com/cube-js/cube/compare/v1.1.0...v1.1.1) (2024-10-31)

### Bug Fixes

- **cubejs-playground:** add missing fonts ([#8866](https://github.com/cube-js/cube/issues/8866)) ([a621b70](https://github.com/cube-js/cube/commit/a621b700b3764e82d54658f5910557ba8347ba0a))

### Features

- **drivers:** introduce Apache Pinot ([#8689](https://github.com/cube-js/cube/issues/8689)) ([0659c84](https://github.com/cube-js/cube/commit/0659c84dc889041a93ac4e788be000d047b8ef11))
- **redshift-driver:** introspection for external schemas/tables (e.g. Spectrum) ([#8849](https://github.com/cube-js/cube/issues/8849)) ([fa4b3b8](https://github.com/cube-js/cube/commit/fa4b3b8161c38cc7d26e9ab00e70892fcbc0b137))
- support context_to_roles in Python configuration ([#8880](https://github.com/cube-js/cube/issues/8880)) ([aadce4f](https://github.com/cube-js/cube/commit/aadce4fbb7ac893ae8e3ec8c8353c440fbadf8ae))

# [1.1.0](https://github.com/cube-js/cube/compare/v1.0.4...v1.1.0) (2024-10-24)

**Note:** Version bump only for package cubejs

## [1.0.4](https://github.com/cube-js/cube/compare/v1.0.3...v1.0.4) (2024-10-23)

### Bug Fixes

- **cubejs-server-core:** driverError overriding ([#8852](https://github.com/cube-js/cube/issues/8852)) ([b7306d2](https://github.com/cube-js/cube/commit/b7306d2eb26d074d83c47ac49aea0fb8ac3c2138))
- return client customized error, if present, on auth fail in playground ([#8842](https://github.com/cube-js/cube/issues/8842)) ([5cce98d](https://github.com/cube-js/cube/commit/5cce98d7bf03c7be244649e8f7503b1a38ded855)), closes [#CORE-1134](https://github.com/cube-js/cube/issues/CORE-1134)

### Reverts

- Revert "fix(cubejs-server-core): driverError overriding (#8852)" (#8854) ([67eaa1c](https://github.com/cube-js/cube/commit/67eaa1cb1ab415f7010a1458912a0e4fffb48cd2)), closes [#8852](https://github.com/cube-js/cube/issues/8852) [#8854](https://github.com/cube-js/cube/issues/8854)

## [1.0.3](https://github.com/cube-js/cube/compare/v1.0.2...v1.0.3) (2024-10-22)

### Features

- **bigquery-driver:** optimize testConnection() with free of charge request ([#8845](https://github.com/cube-js/cube/issues/8845)) ([99ad335](https://github.com/cube-js/cube/commit/99ad335d5a48dd3bfab48720d23cb39773817b9f))
- **redshift-driver:** optimize testConnection() with just establishing connection without real query ([#8847](https://github.com/cube-js/cube/issues/8847)) ([3c20346](https://github.com/cube-js/cube/commit/3c20346cbb3f7680642af1c69b8ad2075a209361))

### Reverts

- Revert "feat(base-driver): add an option to skip connection test probes (#8833)" (#8846) ([d1698ce](https://github.com/cube-js/cube/commit/d1698cebbb017059ec1e7c11397fbdf73d072e49)), closes [#8833](https://github.com/cube-js/cube/issues/8833) [#8846](https://github.com/cube-js/cube/issues/8846)

## [1.0.2](https://github.com/cube-js/cube/compare/v1.0.1...v1.0.2) (2024-10-21)

### Bug Fixes

- **cubejs-playground:** update query builder ([#8827](https://github.com/cube-js/cube/issues/8827)) ([5965e59](https://github.com/cube-js/cube/commit/5965e596e6016b0b11bf4d3ed463b1a8ac2a370f))

### Features

- **base-driver:** add an option to skip connection test probes ([#8833](https://github.com/cube-js/cube/issues/8833)) ([1fd7a3e](https://github.com/cube-js/cube/commit/1fd7a3e6920d47d391c776ac2c926f7410fb68d2))
- **cubesql:** Top-down extractor for rewrites ([#8694](https://github.com/cube-js/cube/issues/8694)) ([e8fe6db](https://github.com/cube-js/cube/commit/e8fe6db3b382bab91d9b2e2b46886095b5f8b2e6))
- OpenAPI - declare meta field ([#8840](https://github.com/cube-js/cube/issues/8840)) ([55b8f63](https://github.com/cube-js/cube/commit/55b8f637ce721e1145050699671051ea524f5b19))
- OpenAPI - declare type field for Cube ([#8837](https://github.com/cube-js/cube/issues/8837)) ([578b90c](https://github.com/cube-js/cube/commit/578b90c9c89926d333498cfe6f2f155579688cb0))
- **sqlplanner:** Base multi staging support ([#8832](https://github.com/cube-js/cube/issues/8832)) ([20ed3e0](https://github.com/cube-js/cube/commit/20ed3e0ac23d3664372b9ee4b5450b0c35f04e91))

## [1.0.1](https://github.com/cube-js/cube/compare/v1.0.0...v1.0.1) (2024-10-16)

### Features

- **cubesql:** QueryRouter - remove some MySQL functionality ([#8818](https://github.com/cube-js/cube/issues/8818)) ([5935964](https://github.com/cube-js/cube/commit/59359641cab81bdea55db83075a1e009b52087a7))
- **firebolt-driver:** Use driver's own test connection method, thanks [@ptiurin](https://github.com/ptiurin) ([#8815](https://github.com/cube-js/cube/issues/8815)) ([70a36d8](https://github.com/cube-js/cube/commit/70a36d800868238baaa5ff0f1202329e329c5160))

# [1.0.0](https://github.com/cube-js/cube/compare/v0.36.11...v1.0.0) (2024-10-15)

### Features

- Enable `CUBESQL_SQL_PUSH_DOWN` by default ([#8814](https://github.com/cube-js/cube/issues/8814)) ([e1a8e8d](https://github.com/cube-js/cube/commit/e1a8e8d124ee80839193a363c939152392095ee8))

### BREAKING CHANGES

- Enabling `CUBESQL_SQL_PUSH_DOWN` is backward incompatible to many default behaviors of SQL API

## [0.36.11](https://github.com/cube-js/cube/compare/v0.36.10...v0.36.11) (2024-10-14)

### Features

- **snowflake-driver:** OAuth token path support ([#8808](https://github.com/cube-js/cube/issues/8808)) ([9d5b7e8](https://github.com/cube-js/cube/commit/9d5b7e8ed6d7618dac0f35784844ef9d17cd751b))

## [0.36.10](https://github.com/cube-js/cube/compare/v0.36.9...v0.36.10) (2024-10-14)

### Features

- New style RBAC framework ([#8766](https://github.com/cube-js/cube/issues/8766)) ([ad81453](https://github.com/cube-js/cube/commit/ad81453757788784c785c7a75202f94a74ab4c26))

## [0.36.9](https://github.com/cube-js/cube/compare/v0.36.8...v0.36.9) (2024-10-14)

### Bug Fixes

- **cubejs-testing:** Fix TZ env var for snapshots update script ([#8811](https://github.com/cube-js/cube/issues/8811)) ([51d85d3](https://github.com/cube-js/cube/commit/51d85d3bd8d8f1d910ff830f81651c69f15c2b49))
- **cubesql:** Fix `TRUNC` SQL push down for Databricks ([#8803](https://github.com/cube-js/cube/issues/8803)) ([95088cc](https://github.com/cube-js/cube/commit/95088cc5a4ed54ec978c502a906eb9ebf2adb6d8))
- **cubesql:** Ignore `__user IS NOT NULL` filter ([#8796](https://github.com/cube-js/cube/issues/8796)) ([c1e542a](https://github.com/cube-js/cube/commit/c1e542a7f4f6e22edeff335846c2e1c53c95116b))
- Error after renaming post aggregate ([#8805](https://github.com/cube-js/cube/issues/8805)) ([d2773a7](https://github.com/cube-js/cube/commit/d2773a746829458139adf5baad87958baa43b331))

### Features

- **cubestore:** Metastore - increase parallelism to 2 ([#8807](https://github.com/cube-js/cube/issues/8807)) ([4d2de83](https://github.com/cube-js/cube/commit/4d2de836cc2bda113859591a4e92e71baf512854))

## [0.36.8](https://github.com/cube-js/cube/compare/v0.36.7...v0.36.8) (2024-10-11)

### Bug Fixes

- **cubestore:** don't add all measures to default preagg index ([#8789](https://github.com/cube-js/cube/issues/8789)) ([fd1d09a](https://github.com/cube-js/cube/commit/fd1d09a54661ce214e2e1d8e94fbf806484eca34))
- **playground:** fix not showing granularities for time dimension ([#8794](https://github.com/cube-js/cube/issues/8794)) ([d77a512](https://github.com/cube-js/cube/commit/d77a512bdfef533d56a125ffbcab9f7fdb8483ad))
- Render LIMIT 0 and OFFSET 0 properly in SQL templates ([#8781](https://github.com/cube-js/cube/issues/8781)) ([6b17731](https://github.com/cube-js/cube/commit/6b17731f84aa494de820dce791e3120e4282bc37))

### Features

- Inherit all env variables to Cube Store (auto provisioning) ([#3045](https://github.com/cube-js/cube/issues/3045)) ([4902e6f](https://github.com/cube-js/cube/commit/4902e6f03a2dc66bba1df603d917ed84f81e52db))
- **playground:** custom granularities support ([#8743](https://github.com/cube-js/cube/issues/8743)) ([336fad4](https://github.com/cube-js/cube/commit/336fad4bab02ca014b08b14547a285587ad138f3))
- **prestodb-driver:** Added support export bucket, gcs only ([#8797](https://github.com/cube-js/cube/issues/8797)) ([8c7dcbb](https://github.com/cube-js/cube/commit/8c7dcbbd88de9f977eb75cf92c9b8decf0218643))
- Renaming of Post Aggregate into Multi Stage ([#8798](https://github.com/cube-js/cube/issues/8798)) ([bd42c44](https://github.com/cube-js/cube/commit/bd42c44c48f571bdf6a5f097854f3a022e85278a))
- **snowflake-driver:** support DefaultAzureCredential and Managed Indentity auth for export bucket ([#8792](https://github.com/cube-js/cube/issues/8792)) ([cf4beff](https://github.com/cube-js/cube/commit/cf4beffa881d711ee0b3df6eff8615e3f7b39333))

## [0.36.7](https://github.com/cube-js/cube/compare/v0.36.6...v0.36.7) (2024-10-08)

### Bug Fixes

- Add explicit parameters limit in PostgreSQL and Redshift drivers ([#8784](https://github.com/cube-js/cube/issues/8784)) ([ad6abec](https://github.com/cube-js/cube/commit/ad6abec6608b86d896f0f43786b23d9c9306f4f8))

### Features

- **cubesql:** Support `DATE_PART` SQL push down with Databricks ([#8779](https://github.com/cube-js/cube/issues/8779)) ([8fa0d53](https://github.com/cube-js/cube/commit/8fa0d53600e6d5b2a42cb94160918939e7f33d10))

## [0.36.6](https://github.com/cube-js/cube/compare/v0.36.5...v0.36.6) (2024-10-03)

### Bug Fixes

- **schema-compiler:** Support minutes and seconds for cubestore format interval ([#8773](https://github.com/cube-js/cube/issues/8773)) ([a7a515d](https://github.com/cube-js/cube/commit/a7a515d6d419dfc576b74968cb40b7cf5d9af751))

## [0.36.5](https://github.com/cube-js/cube/compare/v0.36.4...v0.36.5) (2024-10-02)

### Bug Fixes

- **firebolt-driver:** use default api endpoint if not provided ([#8767](https://github.com/cube-js/cube/issues/8767)) ([779eacd](https://github.com/cube-js/cube/commit/779eacd5ef4427c27889495d75613936b58adf40))
- **schema-compiler:** fix FILTER_PARAMS flow in pre aggregations filtering ([#8761](https://github.com/cube-js/cube/issues/8761)) ([41ed9cc](https://github.com/cube-js/cube/commit/41ed9cc2e19f2ab68cdb76e5b6bedcddd4fd13f0))

### Features

- **cubesqlplanner:** Base joins support ([#8656](https://github.com/cube-js/cube/issues/8656)) ([a2e604e](https://github.com/cube-js/cube/commit/a2e604eef41d3b3f079f1a8e49d95b1b1d6ce846))
- **snowflake-driver:** support azure exports buckets ([#8730](https://github.com/cube-js/cube/issues/8730)) ([37c6ccc](https://github.com/cube-js/cube/commit/37c6cccc7a3b0617d87bcc6775d63f501074b737))

## [0.36.4](https://github.com/cube-js/cube/compare/v0.36.3...v0.36.4) (2024-09-27)

### Bug Fixes

- **duckdb-driver:** fix timeStampCast and dateBin ([#8748](https://github.com/cube-js/cube/issues/8748)) ([a292c3f](https://github.com/cube-js/cube/commit/a292c3faea4abce13a527233df59a22be0577408))
- **schema-compiler:** Warn when `COUNT(*)` is used with a cube/view where `count` is not defined ([#8667](https://github.com/cube-js/cube/issues/8667)) ([4739d94](https://github.com/cube-js/cube/commit/4739d946ca0c6358210fdc81cc2a1906fcffc7f8))

### Features

- **client-core:** timeseries for custom intervals ([#8742](https://github.com/cube-js/cube/issues/8742)) ([ae989e1](https://github.com/cube-js/cube/commit/ae989e131d84e4926b8d9fd7588ec42d23e9bdda))
- **schema-compiler:** expose custom granularities details via meta API and query annotation ([#8740](https://github.com/cube-js/cube/issues/8740)) ([c58e97a](https://github.com/cube-js/cube/commit/c58e97ac1a268845c32f8d5f35ac292324f5fd28))

## [0.36.3](https://github.com/cube-js/cube/compare/v0.36.2...v0.36.3) (2024-09-26)

### Bug Fixes

- **schema-compiler:** fix FILTER_PARAMS propagation from view to cube's SQL query ([#8721](https://github.com/cube-js/cube/issues/8721)) ([ec2c2ec](https://github.com/cube-js/cube/commit/ec2c2ec4d057dd1b29748d2b3847d7b60f96d5c8))

## [0.36.2](https://github.com/cube-js/cube/compare/v0.36.1...v0.36.2) (2024-09-18)

### Bug Fixes

- **cubejs-testing:** cypress - fix rollup tests ([#8725](https://github.com/cube-js/cube/issues/8725)) ([23c9cda](https://github.com/cube-js/cube/commit/23c9cda10f7fe8fe642a6f4a28283b31c92e6240))
- **cubesql:** Support new QuickSight meta queries ([148e4cf](https://github.com/cube-js/cube/commit/148e4cf32890872d8be41b127a05df1aca68179c))
- **schema-compiler:** correct origin timestamp initialization in Granularity instance ([#8710](https://github.com/cube-js/cube/issues/8710)) ([11e941d](https://github.com/cube-js/cube/commit/11e941d822b60063d46d706432e552ad200983dd))

### Features

- **client-core:** extend client types with custom granularity support ([#8724](https://github.com/cube-js/cube/issues/8724)) ([21a63e2](https://github.com/cube-js/cube/commit/21a63e2d415b731fe01aa5fb4b693c9e0991b2be))
- **cubejs-playground:** add support for ungrouped and offset options ([#8719](https://github.com/cube-js/cube/issues/8719)) ([7d77b4a](https://github.com/cube-js/cube/commit/7d77b4ad6ce76e0e6d06f23f0cbb74b265b7c299))
- **cubesql:** Support `[I]LIKE ... ESCAPE ...` SQL push down ([2bda0dd](https://github.com/cube-js/cube/commit/2bda0dd968944e777c5b89b2587a620c448dba10))
- **schema-compiler:** exact match preagg with custom granularity without the need for allow_non_strict_date_range_match option ([#8712](https://github.com/cube-js/cube/issues/8712)) ([47c4d78](https://github.com/cube-js/cube/commit/47c4d78d6334ec8a279b42a7c0940ac778654dc8))
- **schema-compiler:** expose custom granularities via meta API ([#8703](https://github.com/cube-js/cube/issues/8703)) ([4875b8e](https://github.com/cube-js/cube/commit/4875b8eb1c2b9c807156725e77d834edcf7b2632))

## [0.36.1](https://github.com/cube-js/cube/compare/v0.36.0...v0.36.1) (2024-09-16)

### Bug Fixes

- **schema-compiler:** Add missing “quarter” time unit to granularity definitions ([#8708](https://github.com/cube-js/cube/issues/8708)) ([e8d81f2](https://github.com/cube-js/cube/commit/e8d81f2e04ce61215b1185d9adc4320233bc34da))

### Features

- **schema-compiler:** Reference granularities in a proxy dimension ([#8664](https://github.com/cube-js/cube/issues/8664)) ([b7674f3](https://github.com/cube-js/cube/commit/b7674f35aa0a22efe46b0142bd0a42eeb39fc02a))
- **snowflake-driver:** Upgrade snowflake-sdk to 1.13.1 (fix Node.js 20+ crash) ([#8713](https://github.com/cube-js/cube/issues/8713)) ([84bc8de](https://github.com/cube-js/cube/commit/84bc8de9cd1a1a30229444bf3796052fbc994abb))

# [0.36.0](https://github.com/cube-js/cube/compare/v0.35.81...v0.36.0) (2024-09-13)

### BREAKING CHANGES

- Remove support for USER_CONTEXT (#8705) ([8a796f8](https://github.com/cube-js/cube/commit/8a796f838a0b638fda079377b42d2cbf86474315)) - This functionality was deprecated starting from v0.26.0. Please migrate to SECURITY_CONTEXT.
- chore!: Remove support for checkAuthMiddleware ([86eadb3](https://github.com/cube-js/cube/commit/86eadb33fafcd66cd97a4bd0ee2ab4cbb872887a)) - checkAuthMiddleware option was deprecated in 0.26.0, because this option was tightly bound to Express. Since Cube.js supports HTTP **and** WebSockets as transports, we want our authentication API to not rely on transport-specific details. We now recommend using checkAuth.
- feat(cubesql)!: Enable CUBESQL_SQL_NO_IMPLICIT_ORDER by default ([f22e1ef](https://github.com/cube-js/cube/commit/f22e1efaef6cb81ce920aac0e85abc0eebc94bf9)) - It's started to be true. it means that SQL API will not add ordering to queries that doesn't specify ORDER BY, previusly it was true only for ungrouped queries.
- chore!: /v1/run-scheduled-refresh - was removed ([7213ae7](https://github.com/cube-js/cube/commit/7213ae743e026116ac73c4407acba30f318bd050)) - Removing this method since Cube is designed and supposed to be run as a cluster, rather than a serverless application, in a production setting. Use the Orchestration API instead.
- chore!: Remove cache & queue driver for Redis ([eac704e](https://github.com/cube-js/cube/commit/eac704ecc54c2a02ba83475973b66efa0be6b389)) - Starting from v0.32, Cube Store is used as default cache and queue engine. Article: https://cube.dev/blog/replacing-redis-with-cube-store
- chore!: Support for Node.js 16 was removed ([8b83021](https://github.com/cube-js/cube/commit/8b830214ab3d16ebfadc65cb9587a08b0496fb93)) - Node.js is EOL, it was deprecated in v0.35.0.
- feat(docker)!: Remove rxvt-unicode (was used as TERM) ([fb9cb75](https://github.com/cube-js/cube/commit/fb9cb75ed747f804e31768b91913e0b4f38f173c)) - It was not usefull, at the same time this TERM requires a lot of libraries to be installed. It costs 148 MB of additional disk space.
- **cubejs-client/playground:** New query builder & new chart prototyping ([6099144](https://github.com/cube-js/cube/commit/609914492d6ca6c4b2be507a2af0eb5e70fdf6de)) - Playground was upgraded to Playground 2.0 and Chart Prototyping, previously available in Cube Cloud only.

### Features

- **cubesql:** Support `information_schema.sql_implementation_info` meta table ([841f59a](https://github.com/cube-js/cube/commit/841f59a5f4155482cca188464fea89d5b1dc55b6))
- **databricks-jdbc-driver:** Support Java higher then 11 ([03c278d](https://github.com/cube-js/cube/commit/03c278db81a1bbf3363192aeec50993f2fcf8a5b))
- **docker-jdk:** Upgrade JDK to 17 from 11 ([c3a1ccd](https://github.com/cube-js/cube/commit/c3a1ccd04a600951c8a1729cea40601714f173a8))
- **docker:** Upgrade Node.js to 20.17.0 ([9fedf78](https://github.com/cube-js/cube/commit/9fedf784a1567d4e8190f3a24b46f2bc7adc4996))
- **docker:** Upgrade OpenSSL to 3 from (1.1) ([97159e7](https://github.com/cube-js/cube/commit/97159e747faa391922bd435dffa40c70256a4fe8))
- **docker:** Upgrade OS to bookworm from bullseye ([67e9521](https://github.com/cube-js/cube/commit/67e9521d71a5f6feb779217eb81830bc695f09d0))
- **docker:** Upgrade Python to 3.11 from 3.9 ([7684579](https://github.com/cube-js/cube/commit/76845792ad3cb6aa790f606c452453cc663039a7))
- **schema-compiler:** Support custom granularities ([#8537](https://github.com/cube-js/cube/issues/8537)) ([2109849](https://github.com/cube-js/cube/commit/21098494353b9b6274b534b6c656154cb8451c7b))
- **schema-compiler:** Support custom granularities for Cube Store queries ([#8684](https://github.com/cube-js/cube/issues/8684)) ([f7c07a7](https://github.com/cube-js/cube/commit/f7c07a7572c95de26db81308194577b32e289e53))
- Upgrade aws-sdk to 3.650.0 ([0671e4a](https://github.com/cube-js/cube/commit/0671e4a7ae91fbaf23d4a72aa667fc8eb6dcd6a6))

## [0.35.81](https://github.com/cube-js/cube/compare/v0.35.80...v0.35.81) (2024-09-12)

### Bug Fixes

- **api-gateway:** fixes an issue where queries to get the total count of results were incorrectly applying sorting from the original query and also were getting default ordering applied when the query ordering was stripped out ([#8060](https://github.com/cube-js/cube/issues/8060)) Thanks [@rdwoodring](https://github.com/rdwoodring)! ([863f370](https://github.com/cube-js/cube/commit/863f3709e97c904f1c800ad98889dc272dbfddbd))
- **cubesql:** Use load meta with user change for SQL generation calls ([#8693](https://github.com/cube-js/cube/issues/8693)) ([0f7bb3d](https://github.com/cube-js/cube/commit/0f7bb3d3a96447a69835e3c591ebaf67592c3eed))
- Updated jsonwebtoken in all packages ([#8282](https://github.com/cube-js/cube/issues/8282)) Thanks [@jlloyd-widen](https://github.com/jlloyd-widen) ! ([ca7c292](https://github.com/cube-js/cube/commit/ca7c292e0122be50ac7adc9b9d4910623d19f840))

### Features

- **cubestore:** Support date_bin function ([#8672](https://github.com/cube-js/cube/issues/8672)) ([64788de](https://github.com/cube-js/cube/commit/64788dea89b0244911518de203929fc5c773cd8f))
- ksql and rollup pre-aggregations ([#8619](https://github.com/cube-js/cube/issues/8619)) ([cdfbd1e](https://github.com/cube-js/cube/commit/cdfbd1e21ffcf111e40c525f8a391cc0dcee3c11))

## [0.35.80](https://github.com/cube-js/cube/compare/v0.35.79...v0.35.80) (2024-09-09)

### Bug Fixes

- **schema-compiler:** propagate FILTER_PARAMS from view to inner cube's SELECT ([#8466](https://github.com/cube-js/cube/issues/8466)) ([c0466fd](https://github.com/cube-js/cube/commit/c0466fde9b7a3834159d7ec592362edcab6d9795))

### Features

- **cubesql:** Fill pg_description table with cube and members descriptions ([#8618](https://github.com/cube-js/cube/issues/8618)) ([2288c18](https://github.com/cube-js/cube/commit/2288c18bf30d1f3a3299b235fe9b4405d2cb7463))
- **cubesql:** Support join with type coercion ([#8608](https://github.com/cube-js/cube/issues/8608)) ([46b3a36](https://github.com/cube-js/cube/commit/46b3a36936f0f00805144714f0dd87a3c50a5e0a))

## [0.35.79](https://github.com/cube-js/cube/compare/v0.35.78...v0.35.79) (2024-09-04)

### Bug Fixes

- **schema-compiler:** correct string casting for BigQuery ([#8651](https://github.com/cube-js/cube/issues/8651)) ([f6ac9c2](https://github.com/cube-js/cube/commit/f6ac9c2255091b27fc4ac5c2434bf7de7248afb2))
- **schema-compiler:** correct string casting for Databricks ([#8652](https://github.com/cube-js/cube/issues/8652)) ([5cf71d9](https://github.com/cube-js/cube/commit/5cf71d91373899a1dfd8f3382ab59574bd9637a7))
- **schema-compiler:** correct string casting for MySQL ([#8650](https://github.com/cube-js/cube/issues/8650)) ([9408bb4](https://github.com/cube-js/cube/commit/9408bb43a7a63134894f0a7c4e1883e267b051a1))
- **schema-compiler:** correct string concatenation and casting for MS SQL ([#8649](https://github.com/cube-js/cube/issues/8649)) ([0e9f9f1](https://github.com/cube-js/cube/commit/0e9f9f1f475245bd2c41fa05e6fd8a090b07bf99))
- **schema-compiler:** Support `CURRENT_DATE` SQL push down for BigQuery ([9aebd6b](https://github.com/cube-js/cube/commit/9aebd6ba94bdc67c1b83e0b34d769db4088b41d2))

### Features

- **cubejs-api-gateway:** Support returning new security context from check_auth ([#8585](https://github.com/cube-js/cube/issues/8585)) ([704a96c](https://github.com/cube-js/cube/commit/704a96c4cdb4684459c37f5ed4a026f59b89e6f7))

## [0.35.78](https://github.com/cube-js/cube/compare/v0.35.77...v0.35.78) (2024-08-27)

### Bug Fixes

- **cubesql:** Don't clone AST on pre-planning step ([#8644](https://github.com/cube-js/cube/issues/8644)) ([03277b0](https://github.com/cube-js/cube/commit/03277b0e42afcc31fd7b1822aa9570a88f78e788))
- **cubesql:** Don't clone response, improve performance ([#8638](https://github.com/cube-js/cube/issues/8638)) ([4366299](https://github.com/cube-js/cube/commit/436629901d1643193cd52c6af948112c7bc0c838))
- **cubesql:** Fix non-injective functions split rules, make them variadic ([#8563](https://github.com/cube-js/cube/issues/8563)) ([ed33403](https://github.com/cube-js/cube/commit/ed33403eee334642f1ccaf2db075035e3b877368))
- **cubesql:** Normalize incoming date_part and date_trunc tokens ([#8583](https://github.com/cube-js/cube/issues/8583)) ([f985265](https://github.com/cube-js/cube/commit/f9852650cb3b61dd52386f5cc6a1cec6a5752588))

### Features

- **cubejs-api-gateway:** Add description to V1CubeMeta, V1CubeMetaDimension and V1CubeMetaMeasure in OpenAPI ([#8597](https://github.com/cube-js/cube/issues/8597)) ([1afa934](https://github.com/cube-js/cube/commit/1afa934b1db7379a87ee913816e9ce855783d2bb))
- **cubesql:** Introduce max sessions limit ([#8616](https://github.com/cube-js/cube/issues/8616)) ([dfcb596](https://github.com/cube-js/cube/commit/dfcb5966e76a27fd847e2457bf4af2e1c32b21ac))
- **cubesql:** Upgrade serde, serde_json - performance boost ([#8636](https://github.com/cube-js/cube/issues/8636)) ([b4754db](https://github.com/cube-js/cube/commit/b4754dbd7898d928844558adece42668fe6e728f))
- **schema-compiler:** detect boolean columns for schema generation ([#8637](https://github.com/cube-js/cube/issues/8637)) ([178d98f](https://github.com/cube-js/cube/commit/178d98fbc35b1744d0e83e0e4103ce0465ca2244))

## [0.35.77](https://github.com/cube-js/cube/compare/v0.35.76...v0.35.77) (2024-08-26)

### Bug Fixes

- Row deduplication query isn't triggered in views if multiple entry join path is used ([#8631](https://github.com/cube-js/cube/issues/8631)) ([1668175](https://github.com/cube-js/cube/commit/16681754acfd23f1005838f83e8333ac1da1c6c3))
- View builds incorrect join graph if join paths partially shared ([#8632](https://github.com/cube-js/cube/issues/8632)) Thanks [@barakcoh](https://github.com/barakcoh) for the fix hint! ([5ca76db](https://github.com/cube-js/cube/commit/5ca76dbd66c8b0227d3107a1b1c3f1f9316dbd86)), closes [#8499](https://github.com/cube-js/cube/issues/8499)

### Features

- **cubesql:** CubeScan - don't clone strings for non stream response ([#8633](https://github.com/cube-js/cube/issues/8633)) ([df364be](https://github.com/cube-js/cube/commit/df364beae38badbeeb27488a847a34c4431457e8))

## [0.35.76](https://github.com/cube-js/cube/compare/v0.35.75...v0.35.76) (2024-08-24)

### Bug Fixes

- Invalid interval when querying multiple negative interval windows within Cube Store ([#8626](https://github.com/cube-js/cube/issues/8626)) ([716b26a](https://github.com/cube-js/cube/commit/716b26a7fa5c43d1b7970461c5007c872b0ca184))

## [0.35.75](https://github.com/cube-js/cube/compare/v0.35.74...v0.35.75) (2024-08-22)

### Bug Fixes

- Internal: ParserError("Expected AND, found: INTERVAL") in case of trailing and leading window parts are defined for pre-aggregated rolling window measure ([#8611](https://github.com/cube-js/cube/issues/8611)) ([18d0620](https://github.com/cube-js/cube/commit/18d0620e69c9a43a9d0e0d1634132959e40f6860))

## [0.35.74](https://github.com/cube-js/cube/compare/v0.35.73...v0.35.74) (2024-08-22)

### Bug Fixes

- allow non-stream mode for sql over http ([#8610](https://github.com/cube-js/cube/issues/8610)) ([6a16e67](https://github.com/cube-js/cube/commit/6a16e67a45b05eb5c0aad469fb2f7c47f87aaa74))

### Features

- **cubesql:** Add pg_class self-reference ([#8603](https://github.com/cube-js/cube/issues/8603)) ([05ea7e2](https://github.com/cube-js/cube/commit/05ea7e25014b7ac72e32fa0391ce3ea99ab3e01f))

## [0.35.73](https://github.com/cube-js/cube/compare/v0.35.72...v0.35.73) (2024-08-21)

### Bug Fixes

- **cubesql:** Reduce memory usage while converting to DataFrame ([#8598](https://github.com/cube-js/cube/issues/8598)) ([604085e](https://github.com/cube-js/cube/commit/604085e5a2066414eb91128ae020b6e4b92b449f))
- **cubesql:** Use date_part => date_part + date_trunc split only with appropriate date_part argument ([#8552](https://github.com/cube-js/cube/issues/8552)) ([9387072](https://github.com/cube-js/cube/commit/93870720ac872ab06266d4a65a0f9b4aec384f6a))
- **cubestore:** Reduce memory usage while converting to DataFrame ([#8599](https://github.com/cube-js/cube/issues/8599)) ([15f9a0f](https://github.com/cube-js/cube/commit/15f9a0f6791a5128c426c452ea180ffa757f8bc3))

### Features

- **cubejs-schema-compiler:** Support FILTER_GROUP in YAML models ([#8574](https://github.com/cube-js/cube/issues/8574)) ([f3b8b19](https://github.com/cube-js/cube/commit/f3b8b19daff7f1b8dcfd73abf0d1f20e74e5a847))

## [0.35.72](https://github.com/cube-js/cube/compare/v0.35.71...v0.35.72) (2024-08-16)

**Note:** Version bump only for package cubejs

## [0.35.71](https://github.com/cube-js/cube/compare/v0.35.70...v0.35.71) (2024-08-15)

**Note:** Version bump only for package cubejs

## [0.35.70](https://github.com/cube-js/cube/compare/v0.35.69...v0.35.70) (2024-08-14)

### Bug Fixes

- **cubesql:** Don't push down aggregate to grouped query with filters ([df3334c](https://github.com/cube-js/cube/commit/df3334ce6e97e82c4773de5129a615ab5a3d05d8))

## [0.35.69](https://github.com/cube-js/cube/compare/v0.35.68...v0.35.69) (2024-08-12)

### Bug Fixes

- **cubesql:** Split PowerBI count distinct expression ([6a518d3](https://github.com/cube-js/cube/commit/6a518d32c70f8e7da5433d77c4476072829a3ed9))

## [0.35.68](https://github.com/cube-js/cube/compare/v0.35.67...v0.35.68) (2024-08-12)

### Bug Fixes

- **cubestore:** Docker - install ca-certificates ([#8571](https://github.com/cube-js/cube/issues/8571)) ([da40ff6](https://github.com/cube-js/cube/commit/da40ff6efca4822c907631a594380cced4f22978))
- **schema-compiler:** incorrect sql for query with same dimension with different granularities ([#8564](https://github.com/cube-js/cube/issues/8564)) ([b8ec20e](https://github.com/cube-js/cube/commit/b8ec20e33b0c3a44ae81fba35252986737fcef1e))

### Features

- **cubesql:** Support variable number of scalar function arguments in split rewrites ([#8534](https://github.com/cube-js/cube/issues/8534)) ([2300fe8](https://github.com/cube-js/cube/commit/2300fe816ce385dc583355e0d9f18dab90150730))

## [0.35.67](https://github.com/cube-js/cube/compare/v0.35.66...v0.35.67) (2024-08-07)

### Features

- **cubesqlplanner:** Native SQL planner implementation first steps ([#8506](https://github.com/cube-js/cube/issues/8506)) ([fab9c48](https://github.com/cube-js/cube/commit/fab9c4801de41a0d3d05331c5ca85c3e0cdd6266))
- **cubesql:** Support push down cast type templates ([556ca7c](https://github.com/cube-js/cube/commit/556ca7c67b280b18221cb6748cfe20f841b1a7b9))

## [0.35.66](https://github.com/cube-js/cube/compare/v0.35.65...v0.35.66) (2024-08-06)

### Bug Fixes

- **cubestore:** Skip HEAD requests for AWS S3, fix [#8536](https://github.com/cube-js/cube/issues/8536) ([#8542](https://github.com/cube-js/cube/issues/8542)) ([41e3070](https://github.com/cube-js/cube/commit/41e30704fa0d9c72bb2aea9f64d5bddcdf71b796))

## [0.35.65](https://github.com/cube-js/cube/compare/v0.35.64...v0.35.65) (2024-07-26)

### Bug Fixes

- **cubestore:** Import - validate http response for error ([#8520](https://github.com/cube-js/cube/issues/8520)) ([d462b89](https://github.com/cube-js/cube/commit/d462b89410ef94f33b4827384ec4f3f845d56a9f))

### Features

- **native:** Add datetime types support to generate_series() UDTF ([#8511](https://github.com/cube-js/cube/issues/8511)) ([99b3c65](https://github.com/cube-js/cube/commit/99b3c654382de0b750b6204286dc3ddf5dc7a63e))

## [0.35.64](https://github.com/cube-js/cube/compare/v0.35.63...v0.35.64) (2024-07-24)

### Bug Fixes

- **native:** Fix build failures caused by new ShutdownMode param ([#8514](https://github.com/cube-js/cube/issues/8514)) ([80d10fd](https://github.com/cube-js/cube/commit/80d10fda26bc693a9c4e2510283cbc6ca06eb4a5))
- **server:** Start native api gateway only under the flag (not when port specified) ([#8516](https://github.com/cube-js/cube/issues/8516)) ([b533859](https://github.com/cube-js/cube/commit/b533859b8315317b0d457b3ed7b4d67532b8b835))

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

### Bug Fixes

- **schema-compiler:** Replace all toDateTime with toDateTime64 in ClickHouseQuery adapter to handle dates before 1970 ([#8390](https://github.com/cube-js/cube/issues/8390)) Thanks @Ilex4524 ! )j ([f6cff1a](https://github.com/cube-js/cube/commit/f6cff1a8a8dc510ad365ff53c02b1b15a8ba70f2))

### Features

- **native:** Initial support for native api-gateway ([#8472](https://github.com/cube-js/cube/issues/8472)) ([d917d6f](https://github.com/cube-js/cube/commit/d917d6fd422090cc78fc30125731d147a091de6c))
- **rust/cubeclient:** Upgrade reqwest to 0.12.5 (hyper 1) ([#8498](https://github.com/cube-js/cube/issues/8498)) ([f77c3aa](https://github.com/cube-js/cube/commit/f77c3aad67566568117f2c1d42859e2fd53a13d2))

### Performance Improvements

- **cubesql:** Replaced LogicalPlanData::find_member with a caching version ([#8469](https://github.com/cube-js/cube/issues/8469)) ([858d965](https://github.com/cube-js/cube/commit/858d965a42b30e446ae7fd19899cfd9b078ee63f))

## [0.35.61](https://github.com/cube-js/cube/compare/v0.35.60...v0.35.61) (2024-07-19)

### Bug Fixes

- **cubejs-questdb-driver:** fix obsolete column name in SHOW TABLES ([#8493](https://github.com/cube-js/cube/issues/8493)) Thanks [@puzpuzpuz](https://github.com/puzpuzpuz)! ([f135933](https://github.com/cube-js/cube/commit/f135933daf2df9b7a867c9ff1187c5b4bf15483d))
- **cubesql:** Transform `IN` filter with one value to `=` with all expressions ([671e067](https://github.com/cube-js/cube/commit/671e067d25521deadf1502d0dec21da2df5c8e3b))

### Features

- **cubesql:** Support `Null` input type in `SUM` and `AVG` functions ([5ce589a](https://github.com/cube-js/cube/commit/5ce589a1e7b3f8e3f850fdc9abadb26e212f2da4))

## [0.35.60](https://github.com/cube-js/cube/compare/v0.35.59...v0.35.60) (2024-07-17)

### Bug Fixes

- **cubesql:** Fix "unable to represent JsFunction in Python" in queries when using python for cube configuration ([#8449](https://github.com/cube-js/cube/issues/8449)) ([bcef222](https://github.com/cube-js/cube/commit/bcef222ec178af1d3bbe06282c7ab63e8ec6eccf))
- **server-core:** Headers sent multiple times from dev-server handlers crashes it ([#8463](https://github.com/cube-js/cube/issues/8463)) ([45bbb8a](https://github.com/cube-js/cube/commit/45bbb8a739b4d9f079ba78fa48365632ea2eef8c))

### Features

- **cubesql:** Upgrade rust to nightly-2024-07-15 ([#8473](https://github.com/cube-js/cube/issues/8473)) ([6a6a7fe](https://github.com/cube-js/cube/commit/6a6a7fe694c13d04bf048434df92e20ad920aed5))
- **cubesql:** Use lld linker for linux ([#8439](https://github.com/cube-js/cube/issues/8439)) ([a2fb38b](https://github.com/cube-js/cube/commit/a2fb38b410382abdd925d42e10673e9192f0f880))

## [0.35.59](https://github.com/cube-js/cube/compare/v0.35.58...v0.35.59) (2024-07-13)

### Bug Fixes

- Compile data model only once per each version ([#8452](https://github.com/cube-js/cube/issues/8452)) ([2b3bdcd](https://github.com/cube-js/cube/commit/2b3bdcdf451029d056c05c60c57e1c3ec9aca1be))

### Features

- **cubejs-databricks-jdbc-driver:** read-only mode ([#8440](https://github.com/cube-js/cube/issues/8440)) ([6d2f97a](https://github.com/cube-js/cube/commit/6d2f97a4c4a845279dc629780041b9bcb8a47bb9))

## [0.35.58](https://github.com/cube-js/cube/compare/v0.35.57...v0.35.58) (2024-07-10)

### Bug Fixes

- **cubesql:** Fix incorrect `dateRange` when filtering over `date_trunc`'d column in outer query ([90ce14e](https://github.com/cube-js/cube/commit/90ce14e327046a8526012183d040bad0b73cc7cf))
- **postgres-driver:** Don't throw errors for pseudo fields like `__user` ([9c01c1b](https://github.com/cube-js/cube/commit/9c01c1b8486afd65da1166acf9f2782ec97986c2))
- **postgres-driver:** Mapping for generic int as int8 ([#8338](https://github.com/cube-js/cube/issues/8338)) ([3202d8d](https://github.com/cube-js/cube/commit/3202d8d4cc5bd6fca48b8c9b5e1ea9d9566ff8a2))

## [0.35.57](https://github.com/cube-js/cube/compare/v0.35.56...v0.35.57) (2024-07-05)

### Bug Fixes

- **cubesql:** Don't put aggregate functions in `Projection` on split ([4ff4086](https://github.com/cube-js/cube/commit/4ff4086da869a97412a518b9b4572ccc5dcbae7c))
- **cubesql:** Make CAST(COUNT(\*) as float) work properly in some situations ([#8423](https://github.com/cube-js/cube/issues/8423)) ([3ff5c9f](https://github.com/cube-js/cube/commit/3ff5c9fc2349fa35644040787173a6c97d93341f))

### Features

- **cubesql:** Remove implicit order by clause ([#8380](https://github.com/cube-js/cube/issues/8380)) ([3e7d325](https://github.com/cube-js/cube/commit/3e7d325aadff1a572953d843394f55577aef0357))

## [0.35.56](https://github.com/cube-js/cube/compare/v0.35.55...v0.35.56) (2024-07-03)

### Bug Fixes

- **cubesql:** Correctly type `pg_index` fields ([edadaf1](https://github.com/cube-js/cube/commit/edadaf13eeb05c51529fcf10cafd7028454d73fe))
- **cubesql:** Realias outer projection on flatten ([8cbcd7b](https://github.com/cube-js/cube/commit/8cbcd7b038a39b77f8b0309dd959f9a4edaddea3))

### Features

- **docker:** Upgrade node from 18.20.1-bullseye-slim to 18.20.3-bullseye-slim ([#8426](https://github.com/cube-js/cube/issues/8426)) ([57bd39a](https://github.com/cube-js/cube/commit/57bd39a8893e31df9ec5fff3ffd09e1459e3e76c))
- Make graceful shutdown add fatal messages in postgres ([8fe1af2](https://github.com/cube-js/cube/commit/8fe1af223a79253b9beaab923d29e96d13f67cdf))

## [0.35.55](https://github.com/cube-js/cube/compare/v0.35.54...v0.35.55) (2024-06-28)

### Bug Fixes

- **cubejs-databricks-jdbc-driver:** Remove inquirer library (reduce image size) ([#8408](https://github.com/cube-js/cube/issues/8408)) ([682354b](https://github.com/cube-js/cube/commit/682354b5f97ad302e0a9bc5590c902074fefc798))

### Features

- **docker:** Reduce image size by 52.62 mb (remove duckdb sources) ([#8407](https://github.com/cube-js/cube/issues/8407)) ([81bf1ad](https://github.com/cube-js/cube/commit/81bf1ad63aba21e4d31ec9b283308f6b52e2f844))
- **sqlite-driver:** Upgrade sqlite3 to 5.1.7 ([#8406](https://github.com/cube-js/cube/issues/8406)) ([115ce9c](https://github.com/cube-js/cube/commit/115ce9c21042828030a9b9fa3cf505649a82838d))

## [0.35.54](https://github.com/cube-js/cube/compare/v0.35.53...v0.35.54) (2024-06-26)

### Features

- `to_date` rolling window type syntax ([#8399](https://github.com/cube-js/cube/issues/8399)) ([eb7755d](https://github.com/cube-js/cube/commit/eb7755d997f67722daf200040e4e0f3c2814132a))
- **cubesql:** `Interval(MonthDayNano)` multiplication and decomposition ([576f7f7](https://github.com/cube-js/cube/commit/576f7f7de9190a4d466a58f619cb5cf8d6bc6a59))

## [0.35.53](https://github.com/cube-js/cube/compare/v0.35.52...v0.35.53) (2024-06-26)

### Features

- **cubesql:** Decimal128 i.e. NUMERIC(_,_) literal generation for SQL push down ([43e29f9](https://github.com/cube-js/cube/commit/43e29f9f49a683d148b31ea1bcccd28a7c99034e))
- **cubestore:** Docker - upgrade to bookworm ([bfd560c](https://github.com/cube-js/cube/commit/bfd560c5532c2a6c88546853cd7294a9ca1ce1db))
- **cubestore:** Support openssl 3 (upgrade rdkafka) ([f4874d6](https://github.com/cube-js/cube/commit/f4874d64c1bbf9989735bbdc35b504a2e451470e))

## [0.35.52](https://github.com/cube-js/cube/compare/v0.35.51...v0.35.52) (2024-06-20)

**Note:** Version bump only for package cubejs

## [0.35.51](https://github.com/cube-js/cube/compare/v0.35.50...v0.35.51) (2024-06-20)

### Bug Fixes

- **cube:** Rebuild of PreAggregations with aggregating indexes ([#8326](https://github.com/cube-js/cube/issues/8326)) ([cc1de4f](https://github.com/cube-js/cube/commit/cc1de4f3f71bb32f71e88a6db36a59c5cc8f9571))
- **cubesql:** Support `CAST` projection split ([bcbe47b](https://github.com/cube-js/cube/commit/bcbe47b498ee47d563c3354d52db5bca886b0b65))
- **schema-compiler:** add escape sequence to Snowflake QueryFilter ([13d10f6](https://github.com/cube-js/cube/commit/13d10f69bfe1569a61ab091ca917a6dbcab8644a))

### Features

- **cubesql:** support timestamp subtract date in SQL API, tests ([#8372](https://github.com/cube-js/cube/issues/8372)) ([1db6a5c](https://github.com/cube-js/cube/commit/1db6a5c9bc912e52e5874a1ad01a8636523cc2ab))
- **duckdb-driver:** Upgrade to DuckDB 1.0.0 ([#8373](https://github.com/cube-js/cube/issues/8373)) ([e41fa9f](https://github.com/cube-js/cube/commit/e41fa9fda7a4a593baf8e514aa9083e4ee1edb6a))

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

- **client-react:** udpate td with a compare date range ([#8339](https://github.com/cube-js/cube/issues/8339)) ([9ef51f7](https://github.com/cube-js/cube/commit/9ef51f7b12c624873bfa79a302038db7e4b47a6a))
- **cubesql:** Rollup don't work over subquery with asterisk projection ([#8354](https://github.com/cube-js/cube/issues/8354)) ([0bc0306](https://github.com/cube-js/cube/commit/0bc03066ac4bedec4ee2b85d381e36bb13e2edef))
- use tablesSchema over direct information schema ([#8347](https://github.com/cube-js/cube/issues/8347)) ([a4ca555](https://github.com/cube-js/cube/commit/a4ca5553b07944dbed764bbc4b862feac793691e))

### Features

- **cubesql:** support `GREATEST`/`LEAST` SQL functions ([#8325](https://github.com/cube-js/cube/issues/8325)) ([c13a28e](https://github.com/cube-js/cube/commit/c13a28e21514c0e06e41c0ed14e97621bd777cf7))

### Performance Improvements

- **cubesql:** Use an incremental rule scheduler for egg ([5892df7](https://github.com/cube-js/cube/commit/5892df749edb0f290488a196804a5a060e5ea387))

## [0.35.47](https://github.com/cube-js/cube/compare/v0.35.46...v0.35.47) (2024-06-07)

### Bug Fixes

- **cubesql:** Rollup doesn't work over aliased columns ([#8334](https://github.com/cube-js/cube/issues/8334)) ([98e7529](https://github.com/cube-js/cube/commit/98e7529975703f2d4b72cc8f21ce4f8c6fc4c8de))

## [0.35.46](https://github.com/cube-js/cube/compare/v0.35.45...v0.35.46) (2024-06-06)

### Bug Fixes

- **cubestore:** Channel closed in query queue cache ([#8327](https://github.com/cube-js/cube/issues/8327)) ([8898fb1](https://github.com/cube-js/cube/commit/8898fb19bb39dd4778fee695df1ef8497ee28eb2))

## [0.35.45](https://github.com/cube-js/cube/compare/v0.35.44...v0.35.45) (2024-06-05)

### Bug Fixes

- **cubesql:** Don't duplicate time dimensions ([577220d](https://github.com/cube-js/cube/commit/577220ddb88c098dd3142e518fcdb73eb7513b74))
- **cubesql:** Support `DATE_TRUNC` equals literal string ([69ba0ee](https://github.com/cube-js/cube/commit/69ba0eeb9724ee4105ad0d0ff39eedf92dbc715f))

### Performance Improvements

- **cubesql:** Improve rewrite engine performance ([4f78b8a](https://github.com/cube-js/cube/commit/4f78b8a9b75672227e7114be6a524a1b1ced8ff6))

## [0.35.44](https://github.com/cube-js/cube/compare/v0.35.43...v0.35.44) (2024-06-04)

### Bug Fixes

- **databricks-jdbc-driver:** Rolling window & count_distinct_approx (HLL) ([#8323](https://github.com/cube-js/cube/issues/8323)) ([5969f0d](https://github.com/cube-js/cube/commit/5969f0d788fffc1fe3492783eec4270325520d38))
- **query-orchestrator:** Range intersection for 6 digits timestamp, fix [#8320](https://github.com/cube-js/cube/issues/8320) ([#8322](https://github.com/cube-js/cube/issues/8322)) ([667e95b](https://github.com/cube-js/cube/commit/667e95bd9933d67f84409c09f61d47b28156f0c2))

## [0.35.43](https://github.com/cube-js/cube/compare/v0.35.42...v0.35.43) (2024-05-31)

### Bug Fixes

- **native:** CLR - handle 1 level circular references for objects & arrays ([#8314](https://github.com/cube-js/cube/issues/8314)) ([7e8e1ff](https://github.com/cube-js/cube/commit/7e8e1ffdaba612e10f30a2a8eb59eeea47c55dcd))
- **native:** Python - don't crash on extend_context ([#8315](https://github.com/cube-js/cube/issues/8315)) ([e7e7067](https://github.com/cube-js/cube/commit/e7e70673dce7fa923c41d5ae50eb973462a92188))

### Features

- **bigquery-driver:** Use 6 digits precision for timestamps ([#8308](https://github.com/cube-js/cube/issues/8308)) ([568bfe3](https://github.com/cube-js/cube/commit/568bfe34bc6aca136b580acb8873d208b522a2f3))
- sql api over http ([#8254](https://github.com/cube-js/cube/issues/8254)) ([4c18954](https://github.com/cube-js/cube/commit/4c18954452bf14268a2f368960a72fec45f274dc))

## [0.35.42](https://github.com/cube-js/cube/compare/v0.35.41...v0.35.42) (2024-05-30)

### Features

- **cubesql:** Group By Rollup support ([#8281](https://github.com/cube-js/cube/issues/8281)) ([e563798](https://github.com/cube-js/cube/commit/e5637980489608e374f3b89cc219207973a08bbb))

## [0.35.41](https://github.com/cube-js/cube/compare/v0.35.40...v0.35.41) (2024-05-27)

### Features

- **databricks-driver:** Support HLL feature with export bucket ([#8301](https://github.com/cube-js/cube/issues/8301)) ([7f97af4](https://github.com/cube-js/cube/commit/7f97af42d6a6c0645bd778e94f75d62280ffeba6))

## [0.35.40](https://github.com/cube-js/cube/compare/v0.35.39...v0.35.40) (2024-05-24)

### Bug Fixes

- **cubesql:** Match `inDateRange` as time dimension date range in `AND` ([64f176a](https://github.com/cube-js/cube/commit/64f176ab5afc4b0771e45206bff5d6649ca8d672))
- **cubesql:** Propagate error from api-gateway to SQL API ([#8297](https://github.com/cube-js/cube/issues/8297)) ([1a93d3d](https://github.com/cube-js/cube/commit/1a93d3d85de5099a0325750e4d3e203693b50109))

### Features

- **cubesql:** Support `DATE_TRUNC` InList filter ([48e7ad4](https://github.com/cube-js/cube/commit/48e7ad4826f6d279570badae13c93b80c28c8094))

## [0.35.39](https://github.com/cube-js/cube/compare/v0.35.38...v0.35.39) (2024-05-24)

### Bug Fixes

- **cubesql:** Fix time dimension range filter chaining with `OR` operator ([757c4c5](https://github.com/cube-js/cube/commit/757c4c51f608c34b8a857ef7a98229bb3f7b04bb))

### Features

- **cubesql:** Flatten list expression rewrites to improve performance ([96c1549](https://github.com/cube-js/cube/commit/96c1549e1dc2d51f94aacd8fd1b95baef31cc0b1))
- **schema-compiler:** Support multi time dimensions for rollup pre-aggregations ([#8291](https://github.com/cube-js/cube/issues/8291)) ([8b0a056](https://github.com/cube-js/cube/commit/8b0a05657c7bc7f42e0d45ecc7ce03d37e5b1e57))

## [0.35.38](https://github.com/cube-js/cube/compare/v0.35.37...v0.35.38) (2024-05-22)

### Bug Fixes

- **bigquery-driver:** Transform numeric value (big.js) to string ([#8290](https://github.com/cube-js/cube/issues/8290)) ([65c3f55](https://github.com/cube-js/cube/commit/65c3f5541fa4f1ced5813ae642d53c4d9bb80af5))

### Features

- **bigquery-driver:** Upgrade [@google-cloud](https://github.com/google-cloud) SDK to 7 ([#8289](https://github.com/cube-js/cube/issues/8289)) ([57ef124](https://github.com/cube-js/cube/commit/57ef124672f2093054548c8797ca194ee3513923))
- **clickhouse-driver:** SQL API PUSH down - support datetrunc, timestamp_literal ([45bc230](https://github.com/cube-js/cube/commit/45bc2306eb01e2b2cc5c204b64bfa0411ae0144b))
- **clickhouse-driver:** Support countDistinctApprox ([4d36279](https://github.com/cube-js/cube/commit/4d3627996e8cdaa9d77711f9ea21edabc5d3b8d0))
- **duckdb-driver:** Upgrade DuckDB to 0.10.2 ([#8284](https://github.com/cube-js/cube/issues/8284)) ([b057bca](https://github.com/cube-js/cube/commit/b057bca8c77eebc7dcb358efe27ad08703afd966))

## [0.35.37](https://github.com/cube-js/cube/compare/v0.35.36...v0.35.37) (2024-05-20)

### Bug Fixes

- **cubesql:** Make param render respect dialect's reuse params flag ([9c91af2](https://github.com/cube-js/cube/commit/9c91af2e03c84d903ac153337cda9f53682aacf1))

## [0.35.36](https://github.com/cube-js/cube/compare/v0.35.35...v0.35.36) (2024-05-17)

### Bug Fixes

- **cubesql:** Do not generate large graphs for large `IN` filters ([f29c9e4](https://github.com/cube-js/cube/commit/f29c9e481ffa65361a804f6ff1e5bf685cb8d0d5))

## [0.35.35](https://github.com/cube-js/cube/compare/v0.35.34...v0.35.35) (2024-05-17)

### Bug Fixes

- **cubesql:** Remove incorrect LOWER match rules and fallback to SQL pushdown ([#8246](https://github.com/cube-js/cube/issues/8246)) ([6c39f37](https://github.com/cube-js/cube/commit/6c39f376a649eeb7d9c171f1805033b020ef58a7))
- **cubesql:** Remove prefix underscore from aliases ([#8266](https://github.com/cube-js/cube/issues/8266)) ([24e8977](https://github.com/cube-js/cube/commit/24e89779fc4833a8c6b14dc1496d95271c18ebdc))
- **schema-compiler:** Fix failing of `WHERE FALSE` queries ([#8265](https://github.com/cube-js/cube/issues/8265)) ([e63b4ab](https://github.com/cube-js/cube/commit/e63b4ab701cf44162d5e51d65b8d38e812f9085e))

### Features

- **cubesql:** Send expressions as objects ([#8216](https://github.com/cube-js/cube/issues/8216)) ([4deee84](https://github.com/cube-js/cube/commit/4deee845a51e3999f3a75b121e14b570a124ad3a))

## [0.35.34](https://github.com/cube-js/cube/compare/v0.35.33...v0.35.34) (2024-05-15)

### Bug Fixes

- **cubestore:** S3 - STS credentials (align aws-creds to 0.36 in fork) ([#8264](https://github.com/cube-js/cube/issues/8264)) ([1a5b27d](https://github.com/cube-js/cube/commit/1a5b27d1775691456a90d34130841ee03b5edef6))

## [0.35.33](https://github.com/cube-js/cube/compare/v0.35.32...v0.35.33) (2024-05-15)

### Bug Fixes

- **cubesql:** Fix Databricks identifier quotes ([a20f4b2](https://github.com/cube-js/cube/commit/a20f4b2d55c8d0235d327fc0d0cc0c2f587bcd02))
- Mismatched input '10000'. Expecting: '?', 'ALL', <integer> for post-aggregate members in Athena ([#8262](https://github.com/cube-js/cube/issues/8262)) ([59834e7](https://github.com/cube-js/cube/commit/59834e7157bf060804470104e0a713194b811f39))

### Features

- **cubesql:** Rewrites for pushdown of subqueries with empty source ([#8188](https://github.com/cube-js/cube/issues/8188)) ([86a58a5](https://github.com/cube-js/cube/commit/86a58a5f3368a509debfb3f2ba4c83001377127c))
- **firebolt:** Switch to the new auth method ([#8182](https://github.com/cube-js/cube/issues/8182)) ([e559114](https://github.com/cube-js/cube/commit/e55911478d0b3ba916d44f1fa4c04697d7ca7dfb))

## [0.35.32](https://github.com/cube-js/cube/compare/v0.35.31...v0.35.32) (2024-05-14)

### Features

- **cubesql:** Flatten IN lists expressions to improve performance ([#8235](https://github.com/cube-js/cube/issues/8235)) ([66aa01d](https://github.com/cube-js/cube/commit/66aa01d4f8f888993e68a0967e664b9dcfcc51e7))
- **databricks-jdbc-driver:** Support HLL ([#8257](https://github.com/cube-js/cube/issues/8257)) ([da231ed](https://github.com/cube-js/cube/commit/da231ed48ae8386f1726710e80c3a943204f6895))

## [0.35.31](https://github.com/cube-js/cube/compare/v0.35.30...v0.35.31) (2024-05-13)

### Features

- **cubestore:** Support HLL from Apache DataSketches (for DataBricks) ([#8241](https://github.com/cube-js/cube/issues/8241)) ([baf2667](https://github.com/cube-js/cube/commit/baf26677a0ceb50102516d3a7cce7543be4a57a2))
- Support probing `LIMIT 0` queries ([117f332](https://github.com/cube-js/cube/commit/117f3324a4db75c596d6e4b0a1cafffc76aab068))

## [0.35.30](https://github.com/cube-js/cube/compare/v0.35.29...v0.35.30) (2024-05-10)

### Bug Fixes

- **cubesql:** Add alias to rebased window expressions ([990a767](https://github.com/cube-js/cube/commit/990a767e3b2f32c0846907b1dfcff232227b4cbc))
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

### Bug Fixes

- **cubestore:** Queue - remove queue item payload on ack (reduce compaction) ([#8225](https://github.com/cube-js/cube/issues/8225)) ([ac58583](https://github.com/cube-js/cube/commit/ac5858384e26b9b9077e9ffeb42d64da874d1f62))
- **cubestore:** Queue - truncate unused queue rows via TTL ([#8227](https://github.com/cube-js/cube/issues/8227)) ([70ecbef](https://github.com/cube-js/cube/commit/70ecbefadc00eb857362ae647f136873a708bb80))

### Features

- **cubestore:** Allow to truncate queue item payloads via system command ([#8226](https://github.com/cube-js/cube/issues/8226)) ([75d4eac](https://github.com/cube-js/cube/commit/75d4eac3b48acfc02c8affacdf28f1f0897087b0))

## [0.35.26](https://github.com/cube-js/cube/compare/v0.35.25...v0.35.26) (2024-05-02)

### Bug Fixes

- **gateway:** Don't process meta with `onlyCompilerId` flag set ([76ae23e](https://github.com/cube-js/cube/commit/76ae23edbe0adc101c788f671b2f9760dd7c032d))
- **gateway:** missed v1 graphql-to-json endpoint ([a24d69b](https://github.com/cube-js/cube/commit/a24d69bd1db5da3b2773ad2f1a512cd72192c031))
- **gateway:** Optimize `filterVisibleItemsInMeta` performance ([b0fdbd3](https://github.com/cube-js/cube/commit/b0fdbd30d327c0a681a594fbf4ef063ef7a91f58))
- **schema-compiler:** foreign keys assignment ([#8208](https://github.com/cube-js/cube/issues/8208)) ([74affc4](https://github.com/cube-js/cube/commit/74affc40a70a4b6bb19f9ee438d6f4d0e4bd27fa))

### Features

- **schema-compiler:** hierarchies ([#8102](https://github.com/cube-js/cube/issues/8102)) ([c98683f](https://github.com/cube-js/cube/commit/c98683f07a286c6f4cf71c656e3e336295a0cc78))
- **schema-compiler:** Upgrade cron-parser to 4.9, fix [#8203](https://github.com/cube-js/cube/issues/8203) ([#8214](https://github.com/cube-js/cube/issues/8214)) ([f643c4b](https://github.com/cube-js/cube/commit/f643c4ba55e5f69c161dd687c79d7ce359fcf8e5))
- Support post-aggregate time shifts for prior period lookup support ([#8215](https://github.com/cube-js/cube/issues/8215)) ([a9f55d8](https://github.com/cube-js/cube/commit/a9f55d80ee36be6a197cdfed6e1d202a52c1a39d))

## [0.35.25](https://github.com/cube-js/cube/compare/v0.35.24...v0.35.25) (2024-04-29)

### Bug Fixes

- **cubesql:** Disallow ematching cycles ([4902c6d](https://github.com/cube-js/cube/commit/4902c6d3f882a0ee27f62d63d8b0fcda3669a0b1))
- **cubestore:** Error in optimization for nested boolean expressions ([#7891](https://github.com/cube-js/cube/issues/7891)) ([e5d20d2](https://github.com/cube-js/cube/commit/e5d20d228ac07849dbcd4df2f713ea01fc74f0fe))

### Features

- Rolling YTD, QTD and MTD measure support ([#8196](https://github.com/cube-js/cube/issues/8196)) ([bfc0708](https://github.com/cube-js/cube/commit/bfc0708502f78902e481608b4f83c2eee6182636))

## [0.35.24](https://github.com/cube-js/cube/compare/v0.35.23...v0.35.24) (2024-04-26)

### Bug Fixes

- **cubesql:** Fix `date_trunc` over column offset in filters ([1a602be](https://github.com/cube-js/cube/commit/1a602bea8fc17cc258c6d125ea8e7904b33454ff))
- **cubestore:** S3 - STS credentials (backport fix for 0.32.3) ([#8195](https://github.com/cube-js/cube/issues/8195)) ([9d9cb4a](https://github.com/cube-js/cube/commit/9d9cb4a025931da24d560220878b7450f81de22c))

## [0.35.23](https://github.com/cube-js/cube/compare/v0.35.22...v0.35.23) (2024-04-25)

### Bug Fixes

- **client-core:** castNumerics option for CubeApi ([#8186](https://github.com/cube-js/cube/issues/8186)) ([6ff2208](https://github.com/cube-js/cube/commit/6ff22088ede88521c6c5a5e8cb1a77c819301ad4))

### Features

- **api-gateway:** graphql json query conversion ([#8189](https://github.com/cube-js/cube/issues/8189)) ([55ea7c8](https://github.com/cube-js/cube/commit/55ea7c86f49163b6c65fb277966a0e8b572a5fd5))
- **cubesql:** In subquery rewrite ([#8162](https://github.com/cube-js/cube/issues/8162)) ([d17c2a7](https://github.com/cube-js/cube/commit/d17c2a7d58beada203009c5d624974d3a68c6af8))
- primary and foreign keys driver queries ([#8115](https://github.com/cube-js/cube/issues/8115)) ([35bb1d4](https://github.com/cube-js/cube/commit/35bb1d435a75f53f704e9c5e33382093cbc4e115))

## [0.35.22](https://github.com/cube-js/cube/compare/v0.35.21...v0.35.22) (2024-04-22)

### Bug Fixes

- **cubestore:** S3 - wrong credentials ([1313cdd](https://github.com/cube-js/cube/commit/1313cdd6d3882a7c4644e9d397df7745116246bb))

## [0.35.21](https://github.com/cube-js/cube/compare/v0.35.20...v0.35.21) (2024-04-19)

### Bug Fixes

- **cubestore:** MINIORemoteFs - use only simple auth from S3 client ([#8170](https://github.com/cube-js/cube/issues/8170)) ([66403ad](https://github.com/cube-js/cube/commit/66403ad1be6ec34d8b68444553c40dbc1543f3e5))
- Logical boolean measure filter for post aggregate measures support ([92cee95](https://github.com/cube-js/cube/commit/92cee954b1252b0234e3008fb74d88d7b1c3c47f))
- **native:** Better error message on unexpected values while streaming ([#8172](https://github.com/cube-js/cube/issues/8172)) ([bcf3438](https://github.com/cube-js/cube/commit/bcf3438682951d7b917866870fc029af3efe2362))

## [0.35.20](https://github.com/cube-js/cube/compare/v0.35.19...v0.35.20) (2024-04-18)

### Features

- Allow all calculated measure types in post aggregate measures ([66f3acc](https://github.com/cube-js/cube/commit/66f3accd2f4b84ccdc41ece68da5c0c73998df29))
- Post aggregate raw dimension with date range support ([23411cf](https://github.com/cube-js/cube/commit/23411cfe8152687b6a8ed02c073b151217c60e6a))

## [0.35.19](https://github.com/cube-js/cube/compare/v0.35.18...v0.35.19) (2024-04-18)

### Features

- **cubesql:** Update egg and remove some clones ([#8164](https://github.com/cube-js/cube/issues/8164)) ([fe20d35](https://github.com/cube-js/cube/commit/fe20d3575abc7105f827b979266e147299553aee))
- Post aggregate measure filter support ([120f22b](https://github.com/cube-js/cube/commit/120f22b4a52e5328ebfeea60e90b2b1aa1839feb))

## [0.35.18](https://github.com/cube-js/cube/compare/v0.35.17...v0.35.18) (2024-04-17)

### Features

- Post aggregate no granularity time dimension support ([f7a99f6](https://github.com/cube-js/cube/commit/f7a99f6e3f61b766e1581f2af574dc5d81f58101))
- Post aggregate view support ([#8161](https://github.com/cube-js/cube/issues/8161)) ([acb507d](https://github.com/cube-js/cube/commit/acb507d21d29b25592050bcf49c09fb9d88a7d93))

## [0.35.17](https://github.com/cube-js/cube/compare/v0.35.16...v0.35.17) (2024-04-16)

### Bug Fixes

- **cubestore:** Fix wrong optimization in total count queries ([#7964](https://github.com/cube-js/cube/issues/7964)) ([7a8ddf0](https://github.com/cube-js/cube/commit/7a8ddf056a075c9224657d1497e2679502acc515))
- ungrouped not getting applied for MSSQL ([#7997](https://github.com/cube-js/cube/issues/7997)) Thanks @UsmanYasin ! ([3fac97c](https://github.com/cube-js/cube/commit/3fac97cc4c72dc78b8016014cfa536f54c1be9d8))

### Features

- **cubestore:** Upgrade rust-s3 0.26.3 -> 0.32.3 ([#6019](https://github.com/cube-js/cube/issues/6019)) ([4b8d4d3](https://github.com/cube-js/cube/commit/4b8d4d3c3445dcd817945e3a71a70d0084644d4c))
- **cubestore:** Use S3 client in async way ([#8158](https://github.com/cube-js/cube/issues/8158)) ([7f286fe](https://github.com/cube-js/cube/commit/7f286fee90897048f8a2735a2a9e171d40e47da4))

## [0.35.16](https://github.com/cube-js/cube/compare/v0.35.15...v0.35.16) (2024-04-16)

### Features

- Post aggregate time dimensions support ([4071e8a](https://github.com/cube-js/cube/commit/4071e8a112823150e98703a2face4ccb98fd9e86))

## [0.35.15](https://github.com/cube-js/cube/compare/v0.35.14...v0.35.15) (2024-04-15)

### Features

- Post aggregate filter support ([49131ea](https://github.com/cube-js/cube/commit/49131ea8adb025b974ce6a4a596d6637df47bdea))

## [0.35.14](https://github.com/cube-js/cube/compare/v0.35.13...v0.35.14) (2024-04-15)

### Bug Fixes

- **server-core:** Handle schemaPath default value correctly everywhere (refix) ([#8152](https://github.com/cube-js/cube/issues/8152)) ([678f17f](https://github.com/cube-js/cube/commit/678f17f2ef5a2cf38b28e3c095a721f1cd38fb56))

## [0.35.13](https://github.com/cube-js/cube/compare/v0.35.12...v0.35.13) (2024-04-15)

### Features

- Post aggregate measure support ([#8145](https://github.com/cube-js/cube/issues/8145)) ([2648a26](https://github.com/cube-js/cube/commit/2648a26a6149c31615d5df105575e5c30117be6f))

## [0.35.12](https://github.com/cube-js/cube/compare/v0.35.11...v0.35.12) (2024-04-12)

### Bug Fixes

- **snowflake-driver:** Unable to detect a Project Id in the current environment for GCS export bucket ([03ca8fa](https://github.com/cube-js/cube/commit/03ca8fa9b7222ddaf0d52c1ead221ac26827c054))

### Reverts

- Revert "fix(server-core): Handle schemaPath default value correctly everywhere (#8131)" ([22a6f8b](https://github.com/cube-js/cube/commit/22a6f8b5e959db02f8eab89e2a9e873ef0a8ce87)), closes [#8131](https://github.com/cube-js/cube/issues/8131)

## [0.35.11](https://github.com/cube-js/cube/compare/v0.35.10...v0.35.11) (2024-04-11)

### Bug Fixes

- **cubesql:** Support latest Metabase, fix [#8105](https://github.com/cube-js/cube/issues/8105) ([#8130](https://github.com/cube-js/cube/issues/8130)) ([c82bb10](https://github.com/cube-js/cube/commit/c82bb10ed383f9c0cec04def8ac7dc53283f7254))
- **server-core:** Handle schemaPath default value correctly everywhere ([#8131](https://github.com/cube-js/cube/issues/8131)) ([23bd621](https://github.com/cube-js/cube/commit/23bd621541c1646ff6430deef9e0d7fc5f149f2f))

### Features

- **cubestore:** Enable jemalloc allocator for RocksDB on linux GNU ([#8104](https://github.com/cube-js/cube/issues/8104)) ([ee7bcee](https://github.com/cube-js/cube/commit/ee7bceeee43c45e0a183272b16b3932288146370))

## [0.35.10](https://github.com/cube-js/cube/compare/v0.35.9...v0.35.10) (2024-04-09)

### Bug Fixes

- **cubesql:** has_schema_privilege - compatiblity with PostgreSQL ([#8098](https://github.com/cube-js/cube/issues/8098)) ([42586cf](https://github.com/cube-js/cube/commit/42586cfae7f55c998f0034cd3c816f59ba4116df))

## [0.35.9](https://github.com/cube-js/cube/compare/v0.35.8...v0.35.9) (2024-04-08)

### Features

- **cubestore:** Upgrade rust to 1.77.0-nightly (635124704 2024-01-27) ([#7640](https://github.com/cube-js/cube/issues/7640)) ([5aa5abb](https://github.com/cube-js/cube/commit/5aa5abb6bba28b7d981c569982e80641673fd5b7))
- **docker:** Security upgrade Node.js from 18.19.1 to 18.20.1 ([#8094](https://github.com/cube-js/cube/issues/8094)) ([51921cc](https://github.com/cube-js/cube/commit/51921ccfd60ff739a638b32205e2f4eb48110789))

## [0.35.8](https://github.com/cube-js/cube/compare/v0.35.7...v0.35.8) (2024-04-05)

### Bug Fixes

- **schema-compiler:** quote case sensitive idents ([#8063](https://github.com/cube-js/cube/issues/8063)) ([6273532](https://github.com/cube-js/cube/commit/6273532611385d794257e7123d747f01b1c1c436))

### Features

- **cubestore:** Queue - optimize list/pending operations ([#7998](https://github.com/cube-js/cube/issues/7998)) ([815be2f](https://github.com/cube-js/cube/commit/815be2fb90bf84d095f2ab6b208167b8cc8433e7))

## [0.35.7](https://github.com/cube-js/cube/compare/v0.35.6...v0.35.7) (2024-04-03)

### Features

- **cross:** Upgrade LLVM from 14 to 18 (02042024) ([#8064](https://github.com/cube-js/cube/issues/8064)) ([9b30eac](https://github.com/cube-js/cube/commit/9b30eac57442af586d146bbb947b9dcba634cd60))
- **cubestore:** Upgrade LLVM from 14 to 18 ([#8068](https://github.com/cube-js/cube/issues/8068)) ([d1f0cbc](https://github.com/cube-js/cube/commit/d1f0cbc3c76ee5025aa26919a2563f85964a20a8))

## [0.35.6](https://github.com/cube-js/cube/compare/v0.35.5...v0.35.6) (2024-04-02)

### Bug Fixes

- **cubesql:** Remove binary expression associative rewrites ([19f6245](https://github.com/cube-js/cube/commit/19f62450f9fcf23b5484620783275564f2eedca4))
- **cubestore:** RocksStore - migrate table by truncate (unknown tables) ([#8053](https://github.com/cube-js/cube/issues/8053)) ([509d895](https://github.com/cube-js/cube/commit/509d89561eb23283fb0bda14ec03ca4d4a0a7aab))

### Features

- **cubestore:** CacheStore - allow to disable logs ([#8027](https://github.com/cube-js/cube/issues/8027)) ([ce6303c](https://github.com/cube-js/cube/commit/ce6303c5944f7ab0ac6c8411af26d396b0d3d7a8))
- Ungrouped query pre-aggregation support ([#8058](https://github.com/cube-js/cube/issues/8058)) ([2ca99de](https://github.com/cube-js/cube/commit/2ca99de3e7ea813bdb7ea684bed4af886bde237b))

## [0.35.5](https://github.com/cube-js/cube/compare/v0.35.4...v0.35.5) (2024-03-28)

### Bug Fixes

- **cubesql:** SQL push down of ORDER BY causes Invalid query format: "order[0][0]" with value fails to match the required pattern: /^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/ ([#8032](https://github.com/cube-js/cube/issues/8032)) ([0681725](https://github.com/cube-js/cube/commit/0681725c921ea62f7ef813562be0202e93928889))
- **cubestore:** RocksStore - improve panic safety on migration ([#8031](https://github.com/cube-js/cube/issues/8031)) ([f81e2cb](https://github.com/cube-js/cube/commit/f81e2cb1bbcb5f37d01b85bfbeef4b4634fe8332))
- Multiplied count measure can be used as additive measure in pre-… ([#8033](https://github.com/cube-js/cube/issues/8033)) ([f36959c](https://github.com/cube-js/cube/commit/f36959c297bd637c5332ba171a9049c3448b3522))
- **schema-compiler:** Upgrade babel/traverse to 7.24 (GHSA-67hx-6x53-jw92) ([#8002](https://github.com/cube-js/cube/issues/8002)) ([9848e51](https://github.com/cube-js/cube/commit/9848e51a2a5420bc7649775951ea122c03aa52e9))

## [0.35.4](https://github.com/cube-js/cube/compare/v0.35.3...v0.35.4) (2024-03-27)

### Bug Fixes

- **cubesql:** Flatten aliasing breaks wrapped select `ORDER BY` expressions ([#8004](https://github.com/cube-js/cube/issues/8004)) ([eea88ec](https://github.com/cube-js/cube/commit/eea88ecaf3267091d1fbdd761db0451775b9fbe9))
- **query-orchestrator:** Missing dataSource in pre-aggregation preview query ([#7989](https://github.com/cube-js/cube/issues/7989)) ([d3012c8](https://github.com/cube-js/cube/commit/d3012c8075382a4bf66156cb56765f0483d111dc))

### Features

- **cubesql:** Support `DISTINCT` push down ([fc79188](https://github.com/cube-js/cube/commit/fc79188b5a205d29d9acd3669677cc829b0a9013))

## [0.35.3](https://github.com/cube-js/cube/compare/v0.35.2...v0.35.3) (2024-03-22)

### Bug Fixes

- Render all distinct counts as `1` for ungrouped queries to not fail ratio measures ([#7987](https://github.com/cube-js/cube/issues/7987)) ([f756e4b](https://github.com/cube-js/cube/commit/f756e4bd7d48a0e60d505ac0de50ae1469637011))

## [0.35.2](https://github.com/cube-js/cube/compare/v0.35.1...v0.35.2) (2024-03-22)

### Bug Fixes

- **cubesql:** Fix timestamp parsing format string ([938b13c](https://github.com/cube-js/cube/commit/938b13c16706243efdb38f67e687c9778b8ca5f9))
- **mssql-driver:** Unsupported option opt.evictionRunIntervalMillis, refs [#7951](https://github.com/cube-js/cube/issues/7951) ([#7977](https://github.com/cube-js/cube/issues/7977)) ([53f013a](https://github.com/cube-js/cube/commit/53f013ab3ca1f3d7906e182b04cccd3aaabf4506))

### Features

- **cubesql:** Flatten rules to allow multiple level transformation queries be executed by split and SQL push down ([#7979](https://github.com/cube-js/cube/issues/7979)) ([f078adc](https://github.com/cube-js/cube/commit/f078adc38ce3eab39763253d137f291ee7625bf1))
- **cubesql:** Parse timestamp strings as `Date32` ([4ba5a80](https://github.com/cube-js/cube/commit/4ba5a80c6736bcd7e19afc88692c7b6713286850))
- **cubesql:** Support `a ^ b` exponentiation ([914b058](https://github.com/cube-js/cube/commit/914b05893a66269ef41fae3d5b81f903ad1a75e7))
- **cubestore:** Queue - reduce memory usage ([#7975](https://github.com/cube-js/cube/issues/7975)) ([e9848e8](https://github.com/cube-js/cube/commit/e9848e8d6140333dd029be4270b9450e1ca10450))
- **cubestore:** Reduce memory usage while writting WALs ([#7967](https://github.com/cube-js/cube/issues/7967)) ([ee9fa71](https://github.com/cube-js/cube/commit/ee9fa7162be6aa53593c316046a9f60d41ecbd28))
- **query-orchestrator:** Queue - improve performance ([#7983](https://github.com/cube-js/cube/issues/7983)) ([56d48fb](https://github.com/cube-js/cube/commit/56d48fb2bad6210f38cfcc6189b683dd2ead62c2))

### Reverts

- Revert "feat(schema-compiler): composite key generation (#7929)" ([61e4c45](https://github.com/cube-js/cube/commit/61e4c45024bdce7799a2161608b1fdc97e9bd9e8)), closes [#7929](https://github.com/cube-js/cube/issues/7929)

## [0.35.1](https://github.com/cube-js/cube/compare/v0.35.0...v0.35.1) (2024-03-18)

### Bug Fixes

- **cubestore:** Wrong calculation of default compaction trigger size (too often compaction) ([#7960](https://github.com/cube-js/cube/issues/7960)) ([2f4b75b](https://github.com/cube-js/cube/commit/2f4b75b3440f307e33699d4d26e90da9a880bf0a))

### Features

- **docker:** Security upgrade node from 18.19.0 to 18.19.1 ([#7924](https://github.com/cube-js/cube/issues/7924)) ([f33d03e](https://github.com/cube-js/cube/commit/f33d03ed293d489d109b3da774c22d8543c8a081))
- **duckdb-driver:** Upgrade DuckDB to v0.10 ([#7920](https://github.com/cube-js/cube/issues/7920)) ([0e9103a](https://github.com/cube-js/cube/commit/0e9103a78712e80118244306cfcbd670aac2c311))
- **mssql-driver:** Upgrade mssql to 10.0.2 (Node.js 17+ compatiblity), fix [#7951](https://github.com/cube-js/cube/issues/7951) ([7adaea6](https://github.com/cube-js/cube/commit/7adaea6522e47035e67596b4dc0ab15e87312706))
- **schema-compiler:** composite key generation ([#7929](https://github.com/cube-js/cube/issues/7929)) ([5a27200](https://github.com/cube-js/cube/commit/5a27200252cf837fc34ef38b320216d2e37ffa22))

# [0.35.0](https://github.com/cube-js/cube/compare/v0.34.62...v0.35.0) (2024-03-14)

### Bug Fixes

- **cubesql:** Don't ignore second filter over same time dimension ([03e7f3a](https://github.com/cube-js/cube/commit/03e7f3a6e68d911368fdba2a79387cbea995cae5))
- **cubestore-driver:** Compatibility with Node.js 18 (localhost resolved to ipv6) ([f7a4fab](https://github.com/cube-js/cube/commit/f7a4fabc28a81f238446c823794ae2938ecd7290))

### Features

- **docker:** Upgrade Node.js to 18.x ([8b5b923](https://github.com/cube-js/cube/commit/8b5b9237a6822498829ec7e9fba39a1e02b4b7bd))

## [0.34.62](https://github.com/cube-js/cube/compare/v0.34.61...v0.34.62) (2024-03-13)

### Bug Fixes

- **cubesql:** Fix push down column remapping ([8221a53](https://github.com/cube-js/cube/commit/8221a53a6f679ea9dcfce83df948173ccc89c5f0))
- Job API - correct handling on missed pre_aggregation ([#7907](https://github.com/cube-js/cube/issues/7907)) ([6fcaff4](https://github.com/cube-js/cube/commit/6fcaff4a9626f917c5efb2f837178c4244d75ff8))
- Job API - correct handling when job is not_found ([#7894](https://github.com/cube-js/cube/issues/7894)) ([cae1866](https://github.com/cube-js/cube/commit/cae1866dabd1bd13829221f6576a649117bcd142))
- **query-orchestrator:** Job API - builds are not triggered, fix [#7726](https://github.com/cube-js/cube/issues/7726) ([#7909](https://github.com/cube-js/cube/issues/7909)) ([738dbde](https://github.com/cube-js/cube/commit/738dbde3ab3767d935f733ab8770aaa8cba9c631))

## [0.34.61](https://github.com/cube-js/cube/compare/v0.34.60...v0.34.61) (2024-03-11)

### Bug Fixes

- **cubesql:** Error out when temporary table already exists ([1a0a324](https://github.com/cube-js/cube/commit/1a0a324274071b9dd9a667505c86698870e236b0))
- **cubesql:** Fix push down `CASE` with expr ([f1d1242](https://github.com/cube-js/cube/commit/f1d12428870cd6e1a159bd559a3402bb6be778aa))
- **cubesql:** Fix push down column remapping ([c7736cc](https://github.com/cube-js/cube/commit/c7736ccb2c3e1fd809d98adf58bad012fc38c8b7))
- **cubesql:** Partition pruning doesn't work when view uses proxy member ([#7866](https://github.com/cube-js/cube/issues/7866)) ([c02a07f](https://github.com/cube-js/cube/commit/c02a07f99141bcd549ffbbcaa37e95d138ff72a2)), closes [#6623](https://github.com/cube-js/cube/issues/6623)
- **cubesql:** Trim ".0" postfix when converting `Float to `Utf8` ([3131f94](https://github.com/cube-js/cube/commit/3131f94def0a73b29fd1638a40672377339dd7d3))
- Wrong sql generation when multiple primary keys used ([#6689](https://github.com/cube-js/cube/issues/6689)) Thanks [@alexanderlz](https://github.com/alexanderlz) ! ([2540a22](https://github.com/cube-js/cube/commit/2540a224f406b8fd3c09183b826fb899dad7a915))

### Features

- **clickhouse-driver:** Support export bucket for S3 (write only) ([#7849](https://github.com/cube-js/cube/issues/7849)) ([db7e2c1](https://github.com/cube-js/cube/commit/db7e2c10c97dc4b81d136bb1205931ea5eab2918))
- **clickhouse-driver:** Support S3 export for readOnly mode ([7889aad](https://github.com/cube-js/cube/commit/7889aadaefc2142f2a470839065860efb4858b63))
- **cubesql:** Support `FETCH ... n ROWS ONLY` ([53b0c14](https://github.com/cube-js/cube/commit/53b0c149c7e348cfb4b36890d72c5090762e717b))
- **query-orchestrator:** Support unload (export bucket) for readOnly drivers ([76ce7ed](https://github.com/cube-js/cube/commit/76ce7ed176e0c8097bc8024a7e5e82bbb48d5d1c))

### Reverts

- Revert "fix(cubesql): Fix push down column remapping" (#7895) ([dec8901](https://github.com/cube-js/cube/commit/dec8901493814f3028e38e0ed17f5868e6ecd7a5)), closes [#7895](https://github.com/cube-js/cube/issues/7895)

## [0.34.60](https://github.com/cube-js/cube/compare/v0.34.59...v0.34.60) (2024-03-02)

### Bug Fixes

- Bump max connect retries for Cube Store ([00a6ec0](https://github.com/cube-js/cube/commit/00a6ec034c2d5dff8c66647b7ec6cae39c0237ba))
- **client-core:** incorrect index in movePivotItem ([#6684](https://github.com/cube-js/cube/issues/6684)) Thanks to [@telunc](https://github.com/telunc)! ([82217b2](https://github.com/cube-js/cube/commit/82217b28a012158cd8a978fd2e1ad15746ddeae0))
- **cubesql:** Allow different `Timestamp` types in `DATEDIFF` ([de9ef08](https://github.com/cube-js/cube/commit/de9ef081cae21f551a38219bb64749e08e7ca6fc))
- **cubesql:** Incorrect CASE WHEN generated during ungrouped filtered count ([#7859](https://github.com/cube-js/cube/issues/7859)) ([f970937](https://github.com/cube-js/cube/commit/f9709376e5dadfbcbffe9a65dfa9f0ea16df7bab)), closes [#7858](https://github.com/cube-js/cube/issues/7858)
- **cubesql:** Prioritize ungrouped aggregate scans over ungrouped projection scans so most of the members can be pushed down without wrapping ([#7865](https://github.com/cube-js/cube/issues/7865)) ([addde0d](https://github.com/cube-js/cube/commit/addde0d373f01e69485b8a0333850917ffad9a2d))
- **cubesql:** Remove excessive limit on inner wrapped queries ([#7864](https://github.com/cube-js/cube/issues/7864)) ([b97268f](https://github.com/cube-js/cube/commit/b97268fe5caf55c5b7806c597b9f7b75410f6ba4))

### Features

- **cubesql:** In subquery support ([#7851](https://github.com/cube-js/cube/issues/7851)) ([8e2a3ec](https://github.com/cube-js/cube/commit/8e2a3ecc348c4ab9e6a5ab038c46fcf7f4c3dfcc))
- **cubesql:** Support split and SUM(1) ([16e2ee0](https://github.com/cube-js/cube/commit/16e2ee0d290c502f796891e137556aad2275e52d))

## [0.34.59](https://github.com/cube-js/cube/compare/v0.34.58...v0.34.59) (2024-02-28)

### Bug Fixes

- **cubesql:** Replace only simple ungrouped measures in projections to avoid aggregate over aggregate statements ([#7852](https://github.com/cube-js/cube/issues/7852)) ([fa2a89b](https://github.com/cube-js/cube/commit/fa2a89b89f91c8eba175130fca33975200690288))
- Ungrouped filtered count should contain `CASE WHEN` statement bu… ([#7853](https://github.com/cube-js/cube/issues/7853)) ([8c37a40](https://github.com/cube-js/cube/commit/8c37a407b9d8c8f97dab51c9e0e97067c91ff90e))

## [0.34.58](https://github.com/cube-js/cube/compare/v0.34.57...v0.34.58) (2024-02-27)

### Bug Fixes

- **clickhouse-driver:** Correct support for Enum8/16 ([#7814](https://github.com/cube-js/cube/issues/7814)) ([db37fc3](https://github.com/cube-js/cube/commit/db37fc3b0f24511bd9ae3a319b5755c102bb3358))
- **cubesql:** Ambiguous reference in case of same two 16 char prefixes are in query ([5051f66](https://github.com/cube-js/cube/commit/5051f663b4142735ee1cc455936300f765de62a7))
- **cubesql:** Continue wait error is being thrown to users ([cb2376c](https://github.com/cube-js/cube/commit/cb2376c1d2cc0a86aa329072fb2ec02ddadfeaf8))

### Features

- **materialize-driver:** Add cluster option to Materialize driver ([#7773](https://github.com/cube-js/cube/issues/7773)) Thanks [@bobbyiliev](https://github.com/bobbyiliev)! ([917004e](https://github.com/cube-js/cube/commit/917004ebccfeb6509a08e5b9b51e1ae8542cf0af))

## [0.34.57](https://github.com/cube-js/cube/compare/v0.34.56...v0.34.57) (2024-02-26)

### Bug Fixes

- **cubesql:** `ungrouped` query can be routed to Cube Store ([#7810](https://github.com/cube-js/cube/issues/7810)) ([b122837](https://github.com/cube-js/cube/commit/b122837de9cd4fcaca4ddc0e7f85ff695de09483))
- **cubesql:** Can't find rewrite due to AST node limit reached for remaining non-equivalent date and timestamp constant folding rules ([b79f697](https://github.com/cube-js/cube/commit/b79f69739e2b8cb74cfc86db72fa7157dd723960))
- **cubesql:** More readable error message during SQL generation ([d9a7194](https://github.com/cube-js/cube/commit/d9a719479d6ca00ef5b2b511677cd777ceb131e7))
- **cubesql:** Support `Z` postfix in `Timestamp` response transformation ([c013c91](https://github.com/cube-js/cube/commit/c013c913bbeb8dabacc508240ab94956f1172a8b))
- **cubesql:** Timestamp equals filter support ([754a0df](https://github.com/cube-js/cube/commit/754a0df3a20cae5269e961649a01fb5047906645))
- **cubesql:** writerOrChannel.resolve is not a function ([abc95e2](https://github.com/cube-js/cube/commit/abc95e259c82894aea63371e468c98d640b4d42e))

### Features

- **cubesql:** `WHERE` SQL push down ([#7808](https://github.com/cube-js/cube/issues/7808)) ([98b5709](https://github.com/cube-js/cube/commit/98b570946905586f16a502f83b0a1cf8e4aa92a6))
- **cubesql:** Allow replacement of aggregation functions in SQL push down ([#7811](https://github.com/cube-js/cube/issues/7811)) ([97fa757](https://github.com/cube-js/cube/commit/97fa757a0d22e6d7d9432d686005765b28271f7c))
- **cubesql:** Support placeholders in `OFFSET`, `FETCH ...` ([60aad90](https://github.com/cube-js/cube/commit/60aad90a237800f4471bb4efa10ec590b50e19fe))
- **cubesql:** Support temporary tables ([7022611](https://github.com/cube-js/cube/commit/702261156fc3748fc7d3103f28bd4f4648fd4e0b))
- **duckdb-driver:** Add support for local fs duckdb database path ([#7799](https://github.com/cube-js/cube/issues/7799)) ([77cc883](https://github.com/cube-js/cube/commit/77cc883a3d73d37730a82ecf9128b8929abb2ca3))

## [0.34.56](https://github.com/cube-js/cube/compare/v0.34.55...v0.34.56) (2024-02-20)

### Bug Fixes

- **cubesql:** Allow `NULL` values in `CASE` ([a97acdc](https://github.com/cube-js/cube/commit/a97acdc996dd68a0e2c00c155dfe30a863440ecc))
- **cubesql:** Streaming still applies default limit ([a96831d](https://github.com/cube-js/cube/commit/a96831dc7157cf48ae4dc0cd3a81d9f44557e47e))
- Gracefully handle logical expressions in FILTER_PARAMS and introduce FILTER_GROUP to allow use FILTER_PARAMS for logical expressions ([#7766](https://github.com/cube-js/cube/issues/7766)) ([736070d](https://github.com/cube-js/cube/commit/736070d5a33c25d212420a61da6c98a4b4d31188))

## [0.34.55](https://github.com/cube-js/cube/compare/v0.34.54...v0.34.55) (2024-02-15)

### Bug Fixes

- Cannot read properties of null (reading 'includes') ([9b50382](https://github.com/cube-js/cube/commit/9b503827a8f4cf18b926dfec4e65a0dce0de0b70))
- **cubesql:** Quote `FROM` alias for SQL push down to avoid name clas… ([#7755](https://github.com/cube-js/cube/issues/7755)) ([4e2732a](https://github.com/cube-js/cube/commit/4e2732ae9997762a95fc946a5392b50e4dbf8622))

### Features

- **cubesql:** Strings with Unicode Escapes ([#7756](https://github.com/cube-js/cube/issues/7756)) ([49acad5](https://github.com/cube-js/cube/commit/49acad51e6a2b7ffa9ec0b584aaaa6e54f4f1434))

## [0.34.54](https://github.com/cube-js/cube/compare/v0.34.53...v0.34.54) (2024-02-13)

### Bug Fixes

- **cubesql:** Cannot read properties of undefined (reading 'preAggregation') with enabled SQL push down ([ac2cc17](https://github.com/cube-js/cube/commit/ac2cc177fc5d53a213603efd849acb2b4abf7a48))

## [0.34.53](https://github.com/cube-js/cube/compare/v0.34.52...v0.34.53) (2024-02-13)

### Bug Fixes

- **cubesql:** SQL push down can be incorrectly routed to Cube Store pre-aggregations ([#7752](https://github.com/cube-js/cube/issues/7752)) ([1a6451c](https://github.com/cube-js/cube/commit/1a6451c2c0bef6f5efbba2870ec0a11238b1addb))

## [0.34.52](https://github.com/cube-js/cube/compare/v0.34.51...v0.34.52) (2024-02-13)

### Bug Fixes

- **cubesql:** Support new Metabase meta queries ([0bc09fd](https://github.com/cube-js/cube/commit/0bc09fdbefaf8b4e184f4c0803919103789acb0a))
- **cubestore:** Error in queries with GROUP BY DIMENSION ([#7743](https://github.com/cube-js/cube/issues/7743)) ([b9fcc60](https://github.com/cube-js/cube/commit/b9fcc6007864303cbc662a76e7bed1f119b9fc7d))
- **cubestore:** Use file_name instead of id for in memory chunks ([#7704](https://github.com/cube-js/cube/issues/7704)) ([9e2e925](https://github.com/cube-js/cube/commit/9e2e9258fb1d4824e71a423d6a10cfc9df053ca1))

### Features

- **cubesql:** Always Prefer SQL push down over aggregation in Datafusion ([#7751](https://github.com/cube-js/cube/issues/7751)) ([b4b0f05](https://github.com/cube-js/cube/commit/b4b0f05d16eb08f2be3831a5d48468be8d8b9d76))
- **cubesql:** EXTRACT(EPOCH, ...) support ([#7734](https://github.com/cube-js/cube/issues/7734)) ([b4deacd](https://github.com/cube-js/cube/commit/b4deacddf0a2e06b0b1f4216ca735a41e52724e2))
- **cubesql:** Support `TimestampNanosecond` in `CASE` ([69aed08](https://github.com/cube-js/cube/commit/69aed0875afb3b2d56176f4bdfdee7b1acd17ce9))
- **cubesql:** Support Athena OFFSET and LIMIT push down ([00c2a6b](https://github.com/cube-js/cube/commit/00c2a6b3cc88fde5d65d02549d3458818e4a8e42))
- **duckdb-driver:** pass Cube user agent ([6b7aff6](https://github.com/cube-js/cube/commit/6b7aff6afb07ffffdb036f6cd1fbc24bd6ae7cb9))

## [0.34.51](https://github.com/cube-js/cube/compare/v0.34.50...v0.34.51) (2024-02-11)

### Bug Fixes

- **cubesql:** Enable constant folding for unary minus exprs ([704d05a](https://github.com/cube-js/cube/commit/704d05aecafada282a6bcbd57bd0519bb5a12aa5))
- **cubesql:** Fix `CASE` type with `NULL` values ([2b7cc30](https://github.com/cube-js/cube/commit/2b7cc304b101330ca072fa0c4a3a6e9ae9efa2a5))
- **cubesql:** Stabilize split operations for SQL push down ([#7725](https://github.com/cube-js/cube/issues/7725)) ([6241e5e](https://github.com/cube-js/cube/commit/6241e5e9335148947a687f7e3d6b56929ba46c36))
- **native:** Don't crash after query cancellation ([40726ba](https://github.com/cube-js/cube/commit/40726ba3ae27a53a4e6531886513b545a27b1858))
- **native:** Handle JsAsyncChannel error (without panic) ([#7729](https://github.com/cube-js/cube/issues/7729)) ([5480978](https://github.com/cube-js/cube/commit/5480978295232d5cd7b3baf2fe4c48b005338f06))
- **playground:** replace antd daterange picker ([#7735](https://github.com/cube-js/cube/issues/7735)) ([484f1ca](https://github.com/cube-js/cube/commit/484f1ca0d0fa67a148f2db01041837a95d35c1c5))

### Features

- Add `ungrouped` flag to GraphQL API ([#7746](https://github.com/cube-js/cube/issues/7746)) ([6111fbb](https://github.com/cube-js/cube/commit/6111fbbffb8486a1a19ef0846967abe3fa285ad3))
- **cubesql:** Extend `DATEDIFF` push down support ([ecaaf1c](https://github.com/cube-js/cube/commit/ecaaf1ca5566ff7cc27289468440e6a902a09609))
- **cubestore:** CREATE TABLE IF NOT EXISTS support ([#7451](https://github.com/cube-js/cube/issues/7451)) ([79c5e92](https://github.com/cube-js/cube/commit/79c5e92e6aed04b59e88ed61bd4a6dcdbcfb0b92))

## [0.34.50](https://github.com/cube-js/cube/compare/v0.34.49...v0.34.50) (2024-01-31)

### Bug Fixes

- **cubestore:** Single job process don't stop after job done ([#7706](https://github.com/cube-js/cube/issues/7706)) ([b84a345](https://github.com/cube-js/cube/commit/b84a34592648f37bbdf77b9d8691e44a6e3d8f27))
- Merge streaming methods to one interface to allow SQL API use batching and Databricks batching implementation ([#7695](https://github.com/cube-js/cube/issues/7695)) ([73ad72d](https://github.com/cube-js/cube/commit/73ad72dd8a104651cf5ef23a60e8b4c116c97eed))
- **playground:** replace antd popover for settings ([#7699](https://github.com/cube-js/cube/issues/7699)) ([56b44ee](https://github.com/cube-js/cube/commit/56b44eefe02fa21fe0ae89d2bc45ee83f20a5210))

## [0.34.49](https://github.com/cube-js/cube/compare/v0.34.48...v0.34.49) (2024-01-26)

### Bug Fixes

- Revert drillMembers include in views as it's a breaking change. Need to re-introduce it through evaluation on cube level and validating drillMembers are included in view ([#7692](https://github.com/cube-js/cube/issues/7692)) ([6cbbaa0](https://github.com/cube-js/cube/commit/6cbbaa009c87eef4250cebca79de3c9cbb2ace11))

## [0.34.48](https://github.com/cube-js/cube/compare/v0.34.47...v0.34.48) (2024-01-25)

### Bug Fixes

- **cubesql:** Fix unary minus operator precedence ([d5a935a](https://github.com/cube-js/cube/commit/d5a935ac3bb16c1dda6c30982cdc9ef787a24967))
- **cubesql:** Segment mixed with a filter and a date range filter may affect push down of `inDateRange` filter to time dimension ([#7684](https://github.com/cube-js/cube/issues/7684)) ([f29a7be](https://github.com/cube-js/cube/commit/f29a7be8379097b8de657ebc2e46f40bae3ccce9))
- **cubesql:** Support Sigma Sunday week granularity ([3d492eb](https://github.com/cube-js/cube/commit/3d492eb5feb84503a1bffda7481ed8b562939e44))
- Join order is incorrect for snowflake schema views with defined join paths ([#7689](https://github.com/cube-js/cube/issues/7689)) ([454d456](https://github.com/cube-js/cube/commit/454d4563120f591445c219c42dd5bd2fd937f7a6)), closes [#7663](https://github.com/cube-js/cube/issues/7663)

### Features

- **cubesql:** Support KPI chart in Thoughtspot ([dbab39e](https://github.com/cube-js/cube/commit/dbab39e63a1c752a56a2cb06169a479a3e9cb11e))
- **cubesql:** Support unwrapping BINARY expr from SUM(<expr>) ([#7683](https://github.com/cube-js/cube/issues/7683)) ([ce93cc7](https://github.com/cube-js/cube/commit/ce93cc7a0f667409d725b34913405f18d18f629b))
- View members inherit title, format and drillMembers attribute ([#7617](https://github.com/cube-js/cube/issues/7617)) Thanks @CallumWalterWhite ! ([302a756](https://github.com/cube-js/cube/commit/302a7560fd65e1437249701e1fc42b32da5df8fa))

## [0.34.47](https://github.com/cube-js/cube/compare/v0.34.46...v0.34.47) (2024-01-23)

### Features

- **cubesql:** TO_CHAR - support more formats, correct NULL handling ([#7671](https://github.com/cube-js/cube/issues/7671)) ([2d1e2d2](https://github.com/cube-js/cube/commit/2d1e2d216c99af68a2d5cf1b2acd2f5e2a623323))
- **duckdb-driver:** Support advanced configuration for S3 via env variables ([#7544](https://github.com/cube-js/cube/issues/7544)) ([4862195](https://github.com/cube-js/cube/commit/4862195d57a8796c3bbd74696eb41df4505e545c))

## [0.34.46](https://github.com/cube-js/cube/compare/v0.34.45...v0.34.46) (2024-01-18)

### Bug Fixes

- **cubejs-client/react:** types for useCubeQuery ([#7668](https://github.com/cube-js/cube/issues/7668)) Thanks [@tchell](https://github.com/tchell) ! ([2f95f76](https://github.com/cube-js/cube/commit/2f95f76031e4419998bd2001c10d78ec4b75ef2e))

### Features

- **cubesql:** Cache plan rewrites with and without replaced parameters ([#7670](https://github.com/cube-js/cube/issues/7670)) ([c360d3c](https://github.com/cube-js/cube/commit/c360d3c9da61b45f8215cc17db098cfa0a74c899))

## [0.34.45](https://github.com/cube-js/cube/compare/v0.34.44...v0.34.45) (2024-01-16)

### Features

- **cubesql:** Query rewrite cache ([#7647](https://github.com/cube-js/cube/issues/7647)) ([79888af](https://github.com/cube-js/cube/commit/79888afc3823a3ef29ba76c440828c8c5d719ae4))

## [0.34.44](https://github.com/cube-js/cube/compare/v0.34.43...v0.34.44) (2024-01-15)

### Bug Fixes

- **query-orchestrator:** QueryQueue - reduce trafic (unexpected call for query stage) ([#7646](https://github.com/cube-js/cube/issues/7646)) ([ecf3826](https://github.com/cube-js/cube/commit/ecf3826df0e8f7ff5855b725ded4ac7d4f566582))

### Features

- **query-orchestrator:** Queue - reduce traffic for processing (Cube Store only) ([#7644](https://github.com/cube-js/cube/issues/7644)) ([7db90fb](https://github.com/cube-js/cube/commit/7db90fb7532d53d7ce46ba342e89250110f32c6b))

## [0.34.43](https://github.com/cube-js/cube/compare/v0.34.42...v0.34.43) (2024-01-11)

### Bug Fixes

- **playground:** replace antd overlay ([#7645](https://github.com/cube-js/cube/issues/7645)) ([bb6a7d3](https://github.com/cube-js/cube/commit/bb6a7d3ff4b6d6b7972c196877749d3dbc2aecf6))

## [0.34.42](https://github.com/cube-js/cube/compare/v0.34.41...v0.34.42) (2024-01-07)

### Features

- **cubesql:** Compiler cache for rewrite rules ([#7604](https://github.com/cube-js/cube/issues/7604)) ([995889f](https://github.com/cube-js/cube/commit/995889fb7722cda3bf839095949d6d71693dd329))

## [0.34.41](https://github.com/cube-js/cube/compare/v0.34.40...v0.34.41) (2024-01-02)

### Bug Fixes

- **@cubejs-client/ngx:** set module to fesm2015 ([#7571](https://github.com/cube-js/cube/issues/7571)) Thanks [@loremaps](https://github.com/loremaps)! ([aaa8bfd](https://github.com/cube-js/cube/commit/aaa8bfd23e91ff033b4c048c4e6ee4e419c45d98)), closes [#7570](https://github.com/cube-js/cube/issues/7570)
- **cubesql:** Enable `Visitor` on `GROUP BY` expressions ([#7575](https://github.com/cube-js/cube/issues/7575)) ([bcc1a89](https://github.com/cube-js/cube/commit/bcc1a8911fe99f33b0a82e865597dec38101ecad))
- **cubestore:** Panic in hash join ([#7573](https://github.com/cube-js/cube/issues/7573)) ([aa6c08b](https://github.com/cube-js/cube/commit/aa6c08b65dbb16c1008a46fbeca7590715bca46b))
- **databricks-jdbc-driver:** Time series queries with rolling window & time dimension ([#7564](https://github.com/cube-js/cube/issues/7564)) ([79d033e](https://github.com/cube-js/cube/commit/79d033eecc54c4ae5a4e04ed7713fb34957af091))
- Do not fail extractDate on missing result set ([c8567ef](https://github.com/cube-js/cube/commit/c8567eff0a8ccafe04be2948331c11f80bfeab09))

### Features

- **cubesql:** Support Domo data queries ([#7509](https://github.com/cube-js/cube/issues/7509)) ([6d644dc](https://github.com/cube-js/cube/commit/6d644dc5265245b8581eb2c2e3b75f5d6d9f929c))

## [0.34.40](https://github.com/cube-js/cube/compare/v0.34.39...v0.34.40) (2023-12-21)

### Features

- **cubesql:** Do not run split re-aggregate for trivial push down to improve wide table queries ([#7567](https://github.com/cube-js/cube/issues/7567)) ([8dbf879](https://github.com/cube-js/cube/commit/8dbf87986cd58f4860d647d5a0bb33e64a229db1))

## [0.34.39](https://github.com/cube-js/cube/compare/v0.34.38...v0.34.39) (2023-12-21)

### Bug Fixes

- **base-driver:** Support parsing intervals with month|quarter|year granularity ([#7561](https://github.com/cube-js/cube/issues/7561)) ([24c850c](https://github.com/cube-js/cube/commit/24c850ccb74b08f74b5314ed97f872b95cb46b43))
- **clickhouse-driver:** Correct parsing for DateTime('timezone') ([#7565](https://github.com/cube-js/cube/issues/7565)) ([d39e4a2](https://github.com/cube-js/cube/commit/d39e4a25f2982cd2f66434f8095905883b9815ff))

### Features

- **jdbc-driver:** Upgrade java to ^0.14 (to support new JDK & Node.js) ([#7566](https://github.com/cube-js/cube/issues/7566)) ([fe10c9b](https://github.com/cube-js/cube/commit/fe10c9b07ef88162044d86f0552f30b61cc618ed))

## [0.34.38](https://github.com/cube-js/cube/compare/v0.34.37...v0.34.38) (2023-12-19)

### Bug Fixes

- **dremio-driver:** Fix generation of time series SQL ([#7503](https://github.com/cube-js/cube/issues/7503)) ([7e84d4c](https://github.com/cube-js/cube/commit/7e84d4c0eb3639893faf11a10a11316b9870a0d5))

### Features

- **native:** NodeObjSerializer - support serializing sequences ([#7560](https://github.com/cube-js/cube/issues/7560)) ([c19679d](https://github.com/cube-js/cube/commit/c19679d740684fd616ff4d2ce35fe6d95b65160e))

## [0.34.37](https://github.com/cube-js/cube/compare/v0.34.36...v0.34.37) (2023-12-19)

### Bug Fixes

- **clickhouse-driver:** Initial support for DateTime64, fix [#7537](https://github.com/cube-js/cube/issues/7537) ([#7538](https://github.com/cube-js/cube/issues/7538)) ([401e9e1](https://github.com/cube-js/cube/commit/401e9e1b9c07e115804a1f84fade2bb82b55ca29))
- Internal: Error during planning: No field named for pre-aggregat… ([#7554](https://github.com/cube-js/cube/issues/7554)) ([412213c](https://github.com/cube-js/cube/commit/412213cbec40748d0de6f54731686c3d0b263e5c))

### Features

- **client-core:** Add meta field for Cube type definition ([#7527](https://github.com/cube-js/cube/issues/7527)) ([75e201d](https://github.com/cube-js/cube/commit/75e201df5b64538da24103ccf82411b6f46006da))
- **cubesql:** Avoid pushing split down for trivial selects to optimi… ([#7556](https://github.com/cube-js/cube/issues/7556)) ([2bf86e5](https://github.com/cube-js/cube/commit/2bf86e5a70810f5f081a527d6e7b70c8020673aa))
- **native:** Support Python 3.12 ([#7550](https://github.com/cube-js/cube/issues/7550)) ([618a81e](https://github.com/cube-js/cube/commit/618a81ea3075473f05dba8a2114358b9b9af4d82))

## [0.34.36](https://github.com/cube-js/cube/compare/v0.34.35...v0.34.36) (2023-12-16)

### Bug Fixes

- **cubesql:** Improve performance for wide table querying ([#7534](https://github.com/cube-js/cube/issues/7534)) ([0f877d4](https://github.com/cube-js/cube/commit/0f877d41f08aeb1ebc9b22e9b38da931152435d2))

## [0.34.35](https://github.com/cube-js/cube/compare/v0.34.34...v0.34.35) (2023-12-13)

### Bug Fixes

- **cubesql:** Support Sigma Computing table schema sync ([d87bd19](https://github.com/cube-js/cube/commit/d87bd19384e25a161fb2424b3b6c01da675de04e))

### Features

- Add meta property for cube definition ([#7327](https://github.com/cube-js/cube/issues/7327)) Thanks [@mharrisb1](https://github.com/mharrisb1) ! ([0ea12c4](https://github.com/cube-js/cube/commit/0ea12c440b3b3fb8280063a1da7d7f9e6b9b6754))
- **cubesql:** Additional trace event logging for SQL API ([#7524](https://github.com/cube-js/cube/issues/7524)) ([6b700cd](https://github.com/cube-js/cube/commit/6b700cd493b16d4450ce1efaa449207836a47592))

## [0.34.34](https://github.com/cube-js/cube/compare/v0.34.33...v0.34.34) (2023-12-12)

### Bug Fixes

- **api-gateway:** Catch error from contextToApiScopes ([#7521](https://github.com/cube-js/cube/issues/7521)) ([0214472](https://github.com/cube-js/cube/commit/0214472dabf39b8429e11f4830350b2a6bd305a0))
- **api-gateway:** Fallback to global error middelware for async handlers ([#7520](https://github.com/cube-js/cube/issues/7520)) ([74b0862](https://github.com/cube-js/cube/commit/74b0862800ada31681e702992ff1f7da89924b9f))
- **api-gateway:** Removed hasOwnProperty check for request object ([#7517](https://github.com/cube-js/cube/issues/7517)) ([2129a1b](https://github.com/cube-js/cube/commit/2129a1b5bac1e2863e8136adb061708d8660354e))
- **deps:** Upgrade qs (CVE-2022-24999) ([#7518](https://github.com/cube-js/cube/issues/7518)) ([55422a1](https://github.com/cube-js/cube/commit/55422a1b340c8ca067e783952aaed041ab6aed40))

## [0.34.33](https://github.com/cube-js/cube/compare/v0.34.32...v0.34.33) (2023-12-11)

### Features

- **duckdb-driver:** Declare user agent information ([#7490](https://github.com/cube-js/cube/issues/7490)) ([59557e8](https://github.com/cube-js/cube/commit/59557e8492e4338c457b129c6c59bde542410577))
- **query-orchestrator:** Reduce number of touches for pre-aggregations ([#7515](https://github.com/cube-js/cube/issues/7515)) ([c22eb91](https://github.com/cube-js/cube/commit/c22eb9183c6c7c02e137662ee935f418ef45f02a))

## [0.34.32](https://github.com/cube-js/cube/compare/v0.34.31...v0.34.32) (2023-12-07)

### Bug Fixes

- **query-orchestrator:** Queue - clear timer on streaming ([#7501](https://github.com/cube-js/cube/issues/7501)) ([ac2357d](https://github.com/cube-js/cube/commit/ac2357dd52f3316dc33158795388067097c32ece))

### Features

- **cubesql:** Add CURRENT_DATE function definitions for Postgres and Snowflake so those can be pushed down ([ac1ca88](https://github.com/cube-js/cube/commit/ac1ca889add970dd05820b8f077edb968c985994))

## [0.34.31](https://github.com/cube-js/cube/compare/v0.34.30...v0.34.31) (2023-12-07)

### Bug Fixes

- **cubesql:** Avoid constant folding for current_date() function duri… ([#7498](https://github.com/cube-js/cube/issues/7498)) ([e86f4be](https://github.com/cube-js/cube/commit/e86f4be42a6e48a115c2765e0cda84fbf1cc56e7))

### Features

- **cubesql:** Support `Utf8 * Interval` expression ([ea1fa9c](https://github.com/cube-js/cube/commit/ea1fa9ca6e04cf12b4c334b5702d7a5a33f0c364))

## [0.34.30](https://github.com/cube-js/cube/compare/v0.34.29...v0.34.30) (2023-12-04)

### Bug Fixes

- **cubestore:** InplaceAggregate for boolean columns ([#7432](https://github.com/cube-js/cube/issues/7432)) ([a6d3579](https://github.com/cube-js/cube/commit/a6d3579c6930763edc0f72eabdb7892b8675a5c5))
- View tries to join cube twice in case there's join ambiguity ([#7487](https://github.com/cube-js/cube/issues/7487)) ([66f7c17](https://github.com/cube-js/cube/commit/66f7c17a19fb594a8c71ff6d449f25a5a9e727aa))

## [0.34.29](https://github.com/cube-js/cube/compare/v0.34.28...v0.34.29) (2023-12-01)

### Bug Fixes

- **schema-compiler:** td.isDateOperator is not a function ([#7483](https://github.com/cube-js/cube/issues/7483)) ([31c272a](https://github.com/cube-js/cube/commit/31c272ae929bc8e7f7972e46666f84bf8509cc3f))

## [0.34.28](https://github.com/cube-js/cube/compare/v0.34.27...v0.34.28) (2023-11-30)

### Features

- **server-core:** Relative imports resolution for custom drivers ([#7480](https://github.com/cube-js/cube/issues/7480)) ([8f180b3](https://github.com/cube-js/cube/commit/8f180b38e42bed61ebf2dc510631e5390258e8ff))

## [0.34.27](https://github.com/cube-js/cube/compare/v0.34.26...v0.34.27) (2023-11-30)

### Features

- **client-ngx:** enable ivy ([#7479](https://github.com/cube-js/cube/issues/7479)) ([a3c2bbb](https://github.com/cube-js/cube/commit/a3c2bbb760a166d796f4080913bdfc1d352af8bd))
- **cubesql:** Provide password supplied by Postgres connection as a 3rd argument of `check_sql_auth()` ([#7471](https://github.com/cube-js/cube/issues/7471)) ([ee3c19f](https://github.com/cube-js/cube/commit/ee3c19f8d467056c90ee407b3ac386dc1892b678)), closes [#5430](https://github.com/cube-js/cube/issues/5430)

## [0.34.26](https://github.com/cube-js/cube/compare/v0.34.25...v0.34.26) (2023-11-28)

### Bug Fixes

- **cubesql:** Missing template backslash escaping ([#7465](https://github.com/cube-js/cube/issues/7465)) ([4a08de5](https://github.com/cube-js/cube/commit/4a08de5791f7353b925c60ee84d2654e95e7967a))
- **query-orchestartor:** Queue - fix possible memory leak with timer ([#7466](https://github.com/cube-js/cube/issues/7466)) ([535a676](https://github.com/cube-js/cube/commit/535a6765346e08c5848d293c481d9abfa417ca78))
- **schema-compiler:** Incorrect excludes resolution, fix [#7100](https://github.com/cube-js/cube/issues/7100) ([#7468](https://github.com/cube-js/cube/issues/7468)) ([f89ef8d](https://github.com/cube-js/cube/commit/f89ef8d638fafa760ea6415bc670c0c3939ea06b))
- **schema-compiler:** Missing pre-aggregation filter for partitioned pre-aggregation via view, fix [#6623](https://github.com/cube-js/cube/issues/6623) ([#7454](https://github.com/cube-js/cube/issues/7454)) ([567b92f](https://github.com/cube-js/cube/commit/567b92fe716bcc11b19d42815699b476339ef201))

### Features

- **cubesql:** Support SQL push down for several functions ([79e5ac8](https://github.com/cube-js/cube/commit/79e5ac8e998005ebf8b5f72ccf1d63f425f6003c))
- **cubestore-driver:** Queue - protect possible race condition with hearbeat/cancellation on error ([#7467](https://github.com/cube-js/cube/issues/7467)) ([9c0ad07](https://github.com/cube-js/cube/commit/9c0ad0746d54543e264789dd36fa0ce2b91b7626))

## [0.34.25](https://github.com/cube-js/cube/compare/v0.34.24...v0.34.25) (2023-11-24)

### Bug Fixes

- **api-gateway:** Handle invalid query (invalid JSON) as 400 Bad Request ([#7455](https://github.com/cube-js/cube/issues/7455)) ([c632727](https://github.com/cube-js/cube/commit/c6327275f01cd7c2b43750f88b3d6b13809edba4))
- **mongobi-driver:** Correct time zone conversion for minutes, fix [#7152](https://github.com/cube-js/cube/issues/7152) ([#7456](https://github.com/cube-js/cube/issues/7456)) ([5ae2009](https://github.com/cube-js/cube/commit/5ae2009a8231432020e09fb6dfeaa8f40ef152fa))
- Validation problem with ECDSA SSL keys ([#7453](https://github.com/cube-js/cube/issues/7453)) ([567d2ca](https://github.com/cube-js/cube/commit/567d2cad9a81e82dacd27e99fce9f28db42e9d75))

## [0.34.24](https://github.com/cube-js/cube/compare/v0.34.23...v0.34.24) (2023-11-23)

### Bug Fixes

- **athena-driver:** Allow to pass custom credentials, fix [#7407](https://github.com/cube-js/cube/issues/7407) ([#7429](https://github.com/cube-js/cube/issues/7429)) ([f005c5b](https://github.com/cube-js/cube/commit/f005c5b88418c1c9e34f476c25e523f64434c9ca))
- **cubestore:** Natural order for Int96 and Decimal96 ([#7419](https://github.com/cube-js/cube/issues/7419)) ([5671a5c](https://github.com/cube-js/cube/commit/5671a5cb28db517676f451acc928c4d4b36d9bae))

### Features

- **cubestore:** Metrics - track command for data/cache/queue ([#7430](https://github.com/cube-js/cube/issues/7430)) ([91db103](https://github.com/cube-js/cube/commit/91db103b541da77c6f46f692bbf9c5d138cb6156))
- **schema-compiler:** add dimension primaryKey property to transform result ([#7443](https://github.com/cube-js/cube/issues/7443)) ([baabd41](https://github.com/cube-js/cube/commit/baabd41fed8bd1476d0489dc97c3f78c9106a63b))

## [0.34.23](https://github.com/cube-js/cube/compare/v0.34.22...v0.34.23) (2023-11-19)

### Bug Fixes

- **duckdb-driver:** Crash with high concurrency (unsuccessful or closed pending query result) ([#7424](https://github.com/cube-js/cube/issues/7424)) ([a2e42ac](https://github.com/cube-js/cube/commit/a2e42ac930ea1cabb1d13c8c8dbde7c808139d55))
- Match additive pre-aggregations with missing members for views and proxy members ([#7433](https://github.com/cube-js/cube/issues/7433)) ([e236079](https://github.com/cube-js/cube/commit/e236079ad0ecef2b43f285f4a5154bcd8cb8a9e2))

### Features

- **cubesql:** Support `-` (unary minus) SQL push down ([a0a2e12](https://github.com/cube-js/cube/commit/a0a2e129e4cf3264df75bbdf53a962a892e4e9c2))
- **cubesql:** Support `NOT` SQL push down ([#7422](https://github.com/cube-js/cube/issues/7422)) ([7b1ff0d](https://github.com/cube-js/cube/commit/7b1ff0d897ec9a5cfffba1e09444ff7baa8bea5b))
- **cubestore-driver:** Queue - protect possible race condition on removing orphaned queries ([#7426](https://github.com/cube-js/cube/issues/7426)) ([7142e9c](https://github.com/cube-js/cube/commit/7142e9cf146379182cc57e68d4456bd0f00137a5))
- **duckdb-driver:** Upgrade to 0.9.2 ([#7425](https://github.com/cube-js/cube/issues/7425)) ([d630243](https://github.com/cube-js/cube/commit/d630243dfe045acc46426a4d58acfdeeed7ca1da))
- **native:** Jinja - print tracebacks for Python errors ([#7435](https://github.com/cube-js/cube/issues/7435)) ([734094c](https://github.com/cube-js/cube/commit/734094c6c9a9f89de3504d980daa5fa0e0198ae5))

## [0.34.22](https://github.com/cube-js/cube/compare/v0.34.21...v0.34.22) (2023-11-16)

### Bug Fixes

- **cubesql:** Window PARTITION BY, ORDER BY queries fail for SQL push down ([62b359f](https://github.com/cube-js/cube/commit/62b359f2d33d0c8fd59aa570e7e3a83718a3f7e8))

### Features

- **cubesql:** Ambiguous column references for SQL push down ([c5f1648](https://github.com/cube-js/cube/commit/c5f16485f2b7324f5e8c5ce3642ec9e9d29de534))

## [0.34.21](https://github.com/cube-js/cube/compare/v0.34.20...v0.34.21) (2023-11-15)

### Features

- **cubesql:** SQL push down support for synthetic fields ([#7418](https://github.com/cube-js/cube/issues/7418)) ([d2bdc1b](https://github.com/cube-js/cube/commit/d2bdc1bedbb89ffee14d3bda1c8045b833076e35))
- **cubesql:** Support SQL push down for more functions ([#7406](https://github.com/cube-js/cube/issues/7406)) ([b1606da](https://github.com/cube-js/cube/commit/b1606daba70ab92952b1cbbacd94dd7294b17ad5))

## [0.34.20](https://github.com/cube-js/cube/compare/v0.34.19...v0.34.20) (2023-11-14)

### Bug Fixes

- Expose `isVisible` and `public` properties in meta consistently ([35ca1d0](https://github.com/cube-js/cube/commit/35ca1d0c104ea6111d4db3b8649cd52a5fabcf79))

### Features

- **cubesql:** Support `[NOT] IN` SQL push down ([c64994a](https://github.com/cube-js/cube/commit/c64994ac26e1174ce121c79af46fa6a62747b7e9))
- **druid-driver:** Retrieve types for columns with new Druid versions ([#7414](https://github.com/cube-js/cube/issues/7414)) ([b25c199](https://github.com/cube-js/cube/commit/b25c1998441226132f072211523ad4503f96d929))

## [0.34.19](https://github.com/cube-js/cube/compare/v0.34.18...v0.34.19) (2023-11-11)

### Bug Fixes

- support for format in typings for measures ([#7401](https://github.com/cube-js/cube/issues/7401)) ([1f7e1b3](https://github.com/cube-js/cube/commit/1f7e1b300f06a1b53adfa2850314bd9f145f6651))

### Features

- **@cubejs-client/core:** expose total rows ([#7140](https://github.com/cube-js/cube/issues/7140)) ([#7372](https://github.com/cube-js/cube/issues/7372)) Thanks [@hannosgit](https://github.com/hannosgit)! ([a4d08c9](https://github.com/cube-js/cube/commit/a4d08c961c3dad880c9a00df630e6f27917f1898))
- **cubesql:** SQL push down support for window functions ([#7403](https://github.com/cube-js/cube/issues/7403)) ([b1da6c0](https://github.com/cube-js/cube/commit/b1da6c0e38e3b586c3d4b1ddf9c00be57065d960))

## [0.34.18](https://github.com/cube-js/cube/compare/v0.34.17...v0.34.18) (2023-11-10)

### Bug Fixes

- Non-additive pre-aggregations based on proxy dimension doesn't match anymore ([#7396](https://github.com/cube-js/cube/issues/7396)) ([910a49d](https://github.com/cube-js/cube/commit/910a49d87ebfade22ab5ac2463eca9b43f2d5742))

## [0.34.17](https://github.com/cube-js/cube/compare/v0.34.16...v0.34.17) (2023-11-09)

### Bug Fixes

- **cubestore-driver:** Check introspection results on import ([#7382](https://github.com/cube-js/cube/issues/7382)) ([a8b8ad0](https://github.com/cube-js/cube/commit/a8b8ad07fd64b1ff7aea9ba0610e7ab01c174e77))
- **duckdb-driver:** Compatibility issue with Cube Store for DATE type ([#7394](https://github.com/cube-js/cube/issues/7394)) ([3e1389b](https://github.com/cube-js/cube/commit/3e1389b3a94db85d6b4d343e876a349b843cbf27))

## [0.34.16](https://github.com/cube-js/cube/compare/v0.34.15...v0.34.16) (2023-11-06)

### Features

- **cubesql:** Enable `COVAR` aggr functions push down ([89d841a](https://github.com/cube-js/cube/commit/89d841a049c0064b902bf3cdc631cb056984ddd4))
- **native:** Jinja - async render ([#7309](https://github.com/cube-js/cube/issues/7309)) ([cd1019c](https://github.com/cube-js/cube/commit/cd1019c9fa904ba76b334e941726ff871d2e3a44))
- **server:** Mark cube.py as stable ([#7381](https://github.com/cube-js/cube/issues/7381)) ([e0517bb](https://github.com/cube-js/cube/commit/e0517bb4d77ce635b0495ab0b95606c139d40628))

## [0.34.15](https://github.com/cube-js/cube/compare/v0.34.14...v0.34.15) (2023-11-06)

### Bug Fixes

- **cubestore:** Fix broadcasting for worker services ([#7375](https://github.com/cube-js/cube/issues/7375)) ([1726632](https://github.com/cube-js/cube/commit/1726632e9ffa223f8dba9a9c0914d13cb2ad283b))
- Single value non-additive pre-aggregation match in views ([9666f24](https://github.com/cube-js/cube/commit/9666f24650e93ad6fd6990f6b82fb957e26c2885))

## [0.34.14](https://github.com/cube-js/cube/compare/v0.34.13...v0.34.14) (2023-11-05)

### Bug Fixes

- Views with proxy dimensions and non-additive measures don't not match pre-aggregations ([#7374](https://github.com/cube-js/cube/issues/7374)) ([7189720](https://github.com/cube-js/cube/commit/718972090612f31326bad84fcf792d674ea622a9)), closes [#7099](https://github.com/cube-js/cube/issues/7099)

### Features

- **cubesql:** SQL push down for several ANSI SQL functions ([ac2bf15](https://github.com/cube-js/cube/commit/ac2bf15954e6b143b9014ff4b8f72c6098253c82))
- **cubesql:** SQL push down support for `IS NULL` and `IS NOT NULL` expressions ([9b3c27d](https://github.com/cube-js/cube/commit/9b3c27d502adbcda8a98a4de486a9d0baf4307aa))

## [0.34.13](https://github.com/cube-js/cube/compare/v0.34.12...v0.34.13) (2023-10-31)

### Bug Fixes

- **cubesql:** SQL push down for limit and offset for ungrouped queries ([67da8c3](https://github.com/cube-js/cube/commit/67da8c31463d81e0f84ed1430a1c2d848f910f66))
- **cubestore:** Error with pre-aggregation and filters containing comma. ([#7364](https://github.com/cube-js/cube/issues/7364)) ([ac4037e](https://github.com/cube-js/cube/commit/ac4037e7bc4d76fcfee113f3e55174b8335f5e73))

### Features

- **cubestore:** Introduce limit for cache_max_entry_size ([#7363](https://github.com/cube-js/cube/issues/7363)) ([2295fe0](https://github.com/cube-js/cube/commit/2295fe037600137dc52452d2f9a4c806aa784419))

## [0.34.12](https://github.com/cube-js/cube/compare/v0.34.11...v0.34.12) (2023-10-30)

### Bug Fixes

- **duckdb-driver:** Specify schema for information_schema queries ([#7355](https://github.com/cube-js/cube/issues/7355)) ([60f2078](https://github.com/cube-js/cube/commit/60f20783fb295ad4bcdd8a9d6622a04c565a674a))

### Features

- **cubestore-driver:** Queue - protect possible race condition on cancellation ([#7356](https://github.com/cube-js/cube/issues/7356)) ([a788954](https://github.com/cube-js/cube/commit/a7889543f4fc786ab05a6141227d5f31fc6ffff8))

## [0.34.11](https://github.com/cube-js/cube/compare/v0.34.10...v0.34.11) (2023-10-29)

### Features

- **cubesql:** Allow changing current user through `SET user = ?` ([#7350](https://github.com/cube-js/cube/issues/7350)) ([2c9c8d6](https://github.com/cube-js/cube/commit/2c9c8d68d1bd76b005ebf863b6899ea463d59aae))
- **cubestore:** Queue - expose queueId for LIST/ACTIVE/PENDING/TO_CANCEL ([#7351](https://github.com/cube-js/cube/issues/7351)) ([7b1a0c0](https://github.com/cube-js/cube/commit/7b1a0c05347d0a0f874978b4462f580ddc814b93))
- **query-orchestrator:** Pass queueId for orphaned query cancellation ([#7353](https://github.com/cube-js/cube/issues/7353)) ([08ce078](https://github.com/cube-js/cube/commit/08ce078e4f094c18da9fdd265629e4bb01a8a2c9))
- **query-orchestrator:** Use real queueId in processQuery for events ([#7352](https://github.com/cube-js/cube/issues/7352)) ([bf83762](https://github.com/cube-js/cube/commit/bf83762adfb75585ae233b2c671971761f133048))

## [0.34.10](https://github.com/cube-js/cube/compare/v0.34.9...v0.34.10) (2023-10-27)

### Features

- **cubesql:** Introduce `CUBESQL_DISABLE_STRICT_AGG_TYPE_MATCH` to avoid aggregation type checking during querying ([#7316](https://github.com/cube-js/cube/issues/7316)) ([61089f0](https://github.com/cube-js/cube/commit/61089f056af44f69f79420ce9767e4bb68030f26))

## [0.34.9](https://github.com/cube-js/cube/compare/v0.34.8...v0.34.9) (2023-10-26)

### Bug Fixes

- Correct GraphQL endpoint displayed on front-end ([#7226](https://github.com/cube-js/cube/issues/7226)) Thanks [@peterklingelhofer](https://github.com/peterklingelhofer) ! ([4ae0f18](https://github.com/cube-js/cube/commit/4ae0f18b05184c9d3bf1df6bca6d18bbb29f63b1))
- **databricks-jdbc-driver:** Failed to read package.json issue ([#7311](https://github.com/cube-js/cube/issues/7311)) ([abb3506](https://github.com/cube-js/cube/commit/abb35067daa016eec07592db0e41fa0750a112aa))
- **server-core:** Specify maximum for continueWaitTimeout (90 seconds) ([#7301](https://github.com/cube-js/cube/issues/7301)) ([148fb96](https://github.com/cube-js/cube/commit/148fb966d25d2f15d413300304a664ee275a1686))

### Features

- **native:** Jinja - support fields reflection for PyObject ([#7312](https://github.com/cube-js/cube/issues/7312)) ([e2569a9](https://github.com/cube-js/cube/commit/e2569a9407f86380831d410be8df0e9ef27d5b40))
- **native:** Jinja - upgrade engine (fixes macros memory leak & stack overflow) ([#7313](https://github.com/cube-js/cube/issues/7313)) ([792e265](https://github.com/cube-js/cube/commit/792e265507d12e4d55d4777fa83b5c2148acbc9e))
- **oracle-driver:** Update package.json ([#7273](https://github.com/cube-js/cube/issues/7273)) Thanks [@rongfengliang](https://github.com/rongfengliang) ! ([9983661](https://github.com/cube-js/cube/commit/998366131cf825fbf3dd48c6aa48ace2a80776c5))
- Split view support ([#7308](https://github.com/cube-js/cube/issues/7308)) ([1e722dd](https://github.com/cube-js/cube/commit/1e722dd0cc1a29b6e6554b855b7899abe5746a07))

## [0.34.8](https://github.com/cube-js/cube/compare/v0.34.7...v0.34.8) (2023-10-25)

### Bug Fixes

- **cubesql:** column does not exist in case of ORDER BY is called on CASE column ([8ec80e8](https://github.com/cube-js/cube/commit/8ec80e80ed1501bf369e29c68c1e3fc8f894693f))

### Features

- **cubesql:** Aggregation over dimensions support ([#7290](https://github.com/cube-js/cube/issues/7290)) ([745ae38](https://github.com/cube-js/cube/commit/745ae38554571b6890be7db5b1e1b5dc4c51324b))

## [0.34.7](https://github.com/cube-js/cube/compare/v0.34.6...v0.34.7) (2023-10-23)

### Features

- **native:** Jinja - passing filters via TemplateContext from Python ([#7284](https://github.com/cube-js/cube/issues/7284)) ([1e92352](https://github.com/cube-js/cube/commit/1e9235293f3843caacc1d20fc458e0184e933c40))

## [0.34.6](https://github.com/cube-js/cube/compare/v0.34.5...v0.34.6) (2023-10-20)

### Bug Fixes

- **cubestore:** Window function don't work with only one Following bound ([#7216](https://github.com/cube-js/cube/issues/7216)) ([a0d23cb](https://github.com/cube-js/cube/commit/a0d23cbcecdac9d73bc1d2291e11a3e4d08acde1))
- **docs:** update marketing-ui version ([#7249](https://github.com/cube-js/cube/issues/7249)) ([0b6755c](https://github.com/cube-js/cube/commit/0b6755c0f4c7d49cb9ac22ec58165649479aa26d))
- **native:** Init logger earlier without javascript side (silient errors) ([#7228](https://github.com/cube-js/cube/issues/7228)) ([1f6d49d](https://github.com/cube-js/cube/commit/1f6d49dbd0aa2e792db8c3bcefd54a8a434b4000))
- **native:** Jinja - enable autoescape for .jinja (old naming) files ([#7243](https://github.com/cube-js/cube/issues/7243)) ([abf5a0c](https://github.com/cube-js/cube/commit/abf5a0cc05d378691e736c859fdbd9209ad1a336))

### Features

- **native:** Jinja - passing variables via TemplateContext from Python ([#7280](https://github.com/cube-js/cube/issues/7280)) ([e3dec88](https://github.com/cube-js/cube/commit/e3dec888870f6c37da2957ff49988e5769e37f3d))

## [0.34.5](https://github.com/cube-js/cube/compare/v0.34.4...v0.34.5) (2023-10-16)

### Features

- **duckdb-driver:** upgrade duckdb driver to 0.9.1 ([#7174](https://github.com/cube-js/cube/issues/7174)) ([f9889d4](https://github.com/cube-js/cube/commit/f9889d41d5f74e254e17d1bcf35dab6d46da4df4))
- **native:** Support SafeString for Python & Jinja ([#7242](https://github.com/cube-js/cube/issues/7242)) ([c6069cd](https://github.com/cube-js/cube/commit/c6069cd343d75dbc24a59ae4b941460231409350))

## [0.34.4](https://github.com/cube-js/cube/compare/v0.34.3...v0.34.4) (2023-10-14)

### Bug Fixes

- **cross:** Enable shared python for aarch64-unknown-linux-gnu ([#7225](https://github.com/cube-js/cube/issues/7225)) ([2c4f48a](https://github.com/cube-js/cube/commit/2c4f48a615616bb99bc95ad34e9fc1a420c685c0))

## [0.34.3](https://github.com/cube-js/cube/compare/v0.34.2...v0.34.3) (2023-10-12)

### Bug Fixes

- **native:** Improve error on scheduling Python task ([#7222](https://github.com/cube-js/cube/issues/7222)) ([c14c025](https://github.com/cube-js/cube/commit/c14c0251935d04cc2fa887b44f560024c6ce967d))

### Features

- **native:** Enable Python support for Linux/ARM64 ([#7220](https://github.com/cube-js/cube/issues/7220)) ([5486e26](https://github.com/cube-js/cube/commit/5486e2623057c189b6e15ef2e0dcbfa6e5e6dad0))

## [0.34.2](https://github.com/cube-js/cube/compare/v0.34.1...v0.34.2) (2023-10-12)

### Bug Fixes

- **native:** Correct compilation for Linux ARM64 ([#7215](https://github.com/cube-js/cube/issues/7215)) ([669c86c](https://github.com/cube-js/cube/commit/669c86cc8ed785ddfc604861fe7e8556ddd00f88))
- **playground:** rollup designer view members ([#7214](https://github.com/cube-js/cube/issues/7214)) ([b08e27b](https://github.com/cube-js/cube/commit/b08e27b7dc48427a745adccfd03d92088b78dc72))

### Features

- **native:** Support jinja inside yml in fallback build (without python) ([#7203](https://github.com/cube-js/cube/issues/7203)) ([e3e7e0e](https://github.com/cube-js/cube/commit/e3e7e0e987d1f0b433873779063fa709c0b43eeb))

## [0.34.1](https://github.com/cube-js/cube/compare/v0.34.0...v0.34.1) (2023-10-09)

### Bug Fixes

- **@cubejs-client/vue:** Do not add time dimension as order member if granularity is set to `none` ([#7165](https://github.com/cube-js/cube/issues/7165)) ([1557be6](https://github.com/cube-js/cube/commit/1557be6aa5a6a081578c54c48446815d9be37db6))
- **cubesql:** Support unaliased columns for SQL push down ([#7199](https://github.com/cube-js/cube/issues/7199)) ([e92e15c](https://github.com/cube-js/cube/commit/e92e15c81205f9a05f3df88513d4b934fa039886))
- **duckdb-driver:** allow setting default schema ([#7181](https://github.com/cube-js/cube/issues/7181)) ([a52c4d6](https://github.com/cube-js/cube/commit/a52c4d603a00813380075f0c1b20ad44cb026502))
- More descriptive error message for incorrect `incremental: true` non-partitioned setting ([7e12e4e](https://github.com/cube-js/cube/commit/7e12e4e812b9aee3dab69ba8b86709f889f1a5a0))

### Features

- **cubesql:** Support `SET ROLE name` ([d02077e](https://github.com/cube-js/cube/commit/d02077ed44f8641850bf74f200c91cb6864fddbe))
- **cubestore:** Size limit for RocksStore logs file ([#7201](https://github.com/cube-js/cube/issues/7201)) ([43bb693](https://github.com/cube-js/cube/commit/43bb6936905f73221e59db1a38b0f994c3f13160))

# [0.34.0](https://github.com/cube-js/cube/compare/v0.33.65...v0.34.0) (2023-10-03)

**Note:** Version bump only for package cubejs

## [0.33.65](https://github.com/cube-js/cube/compare/v0.33.64...v0.33.65) (2023-10-02)

### Bug Fixes

- **@cubejs-client/vue:** Pivot config can use null from heuristics ([#7167](https://github.com/cube-js/cube/issues/7167)) ([e7043bb](https://github.com/cube-js/cube/commit/e7043bb41099d2c4477430b96a20c562e1908266))

### Features

- **python:** Evaluate Jinja templates inside `.yml` and `.yaml` ([#7182](https://github.com/cube-js/cube/issues/7182)) ([69e9437](https://github.com/cube-js/cube/commit/69e9437deccc2f16442a39ceb95d1583bcea195f))
- **python:** Support `[@template](https://github.com/template).function("name")` definitions ([#7183](https://github.com/cube-js/cube/issues/7183)) ([33823e8](https://github.com/cube-js/cube/commit/33823e8529529e388535a405a9316ad0d86496c9))

## [0.33.64](https://github.com/cube-js/cube/compare/v0.33.63...v0.33.64) (2023-09-30)

### Bug Fixes

- **cubestore:** Escape regex symbols in all versions of like operator ([#7176](https://github.com/cube-js/cube/issues/7176)) ([e58af39](https://github.com/cube-js/cube/commit/e58af39ee779f1a9372ba24b67ebf56ab532f548))
- **cubestore:** Escape regex symbols in like operator ([#7175](https://github.com/cube-js/cube/issues/7175)) ([b88c3f3](https://github.com/cube-js/cube/commit/b88c3f343d78c13ef170136d257cf591de8d3f4b))
- **cubestore:** Wrong pre-aggregations count distinct approx count when using PostgreSQL database ([#7170](https://github.com/cube-js/cube/issues/7170)) ([1d1d4f0](https://github.com/cube-js/cube/commit/1d1d4f055aa96e8b61e59c173aec25635fc4730b))
- **databricks-jdbc-driver:** ensure default UID property is set ([#7168](https://github.com/cube-js/cube/issues/7168)) ([0f357f8](https://github.com/cube-js/cube/commit/0f357f841b908d71f7b13fc5df889ebb6945901f))
- SQL push down expressions incorrectly cache used cube names ([c5f0b03](https://github.com/cube-js/cube/commit/c5f0b03e2582be6969ae2de8d1e7dfc6190141c4))

### Features

- **native:** More concise module structure ([#7180](https://github.com/cube-js/cube/issues/7180)) ([e2a80bf](https://github.com/cube-js/cube/commit/e2a80bfa0515696391f5aa69c299038f6e52c405))
- **native:** Support explicit [@config](https://github.com/config)("<name>") annotations ([d345d74](https://github.com/cube-js/cube/commit/d345d748e947c8479145886f6d4ac9e9fbeeccd6))

## [0.33.63](https://github.com/cube-js/cube/compare/v0.33.62...v0.33.63) (2023-09-26)

### Features

- **cubesql:** Tableau Standard Gregorian missing date groupings support through SQL push down and some other functions([#7172](https://github.com/cube-js/cube/issues/7172)) ([1339f57](https://github.com/cube-js/cube/commit/1339f577badf94aab02483e3431f614b1fe61302))
- **snowflake-driver:** Upgrade snowflake driver to 1.8.0 ([#7171](https://github.com/cube-js/cube/issues/7171)) ([06a66c6](https://github.com/cube-js/cube/commit/06a66c662e93354e2d1a22e1bf406ea9d1c2825b))

## [0.33.62](https://github.com/cube-js/cube/compare/v0.33.61...v0.33.62) (2023-09-25)

### Features

- **native:** Jinja - support dynamic Python objects (methods, **call**, fields) ([#7162](https://github.com/cube-js/cube/issues/7162)) ([6a5f70c](https://github.com/cube-js/cube/commit/6a5f70c3d7b910590f5c8ed411363dc6d694e720))

## [0.33.61](https://github.com/cube-js/cube/compare/v0.33.60...v0.33.61) (2023-09-22)

### Features

- **native:** Python - new Jinja API ([#7160](https://github.com/cube-js/cube/issues/7160)) ([ea250ae](https://github.com/cube-js/cube/commit/ea250ae3a9c2c86b375f1ebeebb8304496a1dfc2))

## [0.33.60](https://github.com/cube-js/cube/compare/v0.33.59...v0.33.60) (2023-09-22)

### Bug Fixes

- **cubesql:** `dataRange` filter isn't being push down to time dimension in case of other filters are used ([a4edfae](https://github.com/cube-js/cube/commit/a4edfae654f99b0d5a1227dfc569e8f2231e9697)), closes [#6312](https://github.com/cube-js/cube/issues/6312)

### Features

- **docker:** Python - disable buffering for STDOUT (PYTHONUNBUFFERED) ([#7153](https://github.com/cube-js/cube/issues/7153)) ([5cd0a17](https://github.com/cube-js/cube/commit/5cd0a175bf903d7118735f3526813aa9dd6f9953))

## [0.33.59](https://github.com/cube-js/cube/compare/v0.33.58...v0.33.59) (2023-09-20)

### Features

- **cubesql:** `EXTRACT` SQL push down ([#7151](https://github.com/cube-js/cube/issues/7151)) ([e30c4da](https://github.com/cube-js/cube/commit/e30c4da555adfc1515fcc9e0253bc89ca8adc58b))
- **cubesql:** Add ability to filter dates inclusive of date being passed in when using `<=` or `>=` ([#7041](https://github.com/cube-js/cube/issues/7041)) Thanks [@darapuk](https://github.com/darapuk) ! ([6b9ae70](https://github.com/cube-js/cube/commit/6b9ae703b113a01fa4e6de54b5597475aed0b85d))
- **native:** Cube.py - support all options ([#7145](https://github.com/cube-js/cube/issues/7145)) ([5e5fb18](https://github.com/cube-js/cube/commit/5e5fb1800d2e97b870281b335bfca28ab00a9c10))

## [0.33.58](https://github.com/cube-js/cube/compare/v0.33.57...v0.33.58) (2023-09-18)

### Bug Fixes

- **cubejs-client/core:** drillDown - check dateRange before destructuring ([#7138](https://github.com/cube-js/cube/issues/7138)) ([21a7cc2](https://github.com/cube-js/cube/commit/21a7cc2e852366ca224bf84e968c90c30e5e2332))
- **docs:** layout styles ([f5e5372](https://github.com/cube-js/cube/commit/f5e537290b0367a3cd8acd0f4570766193592e93))
- **schema-compiler:** YAML - crash on empty string/null, fix [#7126](https://github.com/cube-js/cube/issues/7126) ([#7141](https://github.com/cube-js/cube/issues/7141)) ([73f5ca7](https://github.com/cube-js/cube/commit/73f5ca74a6b6d7ae66b5576c6c4446fda6b1a5de))
- **schema-compiler:** YAML - crash on unammed measure/dimension/join, refs [#7033](https://github.com/cube-js/cube/issues/7033) ([#7142](https://github.com/cube-js/cube/issues/7142)) ([5c6a065](https://github.com/cube-js/cube/commit/5c6a065b5755d9a991bc89beeb15243b119511cb))
- **server-core:** Validation - cors.optionsSuccessStatus should be a number ([#7144](https://github.com/cube-js/cube/issues/7144)) ([1d58d4f](https://github.com/cube-js/cube/commit/1d58d4fe995299dfb2139a821538446ecb63eb2b))

### Features

- new methods for step-by-step db schema fetching ([#7058](https://github.com/cube-js/cube/issues/7058)) ([a362c20](https://github.com/cube-js/cube/commit/a362c2042d4158ae735e9afe0cfeae15c331dc9d))

## [0.33.57](https://github.com/cube-js/cube/compare/v0.33.56...v0.33.57) (2023-09-15)

### Bug Fixes

- **cubesql:** ORDER BY references outer alias instead of inner expression for SQL push down ([778e16c](https://github.com/cube-js/cube/commit/778e16c52319e5ba6d8a786de1e5ff95036b4461))
- **cubestore:** Eviction - schedule compaction as blocking task ([#7133](https://github.com/cube-js/cube/issues/7133)) ([009b1d6](https://github.com/cube-js/cube/commit/009b1d665e2e90c01a60e2a4192ee108b1838a1c))
- **docs:** change header layout ([#7136](https://github.com/cube-js/cube/issues/7136)) ([581faad](https://github.com/cube-js/cube/commit/581faad70b360b3bab79c52ce735a3650ba17ba4))
- **redshift-driver:** resolves issue where redshift column order can come back in a different order, causing pre-aggregations across partitions to encounter a union all error ([#6965](https://github.com/cube-js/cube/issues/6965)) Thanks [@jskarda829](https://github.com/jskarda829) [@magno32](https://github.com/magno32) ! ([30356d9](https://github.com/cube-js/cube/commit/30356d9a43485ae22a7c97378c9c303679480f05))

### Features

- Config - improve validation (use strict mode) ([#6892](https://github.com/cube-js/cube/issues/6892)) ([5e39b9b](https://github.com/cube-js/cube/commit/5e39b9b35d3a3ca04170b3f9c201d790c437c291))
- **cubesql:** `CONCAT` function support for SQL push down ([da24afe](https://github.com/cube-js/cube/commit/da24afebc844f6f983d6f4df76f2367168667036))
- **cubesql:** DATE_TRUNC support for SQL push down ([#7132](https://github.com/cube-js/cube/issues/7132)) ([ae80eb1](https://github.com/cube-js/cube/commit/ae80eb1e45857cde14ec0ff9c15547f917d6fdd2))
- **cubestore:** Int96 and Decimal96 support ([#6952](https://github.com/cube-js/cube/issues/6952)) ([0262d67](https://github.com/cube-js/cube/commit/0262d67b7c3720224f61385356c6accc1e10cd4b))
- **native:** Cube.py - support orchestratorOptions/http/jwt ([#7135](https://github.com/cube-js/cube/issues/7135)) ([7ab977f](https://github.com/cube-js/cube/commit/7ab977ff4ba011f10dcf0c853fdd8822cfa9bf1e))

## [0.33.56](https://github.com/cube-js/cube/compare/v0.33.55...v0.33.56) (2023-09-13)

### Features

- **native:** Cube.py - introduce config decorator ([#7131](https://github.com/cube-js/cube/issues/7131)) ([84fad15](https://github.com/cube-js/cube/commit/84fad151b061d2ae73237b8704a3dbefe95e66ce))
- **native:** Cube.py - support pre_aggregations_schema ([#7130](https://github.com/cube-js/cube/issues/7130)) ([5fc3b8d](https://github.com/cube-js/cube/commit/5fc3b8d8f9e8fbea6ffdcd3e80232c6d142544d7))
- **native:** Initial support for Python 3.12 ([#7129](https://github.com/cube-js/cube/issues/7129)) ([cc81cd2](https://github.com/cube-js/cube/commit/cc81cd211aba32ecd4e62c3f22a4e1cb9705ba44))
- **native:** Python - support tuple type ([#7128](https://github.com/cube-js/cube/issues/7128)) ([2518e16](https://github.com/cube-js/cube/commit/2518e16531b92358cf8fe3fc21f1022687b3f86a))

## [0.33.55](https://github.com/cube-js/cube/compare/v0.33.54...v0.33.55) (2023-09-12)

### Features

- **client-core:** castNumerics option ([#7123](https://github.com/cube-js/cube/issues/7123)) ([9aed9ac](https://github.com/cube-js/cube/commit/9aed9ac2c271d064888db6f3fe726dac350b52ea))
- **cubestore:** Cache - auto trigger for full compaction ([#7120](https://github.com/cube-js/cube/issues/7120)) ([675a0a0](https://github.com/cube-js/cube/commit/675a0a085f7049b23c2b212fc7ecdfd064006986))
- **native:** Cube.py - support semanticLayerSync/schemaVersion ([#7124](https://github.com/cube-js/cube/issues/7124)) ([1df04da](https://github.com/cube-js/cube/commit/1df04da3bbbe205c72a4a645d6ddbf72b0eaf8f7))
- **native:** Jinja - support passing Map/Seq to Python ([#7125](https://github.com/cube-js/cube/issues/7125)) ([b7762a1](https://github.com/cube-js/cube/commit/b7762a1700f35ac2b89eed061900a7fa01fd5866))

## [0.33.54](https://github.com/cube-js/cube/compare/v0.33.53...v0.33.54) (2023-09-12)

### Bug Fixes

- **cubestore:** COUNT(\*) don't work on subquery with aggregation ([#7117](https://github.com/cube-js/cube/issues/7117)) ([fa48e92](https://github.com/cube-js/cube/commit/fa48e92128ce3695963371977d5475cfbe9b3eeb))
- **cubestore:** Fix error in projection pushdown ([#7119](https://github.com/cube-js/cube/issues/7119)) ([88ef3d6](https://github.com/cube-js/cube/commit/88ef3d6d1bae235e47a9948aa0940a06bf2c2687))

### Features

- **cubesql:** `ORDER BY` SQL push down support ([#7115](https://github.com/cube-js/cube/issues/7115)) ([49ea3cf](https://github.com/cube-js/cube/commit/49ea3cf0721f30da142fb021f860cc56b0a85ab6))
- **cubestore:** Increase parallelism for Cache Store ([#7114](https://github.com/cube-js/cube/issues/7114)) ([ad1b146](https://github.com/cube-js/cube/commit/ad1b146fcd63ff8eccfb719f79c82922a7b08b7e))

## [0.33.53](https://github.com/cube-js/cube/compare/v0.33.52...v0.33.53) (2023-09-08)

### Features

- **cubesql:** Ungrouped SQL push down ([#7102](https://github.com/cube-js/cube/issues/7102)) ([4c7fde5](https://github.com/cube-js/cube/commit/4c7fde5a96a5db0978b72d0887e533450123e9f7))

## [0.33.52](https://github.com/cube-js/cube/compare/v0.33.51...v0.33.52) (2023-09-07)

### Features

- **docker:** Upgrade Node.js to 16.20.2 ([#7111](https://github.com/cube-js/cube/issues/7111)) ([c376e23](https://github.com/cube-js/cube/commit/c376e23d22e9dbc3f3429e1bd9cae793df5f3e28))

## [0.33.51](https://github.com/cube-js/cube/compare/v0.33.50...v0.33.51) (2023-09-06)

### Features

- **cubesql:** Support `inet_server_addr` stub ([9ecb180](https://github.com/cube-js/cube/commit/9ecb180add83a06f5689f530df561f79d441311f))
- **native:** Cube.py - support contextToAppId/contextToOrchestratorId ([#6725](https://github.com/cube-js/cube/issues/6725)) ([77e8432](https://github.com/cube-js/cube/commit/77e8432ec133fae605c4ff43f097cf1a3aaeb529))
- **native:** cube.py - support file_repository ([#7107](https://github.com/cube-js/cube/issues/7107)) ([806db35](https://github.com/cube-js/cube/commit/806db353e746f331b897aec8f61e8205ed299089))

## [0.33.50](https://github.com/cube-js/cube/compare/v0.33.49...v0.33.50) (2023-09-04)

### Bug Fixes

- **cubestore:** Eviction - correct size counting for old keys ([#7097](https://github.com/cube-js/cube/issues/7097)) ([8ded651](https://github.com/cube-js/cube/commit/8ded651cfdd0e1595cbbbaf6a2a5b6b212dae631))

### Features

- **cubesql:** Support `Date32` to `Timestamp` coercion ([54bdfee](https://github.com/cube-js/cube/commit/54bdfeec01ce9bbdd78b022fcf02687a9dcf0793))
- **cubestore:** Eviction - truncate expired keys ([#7081](https://github.com/cube-js/cube/issues/7081)) ([327f2dc](https://github.com/cube-js/cube/commit/327f2dc206a5a9b101e4edbc2099d58cb943b44a))

## [0.33.49](https://github.com/cube-js/cube/compare/v0.33.48...v0.33.49) (2023-08-31)

### Bug Fixes

- **databricks-driver:** Uppercase filter values doesn't match in contains filter ([#7067](https://github.com/cube-js/cube/issues/7067)) ([1e29bb3](https://github.com/cube-js/cube/commit/1e29bb396434730fb705c5406c7a7f3df91b7edf))
- **mongobi-driver:** Invalid configuration for mysql2 connection ([#6352](https://github.com/cube-js/cube/issues/6352)) Thanks [@loremaps](https://github.com/loremaps)! ([edaf326](https://github.com/cube-js/cube/commit/edaf32689a4252dfcafd70a10bc9292e17b2f2d7)), closes [#5527](https://github.com/cube-js/cube/issues/5527)

### Features

- **cubesql:** Support multiple values in `SET` ([41af344](https://github.com/cube-js/cube/commit/41af3441bbb1502950a3e34905070f055c783f29))
- **cubestore:** Support eviction for cache ([#7034](https://github.com/cube-js/cube/issues/7034)) ([2f16cdf](https://github.com/cube-js/cube/commit/2f16cdf7fed06b815e3f4b10c75e2635661aed07))

## [0.33.48](https://github.com/cube-js/cube/compare/v0.33.47...v0.33.48) (2023-08-23)

### Features

- **cubesql:** Ungrouped queries support ([#7056](https://github.com/cube-js/cube/issues/7056)) ([1b5c161](https://github.com/cube-js/cube/commit/1b5c161655de0b055bf55dcd399cd6cde199ef46))
- **cubestore:** Optimize CACHE SET on update (uneeded clone) ([#7035](https://github.com/cube-js/cube/issues/7035)) ([8aea083](https://github.com/cube-js/cube/commit/8aea0839156d4b0f0f3c69e867088ab4da593bf5))
- **cubestore:** Optimize QUEUE ACK/CANCEL, CACHE DELETE ([#7054](https://github.com/cube-js/cube/issues/7054)) ([ac7d1a8](https://github.com/cube-js/cube/commit/ac7d1a8ca14a2fefeea40c29eb64745b9127fd21))

## [0.33.47](https://github.com/cube-js/cube/compare/v0.33.46...v0.33.47) (2023-08-15)

### Bug Fixes

- **cubestore:** Reduce memory usage while truncating ([#7031](https://github.com/cube-js/cube/issues/7031)) ([29080d7](https://github.com/cube-js/cube/commit/29080d72661bb23529d35343a7b1f4f20cda9ec6))

### Features

- **cubesql:** Support LIMIT for SQL push down ([1b5c19f](https://github.com/cube-js/cube/commit/1b5c19f03331ca4174b37614b8b41cdafc211ad7))

## [0.33.46](https://github.com/cube-js/cube/compare/v0.33.45...v0.33.46) (2023-08-14)

### Bug Fixes

- **cubestore:** Missing filter in the middle of an index prunes unnecessary partitions ([5690d8d](https://github.com/cube-js/cube/commit/5690d8da87e1d3959620ba43cd0b0648d3285d68))

### Features

- **cubesql:** Initial SQL push down support for BigQuery, Clickhouse and MySQL ([38467ab](https://github.com/cube-js/cube/commit/38467ab7de64803cd51acf4d5fc696938e52f778))

## [0.33.45](https://github.com/cube-js/cube/compare/v0.33.44...v0.33.45) (2023-08-13)

### Features

- **cubesql:** CASE WHEN SQL push down ([#7029](https://github.com/cube-js/cube/issues/7029)) ([80e4a60](https://github.com/cube-js/cube/commit/80e4a609cdb983db0a600d0fff0fd5bfe31652ed))
- **cubesql:** Whole SQL query push down to data sources ([#6629](https://github.com/cube-js/cube/issues/6629)) ([0e8a76a](https://github.com/cube-js/cube/commit/0e8a76a20cb37e675997f384785dd06e09175113))

## [0.33.44](https://github.com/cube-js/cube/compare/v0.33.43...v0.33.44) (2023-08-11)

### Bug Fixes

- **cubejs-schema-compiler:** cube level public should take precedence over dimension level public ([#7005](https://github.com/cube-js/cube/issues/7005)) ([2161227](https://github.com/cube-js/cube/commit/216122708a2fa2732b3b4755e81f3eccde21f070))
- **cubestore:** Store value_version for secondary index as u32 ([#7019](https://github.com/cube-js/cube/issues/7019)) ([c8cfd62](https://github.com/cube-js/cube/commit/c8cfd6248a81edb4950404fa40fa89405f84b387))

### Features

- **client-core:** Add Meta to export ([#6988](https://github.com/cube-js/cube/issues/6988)) Thanks [@philipprus](https://github.com/philipprus)! ([1f2443c](https://github.com/cube-js/cube/commit/1f2443c073b383ed5993afdc348b1ba599443619))
- **schema-compiler:** Support .yaml.jinja extension, fix [#7018](https://github.com/cube-js/cube/issues/7018) ([#7023](https://github.com/cube-js/cube/issues/7023)) ([3f910fb](https://github.com/cube-js/cube/commit/3f910fb90d8b280e71867ff451f4433dec9a4efa))

## [0.33.43](https://github.com/cube-js/cube/compare/v0.33.42...v0.33.43) (2023-08-04)

### Bug Fixes

- **cubestore:** Mark nullable fields for system.cache/queue/queue_results ([#6984](https://github.com/cube-js/cube/issues/6984)) ([9e64e71](https://github.com/cube-js/cube/commit/9e64e71902f99d6ce50d4599116f92fce06ecfa5))
- **duckdb-driver:** Throw error on broken connection ([#6981](https://github.com/cube-js/cube/issues/6981)) ([e64116d](https://github.com/cube-js/cube/commit/e64116d4b597d253c04d66befa4e6b48e7ad05d5))
- **duckdb:** reuse connection ([#6983](https://github.com/cube-js/cube/issues/6983)) ([dc6c154](https://github.com/cube-js/cube/commit/dc6c1540323fe087ac8e4821b09ed46e5c215506))

### Features

- **cubestore:** Limit push down for system tables (cache, queue, queue_results) ([#6977](https://github.com/cube-js/cube/issues/6977)) ([ace2054](https://github.com/cube-js/cube/commit/ace20547de5fce8a28cb3542ea482daa6eebc2b8))
- **duckdb-driver:** Allow to specify memory_limit as env variable ([#6982](https://github.com/cube-js/cube/issues/6982)) ([84211b2](https://github.com/cube-js/cube/commit/84211b2d0c282636f72f42644ce8af5a8d2f302e))

## [0.33.42](https://github.com/cube-js/cube/compare/v0.33.41...v0.33.42) (2023-08-03)

### Bug Fixes

- **duckdb:** load httpfs ([#6976](https://github.com/cube-js/cube/issues/6976)) ([423e824](https://github.com/cube-js/cube/commit/423e824d2ffe9f920d9775ffb851c05ab66c8d5d))
- **mssql-driver:** Fix group by clause for queries with dimensions and cumulative measures in MS SQL ([#6526](https://github.com/cube-js/cube/issues/6526)) Thanks [@mfclarke-cnx](https://github.com/mfclarke-cnx)! ([e0ed3a2](https://github.com/cube-js/cube/commit/e0ed3a27c9decf00c00568ebb01b6689f410a44d))

### Features

- **schema-compiler:** allow passing a catalog ([#6964](https://github.com/cube-js/cube/issues/6964)) ([680f545](https://github.com/cube-js/cube/commit/680f54561a53d4c8b2108204f191ae9a3c22156e))

## [0.33.41](https://github.com/cube-js/cube/compare/v0.33.40...v0.33.41) (2023-07-28)

### Bug Fixes

- **duckdb:** release connection ([fd9f408](https://github.com/cube-js/cube/commit/fd9f408c45a0595be99a6cc039a91c41975f1642))
- **graphql:** snake case query fields ([#6956](https://github.com/cube-js/cube/issues/6956)) ([eeda56a](https://github.com/cube-js/cube/commit/eeda56aae8ea2c4e3a62a27633834da8e1668279))

### Features

- **duckdb:** s3 config support ([#6961](https://github.com/cube-js/cube/issues/6961)) ([7c10f49](https://github.com/cube-js/cube/commit/7c10f49420828eed773789c7db75cf97966a7727))

## [0.33.40](https://github.com/cube-js/cube/compare/v0.33.39...v0.33.40) (2023-07-27)

### Features

- **cubestore:** Tracking data amount using in processing ([#6887](https://github.com/cube-js/cube/issues/6887)) ([92cae7a](https://github.com/cube-js/cube/commit/92cae7a6feffb8f65b8c165e4c958e17c93c47eb))
- **duckdb-driver:** bump duckdb version ([9678ebb](https://github.com/cube-js/cube/commit/9678ebb749e08736d44804f526a8141bdbf91e91))

## [0.33.39](https://github.com/cube-js/cube/compare/v0.33.38...v0.33.39) (2023-07-25)

### Features

- support motherduck token ([#6948](https://github.com/cube-js/cube/issues/6948)) ([f056088](https://github.com/cube-js/cube/commit/f056088e4e9aed4866629fcd6adfe263367b2661))

## [0.33.38](https://github.com/cube-js/cube/compare/v0.33.37...v0.33.38) (2023-07-21)

**Note:** Version bump only for package cubejs

## [0.33.37](https://github.com/cube-js/cube/compare/v0.33.36...v0.33.37) (2023-07-20)

### Bug Fixes

- **schema:** Jinja - show source line in error message ([#6885](https://github.com/cube-js/cube/issues/6885)) ([ec3ce22](https://github.com/cube-js/cube/commit/ec3ce22a54439ca003c725d7ae2fd2c005618dbc))

## [0.33.36](https://github.com/cube-js/cube/compare/v0.33.35...v0.33.36) (2023-07-13)

### Features

- **schema:** Support passing arguments for [@context](https://github.com/context)\_func in jinja templates ([#6882](https://github.com/cube-js/cube/issues/6882)) ([25f149a](https://github.com/cube-js/cube/commit/25f149aaa1d028c671e37612c819aa211117b026))

## [0.33.35](https://github.com/cube-js/cube/compare/v0.33.34...v0.33.35) (2023-07-12)

**Note:** Version bump only for package cubejs

## [0.33.34](https://github.com/cube-js/cube/compare/v0.33.33...v0.33.34) (2023-07-12)

### Features

- **schema:** Support python [@context](https://github.com/context)\_func for jinja templates ([#6729](https://github.com/cube-js/cube/issues/6729)) ([2f2be58](https://github.com/cube-js/cube/commit/2f2be58d49e4baf2b389a94ba2c782f8579022ab))

## [0.33.33](https://github.com/cube-js/cube/compare/v0.33.32...v0.33.33) (2023-07-08)

### Features

- **schema:** Support multitenancy for jinja templates ([#6793](https://github.com/cube-js/cube/issues/6793)) ([fb59e59](https://github.com/cube-js/cube/commit/fb59e590b09e13bfaecb794a9db7d0b861f38d3f))

## [0.33.32](https://github.com/cube-js/cube/compare/v0.33.31...v0.33.32) (2023-07-07)

### Bug Fixes

- **databricks-jdbc-driver:** Return NULL as NULL instead of false for boolean ([#6791](https://github.com/cube-js/cube/issues/6791)) ([7eb02f5](https://github.com/cube-js/cube/commit/7eb02f569464d801ec71215503bc9b3679b5e856))

### Features

- **docker:** Upgrade Node.js to 16.20.1 ([#6789](https://github.com/cube-js/cube/issues/6789)) ([5da7562](https://github.com/cube-js/cube/commit/5da7562013b6696d3c5da96112990fc8d48290ce))

## [0.33.31](https://github.com/cube-js/cube/compare/v0.33.30...v0.33.31) (2023-07-01)

### Bug Fixes

- **cubestore:** FreeInMemoryChunks fires if there is no inmemory chunks ([#6771](https://github.com/cube-js/cube/issues/6771)) ([f9cfd77](https://github.com/cube-js/cube/commit/f9cfd770a646f45c44e554d5b16ba2cddd8b1c48))
- **databricks-jdbc-driver:** Return NULL decimal as NULL instead of 0 ([#6768](https://github.com/cube-js/cube/issues/6768)) ([c2ab19d](https://github.com/cube-js/cube/commit/c2ab19d86d6144e4f91f9e8fb681e17e87bfcef3))

## [0.33.30](https://github.com/cube-js/cube/compare/v0.33.29...v0.33.30) (2023-06-23)

### Features

- **firebolt-driver:** Timezones support ([#6458](https://github.com/cube-js/cube/issues/6458)) ([6cae9b5](https://github.com/cube-js/cube/commit/6cae9b5c034026247f5d86c821f90090e76cad72))

## [0.33.29](https://github.com/cube-js/cube/compare/v0.33.28...v0.33.29) (2023-06-20)

### Features

- **native:** Support darwin-aaarch64 ([#6745](https://github.com/cube-js/cube/issues/6745)) ([ea53e8d](https://github.com/cube-js/cube/commit/ea53e8d83c7d516e541e78175eac5534c7ea7531))

## [0.33.28](https://github.com/cube-js/cube/compare/v0.33.27...v0.33.28) (2023-06-19)

### Bug Fixes

- **native:** Correct support detection (disable on darwin-aaarch64) ([#6737](https://github.com/cube-js/cube/issues/6737)) ([53c6ee9](https://github.com/cube-js/cube/commit/53c6ee9bddfb679dad28d88352e11b2f8a39e6ca))
- **schema:** Load jinja engine only when jinja templates exist ([#6741](https://github.com/cube-js/cube/issues/6741)) ([75469a6](https://github.com/cube-js/cube/commit/75469a67b58d0a730b29b57d4f05a4049462bf10))

## [0.33.27](https://github.com/cube-js/cube/compare/v0.33.26...v0.33.27) (2023-06-17)

### Bug Fixes

- Support unescaped `\\N` as NULL value for Snowflake driver ([#6735](https://github.com/cube-js/cube/issues/6735)) ([1f92ba6](https://github.com/cube-js/cube/commit/1f92ba6f5407f82703c8920b27a3a3e5a16fea41)), closes [#6693](https://github.com/cube-js/cube/issues/6693)

## [0.33.26](https://github.com/cube-js/cube/compare/v0.33.25...v0.33.26) (2023-06-14)

### Bug Fixes

- Correct duration for refresh sheduler interval warning ([#6724](https://github.com/cube-js/cube/issues/6724)) ([3482d29](https://github.com/cube-js/cube/commit/3482d293463c861848b18b773e43ba732ec56a4f))
- **cubestore:** Out of memory on partition compaction ([#6705](https://github.com/cube-js/cube/issues/6705)) ([980b898](https://github.com/cube-js/cube/commit/980b8985e25928cc62446b76bb8d1182b10353ec))

### Features

- **native:** Python - support int64 ([#6709](https://github.com/cube-js/cube/issues/6709)) ([34497ab](https://github.com/cube-js/cube/commit/34497ab2bc0807159a21ba9a4a76b4702038a8e6))
- **presto-driver:** Set query timeout as query_max_run_time to avoid orphaned queries after query timeout ([d6332cf](https://github.com/cube-js/cube/commit/d6332cf824cb0e282b67188fd5618808a7a158bd))
- **schema:** Initial support for jinja templates ([#6704](https://github.com/cube-js/cube/issues/6704)) ([338d1b7](https://github.com/cube-js/cube/commit/338d1b7ed03fc074c06fb028f731c9817ba8d419))

## [0.33.25](https://github.com/cube-js/cube/compare/v0.33.24...v0.33.25) (2023-06-07)

### Bug Fixes

- schema generation joins and quotes ([#6695](https://github.com/cube-js/cube/issues/6695)) ([85dd2b5](https://github.com/cube-js/cube/commit/85dd2b55daba9a0fed3c13131d9c8acae600e136))
- Support indexes in pre-aggs in yaml ([#6587](https://github.com/cube-js/cube/issues/6587)) ([eaec97d](https://github.com/cube-js/cube/commit/eaec97d99a386e39be937ae8770d422512b482ff))

## [0.33.24](https://github.com/cube-js/cube/compare/v0.33.23...v0.33.24) (2023-06-05)

### Bug Fixes

- **ksql-driver:** Reduce default concurrency to 1 as ksql doesn't support multiple queries at the same time right now ([4435661](https://github.com/cube-js/cube/commit/44356618a7aa77311336061b62fb1d6c2861bf3e))
- Multi data source CUBEJS_CONCURRENCY env ([#6683](https://github.com/cube-js/cube/issues/6683)) ([6beb0fb](https://github.com/cube-js/cube/commit/6beb0fb2f9d5b009f22e67ca72f78b3419c40861))
- **native:** Allow to load Python extensions (export public symbols) ([d95cddc](https://github.com/cube-js/cube/commit/d95cddcb477667fa1c6a1f01a3b6571230af8efb))

### Features

- **cubesql:** Support `CURRENT_DATE` scalar function ([ec928a6](https://github.com/cube-js/cube/commit/ec928a67f05ed91517b556b701581d8d04370cc7))
- **cubestore:** Priorities for job types ([#6502](https://github.com/cube-js/cube/issues/6502)) ([a457c5d](https://github.com/cube-js/cube/commit/a457c5da23b95be55e09e68c759e23f1299d17bf))
- **native:** Publish python builds for darwin platform ([#6664](https://github.com/cube-js/cube/issues/6664)) ([edcf8f2](https://github.com/cube-js/cube/commit/edcf8f2c875d3bc821ad56ef4a06841953a1cabb))
- **native:** Python - support async functions ([9232af5](https://github.com/cube-js/cube/commit/9232af5b0b833dcfde10ae7b637bdadc1c4ac972))
- **native:** Python - support more functions in cube.py ([#6687](https://github.com/cube-js/cube/issues/6687)) ([1d10c5b](https://github.com/cube-js/cube/commit/1d10c5b29815672e9e739e3b3a89537a8d0e9404))

## [0.33.23](https://github.com/cube-js/cube/compare/v0.33.22...v0.33.23) (2023-06-01)

### Bug Fixes

- **ci:** Publish - cubestore use gtar on windows ([9feaf49](https://github.com/cube-js/cube/commit/9feaf4944843dde200229dbe56ef3b5ba9069236))
- **docker:** Install libpython3-dev on building step ([11245d2](https://github.com/cube-js/cube/commit/11245d201666c56b07c277b57f23ec99a812516a))

## [0.33.22](https://github.com/cube-js/cube/compare/v0.33.21...v0.33.22) (2023-05-31)

### Features

- **docker:** Enable python builds for x64 ([#6671](https://github.com/cube-js/cube/issues/6671)) ([bc78f2d](https://github.com/cube-js/cube/commit/bc78f2d70b15c05b54e0d169620d7c2c40847fc9))

## [0.33.21](https://github.com/cube-js/cube/compare/v0.33.20...v0.33.21) (2023-05-31)

**Note:** Version bump only for package cubejs

## [0.33.20](https://github.com/cube-js/cube/compare/v0.33.19...v0.33.20) (2023-05-31)

**Note:** Version bump only for package cubejs

## [0.33.19](https://github.com/cube-js/cube/compare/v0.33.18...v0.33.19) (2023-05-30)

### Bug Fixes

- **mssql-driver:** Handle BigInt and other mismatched types ([9cebfef](https://github.com/cube-js/cube/commit/9cebfefbe44d2606195f2f2add0579687c2f4461)), closes [#6658](https://github.com/cube-js/cube/issues/6658)

### Features

- **native:** Publish builds with Python 3.9, 3.10 for Linux ([#6666](https://github.com/cube-js/cube/issues/6666)) ([0b1c7b8](https://github.com/cube-js/cube/commit/0b1c7b8bd13947a7c43d4f91d425a9124a4dde77))

## [0.33.18](https://github.com/cube-js/cube/compare/v0.33.17...v0.33.18) (2023-05-29)

### Features

- **native:** Initial support for cube.py (Python configuration) ([#6465](https://github.com/cube-js/cube/issues/6465)) ([9086be4](https://github.com/cube-js/cube/commit/9086be4bc8e727733f11c08240b463f8f64c879d))

## [0.33.17](https://github.com/cube-js/cube/compare/v0.33.16...v0.33.17) (2023-05-29)

### Bug Fixes

- **mssql-driver:** Server crashes on streaming errors ([2e81345](https://github.com/cube-js/cube/commit/2e81345059c14c0880e8b7d1bde7c9d04e497bfa))

## [0.33.16](https://github.com/cube-js/cube/compare/v0.33.15...v0.33.16) (2023-05-28)

### Bug Fixes

- **mssql-driver:** Pre-aggregations builds hang up if over 10K of rows ([#6661](https://github.com/cube-js/cube/issues/6661)) ([9b20ff4](https://github.com/cube-js/cube/commit/9b20ff4ef78acbb65ebea80adceb227bf96b1727))

## [0.33.15](https://github.com/cube-js/cube/compare/v0.33.14...v0.33.15) (2023-05-26)

### Bug Fixes

- **athena-driver:** Internal: Error during planning: Coercion from [Utf8, Utf8] to the signature Exact([Utf8, Timestamp(Nanosecond, None)]) failed for athena pre-aggregations ([#6655](https://github.com/cube-js/cube/issues/6655)) ([46f7dbd](https://github.com/cube-js/cube/commit/46f7dbdeb0a9f55640d0f7afd7edb67ec101a43a))
- Put temp table strategy drop under lock to avoid missing table r… ([#6642](https://github.com/cube-js/cube/issues/6642)) ([05fcdca](https://github.com/cube-js/cube/commit/05fcdca9795bb7f049d57eb3cdaa38d098554bb8))

## [0.33.14](https://github.com/cube-js/cube/compare/v0.33.13...v0.33.14) (2023-05-25)

### Reverts

- Revert "feat(native): Initial support for cube.py (Python configuration) (#6465)" ([95e9581](https://github.com/cube-js/cube/commit/95e95818e0be316b9d4df83a00e5107efde7081f)), closes [#6465](https://github.com/cube-js/cube/issues/6465)

## [0.33.13](https://github.com/cube-js/cube/compare/v0.33.12...v0.33.13) (2023-05-25)

### Features

- **native:** Initial support for cube.py (Python configuration) ([#6465](https://github.com/cube-js/cube/issues/6465)) ([c41af63](https://github.com/cube-js/cube/commit/c41af63c9ba9778364ebb89ad7b087156b48cfec))

## [0.33.12](https://github.com/cube-js/cube/compare/v0.33.11...v0.33.12) (2023-05-22)

### Bug Fixes

- **client-core:** drill down check the parent date range bounds ([#6639](https://github.com/cube-js/cube/issues/6639)) ([5da5e61](https://github.com/cube-js/cube/commit/5da5e613c4551f09f72ba89e4432534ab1524eaa))
- Invalid query format: \"filters[0]\" does not match any of the allowed types for `or` query ([a3b4cd6](https://github.com/cube-js/cube/commit/a3b4cd67455f9839024495f332f8f4e2963d64ce))

## [0.33.11](https://github.com/cube-js/cube/compare/v0.33.10...v0.33.11) (2023-05-22)

### Bug Fixes

- **docker:** Don't load native on alpine (not supported), fix [#6510](https://github.com/cube-js/cube/issues/6510) ([#6636](https://github.com/cube-js/cube/issues/6636)) ([a01d2f9](https://github.com/cube-js/cube/commit/a01d2f953973eee79b6135f7883ecff56bb71486))

### Features

- **cubestore:** Optimization of inmemory chunk operations for streaming ([#6354](https://github.com/cube-js/cube/issues/6354)) ([146798a](https://github.com/cube-js/cube/commit/146798affff184cab489628a1a40611b7fb5879a))
- **docker:** Install DuckDB as built-in driver ([b1e77bd](https://github.com/cube-js/cube/commit/b1e77bdee1d9b35a46c51387d8d16565d332754b))
- **duckdb-driver:** Upgrade DuckDB to 0.8 ([7d91f9f](https://github.com/cube-js/cube/commit/7d91f9f2ec187f8032a768cb35e46bbd557e8125))
- Improve LangChain support ([15e51bc](https://github.com/cube-js/cube/commit/15e51bcb19ce22c38b71f4685484295fc637e44c))

## [0.33.10](https://github.com/cube-js/cube/compare/v0.33.9...v0.33.10) (2023-05-19)

### Bug Fixes

- remove noisy Warning: Attribute `filter.dimension` is deprecated. Please use 'member' instead of 'dimension' ([546227a](https://github.com/cube-js/cube/commit/546227a96c5c41078c82a75d7e4697dfd505e181))

## [0.33.9](https://github.com/cube-js/cube/compare/v0.33.8...v0.33.9) (2023-05-18)

### Bug Fixes

- **databricks-jdbc-driver:** Error: [Databricks][jdbc](11220) Parameters cannot be used with normal Statement objects, use PreparedStatements instead. ([328bb79](https://github.com/cube-js/cube/commit/328bb79853833420a287fc9e31dda72a9886999d)), closes [#6627](https://github.com/cube-js/cube/issues/6627)
- Invalid query format: \"filters[0]\" does not match any of the allowed types if queryRewrite is using non string values ([ac57b4c](https://github.com/cube-js/cube/commit/ac57b4c15cb39392faa75a01c042fdd5ecb19af9))

## [0.33.8](https://github.com/cube-js/cube/compare/v0.33.7...v0.33.8) (2023-05-17)

### Bug Fixes

- **athena-driver:** Fix partitioned pre-aggregations and column values with `,` through export bucket ([#6596](https://github.com/cube-js/cube/issues/6596)) ([1214cab](https://github.com/cube-js/cube/commit/1214cabf69f9e6216c516d05acadfe7e6178cccf))
- **graphql:** exclude empty cubes, revert equals/notEquals type ([#6619](https://github.com/cube-js/cube/issues/6619)) ([fab1b6a](https://github.com/cube-js/cube/commit/fab1b6abe198b1d83b4484bae66108782e62887c))

### Features

- **bigquery-driver:** Specify timeout for job on testConnection ([#6588](https://github.com/cube-js/cube/issues/6588)) ([d724f09](https://github.com/cube-js/cube/commit/d724f09113e4b4cb8c703fc02e9d9e13c3e05fb7))
- **cubestore-driver:** Queue - protect possible race condition (query def) [#6616](https://github.com/cube-js/cube/issues/6616)) ([60294de](https://github.com/cube-js/cube/commit/60294defff88ee96d95d1d34b1fe17fccdb17b71))

## [0.33.7](https://github.com/cube-js/cube/compare/v0.33.6...v0.33.7) (2023-05-16)

### Bug Fixes

- Limit override isn't respected in queryRewrite ([#6605](https://github.com/cube-js/cube/issues/6605)) ([172c39b](https://github.com/cube-js/cube/commit/172c39b6fb2609f003a364efb63d8091e39607a0))

### Features

- **cubestore-driver:** Queue - protect race conditions ([#6598](https://github.com/cube-js/cube/issues/6598)) ([d503598](https://github.com/cube-js/cube/commit/d50359845d388ccadc8d5a7233ed7317849558f4))
- **cubestore:** Queue - allow to GET by processingId ([#6608](https://github.com/cube-js/cube/issues/6608)) ([f0c1156](https://github.com/cube-js/cube/commit/f0c11565426b5604bae665fc52d5b416f4f055d2))
- **cubestore:** Queue - delete results via scheduler ([#6515](https://github.com/cube-js/cube/issues/6515)) ([8e1ab20](https://github.com/cube-js/cube/commit/8e1ab2044a92f9e71918085a31716a784cd5bb9f))

## [0.33.6](https://github.com/cube-js/cube/compare/v0.33.5...v0.33.6) (2023-05-13)

### Bug Fixes

- **cubesql:** Improve NULL comparison with int/bool ([de1be39](https://github.com/cube-js/cube/commit/de1be39d07ceec5d87d6c7aef8adb65fbe246ee3))
- **cubestore:** Optimize unique constraint check on insert ([c227ae9](https://github.com/cube-js/cube/commit/c227ae9f72cfd66fd47e60fc567c2fde88ebc932))
- **cubestore:** Schedule compaction without blocking rw loop ([f9bbbac](https://github.com/cube-js/cube/commit/f9bbbac278898d02d50e6ff5911658d5e8caf7e3))
- LIMIT is not enforced ([#6586](https://github.com/cube-js/cube/issues/6586)) ([8ca5234](https://github.com/cube-js/cube/commit/8ca52342944b9767f2c34591a9241bf31cf78c71))
- **snowflake-driver:** Bind variable ? not set for partitioned pre-aggregations ([#6594](https://github.com/cube-js/cube/issues/6594)) ([0819075](https://github.com/cube-js/cube/commit/081907568d97fa79f56edf1898b2845affb925cf))

### Features

- **cubestore:** Queue - support cancel, ack, result_blocking by processingId ([#6551](https://github.com/cube-js/cube/issues/6551)) ([2e82839](https://github.com/cube-js/cube/commit/2e82839586f18d8b9ba385ab6f8e5641113ccf0c))
- **cubestore:** Support custom CSV delimiters ([#6597](https://github.com/cube-js/cube/issues/6597)) ([f8c2c88](https://github.com/cube-js/cube/commit/f8c2c888150e9436c90de9ecd45f540bb0314855))

## [0.33.5](https://github.com/cube-js/cube/compare/v0.33.4...v0.33.5) (2023-05-11)

### Bug Fixes

- **cubejs-playground:** sticky bar styles ([#6558](https://github.com/cube-js/cube/issues/6558)) ([c4864c5](https://github.com/cube-js/cube/commit/c4864c508ffd42bebbc8e1fb8d98523fa1d252b7))
- fix includes all for views ([#6559](https://github.com/cube-js/cube/issues/6559)) ([f62044a](https://github.com/cube-js/cube/commit/f62044aa7b60f6a2eeafc60e5649629aadc287a2))
- typo in docs ([8c36022](https://github.com/cube-js/cube/commit/8c3602297299af44bca1427646c95e50ddf4b991))
- yml formatter escapse double quotes ([341e115](https://github.com/cube-js/cube/commit/341e115ef7fd6475f3d947fb0aa878addee86eb9))

### Features

- **cubestore:** Support converting Int16/32, UInt16/32 for DataFrame ([#6569](https://github.com/cube-js/cube/issues/6569)) ([5e0fd4f](https://github.com/cube-js/cube/commit/5e0fd4fd73717d50c1a0684e840d987fb95df495))

## [0.33.4](https://github.com/cube-js/cube/compare/v0.33.3...v0.33.4) (2023-05-07)

### Bug Fixes

- **docs:** fix views example in style guide ([1296ddb](https://github.com/cube-js/cube/commit/1296ddb06d5e0ae151c8dc7ec7f0ad77e429333f))
- join_path for JS schemas and \* includes with prefix ([#6555](https://github.com/cube-js/cube/issues/6555)) ([c76bbef](https://github.com/cube-js/cube/commit/c76bbef36e5b6594bcb79174ceb672d8e2b870ff))

### Features

- **cubestore:** Projection in kafka streaming queries ([#6479](https://github.com/cube-js/cube/issues/6479)) ([e0495a9](https://github.com/cube-js/cube/commit/e0495a9f46dd8a78fce069b31382fdeec37da8d0))

## [0.33.3](https://github.com/cube-js/cube/compare/v0.33.2...v0.33.3) (2023-05-05)

### Bug Fixes

- **databricks-driver:** Increase default JDBC acquire timeout to 2 minutes as it can take more time to reconnect ([dde9bc4](https://github.com/cube-js/cube/commit/dde9bc401d3629e5284e3d160ba41023316b7e8c))
- **databricks-driver:** Remove package.json dependency from DatabricksDriver to avoid package.json not found issues ([#6521](https://github.com/cube-js/cube/issues/6521)) Thanks [@carlagouveia](https://github.com/carlagouveia) ! ([b3ac7a1](https://github.com/cube-js/cube/commit/b3ac7a10b74988af42bd6f289a271a71a92dee5e))
- **graphql:** Pre-aggregations aren't used for GraphQL queries with date range ([#6549](https://github.com/cube-js/cube/issues/6549)) ([a343f4d](https://github.com/cube-js/cube/commit/a343f4d2f0bdf62120e99fb3252f6460f4a85e8e))
- **snowflake-driver:** Float exported as decimal for pre-aggregations ([#6544](https://github.com/cube-js/cube/issues/6544)) ([7c8b8de](https://github.com/cube-js/cube/commit/7c8b8ded5f7bb16248989bd56b0913180321314d))

### Features

- **docker:** Security upgrade node from 16.19.1 to 16.20 ([#6547](https://github.com/cube-js/cube/issues/6547)) ([5523935](https://github.com/cube-js/cube/commit/55239350d8b629d55ec8ee61fd2c4a6f00057f88))

## [0.33.2](https://github.com/cube-js/cube/compare/v0.33.1...v0.33.2) (2023-05-04)

### Bug Fixes

- **cubejs-playground:** remove less file import ([#6537](https://github.com/cube-js/cube/issues/6537)) ([2a4de4b](https://github.com/cube-js/cube/commit/2a4de4b85746ed8c74c57b2cd18c6f69f2e65bd4))
- **cubestore:** Queue - protect possible race condition on GET_RESULT_BLOCKING ([#6535](https://github.com/cube-js/cube/issues/6535)) ([d962c05](https://github.com/cube-js/cube/commit/d962c058f703a5f5156c36ceb8cae074b8d15d02))

### Features

- **cubestore-driver:** Queue - track orphaned set result ([f97fbdc](https://github.com/cube-js/cube/commit/f97fbdcbe1e8c98d568f0614be8074e32d22343f))
- **cubestore:** Queue - allow to do merge_extra, heartbeat by processingId ([#6534](https://github.com/cube-js/cube/issues/6534)) ([bf8df29](https://github.com/cube-js/cube/commit/bf8df29c261625f588faf65984047026d760020a))
- **cubestore:** Queue - expose processingId in queue_add ([#6540](https://github.com/cube-js/cube/issues/6540)) ([24b4798](https://github.com/cube-js/cube/commit/24b4798fb5e26b2be0f04f3e1af76c1b8516a9b8))
- **cubestore:** Queue - expose processingId in queue_retrieve ([#6530](https://github.com/cube-js/cube/issues/6530)) ([43913ce](https://github.com/cube-js/cube/commit/43913ce459e1b60dd04f3a23aed1c617de0e0422))
- **cubestore:** Queue - return success marker on ack (set result) ([e8814bf](https://github.com/cube-js/cube/commit/e8814bf5ebd18431f0d4a61db7fee2a519a5bdd8))

## [0.33.1](https://github.com/cube-js/cube/compare/v0.33.0...v0.33.1) (2023-05-03)

### Bug Fixes

- **firebolt-driver:** Automatically cast boolean parameters ([#6531](https://github.com/cube-js/cube/issues/6531)) ([90136dc](https://github.com/cube-js/cube/commit/90136dc565653a65a6682591206f4cea98b6bd97))
- **snowflake-driver:** Revert read-only mode until useOriginalSqlPreAggregations flow is fixed for read only ([#6532](https://github.com/cube-js/cube/issues/6532)) ([20651a9](https://github.com/cube-js/cube/commit/20651a94f2b4e9919e2b5e7761bb725ca07efc54))

# [0.33.0](https://github.com/cube-js/cube/compare/v0.32.31...v0.33.0) (2023-05-02)

**Note:** Version bump only for package cubejs

## [0.32.31](https://github.com/cube-js/cube/compare/v0.32.30...v0.32.31) (2023-05-02)

### Bug Fixes

- **bigquery-driver:** Revert read-only implementation as `LIMIT 1` queries generate too much usage ([#6523](https://github.com/cube-js/cube/issues/6523)) ([f023484](https://github.com/cube-js/cube/commit/f02348408103f7eeb9d6cd8bea959549a78fe943))
- **cubesql:** Streaming error logging ([e27a525](https://github.com/cube-js/cube/commit/e27a5252380a55682f450bf057767074394179a2))

### Features

- **cubesql:** psqlodbc driver support ([fcfc7ea](https://github.com/cube-js/cube/commit/fcfc7ea9f8bfd027aa549d139b9ed12b773594ec))
- Support snake case security_context in COMPILE_CONTEXT ([#6519](https://github.com/cube-js/cube/issues/6519)) ([feb3fe3](https://github.com/cube-js/cube/commit/feb3fe389462fc4cf3947f8ecf2146c3fe102b9e))

## [0.32.30](https://github.com/cube-js/cube/compare/v0.32.29...v0.32.30) (2023-04-28)

### Bug Fixes

- Always log authentication errors for debugging purpose ([2c309b7](https://github.com/cube-js/cube/commit/2c309b7d8122c19397a649e51d4b2a032361f8ee))
- **cubejs-playground:** SL-58 CC-1686 text and icon fixes ([#6490](https://github.com/cube-js/cube/issues/6490)) ([1fa96f0](https://github.com/cube-js/cube/commit/1fa96f04b7068887c0541f2fcfba51400b8ddc3c))
- **cubesql:** Resolve Grafana introspection issues ([db32377](https://github.com/cube-js/cube/commit/db32377f6e6d45c8c16b12ee7e51fdf1e9687fc9))
- **cubestore:** Error in Sparse HLL entry parsing ([#6498](https://github.com/cube-js/cube/issues/6498)) ([9719cdc](https://github.com/cube-js/cube/commit/9719cdcc39461948e85ea7f2c4bdae377a8dba83))
- model style guide, views folder ([#6493](https://github.com/cube-js/cube/issues/6493)) ([1fa7f99](https://github.com/cube-js/cube/commit/1fa7f994551e05fd2ef417bd2ee7f472881bc2bc))
- **snowflake-driver:** Int is exported to pre-aggregations as decimal… ([#6513](https://github.com/cube-js/cube/issues/6513)) ([3710b11](https://github.com/cube-js/cube/commit/3710b113160d4b0f53b40d6b31ae9c901aa51571))

### Features

- **bigquery-driver:** CI, read-only, streaming and unloading ([#6495](https://github.com/cube-js/cube/issues/6495)) ([4c07431](https://github.com/cube-js/cube/commit/4c07431033df7554ffb6f9d5f64eca156267b3e3))
- **gateway:** GraphQL snake case support ([#6480](https://github.com/cube-js/cube/issues/6480)) ([45a46d0](https://github.com/cube-js/cube/commit/45a46d0556c07842830bf55447f4f575fc158ef6))
- **playground:** cube type tag, public cubes ([#6482](https://github.com/cube-js/cube/issues/6482)) ([cede7a7](https://github.com/cube-js/cube/commit/cede7a71f7d2e8d9dc221669b6b1714ee146d8ea))

## [0.32.29](https://github.com/cube-js/cube/compare/v0.32.28...v0.32.29) (2023-04-25)

### Bug Fixes

- **api-gateway:** passing defaultApiScope env to the contextToApiScopesFn ([#6485](https://github.com/cube-js/cube/issues/6485)) ([b2da950](https://github.com/cube-js/cube/commit/b2da950509e48bdfcd5d6f282f81a00b3794d559))
- **schema-compiler:** yml backtick escaping ([#6478](https://github.com/cube-js/cube/issues/6478)) ([39a9ac0](https://github.com/cube-js/cube/commit/39a9ac0e9a641efeeca0e99d286962aa007551cd))
- **server-core:** Display refresh scheduler interval error as warning ([#6473](https://github.com/cube-js/cube/issues/6473)) ([771f661](https://github.com/cube-js/cube/commit/771f661cadfe9c19bcaad9fe232f7f4901f0778b))

### Features

- **athena-driver:** read-only unload ([#6469](https://github.com/cube-js/cube/issues/6469)) ([d3fee7c](https://github.com/cube-js/cube/commit/d3fee7cbefe5c415573c4d2507b7e61a48f0c91a))
- **cubestore:** Split partitions by filesize ([#6435](https://github.com/cube-js/cube/issues/6435)) ([5854add](https://github.com/cube-js/cube/commit/5854add7a65c39e05ba71c2a558032671797ec26))
- **cubestore:** Traffic tracing for streaming ([#6379](https://github.com/cube-js/cube/issues/6379)) ([37a33e6](https://github.com/cube-js/cube/commit/37a33e65234740a8028c340334510facbea896d7))
- Make configurable cubestore metrics address ([#6437](https://github.com/cube-js/cube/issues/6437)) Thanks [@fbalicchia](https://github.com/fbalicchia) ! ([16d0bcb](https://github.com/cube-js/cube/commit/16d0bcb1489c512f91c21970e21769c9882bfedc))
- **schema-compiler:** Support public/shown for segments ([#6491](https://github.com/cube-js/cube/issues/6491)) ([df6160c](https://github.com/cube-js/cube/commit/df6160cc62f8ab2fccbfb85f2916040aec218fc3))
- semanticLayerSync configs option ([#6483](https://github.com/cube-js/cube/issues/6483)) ([d3b8ed7](https://github.com/cube-js/cube/commit/d3b8ed7401f98429eca42227ef4cfe1f29f90d8e))
- **snowflake-driver:** streaming export, read-only unload ([#6452](https://github.com/cube-js/cube/issues/6452)) ([67565b9](https://github.com/cube-js/cube/commit/67565b975c16f93070de0346056c6a3865bc9fd8))

## [0.32.28](https://github.com/cube-js/cube/compare/v0.32.27...v0.32.28) (2023-04-19)

### Bug Fixes

- **mssql-driver:** release stream connection ([#6453](https://github.com/cube-js/cube/issues/6453)) ([7113fa1](https://github.com/cube-js/cube/commit/7113fa1247f3761c9fde5f8d1af8de6036350e72))
- **schema-compiler:** Incorrect camelize for meta ([#6444](https://github.com/cube-js/cube/issues/6444)) ([c82844e](https://github.com/cube-js/cube/commit/c82844e130b31722fa338022b740ac7285c01190))
- segment not found for path if rollup with segment is used within `rollupJoin` ([6a3ca80](https://github.com/cube-js/cube/commit/6a3ca8032151d530a30bab88c024c451d4141c01))

### Features

- **cubesql:** Support new Thoughtspot introspection ([a04c83a](https://github.com/cube-js/cube/commit/a04c83a070ddbd2924528ce5e7ecf10c7b4f235c))
- **cubesql:** Support psql's `\list` command ([0b30def](https://github.com/cube-js/cube/commit/0b30def71aa5bb53dbc59ddb6a4b63bd59eda95a))
- **playground:** use snake case for data models ([398993a](https://github.com/cube-js/cube/commit/398993ad9772a52ad377542937614148c597ca72))
- using snake case for model files, model templates updates ([#6447](https://github.com/cube-js/cube/issues/6447)) ([37e3180](https://github.com/cube-js/cube/commit/37e3180d4218b342ee0c2fe0ef90464aa6633b54))

## [0.32.27](https://github.com/cube-js/cube/compare/v0.32.26...v0.32.27) (2023-04-14)

### Bug Fixes

- Do not use in memory cache for more than 5 minutes to avoid any long term discrepancies with upstream cache ([aea2daf](https://github.com/cube-js/cube/commit/aea2daf38ddde61fd3f00a886b7679bef060948f))
- **ksql-driver:** Drop table with the topic to avoid orphaned topics ([b113841](https://github.com/cube-js/cube/commit/b113841ad52a52b4ea65192277819fce50725f02))
- Limit pre-aggregations API memory usage by introducing partition loading concurrency ([f565371](https://github.com/cube-js/cube/commit/f565371fe9d05267bbacd37369dbc25780e4da19))

## [0.32.26](https://github.com/cube-js/cube/compare/v0.32.25...v0.32.26) (2023-04-13)

### Bug Fixes

- **cli:** Validate command - allow node require ([#6416](https://github.com/cube-js/cube/issues/6416)) ([ead63d8](https://github.com/cube-js/cube/commit/ead63d823253e8cd42e9353a09d4481e5e889d0b))
- Ensure expired in memory cache value can't be used even in case renewalThreshold hasn't been met ([64f5878](https://github.com/cube-js/cube/commit/64f58789b6eab44d25f8d4305b3e8b299f4735bb))
- re-export FileRepository ([#6425](https://github.com/cube-js/cube/issues/6425)) ([52c9252](https://github.com/cube-js/cube/commit/52c92529c5c13994b81b7af616fdf329cb50a0f6))

### Features

- **schema-compiler:** JS - support snake_case for any properties ([#6396](https://github.com/cube-js/cube/issues/6396)) ([f1b4c0b](https://github.com/cube-js/cube/commit/f1b4c0bfb2e8b8ad20853af6232388c6d764a780))

## [0.32.25](https://github.com/cube-js/cube/compare/v0.32.24...v0.32.25) (2023-04-12)

### Bug Fixes

- **schema-compiler:** Yaml - crash on fully commented file ([#6413](https://github.com/cube-js/cube/issues/6413)) ([29d2eae](https://github.com/cube-js/cube/commit/29d2eaed8c2d8fe7575be732bc21acbf8dadafbb))
- **server-core:** turn off the jobs API renewQuery flag ([#6414](https://github.com/cube-js/cube/issues/6414)) ([81dddb1](https://github.com/cube-js/cube/commit/81dddb1ecb6d5e5d97c6fe3dd277ff3b5f651b51))

### Features

- **api-gateway, server-core:** Orchestration API telemetry ([#6419](https://github.com/cube-js/cube/issues/6419)) ([af0854d](https://github.com/cube-js/cube/commit/af0854d0d8cdc05eab2df090a2d0314a3d27fc23))
- **api-gateway, server-core:** Renamed Permissions to ApiScopes ([#6397](https://github.com/cube-js/cube/issues/6397)) ([800a96b](https://github.com/cube-js/cube/commit/800a96b68e5aa9bbeb2c52edaab0698832a18619))
- **duckdb-driver:** Support countDistinctApprox ([#6409](https://github.com/cube-js/cube/issues/6409)) ([6b1fadb](https://github.com/cube-js/cube/commit/6b1fadb0954ce349a55b88a158d236c2ca31ecfc))

## [0.32.24](https://github.com/cube-js/cube/compare/v0.32.23...v0.32.24) (2023-04-10)

### Reverts

- Revert "feat(docker): Install DuckDB as built-in driver" ([99ea6c1](https://github.com/cube-js/cube/commit/99ea6c15333b3d0c19011cb58ed8a8d10e6b0d3c))

## [0.32.23](https://github.com/cube-js/cube/compare/v0.32.22...v0.32.23) (2023-04-10)

### Bug Fixes

- Fix typo in driver name, DuckDB ([c53a31c](https://github.com/cube-js/cube/commit/c53a31c37b6b70fa3c5e2fc0da97eb958e9fec5d))

### Features

- **docker:** Install DuckDB as built-in driver ([41a0ca1](https://github.com/cube-js/cube/commit/41a0ca11047ed907db354c3f6e49c3e097a584b9))

## [0.32.22](https://github.com/cube-js/cube/compare/v0.32.21...v0.32.22) (2023-04-10)

### Bug Fixes

- Correct the syntax error in the ClickHouse cast method ([#6154](https://github.com/cube-js/cube/issues/6154)) Thanks [@fxx-real](https://github.com/fxx-real)! ([b5ccf18](https://github.com/cube-js/cube/commit/b5ccf18cd56ee9cc9d8c1c9bb5ffd519c00d0f3c))

### Features

- **ducksdb-driver:** Initial support ([#6403](https://github.com/cube-js/cube/issues/6403)) ([00e41bf](https://github.com/cube-js/cube/commit/00e41bfe8084e9dc7831ff14e2302199af2a8fe3))
- **prestodb-driver:** Add schema filter to informationSchemaQuery ([#6382](https://github.com/cube-js/cube/issues/6382)) Thanks [@maatumo](https://github.com/maatumo) ! ([cbf4620](https://github.com/cube-js/cube/commit/cbf4620bd8b0f5bfa0f02006dee007102258744a))
- **schema-compiler:** Yaml - report error on misconfiguration (object instead of array) ([#6407](https://github.com/cube-js/cube/issues/6407)) ([29e3778](https://github.com/cube-js/cube/commit/29e37780c9c4f320c868880266dc73f55840987f))

## [0.32.21](https://github.com/cube-js/cube/compare/v0.32.20...v0.32.21) (2023-04-06)

### Bug Fixes

- **cubestore:** Query Cache - correct size calculation (complex TableValue) ([#6400](https://github.com/cube-js/cube/issues/6400)) ([b0e3c88](https://github.com/cube-js/cube/commit/b0e3c88e33fdcbb9555faf791b01df4eed884686))
- **databricks-jdbc:** startsWith, endsWith, contains filters ([#6401](https://github.com/cube-js/cube/issues/6401)) ([7eeea60](https://github.com/cube-js/cube/commit/7eeea60353f332bd83ae91c10efa47e2371beea3))

### Features

- **cubestore:** Introduce system.query_cache table ([#6399](https://github.com/cube-js/cube/issues/6399)) ([a5e4bd3](https://github.com/cube-js/cube/commit/a5e4bd393c05518e041ca52d386b1bace672be22))

## [0.32.20](https://github.com/cube-js/cube/compare/v0.32.19...v0.32.20) (2023-04-05)

### Features

- **cubestore:** Query cache - limits by time_to_idle and max_capacity ([#6132](https://github.com/cube-js/cube/issues/6132)) ([edd220f](https://github.com/cube-js/cube/commit/edd220f0e76d43b05d482ff457c710d9ab4c2393))
- **cubestore:** Separate configuration for count of metastore and cachestore snapshots ([#6388](https://github.com/cube-js/cube/issues/6388)) ([df73851](https://github.com/cube-js/cube/commit/df738513e06a3c4199255ae58ab9f52db1a4f2ef))
- **questdb-driver:** Add SSL support ([#6395](https://github.com/cube-js/cube/issues/6395)) ([e9a4458](https://github.com/cube-js/cube/commit/e9a445807638f01c3762430339a84f5fe8518a50))

## [0.32.19](https://github.com/cube-js/cube/compare/v0.32.18...v0.32.19) (2023-04-03)

### Features

- **cubesql:** Support `date_trunc = literal date` filter ([#6376](https://github.com/cube-js/cube/issues/6376)) ([0ef53cb](https://github.com/cube-js/cube/commit/0ef53cb978e8995185a731985944b08f1f24949e))
- **cubestore:** Increase default TRANSPORT_MAX_FRAME_SIZE to 32Mb ([#6381](https://github.com/cube-js/cube/issues/6381)) ([73f7ea9](https://github.com/cube-js/cube/commit/73f7ea9a79882cdcdeff9a7bc916077f86489852))
- **schema-compiler:** Support new aliases for join.relationship ([#6367](https://github.com/cube-js/cube/issues/6367)) ([34a18d8](https://github.com/cube-js/cube/commit/34a18d8936d6e3e27d0dcba2456dbac3f03b0d76))
- Support quarter(s) in dateRange for timeDimensions ([#6385](https://github.com/cube-js/cube/issues/6385)) ([5a08721](https://github.com/cube-js/cube/commit/5a087216dfd95be29d414ec83f4494de8e3a1fb2))

## [0.32.18](https://github.com/cube-js/cube/compare/v0.32.17...v0.32.18) (2023-04-02)

### Bug Fixes

- **athena-driver:** Correct handling for NULL values ([#6171](https://github.com/cube-js/cube/issues/6171)) ([a1f4cd4](https://github.com/cube-js/cube/commit/a1f4cd498e84dde91d736cc87825ae3a8095f9bf))
- **playground:** SQL API default port 15432 ([416a9de](https://github.com/cube-js/cube/commit/416a9de677cb9a1a48f426a74e755b78c18e0952))

### Features

- Cube based includes and meta exposure ([#6380](https://github.com/cube-js/cube/issues/6380)) ([ead4ac0](https://github.com/cube-js/cube/commit/ead4ac0b3b78c074f29fcdec3f86282bce42c45a))
- **cubestore:** Allow to configure max message_size/frame_size for transport ([#6369](https://github.com/cube-js/cube/issues/6369)) ([be7d40b](https://github.com/cube-js/cube/commit/be7d40bcf3be29b2fc0edfe2983162c4b0cafbce))
- **schema-compiler:** Introduce public field (alias for shown) ([#6361](https://github.com/cube-js/cube/issues/6361)) ([6d0c66a](https://github.com/cube-js/cube/commit/6d0c66aad43d51dd23cc3b0cbd6ba3454581afd6))

## [0.32.17](https://github.com/cube-js/cube/compare/v0.32.16...v0.32.17) (2023-03-29)

### Bug Fixes

- **deps:** Upgrade minimist, @xmldom/xmldom, refs [#6340](https://github.com/cube-js/cube/issues/6340) ([#6364](https://github.com/cube-js/cube/issues/6364)) ([04a1c9a](https://github.com/cube-js/cube/commit/04a1c9a04ca2eaae841024eb797e7ee549e98445))
- **deps:** Upgrade moment, moment-timezone, refs [#6340](https://github.com/cube-js/cube/issues/6340) ([#6365](https://github.com/cube-js/cube/issues/6365)) ([d967cf3](https://github.com/cube-js/cube/commit/d967cf3fcf5d3f92ee32e47925fd1090f381a9c5))

### Features

- **schema-compiler:** Introduce support for sqlTable (syntactic sugar) ([#6360](https://github.com/cube-js/cube/issues/6360)) ([c73b368](https://github.com/cube-js/cube/commit/c73b368ca20735c63d01d2364b1c2f26d3b24cc5))
- **sqlite-driver:** Upgrade sqlite3 from 5.0.11 to 5.1.6, refs [#6340](https://github.com/cube-js/cube/issues/6340) ([#6287](https://github.com/cube-js/cube/issues/6287)) ([4ec1da3](https://github.com/cube-js/cube/commit/4ec1da361afa9146b4ac955addd89120cfe23e2d))

### Reverts

- Revert "docs(schema): replace camel-case properties with snake-case" ([4731f4a](https://github.com/cube-js/cube/commit/4731f4ae16c1568f6a0956bc89f79f9a81f53e06))
- Revert "docs: use snake-case property names" ([eb97883](https://github.com/cube-js/cube/commit/eb97883d75985bea7abce5bf3cc434473297cc96))

## [0.32.16](https://github.com/cube-js/cube/compare/v0.32.15...v0.32.16) (2023-03-27)

**Note:** Version bump only for package cubejs

## [0.32.15](https://github.com/cube-js/cube/compare/v0.32.14...v0.32.15) (2023-03-24)

### Bug Fixes

- **cubesql:** Allow any aggregation for number measure as it can be a wildcard ([48f8828](https://github.com/cube-js/cube/commit/48f882867b020ca4a0d058ff147e61ef9bea9555))
- **cubestore-driver:** Fix informationSchemaQuery ([#6338](https://github.com/cube-js/cube/issues/6338)) ([387020d](https://github.com/cube-js/cube/commit/387020d13ae3e82bac7b55699880e16f79940016))
- **cubestore:** RocksStore - specify TTL for WAL to protect logs uploading ([#6345](https://github.com/cube-js/cube/issues/6345)) ([3a9cba4](https://github.com/cube-js/cube/commit/3a9cba478387dd4712b9ea3c566db159a32302d5))

### Features

- **cubestore:** Use XXH3 checksum alg for RocksDB ([#6339](https://github.com/cube-js/cube/issues/6339)) ([866f965](https://github.com/cube-js/cube/commit/866f965044536dac72c74160f9da6abc923e3dfb))
- **playground:** BI connection guides, frontend integration snippets ([#6341](https://github.com/cube-js/cube/issues/6341)) ([8fdeb1b](https://github.com/cube-js/cube/commit/8fdeb1b882d959f9e8aec0ae963e5a0b1995e517))

## [0.32.14](https://github.com/cube-js/cube/compare/v0.32.13...v0.32.14) (2023-03-23)

### Bug Fixes

- **cubestore:** Create table failed: Internal: Directory not empty (os error 39) for local dir storage ([103cabe](https://github.com/cube-js/cube/commit/103cabec6e4d6c59ad1890a3dbe42f7d092eeb6e))
- Use last rollup table types to avoid type guessing for unionWithSourceData lambda queries ([#6337](https://github.com/cube-js/cube/issues/6337)) ([15badfe](https://github.com/cube-js/cube/commit/15badfeb2e1829e81dd914db3bbfd33f105e36ea))

## [0.32.13](https://github.com/cube-js/cube/compare/v0.32.12...v0.32.13) (2023-03-22)

### Bug Fixes

- **cubestore-driver:** Queue - correct APM events ([#6251](https://github.com/cube-js/cube/issues/6251)) ([f4fdfc0](https://github.com/cube-js/cube/commit/f4fdfc03278e64c709e91e7df6cf1a9c936a041c))
- **cubestore:** A single-character boolean value in csv is parsed incorrectly ([#6325](https://github.com/cube-js/cube/issues/6325)) ([ce8f3e2](https://github.com/cube-js/cube/commit/ce8f3e21ec675181ec47174428f14c8ddb7f3bf1))

## [0.32.12](https://github.com/cube-js/cube/compare/v0.32.11...v0.32.12) (2023-03-22)

### Bug Fixes

- **cubesql:** Support quicksight AVG Rebase window exprs: Physical plan does not support logical expression SUM(x) PARTITION BY ([#6328](https://github.com/cube-js/cube/issues/6328)) ([5a5d7e4](https://github.com/cube-js/cube/commit/5a5d7e497f05c69541e04df0a464c85eb9a5f506))
- **cubestore:** If job has been scheduled to non-existent node it'd hang around forever ([#6242](https://github.com/cube-js/cube/issues/6242)) ([69ef0b6](https://github.com/cube-js/cube/commit/69ef0b697bbe87a00687137bff318bb0cfc88015))
- **cubestore:** Retying download from external locations ([#6321](https://github.com/cube-js/cube/issues/6321)) ([50841d9](https://github.com/cube-js/cube/commit/50841d92b7e84d3feec7372119eb540327a4d233))

### Features

- **cubestore-driver:** Support timestamp(3) as a column type ([#6324](https://github.com/cube-js/cube/issues/6324)) ([71dbca4](https://github.com/cube-js/cube/commit/71dbca4ed1fb706d36af322d2659435465804825))
- **server-core, query-orchestrator:** sql-runner - supp… ([#6142](https://github.com/cube-js/cube/issues/6142)) ([32c603d](https://github.com/cube-js/cube/commit/32c603d50f462bf3cf9ce852dc9dcf23fc271cf3))

## [0.32.11](https://github.com/cube-js/cube.js/compare/v0.32.10...v0.32.11) (2023-03-21)

### Bug Fixes

- **cubesql:** Ignore timestamps which can't be represented as nanoseconds instead of failing ([e393b06](https://github.com/cube-js/cube.js/commit/e393b0601fb663b03158ea03143a30eb0086ebbf))
- **cubesql:** Quicksight AVG measures support ([#6323](https://github.com/cube-js/cube.js/issues/6323)) ([ada0afd](https://github.com/cube-js/cube.js/commit/ada0afd17b42a54fbecac69b849abf40158991c1))

## [0.32.10](https://github.com/cube-js/cube.js/compare/v0.32.9...v0.32.10) (2023-03-20)

### Bug Fixes

- Refresh Scheduler - empty securityContext in driverFactory ([#6316](https://github.com/cube-js/cube.js/issues/6316)) ([dd4c9d5](https://github.com/cube-js/cube.js/commit/dd4c9d5a508de03b076850daed78afa18e99eb4f))

## [0.32.9](https://github.com/cube-js/cube.js/compare/v0.32.8...v0.32.9) (2023-03-18)

### Bug Fixes

- **cubesql:** Unexpected response from Cube, Field "count" doesn't exist in row ([6bdc91d](https://github.com/cube-js/cube.js/commit/6bdc91d3eaf51dcb25e7321d03b485147511f049))

## [0.32.8](https://github.com/cube-js/cube.js/compare/v0.32.7...v0.32.8) (2023-03-17)

### Bug Fixes

- **cubesql:** Catch error on TcpStream.peer_addr() ([#6300](https://github.com/cube-js/cube.js/issues/6300)) ([d74a1f0](https://github.com/cube-js/cube.js/commit/d74a1f059ce7f1baa9dad5f216e013a3f0f1bc45))
- **cubesql:** Use writable streams with plain objects instead of JSON.stringify pipe for streaming capability ([#6306](https://github.com/cube-js/cube.js/issues/6306)) ([a9b19fa](https://github.com/cube-js/cube.js/commit/a9b19fa1a1a9c2f0710c8058ed797a4b7a48ed7e))
- **cubestore:** Panic: index out of bounds: the len is 0 but the index is 0 ([#6311](https://github.com/cube-js/cube.js/issues/6311)) ([41dbbc0](https://github.com/cube-js/cube.js/commit/41dbbc03365f6067e85a09e8d5cafbfc6fe39990))
- **query-orchestrator:** queues reconciliation logic ([#6286](https://github.com/cube-js/cube.js/issues/6286)) ([b45def4](https://github.com/cube-js/cube.js/commit/b45def46d99710e4fcf43cbc84b1b38341b859f6))

### Features

- **cubestore:** RocksStore - panic protection for broken rw loop ([#6293](https://github.com/cube-js/cube.js/issues/6293)) ([1f2db32](https://github.com/cube-js/cube.js/commit/1f2db32c289c119468f4cbe522fa40bab1a00068))

## [0.32.7](https://github.com/cube-js/cube.js/compare/v0.32.6...v0.32.7) (2023-03-14)

### Bug Fixes

- Cross data source `rollupLambda` uses incorrect refreshKey ([#6288](https://github.com/cube-js/cube.js/issues/6288)) ([f87be98](https://github.com/cube-js/cube.js/commit/f87be98c68bcdcb65bd317b845571ad186586bd1))

## [0.32.6](https://github.com/cube-js/cube.js/compare/v0.32.5...v0.32.6) (2023-03-14)

### Bug Fixes

- **firebolt-driver:** numeric null value ([#6284](https://github.com/cube-js/cube.js/issues/6284)) ([b5cc5b5](https://github.com/cube-js/cube.js/commit/b5cc5b56a16defe3b6412bad384ae5e07eeaeace))

### Features

- **schema-compiler:** quarter granularity ([#6285](https://github.com/cube-js/cube.js/issues/6285)) ([22f6102](https://github.com/cube-js/cube.js/commit/22f6102e137940b6882d55f560d826feceaf470d))

## [0.32.5](https://github.com/cube-js/cube.js/compare/v0.32.4...v0.32.5) (2023-03-13)

### Bug Fixes

- Allow to specify cubestore as cacheAndQueueDriver from config, fix [#6279](https://github.com/cube-js/cube.js/issues/6279) ([#6280](https://github.com/cube-js/cube.js/issues/6280)) ([6b48c65](https://github.com/cube-js/cube.js/commit/6b48c65bf5dfe042b507171afb726328fa16e75c))
- **api-gateway:** permissions cache removed ([#6276](https://github.com/cube-js/cube.js/issues/6276)) ([03be6d2](https://github.com/cube-js/cube.js/commit/03be6d29afbf13a053d5c2036ee4eae53a500892))
- **cubestore:** Setup format_version = 5 (previusly used with RocksDB 6.20.3) ([#6283](https://github.com/cube-js/cube.js/issues/6283)) ([5126d00](https://github.com/cube-js/cube.js/commit/5126d00c64331c99d6aadc3ab7ccbd1c49dc1732))
- **hive-driver:** Follow DriverInterface for query method, fix [#6281](https://github.com/cube-js/cube.js/issues/6281) ([#6282](https://github.com/cube-js/cube.js/issues/6282)) ([e81aba9](https://github.com/cube-js/cube.js/commit/e81aba9fd3dcf8b08092b9c7302067b230d1bbdc))

## [0.32.4](https://github.com/cube-js/cube.js/compare/v0.32.3...v0.32.4) (2023-03-10)

### Bug Fixes

- **api-gateway:** contextToPermissions default permissions parameter ([#6268](https://github.com/cube-js/cube.js/issues/6268)) ([b234afc](https://github.com/cube-js/cube.js/commit/b234afc9877e4d80aa05528b32d9234b2730f0c5))
- **api-gateway:** permissions cache removed ([#6271](https://github.com/cube-js/cube.js/issues/6271)) ([cd32266](https://github.com/cube-js/cube.js/commit/cd322665e5973dd8551bb039b3d0ad66a3a16d7f))
- **api-gateway:** permissions error message ([#6269](https://github.com/cube-js/cube.js/issues/6269)) ([53d71a2](https://github.com/cube-js/cube.js/commit/53d71a2167a9d262520f4be3fbc1a458e8cc704c))
- **api-gateway:** permissions unit test fix ([#6270](https://github.com/cube-js/cube.js/issues/6270)) ([15df7c1](https://github.com/cube-js/cube.js/commit/15df7c101dc664084f475764743694993a5cacdb))
- Catch exception from extendContext (unhandledRejection) ([#6256](https://github.com/cube-js/cube.js/issues/6256)) ([9fa0bd9](https://github.com/cube-js/cube.js/commit/9fa0bd95c49bf4885224b43c42cad0d2327d026d))
- **cubesql:** Failed printing to stdout: Resource temporarily unavailable ([#6272](https://github.com/cube-js/cube.js/issues/6272)) ([d8dd8a6](https://github.com/cube-js/cube.js/commit/d8dd8a60dba1eb3092dd252ccf98fa6e1de43dd4))

### Features

- **cubestore/docker:** Upgrade CLang to 15 ([979246b](https://github.com/cube-js/cube.js/commit/979246bdc8498d874aee755a4858c3008761ed54))
- **cubestore/docker:** Upgrade OS to bullseye ([c512586](https://github.com/cube-js/cube.js/commit/c51258643bb24279ebe71e1279d0078efe32fce9))
- **cubestore:** Support quarter intervals ([#6266](https://github.com/cube-js/cube.js/issues/6266)) ([35cc5c8](https://github.com/cube-js/cube.js/commit/35cc5c83e270e507e36e0602e707c8ed3f64872f))
- **cubestore:** Upgrade RocksDB to 7.9.2 (0.20) ([#6221](https://github.com/cube-js/cube.js/issues/6221)) ([92bbef3](https://github.com/cube-js/cube.js/commit/92bbef34bd8fe34d9c8126bcf93c0535adcb289d))

## [0.32.3](https://github.com/cube-js/cube.js/compare/v0.32.2...v0.32.3) (2023-03-07)

### Features

- **cubestore:** Streaming optimizations ([#6228](https://github.com/cube-js/cube.js/issues/6228)) ([114a847](https://github.com/cube-js/cube.js/commit/114a84778a84ff050f4d08eafdafa21efbf46646))
- **server-core, api-gateway:** Permissions API ([#6240](https://github.com/cube-js/cube.js/issues/6240)) ([aad6aa3](https://github.com/cube-js/cube.js/commit/aad6aa3e327503487d8c9b4b85ec047d3fc0843e))

## [0.32.2](https://github.com/cube-js/cube.js/compare/v0.32.1...v0.32.2) (2023-03-07)

### Bug Fixes

- **cubestore-driver:** Correct pending count and active keys in queue logs ([#6250](https://github.com/cube-js/cube.js/issues/6250)) ([3607c67](https://github.com/cube-js/cube.js/commit/3607c67ea0fa137c5f97e9d3b6fc67805ce85f68))
- **redshift-driver:** fixes column order ([#6068](https://github.com/cube-js/cube.js/issues/6068)) Thanks [@rdwoodring](https://github.com/rdwoodring)! ([3bba803](https://github.com/cube-js/cube.js/commit/3bba803289c15be8cc6c9fe8476dc4ea8f46f40b))
- Replace deprecated `@hapi/joi` with `joi` ([#6223](https://github.com/cube-js/cube.js/issues/6223)) Thanks [@hehex9](https://github.com/hehex9) ! ([ccbcc50](https://github.com/cube-js/cube.js/commit/ccbcc501dc91ef68ca49ddced79316425ae8f215))

### Features

- connection validation and logging ([#6233](https://github.com/cube-js/cube.js/issues/6233)) ([6dc48f8](https://github.com/cube-js/cube.js/commit/6dc48f8dc8045234dfa9fe8922534c5204e6e569))
- **cubestore:** QUEUE - support extended flag for retrieve ([#6248](https://github.com/cube-js/cube.js/issues/6248)) ([9f23924](https://github.com/cube-js/cube.js/commit/9f23924a306c8141aa0cc8d990aeffdd5f3b4135))
- Enable drop pre-aggregations without touch by default. **NOTE:** This change may adversely affect deployments which has incorrectly configured Refresh Worker instance. ([291977b](https://github.com/cube-js/cube.js/commit/291977b58ffb09e5886b6b1dbd46f9acd16a32ec))
- Support rollupLambda across different cubes and data sources ([#6245](https://github.com/cube-js/cube.js/issues/6245)) ([fa284a4](https://github.com/cube-js/cube.js/commit/fa284a4e04ac0f80e9e06fb1dc48b5c07204c5a4))

## [0.32.1](https://github.com/cube-js/cube.js/compare/v0.32.0...v0.32.1) (2023-03-03)

### Bug Fixes

- **cubesql:** Replace stream buffering with async implementation ([#6127](https://github.com/cube-js/cube.js/issues/6127)) ([5186d30](https://github.com/cube-js/cube.js/commit/5186d308cedf103b08c8a8140de84984839c710a))

# [0.32.0](https://github.com/cube-js/cube.js/compare/v0.31.69...v0.32.0) (2023-03-02)

### Features

- **docker:** Remove gcc/g++/cmake/python from final images ([bb0a0e7](https://github.com/cube-js/cube.js/commit/bb0a0e7eac6044141e446f802d858529b4ff9782))
- **docker:** Upgrade Node.js to 16.x ([8fe0e04](https://github.com/cube-js/cube.js/commit/8fe0e048e8d485b8775105be29d96f27cb4adf40))

### Breaking changes

- Use cubestore driver for queue & cache ([a54aef](https://github.com/cube-js/cube.js/commit/5d88fed03a3e6599f9b77610f8657161f9a54aef))
- Remove absolute import for @cubejs-backend/server-core ([361848](https://github.com/cube-js/cube.js/commit/6ee42184da3b36c793ec43284964cd885c361848))
- Remove absolute import for @cubejs-backend/schema-compiler ([f3669](https://github.com/cube-js/cube.js/commit/d028937a92e6bf5d718ffd89b1676f90976f3669))
- Remove absolute import for @cubejs-backend/query-orchestrator ([578c9](https://github.com/cube-js/cube.js/commit/45bd678a547ac71fc7d69471c4c72e49238578c9))
- Remove support for Node.js 12, 15 ([78d76f1](https://github.com/cube-js/cube.js/commit/6568057fa6e6cc7e2fa02a7826d07fe3778d76f1))

## [0.31.69](https://github.com/cube-js/cube.js/compare/v0.31.68...v0.31.69) (2023-03-01)

### Bug Fixes

- **cubestore:** Wrong triggering of table deactivation in some cases ([#6119](https://github.com/cube-js/cube.js/issues/6119)) ([523c0c2](https://github.com/cube-js/cube.js/commit/523c0c2cbcd981a114ab348f83e719526a6a3a0a))
- Throw error message on misconfiguration with Cube Store as queue/cache driver. ([#6199](https://github.com/cube-js/cube.js/issues/6199)) ([ae06ef7](https://github.com/cube-js/cube.js/commit/ae06ef74c15fed996cc2d745d6ecdd5e03bb86f3))

## [0.31.68](https://github.com/cube-js/cube.js/compare/v0.31.67...v0.31.68) (2023-02-28)

### Bug Fixes

- **cubestore:** Metastore logs are uploaded without the corresponding snapshot ([#6222](https://github.com/cube-js/cube.js/issues/6222)) ([cfa4b47](https://github.com/cube-js/cube.js/commit/cfa4b4780f0b1549544bf26e2fb843d69ccdc981))
- **jdbc-driver, databricks-jdbc-driver:** clearTimeout on validate catch, temp table name ([#6220](https://github.com/cube-js/cube.js/issues/6220)) ([c2c7118](https://github.com/cube-js/cube.js/commit/c2c711815a32fa597fa70937a27a14cdc86cad1f))

## [0.31.67](https://github.com/cube-js/cube.js/compare/v0.31.66...v0.31.67) (2023-02-27)

### Bug Fixes

- Reconcile streaming queries in case of cluster execution ([#6204](https://github.com/cube-js/cube.js/issues/6204)) ([dcf7866](https://github.com/cube-js/cube.js/commit/dcf78660bb07bb033665ac698e8b45af3565845b))
- **schema-compiler:** skip empty YAML files ([b068f7f](https://github.com/cube-js/cube.js/commit/b068f7f4f44c8bede3d45aac8df4bc8b4f38912e))

### Features

- **cube-cli:** Schema validation command ([#6208](https://github.com/cube-js/cube.js/issues/6208)) ([1fc6490](https://github.com/cube-js/cube.js/commit/1fc64906e5e628437fb58d35feea8ab3aa5bfe06))

## [0.31.66](https://github.com/cube-js/cube.js/compare/v0.31.65...v0.31.66) (2023-02-27)

### Bug Fixes

- databricks pre-aggregation errors ([#6207](https://github.com/cube-js/cube.js/issues/6207)) ([e7297f7](https://github.com/cube-js/cube.js/commit/e7297f735b64d3ce80ff4715c7b40ccee73cdb40))

### Features

- **cubestore:** Streaming ingestion optimizations ([#6198](https://github.com/cube-js/cube.js/issues/6198)) ([fb896ba](https://github.com/cube-js/cube.js/commit/fb896ba10bb2cbeacd14c7192a4a9b2d1e40abef))
- **docker:** Use Debian (bullseye) for dev images ([#6202](https://github.com/cube-js/cube.js/issues/6202)) ([b4c922a](https://github.com/cube-js/cube.js/commit/b4c922a898e81df0bf3b4116c8ac51072ecc2382))

## [0.31.65](https://github.com/cube-js/cube.js/compare/v0.31.64...v0.31.65) (2023-02-23)

### Bug Fixes

- **oracle-driver:** Release connection after query execution ([#5469](https://github.com/cube-js/cube.js/issues/5469)) ([ff1af78](https://github.com/cube-js/cube.js/commit/ff1af789c6326e25b3a341c4bae1829da95c36c1))

## [0.31.64](https://github.com/cube-js/cube.js/compare/v0.31.63...v0.31.64) (2023-02-21)

### Bug Fixes

- **cubejs-playground:** error overflow ([#6170](https://github.com/cube-js/cube.js/issues/6170)) ([b5f68bb](https://github.com/cube-js/cube.js/commit/b5f68bbc8c0091089bb2c43bcbcf44a9da8a7182))
- **jdbc-driver:** ResourceRequest timeout ([#6186](https://github.com/cube-js/cube.js/issues/6186)) ([b58c44e](https://github.com/cube-js/cube.js/commit/b58c44e116930f6dfaa4107e9c8ca14b472bfd77))

### Features

- **cubesql:** Remove unexpected clone (reduce memory consumption) ([#6185](https://github.com/cube-js/cube.js/issues/6185)) ([904556b](https://github.com/cube-js/cube.js/commit/904556b83e724e6b55e65afd9dbd077bb6c9ea99))
- **cubestore:** Allow tinyint type (map as Int) ([#6174](https://github.com/cube-js/cube.js/issues/6174)) ([47571e5](https://github.com/cube-js/cube.js/commit/47571e566b7991f3bac902fcecce9c3ac3328a10))

## [0.31.63](https://github.com/cube-js/cube.js/compare/v0.31.62...v0.31.63) (2023-02-20)

### Bug Fixes

- **cubesql:** `CAST(column AS DATE)` to DateTrunc day ([8f6fbe2](https://github.com/cube-js/cube.js/commit/8f6fbe274fa659870f9a736f4ac0c8e8406c64d0))
- **cubestore:** Support realiasing for information schema tables ([#6155](https://github.com/cube-js/cube.js/issues/6155)) ([423a3ec](https://github.com/cube-js/cube.js/commit/423a3ec33953e816ca94fb33ed9f4ec2084e640c))
- **databricks-jdbc-driver:** export table drops twice in Databricks ([#6177](https://github.com/cube-js/cube.js/issues/6177)) ([a62d9f0](https://github.com/cube-js/cube.js/commit/a62d9f0c23943224170fddb19f3a7c784920aa48))
- **playground:** closing the last query tab ([aaa75f4](https://github.com/cube-js/cube.js/commit/aaa75f4d0d2cda73ca9070495ef788ef704e7e17))

### Features

- **cubestore:** Use separate scheduler for Cache Store ([#6160](https://github.com/cube-js/cube.js/issues/6160)) ([a17f59b](https://github.com/cube-js/cube.js/commit/a17f59b673f6a6336d52c4eafffbc237c1ca39e7))
- graphql api variables ([#6153](https://github.com/cube-js/cube.js/issues/6153)) ([5f0f705](https://github.com/cube-js/cube.js/commit/5f0f7053022f437e61d23739b9acfb364fb06a16))

## [0.31.62](https://github.com/cube-js/cube.js/compare/v0.31.61...v0.31.62) (2023-02-13)

### Bug Fixes

- streaming ([#6156](https://github.com/cube-js/cube.js/issues/6156)) ([abcbc1e](https://github.com/cube-js/cube.js/commit/abcbc1ed8f496ffa322053cc2dac9a7cc5b38dcd))

### Features

- **cubestore:** Introduce information_schema.columns ([#6152](https://github.com/cube-js/cube.js/issues/6152)) ([c70b155](https://github.com/cube-js/cube.js/commit/c70b155780f3eba491f093cfbb56bd1c297eae34))

## [0.31.61](https://github.com/cube-js/cube.js/compare/v0.31.60...v0.31.61) (2023-02-10)

**Note:** Version bump only for package cubejs

## [0.31.60](https://github.com/cube-js/cube.js/compare/v0.31.59...v0.31.60) (2023-02-10)

### Bug Fixes

- GraphQL API date range filter ([#6138](https://github.com/cube-js/cube.js/issues/6138)) ([dc2ea8c](https://github.com/cube-js/cube.js/commit/dc2ea8cb430a6e013cfa32f42294ee6a7c4206b6))
- **shared:** getProxySettings - use global configuration ([#6137](https://github.com/cube-js/cube.js/issues/6137)) ([e2ba5c1](https://github.com/cube-js/cube.js/commit/e2ba5c1962f2c57e79ce405c22209ab6890f400d))

### Features

- **cubesql:** Redesign member pushdown to support more advanced join… ([#6122](https://github.com/cube-js/cube.js/issues/6122)) ([3bb85e4](https://github.com/cube-js/cube.js/commit/3bb85e492056d73c28b3d006a95e0f9765e6e026))
- **cubestore:** Disable MetastoreEvents for queue & cache ([#6126](https://github.com/cube-js/cube.js/issues/6126)) ([0fd20cb](https://github.com/cube-js/cube.js/commit/0fd20cb409703945fecc3620833b8d5cc683ca0e))
- **cubestore:** Limit pushdown ([#5604](https://github.com/cube-js/cube.js/issues/5604)) ([f593ffe](https://github.com/cube-js/cube.js/commit/f593ffe686b9dfd40af9719226b583fc43f43648))
- **cubestore:** Support SYS DROP QUERY CACHE + metrics ([#6131](https://github.com/cube-js/cube.js/issues/6131)) ([e8d2670](https://github.com/cube-js/cube.js/commit/e8d2670848656fa130bd76f2f98bca6b282143d3))

## [0.31.59](https://github.com/cube-js/cube.js/compare/v0.31.58...v0.31.59) (2023-02-06)

### Bug Fixes

- **server-core:** config validation if custom ContextAcceptor is ([#6120](https://github.com/cube-js/cube.js/issues/6120)) ([6524a84](https://github.com/cube-js/cube.js/commit/6524a848e3459cf7de59fecd2d38f7f88a5fdc6f))

### Features

- **api-gateway, server-core:** added endpoint to fetch db schema ([#5852](https://github.com/cube-js/cube.js/issues/5852)) ([8ffcfd2](https://github.com/cube-js/cube.js/commit/8ffcfd22f75b745c90023c54bad19f6c5c9e87e5))

## [0.31.58](https://github.com/cube-js/cube.js/compare/v0.31.57...v0.31.58) (2023-02-02)

### Bug Fixes

- **jdbc-driver:** streamQuery connection release ([#6108](https://github.com/cube-js/cube.js/issues/6108)) ([3426e6e](https://github.com/cube-js/cube.js/commit/3426e6e46a12aba2e85671cf22dcf4dd005d785c))

### Features

- **cubesql:** Improve catching of panic's reason ([#6107](https://github.com/cube-js/cube.js/issues/6107)) ([c8cf300](https://github.com/cube-js/cube.js/commit/c8cf3007b5bcb4f0362e5e3721eccadf69bcea62))
- **cubestore:** Queue - support custom orphaned timeout ([#6090](https://github.com/cube-js/cube.js/issues/6090)) ([d6702ab](https://github.com/cube-js/cube.js/commit/d6702ab1d7c38111799edf9227e3aa53c51cc237))
- **native:** Correct error handling for neon::channel.send ([#6106](https://github.com/cube-js/cube.js/issues/6106)) ([f71255a](https://github.com/cube-js/cube.js/commit/f71255abdae1d933101e0bc4002fd83373278067))
- **snowflake-sdk:** Security upgrade snowflake-sdk from 1.6.14 to 1.6.18 ([#6097](https://github.com/cube-js/cube.js/issues/6097)) ([1f10c8a](https://github.com/cube-js/cube.js/commit/1f10c8a7cf088328e4e9a30d74e34338709af9b1))

## [0.31.57](https://github.com/cube-js/cube.js/compare/v0.31.56...v0.31.57) (2023-02-02)

### Features

- **cubejs-api-gateway:** Add scope check to sql runner ([#6053](https://github.com/cube-js/cube.js/issues/6053)) ([d79d3aa](https://github.com/cube-js/cube.js/commit/d79d3aae32ce185614a65912f1c8229128792c48))
- **cubejs-server-core:** add dbType to dataSources ([#6089](https://github.com/cube-js/cube.js/issues/6089)) ([5c84467](https://github.com/cube-js/cube.js/commit/5c844677d841a65cb7eb3eadd36e69ed59881fd6))
- **cubestore:** Cache/Queue - implement migrations (truncating) ([#6083](https://github.com/cube-js/cube.js/issues/6083)) ([e8daf5e](https://github.com/cube-js/cube.js/commit/e8daf5e70e947c04675e2acc6654deb9484e2497))
- **cubestore:** Queue - correct sort over priority (+created) ([#6094](https://github.com/cube-js/cube.js/issues/6094)) ([bd489cd](https://github.com/cube-js/cube.js/commit/bd489cd7981dd94766f28ae4621fe993252d63c1))

## [0.31.56](https://github.com/cube-js/cube.js/compare/v0.31.55...v0.31.56) (2023-01-31)

### Bug Fixes

- (dimensions.type.ownedByCube = true) is not allowed if dynamic schemas outside of schema folder are used ([88b09bd](https://github.com/cube-js/cube.js/commit/88b09bd14e13a05599c9f433063832a17664ed92))
- **cubesql:** Allow Thoughtspot `EXTRACT YEAR AS date` ([22d0ad9](https://github.com/cube-js/cube.js/commit/22d0ad967380b4ece695b567e77a216a16b3bf17))
- **cubestore-driver:** Correct cancellation handling ([#6087](https://github.com/cube-js/cube.js/issues/6087)) ([03089ce](https://github.com/cube-js/cube.js/commit/03089cebb5a932eb69abb637ef8b7998beac115c))
- **playground:** filter nullish refs ([9732af6](https://github.com/cube-js/cube.js/commit/9732af6bfe2b3dad78c0b9e669ba1c6b8b4815dd))

### Features

- **cubestore:** Max disk space limit ([#6084](https://github.com/cube-js/cube.js/issues/6084)) ([cc6003b](https://github.com/cube-js/cube.js/commit/cc6003b6a6df9744a6cf4aab833230fdca0a4d11))
- **cubestore:** Max disk space limit per worker ([#6085](https://github.com/cube-js/cube.js/issues/6085)) ([ed2ca79](https://github.com/cube-js/cube.js/commit/ed2ca798b38d5496d4a92a4fe0227e6a25361037))

## [0.31.55](https://github.com/cube-js/cube.js/compare/v0.31.54...v0.31.55) (2023-01-26)

### Bug Fixes

- **cubesql:** Correct Thoughtspot day in quarter offset ([d62079e](https://github.com/cube-js/cube.js/commit/d62079eaadaaa81d9b1e45580b27d7597192263e))

### Features

- **cubestore-driver:** Queue - support persistent flag/stream handling ([#6046](https://github.com/cube-js/cube.js/issues/6046)) ([5b12ec8](https://github.com/cube-js/cube.js/commit/5b12ec874f33d30a02df55f792f1fc1ce0a30bf4))
- new logo with background ([#6062](https://github.com/cube-js/cube.js/issues/6062)) ([7c3601b](https://github.com/cube-js/cube.js/commit/7c3601b259a05fe3c8318e2aa62f3553f45de5a3))

## [0.31.54](https://github.com/cube-js/cube.js/compare/v0.31.53...v0.31.54) (2023-01-25)

### Bug Fixes

- **cubestore:** Support macOS 11 for standalone binary ([#6060](https://github.com/cube-js/cube.js/issues/6060)) ([6d2dff7](https://github.com/cube-js/cube.js/commit/6d2dff76021b2ee86ab6856f1f5c7864a9da658f))

### Features

- **cubestore:** Filter for data from kafka streams ([#6054](https://github.com/cube-js/cube.js/issues/6054)) ([3b1b1ab](https://github.com/cube-js/cube.js/commit/3b1b1ab052c96d1de635da0eb10fdea5701c0442))
- **cubestore:** Queue - improve orphaned detection + optimizations ([#6051](https://github.com/cube-js/cube.js/issues/6051)) ([70e68ae](https://github.com/cube-js/cube.js/commit/70e68ae35ed7bc66b8306512ae68c7919e45e2cc))

## [0.31.53](https://github.com/cube-js/cube.js/compare/v0.31.52...v0.31.53) (2023-01-24)

### Bug Fixes

- Last lambda partition is incorrectly filtered in case of middle partition is filtered out ([656edcf](https://github.com/cube-js/cube.js/commit/656edcf24f8a784cf3b5425e7d501260d48a9dc1))

### Features

- **cubestore:** Correct metrics for queue/cache (ignore long commands) ([#6047](https://github.com/cube-js/cube.js/issues/6047)) ([1d5a1f6](https://github.com/cube-js/cube.js/commit/1d5a1f679b3b1ed8a2b132acb63640405443dc43))

## [0.31.52](https://github.com/cube-js/cube.js/compare/v0.31.51...v0.31.52) (2023-01-23)

### Bug Fixes

- **query-orchestrator:** streams cluster ([#6048](https://github.com/cube-js/cube.js/issues/6048)) ([c5b6702](https://github.com/cube-js/cube.js/commit/c5b6702f69788cbcbf959608e5e86abfa4bd5385))

### Features

- **api-gateway, server-core:** Added dataSources method ([#5789](https://github.com/cube-js/cube.js/issues/5789)) ([128d017](https://github.com/cube-js/cube.js/commit/128d017e2c8eb75d9439a3d09c6739bc0c552938))
- **cubestore:** Support multiple parallel QUEUE RESULT_BLOCKING ([#6038](https://github.com/cube-js/cube.js/issues/6038)) ([d8be78a](https://github.com/cube-js/cube.js/commit/d8be78a95f8fae466615063433a5ae53ab1c1bd6))
- **cubestore:** Upgrade warp to 0.3.3 (use crates.io instead of git) ([#6043](https://github.com/cube-js/cube.js/issues/6043)) ([d2307a8](https://github.com/cube-js/cube.js/commit/d2307a8ce3449d8085810463e97d33de4b340db4))
- new dark scheme at README ([#6044](https://github.com/cube-js/cube.js/issues/6044)) ([a44ebbb](https://github.com/cube-js/cube.js/commit/a44ebbbe8c0cbd2b94db6dfd5f7b9f30fa18bd34))

## [0.31.51](https://github.com/cube-js/cube.js/compare/v0.31.50...v0.31.51) (2023-01-21)

### Bug Fixes

- Incorrect partition filter for real time partitions ([6520279](https://github.com/cube-js/cube.js/commit/6520279096048f3cd19b38e5c431dd5a5c29323a))

## [0.31.50](https://github.com/cube-js/cube.js/compare/v0.31.49...v0.31.50) (2023-01-21)

### Bug Fixes

- Lambda pre-aggregations aren't served by external refresh instance ([bd45720](https://github.com/cube-js/cube.js/commit/bd4572083a9560cddb7ef23e2eccc9c9c806378a))
- Real time partitions seal just after created ([882a7df](https://github.com/cube-js/cube.js/commit/882a7df2d1184e754c41d7f5daea9f43f3ac53f8))

## [0.31.49](https://github.com/cube-js/cube.js/compare/v0.31.48...v0.31.49) (2023-01-20)

### Bug Fixes

- Content version gets updated on build range change ([0adc634](https://github.com/cube-js/cube.js/commit/0adc63493d51c74a68004c09fb239f1234dffe4a))
- **cubestore-driver:** Unexpected token u in JSON at position 0 ([#6037](https://github.com/cube-js/cube.js/issues/6037)) ([1d00521](https://github.com/cube-js/cube.js/commit/1d005214c2b18f092ede5c9187798af151793cf8))

## [0.31.48](https://github.com/cube-js/cube.js/compare/v0.31.47...v0.31.48) (2023-01-20)

### Features

- **cubesql:** Postgres protocol - stream support ([#6025](https://github.com/cube-js/cube.js/issues/6025)) ([d5786df](https://github.com/cube-js/cube.js/commit/d5786df63a1f48dec2697a8bb5e8c017c1b13ae4))
- **cubesql:** Streams - cancel query and drop conection handling ([8c585f2](https://github.com/cube-js/cube.js/commit/8c585f24003c768300a31e0ed6774a3a724e54fa))
- **cubestore:** Collect metrics for queue/cache commands ([#6032](https://github.com/cube-js/cube.js/issues/6032)) ([9ac7d0c](https://github.com/cube-js/cube.js/commit/9ac7d0ce67571c6945b7afe4095c995a1190da19))
- **cubestore:** Support negative priority for queue ([#6031](https://github.com/cube-js/cube.js/issues/6031)) ([9924a1a](https://github.com/cube-js/cube.js/commit/9924a1a900f83fe246ede0b3a19e0c6ea7d5efc4))
- streaming desync ([#6034](https://github.com/cube-js/cube.js/issues/6034)) ([a4c8b09](https://github.com/cube-js/cube.js/commit/a4c8b09a680d2857d28a42817d29fa567dcf63b2))

## [0.31.47](https://github.com/cube-js/cube.js/compare/v0.31.46...v0.31.47) (2023-01-18)

### Bug Fixes

- Re-use external connection for CubeStore in queue ([#6028](https://github.com/cube-js/cube.js/issues/6028)) ([ff724f7](https://github.com/cube-js/cube.js/commit/ff724f734039d059b762c9200688c7b1afcb0f76))

### Features

- **cubestore:** Correct queue add handling ([#6022](https://github.com/cube-js/cube.js/issues/6022)) ([0f4a431](https://github.com/cube-js/cube.js/commit/0f4a431bc6c2b1324bbfd53dd1204874120a5082))

## [0.31.46](https://github.com/cube-js/cube.js/compare/v0.31.45...v0.31.46) (2023-01-18)

### Bug Fixes

- **athena-driver:** Help user to understand CUBEJS_AWS_S3_OUTPUT_LOCATION parameter is missing ([#5991](https://github.com/cube-js/cube.js/issues/5991)) Thanks [@fbalicchia](https://github.com/fbalicchia) ! ([eedd12a](https://github.com/cube-js/cube.js/commit/eedd12a18645d2d6faa68a43963a6cb98560b5a6))
- Do not fail if partitions are not ready for specified date range interval but there's at least one ready for pre-aggregation ([#6026](https://github.com/cube-js/cube.js/issues/6026)) ([2d39fe4](https://github.com/cube-js/cube.js/commit/2d39fe48b31b544f4aefa00c87e084102d134b2e))

### Features

- **query-orchestrator:** Introduce CubeStoreQueueDriver ([#6014](https://github.com/cube-js/cube.js/issues/6014)) ([f4744bf](https://github.com/cube-js/cube.js/commit/f4744bfb218a8a8cb28effe28237867157d01074))

## [0.31.45](https://github.com/cube-js/cube.js/compare/v0.31.44...v0.31.45) (2023-01-16)

### Bug Fixes

- **cubestore:** Panic on combination of `Union All` and single select in root `Union All` ([#6012](https://github.com/cube-js/cube.js/issues/6012)) ([0d1a3d8](https://github.com/cube-js/cube.js/commit/0d1a3d8e2732d47f193aa5cf018e8ca5c6d7cf31))
- Do not update structure version on build range end update ([#6015](https://github.com/cube-js/cube.js/issues/6015)) ([7891b6c](https://github.com/cube-js/cube.js/commit/7891b6c7d3033cfdf4705402ce54a156f24f29e3))

### Features

- **cubestore:** Initial queue support ([#5541](https://github.com/cube-js/cube.js/issues/5541)) ([7109039](https://github.com/cube-js/cube.js/commit/7109039beaf5819fa1179751e98a3431a04b3cac))

## [0.31.44](https://github.com/cube-js/cube.js/compare/v0.31.43...v0.31.44) (2023-01-16)

### Bug Fixes

- Rollups are sorted by name in `rollups` params of pre-aggregations ([#6011](https://github.com/cube-js/cube.js/issues/6011)) ([235ec70](https://github.com/cube-js/cube.js/commit/235ec70c296846f02e337acc4be90b4863556d15))

## [0.31.43](https://github.com/cube-js/cube.js/compare/v0.31.42...v0.31.43) (2023-01-16)

### Bug Fixes

- **cubestore:** Panic when nested Union All ([#6010](https://github.com/cube-js/cube.js/issues/6010)) ([331242e](https://github.com/cube-js/cube.js/commit/331242e263435513544ece0ee7b174d14562f05b))
- fix a mistake in the websocket context acceptance mechanism ([#6006](https://github.com/cube-js/cube.js/issues/6006)) ([1d8a9bd](https://github.com/cube-js/cube.js/commit/1d8a9bd06804681ef74f6e4e70357cefb05f9c29))

### Features

- Pre-aggregations API for `rollupLambda` support ([#6009](https://github.com/cube-js/cube.js/issues/6009)) ([b90b3f2](https://github.com/cube-js/cube.js/commit/b90b3f2b5c991a0046220d7821ce74fc11ddbb85))

## [0.31.42](https://github.com/cube-js/cube.js/compare/v0.31.41...v0.31.42) (2023-01-15)

### Bug Fixes

- **cubestore:** Large memory consumption while waiting for the queue cache ([#6004](https://github.com/cube-js/cube.js/issues/6004)) ([8c029bb](https://github.com/cube-js/cube.js/commit/8c029bb96cf4c451f061cef2d83b5d5dc3a8637c))
- **cubestore:** Large memory consumption while waiting for the queue cache: drop context ([#6005](https://github.com/cube-js/cube.js/issues/6005)) ([b39920f](https://github.com/cube-js/cube.js/commit/b39920f6f5f2b10b2ea598712bd5f5d1b02a3308))

### Features

- **api-gateway:** added result filter for sql-runner ([#5985](https://github.com/cube-js/cube.js/issues/5985)) ([89f1b92](https://github.com/cube-js/cube.js/commit/89f1b922e1e3d8a02fbdaf2bda2c2cb77c0bdc78))
- Multiple rollups in `rollupLambda` support ([#6008](https://github.com/cube-js/cube.js/issues/6008)) ([84fff0d](https://github.com/cube-js/cube.js/commit/84fff0d7745ccf657bb8eca0c5cc426c82ffd516))
- **native:** Channel.resolve/reject without lock ([#6001](https://github.com/cube-js/cube.js/issues/6001)) ([9133a3b](https://github.com/cube-js/cube.js/commit/9133a3bf885ca17f14ac18041a1692201d4c1cfb))

## [0.31.41](https://github.com/cube-js/cube.js/compare/v0.31.40...v0.31.41) (2023-01-13)

### Bug Fixes

- **cubestore:** Metastore get_tables_with_path cache is not work correctly ([#6000](https://github.com/cube-js/cube.js/issues/6000)) ([75699c8](https://github.com/cube-js/cube.js/commit/75699c85f3b428d3e1967642b2c440be77e7b095))

### Features

- streaming capabilities ([#5995](https://github.com/cube-js/cube.js/issues/5995)) ([d336c4e](https://github.com/cube-js/cube.js/commit/d336c4eaa3547422484bb003df19dfd4c7be5f96))

## [0.31.40](https://github.com/cube-js/cube.js/compare/v0.31.39...v0.31.40) (2023-01-12)

### Features

- **cubesql:** Support `date_trunc` over column filter ([c9e71e6](https://github.com/cube-js/cube.js/commit/c9e71e6a13f41e2af388b7f91043e4118ba91f40))
- **docker:** Alpine - migrate to 3.15 (3.14 was removed) ([#5997](https://github.com/cube-js/cube.js/issues/5997)) ([cf9078a](https://github.com/cube-js/cube.js/commit/cf9078a2c29d31d78b8c1e100ce254495128e1a0))
- Hooks for the context rejection mechanism ([#5901](https://github.com/cube-js/cube.js/issues/5901)) ([b6f0506](https://github.com/cube-js/cube.js/commit/b6f0506e1b5821a16857d6695a40c6b957887941))

## [0.31.39](https://github.com/cube-js/cube.js/compare/v0.31.38...v0.31.39) (2023-01-12)

### Bug Fixes

- **cubesql:** Query cancellation for simple query protocol ([#5987](https://github.com/cube-js/cube.js/issues/5987)) ([aae758f](https://github.com/cube-js/cube.js/commit/aae758f83d45a2572caddfc5f85663e059406c78))

### Features

- Re-use connection to Cube Store from externalDriver ([#5993](https://github.com/cube-js/cube.js/issues/5993)) ([69a35ed](https://github.com/cube-js/cube.js/commit/69a35edb74e9b5e09f74b13a91afe7076fa344cb))

## [0.31.38](https://github.com/cube-js/cube.js/compare/v0.31.37...v0.31.38) (2023-01-11)

### Bug Fixes

- **athena-driver:** Add catalog config ([#5835](https://github.com/cube-js/cube.js/issues/5835)) Thanks [@tkislan](https://github.com/tkislan) ! ([c33a5c5](https://github.com/cube-js/cube.js/commit/c33a5c596622c5a2b67987da6cfd3f8bef6acebe))
- **athena-driver:** TypeError: table.join is not a function ([#5988](https://github.com/cube-js/cube.js/issues/5988)) ([4e56a04](https://github.com/cube-js/cube.js/commit/4e56a0402dbb13d757e073fb38a547522c0936d1)), closes [#5143](https://github.com/cube-js/cube.js/issues/5143)
- Delete `CUBEJS_DROP_PRE_AGG_WITHOUT_TOUCH` pre-aggregations only after refresh end to avoid cold start removals ([#5982](https://github.com/cube-js/cube.js/issues/5982)) ([58ad02f](https://github.com/cube-js/cube.js/commit/58ad02f24c5cc5940d6c668a0586fd66ff843795))

### Features

- **cubesql:** Improve memory usage in writting for pg-wire ([#4870](https://github.com/cube-js/cube.js/issues/4870)) ([401fbcf](https://github.com/cube-js/cube.js/commit/401fbcfa1e11a36d65555f7848280f5e60801808))
- **docker:** Upgrade Node.js to 14.21.1 ([#5970](https://github.com/cube-js/cube.js/issues/5970)) ([0394ed2](https://github.com/cube-js/cube.js/commit/0394ed2b38827e345a4a43ab35b129dfd845057b))

## [0.31.37](https://github.com/cube-js/cube.js/compare/v0.31.36...v0.31.37) (2023-01-09)

### Bug Fixes

- Remove missed `cacheFn` console.log ([ec9e56b](https://github.com/cube-js/cube.js/commit/ec9e56b79e5acf0399c634e3e50b3457175404b6))

### Features

- **cubejs-api-gateway:** added endpoint to run sql query directly ([#5786](https://github.com/cube-js/cube.js/issues/5786)) ([61d5798](https://github.com/cube-js/cube.js/commit/61d579834800c839defd3c2991aae65a42318027))
- **cubestore:** Support lazy initialization for CacheStore ([#5933](https://github.com/cube-js/cube.js/issues/5933)) ([37b4a95](https://github.com/cube-js/cube.js/commit/37b4a953e6c3bc535520203b23fef1632bf04aee))

## [0.31.36](https://github.com/cube-js/cube.js/compare/v0.31.35...v0.31.36) (2023-01-08)

### Bug Fixes

- graphql non capital cube name issue ([#5680](https://github.com/cube-js/cube.js/issues/5680)) Thanks @MattFanto! ([47956ea](https://github.com/cube-js/cube.js/commit/47956ea695443547a13c4f05eed382a750324f11)), closes [#5643](https://github.com/cube-js/cube.js/issues/5643)
- Potential OOM due to recursion while recovering Cube Store websocket ([8a0fd1f](https://github.com/cube-js/cube.js/commit/8a0fd1fe1901b80b369bd9fe5feff0da4f054ed5))

### Features

- **cubestore:** Set metastore current snapshot system command ([#5940](https://github.com/cube-js/cube.js/issues/5940)) ([f590a20](https://github.com/cube-js/cube.js/commit/f590a20a5ff3d81e500f54f03eeb31698fbf8a42))

## [0.31.35](https://github.com/cube-js/cube.js/compare/v0.31.34...v0.31.35) (2023-01-07)

### Bug Fixes

- **client-core:** Added type CubesMap for cubeMap in Meta ([#5897](https://github.com/cube-js/cube.js/issues/5897)) ([92d1ccb](https://github.com/cube-js/cube.js/commit/92d1ccb9166e3a608424a1e11457f1746119e5f2))
- **cubestore:** Get query execution results even after web socket disconnect ([#5931](https://github.com/cube-js/cube.js/issues/5931)) ([c6ccc1a](https://github.com/cube-js/cube.js/commit/c6ccc1a1c37d3f0c2fb5b4cdeacafdcad39321b3))
- **cubestore:** Maintain minimum count of metastore snapshots ([#5925](https://github.com/cube-js/cube.js/issues/5925)) ([b303aa6](https://github.com/cube-js/cube.js/commit/b303aa68b7040d22334673c436808564a505e097))
- Reduce memory footprint for pre-aggregations with many partitions by caching partition SQL ([5f72d8f](https://github.com/cube-js/cube.js/commit/5f72d8f99b588e579527ed3c8f550bf7949fab4e))

### Features

- **cubejs-cli:** add type-checking for `cube.js` files to newly-generated projects ([ba31d4f](https://github.com/cube-js/cube.js/commit/ba31d4fac969faa2a0bd35c15863595c66f36ea0))
- **cubesql:** Support `NULLIF` in projection ([129fc58](https://github.com/cube-js/cube.js/commit/129fc580579062be73d362cfa829e3af82f37ad0))
- **cubesql:** Support Thoughtspot starts/ends LIKE exprs ([e6798cc](https://github.com/cube-js/cube.js/commit/e6798cca8f9de33badf34b9cd64c41a2a7e6ce88))

## [0.31.34](https://github.com/cube-js/cube.js/compare/v0.31.33...v0.31.34) (2023-01-05)

### Bug Fixes

- **client-core:** move @babel/runtime to dependencies ([#5917](https://github.com/cube-js/cube.js/issues/5917)) ([67221af](https://github.com/cube-js/cube.js/commit/67221afa040bb71381369306b3cc9b5067094589))
- Reuse queries cache in refresh worker pre-aggregation iterator to reduce memory usage in high concurrency environment ([4ae21fa](https://github.com/cube-js/cube.js/commit/4ae21fa54d08b25420694caec5ffd6658c8e96f6))

## [0.31.33](https://github.com/cube-js/cube.js/compare/v0.31.32...v0.31.33) (2023-01-03)

### Bug Fixes

- **@cubejs-client/core:** add missing Series.shortTitle typing ([#5860](https://github.com/cube-js/cube.js/issues/5860)) ([5dd78b9](https://github.com/cube-js/cube.js/commit/5dd78b9544965304757c72d8f305741f78bfc935))

### Features

- **cubesql:** Support Node.js 18 ([#5900](https://github.com/cube-js/cube.js/issues/5900)) ([839fa75](https://github.com/cube-js/cube.js/commit/839fa752229d23c25f97df66b845fe6d8ebfd341))
- Introduce CubeStoreCacheDriver ([#5511](https://github.com/cube-js/cube.js/issues/5511)) ([fb39776](https://github.com/cube-js/cube.js/commit/fb3977631adb126183d5d2775c70821c6c099897))

## [0.31.32](https://github.com/cube-js/cube.js/compare/v0.31.31...v0.31.32) (2022-12-28)

### Features

- **cubesql:** Allow postprocessing with JOIN below Cube query limit ([56f5399](https://github.com/cube-js/cube.js/commit/56f5399fb37dbb7b388951b1db0be21dda2a94d4))
- **cubesql:** Support `LEFT`, `RIGHT` in projection ([282ad3a](https://github.com/cube-js/cube.js/commit/282ad3ab3106d81a1361f94a499cfc7dc716f3e6))
- **cubestore:** Direct kafka download support for ksql streams and t… ([#5880](https://github.com/cube-js/cube.js/issues/5880)) ([fcb5c5e](https://github.com/cube-js/cube.js/commit/fcb5c5e7825df32249a8021580b582821761c7b1))
- **cubestore:** Installer - download binary without GH API ([#5885](https://github.com/cube-js/cube.js/issues/5885)) ([51a0f39](https://github.com/cube-js/cube.js/commit/51a0f3951caf62fdf8e88d80faf16d11a2b9158b))

## [0.31.31](https://github.com/cube-js/cube.js/compare/v0.31.30...v0.31.31) (2022-12-23)

### Bug Fixes

- **cubesql:** Improve Thoughtspot `WHERE IN` support ([6212efe](https://github.com/cube-js/cube.js/commit/6212efe428e504cd8c06797dfaa2b81783b80777))
- **cubesql:** Support Thoughtspot DATEADD queries ([58b5669](https://github.com/cube-js/cube.js/commit/58b566903685ac3d14e78d61dc38c38c43aa5c3c))

### Features

- **cubestore-driver:** Introduce `CUBEJS_CUBESTORE_NO_HEART_BEAT_TIMEOUT` env ([e67e800](https://github.com/cube-js/cube.js/commit/e67e800119e4efe0456eef347a573fa9feaf10e7))
- **cubestore:** Initial support for KV cache ([#5861](https://github.com/cube-js/cube.js/issues/5861)) ([498d4b5](https://github.com/cube-js/cube.js/commit/498d4b5c5dae2ddf1ff1336566580cff1a656a92))
- **playground:** allow for query renaming ([#5862](https://github.com/cube-js/cube.js/issues/5862)) ([681a2d8](https://github.com/cube-js/cube.js/commit/681a2d804880b17254e37a132008086c7c3fe81f))

## [0.31.30](https://github.com/cube-js/cube.js/compare/v0.31.29...v0.31.30) (2022-12-22)

### Bug Fixes

- **cubesql:** Improve Thoughtspot compatibility ([4d6511a](https://github.com/cube-js/cube.js/commit/4d6511a3d18ea877f06775c1aae154b5665feda0))
- **cubestore-driver:** Increase retry timeout to avoid endless loop connection retries ([8bd19fa](https://github.com/cube-js/cube.js/commit/8bd19fafb36da6d97000da20417e98f23d92a424))
- Pre-aggregation table is not found after it was successfully created for `CUBEJS_DROP_PRE_AGG_WITHOUT_TOUCH` strategy ([#5858](https://github.com/cube-js/cube.js/issues/5858)) ([f602fee](https://github.com/cube-js/cube.js/commit/f602feea9c2a25a0de8604e1297484f71ac8437a))

### Features

- **@cubejs-client/core:** expose shortTitle in seriesNames ([#5836](https://github.com/cube-js/cube.js/issues/5836)) Thanks [@euljr](https://github.com/euljr) ! ([1058d5a](https://github.com/cube-js/cube.js/commit/1058d5afd2784302b23215d73c679d35ceb785a8))

## [0.31.29](https://github.com/cube-js/cube.js/compare/v0.31.28...v0.31.29) (2022-12-18)

### Bug Fixes

- Error: WebSocket is not open: readyState 2 (CLOSING) ([#5846](https://github.com/cube-js/cube.js/issues/5846)) ([a5be099](https://github.com/cube-js/cube.js/commit/a5be099f1d339ceb17c89439d6195c5718c726bb))

## [0.31.28](https://github.com/cube-js/cube.js/compare/v0.31.27...v0.31.28) (2022-12-16)

### Features

- Support `string`, `time` and `boolean` measures ([#5842](https://github.com/cube-js/cube.js/issues/5842)) ([4543ede](https://github.com/cube-js/cube.js/commit/4543edefe5b2432c90bb8530bc6a3c24c5548de3))

## [0.31.27](https://github.com/cube-js/cube.js/compare/v0.31.26...v0.31.27) (2022-12-16)

### Bug Fixes

- **api-gateway:** pre-aggregations/jobs auth middleware ([#5840](https://github.com/cube-js/cube.js/issues/5840)) ([b527cd9](https://github.com/cube-js/cube.js/commit/b527cd9fab348b404b6ed83fbbdb963c23928b07))

## [0.31.26](https://github.com/cube-js/cube.js/compare/v0.31.25...v0.31.26) (2022-12-13)

### Bug Fixes

- **cubestore:** Temporary disable compression to check if it fixes running on Mac OS X Ventura ([8e89427](https://github.com/cube-js/cube.js/commit/8e8942776f28a4a35a5dc3ad22360691da43682f)), closes [#5712](https://github.com/cube-js/cube.js/issues/5712)
- **trino-driver:** Timezone issue fix ([#5731](https://github.com/cube-js/cube.js/issues/5731)) Thanks [@yuraborue](https://github.com/yuraborue) ! ([70df903](https://github.com/cube-js/cube.js/commit/70df9034f4246be06956cbf1dc69de709d4d3df8))

### Features

- **cubestore:** Introduce pre-aggregation table touch and allow to drop tables without touch using `CUBEJS_DROP_PRE_AGG_WITHOUT_TOUCH` env ([#5794](https://github.com/cube-js/cube.js/issues/5794)) ([ad6c1e8](https://github.com/cube-js/cube.js/commit/ad6c1e8d09c228c28bb957755339a1146f54d6c9))
- persistent queue ([#5793](https://github.com/cube-js/cube.js/issues/5793)) ([02a8e02](https://github.com/cube-js/cube.js/commit/02a8e027c1f414c50cb49f257ce68c01425400ec))

## [0.31.25](https://github.com/cube-js/cube.js/compare/v0.31.24...v0.31.25) (2022-12-10)

### Bug Fixes

- **cubesql:** normalize column names in filter node ([#5788](https://github.com/cube-js/cube.js/issues/5788)) ([28aa008](https://github.com/cube-js/cube.js/commit/28aa008d8060173b2af2052577afdc26cc32c36d))

## [0.31.24](https://github.com/cube-js/cube.js/compare/v0.31.23...v0.31.24) (2022-12-09)

### Bug Fixes

- **cubesql:** Support `CAST` in `HAVING` clause ([17ba3e2](https://github.com/cube-js/cube.js/commit/17ba3e212fb801fbffec99ef043b443f6f2a698f))

### Features

- **api-gateway, server-core:** Added dataSources list to extended meta ([#5743](https://github.com/cube-js/cube.js/issues/5743)) ([2c5db32](https://github.com/cube-js/cube.js/commit/2c5db32f2ded074ebe5e83668eee8c024101240b))

### Reverts

- Revert "feat(api-gateway, server-core): Added dataSources list to extended meta (#5743)" (#5785) ([3c61467](https://github.com/cube-js/cube.js/commit/3c614674fed6ca17df08bbba8c835ef110167570)), closes [#5743](https://github.com/cube-js/cube.js/issues/5743) [#5785](https://github.com/cube-js/cube.js/issues/5785)
- Revert "chore(cubejs-api-gateway): added endpoint to run sql query directly (#5723)" (#5784) ([f1140de](https://github.com/cube-js/cube.js/commit/f1140de508e359970ac82b50bae1c4bf152f6041)), closes [#5723](https://github.com/cube-js/cube.js/issues/5723) [#5784](https://github.com/cube-js/cube.js/issues/5784)

## [0.31.23](https://github.com/cube-js/cube.js/compare/v0.31.22...v0.31.23) (2022-12-09)

### Bug Fixes

- **jdbc-driver:** set default encoding ([#5773](https://github.com/cube-js/cube.js/issues/5773)) ([734167a](https://github.com/cube-js/cube.js/commit/734167a4fd555d15f722945fe5d2957663a144df))

### Features

- query limits ([#5763](https://github.com/cube-js/cube.js/issues/5763)) ([4ec172b](https://github.com/cube-js/cube.js/commit/4ec172b3dd27193d145afcdb8d9bf7ef1bd7505d))
- **cubesql:** Support `CASE` statements in cube projection ([e7ae68c](https://github.com/cube-js/cube.js/commit/e7ae68c1afcb1152c0248f61ee355f0b30cc9b73))
- introducing query persistent flag ([#5744](https://github.com/cube-js/cube.js/issues/5744)) ([699e772](https://github.com/cube-js/cube.js/commit/699e772be0e2e1b6eef4e59ff8e3857d1166bcef))

## [0.31.22](https://github.com/cube-js/cube.js/compare/v0.31.21...v0.31.22) (2022-12-07)

### Bug Fixes

- **cubesql:** Metabase - auto-generated charts for cubes containing string dimensions ([#5728](https://github.com/cube-js/cube.js/issues/5728)) ([72be686](https://github.com/cube-js/cube.js/commit/72be68671faaa4c938374f95cb8cb81578ef4fdb))
- **cubestore:** Added long running job fetches burst network ([385c4a6](https://github.com/cube-js/cube.js/commit/385c4a6f92e2da209804276d85f1dec1d404ed8e))
- **cubestore:** Increase default stale stream timeout to allow replays to catch up with large kafka streams ([43cf0b1](https://github.com/cube-js/cube.js/commit/43cf0b1c700c5fc15ed907c694916569e5647bf3))
- **cubestore:** Row with id 1 is not found for SchemaRocksTable -- fix log replay order during metastore loading from dump ([b74c072](https://github.com/cube-js/cube.js/commit/b74c0721fb8d62d9e52570a3d5b883604b00648c))
- **playground:** reload the page in case chunks got stale ([#5646](https://github.com/cube-js/cube.js/issues/5646)) ([2215595](https://github.com/cube-js/cube.js/commit/2215595369ac0ceab89c95760ecea6e9fd9cc1af))
- return failed query request ids ([#5729](https://github.com/cube-js/cube.js/issues/5729)) ([22cd580](https://github.com/cube-js/cube.js/commit/22cd580c412a64ce92dd1410f13af19709c27b9d))

### Features

- **playground:** yaml rollup support ([#5727](https://github.com/cube-js/cube.js/issues/5727)) ([7a267f9](https://github.com/cube-js/cube.js/commit/7a267f99def74fe56f1505c7d56d42ea0c24f842))

## [0.31.21](https://github.com/cube-js/cube.js/compare/v0.31.20...v0.31.21) (2022-12-06)

### Bug Fixes

- **cubestore:** `Error in processing loop: Row with id is not found for JobRocksTable` for streaming jobs ([f2de503](https://github.com/cube-js/cube.js/commit/f2de503365c4dece0b01a2f8817b231ab0fced4e))
- **cubestore:** Avoid streaming jobs waiting for regular jobs to complete ([#5725](https://github.com/cube-js/cube.js/issues/5725)) ([89d5bf4](https://github.com/cube-js/cube.js/commit/89d5bf4eda6d2b8ba41febe121322aba5605d107))
- **cubestore:** Chunk all streaming input into bigger chunks based on time spent in write chunk operations and remove sequence gap checks as those fail for tables ([1f2d9bf](https://github.com/cube-js/cube.js/commit/1f2d9bf6f1df8e8d961300d4c632240bc5290eb3))
- **cubestore:** Replay streams with `earliest` offset even for `latest` setting after failure ([7ca71f5](https://github.com/cube-js/cube.js/commit/7ca71f5969f9550151afcd646864a4d5eaeca3ab))
- **cubestore:** ReplayHandle reconcile fails due to merge ReplayHandles with Chunks ([#5713](https://github.com/cube-js/cube.js/issues/5713)) ([ac213a7](https://github.com/cube-js/cube.js/commit/ac213a7bb648fbe4a6f645227640d7085a0b578e))
- **cubestore:** Streaming jobs stuck as stale ([1cf3432](https://github.com/cube-js/cube.js/commit/1cf3432c23a20ec682a40fd0f86192b98b0a6fc0))
- **databricks-jdbc:** Databricks pre-aggregations schema with UC ([#5726](https://github.com/cube-js/cube.js/issues/5726)) ([6281471](https://github.com/cube-js/cube.js/commit/6281471ad981372a938449114cd51e5cf1326884))
- **playground:** clear search value ([#5720](https://github.com/cube-js/cube.js/issues/5720)) ([5b177f2](https://github.com/cube-js/cube.js/commit/5b177f2531789928a75f1e0c47c8f25bdd7c0251))
- More explanatory `No pre-aggregation partitions were built yet` message ([#5702](https://github.com/cube-js/cube.js/issues/5702)) ([ec39baa](https://github.com/cube-js/cube.js/commit/ec39baa80b9980e4b72be31c53f6704884b8ac5c))

## [0.31.20](https://github.com/cube-js/cube.js/compare/v0.31.19...v0.31.20) (2022-12-02)

### Bug Fixes

- **clickhouse-driver:** Make ClickHouse driver `readOnly` by default ([e6a85d1](https://github.com/cube-js/cube.js/commit/e6a85d1856ee0a7d6b2b4f3e59f6e8edaed609a2)), closes [#5479](https://github.com/cube-js/cube.js/issues/5479)
- **clickhouse-driver:** ParserError("Expected ',' or ')' after column definition, found: (") when Nullable(Float64) is present ([81b2247](https://github.com/cube-js/cube.js/commit/81b224747f1423f263f384546f182b3d47f22a3d))
- **cubesql:** Fix escape symbols in `LIKE` expressions ([5f3cd50](https://github.com/cube-js/cube.js/commit/5f3cd50ea311900adc27ba2b30c72a05a3453a1d))
- **cubestore:** Orphaned replay handles after table drop ([0e4b876](https://github.com/cube-js/cube.js/commit/0e4b876556d8d88e6fb3e4270d6a7852acb0fd00))
- **cubestore:** Sort by seq column to reduce Unexpected sequence increase gap ([b5f06d0](https://github.com/cube-js/cube.js/commit/b5f06d0c10217f6e193e6729adba30e2d9af2b92))
- **oracle-driver:** Make oracle driver `readOnly` by default so pre-aggregations can be used ([5efccba](https://github.com/cube-js/cube.js/commit/5efccbaa8e15d4c85f59f0465bca62a919ace78b))

### Features

- **cubesql:** Support Thoughtspot include filter search ([745fe5d](https://github.com/cube-js/cube.js/commit/745fe5d2806b4c6c9e76d6061aa038892ec7438f))
- **cubesql:** Support ThoughtSpot search filters ([ee0fde4](https://github.com/cube-js/cube.js/commit/ee0fde4798894c619f63cfd87cfc118c7ff1fc78))

## [0.31.19](https://github.com/cube-js/cube.js/compare/v0.31.18...v0.31.19) (2022-11-29)

### Bug Fixes

- Incorrect filter pushdown when filtering two left-joined tables ([#5685](https://github.com/cube-js/cube.js/issues/5685)) ([c775869](https://github.com/cube-js/cube.js/commit/c77586961078ec67395af39e3f233025833d4f6e)), closes [#3777](https://github.com/cube-js/cube.js/issues/3777)
- packages/cubejs-client-core/package.json to reduce vulnerabilities ([#5352](https://github.com/cube-js/cube.js/issues/5352)) ([76e8e43](https://github.com/cube-js/cube.js/commit/76e8e43c05d857ce4338d0d184a987e4edae735c))
- packages/cubejs-client-vue/package.json to reduce vulnerabilities ([#5361](https://github.com/cube-js/cube.js/issues/5361)) ([effc694](https://github.com/cube-js/cube.js/commit/effc694b29da181899ff74f7f0107eeaa9aaec76))
- packages/cubejs-query-orchestrator/package.json to reduce vulnerabilities ([#5347](https://github.com/cube-js/cube.js/issues/5347)) ([3442ddf](https://github.com/cube-js/cube.js/commit/3442ddf319caeae8c5506f4cec0fde1ebcf9d147))
- packages/cubejs-schema-compiler/package.json to reduce vulnerabilities ([#5359](https://github.com/cube-js/cube.js/issues/5359)) ([cd81554](https://github.com/cube-js/cube.js/commit/cd81554079533eae2c35ce142b26ad862b6e250d))
- packages/cubejs-schema-compiler/package.json to reduce vulnerabilities ([#5408](https://github.com/cube-js/cube.js/issues/5408)) ([6b7a95f](https://github.com/cube-js/cube.js/commit/6b7a95f96dacb42a0a50fe77afcc11216f10c29a))
- packages/cubejs-templates/package.json to reduce vulnerabilities ([#5342](https://github.com/cube-js/cube.js/issues/5342)) ([825605a](https://github.com/cube-js/cube.js/commit/825605aa8d85dca29b6d3d7a230e361c1374746b))

### Features

- catalog support for the Databricks driver ([#5666](https://github.com/cube-js/cube.js/issues/5666)) ([de5ba9a](https://github.com/cube-js/cube.js/commit/de5ba9a247543b432ea82b4371ddb052f1c91227))

## [0.31.18](https://github.com/cube-js/cube.js/compare/v0.31.17...v0.31.18) (2022-11-28)

### Bug Fixes

- **cubesql:** Prevent infinite limit push down ([f26d40a](https://github.com/cube-js/cube.js/commit/f26d40a3b91a2651811cfa622424c6ed2e44dfef))
- **cubesql:** Push down projection to CubeScan with literals ([207616d](https://github.com/cube-js/cube.js/commit/207616d8e51649a78b854b02fc83611472aea715))

### Features

- **cubesql:** Sigma Computing date filters ([404e3f4](https://github.com/cube-js/cube.js/commit/404e3f42ac02be8c0c9eaacb6cf81bb516616001))
- yaml schema generation ([#5676](https://github.com/cube-js/cube.js/issues/5676)) ([fc0b810](https://github.com/cube-js/cube.js/commit/fc0b81016e5c66156d06f6834187d3323e9e8ca8))

## [0.31.17](https://github.com/cube-js/cube.js/compare/v0.31.16...v0.31.17) (2022-11-23)

### Features

- **cubestore:** Introduce CacheStore ([#5607](https://github.com/cube-js/cube.js/issues/5607)) ([36dee61](https://github.com/cube-js/cube.js/commit/36dee61de05b544edb2b413afa4b9582f81a5eec))

## [0.31.16](https://github.com/cube-js/cube.js/compare/v0.31.15...v0.31.16) (2022-11-23)

### Features

- **ksql-driver:** Support offset earliest, replays and per partition streaming ([#5663](https://github.com/cube-js/cube.js/issues/5663)) ([3a79d02](https://github.com/cube-js/cube.js/commit/3a79d02c9794b5e437a52f9f0be72eb82a92805a))

## [0.31.15](https://github.com/cube-js/cube.js/compare/v0.31.14...v0.31.15) (2022-11-17)

### Bug Fixes

- **@cubejs-backend/snowflake-driver:** Make `CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE=true` by default ([be12c40](https://github.com/cube-js/cube.js/commit/be12c40ca7acda11409774f5aa407741fdfde871))
- `extends` YAML support ([982885e](https://github.com/cube-js/cube.js/commit/982885e8e41b351e27919551688f50f7e5af3a5a))
- **client-react:** check meta changes ([4c44551](https://github.com/cube-js/cube.js/commit/4c44551b880bd4ff34d443241c1c0c28cae0d5f8))
- **druid-driver:** Respect day light saving ([#5613](https://github.com/cube-js/cube.js/issues/5613)) ([388c992](https://github.com/cube-js/cube.js/commit/388c992ca7a2d3730249fafb2a53b3accff21451))
- packages/cubejs-client-core/package.json to reduce vulnerabilities ([#5415](https://github.com/cube-js/cube.js/issues/5415)) ([fb2de68](https://github.com/cube-js/cube.js/commit/fb2de682670bd28b2879d0460fac990d9b653dce))
- packages/cubejs-client-react/package.json to reduce vulnerabilities ([#5390](https://github.com/cube-js/cube.js/issues/5390)) ([0ab9c30](https://github.com/cube-js/cube.js/commit/0ab9c30692c70d3776a8429197915090fef61d4f))
- packages/cubejs-databricks-jdbc-driver/package.json to reduce vulnerabilities ([#5413](https://github.com/cube-js/cube.js/issues/5413)) ([6a891f0](https://github.com/cube-js/cube.js/commit/6a891f0bc34dcaa8c955cd0ac20121d4d074e228))
- packages/cubejs-databricks-jdbc-driver/package.json to reduce vulnerabilities ([#5429](https://github.com/cube-js/cube.js/issues/5429)) ([a45c9a8](https://github.com/cube-js/cube.js/commit/a45c9a828b38d13da9a4194fdfc23e11674aa7cd))
- packages/cubejs-query-orchestrator/package.json to reduce vulnerabilities ([#5409](https://github.com/cube-js/cube.js/issues/5409)) ([5e9fe68](https://github.com/cube-js/cube.js/commit/5e9fe68b40164ca12e9cecb0aefda31c06b57f28))
- packages/cubejs-templates/package.json to reduce vulnerabilities ([#5403](https://github.com/cube-js/cube.js/issues/5403)) ([c9706cb](https://github.com/cube-js/cube.js/commit/c9706cbfcd6480dbd58ca18ab064ded185f710d3))
- **server-core:** Force flush events if their count is greater than the agent frame size ([#5602](https://github.com/cube-js/cube.js/issues/5602)) ([17d1d98](https://github.com/cube-js/cube.js/commit/17d1d989bd03459adae1e6c6714843fc82d99163))

### Features

- **databricks-jdbc:** jdbc (jar) driver update ([#5610](https://github.com/cube-js/cube.js/issues/5610)) ([aacd8cd](https://github.com/cube-js/cube.js/commit/aacd8cd356429e4da21749b92eb457c03a1a3f76))
- **databricks-jdbc:** jdbc (jar) driver update ([#5612](https://github.com/cube-js/cube.js/issues/5612)) ([372ed71](https://github.com/cube-js/cube.js/commit/372ed71c6edd61d862d62cb0522fbc47c0f997b2))
- **docs:** add new component to display code snippets side-by-side ([146328c](https://github.com/cube-js/cube.js/commit/146328c7e7eab41c7cfe4e67d26af58eeca9c09c))
- Replace YAML parser to provide more meaningful parse errors ([9984066](https://github.com/cube-js/cube.js/commit/99840665ee31aa8f14cf9c83b19d4a4cbc3a978a))
- Support snake case in YAML relationship field ([f20fe6b](https://github.com/cube-js/cube.js/commit/f20fe6baa43142589a5a85be5af1daefb56acd8b))

### Reverts

- Revert "feat(databricks-jdbc): jdbc (jar) driver update (#5610)" (#5611) ([23ed416](https://github.com/cube-js/cube.js/commit/23ed416d9540afc4fe027f7e3c6917af387f06f7)), closes [#5610](https://github.com/cube-js/cube.js/issues/5610) [#5611](https://github.com/cube-js/cube.js/issues/5611)

## [0.31.14](https://github.com/cube-js/cube.js/compare/v0.31.13...v0.31.14) (2022-11-14)

### Bug Fixes

- **cubesql:** Allow referencing CTEs in UNION ([7f5bc83](https://github.com/cube-js/cube.js/commit/7f5bc8316d6119ffe1703bc1bb41e54586f9d19b))
- **cubesql:** Keep CubeScan literal values relation ([6d3856a](https://github.com/cube-js/cube.js/commit/6d3856acdfeea8d93866a1f36513016a5a04b2e8))
- **playground:** schemaVersion updates ([5c4880f](https://github.com/cube-js/cube.js/commit/5c4880f1fa30189798c0b0ea42df67c7fd0aea75))

### Features

- **@cubejs-backend/mssql-driver:** Make MSSQL `readOnly` by default ([#5584](https://github.com/cube-js/cube.js/issues/5584)) ([ddf0369](https://github.com/cube-js/cube.js/commit/ddf036992aebc61fdd99d2a67753c63528bba9db))
- **cubesql:** Join Cubes ([#5585](https://github.com/cube-js/cube.js/issues/5585)) ([c687e42](https://github.com/cube-js/cube.js/commit/c687e42f9280f611152f7c154fdf136e6d9ce402))
- **playground:** ability to rerun queries ([#5597](https://github.com/cube-js/cube.js/issues/5597)) ([6ef8ce9](https://github.com/cube-js/cube.js/commit/6ef8ce91740086dc0b10ea11d7133f8be1e2ef0a))

## [0.31.13](https://github.com/cube-js/cube.js/compare/v0.31.12...v0.31.13) (2022-11-08)

### Bug Fixes

- Make Trino driver CommonJS compatible ([#5581](https://github.com/cube-js/cube.js/issues/5581)) ([ca8fd4e](https://github.com/cube-js/cube.js/commit/ca8fd4e42f4d8f87667507920f02cbb1a7072763))
- **cubestore:** Merge for streaming union ([#5554](https://github.com/cube-js/cube.js/issues/5554)) ([310649a](https://github.com/cube-js/cube.js/commit/310649af9a280484481dc09063178c07f3f36131))

### Features

- export bucket CVS files escape symbol support ([#5570](https://github.com/cube-js/cube.js/issues/5570)) ([09ceffb](https://github.com/cube-js/cube.js/commit/09ceffbefc75417555f8ff90f6277bd9c419d751))
- notStartsWith/notEndsWith filters support ([#5579](https://github.com/cube-js/cube.js/issues/5579)) ([8765833](https://github.com/cube-js/cube.js/commit/87658333df0194db07c3ce0ae6f94a292f8bd592))
- YAML snake case and `.yaml` extension support ([#5578](https://github.com/cube-js/cube.js/issues/5578)) ([c8af286](https://github.com/cube-js/cube.js/commit/c8af2864af2dcc532abdc3629cf58893d874c190))

## [0.31.12](https://github.com/cube-js/cube.js/compare/v0.31.11...v0.31.12) (2022-11-05)

### Bug Fixes

- Cannot read property 'apply' of undefined on missed dimension sql ([#5559](https://github.com/cube-js/cube.js/issues/5559)) ([6f85096](https://github.com/cube-js/cube.js/commit/6f850967ba834bbdd87284fa957859d66d19344a))
- YAML filter support ([fb6fade](https://github.com/cube-js/cube.js/commit/fb6fade4bfa5b0fb2e3b6895e378aef97ea26f95))
- **cubestore:** Fix partition pruning ([#5548](https://github.com/cube-js/cube.js/issues/5548)) ([8bc4aee](https://github.com/cube-js/cube.js/commit/8bc4aeeff5502d2e17763bf503ebe396227b48ae))
- No column found in case non equals filter query incorrectly matched against rollup with no dimensions ([#5552](https://github.com/cube-js/cube.js/issues/5552)) ([73b3203](https://github.com/cube-js/cube.js/commit/73b3203925bf9d8221001f730bb23272dd4e47e6))
- TypeError: Cannot read property 'dimension' of undefined for rolling window rollup without time dimension ([#5553](https://github.com/cube-js/cube.js/issues/5553)) ([03c3b6f](https://github.com/cube-js/cube.js/commit/03c3b6f4197ff8e6a77fc6bb7c08e4730cbfde66))

### Features

- Trino driver ([e58c392](https://github.com/cube-js/cube.js/commit/e58c3924781b65f5631ee241b39a0bee1366273d))
- **cubesql:** Support Skyvia date granularities ([df69d93](https://github.com/cube-js/cube.js/commit/df69d93e3f0c016d4767e0509ca523b60bc74099))

## [0.31.11](https://github.com/cube-js/cube.js/compare/v0.31.10...v0.31.11) (2022-11-02)

### Bug Fixes

- **@cubejs-backend/prestodb-driver:** Replace double escaping in contain filter ([#5529](https://github.com/cube-js/cube.js/issues/5529)) ([7870705](https://github.com/cube-js/cube.js/commit/7870705b04697faf8cb994c0794bc86437a9e3cf)), closes [#5528](https://github.com/cube-js/cube.js/issues/5528)
- **cubestore:** Fix streaming index ([#5550](https://github.com/cube-js/cube.js/issues/5550)) ([061305e](https://github.com/cube-js/cube.js/commit/061305e7a7549ae2d79e5391afd79f3d2af7d628))

### Features

- **cubestore:** Sealing partition ([#5523](https://github.com/cube-js/cube.js/issues/5523)) ([70ee72c](https://github.com/cube-js/cube.js/commit/70ee72cca5b9a77bf14994f05b0e77148089362c))

## [0.31.10](https://github.com/cube-js/cube.js/compare/v0.31.9...v0.31.10) (2022-11-01)

### Bug Fixes

- Revert back strict shown checks behavior for consistency reasons ([#5551](https://github.com/cube-js/cube.js/issues/5551)) ([c3ee4e6](https://github.com/cube-js/cube.js/commit/c3ee4e6911d07f7d5cb8977f563555157f7b5f2b)), closes [#5542](https://github.com/cube-js/cube.js/issues/5542)

## [0.31.9](https://github.com/cube-js/cube.js/compare/v0.31.8...v0.31.9) (2022-11-01)

### Bug Fixes

- **@cubejs-client/core:** `startsWith` and `endsWith` to filterOperatorsForMember ([#5544](https://github.com/cube-js/cube.js/issues/5544)) ([583de4a](https://github.com/cube-js/cube.js/commit/583de4a58c841542f3138c5ce836dbfedd19d4de))
- You requested hidden member in case of rolling window measure is being used in a view ([#5546](https://github.com/cube-js/cube.js/issues/5546)) ([c00ea43](https://github.com/cube-js/cube.js/commit/c00ea432021a02638361b278f8eb24801a7a867b))

## [0.31.8](https://github.com/cube-js/cube.js/compare/v0.31.7...v0.31.8) (2022-10-30)

### Bug Fixes

- **cubesql:** Count measure type changed from u64 to i64 ([#5535](https://github.com/cube-js/cube.js/issues/5535)) ([f568851](https://github.com/cube-js/cube.js/commit/f568851948cff16ddc53a974d46a77a8698dbdf1))

### Features

- YAML support ([#5539](https://github.com/cube-js/cube.js/issues/5539)) ([29c19db](https://github.com/cube-js/cube.js/commit/29c19db9315ef7ad40350150f74f9519d6ff4a98))
- **cubesql:** Support `BOOL_AND`, `BOOL_OR` aggregate functions ([#5533](https://github.com/cube-js/cube.js/issues/5533)) ([a2e6e38](https://github.com/cube-js/cube.js/commit/a2e6e386557bfbf43b2a4907c1fa3aef07ea90f2))
- **cubesql:** Support Sigma Computing number filters ([f2f2abd](https://github.com/cube-js/cube.js/commit/f2f2abdbdafdd4669e1bd223b5b2f50a24c42b86))
- **cubesql:** Thoughspot - count distinct with year and month ([#5450](https://github.com/cube-js/cube.js/issues/5450)) ([d44baad](https://github.com/cube-js/cube.js/commit/d44baad34dab8dbf70aa7c9b011dfe17f93b1375))

## [0.31.7](https://github.com/cube-js/cube.js/compare/v0.31.6...v0.31.7) (2022-10-27)

### Bug Fixes

- Remove empty hidden cubes from meta ([#5531](https://github.com/cube-js/cube.js/issues/5531)) ([f93e851](https://github.com/cube-js/cube.js/commit/f93e8515598fd90708606220dd5d96b4e2396fd6))

### Features

- **cubesql:** Support `is null`, `is not null` in SELECT ([d499c47](https://github.com/cube-js/cube.js/commit/d499c47c6cec4ac7b72d26418598bfbe515c6c62))
- **cubesql:** Support Sigma Computing string filters ([d18b971](https://github.com/cube-js/cube.js/commit/d18b9712c4eae1ced2b8f94681e1676d854e99a5))

## [0.31.6](https://github.com/cube-js/cube.js/compare/v0.31.5...v0.31.6) (2022-10-20)

### Bug Fixes

- **docker:** Correct lock for deps with yarn.lock ([#5509](https://github.com/cube-js/cube.js/issues/5509)) ([97ca524](https://github.com/cube-js/cube.js/commit/97ca524556178a1210c796ee5bac42d476c7c3df))

### Features

- **snowflake-driver:** Upgrade snowflake-sdk ([#5508](https://github.com/cube-js/cube.js/issues/5508)) ([2863a93](https://github.com/cube-js/cube.js/commit/2863a93508c8ddf7a4a78a45386b79c2ae5f5ed6))

## [0.31.5](https://github.com/cube-js/cube.js/compare/v0.31.4...v0.31.5) (2022-10-20)

### Bug Fixes

- **cubesql:** Use real database name in Postgres meta layer ([031a90f](https://github.com/cube-js/cube.js/commit/031a90fac068ce35f18823a034f65b486153c726))

### Features

- **cubesql:** Allow `EXTRACT` field to be parsed as a string ([cad1e0b](https://github.com/cube-js/cube.js/commit/cad1e0b4452233866f87d3856823ef81ca117444))
- **cubestore:** CC-1133 - add headers to telemetry request ([#5473](https://github.com/cube-js/cube.js/issues/5473)) ([26fa817](https://github.com/cube-js/cube.js/commit/26fa8173961adfc756cee0a9bb626219f33c35b2))
- pre-aggregations build jobs API endpoint ([#5251](https://github.com/cube-js/cube.js/issues/5251)) ([4aee3ef](https://github.com/cube-js/cube.js/commit/4aee3efba9c14b4698c81707d92af11c98978c81))

## [0.31.4](https://github.com/cube-js/cube.js/compare/v0.31.3...v0.31.4) (2022-10-13)

### Bug Fixes

- **cubestore:** Extension planner for LogicalAlias error ([#5459](https://github.com/cube-js/cube.js/issues/5459)) ([8153459](https://github.com/cube-js/cube.js/commit/8153459c70ddf9acef7e886a6e44971c1aa82660))
- **druid-driver:** query timezone fix ([#5372](https://github.com/cube-js/cube.js/issues/5372)) ([ccad5fc](https://github.com/cube-js/cube.js/commit/ccad5fc58e23c55283c8e3014908cb51fa5b5fbc))

### Features

- `shown` flag for cubes and views ([#5455](https://github.com/cube-js/cube.js/issues/5455)) ([e7ef446](https://github.com/cube-js/cube.js/commit/e7ef4467adfc581e07c14b315a43c0eb4d1e8c11))

## [0.31.3](https://github.com/cube-js/cube.js/compare/v0.31.2...v0.31.3) (2022-10-08)

### Bug Fixes

- drivers imports alignment ([#5448](https://github.com/cube-js/cube.js/issues/5448)) ([ab12426](https://github.com/cube-js/cube.js/commit/ab1242650ba0368b855176b9c6ca2d73073acf0e))

## [0.31.2](https://github.com/cube-js/cube.js/compare/v0.31.1...v0.31.2) (2022-10-08)

### Bug Fixes

- Added connection test completed successfully log ([#5444](https://github.com/cube-js/cube.js/issues/5444)) ([a8e1180](https://github.com/cube-js/cube.js/commit/a8e1180d4c05e8de94b116f46f40db531d91e0eb))
- Temp tables dropped for ksql after successfully created ([#5445](https://github.com/cube-js/cube.js/issues/5445)) ([7726c82](https://github.com/cube-js/cube.js/commit/7726c82fc833a6b14eeeb498a31df62ba9818336))
- **cubesql:** Handle Panic on simple query executiony ([#5394](https://github.com/cube-js/cube.js/issues/5394)) ([84dc442](https://github.com/cube-js/cube.js/commit/84dc442eb1c42bc3c7b7b03fe365c7c0a948e328))

### Features

- **mssql-driver:** add column type mapping for a bit type ([#5442](https://github.com/cube-js/cube.js/issues/5442)) ([0ed7ba4](https://github.com/cube-js/cube.js/commit/0ed7ba4b483fff8653602f89f43bd1b03b458859))
- Includes and Excludes directives for Cube Views ([#5437](https://github.com/cube-js/cube.js/issues/5437)) ([7c35604](https://github.com/cube-js/cube.js/commit/7c356049b56a0ea58a4ad8f2628ffaff2ac307d4))
- **cubesql:** Support boolean decoding in pg-wire ([#5436](https://github.com/cube-js/cube.js/issues/5436)) ([4fd2ee6](https://github.com/cube-js/cube.js/commit/4fd2ee6cd238161f889896739a00f09e6dc11651))

## [0.31.1](https://github.com/cube-js/cube.js/compare/v0.31.0...v0.31.1) (2022-10-04)

### Bug Fixes

- Cube not found for path due to FILTER_PARAMS are used in members ([#5417](https://github.com/cube-js/cube.js/issues/5417)) ([bfe76bf](https://github.com/cube-js/cube.js/commit/bfe76bfbf3bd5f51a408088f8f5fefb35e17e22f))
- **extensions:** fix SELECT handling in the Funnels extensions ([#5397](https://github.com/cube-js/cube.js/issues/5397)) ([041f591](https://github.com/cube-js/cube.js/commit/041f591482cc75d840310a9b7592158c1b33fa37))
- **playground:** bar chart pivot config ([36b9ec2](https://github.com/cube-js/cube.js/commit/36b9ec2f3de5ba3b6b047f3669f8e162321d854e))

### Features

- **console-ui:** show error stack trace ([7ab15c6](https://github.com/cube-js/cube.js/commit/7ab15c6fbd46696d36c95f197711284d316c9f70))

# [0.31.0](https://github.com/cube-js/cube.js/compare/v0.30.75...v0.31.0) (2022-10-03)

- feat!: Cube Views implementation (#5278) ([9937356](https://github.com/cube-js/cube.js/commit/99373563b610d1f15dfc44fafac0c329c1dc9a0d)), closes [#5278](https://github.com/cube-js/cube.js/issues/5278)

### Bug Fixes

- **cubesql:** Allow derived tables to have a dot in column name ([#5391](https://github.com/cube-js/cube.js/issues/5391)) ([f83009c](https://github.com/cube-js/cube.js/commit/f83009cf193a6313296dddffa45bffa08ec01725))
- **cubesql:** cast strings to timestamp ([#5331](https://github.com/cube-js/cube.js/issues/5331)) ([a706258](https://github.com/cube-js/cube.js/commit/a706258f85faa3f99150127a2c78f885e99e3aaf))
- **cubesql:** Metabase - substring \_\_user ([#5328](https://github.com/cube-js/cube.js/issues/5328)) ([a25c8bf](https://github.com/cube-js/cube.js/commit/a25c8bf3ddad9c589918b91f05df440eb31a2ad4))
- **cubesql:** udf format_type prepared statement fix ([#5260](https://github.com/cube-js/cube.js/issues/5260)) ([307ed1b](https://github.com/cube-js/cube.js/commit/307ed1b6cc9b242e76d48241cb871b36f571f91e))
- **cubesql:** WHERE Lower / Upper in list with multiply items ([#5376](https://github.com/cube-js/cube.js/issues/5376)) ([2269b2b](https://github.com/cube-js/cube.js/commit/2269b2bb41293107f8e8fca118218c56bf3eca53))
- **cubestore:** Aggregate function MERGE not allowed for column type bytes ([#5166](https://github.com/cube-js/cube.js/issues/5166)) ([7626ed5](https://github.com/cube-js/cube.js/commit/7626ed5fa308854ba625f0956c29b21c84484b67))
- **cubestore:** Fix error: Internal: channel closed on the next request after cubestore cloud process got OOM ([#5238](https://github.com/cube-js/cube.js/issues/5238)) ([cb81fdb](https://github.com/cube-js/cube.js/commit/cb81fdb79c5b768892831437659e1591773d8e15))
- **postgres-driver:** release method should not throw ([#5395](https://github.com/cube-js/cube.js/issues/5395)) ([9423f46](https://github.com/cube-js/cube.js/commit/9423f46141eb73eaac24d9b16c449ff2dbc2918a))
- **query-orchestrator:** pre-aggs build range queries cache key alignment ([#5377](https://github.com/cube-js/cube.js/issues/5377)) ([5896c4a](https://github.com/cube-js/cube.js/commit/5896c4aa4d5a469a892ca9f0e758dc5c6ef6c350))
- **schema-compiler:** throw an error for the empty pre-aggs ([#5392](https://github.com/cube-js/cube.js/issues/5392)) ([4afd604](https://github.com/cube-js/cube.js/commit/4afd6041dae8175fb8d292e9ee0db15969239c81))

### Features

- **cubesql:** Holistics - in dates list filter ([#5333](https://github.com/cube-js/cube.js/issues/5333)) ([94b6509](https://github.com/cube-js/cube.js/commit/94b650928a81be9ea203e50612ea194d9558b298))
- **cubesql:** Support joins with distinct ([#5340](https://github.com/cube-js/cube.js/issues/5340)) ([da4304f](https://github.com/cube-js/cube.js/commit/da4304fef51e33d9c29627d9da92925569943083))
- multiple data source ([#5326](https://github.com/cube-js/cube.js/issues/5326)) ([334af8c](https://github.com/cube-js/cube.js/commit/334af8c56cd02ae551844e9d1e9ab5e107fb1555))
- **cubesql:** Add `float8`, `bool` casts ([b345ade](https://github.com/cube-js/cube.js/commit/b345ade898d6a0ec14e320d66129e985244cddb4))
- **cubesql:** Allow `char_length` function to be used with cubes ([e99344f](https://github.com/cube-js/cube.js/commit/e99344f4e056ef6698f5d92c9e8b79801871a199))
- **cubesql:** Allow filter by exact year (Tableau) ([#5367](https://github.com/cube-js/cube.js/issues/5367)) ([c31e59d](https://github.com/cube-js/cube.js/commit/c31e59d4763e0dd45e96b8e39eb9bcf914370eae))
- **cubesql:** Holistics - support range of charts ([#5325](https://github.com/cube-js/cube.js/issues/5325)) ([d16b4c2](https://github.com/cube-js/cube.js/commit/d16b4c2dc0a582d8e28e48a1e5fae3ff2fe7b0de))
- **cubesql:** Support `date_trunc` over column filter with `<=` ([b30d239](https://github.com/cube-js/cube.js/commit/b30d239ae4e00d8d547f0aa65b324f1f0d3af3f1))
- **query-orchestrator:** introduce unload without temp table ([#5324](https://github.com/cube-js/cube.js/issues/5324)) ([3dcbd2e](https://github.com/cube-js/cube.js/commit/3dcbd2ed1d214d56bfde2183538fce3ec7d65595))
- **testing:** databricks test suite ([#5311](https://github.com/cube-js/cube.js/issues/5311)) ([b77f33b](https://github.com/cube-js/cube.js/commit/b77f33ba9d804d8ca8746fe99d6050ebe26b4528))

### BREAKING CHANGES

- The logic of how cubes are included in joins has been changed. There are multiple member reference constructs that are now forbidden and should be rewritten. You can't reference foreign cubes anymore to define members inside other cubes: `${ForeignCube}.foo`. `foo` member should be defined in `ForeignCube` and referenced as `${ForeignCube.foo}`. You also can't mix references and members without `CUBE` self-reference. For example `${ForeignCube.foo} + bar` is invalid and `${ForeignCube.foo} + ${CUBE}.bar` should be used instead. If not fixed, it'll lead to missing tables in the `FROM` clause.

## [0.30.75](https://github.com/cube-js/cube.js/compare/v0.30.74...v0.30.75) (2022-09-22)

### Bug Fixes

- Cannot read 'map' property of undefined for `rollupLambda` in BigQuery ([47f700f](https://github.com/cube-js/cube.js/commit/47f700fbece156f0d32ba4a9854ecb826bc4641b))
- Invalid identifier day for month `lambdaRollup` ([#5338](https://github.com/cube-js/cube.js/issues/5338)) ([bacc643](https://github.com/cube-js/cube.js/commit/bacc64309b09c99cddc0a57bfcca94ee01dd5877))
- **cubesql:** Allow interval sum chaining ([eabbdc2](https://github.com/cube-js/cube.js/commit/eabbdc27b2a4cd38b4b722ad0c63e2d698868742))
- **databricks-driver:** using the ILIKE operator in the DatabricksFilter.likeIgnoreCase method ([#5334](https://github.com/cube-js/cube.js/issues/5334)) ([a81ca35](https://github.com/cube-js/cube.js/commit/a81ca3507124ec213ae0158918ac231ab9387b26))

## [0.30.74](https://github.com/cube-js/cube.js/compare/v0.30.73...v0.30.74) (2022-09-20)

### Features

- **cubesql:** Support LOWER(?column) IN (?literal) ([#5319](https://github.com/cube-js/cube.js/issues/5319)) ([2e85182](https://github.com/cube-js/cube.js/commit/2e85182c8863d5aaeda07157fade2c00fa27f4e5))

## [0.30.73](https://github.com/cube-js/cube.js/compare/v0.30.72...v0.30.73) (2022-09-19)

### Features

- **cubesql:** Increase limits for statements/portals/cursors ([#5146](https://github.com/cube-js/cube.js/issues/5146)) ([363b42d](https://github.com/cube-js/cube.js/commit/363b42dd4f48cbef31b1832906ae4069023643ca))

### Reverts

- Revert "feat: strategy without creating temp table (#5299)" ([0a3d646](https://github.com/cube-js/cube.js/commit/0a3d6464c660d34742cfd08570c25284e03a3d14)), closes [#5299](https://github.com/cube-js/cube.js/issues/5299)

## [0.30.72](https://github.com/cube-js/cube.js/compare/v0.30.71...v0.30.72) (2022-09-18)

### Bug Fixes

- **cubestore:** Immediately restart stale streaming jobs to avoid processing gaps for sparse streams ([60075af](https://github.com/cube-js/cube.js/commit/60075af851050b5b21b12881d553e318527e4ce7))

### Features

- Introduce `rollupLambda` rollup type ([#5315](https://github.com/cube-js/cube.js/issues/5315)) ([6fd5ee4](https://github.com/cube-js/cube.js/commit/6fd5ee4ed3a7fe98f55ce2f3dc900be1f089e590))
- strategy without creating temp table ([#5299](https://github.com/cube-js/cube.js/issues/5299)) ([8e8f500](https://github.com/cube-js/cube.js/commit/8e8f500216e85676a971b62b57c5af0e48c6a9f9))
- **cubesql:** starts_with, ends_with, LOWER(?column) = ?literal ([#5310](https://github.com/cube-js/cube.js/issues/5310)) ([321b74f](https://github.com/cube-js/cube.js/commit/321b74f03cc5e929ca18c69d15a0734cfa6613f6))

## [0.30.71](https://github.com/cube-js/cube.js/compare/v0.30.70...v0.30.71) (2022-09-16)

### Bug Fixes

- **@cubesj-backend/databricks-driver:** Incorrect escaping of CSV passed to Cube Store -- Can't parse timestamp errors ([21b139c](https://github.com/cube-js/cube.js/commit/21b139ce84a3e6df223338f272f00ea1b2143a1c))

### Features

- **cubesql:** Holistics - string not contains filter ([#5307](https://github.com/cube-js/cube.js/issues/5307)) ([3e563db](https://github.com/cube-js/cube.js/commit/3e563db34b60bc19016b4e3769d96bcdf5a4e42b))
- **cubesql:** Support filtering date within one granularity unit ([427e846](https://github.com/cube-js/cube.js/commit/427e8460e749c1a32f4f9166e19621bc11bee61c))
- **cubesql:** Support startsWith/endsWidth filters (QuickSight) ([#5302](https://github.com/cube-js/cube.js/issues/5302)) ([867279a](https://github.com/cube-js/cube.js/commit/867279abe91b10f61fefaae2cc2578180e1c2f1f))

## [0.30.70](https://github.com/cube-js/cube.js/compare/v0.30.69...v0.30.70) (2022-09-14)

### Features

- **cubejs-docker:** latest-jdk image added to the release cycle ([#5288](https://github.com/cube-js/cube.js/issues/5288)) ([155076a](https://github.com/cube-js/cube.js/commit/155076acb33ca748983b499b2944f8a6764330e0))

## [0.30.69](https://github.com/cube-js/cube.js/compare/v0.30.68...v0.30.69) (2022-09-13)

### Bug Fixes

- **cubesql:** LOWER(\_\_user) IN (?literal) (ThoughtSpot) ([#5292](https://github.com/cube-js/cube.js/issues/5292)) ([0565a16](https://github.com/cube-js/cube.js/commit/0565a16c62cad0c854c59ea4e8f8a6c918883d67))
- **databricks-driver:** remove env user agent ([#5286](https://github.com/cube-js/cube.js/issues/5286)) ([cb903bb](https://github.com/cube-js/cube.js/commit/cb903bbfacdb6cf9e4f624b08433142c5c416d4a))
- **snowflake-driver:** added agent ([#5273](https://github.com/cube-js/cube.js/issues/5273)) ([3a231fb](https://github.com/cube-js/cube.js/commit/3a231fb6ef54fa5b850cf9dc1d78bcd5c6241ebb))
- migrate base driver to ts ([#5233](https://github.com/cube-js/cube.js/issues/5233)) ([c24f545](https://github.com/cube-js/cube.js/commit/c24f5450d68896e06ef6830d9348c0370c22b34c))

### Features

- **base-driver:** Split BaseDriver to @cubejs-backend/base-driver ([#5283](https://github.com/cube-js/cube.js/issues/5283)) ([ca7f9d2](https://github.com/cube-js/cube.js/commit/ca7f9d280c3518e012683c23b82175ec1f96d2a8))
- **cubesql:** Holistics - support range of charts ([#5281](https://github.com/cube-js/cube.js/issues/5281)) ([f52c682](https://github.com/cube-js/cube.js/commit/f52c6827b3fc29f0588d62974eef6323ff32acae))
- **cubesql:** Support `pg_catalog.pg_stats` meta layer table ([f2a1da2](https://github.com/cube-js/cube.js/commit/f2a1da2666852d33c7583cecf696a6a130b00a99))

## [0.30.68](https://github.com/cube-js/cube.js/compare/v0.30.67...v0.30.68) (2022-09-09)

### Features

- **cubesql:** Support IN for \_\_user (ThoughtSpot) ([#5269](https://github.com/cube-js/cube.js/issues/5269)) ([d9aaefc](https://github.com/cube-js/cube.js/commit/d9aaefc65c14bffe87a0676a4e6222d08caf538d))
- **cubesql:** Support interval multiplication ([bb2e82a](https://github.com/cube-js/cube.js/commit/bb2e82ac46a877f8c75996b42bd73bc5c35102ef))

## [0.30.67](https://github.com/cube-js/cube.js/compare/v0.30.66...v0.30.67) (2022-09-09)

### Bug Fixes

- freeze node.js dependencies for images ([#5270](https://github.com/cube-js/cube.js/issues/5270)) ([7ea053e](https://github.com/cube-js/cube.js/commit/7ea053ef07fab6e35b1fa6ec9bd62778c871d221))
- **cubesql:** Show `MEASURE()` instead of `NUMBER()` in measure aggregation type doesn't match error. ([#5268](https://github.com/cube-js/cube.js/issues/5268)) ([a76059e](https://github.com/cube-js/cube.js/commit/a76059eff93c0098ef812f9d09fe996489126cd5))
- **schema-compiler:** messed order in sub-query aggregate ([#5257](https://github.com/cube-js/cube.js/issues/5257)) ([a6ad9f6](https://github.com/cube-js/cube.js/commit/a6ad9f63fd9db502d1e59aa4fe30dbe5c34c364d))

### Features

- **cubesql:** Holistics - GROUP BY dates support ([#5264](https://github.com/cube-js/cube.js/issues/5264)) ([7217950](https://github.com/cube-js/cube.js/commit/7217950c9f0954e23f37efd3609e5eff4d125620))
- **testing:** driver test suite ([#5256](https://github.com/cube-js/cube.js/issues/5256)) ([2e94ec5](https://github.com/cube-js/cube.js/commit/2e94ec571de82e2214ac6d87e762b38c85109585))

## [0.30.66](https://github.com/cube-js/cube.js/compare/v0.30.65...v0.30.66) (2022-09-08)

### Bug Fixes

- driver's constructor parameters initializers ([#5258](https://github.com/cube-js/cube.js/issues/5258)) ([08324a5](https://github.com/cube-js/cube.js/commit/08324a50c29d8f1dad138bfdc1396038cd7ae462))

## [0.30.65](https://github.com/cube-js/cube.js/compare/v0.30.64...v0.30.65) (2022-09-07)

### Bug Fixes

- **cubestore:** Fix 'no such file or directory' after cubestore restart ([#5247](https://github.com/cube-js/cube.js/issues/5247)) ([efaf897](https://github.com/cube-js/cube.js/commit/efaf8972c2751cbb5c5852b91a5c22b171f6064b))

### Features

- **cubesql:** Holistics - support in subquery introspection query ([#5248](https://github.com/cube-js/cube.js/issues/5248)) ([977a251](https://github.com/cube-js/cube.js/commit/977a251fa869344dedebc1315635f2ff9e7de07b))
- **cubesql:** Holistics - support left join introspection query ([#5249](https://github.com/cube-js/cube.js/issues/5249)) ([455d31f](https://github.com/cube-js/cube.js/commit/455d31f531c783c5f621303a1fa4bce01e1fac61))
- **docker:** Install CrateDB driver ([#5252](https://github.com/cube-js/cube.js/issues/5252)) ([40fdfd6](https://github.com/cube-js/cube.js/commit/40fdfd6e7c25450f6c78f7b55e9ffa2d000b1b04))

## [0.30.64](https://github.com/cube-js/cube.js/compare/v0.30.63...v0.30.64) (2022-09-07)

### Bug Fixes

- **cubesql:** select column with the same name as table ([#5235](https://github.com/cube-js/cube.js/issues/5235)) ([1a20f6f](https://github.com/cube-js/cube.js/commit/1a20f6fe2772d36f693821f19ab43e830c198651))

### Features

- **cubesql:** Holistics - support schema privilege query ([#5240](https://github.com/cube-js/cube.js/issues/5240)) ([ae59ddf](https://github.com/cube-js/cube.js/commit/ae59ddffc9fa5b47d00efca6f03f11e5533e5b89))
- **cubesql:** Support nullif with scalars ([#5241](https://github.com/cube-js/cube.js/issues/5241)) ([138dcae](https://github.com/cube-js/cube.js/commit/138dcae3ab9c763f4446f684da101421219abe5b))
- **cubesql:** Support yearly granularity (ThoughtSpot) ([#5236](https://github.com/cube-js/cube.js/issues/5236)) ([416ddd8](https://github.com/cube-js/cube.js/commit/416ddd87042c7fb805b7b9c3c4b6a0bb53552236))

## [0.30.63](https://github.com/cube-js/cube.js/compare/v0.30.62...v0.30.63) (2022-09-05)

### Bug Fixes

- **databricks-driver:** add deprecation of token in jdbc connection string ([#5208](https://github.com/cube-js/cube.js/issues/5208)) ([e59d72c](https://github.com/cube-js/cube.js/commit/e59d72cce23fdcb8a2eb6fa076ca13ace4d75a27))
- **query-orchestrator:** delete temp tables ([#5231](https://github.com/cube-js/cube.js/issues/5231)) ([a39944a](https://github.com/cube-js/cube.js/commit/a39944a44a9e66013087cd98d18aa9a430cc789f))

## [0.30.62](https://github.com/cube-js/cube.js/compare/v0.30.61...v0.30.62) (2022-09-02)

### Features

- **cubesql:** Superset - serverside paging ([#5204](https://github.com/cube-js/cube.js/issues/5204)) ([dfd695d](https://github.com/cube-js/cube.js/commit/dfd695d2f09ea00b4a0b8ef816a597b5c3986ce6))
- **cubesql:** Support dow granularity (ThoughtSpot) ([#5172](https://github.com/cube-js/cube.js/issues/5172)) ([0919e40](https://github.com/cube-js/cube.js/commit/0919e40c9283db45b3856fcf81993d985cbfc7ac))
- **cubesql:** Support doy granularity (ThoughtSpot) ([#5232](https://github.com/cube-js/cube.js/issues/5232)) ([be26775](https://github.com/cube-js/cube.js/commit/be2677535ae1b7bd231530c3bb9266016f9b4c8b))

## [0.30.61](https://github.com/cube-js/cube.js/compare/v0.30.60...v0.30.61) (2022-09-01)

### Bug Fixes

- Correct error message when no pre-aggregation partitions were built and API instance accessing those ([82f2378](https://github.com/cube-js/cube.js/commit/82f2378703d561a93b86b0707d76fe7093a8edf9))

### Features

- **cubesql:** Eliminate literal filter (true or true = true) ([#5142](https://github.com/cube-js/cube.js/issues/5142)) ([7a6f8f9](https://github.com/cube-js/cube.js/commit/7a6f8f9ae91cd314f5c2699dadbd2c5b79c1e73e))
- **cubesql:** Improve support (formats) for TO_TIMESTAMP function ([#5218](https://github.com/cube-js/cube.js/issues/5218)) ([044c3e1](https://github.com/cube-js/cube.js/commit/044c3e1585479b59c391d31c9783ee46908bbcc3))
- **cubesql:** Push down limit through projection ([#5206](https://github.com/cube-js/cube.js/issues/5206)) ([3c6ff7d](https://github.com/cube-js/cube.js/commit/3c6ff7d6eddc925567234a5ec94606eb09970b33))
- **cubesql:** Support `LOCALTIMESTAMP` ([0089a65](https://github.com/cube-js/cube.js/commit/0089a65ae86019df159c0d9dbe5323ebc38c7172))

## [0.30.60](https://github.com/cube-js/cube.js/compare/v0.30.59...v0.30.60) (2022-08-28)

### Bug Fixes

- **cubestore:** Read an inline table only on the assigned worker ([#5147](https://github.com/cube-js/cube.js/issues/5147)) ([15c2aa0](https://github.com/cube-js/cube.js/commit/15c2aa0c6ff7460450b432a0cc6ff0bd04126faf))

## [0.30.59](https://github.com/cube-js/cube.js/compare/v0.30.58...v0.30.59) (2022-08-26)

### Bug Fixes

- **cubesql:** Persist dbname from connection for pg-wire ([#5165](https://github.com/cube-js/cube.js/issues/5165)) ([6bdf5df](https://github.com/cube-js/cube.js/commit/6bdf5df270d00840b1c49e0733c1a5cf8bbc18e3))
- **cubestore-driver:** Correct syntax for string cast ([#5160](https://github.com/cube-js/cube.js/issues/5160)) ([961a2f2](https://github.com/cube-js/cube.js/commit/961a2f2e838fd8d07b7b15b0d89905a2e596c872))

### Features

- **cubestore:** Support Cast from timestamp to string ([#5163](https://github.com/cube-js/cube.js/issues/5163)) ([651a584](https://github.com/cube-js/cube.js/commit/651a5846fd76b2b4792e6f2b83607581ba564cf8))

## [0.30.58](https://github.com/cube-js/cube.js/compare/v0.30.57...v0.30.58) (2022-08-25)

### Bug Fixes

- **@cubejs-backend/cubestore-driver:** Aggregate function MERGE not allowed for column type bytes ([#5164](https://github.com/cube-js/cube.js/issues/5164)) ([6cf5ad2](https://github.com/cube-js/cube.js/commit/6cf5ad2abbb61edb21d27074026fb4c8a1c63f4a))
- **@cubejs-backend/cubestore-driver:** ParseError "Expected (, found: ..." ([6ed62ae](https://github.com/cube-js/cube.js/commit/6ed62aebcfeda10726cdce4e359de95e136c6b89))

### Features

- **cubesql:** Support qtr granularity in DateTrunc for analytics queries ([#5159](https://github.com/cube-js/cube.js/issues/5159)) ([ce13846](https://github.com/cube-js/cube.js/commit/ce1384631f73a6405fd3c82502f0cbb24154259e))

## [0.30.57](https://github.com/cube-js/cube.js/compare/v0.30.56...v0.30.57) (2022-08-25)

### Bug Fixes

- **databricks-jdbc:** databricks multiple downloads of jar ([#5152](https://github.com/cube-js/cube.js/issues/5152)) ([ccb87bc](https://github.com/cube-js/cube.js/commit/ccb87bcbcf717cde530cb47c5442b0a58892bdef))
- **databricks-jdbc:** databricks user-agent ([#5144](https://github.com/cube-js/cube.js/issues/5144)) ([a3a8b89](https://github.com/cube-js/cube.js/commit/a3a8b89e0fd83ffae87325dad85f7e9b7060481f))

### Features

- **cubesql:** DiscardAll in QE - clear prepared statements ([b6fb724](https://github.com/cube-js/cube.js/commit/b6fb72407d3c414175b1831d4d07ae0cb6f8ed53))
- **cubesql:** Support new Superset version ([#5154](https://github.com/cube-js/cube.js/issues/5154)) ([148a062](https://github.com/cube-js/cube.js/commit/148a062530cb399cb96da84821928f5e39e871ce))
- **cubesql:** Support pg_catalog.pg_prepared_statements table ([e03e557](https://github.com/cube-js/cube.js/commit/e03e55709598cc5b0d8efcfec281be76c7caa351))
- **cubestore:** Aggregating index in pre-aggregations ([#5016](https://github.com/cube-js/cube.js/issues/5016)) ([0c3caca](https://github.com/cube-js/cube.js/commit/0c3caca7ac6bfe12fb95a15bb1bc538d1363ecdf))
- **helm-charts:** add values to configure service account ([#5130](https://github.com/cube-js/cube.js/issues/5130)) ([2cc693c](https://github.com/cube-js/cube.js/commit/2cc693cec778bc8e1ee3f72c4c88d18b406f379d))

## [0.30.56](https://github.com/cube-js/cube.js/compare/v0.30.55...v0.30.56) (2022-08-23)

### Bug Fixes

- **cubesql:** array_upper && array_lower UDFs return type fix ([#5136](https://github.com/cube-js/cube.js/issues/5136)) ([9451a86](https://github.com/cube-js/cube.js/commit/9451a86f853c87aef0992c568f5c0a44a0b8610d))
- **cubesql:** Normalize error messsage ([ac00acb](https://github.com/cube-js/cube.js/commit/ac00acbfa71285d5eb423edf42a5a45eb3792c63))

### Features

- Support usage of CTE (with realiasing) ([e64db05](https://github.com/cube-js/cube.js/commit/e64db05c7084568acb1a28b9dada56dd75d35bba))
- **cubesql:** Disable optimizers for analytics queries ([b710c95](https://github.com/cube-js/cube.js/commit/b710c95529fba4ccf853d00436c8ba6ce48a818b))

## [0.30.55](https://github.com/cube-js/cube.js/compare/v0.30.54...v0.30.55) (2022-08-20)

### Reverts

- Revert "fix(cubejs): Fixes CubeStoreDriver user/password auth (#5123)" ([3571d5a](https://github.com/cube-js/cube.js/commit/3571d5a0cfaccfa6da3373197ec536055b6e130a)), closes [#5123](https://github.com/cube-js/cube.js/issues/5123)

## [0.30.54](https://github.com/cube-js/cube.js/compare/v0.30.53...v0.30.54) (2022-08-19)

### Bug Fixes

- **@cubejs-client/vue3:** avoid setQuery Vue3 warnings ([#5084](https://github.com/cube-js/cube.js/issues/5084)) ([#5120](https://github.com/cube-js/cube.js/issues/5120)) ([d380da4](https://github.com/cube-js/cube.js/commit/d380da4e2db09c3eccd8d175287db314568638b1))
- **cubejs:** Fixes CubeStoreDriver user/password auth ([#5123](https://github.com/cube-js/cube.js/issues/5123)) ([8f93347](https://github.com/cube-js/cube.js/commit/8f93347f9cb1c8206b61fe9a472e1805489e0d13))

### Features

- **@cubejs-client/vue3:** support logical operator filters ([#2950](https://github.com/cube-js/cube.js/issues/2950)) ([#5119](https://github.com/cube-js/cube.js/issues/5119)) ([077bb75](https://github.com/cube-js/cube.js/commit/077bb75ac529bf2c32a1e525ba23724a15733aa1))
- **cubesql:** Catch panic on Portal (DF.stream) - return error to the client ([a80cdc7](https://github.com/cube-js/cube.js/commit/a80cdc7a8ed9c66d1ad8d5c7e261e23b10d6d5d0))

## [0.30.53](https://github.com/cube-js/cube.js/compare/v0.30.52...v0.30.53) (2022-08-18)

**Note:** Version bump only for package cubejs

## [0.30.52](https://github.com/cube-js/cube.js/compare/v0.30.51...v0.30.52) (2022-08-18)

### Bug Fixes

- **cubesql:** SUM(CAST(rows.col AS Decimal(38, 10))) expression can't be coerced in Power BI ([#5107](https://github.com/cube-js/cube.js/issues/5107)) ([0037fb4](https://github.com/cube-js/cube.js/commit/0037fb416c68b47e055e846d724ce276b1675879))
- **cubesql:** Type coercion for CASE WHEN THEN ([88b124d](https://github.com/cube-js/cube.js/commit/88b124d2d0549e4d55678fddef73f4a5796c4ada))

### Features

- **cubesql:** Support Redshift connection (ThoughtSpot) ([b244d59](https://github.com/cube-js/cube.js/commit/b244d595487503c3597dc63b6aedca85170e7424))

## [0.30.51](https://github.com/cube-js/cube.js/compare/v0.30.50...v0.30.51) (2022-08-17)

### Bug Fixes

- **cubejs:** Fixes build_range_end for preaggregatons REST API ([#5110](https://github.com/cube-js/cube.js/issues/5110)) ([a1482e5](https://github.com/cube-js/cube.js/commit/a1482e55ca459859914a49e2726fc7c0fea2ce87))

## [0.30.50](https://github.com/cube-js/cube.js/compare/v0.30.49...v0.30.50) (2022-08-16)

**Note:** Version bump only for package cubejs

## [0.30.49](https://github.com/cube-js/cube.js/compare/v0.30.48...v0.30.49) (2022-08-16)

### Bug Fixes

- **server-core:** driverCache removed ([#5103](https://github.com/cube-js/cube.js/issues/5103)) ([e40abf2](https://github.com/cube-js/cube.js/commit/e40abf26b4c6d7d5a927d16998cf46d36ec909ce))

## [0.30.48](https://github.com/cube-js/cube.js/compare/v0.30.47...v0.30.48) (2022-08-14)

### Features

- **cubesql:** Cubes JOIN support ([#5099](https://github.com/cube-js/cube.js/issues/5099)) ([4995476](https://github.com/cube-js/cube.js/commit/4995476e974d4f5ea732e24de6e19fdcd3e308a2))

## [0.30.47](https://github.com/cube-js/cube.js/compare/v0.30.46...v0.30.47) (2022-08-12)

### Bug Fixes

- **mssql-driver:** datetime2 mapping ([#5057](https://github.com/cube-js/cube.js/issues/5057)) ([769104e](https://github.com/cube-js/cube.js/commit/769104e305a585710f1aa7c0bccf1d94472132ef))
- **server-core:** determining custom drivers from the cube.js file ([#5088](https://github.com/cube-js/cube.js/issues/5088)) ([23688db](https://github.com/cube-js/cube.js/commit/23688dbcc65ccb89b927c6dbca0c1fe715fa7eca))

### Features

- **cubejs:** LambdaView: hybrid query of source tables and pre-aggregation tables. ([#4718](https://github.com/cube-js/cube.js/issues/4718)) ([4ae826b](https://github.com/cube-js/cube.js/commit/4ae826b4d27afbfce366830150e130f29c7fcbbf))
- **cubesql:** Datastudio - string startWith filter support ([#5093](https://github.com/cube-js/cube.js/issues/5093)) ([3c21986](https://github.com/cube-js/cube.js/commit/3c21986044732c218ff0c04798cd3bc2fbc6b43c))
- **cubesql:** Metabase v0.44 support ([#5097](https://github.com/cube-js/cube.js/issues/5097)) ([1b2f53b](https://github.com/cube-js/cube.js/commit/1b2f53b8bbff655fa418763534e0ac88f896afcf))
- **cubesql:** Support COALESCE function ([199c775](https://github.com/cube-js/cube.js/commit/199c775d607a70d26c9afa473f397b0f3d1c6e20))
- **cubesql:** Support REGEXP_SUBSTR function (Redshift) ([#5090](https://github.com/cube-js/cube.js/issues/5090)) ([3c9f024](https://github.com/cube-js/cube.js/commit/3c9f024226f6e29f2bedabe7fb88d3fb124e55c7))

## [0.30.46](https://github.com/cube-js/cube.js/compare/v0.30.45...v0.30.46) (2022-08-10)

### Bug Fixes

- **@cubejs-client/vue3:** fix removeOffset warning ([#5082](https://github.com/cube-js/cube.js/issues/5082)) ([#5083](https://github.com/cube-js/cube.js/issues/5083)) ([e1d427b](https://github.com/cube-js/cube.js/commit/e1d427b84aa0b484c9d255b536a4a0b2abab6054))
- **client-core:** Fix a type inference failure ([#5067](https://github.com/cube-js/cube.js/issues/5067)) ([794708e](https://github.com/cube-js/cube.js/commit/794708e7ea3d540afdd86c58b32bab1c6a0d89c4))
- **cubestore:** Fix panic in rolling window processing ([#5066](https://github.com/cube-js/cube.js/issues/5066)) ([acf48ed](https://github.com/cube-js/cube.js/commit/acf48ed7473742be2d5660485e4c4d27a53e22c3))

### Features

- **cubesql:** Datastudio - aggr by month and day support ([#5025](https://github.com/cube-js/cube.js/issues/5025)) ([da3ed59](https://github.com/cube-js/cube.js/commit/da3ed59910b968cf0523f49eea7758f33d427b3e))
- **cubesql:** Datastudio - between dates filter support ([#5022](https://github.com/cube-js/cube.js/issues/5022)) ([20f7d64](https://github.com/cube-js/cube.js/commit/20f7d649574a522c380bfd069667f929855bd6d1))
- **cubesql:** Datastudio - Min/Max datetime aggregation support ([#5021](https://github.com/cube-js/cube.js/issues/5021)) ([7cf1f75](https://github.com/cube-js/cube.js/commit/7cf1f7520956304a74e79b7acc54b61f907d0706))
- **cubesql:** Support DEALLOCATE in pg-wire ([06b6476](https://github.com/cube-js/cube.js/commit/06b647687afa37fcca075e018910049ad0ac0883))
- **drivers:** Bootstraps CrateDB driver ([#4929](https://github.com/cube-js/cube.js/issues/4929)) ([db87b8f](https://github.com/cube-js/cube.js/commit/db87b8f18686607498467c6ff0f71abcd1e38c5d))
- **schema-compiler:** aggregated sub query group by clause ([#5078](https://github.com/cube-js/cube.js/issues/5078)) ([473398d](https://github.com/cube-js/cube.js/commit/473398d0a4a983730aba115766afc53f2dd829a6))
- **sqlite-driver:** Bump sqlite3 to ^5.0.11 ([8a16cc7](https://github.com/cube-js/cube.js/commit/8a16cc72d6910a2670b2c751ab9af186229f2dbb))

## [0.30.45](https://github.com/cube-js/cube.js/compare/v0.30.44...v0.30.45) (2022-08-05)

### Bug Fixes

- **cubestore:** Support a space separated binary strings for varbinary fields in csv ([#5061](https://github.com/cube-js/cube.js/issues/5061)) ([c67793e](https://github.com/cube-js/cube.js/commit/c67793eb0aa772548cd6c05767be17cfe9a86d18))
- **query-orchestrator:** API instance throw on missing partitions ([#5069](https://github.com/cube-js/cube.js/issues/5069)) ([7b16875](https://github.com/cube-js/cube.js/commit/7b16875ee204ddc9603dc2be900799d748facae1))
- **query-orchestrator:** excessive buildRange queries ([d818611](https://github.com/cube-js/cube.js/commit/d8186116aa99247e20609db22a993dc0a548cfdf))

### Features

- **cubesql:** Support binary bitwise operators (>>, <<) ([7363879](https://github.com/cube-js/cube.js/commit/7363879184395b3c499f9b678da7152362226ea0))
- **cubesql:** Support svv_tables table (Redshift) ([#5060](https://github.com/cube-js/cube.js/issues/5060)) ([d3ed3ac](https://github.com/cube-js/cube.js/commit/d3ed3aca798d41fe4e1919c9fde2f7610435168c))
- max partitions per cube configuration ([4b3739c](https://github.com/cube-js/cube.js/commit/4b3739c65133316802bfb377dec9743365a99566))

## [0.30.44](https://github.com/cube-js/cube.js/compare/v0.30.43...v0.30.44) (2022-08-01)

### Bug Fixes

- **cubesql:** Ignore IO's UnexpectedEof|BrokenPipe on handling error ([98deb73](https://github.com/cube-js/cube.js/commit/98deb7362bf772816af88173e6669bf486c328a9))
- **server-core:** driverFactory results assertion ([5eaad01](https://github.com/cube-js/cube.js/commit/5eaad018a4150c1ab7d56f68f372ba681548d84d))

## [0.30.43](https://github.com/cube-js/cube.js/compare/v0.30.42...v0.30.43) (2022-07-28)

### Bug Fixes

- **cubesq:** Ignore BrokenPipe/UnexpectedEOF as error in pg-wire ([4ec01d2](https://github.com/cube-js/cube.js/commit/4ec01d269f2216f74b841ebe2fd96d3b8597fdcc))

### Features

- **cubesql:** Security Context switching (Row Access) ([731e1ab](https://github.com/cube-js/cube.js/commit/731e1ab6d9362fb9a1857f5276e22a565f79781c))
- **databricks-jdbc:** UserAgentEntry property configuration for the databricks-jdbc driver ([bccae12](https://github.com/cube-js/cube.js/commit/bccae12715fcbfdecbc2175de5e4ad6601ba374f))

## [0.30.42](https://github.com/cube-js/cube.js/compare/v0.30.41...v0.30.42) (2022-07-27)

### Features

- **cubesql:** Metabase - support Summarize by week of year ([#5000](https://github.com/cube-js/cube.js/issues/5000)) ([37589a9](https://github.com/cube-js/cube.js/commit/37589a9e58b0c8f14922041432647b814759f22a))
- **server-core:** disable health checks for API instances with rollup only mode ([a6601e5](https://github.com/cube-js/cube.js/commit/a6601e5ad82922446654d832cf5e7a6cb7cee870))

## [0.30.41](https://github.com/cube-js/cube.js/compare/v0.30.40...v0.30.41) (2022-07-27)

### Bug Fixes

- **databricks-jdbc:** postinstall script with accept_policy env ([1e38791](https://github.com/cube-js/cube.js/commit/1e387919e8636fc9320ec2f043073e91f5b2e671))

## [0.30.40](https://github.com/cube-js/cube.js/compare/v0.30.39...v0.30.40) (2022-07-26)

### Bug Fixes

- **client-react:** update hooks for React 18 StrictMode ([#4999](https://github.com/cube-js/cube.js/issues/4999)) ([fd6352c](https://github.com/cube-js/cube.js/commit/fd6352c61a614afef61b2a9d4332ecf300594b3b))
- **server-core:** restoring health check for the API instances ([bfaee0f](https://github.com/cube-js/cube.js/commit/bfaee0fe26da9b3ea82c41d62df1b8a0018ade31))

## [0.30.39](https://github.com/cube-js/cube.js/compare/v0.30.38...v0.30.39) (2022-07-25)

### Bug Fixes

- **query-orchestrator:** API instance throw on missing partitions ([3bf0882](https://github.com/cube-js/cube.js/commit/3bf08822d77c317e145fc498207372618c862498))

## [0.30.38](https://github.com/cube-js/cube.js/compare/v0.30.37...v0.30.38) (2022-07-25)

### Features

- **cubesql:** Define standard_conforming_strings (SQLAlchemy compatibility) ([8fbc046](https://github.com/cube-js/cube.js/commit/8fbc0467c2e3e37fa9c4b320630dc9200884f3ee)), closes [#L2994](https://github.com/cube-js/cube.js/issues/L2994)
- **cubesql:** Support Cast(expr as Regclass) ([e3cafe4](https://github.com/cube-js/cube.js/commit/e3cafe4a0a291d61545e8855425b8755f3629a4e))
- **cubesql:** Support for new introspection query in SQLAlchemy ([0dbc9e6](https://github.com/cube-js/cube.js/commit/0dbc9e6551016d12155bba27a57b9a17e13dbd02)), closes [#L3381](https://github.com/cube-js/cube.js/issues/L3381)
- **cubesql:** Support pg_catalog.pg_sequence table ([fe057bf](https://github.com/cube-js/cube.js/commit/fe057bf256b8744a9c3f407908808cefa6cd6d8c))
- **docker:** jdk containers ([90dbe9f](https://github.com/cube-js/cube.js/commit/90dbe9f846b3aadaa6fe2b7cf8462cf1fb53e413))

### Reverts

- revert "docs: update Home page to use new grid (#4898)" ([71bcdf8](https://github.com/cube-js/cube.js/commit/71bcdf82636a54eb326e6a95efbab6a2d6b7df87)), closes [#4898](https://github.com/cube-js/cube.js/issues/4898)

## [0.30.37](https://github.com/cube-js/cube.js/compare/v0.30.36...v0.30.37) (2022-07-20)

### Bug Fixes

- **cubesql:** Correct UDTF behavior with no batch sections ([f52c89a](https://github.com/cube-js/cube.js/commit/f52c89a1baedd9e5a259b663f9427e02ade9fb10))
- **cubestore:** Optimize big plans send out for big number of workers ([#4948](https://github.com/cube-js/cube.js/issues/4948)) ([79421af](https://github.com/cube-js/cube.js/commit/79421afe65dab919f98116c5047a9f1a546fb952))

### Features

- **cubesql:** Add `pg_constraint` pg_type ([e9beb5f](https://github.com/cube-js/cube.js/commit/e9beb5fd875e8f2d181aec45035849e503a61e6b))
- **cubestore:** Stores build_range_end in table metadata ([#4938](https://github.com/cube-js/cube.js/issues/4938)) ([69f730f](https://github.com/cube-js/cube.js/commit/69f730fcb69709ddd14e6c44640d409cc382a075))

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
- **server-core:** CUBEJS_PRE_AGGREGATIONS_SCHEMA behavior ([4838047](https://github.com/cube-js/cube.js/commit/4838047f3a9981aef7d3961f329c0388df97a88c))

### Features

- **cubesql:** Metabase - datetime filters with 'starting from' flag support ([#4882](https://github.com/cube-js/cube.js/issues/4882)) ([4cc01f1](https://github.com/cube-js/cube.js/commit/4cc01f1750b141ad851081efafb1833133420885))
- **cubesql:** Support `PREPARE` queries in pg-wire ([#4906](https://github.com/cube-js/cube.js/issues/4906)) ([2e2ae63](https://github.com/cube-js/cube.js/commit/2e2ae6347692ae5ae77fcf6f921c97b5c5bd10f1))

### Reverts

- Revert "chore: Refactor Rollup config, fix warnings, and support more file extensions (#4541)" ([272dd8b](https://github.com/cube-js/cube.js/commit/272dd8bd0e549ca764fb535225c5d08ebf389e00)), closes [#4541](https://github.com/cube-js/cube.js/issues/4541)

## [0.30.34](https://github.com/cube-js/cube.js/compare/v0.30.33...v0.30.34) (2022-07-12)

### Bug Fixes

- **api-gateway:** Sending "Incoming network usage" event after context resolver ([#4896](https://github.com/cube-js/cube.js/issues/4896)) ([eecf776](https://github.com/cube-js/cube.js/commit/eecf776c2b9f959b4601143d92a7921b70709a95))
- Cache allDefinitions Object.assign invocations to optimize highly dynamic schema SQL execution ([#4894](https://github.com/cube-js/cube.js/issues/4894)) ([ad3fea8](https://github.com/cube-js/cube.js/commit/ad3fea8610aaee7aae33623821a2aa3a3416a62a))
- **cubestore:** Elaborate on can't be listed after upload error ([bd1ec69](https://github.com/cube-js/cube.js/commit/bd1ec6930bf3de3173208e0416ab8c35f2d47a76))
- **cubestore:** Fix index selection in case of AND in conditions ([a0e03ab](https://github.com/cube-js/cube.js/commit/a0e03abe698cdd9db09653824f80ee688feca6d5))
- **cubestore:** removing HLL columns from indexes ([#4884](https://github.com/cube-js/cube.js/issues/4884)) ([e7df6e8](https://github.com/cube-js/cube.js/commit/e7df6e882868979d7ccaa71ea1904b6d636ca206))
- **server-core:** driverFactory result type checking ([6913a49](https://github.com/cube-js/cube.js/commit/6913a49005ae1e88a8e184be5cbf9f8d9f4e6930))

### Features

- **cubesql:** Metabase - BETWEEN filters support ([#4852](https://github.com/cube-js/cube.js/issues/4852)) ([b191120](https://github.com/cube-js/cube.js/commit/b19112079f0f9a51d6703e37afaa121d09ce31e4))
- **cubesql:** Metabase - filters with relative dates support ([#4851](https://github.com/cube-js/cube.js/issues/4851)) ([423be2f](https://github.com/cube-js/cube.js/commit/423be2f33d40ccd5681c47201586ac93944ac9dd))
- **cubesql:** Support Extract(DAY/DOW), Binary (?expr + ?literal_expr) for rewriting (Metabase) ([#4887](https://github.com/cube-js/cube.js/issues/4887)) ([2565705](https://github.com/cube-js/cube.js/commit/2565705fcff6a3d3dc4ff5ac2dcd819d8ad040db))
- **cubesql:** Support Substring for rewriting (Metabase) ([#4881](https://github.com/cube-js/cube.js/issues/4881)) ([8fadebd](https://github.com/cube-js/cube.js/commit/8fadebd7670e9f461a16e51e5114812933722ddd))
- **server-core:** pre-aggregations building by API instances only if CUBEJS_PRE_AGGREGATIONS_BUILDER is set ([a203513](https://github.com/cube-js/cube.js/commit/a203513666bb36c7f45e25fdf24908bf6d44ac3d))

## [0.30.33](https://github.com/cube-js/cube.js/compare/v0.30.32...v0.30.33) (2022-07-07)

### Bug Fixes

- **server-core:** original dbType behavior restored ([#4874](https://github.com/cube-js/cube.js/issues/4874)) ([e4231e0](https://github.com/cube-js/cube.js/commit/e4231e0c43d8057a2e100b616ea22a09dca6ee0d))

## [0.30.32](https://github.com/cube-js/cube.js/compare/v0.30.31...v0.30.32) (2022-07-07)

### Bug Fixes

- **cubesql:** Correct portal pagination (use PortalSuspended) in pg-wire ([#4872](https://github.com/cube-js/cube.js/issues/4872)) ([63aad19](https://github.com/cube-js/cube.js/commit/63aad191ea2be58291b0ce8709e1352a62cbd8a4))

### Features

- **cubesql:** Support grant tables (columns, tables) ([a3d9493](https://github.com/cube-js/cube.js/commit/a3d949324e1ac879606b45faed4812b30b07173b))

## [0.30.31](https://github.com/cube-js/cube.js/compare/v0.30.30...v0.30.31) (2022-07-07)

### Bug Fixes

- **playground:** member search ([#4850](https://github.com/cube-js/cube.js/issues/4850)) ([c110ca5](https://github.com/cube-js/cube.js/commit/c110ca5c517d7f513442ad17bba9d62477877fd9))
- **server-core:** dbType assertion ([237f920](https://github.com/cube-js/cube.js/commit/237f92013ec4d8ed34f958018fb85ae8e5b41600))

### Features

- **cubesql:** Initial support for canceling queries in pg-wire ([#4847](https://github.com/cube-js/cube.js/issues/4847)) ([bce0f99](https://github.com/cube-js/cube.js/commit/bce0f994d59a48f221cce3d21e3c2f3244e5f3a1))

## [0.30.30](https://github.com/cube-js/cube.js/compare/v0.30.29...v0.30.30) (2022-07-05)

### Bug Fixes

- drivers default concurrency values ([4b7296f](https://github.com/cube-js/cube.js/commit/4b7296f266b49e3d38dce1ff82ce4edd703121bc))
- **client-react:** useIsMounted hook compatible with React 18 StrictMode ([#4740](https://github.com/cube-js/cube.js/issues/4740)) ([aa7d3a4](https://github.com/cube-js/cube.js/commit/aa7d3a4788d38027f59c77bd567270a98a43b689))
- **cubesql:** Invalid argument error: all columns in a record batch must have the same length ([895f8cf](https://github.com/cube-js/cube.js/commit/895f8cf301a951907aa4cd3ea190ea1cfeb3be73))

### Features

- **cubesql:** Superset ILIKE support for Search all filter options feature ([2532040](https://github.com/cube-js/cube.js/commit/2532040792faa9ed0a151d85cead1c1bd425d3ce))
- **cubesql:** Support Interval type for pg-wire ([4c8a82c](https://github.com/cube-js/cube.js/commit/4c8a82caf3b64c295bf7606e6a694f6cda50491c))
- centralized concurrency setting ([#4735](https://github.com/cube-js/cube.js/issues/4735)) ([1c897a1](https://github.com/cube-js/cube.js/commit/1c897a13c62049e23d26009351622b2a93c0a745))
- **cubesql:** Support for metabase literal queries ([#4843](https://github.com/cube-js/cube.js/issues/4843)) ([6d45d55](https://github.com/cube-js/cube.js/commit/6d45d558e0c58c37c515d07cae367eed5624cb3a))
- dbt metric should take table alias into account ([#4842](https://github.com/cube-js/cube.js/issues/4842)) Thanks [@bartlomiejolma](https://github.com/bartlomiejolma) ! ([413d8df](https://github.com/cube-js/cube.js/commit/413d8dfb9b3cc6a3fcf28679b82241468dcb07e7))

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
- More explanatory error message on failed rollupJoin match ([5808e37](https://github.com/cube-js/cube.js/commit/5808e375baf31259d8d070f3d5ff5b2a4faf5db4)), closes [#4746](https://github.com/cube-js/cube.js/issues/4746)

## [0.30.27](https://github.com/cube-js/cube.js/compare/v0.30.26...v0.30.27) (2022-06-24)

### Bug Fixes

- **client-vue:** Fix boolean operator in filter without using builderProps ([#4782](https://github.com/cube-js/cube.js/issues/4782)) ([904171e](https://github.com/cube-js/cube.js/commit/904171ee85185f1ba1fcf812ba58ae6bc11b8407))
- **cubesql:** Correct TransactionStatus for Sync in pg-wire ([90c6265](https://github.com/cube-js/cube.js/commit/90c62658fe076060161e6384e0b3dcc8e7e94dd4))
- **cubesql:** Return error on execute for unknown portal in pg-wire ([0b87261](https://github.com/cube-js/cube.js/commit/0b872614f30f5fd9b22c88916ad4edba604f8d02))
- **cubesql:** thread 'tokio-runtime-worker' panicked at 'called `Option::unwrap()` on a `None` value' in case of MEASURE() called on a dimension ([5d62c5a](https://github.com/cube-js/cube.js/commit/5d62c5af1562696ccb192c800ed2047b8345f8f8))
- **questdb-driver:** schema query error due to system tables ([#4762](https://github.com/cube-js/cube.js/issues/4762)) ([5571a70](https://github.com/cube-js/cube.js/commit/5571a70a987b18e6d28b3200a482b76054f2639b))

### Features

- **cubesql:** Metabase interval date range filter support ([#4763](https://github.com/cube-js/cube.js/issues/4763)) ([221715a](https://github.com/cube-js/cube.js/commit/221715adee2876585c639e8918dc0f171ad91a86))
- **cubesql:** Support Numeric type (text + binary) in pg-wire ([db7ec5c](https://github.com/cube-js/cube.js/commit/db7ec5c2d0a726b99daf014a70cdee8c15d3721b))
- **cubesql:** Workaround for Metabase introspection query ([ee7b3cf](https://github.com/cube-js/cube.js/commit/ee7b3cfd7401882bf802d668e5709e4f02c64be3))
- **cubestore:** Aggregating index ([#4379](https://github.com/cube-js/cube.js/issues/4379)) ([a0bd36c](https://github.com/cube-js/cube.js/commit/a0bd36c0898f66da907f3c04c439fc31826777a0)), closes [#4599](https://github.com/cube-js/cube.js/issues/4599) [#4728](https://github.com/cube-js/cube.js/issues/4728)

## [0.30.26](https://github.com/cube-js/cube.js/compare/v0.30.25...v0.30.26) (2022-06-20)

### Features

- **cubesql:** Correct implementation for placeholder binder/finder in pg-wire ([fa018bd](https://github.com/cube-js/cube.js/commit/fa018bd62fd0d7f66c8aa0b68b43cc37d73d65ac))
- **cubesql:** Replace timestamptz CAST with timestamp ([9e7c1bd](https://github.com/cube-js/cube.js/commit/9e7c1bd69adae367a65f77339087194e7e1bc5fe))
- **cubesql:** Support Int8 for Bind + binary in pg-wire ([f28fbd5](https://github.com/cube-js/cube.js/commit/f28fbd5049e0a72adbb0f078e45728d60b481ca2))
- **cubesql:** Support placeholders in `WITH` and `LIMIT` ([#4768](https://github.com/cube-js/cube.js/issues/4768)) ([d444c0f](https://github.com/cube-js/cube.js/commit/d444c0fda31b3cdf824e85c3f03d76d8a3f47211))
- **cubesql:** Workaround CTEs with subqueries (Sigma) ([#4767](https://github.com/cube-js/cube.js/issues/4767)) ([d99a02f](https://github.com/cube-js/cube.js/commit/d99a02f508418c9a054977572da0985f627acfc3))

## [0.30.25](https://github.com/cube-js/cube.js/compare/v0.30.24...v0.30.25) (2022-06-16)

### Bug Fixes

- **cubejs-docker:** fix typos in dev.Dockerfile ([f1cdf55](https://github.com/cube-js/cube.js/commit/f1cdf55348a8e2049a27a53b54afd33feefbb4df))
- **databricks-jdbc:** communication link failure ([9c3577d](https://github.com/cube-js/cube.js/commit/9c3577d4935d355b4de79c3d84771e591ada8ec2))

### Features

- **cubestore:** Compaction in-memory chunks ([#4701](https://github.com/cube-js/cube.js/issues/4701)) ([7c11e37](https://github.com/cube-js/cube.js/commit/7c11e379146a6bd3614452dbf12eb544f6c658a4))
- logging cubesql queries errors ([#4550](https://github.com/cube-js/cube.js/issues/4550)) ([10021c3](https://github.com/cube-js/cube.js/commit/10021c34f28348183fd30584d8bb97a97103b91e))
- **cubesql:** PowerBI support for wrapped queries ([#4752](https://github.com/cube-js/cube.js/issues/4752)) ([fc129d4](https://github.com/cube-js/cube.js/commit/fc129d4364ea89ea32aa903cda9499133959fdbe))

## [0.30.24](https://github.com/cube-js/cube.js/compare/v0.30.23...v0.30.24) (2022-06-14)

### Features

- **databricks-jdbc-driver:** Export bucket mount dir setting ([483d094](https://github.com/cube-js/cube.js/commit/483d0947401687d0c492a6f98f63bc915d648428))

## [0.30.23](https://github.com/cube-js/cube.js/compare/v0.30.22...v0.30.23) (2022-06-14)

### Features

- **databricks-jdbc:** jdbc implementation of the export bucket feature ([d534e67](https://github.com/cube-js/cube.js/commit/d534e670a4568b905b55d0f5504d5c836d5841b2))

## [0.30.22](https://github.com/cube-js/cube.js/compare/v0.30.21...v0.30.22) (2022-06-14)

### Bug Fixes

- **@cubejs-backend/databricks-jdbc-driver:** Missing prefix during blob listing ([0ffea61](https://github.com/cube-js/cube.js/commit/0ffea61a8d3de5afe2cbee351d359d262669024c))

## [0.30.21](https://github.com/cube-js/cube.js/compare/v0.30.20...v0.30.21) (2022-06-13)

### Features

- **databricks-jdbc:** improving performance ([1ef4504](https://github.com/cube-js/cube.js/commit/1ef45040f912d7f81b76a0495ddfa5db28ad9067))

## [0.30.20](https://github.com/cube-js/cube.js/compare/v0.30.19...v0.30.20) (2022-06-11)

### Bug Fixes

- **cubesql:** Send `Empty Query` message on empty query ([88e966d](https://github.com/cube-js/cube.js/commit/88e966d12e31e6277ac02bf9a1b44cd7c8722311))

### Features

- **cubesql:** Support pg_catalog.pg_roles table ([eed0727](https://github.com/cube-js/cube.js/commit/eed0727fe70b9fddfaddf8c32821fc721c911ae8))
- **cubesql:** Support pg_my_temp_schema, pg_is_other_temp_schema UDFs ([c843491](https://github.com/cube-js/cube.js/commit/c843491c834204231424a21bc8a89b18336cc68a))

## [0.30.19](https://github.com/cube-js/cube.js/compare/v0.30.18...v0.30.19) (2022-06-10)

### Bug Fixes

- **api-gateway:** correct calculation of total if there is a query property offset ([c176891](https://github.com/cube-js/cube.js/commit/c176891b354d4d8fc9b0e2806bdebd51bf452f66))

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

## [0.30.15](https://github.com/cube-js/cube.js/compare/v0.30.14...v0.30.15) (2022-06-06)

**Note:** Version bump only for package cubejs

## [0.30.14](https://github.com/cube-js/cube.js/compare/v0.30.13...v0.30.14) (2022-06-06)

### Bug Fixes

- **databricks-jdbc:** concurrency improvements ([e7857f0](https://github.com/cube-js/cube.js/commit/e7857f0231db15c81a9eb52669318d51774a1fdb))

### Features

- **cubesql:** Auto-closing hold cursos on transaction end (simple query) ([79725ec](https://github.com/cube-js/cube.js/commit/79725ec3b02abde6d9cf3f5d3e45e60518a8386f))
- **cubesql:** cast DECIMAL with default precision and scale ([#4709](https://github.com/cube-js/cube.js/issues/4709)) ([771d179](https://github.com/cube-js/cube.js/commit/771d1797f4084fff68f0291c55a37b25b32fb5e2))
- **cubesql:** Support CAST for name, int2/4/8 ([#4711](https://github.com/cube-js/cube.js/issues/4711)) ([36fe891](https://github.com/cube-js/cube.js/commit/36fe891fd102c165eb28b0c5561151934751f143))
- **cubesql:** Support CLOSE [name | ALL] (cursors) for pg-wire ([#4712](https://github.com/cube-js/cube.js/issues/4712)) ([91048bd](https://github.com/cube-js/cube.js/commit/91048bd48ddf755b436f41a1bdfff8b24d4bf5f5))
- **cubesql:** Support Metabase pg_type introspection query ([2401dbf](https://github.com/cube-js/cube.js/commit/2401dbf9e5b5de75c5f7cf31e3586135a0a016e5))
- **helm-charts:** Add imagePullSecrets in k8s manifest ([#4491](https://github.com/cube-js/cube.js/issues/4491)) ([cdda813](https://github.com/cube-js/cube.js/commit/cdda8132d2a5f53d805c32e9e999225dee666c44))

## [0.30.13](https://github.com/cube-js/cube.js/compare/v0.30.12...v0.30.13) (2022-06-05)

### Features

- **cubesql:** PowerBI is not empty filter ([e31ffdc](https://github.com/cube-js/cube.js/commit/e31ffdcd762236fb54d454ede7e892acb54bdcee))

## [0.30.12](https://github.com/cube-js/cube.js/compare/v0.30.11...v0.30.12) (2022-06-03)

### Features

- **databricks-jdbc:** export bucket for azure via databricks api v2.0 ([3d6b8c6](https://github.com/cube-js/cube.js/commit/3d6b8c6e14ccaa784ed07bf13adcd72f948526a8))

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
- **driver-materialize:** filter non-materialized objects from schema ([#4584](https://github.com/cube-js/cube.js/issues/4584)) ([a32a9a9](https://github.com/cube-js/cube.js/commit/a32a9a94f9eabb9f5a4c7f7201d902f88fbe321b))

### Features

- **cubesql:** Allow `::information_schema.cardinal_number` casting ([b198fb3](https://github.com/cube-js/cube.js/commit/b198fb3a70b3d075ccdfaff638dc8f36e6530944))
- **cubesql:** excel subquery column with same name ([#4602](https://github.com/cube-js/cube.js/issues/4602)) ([ea3a0bc](https://github.com/cube-js/cube.js/commit/ea3a0bc4a944cd724672056f5885110c7cee90cd))
- **cubesql:** PowerBI basic queries support ([455ae07](https://github.com/cube-js/cube.js/commit/455ae076880f305ed73d1d217a87f908837070f5))
- **cubesql:** Support array_upper, array_lower UDFs ([5a3b6bb](https://github.com/cube-js/cube.js/commit/5a3b6bb31c5af920c706b56a8e3c5046f272f8ca))
- **cubesql:** Support to_char UDF ([#4600](https://github.com/cube-js/cube.js/issues/4600)) ([48077a9](https://github.com/cube-js/cube.js/commit/48077a95fccf48309085e6f1f9b2652c581ab3a3))
- **schema-compiler:** allowNonStrictDateRangeMatch flag support for the pre-aggregations with time dimension ([#4582](https://github.com/cube-js/cube.js/issues/4582)) ([31d9fae](https://github.com/cube-js/cube.js/commit/31d9faea762f2103a19e6d3062e272b9dbc88dc8))

## [0.30.7](https://github.com/cube-js/cube.js/compare/v0.30.6...v0.30.7) (2022-05-26)

### Bug Fixes

- **cubesql:** Correct command completion for SET in pg-wire ([ab42e54](https://github.com/cube-js/cube.js/commit/ab42e54b2c49aea63d4db75e9332655159fa73e6))
- **playground:** query validation ([2052895](https://github.com/cube-js/cube.js/commit/205289549c964eba01e76fdadca86c4e8117e967))

### Features

- **cubesql:** Support escaped string literals, E'str' ([ef9700d](https://github.com/cube-js/cube.js/commit/ef9700d8f7a1ccd0a31aeece70fdcecee092eb9f))
- **cubesql:** Support multiple stmts for simple query in pg-wire ([0f645cb](https://github.com/cube-js/cube.js/commit/0f645cbd0a4bf25d0a03a14d366607ae716fc792))

## [0.30.6](https://github.com/cube-js/cube.js/compare/v0.30.5...v0.30.6) (2022-05-24)

### Bug Fixes

- **cubesql:** Normalize column names for joins and aliased columns ([7faadc9](https://github.com/cube-js/cube.js/commit/7faadc9c96d4cb80b7318a1955cd01e854ca2272))

### Features

- **cubesql:** Support `_pg_truetypid`, `_pg_truetypmod` UDFs ([1436a76](https://github.com/cube-js/cube.js/commit/1436a76c71e7cec8a62149def9fc2de39a48acef))

## [0.30.5](https://github.com/cube-js/cube.js/compare/v0.30.4...v0.30.5) (2022-05-23)

### Bug Fixes

- **schema-compiler:** exclude non-included dimensions from drillMembers ([#4569](https://github.com/cube-js/cube.js/issues/4569)) ([c1f12d1](https://github.com/cube-js/cube.js/commit/c1f12d182a3f7434a296a97b640b19b705dba921))

### Features

- **packages:** add Firebolt driver ([#4546](https://github.com/cube-js/cube.js/issues/4546)) ([9789d95](https://github.com/cube-js/cube.js/commit/9789d954064893ba2e3265127be5b734bf1f6db5))
- statefulset annotations ([#4422](https://github.com/cube-js/cube.js/issues/4422)) ([27c1ff2](https://github.com/cube-js/cube.js/commit/27c1ff2f0d89ac3c4401aaa897ee3ebf2542b6d0))

## [0.30.4](https://github.com/cube-js/cube.js/compare/v0.30.3...v0.30.4) (2022-05-20)

### Bug Fixes

- **cubesql:** Skip returning of schema for special queries in pg-wire ([479ec78](https://github.com/cube-js/cube.js/commit/479ec78836cc095dda8c3725e1378b9f60f56233))
- **cubesql:** Wrong format in RowDescription, support i16/i32/f32 ([0c52cd6](https://github.com/cube-js/cube.js/commit/0c52cd6180e7cf43aeb735ec901da07508ff4598))
- **cubestore:** Prevent deleting a schema that contains tables ([2824fb2](https://github.com/cube-js/cube.js/commit/2824fb20f68401394b05e7e7bc085a669fd39f69))
- **playground:** refresh stale tokens ([#4551](https://github.com/cube-js/cube.js/issues/4551)) ([80ce87d](https://github.com/cube-js/cube.js/commit/80ce87da7fdcbe44aa26605ee33b9cfbb3cbfb24))
- **playground:** Remove all time time dimension without granularity ([#4564](https://github.com/cube-js/cube.js/issues/4564)) ([054f488](https://github.com/cube-js/cube.js/commit/054f488ce6b8bfa103cd435f99178ca1f2fa38c7))
- refresh worker force queue reconcile logic ([#4529](https://github.com/cube-js/cube.js/issues/4529)) ([144e97d](https://github.com/cube-js/cube.js/commit/144e97d178370142d7d7c433fd89212a33df63da))

### Features

- Download streaming `select * from table` originalSql pre-aggregations directly ([#4537](https://github.com/cube-js/cube.js/issues/4537)) ([a62d81a](https://github.com/cube-js/cube.js/commit/a62d81abe67fc94ca57cabf914cc55800fc89d96))
- **cubesql:** Allow ::oid casting ([bb31838](https://github.com/cube-js/cube.js/commit/bb318383028ce9557ccd45ae03cd33f05705bff2))
- **cubesql:** Initial support for type receivers ([452f504](https://github.com/cube-js/cube.js/commit/452f504b7c57d6c669de2eabba935c7a398aa7d2))
- **cubesql:** Support ||, correct schema/catalog/ordinal_position ([6d6cbf5](https://github.com/cube-js/cube.js/commit/6d6cbf5ee743e527d8b9f64008cdc0d12103abf6))
- **cubesql:** Support DISCARD [ALL | PLANS | SEQUENCES | TEMPORARY |… ([#4560](https://github.com/cube-js/cube.js/issues/4560)) ([390c764](https://github.com/cube-js/cube.js/commit/390c764a98fb58fc294cdfe08ed224f2318e1b31))
- **cubesql:** Support IS TRUE|FALSE ([4d227b1](https://github.com/cube-js/cube.js/commit/4d227b11cbe93352d735c81b50da79f256266bb9))
- **cubestore:** Adds minio s3 region env to cubestore cloud storage ([#4539](https://github.com/cube-js/cube.js/issues/4539)) Thanks [@olejeglejeg](https://github.com/olejeglejeg)! ([739d79c](https://github.com/cube-js/cube.js/commit/739d79caeced2377164809251a3ef69bf79edddc))

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

### Bug Fixes

- **materialize-driver:** commit cursor on release ([#4526](https://github.com/cube-js/cube.js/issues/4526)) ([441581c](https://github.com/cube-js/cube.js/commit/441581cb38406e74081c0a06fe747dd8efb8cb8d))

### Features

- **cubesql:** Add CUBEJS_PG_SQL_PORT env support and SQL API reference docs ([#4531](https://github.com/cube-js/cube.js/issues/4531)) ([de60d71](https://github.com/cube-js/cube.js/commit/de60d71c360be47e3231e7eafa349b9a0fddd244))
- **cubesql:** Provide specific error messages for not matched expressions ([e035780](https://github.com/cube-js/cube.js/commit/e0357801bd39269585dd31d6ad932b32287a05af))
- **cubesql:** Support `quarter` field in `date_part` SQL function ([7fdf4ac](https://github.com/cube-js/cube.js/commit/7fdf4acf6ce60387d3fa716c572e1611a77c205b))

# [0.30.0](https://github.com/cube-js/cube.js/compare/v0.29.57...v0.30.0) (2022-05-11)

### Features

- **cubesql:** Support dynamic key in ArrayIndex expression ([#4504](https://github.com/cube-js/cube.js/issues/4504)) ([115dd55](https://github.com/cube-js/cube.js/commit/115dd55ed390b8617d592add832b1aefde636265))
- **streamlined-config:** CUBEJS_EXTERNAL_DEFAULT and CUBEJS_SCHEDULED_REFRESH_DEFAULT defaults changed to "true" ([#4367](https://github.com/cube-js/cube.js/issues/4367)) ([d52adaf](https://github.com/cube-js/cube.js/commit/d52adaf9d7e95d9892348c8a2fbc971c4652dae3))

## [0.29.57](https://github.com/cube-js/cube.js/compare/v0.29.56...v0.29.57) (2022-05-11)

### Bug Fixes

- **cubesql:** Fix format_type udf usage with tables ([a49b2b4](https://github.com/cube-js/cube.js/commit/a49b2b44c10e4da42443cfd948404d2bc60671ec))
- **cubesql:** Reject `SELECT INTO` queries gracefully ([8b67ff7](https://github.com/cube-js/cube.js/commit/8b67ff7d0de1c5ca0b3852a342931d348ec2422c))
- **drivers:** Fixes error when data result is empty for AthenaDriver ([1e7e203](https://github.com/cube-js/cube.js/commit/1e7e203c63ecccae3fb199afd515850cd834d074))

### Features

- persistence access mode ([#4420](https://github.com/cube-js/cube.js/issues/4420)) ([d479e98](https://github.com/cube-js/cube.js/commit/d479e9836ef20989936574dfed9ee7ca7c7c78c3))

## [0.29.56](https://github.com/cube-js/cube.js/compare/v0.29.55...v0.29.56) (2022-05-06)

### Bug Fixes

- **docker:** Pack redshift driver for dev docker image, fix [#4497](https://github.com/cube-js/cube.js/issues/4497) ([c80fffc](https://github.com/cube-js/cube.js/commit/c80fffced22dd621a71adda088d374d52799d90c))

### Features

- **cubesql:** Correct support for regclass in CAST expr ([#4499](https://github.com/cube-js/cube.js/issues/4499)) ([cdab58a](https://github.com/cube-js/cube.js/commit/cdab58abeb4251c45e0365ff2a8584c9094f6d4d))
- **cubesql:** More descriptive error messages ([812db77](https://github.com/cube-js/cube.js/commit/812db772a651e0df1f7bc0d1dba97192c65ea834))
- **cubesql:** Partial support for Tableau's table_cat query ([#4466](https://github.com/cube-js/cube.js/issues/4466)) ([f1956d3](https://github.com/cube-js/cube.js/commit/f1956d3240bf067e1ecbee0997303ae76ab3fcaa))
- **cubesql:** Support pg_catalog.pg_enum postgres table ([2db445a](https://github.com/cube-js/cube.js/commit/2db445a120832390dd2577192597e30768b29918))
- **cubesql:** Support pg_get_constraintdef UDF ([#4487](https://github.com/cube-js/cube.js/issues/4487)) ([7a3018d](https://github.com/cube-js/cube.js/commit/7a3018d24326124b5e9257264a11fa09bc565f57))
- **cubesql:** Support pg_type_is_visible postgres udf ([47fc285](https://github.com/cube-js/cube.js/commit/47fc285c07c9633bee2482ef246f5436dd79dff3))
- **cubestore:** Wires ParquetMetadataCache ([#4297](https://github.com/cube-js/cube.js/issues/4297)) ([e20f0e7](https://github.com/cube-js/cube.js/commit/e20f0e78435c97a2b3cbaa7cb9c035d63f05ee18))

## [0.29.55](https://github.com/cube-js/cube.js/compare/v0.29.54...v0.29.55) (2022-05-04)

### Bug Fixes

- **cubesql:** Correct handling for boolean type ([cff6c8b](https://github.com/cube-js/cube.js/commit/cff6c8b4d69c9b8bade7ce1e6e2f2502a44f3918))
- **cubesql:** Tableau new regclass query fast fix ([2a7ff1e](https://github.com/cube-js/cube.js/commit/2a7ff1e20fc79dccd9cff94e6225d657569ed06e))

### Features

- **cubesql:** Tableau cubes without count measure support ([931e2f5](https://github.com/cube-js/cube.js/commit/931e2f5fb5fa29b19347b7858a8b4f892162f169))

## [0.29.54](https://github.com/cube-js/cube.js/compare/v0.29.53...v0.29.54) (2022-05-03)

### Bug Fixes

- Disable UPX compressing for Cube Store arm64-linux-gnu, fix [#4474](https://github.com/cube-js/cube.js/issues/4474) ([#4476](https://github.com/cube-js/cube.js/issues/4476)) ([3cb8586](https://github.com/cube-js/cube.js/commit/3cb85867c76087e646cb323c21e788ffa58ade15))
- Make dimensions and measures nullable fields for GraphQL API ([#4477](https://github.com/cube-js/cube.js/issues/4477)) Thanks @MarkLyck ! ([84e298e](https://github.com/cube-js/cube.js/commit/84e298ed06d6aa68735eecc61a311484eb9cf65b)), closes [#4399](https://github.com/cube-js/cube.js/issues/4399)
- **cubesql:** Using same alias on column yields Option.unwrap() panic ([a674c5f](https://github.com/cube-js/cube.js/commit/a674c5f98f8c643ed407fcf1cac528c797c43746))
- Prestodb timestamp cast to work in prestodb and athena ([#4419](https://github.com/cube-js/cube.js/issues/4419)) Thanks [@apzeb](https://github.com/apzeb) ! ([8f8f61a](https://github.com/cube-js/cube.js/commit/8f8f61a7186aeb63876bfcb8d440a7c35e27e0b3)), closes [#4221](https://github.com/cube-js/cube.js/issues/4221)

### Features

- **cubejs:** rollupJoin between multiple databases ([#4371](https://github.com/cube-js/cube.js/issues/4371)) ([6cd77d5](https://github.com/cube-js/cube.js/commit/6cd77d542ec8af570f556a4cefd1710ab2e5f508))
- **cubesql:** Tableau boolean filters support ([33aa5f1](https://github.com/cube-js/cube.js/commit/33aa5f138b44ccf60afc6e562b9bf71c2fe6257c))
- **cubesql:** Tableau cast projection queries support ([71ec644](https://github.com/cube-js/cube.js/commit/71ec64444e182a0a1c92818d655b40f78e463684))
- **cubesql:** Tableau contains support ([71dcad0](https://github.com/cube-js/cube.js/commit/71dcad091dc8e60958c717bd01e07db050abf8af))
- **cubesql:** Tableau min max number dimension support ([2abe13e](https://github.com/cube-js/cube.js/commit/2abe13e3155ad03ec3837da38bd465fbee0eb2f9))
- **cubesql:** Tableau not null filter support ([d48d0e0](https://github.com/cube-js/cube.js/commit/d48d0e03d05559413ddcff0ce980f6cf96cd24bc))
- **cubesql:** Tableau week support ([6d987ea](https://github.com/cube-js/cube.js/commit/6d987ea6062a90843084b72b254d068f46e26601))
- Detailed client TS types ([#4446](https://github.com/cube-js/cube.js/issues/4446)) Thanks [@reify-thomas-smith](https://github.com/reify-thomas-smith) ! ([977cce0](https://github.com/cube-js/cube.js/commit/977cce0c440bc73c0e6b5ad0c10af926b7386873)), closes [#4202](https://github.com/cube-js/cube.js/issues/4202)

## [0.29.53](https://github.com/cube-js/cube.js/compare/v0.29.52...v0.29.53) (2022-04-29)

### Bug Fixes

- **@cubejs-client/core:** Correct LogicalAndFilter/LogicalOrFilter types: allow any filter types in and / or ([#4343](https://github.com/cube-js/cube.js/issues/4343)) Thanks [@tchell](https://github.com/tchell) ! ([699a2f4](https://github.com/cube-js/cube.js/commit/699a2f45910785fb62d4abbeffff35b0b9708dd5))
- **@cubejs-client/core:** fix HTTP poll not working if Cube API stops and recovers ([#3506](https://github.com/cube-js/cube.js/issues/3506)) Thanks [@rongfengliang](https://github.com/rongfengliang) ! ([c207c3c](https://github.com/cube-js/cube.js/commit/c207c3c9e22242e8a4c6e01a2f60d10949a75366))
- **cubejs-playground:** fix history header scrolling in graphql explorer ([#4410](https://github.com/cube-js/cube.js/issues/4410)) ([a8b82b5](https://github.com/cube-js/cube.js/commit/a8b82b547c7f86069e62cb7d0173ccf4bf2dc766))
- **cubesql:** fix pg_constraint confkey type ([#4462](https://github.com/cube-js/cube.js/issues/4462)) ([82c25fd](https://github.com/cube-js/cube.js/commit/82c25fd98961a4130607cd1b93049d9b6f3093e7))
- **cubestore:** Index selection for different permutations of columns in a group by omits sorted indexes ([#4455](https://github.com/cube-js/cube.js/issues/4455)) ([fb31edd](https://github.com/cube-js/cube.js/commit/fb31edd7ed86c9b3ef6a5e1391d47c670bd6560e))
- **playground:** change the url on query change ([5c59298](https://github.com/cube-js/cube.js/commit/5c592988fa0e85a592b907a5b32f1b223c38d07d))

### Features

- **@cubejs-client/core:** Accept immutable queries ([#4366](https://github.com/cube-js/cube.js/issues/4366)) Thanks [@reify-thomas-smith](https://github.com/reify-thomas-smith)! ([19b1514](https://github.com/cube-js/cube.js/commit/19b1514d75cc47e0f081dd02e8de0a34aed118bb)), closes [#4160](https://github.com/cube-js/cube.js/issues/4160)
- **client-core:** Add HTTP status code to RequestError ([#4412](https://github.com/cube-js/cube.js/issues/4412)) ([6ec4fdf](https://github.com/cube-js/cube.js/commit/6ec4fdf6921db90bd64cb29f466fa1680f3b7eb4))
- **client-vue:** boolean filters support ([#4314](https://github.com/cube-js/cube.js/issues/4314)) ([8a3bb3d](https://github.com/cube-js/cube.js/commit/8a3bb3dc8e427c30d2ac0cdd504662087dcf1e19))
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
- **packages:** Materialize driver ([#4320](https://github.com/cube-js/cube.js/issues/4320)) ([d40d13b](https://github.com/cube-js/cube.js/commit/d40d13b0a80cd65b27337e87a523314af585dbc6))
- **playground:** display error stack traces ([#4438](https://github.com/cube-js/cube.js/issues/4438)) ([0932cda](https://github.com/cube-js/cube.js/commit/0932cdad2a66caefd29d648ab63dd72f91239438))

## [0.29.52](https://github.com/cube-js/cube.js/compare/v0.29.51...v0.29.52) (2022-04-23)

**Note:** Version bump only for package cubejs

## [0.29.51](https://github.com/cube-js/cube.js/compare/v0.29.50...v0.29.51) (2022-04-22)

### Bug Fixes

- **cubesql:** Bool encoding for text format in pg-wire ([7faf34b](https://github.com/cube-js/cube.js/commit/7faf34b4dee421202528aa2e9985acbfcc8da6b9))
- **cubesql:** current_schema() UDF ([69a75dc](https://github.com/cube-js/cube.js/commit/69a75dc3fe29be97eecf2f0eeb97a642a2328212))
- **cubesql:** Proper handling for Postgresql table reference ([35f5635](https://github.com/cube-js/cube.js/commit/35f56350f39f22665e71fa53a1e6fc5d7bb02262))
- **playground:** chart templates ([#4426](https://github.com/cube-js/cube.js/issues/4426)) ([ebc83a9](https://github.com/cube-js/cube.js/commit/ebc83a9ef9080ba20560fe5718b7b9fb11ec95fd))

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
- **databricks-driver:** Export bucket support for S3/Azure ([#4430](https://github.com/cube-js/cube.js/issues/4430)) ([4512126](https://github.com/cube-js/cube.js/commit/4512126346fd2650fdcba3936ed1eb129b34af9d))
- **query-language:** "startsWith", "endsWith" filters support ([#4128](https://github.com/cube-js/cube.js/issues/4128)) ([e8c72d6](https://github.com/cube-js/cube.js/commit/e8c72d630eecd930a8fd36fc52f9b594a45d59c0))

## [0.29.50](https://github.com/cube-js/cube.js/compare/v0.29.49...v0.29.50) (2022-04-18)

### Bug Fixes

- **playground:** match rollup button ([#4401](https://github.com/cube-js/cube.js/issues/4401)) ([18e90bb](https://github.com/cube-js/cube.js/commit/18e90bbf9c76911fe9a9323a499afa1c429951a3))

### Features

- **cubesql:** Initial support for Binary format in pg-wire ([a36845c](https://github.com/cube-js/cube.js/commit/a36845c5edcb6bd77172de2cebcd67a700df5224))
- **cubesql:** Support Describe(Portal) for pg-wire ([34cf111](https://github.com/cube-js/cube.js/commit/34cf111249c3fede986c3633fe8d5f0cade3ed91))
- **cubesql:** Support pg_depend postgres table ([ceb35d4](https://github.com/cube-js/cube.js/commit/ceb35d4825cd2ac76ad191eef950ab3be126c3de))

## [0.29.49](https://github.com/cube-js/cube.js/compare/v0.29.48...v0.29.49) (2022-04-15)

### Bug Fixes

- **athena:** Support plain bucket for CUBEJS_DB_EXPORT_BUCKET ([#4390](https://github.com/cube-js/cube.js/issues/4390)) ([4b4dd60](https://github.com/cube-js/cube.js/commit/4b4dd60256dcbd36d049fb02078459a7873f1ecd))
- **mssql:** Add uniqueidentifier type support -- Custom type 'uniqueidentifier' is not supported ([#4386](https://github.com/cube-js/cube.js/issues/4386)) Thanks [@jdeksup](https://github.com/jdeksup)! ([fb77332](https://github.com/cube-js/cube.js/commit/fb773325a7d2e8fa3c923a8c332e275e178b49c6))

## [0.29.48](https://github.com/cube-js/cube.js/compare/v0.29.47...v0.29.48) (2022-04-14)

### Bug Fixes

- **cubesql:** Support pg_catalog.format_type through fully qualified name ([9eafae0](https://github.com/cube-js/cube.js/commit/9eafae0c4eafc2ad1d8517be9dbf292c1650c64a))
- **cubestore:** Empty CUBESTORE_S3_SUB_PATH leads to can't list file error ([#4324](https://github.com/cube-js/cube.js/issues/4324)) ([0b35064](https://github.com/cube-js/cube.js/commit/0b350645bd17e1d2ae7ac7a65a30478a2ba53eb1))
- **cubestore:** Inactive partition compaction: replace error with warn ([#4337](https://github.com/cube-js/cube.js/issues/4337)) ([2ad61ee](https://github.com/cube-js/cube.js/commit/2ad61ee0b956d0d71774b8b304ae24a361c8c5cf))
- **cubestore:** Support a space separated binary strings for HyperLogLog fields in csv -- Can't parse column value for countDistinctApprox in Athena ([#4383](https://github.com/cube-js/cube.js/issues/4383)) ([8b320b6](https://github.com/cube-js/cube.js/commit/8b320b6fb124d5670677775d413cf71a0d9e6ff5))

### Features

- **cubesql:** Initial support for prepared statements in pg-wire ([#4244](https://github.com/cube-js/cube.js/issues/4244)) ([912b52a](https://github.com/cube-js/cube.js/commit/912b52a5cb8d72820c68843e15a2ef83233b952f))
- **query-language:** "total" flag support ([#4134](https://github.com/cube-js/cube.js/issues/4134)) ([51aef5e](https://github.com/cube-js/cube.js/commit/51aef5ede6e9b0c0e0e8749119e98102f168b8ca))
- Support Compound Primary Keys ([#4370](https://github.com/cube-js/cube.js/issues/4370)) Thanks [@rccoe](https://github.com/rccoe)! ([0e3983c](https://github.com/cube-js/cube.js/commit/0e3983ccb30a3f6815ba5ee28aa09807e55fd2a3)), closes [#4364](https://github.com/cube-js/cube.js/issues/4364)
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

**Note:** Version bump only for package cubejs

## [0.29.43](https://github.com/cube-js/cube.js/compare/v0.29.42...v0.29.43) (2022-04-07)

### Bug Fixes

- **cubesql:** Rewrites don't respect projection column order ([cfe35a7](https://github.com/cube-js/cube.js/commit/cfe35a7b65390db43f1e7c68ac54c82c2ec8af49))
- **cubestore:** Filters aren't included in index choosing ([c582622](https://github.com/cube-js/cube.js/commit/c5826221f24dbd45cd863263a65fe2c90a45b337))

### Features

- **cubesql:** Rewrite engine error handling ([3fba823](https://github.com/cube-js/cube.js/commit/3fba823bc561d7a985c89c4cf437a6595ef88a7c))
- **cubesql:** Upgrade rust to 1.61.0-nightly (2022-02-22) ([c836065](https://github.com/cube-js/cube.js/commit/c8360658ccb8e5e3e6cfcd62da2d156b44ee8456))
- **cubestore:** Add CUBEJS_GH_API_TOKEN env var to download script ([#4282](https://github.com/cube-js/cube.js/issues/4282)) Thanks [@icebob](https://github.com/icebob)! ([ac57162](https://github.com/cube-js/cube.js/commit/ac571623ed39bfc02c73d5d6bfc01a7e2fa2fa74))
- **cubestore:** Explain implementation ([#4303](https://github.com/cube-js/cube.js/issues/4303)) ([53fe3f3](https://github.com/cube-js/cube.js/commit/53fe3f325cc47a6f8c53ad2582a9608f4ae1555d))

## [0.29.42](https://github.com/cube-js/cube.js/compare/v0.29.41...v0.29.42) (2022-04-04)

### Bug Fixes

- **@cubejs-backend/snowflake-driver:** Download numbers with 0 scale as integers in pre-aggregations ([42eb582](https://github.com/cube-js/cube.js/commit/42eb582908080d4e9e2856c09290950957230fcd))
- **cubesql:** Allow quoted variables with SHOW <variable> syntax ([#4313](https://github.com/cube-js/cube.js/issues/4313)) ([3eece0e](https://github.com/cube-js/cube.js/commit/3eece0e70817b2b72406b146a95a5757cdfb994c))
- **playground:** adding a query parameter in Playground requires one extra click to type in search field ([#4295](https://github.com/cube-js/cube.js/issues/4295)) ([4abab96](https://github.com/cube-js/cube.js/commit/4abab964ce05afe5583832958205eff75c1417b0))
- **playground:** rollup designer count distinct warning ([#4309](https://github.com/cube-js/cube.js/issues/4309)) ([add2dd3](https://github.com/cube-js/cube.js/commit/add2dd3106f7fd9deb3ea88fa74467ccd1b9244f))
- **playground:** transparency under the searchbar in some cases ([#4301](https://github.com/cube-js/cube.js/issues/4301)) ([6cdb20c](https://github.com/cube-js/cube.js/commit/6cdb20cd5a7e36559327f389221b8568c2f68bb0))

### Features

- **cubesql:** Rewrite engine segments support ([48b0767](https://github.com/cube-js/cube.js/commit/48b0767aa880a2373d6a6fa15b2e0a1815bb1865))

## [0.29.41](https://github.com/cube-js/cube.js/compare/v0.29.40...v0.29.41) (2022-04-03)

### Bug Fixes

- **jdbc-driver:** Handle mvn error for empty java dependencies ([#4307](https://github.com/cube-js/cube.js/issues/4307)) ([9e12511](https://github.com/cube-js/cube.js/commit/9e12511e29655d855846781b90db6fd2e43ddff4))

## [0.29.40](https://github.com/cube-js/cube.js/compare/v0.29.39...v0.29.40) (2022-04-03)

### Bug Fixes

- **cubesql:** Table columns should take precedence over projection to mimic MySQL and Postgres behavior ([60d6e45](https://github.com/cube-js/cube.js/commit/60d6e4511267b132df204fe7b637953d81e5f980))

### Features

- **cubestore:** IN operator support for decimal type ([#4306](https://github.com/cube-js/cube.js/issues/4306)) ([d728163](https://github.com/cube-js/cube.js/commit/d728163ce907e3643d6ba268f7fac7b4abe6e9fa))

## [0.29.39](https://github.com/cube-js/cube.js/compare/v0.29.38...v0.29.39) (2022-04-01)

### Bug Fixes

- **native:** Post installation issue with npm (avoid bundledDependencies) ([6163838](https://github.com/cube-js/cube.js/commit/6163838926586cf1a17bea353cb23b61e48469ca))

## [0.29.38](https://github.com/cube-js/cube.js/compare/v0.29.37...v0.29.38) (2022-04-01)

### Bug Fixes

- **cubesql:** Deallocate statement on specific command in MySQL protocol ([ab3f36c](https://github.com/cube-js/cube.js/commit/ab3f36c182f603f348115463926e1cbe8ee40fd6))
- **cubesql:** Enable EXPLAIN support for postgres ([c0244d1](https://github.com/cube-js/cube.js/commit/c0244d10bd43647c6e6f562be3e30ec3d1ae66d9))
- **cubestore:** Add compaction and chunk writing guards to cleanup uploads folder on any failures to minimize storage bloating ([b6e79fa](https://github.com/cube-js/cube.js/commit/b6e79fad0e820c9e0d1c6edf23837973b7b8b4ac))
- **native:** Post installation issue with npm ([#4302](https://github.com/cube-js/cube.js/issues/4302)) ([8aa9c71](https://github.com/cube-js/cube.js/commit/8aa9c71b1ae473579b6d438aeebdac28a7fd3d19))

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
- **cubesql:** Rewrite engine: support for stacked time series charts ([c1add2c](https://github.com/cube-js/cube.js/commit/c1add2c9c52d1cd884dfefe4db978a853a76c83e))
- packages/cubejs-bigquery-driver/package.json to reduce vulnerabilities ([#3924](https://github.com/cube-js/cube.js/issues/3924)) ([e44ca40](https://github.com/cube-js/cube.js/commit/e44ca402acf41ab30cd66d8fe36dd160b1062600))
- **cubesql:** Rewrite engine can't parse `db` prefixed table names ([b7d9382](https://github.com/cube-js/cube.js/commit/b7d93827750b8d72e871abd527f1c0a649e5e6c2))

### Features

- **@cubejs-backend/schema-compiler:** Allow filtering with or/and in FILTER_PARAMS ([#4253](https://github.com/cube-js/cube.js/issues/4253)) Thanks [@tchell](https://github.com/tchell)! ([cb1446c](https://github.com/cube-js/cube.js/commit/cb1446c4e788bcb43bdc90fc0a48ffc49fd0d891))
- **cubesql:** Global Meta Tables ([88db9ea](https://github.com/cube-js/cube.js/commit/88db9eab3854a89cd93cfdce3a9fad9a180f3b45))
- **cubesql:** Global Meta Tables - add tests ([42e9517](https://github.com/cube-js/cube.js/commit/42e9517d1ac1673622bb9b03352af94f8ec968ba))
- **cubesql:** Global Meta Tables - cargo fmt ([c8336d9](https://github.com/cube-js/cube.js/commit/c8336d92a7867fd2b78e1542d61b42519fa9e3f2))
- **cubesql:** Support pg_catalog.pg_range table ([625c03a](https://github.com/cube-js/cube.js/commit/625c03ae965b2730d924812a7d16aec3fbdf5369))
- Introduce `CUBEJS_ALLOW_UNGROUPED_WITHOUT_PRIMARY_KEY` env ([#2941](https://github.com/cube-js/cube.js/issues/2941)) Thanks [@vignesh-123](https://github.com/vignesh-123) ! ([fa829bf](https://github.com/cube-js/cube.js/commit/fa829bf6446279e37c5aee75ceb914d274cba974))

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
- **query-orchestrator:** Pin es5-ext version ([#4257](https://github.com/cube-js/cube.js/issues/4257)) ([4a17e9c](https://github.com/cube-js/cube.js/commit/4a17e9c1f18a2c1f0fa826caa8d196a45bb323f5))

### Features

- **cubesql:** Split variables to session / server for MySQL ([#4255](https://github.com/cube-js/cube.js/issues/4255)) ([f78b539](https://github.com/cube-js/cube.js/commit/f78b5396e217d9fdf6cf970bd837f767c5b8a2f5))

## [0.29.34](https://github.com/cube-js/cube.js/compare/v0.29.33...v0.29.34) (2022-03-21)

### Bug Fixes

- **cubesql:** Disable MySQL specific functions/statements for pg-wire protocol ([#4222](https://github.com/cube-js/cube.js/issues/4222)) ([21f6cde](https://github.com/cube-js/cube.js/commit/21f6cde31537e515daedd7266e958e7b259f0ace))

### Features

- **cubesql:** Correct response for SslRequest in pg-wire ([#4238](https://github.com/cube-js/cube.js/issues/4238)) ([bd1468a](https://github.com/cube-js/cube.js/commit/bd1468aa0a5851c9bcddb81dfd0d1da5c080972f))
- **server:** Allow to configure allowNodeRequire for DataSchemaCompiler ([#4235](https://github.com/cube-js/cube.js/issues/4235)) ([64b8ff6](https://github.com/cube-js/cube.js/commit/64b8ff66a84827b160dccd204eeba6b6d2031923))

## [0.29.33](https://github.com/cube-js/cube.js/compare/v0.29.32...v0.29.33) (2022-03-17)

### Bug Fixes

- **cubesql:** Add numeric_scale field for information_schema.columns ([2e2877a](https://github.com/cube-js/cube.js/commit/2e2877ab8a1d144f529661481b7fc6ddef7d3c85))
- **helm-charts:** add missing config for sqlAPI ([#4191](https://github.com/cube-js/cube.js/issues/4191)) ([7a7c5da](https://github.com/cube-js/cube.js/commit/7a7c5da9206d7d64532b1479dc476b751da4b96c))
- **playground:** rollup designer query compatibility ([#4224](https://github.com/cube-js/cube.js/issues/4224)) ([000e28d](https://github.com/cube-js/cube.js/commit/000e28d1ab44bbb3d79ac60727b3e654e9167a03))
- TypeError: Cannot create proxy with a non-object as target or handler /cubejs-api/v1/run-scheduled-refresh request without auth params ([a04f13f](https://github.com/cube-js/cube.js/commit/a04f13f91c3ce2b7168a1eff4067df068ea320ae))

### Features

- **cubejs-api-gateway:** add dbType to load req success event for db usage analytics ([fc23028](https://github.com/cube-js/cube.js/commit/fc230285f86917b6174d383a201c5b56dd710d44))
- **cubesql:** Enable PostgresServer via env variable ([39b6528](https://github.com/cube-js/cube.js/commit/39b6528d91b569bdae90362e1a693404a4eef958))
- **cubesql:** Initial support for pg-wire protocol ([1b87c8c](https://github.com/cube-js/cube.js/commit/1b87c8cc67055ab0be0c208505d2bd50b7abffc8))
- **cubesql:** Support meta layer and dialect for Postgres service ([#4215](https://github.com/cube-js/cube.js/issues/4215)) ([46af90d](https://github.com/cube-js/cube.js/commit/46af90d6d41d147b33f9e9eed24e830857243967))
- **cubesql:** Support PLAIN authentication method to pg-wire ([#4229](https://github.com/cube-js/cube.js/issues/4229)) ([c4fbd8c](https://github.com/cube-js/cube.js/commit/c4fbd8c9f12ffed396754712f912868f147c697a))
- **cubesql:** Support SHOW processlist ([0194098](https://github.com/cube-js/cube.js/commit/0194098af10e77c84ef141dc372f3abc46b3b514))
- **playground:** non-additive measures message ([#4236](https://github.com/cube-js/cube.js/issues/4236)) ([ae18bbc](https://github.com/cube-js/cube.js/commit/ae18bbcb9030d0eef03c74410c25902602ec6d43))

## [0.29.32](https://github.com/cube-js/cube.js/compare/v0.29.31...v0.29.32) (2022-03-10)

### Bug Fixes

- **@cubejs-backend/dbt-schema-extension:** dbt metric types are not p… ([#4188](https://github.com/cube-js/cube.js/issues/4188)) ([30179f7](https://github.com/cube-js/cube.js/commit/30179f7bbe029a62638cfb12056fa3ae63bd647b))

### Features

- **cubesql:** Support information_schema.processlist ([#4185](https://github.com/cube-js/cube.js/issues/4185)) ([4179fb0](https://github.com/cube-js/cube.js/commit/4179fb006104275ba0d7074d681cd937efb0a8fc))

## [0.29.31](https://github.com/cube-js/cube.js/compare/v0.29.30...v0.29.31) (2022-03-09)

### Bug Fixes

- **@cubejs-backend/dbt-schema-extension:** Identifiers are not properly escaped in case for Dbt cloud ([be28d61](https://github.com/cube-js/cube.js/commit/be28d61f09b3de8e1960e46646f88c85ece4bf56))
- **athena:** Fixes export bucket location. Fixes column order. ([#4183](https://github.com/cube-js/cube.js/issues/4183)) ([abd40a7](https://github.com/cube-js/cube.js/commit/abd40a79e360cd9a9eceeb56a450102bd782f3d9))
- allow post requests for sql ([#4180](https://github.com/cube-js/cube.js/issues/4180)) ([2d101ee](https://github.com/cube-js/cube.js/commit/2d101ee46dfdd4ab6a5540671983af510b4a716b))

## [0.29.30](https://github.com/cube-js/cube.js/compare/v0.29.29...v0.29.30) (2022-03-04)

### Bug Fixes

- **@cubejs-backend/cubestore-driver:** Empty tables in Cube Store if index is being used ([#4170](https://github.com/cube-js/cube.js/issues/4170)) ([2585c12](https://github.com/cube-js/cube.js/commit/2585c124f5ba3bc843e19a7f8177c8dbb35ad1cc))
- **api-gateway:** GraphQL cannot read property findIndex of undefined ([51d48f1](https://github.com/cube-js/cube.js/commit/51d48f1754c10f5c953b71f7807773abc9652d72))

## [0.29.29](https://github.com/cube-js/cube.js/compare/v0.29.28...v0.29.29) (2022-03-03)

### Bug Fixes

- **@cubejs-backend/schema-compiler:** Add strictness to booleans ([#4157](https://github.com/cube-js/cube.js/issues/4157)) Thanks [@zpencerq](https://github.com/zpencerq)! ([e918837](https://github.com/cube-js/cube.js/commit/e918837ec8c5eb7965620f43408a92f4c2b2bec5))
- Timestamp for quarter range in time series has incorrect ending period of 23:59:99 ([#4162](https://github.com/cube-js/cube.js/issues/4162)) Thanks @Yashkochar20 ! ([8e27ae7](https://github.com/cube-js/cube.js/commit/8e27ae73bcb4c8524690122779f2d9e5f1ba7c12))
- **@cubejs-backend/bigquery-driver:** Cancel queries on polling timeout so those aren't dangling around for hours ([f36ff74](https://github.com/cube-js/cube.js/commit/f36ff74e52099839f6ea796bf4571100cdc332cf))
- **cubestore:** Add list file size validation after file upload to check upload consistency ([#4093](https://github.com/cube-js/cube.js/issues/4093)) ([1c62859](https://github.com/cube-js/cube.js/commit/1c62859747b2773b418b756178e03e685381ca82))
- **cubestore:** Deactivate tables on data corruption to allow refresh worker to reconcile failing partitions ([#4092](https://github.com/cube-js/cube.js/issues/4092)) ([2c3c83a](https://github.com/cube-js/cube.js/commit/2c3c83a97c2dfd6e6f6d5dcd3acc4afe5fda294f))
- **cubestore:** Do not spawn select workers for router nodes ([8c07bba](https://github.com/cube-js/cube.js/commit/8c07bbab4efdd3a3d929726870193401f1d42479))
- **cubestore:** Do not warmup chunks on table creation to avoid stuck on warmup of unpartitioned chunks situation ([4c27d51](https://github.com/cube-js/cube.js/commit/4c27d51a0e01b26cd7d9447454a45da25e9ce4a7))
- **cubestore:** Jobs are fetched only once 5 seconds if there's a queue ([dee115f](https://github.com/cube-js/cube.js/commit/dee115f39375ebae478dfa023c6e33e511b310b9))
- **cubestore:** Leading decimal zeros are truncated during formatting ([a97f34b](https://github.com/cube-js/cube.js/commit/a97f34b0adb088223234ae187192e6be2b483cd4))
- **cubestore:** Postpone deletion of partitions and chunks after metastore log commits to avoid missing files on sudden metastore loss ([#4094](https://github.com/cube-js/cube.js/issues/4094)) ([493c53e](https://github.com/cube-js/cube.js/commit/493c53e97f225c086524e59297b10c5a8ef4646b))
- **helm-charts:** correct s3 output location env variable ([#4098](https://github.com/cube-js/cube.js/issues/4098)) ([88db58d](https://github.com/cube-js/cube.js/commit/88db58d1997e70b907fd03dfbe7e34e838f979ab))
- **playground:** auto size chart window ([#4042](https://github.com/cube-js/cube.js/issues/4042)) ([ee496b3](https://github.com/cube-js/cube.js/commit/ee496b33306bb2fafb3635add53d85ae05c7620f))
- **playground:** prevent params to shift around when removing filters ([e3d17ae](https://github.com/cube-js/cube.js/commit/e3d17ae7b2b8340ee39ea6464773b2676dc5a9f6))

### Features

- **cubestore:** Decimal partition pruning ([#4089](https://github.com/cube-js/cube.js/issues/4089)) ([c00efad](https://github.com/cube-js/cube.js/commit/c00efadfa3a841bd9bb5707fd5e98904ca9112bc))
- **cubestore:** Introduce CUBESTORE_EVENT_LOOP_WORKER_THREADS to allow set tokio worker threads explicitly ([9349a11](https://github.com/cube-js/cube.js/commit/9349a112a795787b749f88e4179cfc8ae56575e1))
- **cubestore:** Repartition single chunks instead of partition as a whole to speed up ingestion of big tables ([#4125](https://github.com/cube-js/cube.js/issues/4125)) ([af65cdd](https://github.com/cube-js/cube.js/commit/af65cddb0728c6d101bc076b7af88db1b684cc9e))
- **packages:** add QuestDB driver ([#4096](https://github.com/cube-js/cube.js/issues/4096)) ([de8823b](https://github.com/cube-js/cube.js/commit/de8823b524d372dd1d9b661e643d9d4664260b58))
- Compact JSON array based response data format support ([#4046](https://github.com/cube-js/cube.js/issues/4046)) ([e74d73c](https://github.com/cube-js/cube.js/commit/e74d73c140f56e71a24c35a5f03e9af63022bced)), closes [#1](https://github.com/cube-js/cube.js/issues/1)
- Unwinds CubeStore select worker panics to provide descriptive error messages ([#4097](https://github.com/cube-js/cube.js/issues/4097)) ([6e21434](https://github.com/cube-js/cube.js/commit/6e214345fe12d55534174d80a05a18597ffdd17a))

## [0.29.28](https://github.com/cube-js/cube.js/compare/v0.29.27...v0.29.28) (2022-02-10)

### Bug Fixes

- **@cubejs-backend/athena-driver:** Batching and export support ([#4039](https://github.com/cube-js/cube.js/issues/4039)) ([108f42a](https://github.com/cube-js/cube.js/commit/108f42afdd58ae0027b1b81730f7ca9e72ab9122))
- **cubesql:** Allow to pass measure as an argument in COUNT function ([#4063](https://github.com/cube-js/cube.js/issues/4063)) ([c48c7ea](https://github.com/cube-js/cube.js/commit/c48c7ea1c86a64463a84a9ffc1c06aa605c6331c))

## [0.29.27](https://github.com/cube-js/cube.js/compare/v0.29.26...v0.29.27) (2022-02-09)

### Bug Fixes

- **cubesql:** Unique filtering for measures/dimensions/segments in Request ([552c87b](https://github.com/cube-js/cube.js/commit/552c87bf38479133e2c8dac20ac1c29eb034c762))
- **cubestore:** Ensure file size matching during downloads to localize any remote fs consistency issues ([#4054](https://github.com/cube-js/cube.js/issues/4054)) ([38fdf35](https://github.com/cube-js/cube.js/commit/38fdf3514eb5420bd6176dfae9cb4b6aa9ec6b5a))
- **cubestore:** Schema type mismatch when in memory chunks are queried ([#4024](https://github.com/cube-js/cube.js/issues/4024)) ([614809b](https://github.com/cube-js/cube.js/commit/614809b7db2e15ec65b671752da7a27474abf8b7))

### Features

- **cubesql:** Move execution to Query Engine ([2d84b6b](https://github.com/cube-js/cube.js/commit/2d84b6b98fc03d84f858bd152f2359232e9ea8ed))

## [0.29.26](https://github.com/cube-js/cube.js/compare/v0.29.25...v0.29.26) (2022-02-07)

### Bug Fixes

- **cubejs-playground:** tab close button styles ([#4047](https://github.com/cube-js/cube.js/issues/4047)) ([a6307e9](https://github.com/cube-js/cube.js/commit/a6307e935e9effbe831958b35a088e28cbba05fc))
- Use prototype name matching instead of classes to allow exact version mismatches for AbstractExtension ([75545e8](https://github.com/cube-js/cube.js/commit/75545e8ffbf88fd693e4c8c4afd78d86830925b2))
- **@cubejs-client/ngx:** cubejs.watch() not producing errors ([#3974](https://github.com/cube-js/cube.js/issues/3974)) Thanks @PieterVanZyl-Dev! ([1ee6740](https://github.com/cube-js/cube.js/commit/1ee6740abb51b84296ae65ee565114269b621b65)), closes [#3961](https://github.com/cube-js/cube.js/issues/3961)
- **cubesql:** Ignore case sensitive search for usage of identifiers ([a50f8a2](https://github.com/cube-js/cube.js/commit/a50f8a25e8064f98eb7931c643d2ce67be340ad0))

### Features

- **cubesql:** Support information_schema.COLLATIONS table ([#4018](https://github.com/cube-js/cube.js/issues/4018)) ([262314d](https://github.com/cube-js/cube.js/commit/262314dd939b57851c264f038e4f032d8b98bab8))
- **cubesql:** Support prepared statements in MySQL protocol ([#4005](https://github.com/cube-js/cube.js/issues/4005)) ([6b2f61c](https://github.com/cube-js/cube.js/commit/6b2f61cafbcf4758bba1d16a344871a84d0767f3))
- **cubesql:** Support SHOW COLLATION ([#4025](https://github.com/cube-js/cube.js/issues/4025)) ([95b5d0e](https://github.com/cube-js/cube.js/commit/95b5d0ee8af9054c64e5dac50a89db7bb6d8a5fc))

## [0.29.25](https://github.com/cube-js/cube.js/compare/v0.29.24...v0.29.25) (2022-02-03)

### Bug Fixes

- Out of memory in case of empty table has been used to build partitioned pre-aggregation ([#4021](https://github.com/cube-js/cube.js/issues/4021)) ([314cc3c](https://github.com/cube-js/cube.js/commit/314cc3c3f47d6ba9282a1bb969c2e27bdfb58a57))
- **@cubejs-bacend/api-gateway:** fix a type issue where queries did not properly support logical and and logical or operators ([#4016](https://github.com/cube-js/cube.js/issues/4016)) Thanks [@rdwoodring](https://github.com/rdwoodring)! ([bb2d230](https://github.com/cube-js/cube.js/commit/bb2d230f0428e58e636dbde1caa5a3c08989f268))
- **cubestore:** Decimals without integral part are ignoring sign during to_string() ([b02b1a6](https://github.com/cube-js/cube.js/commit/b02b1a6b9ffe101869e22e4e65065f017f929a30))

### Features

- **@cubejs-backend/snowflake-driver:** CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY env variable support ([38f4840](https://github.com/cube-js/cube.js/commit/38f484010f56ad44193147188f6993d6d1906ebe))
- Load metrics from DBT project ([#4000](https://github.com/cube-js/cube.js/issues/4000)) ([2975d84](https://github.com/cube-js/cube.js/commit/2975d84cd2a2d3bba3c31a7744ab5a5fb3789b6e))
- **cubestore:** Support quarter granularity for date_trunc fn ([#4011](https://github.com/cube-js/cube.js/issues/4011)) ([404482d](https://github.com/cube-js/cube.js/commit/404482def1f6ea7324d329a1943e6c8270518203))

## [0.29.24](https://github.com/cube-js/cube.js/compare/v0.29.23...v0.29.24) (2022-02-01)

### Bug Fixes

- Correct error message on missing partitions ([e953296](https://github.com/cube-js/cube.js/commit/e953296ff4258023a601ea5d5ab91dc8cda4ff14))
- Remove orphaned tables on error while pre-aggregation creation ([#3996](https://github.com/cube-js/cube.js/issues/3996)) ([0548435](https://github.com/cube-js/cube.js/commit/054843533b2421d87874fa25adf3a94892f24bd4))
- **cubesql:** Ignore @@ global prefix for system defined variables ([80caef0](https://github.com/cube-js/cube.js/commit/80caef0f2eb145a4405d8edcb4d650179b22c593))

### Features

- **cubesql:** Support binary expression for measures ([#4009](https://github.com/cube-js/cube.js/issues/4009)) ([475a614](https://github.com/cube-js/cube.js/commit/475a6148aa8d87183e7680888d27737c0290e401))
- **cubesql:** Support COUNT(1) ([#4004](https://github.com/cube-js/cube.js/issues/4004)) ([df33d89](https://github.com/cube-js/cube.js/commit/df33d89b1a19c452b1a97b49e640c4ed1a53e1ad))
- **cubesql:** Support SHOW COLUMNS ([#3995](https://github.com/cube-js/cube.js/issues/3995)) ([bbf7e6c](https://github.com/cube-js/cube.js/commit/bbf7e6c232d9c91ccd9421f01f4fdda07ef82998))
- **cubesql:** Support SHOW TABLES via QE ([#4001](https://github.com/cube-js/cube.js/issues/4001)) ([bac2aaa](https://github.com/cube-js/cube.js/commit/bac2aaae130c0863790b2178884a157dcdf0c55d))
- **cubesql:** Support USE 'db' (success reply) ([bd945fb](https://github.com/cube-js/cube.js/commit/bd945fbc12a9250a90f240127ad1ac9910011a01))

## [0.29.23](https://github.com/cube-js/cube.js/compare/v0.29.22...v0.29.23) (2022-01-26)

### Bug Fixes

- Cannot read property ‘last_updated_at’ of undefined ([#3980](https://github.com/cube-js/cube.js/issues/3980)) ([74d75e7](https://github.com/cube-js/cube.js/commit/74d75e743eb0549eea443e84d7278e7b8f78e6af))
- Provide more readable message for CSV parsing error ([0b9a3f8](https://github.com/cube-js/cube.js/commit/0b9a3f897a88dce2ad4387990bd64e9e06624839))
- **@cubejs-client/core:** restore support for Angular by removing dependency on `@cubejs-client/dx` ([#3972](https://github.com/cube-js/cube.js/issues/3972)) ([13d30dc](https://github.com/cube-js/cube.js/commit/13d30dc98a08c6ef93808adaf1be6c2aa10c664a))
- **client-core:** apiToken nullable check ([3f93f68](https://github.com/cube-js/cube.js/commit/3f93f68170ff87a50bd6bbf768e1cd36c478c20c))
- Error: column does not exist during in case of subQuery for rolling window measure ([6084407](https://github.com/cube-js/cube.js/commit/6084407cb7cad3f0d239959b072f4fa011aa29a4))

### Features

- **cubesql:** Setup more system variables ([97fe231](https://github.com/cube-js/cube.js/commit/97fe231b36d1d2497e0a913b2ae35f3f41f98e53))

## [0.29.22](https://github.com/cube-js/cube.js/compare/v0.29.21...v0.29.22) (2022-01-21)

### Bug Fixes

- **postgres-driver:** Support enums as UD types for columns, fix [#3946](https://github.com/cube-js/cube.js/issues/3946) ([#3957](https://github.com/cube-js/cube.js/issues/3957)) ([842cc53](https://github.com/cube-js/cube.js/commit/842cc53d59a323b56a575f697ddccacbf7b3a632))

### Features

- **cubesql:** Execute SHOW VARIABLES [LIKE 'pattern'] via QE instead of hardcoding ([#3960](https://github.com/cube-js/cube.js/issues/3960)) ([48c0d77](https://github.com/cube-js/cube.js/commit/48c0d774b8c206dee3f2280fadcbeb832d695dc9))

## [0.29.21](https://github.com/cube-js/cube.js/compare/v0.29.20...v0.29.21) (2022-01-17)

### Bug Fixes

- **playground:** rollup designer undefined key ([3213e70](https://github.com/cube-js/cube.js/commit/3213e70947c0d1568de804ad3f6c7d515e81b56f))

### Features

- **@cubejs-backend/schema-compiler:** extend the schema generation API ([#3936](https://github.com/cube-js/cube.js/issues/3936)) ([48b2335](https://github.com/cube-js/cube.js/commit/48b2335c7d9810dc433fd8c76f4b3ec8a7b83442))
- **cubestore:** Bump Clang to 12 ([8a16102](https://github.com/cube-js/cube.js/commit/8a161023a183447a45dabc59cc256fc01322ff45))
- **cubestore:** Use OpenSSL 1.1.1l ([1e18bec](https://github.com/cube-js/cube.js/commit/1e18bec92be6756139387b0e9fef17c7c2cd388d))
- Surfaces lastUpdateAt for used preaggregation tables and lastRefreshTime for queries using preaggreagation tables ([#3890](https://github.com/cube-js/cube.js/issues/3890)) ([f4ae73d](https://github.com/cube-js/cube.js/commit/f4ae73d9d4fd6b84483c99e8bdd1f9588efde5a2)), closes [#3540](https://github.com/cube-js/cube.js/issues/3540)
- **cubesql:** Improve error messages ([#3829](https://github.com/cube-js/cube.js/issues/3829)) ([8293e52](https://github.com/cube-js/cube.js/commit/8293e52a4a509e8559949d8af6446ef8a04e33f5))

## [0.29.20](https://github.com/cube-js/cube.js/compare/v0.29.19...v0.29.20) (2022-01-10)

### Bug Fixes

- **cubesql:** Alias binding problem with escapes (<expr> as '') ([8b2c002](https://github.com/cube-js/cube.js/commit/8b2c002537e5151b51328e041828153ac77bf231))

## [0.29.19](https://github.com/cube-js/cube.js/compare/v0.29.18...v0.29.19) (2022-01-09)

**Note:** Version bump only for package cubejs

## [0.29.18](https://github.com/cube-js/cube.js/compare/v0.29.17...v0.29.18) (2022-01-09)

### Bug Fixes

- Try to fix Zalgo bug https://github.com/Marak/colors.js/issues/285 ([796c404](https://github.com/cube-js/cube.js/commit/796c4040be5f37d39b519b33fa36c61e3c1b0707))
- **schema-compiler:** Handle null Values in Security Context ([#3868](https://github.com/cube-js/cube.js/issues/3868)) Thanks [@joealden](https://github.com/joealden) ! ([739367b](https://github.com/cube-js/cube.js/commit/739367bd50c1863ee8bae17741c0591065ea47a1))

## [0.29.17](https://github.com/cube-js/cube.js/compare/v0.29.16...v0.29.17) (2022-01-05)

### Bug Fixes

- Do not instantiate SqlParser if rewriteQueries is false to save cache memory ([00a239f](https://github.com/cube-js/cube.js/commit/00a239fae57ae9448337bd816b2ae5ca17f15230))
- TypeError: Cannot read property 'map' of undefined\n at /cube/node_modules/@cubejs-backend/server-core/src/core/RefreshScheduler.ts:349:86\n ([ca33fe4](https://github.com/cube-js/cube.js/commit/ca33fe4ad6f6c829d5af2c7915057c68b0b58c43))
- **cubejs-cli:** use `process.cwd()` in `typegen` to resolve current project's `node_modules/` ([daceca7](https://github.com/cube-js/cube.js/commit/daceca7bdcc1be3aa6d0f87fd3f27e9409cc4199))

## [0.29.16](https://github.com/cube-js/cube.js/compare/v0.29.15...v0.29.16) (2022-01-05)

### Bug Fixes

- **@cubejs-backend/snowflake-driver:** OperationFailedError: SQL compilation error: invalid value [?] for parameter 'STATEMENT_TIMEOUT_IN_SECONDS' ([4a1503e](https://github.com/cube-js/cube.js/commit/4a1503e3b5fe5bdaa5a6af989c26b62a39d24058))
- `refreshKey` is evaluated ten times more frequently if `sql` and `every` are simultaneously defined ([#3873](https://github.com/cube-js/cube.js/issues/3873)) ([c93ae12](https://github.com/cube-js/cube.js/commit/c93ae127b4d0ce2c214d3665f8d86f6ade5cce6f))

## [0.29.15](https://github.com/cube-js/cube.js/compare/v0.29.14...v0.29.15) (2021-12-30)

### Bug Fixes

- **docker:** remove cache from alpine image ([a52be2f](https://github.com/cube-js/cube.js/commit/a52be2feca4a7b78e791a3bd7d78ab74db55c53f))

### Features

- **cubestore:** Mark linux GNU as supported on ARM64 (post installer) ([3b385e5](https://github.com/cube-js/cube.js/commit/3b385e54d9f193559e5416a61349c707b40f5653))
- **native/cubesql:** Build for ARM64 linux GNU ([5351c41](https://github.com/cube-js/cube.js/commit/5351c41110d1940956b242e85b879db6f3622d21))
- Introduce single unified CUBEJS_DB_QUERY_TIMEOUT env variable to set all various variables that control database query timeouts ([#3864](https://github.com/cube-js/cube.js/issues/3864)) ([33c6292](https://github.com/cube-js/cube.js/commit/33c6292059e65e293a7e3d61e1f1e0c1413eeece))

## [0.29.14](https://github.com/cube-js/cube.js/compare/v0.29.12-1...v0.29.14) (2021-12-29)

**Note:** Version bump only for package cubejs

## [0.29.13](https://github.com/cube-js/cube.js/compare/v0.29.12...v0.29.13) (2021-12-29)

### Bug Fixes

- arm64 support for cubestore docker images ([#3849](https://github.com/cube-js/cube.js/issues/3849)) ([305c234](https://github.com/cube-js/cube.js/commit/305c2348f5fd6a9c9f4b724b58c7723fc313c890))

## [0.29.12](https://github.com/cube-js/cube.js/compare/v0.29.11...v0.29.12) (2021-12-29)

### Bug Fixes

- **cubesql/native:** Return errors to the client (missing field ) ([82b22e4](https://github.com/cube-js/cube.js/commit/82b22e48c6fc4d61f62bf43795d5b50ba8a3fd67))
- **cubestore:** Do not fail scheduler loop on lagged broadcast receive ([11a2a67](https://github.com/cube-js/cube.js/commit/11a2a67bc733a76ab6a01ca1a4c3853e5d15ea4e))

### Features

- Split batching upload of pre-aggregations into multiple files to enhance performance and avoid load balancer upload limits ([#3857](https://github.com/cube-js/cube.js/issues/3857)) ([6f71419](https://github.com/cube-js/cube.js/commit/6f71419b1c921a0d4e39231370b181d390b01f3d))
- **cubesql:** Ignore KILL statement without error ([20590f3](https://github.com/cube-js/cube.js/commit/20590f39bc1931f5b23b14d81aa48562e373c95b))

## [0.29.11](https://github.com/cube-js/cube.js/compare/v0.29.10...v0.29.11) (2021-12-24)

### Bug Fixes

- **cubestore:** Respect pending chunks for compaction ([ac23554](https://github.com/cube-js/cube.js/commit/ac235545a0516a27009035b72defaa338a2fc5d3))

### Features

- **cubestore:** Build binary for aarch64-unknown-linux-gnu ([#3844](https://github.com/cube-js/cube.js/issues/3844)) ([38b8054](https://github.com/cube-js/cube.js/commit/38b8054308353bb11a023d6c47b05761e7bc7535))
- **cubestore:** Support docker image for AMR64 platform ([#3841](https://github.com/cube-js/cube.js/issues/3841)) ([6514fdc](https://github.com/cube-js/cube.js/commit/6514fdcbf4d202ce4ccdfa2d0f6f8f2f2cd00718))

## [0.29.10](https://github.com/cube-js/cube.js/compare/v0.29.9...v0.29.10) (2021-12-22)

### Bug Fixes

- **cubestore:** Do not show errors for not uploaded chunks scheduled for removal ([ca94fb2](https://github.com/cube-js/cube.js/commit/ca94fb284d9d70753099a448b59b60690a229d93))
- **cubestore:** Re-parent chunks on compaction instead of repartitioning ([cb6b9d5](https://github.com/cube-js/cube.js/commit/cb6b9d536182ff7843ecf7726db886fb3f90511c))

## [0.29.9](https://github.com/cube-js/cube.js/compare/v0.29.8...v0.29.9) (2021-12-22)

### Bug Fixes

- TypeError: Cannot read property 'filter' of undefined on pre-aggregations cache only load ([eddecdf](https://github.com/cube-js/cube.js/commit/eddecdf7f3c8e7a70c84a75d2a1815c6da8db780))

### Features

- **cubestore:** Introduce table partition_split_threshold to allow large scale partitions ([#3837](https://github.com/cube-js/cube.js/issues/3837)) ([2bdebce](https://github.com/cube-js/cube.js/commit/2bdebced3b4c0901216557dc5729f8be1b739854))

## [0.29.8](https://github.com/cube-js/cube.js/compare/v0.29.7...v0.29.8) (2021-12-21)

### Bug Fixes

- **@cubejs-client/core:** Add 'meta' field to typescript TCubeMember type [#3682](https://github.com/cube-js/cube.js/issues/3682) ([#3815](https://github.com/cube-js/cube.js/issues/3815)) Thanks @System-Glitch! ([578c0a7](https://github.com/cube-js/cube.js/commit/578c0a75ec2830f48b5d6156370f9343b2fd8b6b))
- **@cubejs-client/vue:** fix error when executing deletion ([#3806](https://github.com/cube-js/cube.js/issues/3806)) Thanks [@18207680061](https://github.com/18207680061)! ([9d220a8](https://github.com/cube-js/cube.js/commit/9d220a8cf3bf040b51efefb9f74beb5545a89035))
- **cubestore:** Reduce excessive startup memory usage ([4f0dfc8](https://github.com/cube-js/cube.js/commit/4f0dfc816edd41f8c1986e7665a1f318b8fe4b70))

### Features

- **cubesql:** Improve selection finder for ORDER BY ([d28897b](https://github.com/cube-js/cube.js/commit/d28897b67d233f04f2d0d22adf28f191b3320ebf))
- **cubesql:** Introduce information_schema.key_column_usage ([922b6e2](https://github.com/cube-js/cube.js/commit/922b6e2641198499eb46467085f5d32a3e4a65f6))
- **cubesql:** Introduce information_schema.referential_constraints ([cdfdcd7](https://github.com/cube-js/cube.js/commit/cdfdcd771d124fbc97358895d78c2d9770abab5c))
- **cubesql:** Introduce information_schema.schemata ([3035231](https://github.com/cube-js/cube.js/commit/303523185c65c6cdc60e1a0cb5cabf00fd2315b9))
- **cubesql:** Rewrite general planner to pass restrictions for QE ([28e127b](https://github.com/cube-js/cube.js/commit/28e127bc6f2ec6ccb24e9c5942b2b97a2593c12f))

## [0.29.7](https://github.com/cube-js/cube.js/compare/v0.29.6...v0.29.7) (2021-12-20)

### Bug Fixes

- Table cache incorrectly invalidated after merge multi-tenant queues by default change ([#3828](https://github.com/cube-js/cube.js/issues/3828)) ([540446a](https://github.com/cube-js/cube.js/commit/540446a76e19be3aec38b96ad81c149f567e9e40))
- **cubejs-cli:** make `typegen` command throw useful errors ([04358be](https://github.com/cube-js/cube.js/commit/04358bef41d4185483875c02a3054781d26c00a7))

## [0.29.6](https://github.com/cube-js/cube.js/compare/v0.29.5...v0.29.6) (2021-12-19)

### Bug Fixes

- refresh process for `useOriginalSqlPreAggregations` is broken ([#3826](https://github.com/cube-js/cube.js/issues/3826)) ([f0bf070](https://github.com/cube-js/cube.js/commit/f0bf070462b6b69c76a5c34ee97fc5a5ec8fab15))

## [0.29.5](https://github.com/cube-js/cube.js/compare/v0.29.4...v0.29.5) (2021-12-17)

### Features

- **@cubejs-client/dx:** introduce new dependency for Cube.js Client ([5bfaf1c](https://github.com/cube-js/cube.js/commit/5bfaf1cf99d68dfcdddb04f2b3151ad451657ff9))
- **cubejs-cli:** generate TypeScript types from API for use by `@cubejs-client/core` ([b97d9ca](https://github.com/cube-js/cube.js/commit/b97d9ca7f1d611c73fdd4ef3ff8ebfc77bc8aff7))
- **cubesql:** Support CompoundIdentifier in compiling ([030c981](https://github.com/cube-js/cube.js/commit/030c98150a228c2c5e80c2530266509e864ed3c9))
- **cubesql:** Support DATE with compound identifier ([fa959d8](https://github.com/cube-js/cube.js/commit/fa959d89406ab84d6764e0cc035b819b2f7dae21))
- **cubesql:** Support DATE, DATE_ADD, NOW fuunctions & Intervals ([a71340c](https://github.com/cube-js/cube.js/commit/a71340c56d58377eb384a02a252bd3064c74595f))
- **cubesql:** Support hours interval ([b2d4b53](https://github.com/cube-js/cube.js/commit/b2d4b53642abbd587cd207f6f29021c2fdb74a57))

## [0.29.4](https://github.com/cube-js/cube.js/compare/v0.29.3...v0.29.4) (2021-12-16)

### Bug Fixes

- Validate `contextToAppId` is in place when COMPILE_CONTEXT is used ([54a8b84](https://github.com/cube-js/cube.js/commit/54a8b843eca965c6a098118da04ec480ea06fa76))
- **cubejs-playground:** responsive filter group size ([5129cca](https://github.com/cube-js/cube.js/commit/5129cca0883394ea247e706d7bfc6cfd81f315de))
- **cubejs-playground:** responsive filter group size \* 2 ([2e340df](https://github.com/cube-js/cube.js/commit/2e340df88583013133b05e6337972dee2f1a5c15))
- **cubesql:** IF function, support array & scalar ([1b04ad1](https://github.com/cube-js/cube.js/commit/1b04ad1b0873a689414edbfcce5f6436d651f55e))
- **cubesql:** LIKE '%(%)%' ([c75efaa](https://github.com/cube-js/cube.js/commit/c75efaa18566b8e4bf8e2448c9f9066ef2dc815c))
- **cubesql:** Substr with negative count should return empty string (not an error) ([197b9e5](https://github.com/cube-js/cube.js/commit/197b9e5edbdabfc8167ef55d643240b6a597dad4))

### Features

- **cubesql:** Support LOCATE function ([9692ae3](https://github.com/cube-js/cube.js/commit/9692ae3fa741c600e095a727c726353236c40aa7))
- **cubesql:** Support SUBSTRING with commans syntax ([ffb0a6b](https://github.com/cube-js/cube.js/commit/ffb0a6b321cfdc767ff6b0f7b93313cd3aee5c42))
- **cubesql:** Support UCASE function ([8853ec6](https://github.com/cube-js/cube.js/commit/8853ec6a194a43bf71f2e28344d80637c798f8ed))

## [0.29.3](https://github.com/cube-js/cube.js/compare/v0.29.2...v0.29.3) (2021-12-15)

### Bug Fixes

- **api-gateway:** skip GraphQL types generation for empty cubes ([28f3c40](https://github.com/cube-js/cube.js/commit/28f3c40bd6fa23dc33b083dda3f8d66d1f5dacc0))
- **playground:** use playground token when security context provided ([74e22e3](https://github.com/cube-js/cube.js/commit/74e22e3aa6355106bf51bd8f9f08ffdeec8d5f9b))

## [0.29.2](https://github.com/cube-js/cube.js/compare/v0.29.1...v0.29.2) (2021-12-15)

### Bug Fixes

- **playground:** respect security context token in GraphQL sandbox ([3d4a18b](https://github.com/cube-js/cube.js/commit/3d4a18b720c2c298834ee41055244a0af6292ce2))

## [0.29.1](https://github.com/cube-js/cube.js/compare/v0.29.0...v0.29.1) (2021-12-15)

### Bug Fixes

- bump min supported GraphQL version ([4490fe2](https://github.com/cube-js/cube.js/commit/4490fe22952aa41d93bab2a3568d48a38e927d6e))

# [0.29.0](https://github.com/cube-js/cube.js/compare/v0.28.67...v0.29.0) (2021-12-14)

### Features

- Reuse queue by default for multi-tenancy instead of creating it per tenant ([f2a2fb9](https://github.com/cube-js/cube.js/commit/f2a2fb9a092f61aa110da4cd44db69857bc35391))
- **playground:** GraphiQL sandbox. Allow using the cube GraphQL API ([#3810](https://github.com/cube-js/cube.js/issues/3810)) ([1f39042](https://github.com/cube-js/cube.js/commit/1f39042ac063feaa852b3674a1b857d2dbbc9d17))

### Reverts

- Revert "BREAKING CHANGE: 0.29 (#3809)" (#3811) ([db005ed](https://github.com/cube-js/cube.js/commit/db005edc04d48e8251250ab9d0e19f496cf3b52b)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809) [#3811](https://github.com/cube-js/cube.js/issues/3811)

- BREAKING CHANGE: 0.29 (#3809) ([6f1418b](https://github.com/cube-js/cube.js/commit/6f1418b9963774844f341682e594601a56bb0084)), closes [#3809](https://github.com/cube-js/cube.js/issues/3809)

### Bug Fixes

- **client-core:** nullish measure values ([#3664](https://github.com/cube-js/cube.js/issues/3664)) ([f28f803](https://github.com/cube-js/cube.js/commit/f28f803521c9020ce639f68612143c2e962975ea))

### chore

- angular 12 ([#2863](https://github.com/cube-js/cube.js/issues/2863)) ([18efb65](https://github.com/cube-js/cube.js/commit/18efb65b3acbbd7da00ae02967d13070e9a33a90))

### BREAKING CHANGES

- Before this change querying queue was created for each tenant
- Drop support for Node.js 10 (12.x is a minimal version)
- Upgrade Node.js to 14 for Docker images
- Drop support for Node.js 15
- drop Angular 10/11 support
- **client-core:** All undefined/null measure values will now be converted to 0

## [0.28.67](https://github.com/cube-js/cube.js/compare/v0.28.66...v0.28.67) (2021-12-14)

### Bug Fixes

- **playground:** Revert GraphQL support ([#3807](https://github.com/cube-js/cube.js/issues/3807)) ([6eb894b](https://github.com/cube-js/cube.js/commit/6eb894b856eaa918a66bf537e88d90bb6ae9f71e))

## [0.28.66](https://github.com/cube-js/cube.js/compare/v0.28.65...v0.28.66) (2021-12-14)

### Bug Fixes

- **client-vue:** add wrapWithQueryRenderer prop ([#3801](https://github.com/cube-js/cube.js/issues/3801)) ([c211e0a](https://github.com/cube-js/cube.js/commit/c211e0a0b1d260167128edf07b725c905f0dc63e))
- **cubestore:** Cleanup non active never written partitions as part of delete middle man ([#3802](https://github.com/cube-js/cube.js/issues/3802)) ([7b31c2f](https://github.com/cube-js/cube.js/commit/7b31c2fc47e568b9ad2bdd2e891202d413c7d795))
- **cubestore:** Drop created but not written partitions during reconciliation ([98326f1](https://github.com/cube-js/cube.js/commit/98326f118f33f95dc926e5c7277fa9522e636475))
- **cubestore:** GCTask queue holds a lot of delete middle man partition jobs so it looks like a memory leak ([d3ffb07](https://github.com/cube-js/cube.js/commit/d3ffb077a3b70d4843771bebedd5659d47f115f3))
- **cubestore:** Limit chunk count in a single repartition hop to avoid repartition timeouts ([67ca6c8](https://github.com/cube-js/cube.js/commit/67ca6c81a838848dfec411f704d0883dc9971cf5))

### Features

- **api-gateway:** Event property for SQL and GraphQL API ([#3774](https://github.com/cube-js/cube.js/issues/3774)) ([0e83a72](https://github.com/cube-js/cube.js/commit/0e83a72064698a1c8cf3609b562bcbc29f7df5ba))
- **playground:** GraphiQL sandbox. Allow using the cube GraphQL API ([#3803](https://github.com/cube-js/cube.js/issues/3803)) ([6c848c0](https://github.com/cube-js/cube.js/commit/6c848c02fae2f2658e2051055d4bf8419554fb26))

## [0.28.65](https://github.com/cube-js/cube.js/compare/v0.28.64...v0.28.65) (2021-12-10)

### Bug Fixes

- **api-gateway:** GraphQL boolean filters ([#3776](https://github.com/cube-js/cube.js/issues/3776)) ([285b371](https://github.com/cube-js/cube.js/commit/285b37157af064483f36e9f7806e12f98b3a5de7))
- **cubesql:** Special NULL handling for LEAST function ([edb4b02](https://github.com/cube-js/cube.js/commit/edb4b02fe7de249f4d107b5556e3afef433a8e82))
- **cubestore:** Introduce network protocol hand shakes to avoid corrupted messages deserialization ([aac0a5b](https://github.com/cube-js/cube.js/commit/aac0a5b1a89ffb8607460b48cfbdc6be16c0b199))
- **graphql-api:** Remove irrelevant float filters ([#3778](https://github.com/cube-js/cube.js/issues/3778)) ([3147fe4](https://github.com/cube-js/cube.js/commit/3147fe4e642404b999063a5e45340fa4094f367e))

### Features

- **api-gateway:** Enable GraphiQL header editor to pass security context in dev mode ([#3779](https://github.com/cube-js/cube.js/issues/3779)) ([8c335a2](https://github.com/cube-js/cube.js/commit/8c335a2bdf65446ed3ab3ac00c09452751e60a03))
- **api-gateway:** GraphQL root orderBy ([#3788](https://github.com/cube-js/cube.js/issues/3788)) ([cd88d26](https://github.com/cube-js/cube.js/commit/cd88d269a5c7ab12ec8e326718fdfd2935ab70bd))
- **cubesql:** Introduce convert_tz fn (stub) ([1f08272](https://github.com/cube-js/cube.js/commit/1f08272bca28a79c46e8b28fbe43847b276958fb))
- **cubesql:** Introduce support for least function ([434084e](https://github.com/cube-js/cube.js/commit/434084ed8ea427d5afd2a999951cfacadfbb40d2))
- **cubesql:** Introduce time_format fn (stub) ([9c9b217](https://github.com/cube-js/cube.js/commit/9c9b21728a7fe192f8452c4959482b473979f2f2))
- **cubesql:** Introduce timediff fn (stub) ([29dfb97](https://github.com/cube-js/cube.js/commit/29dfb9716298c5a579c0ffba6742e13a29325670))
- **cubesql:** Support compound identifier in ORDER BY ([6d08ba8](https://github.com/cube-js/cube.js/commit/6d08ba89216f8c856ef10dfe7ac9366c544eea3d))
- **cubesql:** Support performance_schema.session_variables & global_variables ([a807858](https://github.com/cube-js/cube.js/commit/a807858cdc3d55790be75165d8b1c14045229d80))
- **cubesql:** Support type coercion for IF function ([3b3f48c](https://github.com/cube-js/cube.js/commit/3b3f48c15166f6e172fa9b3bffe1ea6b5448bad4))
- **cubestore:** Sort NULLS LAST by default ([#3785](https://github.com/cube-js/cube.js/issues/3785)) ([02744e8](https://github.com/cube-js/cube.js/commit/02744e8c6d33363c6383aa0bf38733074c8e7d09))
- **helm-charts:** Add sqlPort config to enable SQL Api ([#3706](https://github.com/cube-js/cube.js/issues/3706)) ([8705893](https://github.com/cube-js/cube.js/commit/87058936d76ebe99bf31fafb65b168056a18a11a))
- **playground:** GraphiQL sandbox. Allow using the cube GraphQL API ([#3794](https://github.com/cube-js/cube.js/issues/3794)) ([ea9630f](https://github.com/cube-js/cube.js/commit/ea9630fad3894ed4357d188a2dd5ade0e474e0b0))

## [0.28.64](https://github.com/cube-js/cube.js/compare/v0.28.63...v0.28.64) (2021-12-05)

### Bug Fixes

- **cubestore:** Support `\N` as null value during CSV imports ([fbba787](https://github.com/cube-js/cube.js/commit/fbba7874ed7d572d7656818f7305bb9e31210559))

## [0.28.63](https://github.com/cube-js/cube.js/compare/v0.28.62...v0.28.63) (2021-12-03)

### Bug Fixes

- **cubesql:** Crash with WHEN ELSE IF ([7eeadf5](https://github.com/cube-js/cube.js/commit/7eeadf54433c3cbb8f5f501cacbd84d2766be52b))
- **cubesql:** Information_schema.COLUMNS - correct DATA_TYPE fields ([337d1d1](https://github.com/cube-js/cube.js/commit/337d1d1e74d52fa58685107c4217a0987e203cba))
- **cubesql:** Initial support for compound identifiers ([e95fdb6](https://github.com/cube-js/cube.js/commit/e95fdb69760e999b0961b13471c464ea8489520c))

### Features

- **cubesql:** Implement IF function ([0e08399](https://github.com/cube-js/cube.js/commit/0e083999559293798ae7d2c45c635ef1120721de))
- **cubesql:** Implement INFORMATION_SCHEMA.COLUMNS (compatibility with MySQL) ([f37e625](https://github.com/cube-js/cube.js/commit/f37e625694e1ac2c7ba8441549dc2f0771c6c306))
- **cubesql:** Implement INFORMATION_SCHEMA.TABLES (compatibility with MySQL) ([ed0e774](https://github.com/cube-js/cube.js/commit/ed0e774d5868031cc05e8746c5b2377bfc5ea454))
- **cubesql:** Initial support for information_schema.statistics ([e478baa](https://github.com/cube-js/cube.js/commit/e478baad7d8ea340141ed868677eae3d2c09eb44))
- **cubesql:** WHERE 1 <> 1 LIMIT 0 - (metabase introspection) ([431b1e9](https://github.com/cube-js/cube.js/commit/431b1e9873bf08a2440af7cb8140be2bccc0ec00))

## [0.28.62](https://github.com/cube-js/cube.js/compare/v0.28.61...v0.28.62) (2021-12-02)

### Bug Fixes

- **playground:** rollup designer overflow ([60ce358](https://github.com/cube-js/cube.js/commit/60ce35887bad0614cc38b0e1d41725f283699a08))

### Features

- **cubesql:** Specify transaction_isolation, transaction_read_only ([81a8f2d](https://github.com/cube-js/cube.js/commit/81a8f2d7c791938f01b56572b757edf1630b724e))
- **cubesql:** Support ISNULL ([f0a4b62](https://github.com/cube-js/cube.js/commit/f0a4b62f4bd2a1ba2caf37c764b117b352a2f2b3))

## [0.28.61](https://github.com/cube-js/cube.js/compare/v0.28.60...v0.28.61) (2021-11-30)

### Bug Fixes

- **@cubejs-backend/mongobi-driver:** Create table failed: User: Can't parse timestamp: Invalid date ([1843f28](https://github.com/cube-js/cube.js/commit/1843f2851e61bd4e7f1831a8167db939ec239313))
- **cubesql:** Ignore SET NAMES on AST level ([495f245](https://github.com/cube-js/cube.js/commit/495f245b2652f7bf688c9f8d253c7cdf4b96edcc))
- **cubestore:** Internal: Execution error: Internal: Arrow error: Invalid argument error: number of columns(4) must match number of fields(5) in schema for streaming tables ([#3737](https://github.com/cube-js/cube.js/issues/3737)) ([d35cc1f](https://github.com/cube-js/cube.js/commit/d35cc1f2e55f89811e41774b56c2c7631083fa4b))
- **cubestore:** Support escaping sequence for ILIKE ([#3744](https://github.com/cube-js/cube.js/issues/3744)) ([fbe7376](https://github.com/cube-js/cube.js/commit/fbe73767852135e52bb61b794191b5e0c652c15f)), closes [#3742](https://github.com/cube-js/cube.js/issues/3742)
- Clarify pre-aggregation build error messages ([cf17f64](https://github.com/cube-js/cube.js/commit/cf17f64406d4068385ed611d102c4bc6ea979552)), closes [#3469](https://github.com/cube-js/cube.js/issues/3469)
- Generate consistent request ids for not annotated API requests ([331a819](https://github.com/cube-js/cube.js/commit/331a8198cd362c41dbfb0d07c3be27099522b467))

### Features

- **cubesql:** Return Status:SERVER_SESSION_STATE_CHANGED on SET operator ([6f7adf8](https://github.com/cube-js/cube.js/commit/6f7adf8abfc276386eff007f5fded08bfb0559da))
- **cubesql:** Skip SET {GLOBAL|SESSION} TRANSACTION isolation on AST level ([3afe2b1](https://github.com/cube-js/cube.js/commit/3afe2b1741f7d37a0976a57b3a905f4760f6833c))
- **cubesql:** Specify max_allowed_packet, auto_increment_increment ([dd4a22d](https://github.com/cube-js/cube.js/commit/dd4a22d713181df3e5862fd457bff32221f8eaa1))
- **cubesql:** Support specifying ColumnFlags in QE ([4170b27](https://github.com/cube-js/cube.js/commit/4170b273bb1f2743cf68af54ba365feb6ad81b7c))
- **cubestore:** Minio support ([#3738](https://github.com/cube-js/cube.js/issues/3738)) ([c857562](https://github.com/cube-js/cube.js/commit/c857562fdf1dac6644039fee646a3be1177591f1)), closes [#3510](https://github.com/cube-js/cube.js/issues/3510)
- **docker:** Upgrade node:12.22.7 ([b9e57d8](https://github.com/cube-js/cube.js/commit/b9e57d8ad60581f808c750fa89817ebdea588cb8))

## [0.28.60](https://github.com/cube-js/cube.js/compare/v0.28.59...v0.28.60) (2021-11-25)

### Bug Fixes

- **@cubejs-backend/mongobi-driver:** Show all tables if database isn't set ([6a55438](https://github.com/cube-js/cube.js/commit/6a554385c05371304755037ac17d189477164ffb))
- **@cubejs-backend/mongobi-driver:** Unsupported mapping for data type: 5 ([9d42c9c](https://github.com/cube-js/cube.js/commit/9d42c9c1b68243c0bce5444b91615fa5907b8ed3))
- **cubesql:** MySQL CLI connection error with COM_FIELD_LIST ([#3728](https://github.com/cube-js/cube.js/issues/3728)) ([aef1401](https://github.com/cube-js/cube.js/commit/aef14014fce87b8b47dc41ead066a7647f6fe225))
- **cubesql:** Pass selection for server variables to QE ([#3724](https://github.com/cube-js/cube.js/issues/3724)) ([4c66581](https://github.com/cube-js/cube.js/commit/4c66581c00e01c905f0d631aeeada43a4b75ad67))
- **docker:** Install cmake for dev images ([470678a](https://github.com/cube-js/cube.js/commit/470678a5fab09742c955c09279ce43e0b92f1d0e))
- **docker:** Reduce images size ([1bb00e3](https://github.com/cube-js/cube.js/commit/1bb00e3667e30b864365a4b421b22eb3d1caf977))
- **native:** Return promise for registerInterface ([be97a84](https://github.com/cube-js/cube.js/commit/be97a8433dd0d2e400734c6ab3d3110d59ad4d1a))
- Empty pre-aggregation with partitionGranularity throws ParserError("Expected identifier, found: )") ([#3714](https://github.com/cube-js/cube.js/issues/3714)) ([86c6aaf](https://github.com/cube-js/cube.js/commit/86c6aaf8001d16b78f3a3029903affae4a82f41d))

### Features

- **cubesql:** Enable unicode_expression (required for LEFT) ([4059a17](https://github.com/cube-js/cube.js/commit/4059a1785fece8293461323ac1de5940a8b9876d))
- **cubesql:** Support insrt function ([61bdc99](https://github.com/cube-js/cube.js/commit/61bdc991ef634b39c2684e3923f63cb83f5b598c))
- **native:** Support Node.js 17 ([91f5d51](https://github.com/cube-js/cube.js/commit/91f5d517d59067fc5e643219c2f4f83ec915259c))

## [0.28.59](https://github.com/cube-js/cube.js/compare/v0.28.58...v0.28.59) (2021-11-21)

### Bug Fixes

- Dropping orphaned tables logging messages don't have necessary t… ([#3710](https://github.com/cube-js/cube.js/issues/3710)) ([1962e0f](https://github.com/cube-js/cube.js/commit/1962e0fe6ce999879d1225cef27a24ea8cb8490a))
- **cubestore:** Ensure strict meta store consistency with single thre… ([#3696](https://github.com/cube-js/cube.js/issues/3696)) ([135bc3f](https://github.com/cube-js/cube.js/commit/135bc3feeb3db8dc0547954a50d1c3ec27195d40))
- **cubestore:** unexpected value Int(42) for type String ([e6eab32](https://github.com/cube-js/cube.js/commit/e6eab32495be386bed80d73545a960a75322e0da))

### Features

- **cubesql:** Strict check for dates in binary expr ([3919c42](https://github.com/cube-js/cube.js/commit/3919c4229b3983592cc3ae3b7d3c6b7e966ef3ad))

## [0.28.58](https://github.com/cube-js/cube.js/compare/v0.28.57...v0.28.58) (2021-11-18)

### Bug Fixes

- **@cubejs-backend/clickhouse-driver:** clickhouse joins full key query aggregate fails ([#3600](https://github.com/cube-js/cube.js/issues/3600)) Thanks [@antnmxmv](https://github.com/antnmxmv)! ([c6451cd](https://github.com/cube-js/cube.js/commit/c6451cdef8498d4d645167ed3da7cf2f599a2e5b)), closes [#3534](https://github.com/cube-js/cube.js/issues/3534)
- **cubesql:** Parsing error with single comment line ([a7697c1](https://github.com/cube-js/cube.js/commit/a7697c1751a5e0003fa85ed83be033d6db651d7b))
- **cubestore:** Error during planning: The field has has qualifier for UNION query with IN ([#3697](https://github.com/cube-js/cube.js/issues/3697)) ([01a6d6f](https://github.com/cube-js/cube.js/commit/01a6d6f121e7800a70c055c0e616eda03b642267)), closes [#3693](https://github.com/cube-js/cube.js/issues/3693)

### Features

- **cubesql:** Casting to date on compare with time dimension ([8bc7b26](https://github.com/cube-js/cube.js/commit/8bc7b26833b38d200e4ea6bb9bf717cfec6da9db))
- **cubesql:** Strict check for dates in BETWEEN operator ([329da03](https://github.com/cube-js/cube.js/commit/329da03e9c17018d1c8356b888f1ade33150eca0))
- **cubesql:** Support DATE_TRUNC(string, column) ([#3677](https://github.com/cube-js/cube.js/issues/3677)) ([e4e9b4e](https://github.com/cube-js/cube.js/commit/e4e9b4e6de3de6e59322e3a78e1a26573d7b84dd))
- **gateway:** GraphQL endpoint ([#3555](https://github.com/cube-js/cube.js/issues/3555)) Thanks [@lvauvillier](https://github.com/lvauvillier)! ([ed85192](https://github.com/cube-js/cube.js/commit/ed85192e1af5a35b5e4b05a777e02b140f25b32d)), closes [#3433](https://github.com/cube-js/cube.js/issues/3433)

## [0.28.57](https://github.com/cube-js/cube.js/compare/v0.28.56...v0.28.57) (2021-11-16)

### Bug Fixes

- **cubesql:** Support identifier escaping in h/m/s granularaties ([1641b69](https://github.com/cube-js/cube.js/commit/1641b698ef106489e93804bdcf364e863d7ce072))

### Features

- **cubesql:** Initial support for INFORMATION_SCHEMA ([d1fac9e](https://github.com/cube-js/cube.js/commit/d1fac9e75cb01cbf6a1207b6e69a999e9d755d1e))
- **cubesql:** Support schema() ([3af3c84](https://github.com/cube-js/cube.js/commit/3af3c841f3cf4beb6950c83a12c86fe2320cd0bc))
- **cubesql:** Support SHOW WARNINGS ([73d91c0](https://github.com/cube-js/cube.js/commit/73d91c0f6db1d1b7d0945b15cc93cb349b26f573))
- **cubesql:** Support USER(), CURRENT_USER() ([8a848aa](https://github.com/cube-js/cube.js/commit/8a848aa872fc5d34456b3ef73e72480c9c0914c0))

## [0.28.56](https://github.com/cube-js/cube.js/compare/v0.28.55...v0.28.56) (2021-11-14)

### Bug Fixes

- **cubestore:** Drop not ready tables 30 minutes after creation to avoid metastore bloating ([e775682](https://github.com/cube-js/cube.js/commit/e775682464402e194be5cf1e22eba6880747644f))
- **cubestore:** Invalidate tables cache only on table changing operations to reduce write lock contention ([28549b8](https://github.com/cube-js/cube.js/commit/28549b8906a7da446d409a9550d414ea3afe7025))
- **cubestore:** Replace all_rows access with scans to optimize allocations ([ab985c8](https://github.com/cube-js/cube.js/commit/ab985c89b16e9bb5786be8224d444c58819288d9))

## [0.28.55](https://github.com/cube-js/cube.js/compare/v0.28.54...v0.28.55) (2021-11-12)

### Bug Fixes

- **cubestore:** fix float comparisons causing unsorted data in merges ([c5b5d2c](https://github.com/cube-js/cube.js/commit/c5b5d2c2936f8974cbea6fe083ba38ad6caed793))
- **cubestore:** Timeout for create table finalization ([d715345](https://github.com/cube-js/cube.js/commit/d7153456a7c2e6d5cae035d83ea55ba488455cb7))
- JSON.stringify schema version instead of calling .toString() to simplify usage with objects ([50a191e](https://github.com/cube-js/cube.js/commit/50a191ecbc9df970ccb44126f5ad743bda0868e0))
- **cubestore:** Do not fail `swap_chunks` with assert -- allow to gracefully capture error ([8c8b6eb](https://github.com/cube-js/cube.js/commit/8c8b6ebe328c609ba782495415d2ff3562fe31f8))
- **cubestore:** Do not fail on repartition of empty chunks ([41b3054](https://github.com/cube-js/cube.js/commit/41b30549b9c3ec2cca0899f1f778c8334168b94b))
- **cubestore:** speed up HLL++ merges, up to 180x in some cases ([24ecbc3](https://github.com/cube-js/cube.js/commit/24ecbc38462071fa0acb465261471e868bf6e1c4))
- **cubestore:** system.tables can affect table visibility cache by including non ready tables. Ignore delete middle man reconciliation errors. ([dce711f](https://github.com/cube-js/cube.js/commit/dce711ff73460ee8ef9893ef4e6cc17273f55f7b))

### Features

- Introduce checkSqlAuth (auth hook for SQL API) ([3191b73](https://github.com/cube-js/cube.js/commit/3191b73816cd63d242349041c54a7037e9027c1a))

## [0.28.54](https://github.com/cube-js/cube.js/compare/v0.28.53...v0.28.54) (2021-11-09)

### Bug Fixes

- **@cubejs-backend/snowflake-driver:** Do not trigger cluster start on test connection ([3bb2d8f](https://github.com/cube-js/cube.js/commit/3bb2d8f466987402a9af6e5c0a7e6667846b1d19))
- **cubestore:** Introduce file suffixes to avoid parquet write clashes in case of meta store recovery ([#3639](https://github.com/cube-js/cube.js/issues/3639)) ([4d01e8b](https://github.com/cube-js/cube.js/commit/4d01e8b99b9cc5496d3e6f587b3502efe0fc4584))
- **cubestore:** Row with id is not found for PartitionRocksTable. Repartition not active partitions on reconcile. Do not drop orphaned jobs that weren't scheduled. Repartition only limited amount of rows during single repartition job. ([#3636](https://github.com/cube-js/cube.js/issues/3636)) ([55bbc60](https://github.com/cube-js/cube.js/commit/55bbc606ff0a013917731ec939cfda5927413925))

### Features

- **cubestore:** System tables and commands for debugging ([#3638](https://github.com/cube-js/cube.js/issues/3638)) ([22650a1](https://github.com/cube-js/cube.js/commit/22650a1f8e5c3bae85c735fbe9f31632610f567f))

## [0.28.53](https://github.com/cube-js/cube.js/compare/v0.28.52...v0.28.53) (2021-11-04)

### Bug Fixes

- **playground:** displaying boolean values ([76396ea](https://github.com/cube-js/cube.js/commit/76396ea4b28b4c7f72f11a51857955730d2f421f))
- `TypeError: Cannot read property 'joins' of null` in case of queryRewrite returns empty query ([#3627](https://github.com/cube-js/cube.js/issues/3627)) ([d880d0c](https://github.com/cube-js/cube.js/commit/d880d0ca434e754858cad18815ba34a132950ce2))

### Features

- **cubesql:** Real connection_id ([24d9804](https://github.com/cube-js/cube.js/commit/24d98041b4752f15156b9062dad98c801761ab0f))
- **cubesql:** Specify MySQL version as 8.0.25 in protocol ([eb7e73e](https://github.com/cube-js/cube.js/commit/eb7e73eac5819f8549f51e841f2f4fdc90ba7f32))

## [0.28.52](https://github.com/cube-js/cube.js/compare/v0.28.51...v0.28.52) (2021-11-03)

### Bug Fixes

- **client-core:** dayjs global locale conflict ([#3606](https://github.com/cube-js/cube.js/issues/3606)) Thanks @LvtLvt! ([de7471d](https://github.com/cube-js/cube.js/commit/de7471dfecd1c49f2e9554c92307d3f7c5b8eb9a))
- **cubesql:** MYSQL_TYPE_STRING for Booleans was null ([fcdd8f5](https://github.com/cube-js/cube.js/commit/fcdd8f57c34766f3d9d3827795142474a3489422))
- Empty data partitioned pre-aggregations are incorrectly handled -- value provided is not in a recognized RFC2822 or ISO format ([9f3acd5](https://github.com/cube-js/cube.js/commit/9f3acd572bcd2421bf8d3581c4c6287b62e77313))
- packages/cubejs-query-orchestrator/package.json to reduce vulnerabilities ([#3281](https://github.com/cube-js/cube.js/issues/3281)) ([a6a62ea](https://github.com/cube-js/cube.js/commit/a6a62ea5832a13b519ca4455af1f317cf7af64d9))

### Features

- **cubeclient:** Granularity is an optional field ([c381570](https://github.com/cube-js/cube.js/commit/c381570b786d27c49deb701c43858cd6e2facf02))
- **cubesql:** Execute SHOW DATABASES from AST ([cd2b4ac](https://github.com/cube-js/cube.js/commit/cd2b4acac41db5ced6d706c4acc6dcf46f9179ac))
- **cubesql:** Improve filter pushing (dateRange -> timeDimension) and segment ([8d7ea9b](https://github.com/cube-js/cube.js/commit/8d7ea9b076c26d6576474d6122dbffedeacd6e8e))
- **cubestore:** partitioned indexes for faster joins ([8ca605f](https://github.com/cube-js/cube.js/commit/8ca605f8cf2e0a2bf6cc08755f74ff4f8c096cb0))

## [0.28.51](https://github.com/cube-js/cube.js/compare/v0.28.50...v0.28.51) (2021-10-30)

### Bug Fixes

- Prevent mutation pre-aggregation objects, debug API ([#3605](https://github.com/cube-js/cube.js/issues/3605)) ([0f982ce](https://github.com/cube-js/cube.js/commit/0f982ce7951a6bec059cd9710b26b2f9b3f8b78a))
- **cubejs-client-ngx:** FilterMember.replace() will no longer update all filters with replacement ([#3597](https://github.com/cube-js/cube.js/issues/3597)) ([f972ad3](https://github.com/cube-js/cube.js/commit/f972ad375a7baccf3f67b58a8f2148ab8b278201))
- **native:** warning - is missing a bundled dependency node-pre-gyp ([0bee2f7](https://github.com/cube-js/cube.js/commit/0bee2f7f1776eb8e11cfed003f2e4741c73b1f48))

### Features

- **cubesql:** Skip SET = <expr> ([616023a](https://github.com/cube-js/cube.js/commit/616023a433cdf49fe76fc175b7c24abe267ea5f2))
- **cubesql:** Support db(), version() via QE ([5a289e1](https://github.com/cube-js/cube.js/commit/5a289e15f0c689ac3277edbb9c50bb11f34abdcc))
- **cubesql:** Support system variables ([#3592](https://github.com/cube-js/cube.js/issues/3592)) ([d2bd1fa](https://github.com/cube-js/cube.js/commit/d2bd1fab4674105e777b799db580d608b2c17caf))
- **cubesql:** Use real Query Engine for simple queries ([cc907d3](https://github.com/cube-js/cube.js/commit/cc907d3e2b35462a789427e084989c2ee4a693db))

## [0.28.50](https://github.com/cube-js/cube.js/compare/v0.28.49...v0.28.50) (2021-10-28)

### Bug Fixes

- **@cubejs-backend/mysql-driver:** Handle utf8mb4_bin as string ([a897392](https://github.com/cube-js/cube.js/commit/a8973924b3e940ea42880e456a603a3096994bef))
- **cubestore:** Added CUBESTORE_META_BIND_ADDR and CUBESTORE_WORKER_BIND_ADDR to allow for IPv6 binds ([435f8fc](https://github.com/cube-js/cube.js/commit/435f8fcf1d02bc6da5caaae223e9c64cd2e7e8be))
- **cubestore:** assertion failed: `(left == right)` in case of trying to access table streaming table right after creation ([c900d51](https://github.com/cube-js/cube.js/commit/c900d517d4a2105a202faf79585c37ec6d56a298))
- **native:** Correct logging level for native module ([c1a8439](https://github.com/cube-js/cube.js/commit/c1a843909d6681c718e3634f60684705cdc32f29))

### Features

- **native:** Simplify filters while converting to JSON RPC ([acab66a](https://github.com/cube-js/cube.js/commit/acab66a54e4cb7ca8a64717392f0dddc362f6057))
- Validate return type for dbType/driverFactory/externalDriverFactory in runtime ([#2657](https://github.com/cube-js/cube.js/issues/2657)) ([10e269f](https://github.com/cube-js/cube.js/commit/10e269f9febe26902838a2d7fa611a0f1d375d3e))

## [0.28.49](https://github.com/cube-js/cube.js/compare/v0.28.48...v0.28.49) (2021-10-23)

### Bug Fixes

- **@cubejs-backend/server-core:** Clean fetching pre-aggregation ranges, debug API ([#3573](https://github.com/cube-js/cube.js/issues/3573)) ([7beb090](https://github.com/cube-js/cube.js/commit/7beb09091ba5bdd8d86840e6347df8e81a83d09d))
- **cubesql:** Correct LE (<), GT (>) handling for DateTime filtering ([55e805a](https://github.com/cube-js/cube.js/commit/55e805a9e1fbfd462d3ce49eccd14ad815ac8c26))

## [0.28.48](https://github.com/cube-js/cube.js/compare/v0.28.47...v0.28.48) (2021-10-22)

### Bug Fixes

- Use BaseQuery#evaluateSql() for evaluate refresh range references, pre-aggregations debug API ([#3352](https://github.com/cube-js/cube.js/issues/3352)) ([ea81650](https://github.com/cube-js/cube.js/commit/ea816509ee9c07707bb46fc8fac83e55c52aaf00))
- **@cubejs-backend/ksql-driver:** Missing in docker ([4af6c8a](https://github.com/cube-js/cube.js/commit/4af6c8af71034272bae721e7f28c038776286f31))
- **@cubejs-backend/ksql-driver:** Scaffolding for empty schema generates empty prefix ([091e45c](https://github.com/cube-js/cube.js/commit/091e45c66b712491699856d0a203e442bdfbd888))
- **@cubejs-backend/ksql-driver:** Unquoted describe ([61dba66](https://github.com/cube-js/cube.js/commit/61dba66ef3f38e21d2b4f6f0d602009b66175e0a))

### Features

- **cubesql:** EXPLAIN <stmt> (debug info) ([7f0b57f](https://github.com/cube-js/cube.js/commit/7f0b57f1ed593ad51df7647aeeb9ee25055edfa6))

## [0.28.47](https://github.com/cube-js/cube.js/compare/v0.28.46...v0.28.47) (2021-10-22)

### Bug Fixes

- diagnostics - cannot convert undefined or null to object ([#3564](https://github.com/cube-js/cube.js/issues/3564)) ([643c9f8](https://github.com/cube-js/cube.js/commit/643c9f8b744ce115e70dd1d42800bc01be17e675))

### Features

- ksql support ([#3507](https://github.com/cube-js/cube.js/issues/3507)) ([b7128d4](https://github.com/cube-js/cube.js/commit/b7128d43d2aaffdd7273555779176b3efe4e2aa6))
- **cubesql:** Simplify root AND in where ([a417d4b](https://github.com/cube-js/cube.js/commit/a417d4b9166d1ac00346ea41323d6bd6e0e4e222))
- **cubesql:** Support SHOW DATABASES (alias) ([f1c4d3f](https://github.com/cube-js/cube.js/commit/f1c4d3f922fd36cb3b0af25b44301a26b801602f))
- **playground:** placeholder for BI (refers to documentation) ([#3563](https://github.com/cube-js/cube.js/issues/3563)) ([6b7da77](https://github.com/cube-js/cube.js/commit/6b7da77639e58d129e3c31b466e877406edd6a2c))

## [0.28.46](https://github.com/cube-js/cube.js/compare/v0.28.45...v0.28.46) (2021-10-20)

### Bug Fixes

- update error message for join across data sources ([#3435](https://github.com/cube-js/cube.js/issues/3435)) ([5ad72cc](https://github.com/cube-js/cube.js/commit/5ad72ccf0f6bbba3c362b04427b172210f0b8ada))
- **@cubejs-backend/snowflake-driver:** escape date_from and date_to in generated series SQL ([#3542](https://github.com/cube-js/cube.js/issues/3542)) Thanks to [@zpencerq](https://github.com/zpencerq) ! ([858b7fa](https://github.com/cube-js/cube.js/commit/858b7fa2dbc5ed08350a6f875189f6f608c6d55c)), closes [#3215](https://github.com/cube-js/cube.js/issues/3215)
- **native:** Catch errors in authentication handshake (msql_srv) ([#3560](https://github.com/cube-js/cube.js/issues/3560)) ([9012399](https://github.com/cube-js/cube.js/commit/90123990fa5713fc1351ba0540776a9f7cd78dce))
- **schema-compiler:** assign isVisible to segments ([#3484](https://github.com/cube-js/cube.js/issues/3484)) Thanks to [@piktur](https://github.com/piktur)! ([53fdf27](https://github.com/cube-js/cube.js/commit/53fdf27bb522608f36343c4a14ef32bf64c43200))

### Features

- **prestodb-driver:** Bump prestodb-client ([08e32eb](https://github.com/cube-js/cube.js/commit/08e32eb87545bc3578ac0aec9f8902a077458d4c))
- **prestodb-driver:** Support SSL ([b243e9f](https://github.com/cube-js/cube.js/commit/b243e9f3406a04e956a32bcd55a437749c69f632))

## [0.28.45](https://github.com/cube-js/cube.js/compare/v0.28.44...v0.28.45) (2021-10-19)

**Note:** Version bump only for package cubejs

## [0.28.44](https://github.com/cube-js/cube.js/compare/v0.28.43...v0.28.44) (2021-10-18)

### Bug Fixes

- **native:** Compile under Debian 9 (minimize libc requirement) ([9bcfb34](https://github.com/cube-js/cube.js/commit/9bcfb34b8aa539067bd0e190c06e748cb55ac8f9))

### Features

- **native:** Enable logger ([f0e2812](https://github.com/cube-js/cube.js/commit/f0e2812491302770b1e62ac4a87d50c58551bea3))

## [0.28.43](https://github.com/cube-js/cube.js/compare/v0.28.42...v0.28.43) (2021-10-17)

### Bug Fixes

- **native:** Allow to install Cube.js on unsupported systems ([71ce6a4](https://github.com/cube-js/cube.js/commit/71ce6a4eaa78870a3716bf8c9f1e091d08639753))
- **native:** Split musl/libc packages (musl is unsupported for now) ([836bd5f](https://github.com/cube-js/cube.js/commit/836bd5f3a2125326144819831c6b04962bdc0565))

### Features

- **native:** Support windows ([287665b](https://github.com/cube-js/cube.js/commit/287665be7c229c36b0cdc4646aefd1b683767e47))

## [0.28.42](https://github.com/cube-js/cube.js/compare/v0.28.41...v0.28.42) (2021-10-15)

### Bug Fixes

- **cubesql:** SET NAMES utf8mb4 ([9229123](https://github.com/cube-js/cube.js/commit/9229123b8160eefe47f071063a455eb854199ebf))
- **postgres-drive:** Move cubejs-backend/testing to devDep (reduce size) ([a3667c6](https://github.com/cube-js/cube.js/commit/a3667c675ec1b2487f669fe05532ee094ae1ca8c))

### Features

- **native:** CubeSQL - support auth via JWT (from user) ([#3536](https://github.com/cube-js/cube.js/issues/3536)) ([a10bd59](https://github.com/cube-js/cube.js/commit/a10bd5921627712182a67fda1e2b170e0373102c))
- **playground:** Support react chartjs charts drilldowns ([#3500](https://github.com/cube-js/cube.js/issues/3500)) ([499e37e](https://github.com/cube-js/cube.js/commit/499e37ed1b19bec83e408dbca73a30dc60a93b71))
- Integrate SQL Connector to Cube.js ([#3544](https://github.com/cube-js/cube.js/issues/3544)) ([f90de4c](https://github.com/cube-js/cube.js/commit/f90de4c9283178962f501826a8a64abb674c37d1))

## [0.28.41](https://github.com/cube-js/cube.js/compare/v0.28.40...v0.28.41) (2021-10-12)

### Bug Fixes

- **@cubejs-backend/snowflake-driver:** Reduce memory usage during batching downloads ([8748344](https://github.com/cube-js/cube.js/commit/8748344dd2ea3245532c85cb7a9cea6300d72acd))
- **cubestore:** fix parquet statistics for string columns ([565465a](https://github.com/cube-js/cube.js/commit/565465a02328875340d63046245637a3544ce2f1))
- **cubestore-driver:** Use ILIKE for contains filter ([#3502](https://github.com/cube-js/cube.js/issues/3502)) ([c1b2e10](https://github.com/cube-js/cube.js/commit/c1b2e10023b890276dc251db9c6d7b9bfa04e2d3))

### Features

- Introduce @cubejs-backend/native 🦀 ([#3531](https://github.com/cube-js/cube.js/issues/3531)) ([5fd511e](https://github.com/cube-js/cube.js/commit/5fd511e8804c26d06bdc166df05d630c650f23fc))
- Introduce cubeclient (rust client) ([ff44347](https://github.com/cube-js/cube.js/commit/ff443477925e9948b9b6e190370696e1d8375ee0))
- Introduce SQL Connector for Cube.js 🚀 ([#3527](https://github.com/cube-js/cube.js/issues/3527)) ([7d97398](https://github.com/cube-js/cube.js/commit/7d97398bc11b64c1c77463030263316fad1da27a))

## [0.28.40](https://github.com/cube-js/cube.js/compare/v0.28.39...v0.28.40) (2021-09-30)

### Bug Fixes

- Count distinct by week matches daily rollup in case of data range is daily ([#3490](https://github.com/cube-js/cube.js/issues/3490)) ([2401418](https://github.com/cube-js/cube.js/commit/24014188d63baa6f178242e738f72790369234a9))
- **@cubejs-backend/schema-compiler:** check segments when matching pre-aggregations ([#3494](https://github.com/cube-js/cube.js/issues/3494)) ([9357484](https://github.com/cube-js/cube.js/commit/9357484cdc924046b7371c7b701d307b1df84089))
- **@cubejs-backend/schema-compiler:** CubePropContextTranspiler expli… ([#3461](https://github.com/cube-js/cube.js/issues/3461)) ([2ae7f1d](https://github.com/cube-js/cube.js/commit/2ae7f1d51b0caca0fe9755a98463a6898960a11f))
- **@cubejs-backend/schema-compiler:** match query with no dimensions … ([#3472](https://github.com/cube-js/cube.js/issues/3472)) ([2a5dd4c](https://github.com/cube-js/cube.js/commit/2a5dd4cbe4805d9dea8f5f8e630bf613198195a3))

### Features

- **docker:** Use Node 12.22.6 ([9de777c](https://github.com/cube-js/cube.js/commit/9de777c893d0a0bf9c3b01a4c081eab24b98c417))

## [0.28.39](https://github.com/cube-js/cube.js/compare/v0.28.38...v0.28.39) (2021-09-22)

### Bug Fixes

- **@cubejs-client/vue:** catch `dryRun` errors on `query-builder` mount ([#3450](https://github.com/cube-js/cube.js/issues/3450)) ([189491c](https://github.com/cube-js/cube.js/commit/189491c87ac2d3f5e1ddac516d7ab236d6a7c09b))
- **cubestore:** fix Docker Hub repository name ([14cd0c3](https://github.com/cube-js/cube.js/commit/14cd0c32079b75d058967d6e7be88cac4fb5d2e2))
- **cubestore:** fix string-to-timestamp conversion ([654e81d](https://github.com/cube-js/cube.js/commit/654e81d42ef90bdcfffbe5c9760aa231facf8a43))
- **cubestore:** invalid data after compaction of binary columns ([064a9f4](https://github.com/cube-js/cube.js/commit/064a9f46995ddbf35fefa3c25f5c3a6e47d96c1a))
- **postgres-driver:** Unable to detect type for field "[@parser](https://github.com/parser)" ([#3475](https://github.com/cube-js/cube.js/issues/3475)) ([3e70d47](https://github.com/cube-js/cube.js/commit/3e70d47968524dc051d965b9f981beaaab0200c3))

## [0.28.38](https://github.com/cube-js/cube.js/compare/v0.28.37...v0.28.38) (2021-09-20)

### Bug Fixes

- **@cubejs-backend/query-orchestrator:** rollup only mode error message update ([c9c5ac0](https://github.com/cube-js/cube.js/commit/c9c5ac0ac33ce26f9505ce6683a41a9aaf742d63))
- **@cubejs-backend/schema-compiler:** CubeValidator human readable error messages ([#3425](https://github.com/cube-js/cube.js/issues/3425)) ([22db0a6](https://github.com/cube-js/cube.js/commit/22db0a6b607b7ef1d2cbe399415ac64c639325f5))
- **cubestore:** improve diagnostics on invalid configurations ([95f3810](https://github.com/cube-js/cube.js/commit/95f3810c28d777455c7c180b91f13c4fadc623de))
- **playground:** member visibility filter ([958fad1](https://github.com/cube-js/cube.js/commit/958fad1661d75fa7f387d837c209e5494ca94af4))

### Features

- **playground:** time zone for cron expressions ([#3441](https://github.com/cube-js/cube.js/issues/3441)) ([b27f509](https://github.com/cube-js/cube.js/commit/b27f509c690c7970ea5443650a141a1bbfcc947b))

## [0.28.37](https://github.com/cube-js/cube.js/compare/v0.28.36...v0.28.37) (2021-09-17)

### Bug Fixes

- **docs:** formulas rendering in MDX ([#3423](https://github.com/cube-js/cube.js/issues/3423)) ([3f51afb](https://github.com/cube-js/cube.js/commit/3f51afbd3f99206392c019932b58691054b621f2))
- **postgres-driver:** Use correct encoding for POSTGRES_HLL ([#3429](https://github.com/cube-js/cube.js/issues/3429)) ([0b54996](https://github.com/cube-js/cube.js/commit/0b54996ea2f4828b975caabfef5d6360429e713f))
- **query-orchestrator:** Wrong passing data source for partition range loader, debug API ([#3426](https://github.com/cube-js/cube.js/issues/3426)) ([dfaba5c](https://github.com/cube-js/cube.js/commit/dfaba5ca5a3f6fbb0f0e27e1133500b3e33a44b9))
- **website:** side menu uniq id ([#3427](https://github.com/cube-js/cube.js/issues/3427)) ([60f636e](https://github.com/cube-js/cube.js/commit/60f636ea3c94c56ca61d28b85549bcf3e25280d5))

### Features

- **cubestore:** ILIKE operator ([6a3fe64](https://github.com/cube-js/cube.js/commit/6a3fe647fb5f93932521591b6a7c572b88758bfe))
- **playground:** add rollup button ([#3424](https://github.com/cube-js/cube.js/issues/3424)) ([a5db7f1](https://github.com/cube-js/cube.js/commit/a5db7f1905d1eb50bb6e78b4c6c54e03ba7499c9))

## [0.28.36](https://github.com/cube-js/cube.js/compare/v0.28.35...v0.28.36) (2021-09-14)

### Bug Fixes

- **docs:** feedback block empty message ([#3420](https://github.com/cube-js/cube.js/issues/3420)) ([c9811dc](https://github.com/cube-js/cube.js/commit/c9811dcd1344940709c2a61aef8b96b583860580))

### Features

- **@cubejs-client/react:** useCubeMeta hook ([#3050](https://github.com/cube-js/cube.js/issues/3050)) ([e86b3fa](https://github.com/cube-js/cube.js/commit/e86b3fab76f1fa947262c1a54b18f5c7143f0306))
- **cubestore:** support reading of postgres-hll sketches ([72c38ba](https://github.com/cube-js/cube.js/commit/72c38badec1ee2ffc64218299653af1897042671))
- **postgres-driver:** Export HLL in base64 encoding + HLL_POSTGRES (for cubestore) ([f2b6d41](https://github.com/cube-js/cube.js/commit/f2b6d41fa43fba7c949bb753137effc80110eee0))

## [0.28.35](https://github.com/cube-js/cube.js/compare/v0.28.34...v0.28.35) (2021-09-13)

### Bug Fixes

- **@cubejs-backend/snowflake-driver:** ReferenceError host is not defined ([#3417](https://github.com/cube-js/cube.js/issues/3417)) ([bf61aa3](https://github.com/cube-js/cube.js/commit/bf61aa3241d55511ca321a1f119bf560d6dc74d7)), closes [#2923](https://github.com/cube-js/cube.js/issues/2923)
- **cubejs-client-core:** keep order of elements within tableColumns ([#3368](https://github.com/cube-js/cube.js/issues/3368)) ([b9a0f47](https://github.com/cube-js/cube.js/commit/b9a0f4744543d2d5facdbb191638f05790d21070))
- **docs:** feedback block empty state ([af37065](https://github.com/cube-js/cube.js/commit/af37065c309ec795eae3bc11b84d8f63ea2f3613))
- **gateway:** hidden members filtering ([#3384](https://github.com/cube-js/cube.js/issues/3384)) ([43ac8c3](https://github.com/cube-js/cube.js/commit/43ac8c30247944543f917cd893787ee8485fc985))

## [0.28.34](https://github.com/cube-js/cube.js/compare/v0.28.33...v0.28.34) (2021-09-13)

### Features

- **bigquery-driver:** Use INFORMATION_SCHEMA.COLUMNS for introspection ([ef22c6c](https://github.com/cube-js/cube.js/commit/ef22c6c7ae6ebe28e7d9683a1d7eef0f0b426b3c))
- **docs:** feedback block ([#3400](https://github.com/cube-js/cube.js/issues/3400)) ([688e74d](https://github.com/cube-js/cube.js/commit/688e74de28728f9bcd98a3929cfa00c9699acc18))

## [0.28.33](https://github.com/cube-js/cube.js/compare/v0.28.32...v0.28.33) (2021-09-11)

### Bug Fixes

- `updateWindow` validation isn't consistent with `refreshKey` interval parsing -- allow `s` at the end of time interval ([#3403](https://github.com/cube-js/cube.js/issues/3403)) ([57559e7](https://github.com/cube-js/cube.js/commit/57559e7841657e1900a30522ce5a178d091b6474))
- Handle "rollup only" mode for pre-aggregation preview data query, Debug API ([#3402](https://github.com/cube-js/cube.js/issues/3402)) ([f45626a](https://github.com/cube-js/cube.js/commit/f45626a72a49004f0d348310e3e05ee068f5079a))
- **docs:** h4 headers extra id params ([#3399](https://github.com/cube-js/cube.js/issues/3399)) ([0bc5f44](https://github.com/cube-js/cube.js/commit/0bc5f443f8b8a188582f26fadb1072385a2b747e))
- **helm-charts:** Fix live/ready probe path and expose options ([#3390](https://github.com/cube-js/cube.js/issues/3390)) ([97e2d47](https://github.com/cube-js/cube.js/commit/97e2d4777d1400b0b6c395977c23111314e1e289))

### Features

- Add ability to pass through generic-pool options ([#3364](https://github.com/cube-js/cube.js/issues/3364)) Thanks to @TRManderson! ([582a3e8](https://github.com/cube-js/cube.js/commit/582a3e82be8d2d676a45b1183eb35b5215fc78a9)), closes [#3340](https://github.com/cube-js/cube.js/issues/3340)

## [0.28.32](https://github.com/cube-js/cube.js/compare/v0.28.31...v0.28.32) (2021-09-06)

### Bug Fixes

- **cubestore:** 'unsorted data in merge' ([f4fad69](https://github.com/cube-js/cube.js/commit/f4fad697332de369292c30087e74c2a5af2723b7))
- **cubestore:** do not log AWS credentials, close [#3366](https://github.com/cube-js/cube.js/issues/3366) ([9aae6e5](https://github.com/cube-js/cube.js/commit/9aae6e585e87b39714d2273e9406913d1f3a8566))
- **helm-charts:** Add global.apiSecretFromSecret value and fix worker deployment naming ([#3346](https://github.com/cube-js/cube.js/issues/3346)) ([425b0a7](https://github.com/cube-js/cube.js/commit/425b0a714d791c3cee58847d98a6e8dc697b6577))
- HLL Rolling window query fails ([#3380](https://github.com/cube-js/cube.js/issues/3380)) ([581a52a](https://github.com/cube-js/cube.js/commit/581a52a856aeee067bac9a680e22694bf507af04))

### Features

- **playground:** rollup designer member search ([#3374](https://github.com/cube-js/cube.js/issues/3374)) ([7f6a877](https://github.com/cube-js/cube.js/commit/7f6a87708cad6e41fa71c307d1f371ee069bf907))

## [0.28.31](https://github.com/cube-js/cube.js/compare/v0.28.30...v0.28.31) (2021-09-02)

### Bug Fixes

- **cubestore:** fix crash on 'unexpected accumulator state List([NULL])' ([cbc0d52](https://github.com/cube-js/cube.js/commit/cbc0d5255d89481a1e88dacbf3b0dd03dc189839))
- **playground:** unsupported regex in safari ([308cc10](https://github.com/cube-js/cube.js/commit/308cc10b999348be6e46d1a9c270be9921086da3))

## [0.28.30](https://github.com/cube-js/cube.js/compare/v0.28.29...v0.28.30) (2021-09-01)

### Bug Fixes

- **playground:** sql formatting ([081b725](https://github.com/cube-js/cube.js/commit/081b72569ee9dc1dfb3c791711946acd795af98a))
- **playground:** warn when a query can't be rolled up ([be576ba](https://github.com/cube-js/cube.js/commit/be576baebded11dc5528a2974ee3a4da5028ed85))

## [0.28.29](https://github.com/cube-js/cube.js/compare/v0.28.28...v0.28.29) (2021-08-31)

### Bug Fixes

- **docs:** revert MDX components for recipes ([#3178](https://github.com/cube-js/cube.js/issues/3178)) ([#3343](https://github.com/cube-js/cube.js/issues/3343)) ([1d835f2](https://github.com/cube-js/cube.js/commit/1d835f2510b15d0cd6ae862a805c23508f082915))
- **playground:** allow both sql and every, info tooltips ([#3344](https://github.com/cube-js/cube.js/issues/3344)) ([a2177e6](https://github.com/cube-js/cube.js/commit/a2177e6e79fc0269be59a8ed20a7f23b9d55b83c))
- **typings:** schemaVersion supports an async fn ([#3339](https://github.com/cube-js/cube.js/issues/3339)) ([f344ff3](https://github.com/cube-js/cube.js/commit/f344ff3c6f7b1d55c2cd874a84cd31668a131c35))

### Features

- Mixed rolling window and regular measure queries from rollup support ([#3326](https://github.com/cube-js/cube.js/issues/3326)) ([3147e33](https://github.com/cube-js/cube.js/commit/3147e339f14ede73e5b0d14d05b9dd1f8b79e7b8))
- Support multi-value filtering on same column through FILTER_PARAMS ([#2854](https://github.com/cube-js/cube.js/issues/2854)) Thanks to [@omab](https://github.com/omab)! ([efc5745](https://github.com/cube-js/cube.js/commit/efc57452af44ee31092b8dfbb33a7ba23e86bba5))
- **docs:** MDX components for recipes ([#3178](https://github.com/cube-js/cube.js/issues/3178)) ([69ac9e0](https://github.com/cube-js/cube.js/commit/69ac9e054736b4cb64602f4f85f34a665ae44b01))
- **helm-charts:** Update to version v0.28.28 ([#3342](https://github.com/cube-js/cube.js/issues/3342)) ([c33473c](https://github.com/cube-js/cube.js/commit/c33473cd0266e6c6b63882046c6b7e2cd9f1fe21))
- **playground:** support refresh key cron expression ([#3332](https://github.com/cube-js/cube.js/issues/3332)) ([8def69c](https://github.com/cube-js/cube.js/commit/8def69c89b736f83f9d694abf78bd53eca18f0a2))
- add cubestore helm chart ([50fe798](https://github.com/cube-js/cube.js/commit/50fe798b775d04351b8e9b1648a65a2586c211e0))

## [0.28.28](https://github.com/cube-js/cube.js/compare/v0.28.27...v0.28.28) (2021-08-26)

### Bug Fixes

- **cubestore:** 'unsorted data' assertion with high-precision timestamps ([58a8cb4](https://github.com/cube-js/cube.js/commit/58a8cb453953d1b7b51f95b85364b708a5e0aa8c))
- **playground:** bar chart overlap ([85cebc2](https://github.com/cube-js/cube.js/commit/85cebc27c79a38e9ce98a5446e9e840245b00ee2))
- **playground:** reset state on query change ([#3321](https://github.com/cube-js/cube.js/issues/3321)) ([9c0d4fe](https://github.com/cube-js/cube.js/commit/9c0d4fe647f3f90a4d1a65782f5625469079b579))
- **server-core:** Wrapped ContinueWaitError for expandPartitionsInPreAggregations, refresh scheduler ([#3325](https://github.com/cube-js/cube.js/issues/3325)) ([f37f977](https://github.com/cube-js/cube.js/commit/f37f977c3352a3b820a767ff89ffcac2aeaaac70))

### Features

- **cubestore:** readiness and liveness probes ([888b0f1](https://github.com/cube-js/cube.js/commit/888b0f1b1b3fc50fe8d1dacd8718167ec2a69057))

## [0.28.27](https://github.com/cube-js/cube.js/compare/v0.28.26...v0.28.27) (2021-08-25)

**Note:** Version bump only for package cubejs

## [0.28.26](https://github.com/cube-js/cube.js/compare/v0.28.25...v0.28.26) (2021-08-24)

### Bug Fixes

- **cubestore:** "Unsupported Encoding DELTA_BYTE_ARRAY" ([29fcd40](https://github.com/cube-js/cube.js/commit/29fcd407e6d00e8d2080224cc2a86befb8cbeeac))

### Features

- **cubestore:** SQL extension for rolling window queries ([88a91e7](https://github.com/cube-js/cube.js/commit/88a91e74682ed4b65eea227db415c7a4845805cf))

## [0.28.25](https://github.com/cube-js/cube.js/compare/v0.28.24...v0.28.25) (2021-08-20)

### Bug Fixes

- **@cubejs-client/core:** prevent redundant auth updates ([#3288](https://github.com/cube-js/cube.js/issues/3288)) ([5ebf211](https://github.com/cube-js/cube.js/commit/5ebf211fde56aeee1a59f0cabc9f7ae3f8e9ffaa))
- **cubestore:** do not keep zombie child processes ([bfe3483](https://github.com/cube-js/cube.js/commit/bfe34839bcc1382b0a207995c06890adeedf38e7))

### Features

- **@cubejs-backend/dremio-driver:** support quarter granularity ([b193b04](https://github.com/cube-js/cube.js/commit/b193b048db4f104683ca5dc518b09647a8435a4e))
- **@cubejs-backend/mysql-driver:** Support quarter granularity ([#3289](https://github.com/cube-js/cube.js/issues/3289)) ([6922e5d](https://github.com/cube-js/cube.js/commit/6922e5da50d2056c00a2ca248665e133a6de28be))
- **@cubejs-client/playground:** support quarter granularity ([b93972a](https://github.com/cube-js/cube.js/commit/b93972a0f5b01140eed559036efec2feea6c2a73))
- **athena-driver:** Allow to configure workGroup ([#3254](https://github.com/cube-js/cube.js/issues/3254)) ([a300038](https://github.com/cube-js/cube.js/commit/a300038921c8fb93f2d255b32afc5c7e8150cf65))

## [0.28.24](https://github.com/cube-js/cube.js/compare/v0.28.23...v0.28.24) (2021-08-19)

### Bug Fixes

- **@cubejs-client/playground:** gaps in Safari 13 ([2336a94](https://github.com/cube-js/cube.js/commit/2336a94c065a1ee59dadaa385ddca4709fb9a197))
- **server-core:** Recreate agent transport after rejected WS connection, on retries ([#3280](https://github.com/cube-js/cube.js/issues/3280)) ([48c4e88](https://github.com/cube-js/cube.js/commit/48c4e88bd1a641bf016fc9fa78d5d150a08e0239))

### Features

- Added Quarter to the timeDimensions of ([3f62b2c](https://github.com/cube-js/cube.js/commit/3f62b2c125b2b7b752e370b65be4c89a0c65a623))
- Support quarter granularity ([4ad1356](https://github.com/cube-js/cube.js/commit/4ad1356ac2d2c4d479c25e60519b0f7b4c1605bb))
- **oracle-driver:** Allow to specify port via env variable, fix [#3210](https://github.com/cube-js/cube.js/issues/3210) ([#3274](https://github.com/cube-js/cube.js/issues/3274)) ([95230c9](https://github.com/cube-js/cube.js/commit/95230c9ffc59819c297d4d3e393a4a1665ec4e23))

## [0.28.23](https://github.com/cube-js/cube.js/compare/v0.28.22...v0.28.23) (2021-08-18)

### Bug Fixes

- **@cubejs-client/playground:** use title for vue charts ([#3273](https://github.com/cube-js/cube.js/issues/3273)) ([b4761e1](https://github.com/cube-js/cube.js/commit/b4761e11d14f5ea6f46fb0b6ca2d8b093e83df13))

## [0.28.22](https://github.com/cube-js/cube.js/compare/v0.28.21...v0.28.22) (2021-08-17)

### Features

- **@cubejs-client/playground:** rollup designer settings ([#3261](https://github.com/cube-js/cube.js/issues/3261)) ([67aa26e](https://github.com/cube-js/cube.js/commit/67aa26eb39eb1f64d0576bc832bd0fd1be02e55b))
- **cubejs-api-gateway:** Addition of `next` keyword for relative date ranges ([#3234](https://github.com/cube-js/cube.js/issues/3234)) Thanks to @JoshMentzer! ([f05922a](https://github.com/cube-js/cube.js/commit/f05922a49c0eca4a6e13f22e602066d2b4dbd8eb)), closes [#2866](https://github.com/cube-js/cube.js/issues/2866)

## [0.28.21](https://github.com/cube-js/cube.js/compare/v0.28.20...v0.28.21) (2021-08-16)

### Bug Fixes

- Pre-aggregations should have default refreshKey of every 1 hour if it doesn't set for Cube ([#3259](https://github.com/cube-js/cube.js/issues/3259)) ([bc472aa](https://github.com/cube-js/cube.js/commit/bc472aac1a666c84ed9e7e00b2d4e9018a6b5719))
- **@cubejs-client/playground:** display segments in the Rollup Designer ([73d6778](https://github.com/cube-js/cube.js/commit/73d677882441cf848c6ab39eab229cb54fc1c02f))

## [0.28.20](https://github.com/cube-js/cube.js/compare/v0.28.19...v0.28.20) (2021-08-15)

### Bug Fixes

- **druid-driver:** Support contains (LIKE), fix [#3109](https://github.com/cube-js/cube.js/issues/3109) ([#3121](https://github.com/cube-js/cube.js/issues/3121)) ([9340a4c](https://github.com/cube-js/cube.js/commit/9340a4c247bb2e4148e2e89f56e69f632f35e3ef))
- **redshift-driver:** Dont load user defined types ([bd25e7d](https://github.com/cube-js/cube.js/commit/bd25e7d7b84d7a057a8d4b6543f305584c74a624))
- **server-core:** Skip expand partitions for unused pre-aggregations ([#3255](https://github.com/cube-js/cube.js/issues/3255)) ([044ba8e](https://github.com/cube-js/cube.js/commit/044ba8e0c96bbcd1dc43c5aff114f458d9689204))

## [0.28.19](https://github.com/cube-js/cube.js/compare/v0.28.18...v0.28.19) (2021-08-13)

### Bug Fixes

- **cubestore:** add equality comparison between bool and int, fix [#3154](https://github.com/cube-js/cube.js/issues/3154) ([b3dc224](https://github.com/cube-js/cube.js/commit/b3dc2249af8fe397371213f933aab77fa12828e9))

## [0.28.18](https://github.com/cube-js/cube.js/compare/v0.28.17...v0.28.18) (2021-08-12)

### Bug Fixes

- **@cubejs-client/playground:** query param change on tab change ([#3240](https://github.com/cube-js/cube.js/issues/3240)) ([128711a](https://github.com/cube-js/cube.js/commit/128711a947b629e6a7241fe9150d57c8a23c7f99))
- **cubestore:** update datafusion to a new version ([ee80b3a](https://github.com/cube-js/cube.js/commit/ee80b3a2d16138768200e72cb7431fb067398ee8))

### Features

- **postgres-driver:** Load ud-types from pg_types for type detection ([b88d30c](https://github.com/cube-js/cube.js/commit/b88d30cfceb66682fbd0508352d441ba930f9e7c))

## [0.28.17](https://github.com/cube-js/cube.js/compare/v0.28.16...v0.28.17) (2021-08-11)

### Bug Fixes

- **@cubejs-client/core:** client hangs when loading big responses (node) ([#3216](https://github.com/cube-js/cube.js/issues/3216)) ([33a16f9](https://github.com/cube-js/cube.js/commit/33a16f983f5bbbb88e62f737ee2dc0670f3710c7))

## [0.28.16](https://github.com/cube-js/cube.js/compare/v0.28.15...v0.28.16) (2021-08-10)

**Note:** Version bump only for package cubejs

## [0.28.15](https://github.com/cube-js/cube.js/compare/v0.28.14...v0.28.15) (2021-08-06)

### Bug Fixes

- **@cubejs-client/core:** do not filter out time dimensions ([#3201](https://github.com/cube-js/cube.js/issues/3201)) ([0300093](https://github.com/cube-js/cube.js/commit/0300093e0af29b87e7a9018dc8159c1299e3cd85))
- **athena-driver:** Typings for driver (wrong import) ([fe6d429](https://github.com/cube-js/cube.js/commit/fe6d429c8ddf04702401c424143c113e92da0650))
- **docs:** change invalid api url ([#3205](https://github.com/cube-js/cube.js/issues/3205)) ([0c4aded](https://github.com/cube-js/cube.js/commit/0c4aded2579438d0ed31109cbfe3201805c5ea60))
- **examples:** change api url ([96d5c04](https://github.com/cube-js/cube.js/commit/96d5c045e8a2431879a7633b9ae5437c6951c340))

### Features

- **athena-driver:** Use getWorkGroup instead of SELECT 1 for testConnection ([a99a6e4](https://github.com/cube-js/cube.js/commit/a99a6e4bb5b5dad5558a74683f8618696c1786c0))

## [0.28.14](https://github.com/cube-js/cube.js/compare/v0.28.13...v0.28.14) (2021-08-05)

### Bug Fixes

- **@cubejs-client/playground:** live preview styles ([#3191](https://github.com/cube-js/cube.js/issues/3191)) ([862ee27](https://github.com/cube-js/cube.js/commit/862ee2761e0e41a2598b16c13d4d8b0e68b8bb29))
- **cubestore:** proper support for nulls in group by ([922138d](https://github.com/cube-js/cube.js/commit/922138d0c0e294f67aef133b42aeee85070e7a9a))
- **docs:** change top banner api url & catch network error ([0d600e5](https://github.com/cube-js/cube.js/commit/0d600e5fa8c30cf172704694c4f14f1e91d7074b))
- **docs:** fix broken image link ([5179071](https://github.com/cube-js/cube.js/commit/5179071c806745a65eaa335dc4a2bef10dd6bb05))
- **examples:** change invalid api url ([#3194](https://github.com/cube-js/cube.js/issues/3194)) ([d792631](https://github.com/cube-js/cube.js/commit/d79263149ba2bc2127fd8adb041e98c5d7593c05))

## [0.28.13](https://github.com/cube-js/cube.js/compare/v0.28.12...v0.28.13) (2021-08-04)

### Bug Fixes

- Support rolling `countDistinctApprox` rollups ([#3185](https://github.com/cube-js/cube.js/issues/3185)) ([e731992](https://github.com/cube-js/cube.js/commit/e731992b351f68f1ee249c9412f679b1903a6f28))
- **cubestore:** improve errors for env var parse failures ([dbedd4e](https://github.com/cube-js/cube.js/commit/dbedd4e9103b4ce3d22c86ede4d1dc8b56f64f24))
- Load build range query only on refresh key changes ([#3184](https://github.com/cube-js/cube.js/issues/3184)) ([40c6ee0](https://github.com/cube-js/cube.js/commit/40c6ee036de73144c12072c71df10081801d4e6b))

### Reverts

- Revert "misc: rollup dependencies fix" ([c08f93c](https://github.com/cube-js/cube.js/commit/c08f93cce990b5851287e89ad51980f78bbdef55))

## [0.28.12](https://github.com/cube-js/cube.js/compare/v0.28.11...v0.28.12) (2021-07-31)

### Bug Fixes

- **bigquery-driver:** Ignore dataSets with wrong permissions ([6933807](https://github.com/cube-js/cube.js/commit/6933807234b50b4eccee143fbf0595760f367701))

## [0.28.11](https://github.com/cube-js/cube.js/compare/v0.28.10...v0.28.11) (2021-07-31)

### Bug Fixes

- **docs:** banner time range ([6eddf4a](https://github.com/cube-js/cube.js/commit/6eddf4a555cf1ef3f9e99c88f26dbe40d0f4a302))
- Pre-aggregation table is not found for after it was successfully created in Refresh Worker ([334af1c](https://github.com/cube-js/cube.js/commit/334af1cc3c590e8b835958511d6ec0e17a77b0f3))

### Reverts

- Revert "chore: Do not test dev images" ([712b82a](https://github.com/cube-js/cube.js/commit/712b82ac0994e513962e41c0de4a9e67a9e1466a))

## [0.28.10](https://github.com/cube-js/cube.js/compare/v0.28.9...v0.28.10) (2021-07-30)

### Bug Fixes

- **cubestore:** date_add and date_sub on columnar data ([418c017](https://github.com/cube-js/cube.js/commit/418c017461c88fec0e4e28e1f1a64d97a0765718))

### Features

- Rolling window rollup support ([#3151](https://github.com/cube-js/cube.js/issues/3151)) ([109ab5b](https://github.com/cube-js/cube.js/commit/109ab5bec32255244412a24bf75402abd1cbfe49))

## [0.28.9](https://github.com/cube-js/cube.js/compare/v0.28.8...v0.28.9) (2021-07-29)

### Bug Fixes

- Optimize timestamp formatting and table names loading for large partition range serving ([#3166](https://github.com/cube-js/cube.js/issues/3166)) ([e1f8dc5](https://github.com/cube-js/cube.js/commit/e1f8dc5aab469b060f0fe8c69467117171c070fd))
- **@cubejs-client/core:** data blending without date range ([#3161](https://github.com/cube-js/cube.js/issues/3161)) ([cc7c140](https://github.com/cube-js/cube.js/commit/cc7c1401b1d4e7d66fa4215997cfa4f19f8a5707))
- **examples:** change invalid token ([dba8ae5](https://github.com/cube-js/cube.js/commit/dba8ae5bdf5bf8ee5b772cf294e1f56b4cc39eb4))
- **query-orchestrator:** Improved manual cancellation for preaggregation request from queue, Debug API ([#3156](https://github.com/cube-js/cube.js/issues/3156)) ([11284ce](https://github.com/cube-js/cube.js/commit/11284ce8085673486fb50d49a20ba005a45f1647))

### Features

- add env var for SNS || Pub/Sub topic ([#3086](https://github.com/cube-js/cube.js/issues/3086)) Thanks to [@msambol](https://github.com/msambol)! ([bc9289c](https://github.com/cube-js/cube.js/commit/bc9289c91fc9bf1e9f067e84041151ff0c189acd)), closes [#1014](https://github.com/cube-js/cube.js/issues/1014)
- **cubestore:** add `date_sub` function ([3bf2520](https://github.com/cube-js/cube.js/commit/3bf25203db8e0ebde00c224fd9462a3a2e54bee6))

## [0.28.8](https://github.com/cube-js/cube.js/compare/v0.28.7...v0.28.8) (2021-07-25)

### Bug Fixes

- Hide debug logs, Debug API ([#3152](https://github.com/cube-js/cube.js/issues/3152)) ([601f65f](https://github.com/cube-js/cube.js/commit/601f65f3e390e738da4f03d313935a2506a77290))

## [0.28.7](https://github.com/cube-js/cube.js/compare/v0.28.6...v0.28.7) (2021-07-25)

### Bug Fixes

- **@cubejs-client/react:** QueryBuilder incorrectly deduplicates filters based only on member instead of member+operator ([#2948](https://github.com/cube-js/cube.js/issues/2948)) ([#3147](https://github.com/cube-js/cube.js/issues/3147)) ([69d5baf](https://github.com/cube-js/cube.js/commit/69d5baf761bf6a55ec487cb179c61878bbfa6089))

### Features

- Remove job from pre-aggregations queue, Debug API ([#3148](https://github.com/cube-js/cube.js/issues/3148)) ([2e244db](https://github.com/cube-js/cube.js/commit/2e244dbde868a002d5c5d4396bb5274679cf9168))

## [0.28.6](https://github.com/cube-js/cube.js/compare/v0.28.5...v0.28.6) (2021-07-22)

### Bug Fixes

- **@cubejs-client/playground:** week granularity ([#3146](https://github.com/cube-js/cube.js/issues/3146)) ([9697a64](https://github.com/cube-js/cube.js/commit/9697a646e5e58edc54c4b08a810e0faecc4d0c69))
- **api-gateway:** Debug API, allow subscribe to pre-aggregations queue events only by playground JWT ([#3144](https://github.com/cube-js/cube.js/issues/3144)) ([700080f](https://github.com/cube-js/cube.js/commit/700080f33c13fd149acc6f6bad26a11ea0528211))

### Features

- **@cubejs-client/ngx:** async CubejsClient initialization ([#2876](https://github.com/cube-js/cube.js/issues/2876)) ([bba3a01](https://github.com/cube-js/cube.js/commit/bba3a01d2a072093509633f2d26e8df9677f940c))

## [0.28.5](https://github.com/cube-js/cube.js/compare/v0.28.4...v0.28.5) (2021-07-21)

### Bug Fixes

- **@cubejs-backend/server-core:** is ready for query processing for AWS ([22761b5](https://github.com/cube-js/cube.js/commit/22761b50d1a436973937807332c2f8aa85c21c06))
- **@cubejs-client/playground:** Rollup Designer can use pre-agg ([#3142](https://github.com/cube-js/cube.js/issues/3142)) ([3021132](https://github.com/cube-js/cube.js/commit/30211328ecbf47720a562584f40e6db4e0af1e89))
- **cubestore:** only pick index with exact column order ([f873a0c](https://github.com/cube-js/cube.js/commit/f873a0c2ba31dbc2ca80d4cdb0d5151a39f9e912))

## [0.28.4](https://github.com/cube-js/cube.js/compare/v0.28.3...v0.28.4) (2021-07-20)

### Bug Fixes

- **@cubejs-backend/server:** ready for query processing check ([#3133](https://github.com/cube-js/cube.js/issues/3133)) ([e3bf9e1](https://github.com/cube-js/cube.js/commit/e3bf9e19019da4dd76226704f3035ce7c8a845eb))
- **@cubejs-client/playground:** white strip at the bottom ([2eb972d](https://github.com/cube-js/cube.js/commit/2eb972d302e8b67ec77449cfbf28a32e4c6bdb5f))

### Features

- **@cubejs-client/playground:** rollup designer v2 updates ([#3124](https://github.com/cube-js/cube.js/issues/3124)) ([e69b79c](https://github.com/cube-js/cube.js/commit/e69b79c276c867c6c8f64cf4667aaf16e479e4d8))
- **athena-driver:** Use AWS-SDK v3 (modular) ([f14b7c1](https://github.com/cube-js/cube.js/commit/f14b7c1a12d25fc9e140bfb352a1eb4ca4a11cda))

## [0.28.3](https://github.com/cube-js/cube.js/compare/v0.28.2...v0.28.3) (2021-07-20)

### Bug Fixes

- **cubestore:** Installer (bad path) ([fe3458f](https://github.com/cube-js/cube.js/commit/fe3458ff492c1104c8719ec0b90a5c3b5e93a588))
- **query-orchestrator:** Wrong detection for 0 (zero integer) ([#3126](https://github.com/cube-js/cube.js/issues/3126)) ([39a6b1c](https://github.com/cube-js/cube.js/commit/39a6b1c2928995de4e91cb5242dd18a964e6d616))

## [0.28.2](https://github.com/cube-js/cube.js/compare/v0.28.1...v0.28.2) (2021-07-20)

### Bug Fixes

- Use experimental flag for pre-aggregations queue events bus, debug API ([#3130](https://github.com/cube-js/cube.js/issues/3130)) ([4f141d7](https://github.com/cube-js/cube.js/commit/4f141d789cd0058b2e7c07af6a16642d6c145834))
- **cubestore:** allow to specify join columns in any order, fix [#2987](https://github.com/cube-js/cube.js/issues/2987) ([b59aaab](https://github.com/cube-js/cube.js/commit/b59aaabc765c123dfa680e1866f79e6225219c76))
- **snowflake-driver:** Restart connection if it's not up ([dab86db](https://github.com/cube-js/cube.js/commit/dab86dbe49137157b3e154eda61972d42dfb7e7f))
- Close Cube Store process on process exit ([#3082](https://github.com/cube-js/cube.js/issues/3082)) ([f22f71a](https://github.com/cube-js/cube.js/commit/f22f71a4fe2240a9db58c035cb87d1b0d47e5b72))

### Features

- Support every for refreshKey with SQL ([63cd8f4](https://github.com/cube-js/cube.js/commit/63cd8f4673f9312f9b685352c18bb3ed01a40e6c))

## [0.28.1](https://github.com/cube-js/cube.js/compare/v0.28.0...v0.28.1) (2021-07-19)

### Features

- Subscribe to pre-aggregations queue events, debug API ([#3116](https://github.com/cube-js/cube.js/issues/3116)) ([9f0e52e](https://github.com/cube-js/cube.js/commit/9f0e52e012bf94d2adb5a70a75b4b1fb151238ea))

# [0.28.0](https://github.com/cube-js/cube.js/compare/v0.27.53...v0.28.0) (2021-07-17)

### Bug Fixes

- **@cubejs-backend/server-core:** update Production Checklist URL ([244f7a2](https://github.com/cube-js/cube.js/commit/244f7a251f206fa03dc3b45dc6ce7c46ec4bf810))

### Features

- Move partition range evaluation from Schema Compiler to Query Orchestrator to allow unbounded queries on partitioned pre-aggregations ([8ea654e](https://github.com/cube-js/cube.js/commit/8ea654e93b57014fb2409e070b3a4c381985a9fd))

## [0.27.53](https://github.com/cube-js/cube.js/compare/v0.27.52...v0.27.53) (2021-07-13)

### Bug Fixes

- **@cubejs-client/playground:** Rollup Designer time dimension granularity ([c4a19bd](https://github.com/cube-js/cube.js/commit/c4a19bdeabf362f6084623df18cdef6245f62ca8))

### Features

- **@cubejs-client/playground:** save pre-aggregations from the Rollup Designer ([#3096](https://github.com/cube-js/cube.js/issues/3096)) ([866f949](https://github.com/cube-js/cube.js/commit/866f949f2fc05e189a30b943a963aa7a3f697c81))

## [0.27.52](https://github.com/cube-js/cube.js/compare/v0.27.51...v0.27.52) (2021-07-13)

### Bug Fixes

- **cubestore:** crash on count(distinct ...) ([516924d](https://github.com/cube-js/cube.js/commit/516924d2615ff201a7b92d88e723acb0527c0b94))
- **cubestore-driver:** Map UUID to VARCHAR(64) ([#3101](https://github.com/cube-js/cube.js/issues/3101)) ([f87e60e](https://github.com/cube-js/cube.js/commit/f87e60e5b1b326ddd5d6b95f0d231be688728338))

## [0.27.51](https://github.com/cube-js/cube.js/compare/v0.27.50...v0.27.51) (2021-07-13)

### Bug Fixes

- Use orphaned timeout from query body, pre-aggregations queue, debug API ([#3088](https://github.com/cube-js/cube.js/issues/3088)) ([83e0a0a](https://github.com/cube-js/cube.js/commit/83e0a0a2d68fe94f193cfbb4fedb3f44d0b3cc27))
- **@cubejs-client/core:** incorrect types for logical and/or in query filters ([#3083](https://github.com/cube-js/cube.js/issues/3083)) ([d7014a2](https://github.com/cube-js/cube.js/commit/d7014a21add8d264d92987a3c840d98d09545457))

### Features

- Manual build pre-aggregations, getter for queue state, debug API ([#3080](https://github.com/cube-js/cube.js/issues/3080)) ([692372e](https://github.com/cube-js/cube.js/commit/692372e1da6ffa7b7c1718d8332d80d1e2ce2b2d))

## [0.27.50](https://github.com/cube-js/cube.js/compare/v0.27.49...v0.27.50) (2021-07-12)

### Bug Fixes

- **@cubejs-client/playground:** docs links ([7c06822](https://github.com/cube-js/cube.js/commit/7c06822208744cb2c00cdf1fc6ee3d0ad3c59379))
- **cubestore-driver:** Use correct syntax for Intervals ([53392d7](https://github.com/cube-js/cube.js/commit/53392d7b91c8308f033fa23a303ade265af30912))

### Features

- **cubestore:** Introduce support for DATE_ADD ([#3085](https://github.com/cube-js/cube.js/issues/3085)) ([071d7b4](https://github.com/cube-js/cube.js/commit/071d7b430566b0f42e2fc209b1888f9b4b9bb4e7))

## [0.27.49](https://github.com/cube-js/cube.js/compare/v0.27.48...v0.27.49) (2021-07-08)

### Bug Fixes

- **@cubejs-client/playground:** push only the query param on tab change ([a08153b](https://github.com/cube-js/cube.js/commit/a08153bf91065baa2a95de9749bd786c4057f191))

### Features

- Execute refreshKeys in externalDb (only for every) ([#3061](https://github.com/cube-js/cube.js/issues/3061)) ([75167a0](https://github.com/cube-js/cube.js/commit/75167a0e92028dbdd6f24aca85331b84be8ec3c3))

## [0.27.48](https://github.com/cube-js/cube.js/compare/v0.27.47...v0.27.48) (2021-07-08)

### Bug Fixes

- **@cubejs-client/core:** Long Query 413 URL too large ([#3072](https://github.com/cube-js/cube.js/issues/3072)) ([67de4bc](https://github.com/cube-js/cube.js/commit/67de4bc3de69a4da86d4c8d241abe5d921d0e658))
- **@cubejs-client/core:** week granularity ([#3076](https://github.com/cube-js/cube.js/issues/3076)) ([80812ea](https://github.com/cube-js/cube.js/commit/80812ea4027a929729187b096088f38829e9fa27))
- **@cubejs-client/playground:** new tab opening, tabs refactoring, limit ([#3071](https://github.com/cube-js/cube.js/issues/3071)) ([9eb0950](https://github.com/cube-js/cube.js/commit/9eb09505f5d807cff3aef36a36140ff7a8b4c650))
- **cubestore:** fix panic 'Unexpected accumulator state List([NULL])' ([cfe8647](https://github.com/cube-js/cube.js/commit/cfe8647d9ffd03dfde3d0fc028249a3c43ecb527))

### Performance Improvements

- **@cubejs-client/core:** speed up the pivot implementaion ([#3075](https://github.com/cube-js/cube.js/issues/3075)) ([d6d7a85](https://github.com/cube-js/cube.js/commit/d6d7a858ea8e3940b034cd12ed1630c53e55ea6d))

## [0.27.47](https://github.com/cube-js/cube.js/compare/v0.27.46...v0.27.47) (2021-07-06)

### Bug Fixes

- **@cubejs-client/playground:** security context update ([#3056](https://github.com/cube-js/cube.js/issues/3056)) ([2a879e2](https://github.com/cube-js/cube.js/commit/2a879e2695f58d9204b05c98d7bd80b7f55f572c))
- **@cubejs-client/playground:** wrong redirect to schema page ([#3064](https://github.com/cube-js/cube.js/issues/3064)) ([2c6f9e8](https://github.com/cube-js/cube.js/commit/2c6f9e878604055e051ea37420379ea0a1cdca13))
- **postgres-driver:** Catch error in streaming ([f73b648](https://github.com/cube-js/cube.js/commit/f73b6487cbf3403f0ea5a5c0081807b99664f619))
- **postgres-driver:** Convert Date column to UTC date ([d1d0944](https://github.com/cube-js/cube.js/commit/d1d094462425c82a795ad5bf0bfae151bffbdf7d))
- **postgres-driver:** Map numeric to decimal ([22b3536](https://github.com/cube-js/cube.js/commit/22b35367477cce7a03b77b9145617a6d7b1f601d))
- **postgres-driver:** Support mapping for bpchar ([46a3860](https://github.com/cube-js/cube.js/commit/46a386047a3bf4bcab7e1d5dec3aaf2f0f98900c))

### Features

- **@cubejs-client/playground:** rollup designer v2 ([#3018](https://github.com/cube-js/cube.js/issues/3018)) ([07e2427](https://github.com/cube-js/cube.js/commit/07e2427bb8050a74bae3a4d9206a7cfee6944022))
- **cubestore:** add some configuration variables ([23e26fa](https://github.com/cube-js/cube.js/commit/23e26fae914f2dd20c82bc61ae3836b4a384b1cf))

## [0.27.46](https://github.com/cube-js/cube.js/compare/v0.27.45...v0.27.46) (2021-07-01)

### Bug Fixes

- **query-orchestrator:** Incorrect passing params for fetch preview pre-aggregation data, debug API ([#3039](https://github.com/cube-js/cube.js/issues/3039)) ([bedc064](https://github.com/cube-js/cube.js/commit/bedc064039ebb2da4f6dbb7854524351deac9bd4))
- CUBEJS_REFRESH_WORKER shouldn't enabled externalRefresh ([7b2e9ee](https://github.com/cube-js/cube.js/commit/7b2e9ee68c37efa1eeef53a4e7b5cf4a86e0b065))
- Priorities for REFRESH_WORKER/SCHEDULED_REFRESH/SCHEDULED_REFRESH_TIMER ([176cdfd](https://github.com/cube-js/cube.js/commit/176cdfd00e63d8d7d6715e9079ad01edae1eb84a))

### Features

- Rename refreshRangeStart/End to buildRangeStart/End ([232d117](https://github.com/cube-js/cube.js/commit/232d1179623b567b96b026ce35522b177bcafce5))

## [0.27.45](https://github.com/cube-js/cube.js/compare/v0.27.44...v0.27.45) (2021-06-30)

### Bug Fixes

- **@cubejs-client/playground:** filter reset ([91f357d](https://github.com/cube-js/cube.js/commit/91f357dd72ff2745a66e843674f70f1a302c173f))
- Unexpected refresh value for refreshKey (earlier then expected) ([#3031](https://github.com/cube-js/cube.js/issues/3031)) ([55f75ac](https://github.com/cube-js/cube.js/commit/55f75ac95c93ab07b5e04158236b2356c2482f2c))

### Features

- Introduce CUBEJS_REFRESH_WORKER and CUBEJS_ROLLUP_ONLY ([68cb358](https://github.com/cube-js/cube.js/commit/68cb358d627d3ffcf5b73fecc687db66e44209db))

## [0.27.44](https://github.com/cube-js/cube.js/compare/v0.27.43...v0.27.44) (2021-06-29)

### Bug Fixes

- **clickhouse-driver:** Correct support for Decimal ([#3011](https://github.com/cube-js/cube.js/issues/3011)) ([e61a775](https://github.com/cube-js/cube.js/commit/e61a77540e9840b7d6034a942e9baf02e29e5031))
- **cubestore:** do not store error results in cache ([636ccec](https://github.com/cube-js/cube.js/commit/636ccec1c7a5d831cea1beee2275c335b5f62b8f))
- **cubestore:** merge operation on unsorted data ([7b6c67d](https://github.com/cube-js/cube.js/commit/7b6c67d2e5f7ab93612de096cc25723ab10cec0a))

## [0.27.43](https://github.com/cube-js/cube.js/compare/v0.27.42...v0.27.43) (2021-06-25)

### Bug Fixes

- **mysql-driver:** Empty tables with streaming ([da90b36](https://github.com/cube-js/cube.js/commit/da90b36c2e6eac0a1e2ddc8882d49f44b3c062f3))

## [0.27.42](https://github.com/cube-js/cube.js/compare/v0.27.41...v0.27.42) (2021-06-25)

### Bug Fixes

- **mysql/mongobi:** Map newdecimal to decimal ([641e888](https://github.com/cube-js/cube.js/commit/641e8889b17a79a2fd87d7d7e830c656c34e2fcc))

## [0.27.41](https://github.com/cube-js/cube.js/compare/v0.27.40...v0.27.41) (2021-06-25)

### Bug Fixes

- Use CUBEJS_REDIS_URL in serverless templates, fix [#2970](https://github.com/cube-js/cube.js/issues/2970) ([ca5d89e](https://github.com/cube-js/cube.js/commit/ca5d89ef9851ffff346e0f9262e2e4e5ac2cf8ba))
- Use timeDimension without s on the end ([#2997](https://github.com/cube-js/cube.js/issues/2997)) ([5313836](https://github.com/cube-js/cube.js/commit/531383699d888efbea87a8eec27e839cc6142f41))

### Features

- Allow to specify cacheAndQueueDriver (CUBEJS_CACHE_AND_QUEUE_DRIVER) in cube.js ([#2859](https://github.com/cube-js/cube.js/issues/2859)) ([7115828](https://github.com/cube-js/cube.js/commit/7115828d5e3a902013e320db23dd404ee563eb3d))
- Fetch pre-aggregation data preview by partition, debug api ([#2951](https://github.com/cube-js/cube.js/issues/2951)) ([4207f5d](https://github.com/cube-js/cube.js/commit/4207f5dea4f6c7a0237428f2d6fad468b98161a3))
- **cubestore:** debug data dumps for select queries ([b08617f](https://github.com/cube-js/cube.js/commit/b08617f59c835819319133ee676b62a078788845))

## [0.27.40](https://github.com/cube-js/cube.js/compare/v0.27.39...v0.27.40) (2021-06-23)

### Bug Fixes

- **cubestore:** refresh AWS credentials on timer, fix [#2946](https://github.com/cube-js/cube.js/issues/2946) ([23dee35](https://github.com/cube-js/cube.js/commit/23dee354573668f11553227ca50b5cf0b283d84a))

### Features

- **cubestore:** add now() and unix_timestamp() scalar function ([b40f3a8](https://github.com/cube-js/cube.js/commit/b40f3a896a1c65aece8c200733dd5cb8fe67d7be))
- **mssql-driver:** Use DATETIME2 for timeStampCast ([ed13768](https://github.com/cube-js/cube.js/commit/ed13768d842392491a2545ac2f465d24e43986ba))

## [0.27.39](https://github.com/cube-js/cube.js/compare/v0.27.38...v0.27.39) (2021-06-22)

### Bug Fixes

- **@cubejs-client/playground:** invalid token ([#2991](https://github.com/cube-js/cube.js/issues/2991)) ([5a8db99](https://github.com/cube-js/cube.js/commit/5a8db99659eda7bd6c0d2a09f77870cdfa1a3b06))
- Skip empty pre-aggregations sql, debug API ([#2989](https://github.com/cube-js/cube.js/issues/2989)) ([6629ca1](https://github.com/cube-js/cube.js/commit/6629ca15ae8f3e6d2efa83b019468f96a2473c18))

## [0.27.38](https://github.com/cube-js/cube.js/compare/v0.27.37...v0.27.38) (2021-06-22)

### Bug Fixes

- **query-orchestrator:** Re-use pre-aggregations cache loader for debug API ([#2981](https://github.com/cube-js/cube.js/issues/2981)) ([2a5f26e](https://github.com/cube-js/cube.js/commit/2a5f26e83c88246644151cf504624ced1ff1d431))

## [0.27.37](https://github.com/cube-js/cube.js/compare/v0.27.36...v0.27.37) (2021-06-21)

### Bug Fixes

- **cubestore:** fix 'Failed to write RLE run' ([845094b](https://github.com/cube-js/cube.js/commit/845094b0bd96f7d10ba7bb9faa84b95404b9527b))
- **mssql-driver:** Support case sensitive collation ([fd40d4a](https://github.com/cube-js/cube.js/commit/fd40d4a31f0c0d9ffa95b7d1572736918be88135))
- **mssql-driver:** Use DATETIME2 type in dateTimeCast ([#2962](https://github.com/cube-js/cube.js/issues/2962)) ([c8563ab](https://github.com/cube-js/cube.js/commit/c8563abca030bf43c3ef5f72ab00e294dcac5cd0))

### Features

- Remove support for view (dead code) ([de41702](https://github.com/cube-js/cube.js/commit/de41702492342f379d4098c065cbf6a61e0c5314))
- Support schema without references postfix ([22388cc](https://github.com/cube-js/cube.js/commit/22388cce0773aa57ac8888507904f6c2bd15f5ed))

## [0.27.36](https://github.com/cube-js/cube.js/compare/v0.27.35...v0.27.36) (2021-06-21)

### Bug Fixes

- **@cubejs-backend/server-core:** ready for query processing flag ([#2984](https://github.com/cube-js/cube.js/issues/2984)) ([8894a50](https://github.com/cube-js/cube.js/commit/8894a501a580a86c60a3297958f0133b31182a38))
- **api-gateway:** debug info, handle authInfo for Meta API ([#2979](https://github.com/cube-js/cube.js/issues/2979)) ([d9c4ef3](https://github.com/cube-js/cube.js/commit/d9c4ef34bdef47b9d3ff28e07087c3a44299887d))
- **elasticsearch-driver:** Make readOnly ([15ec569](https://github.com/cube-js/cube.js/commit/15ec5690fde529bf8294111150d44916139168fe))

## [0.27.35](https://github.com/cube-js/cube.js/compare/v0.27.34...v0.27.35) (2021-06-18)

### Bug Fixes

- **@cubejs-client/vue:** support all pivotConfig props ([#2964](https://github.com/cube-js/cube.js/issues/2964)) ([8c13b2b](https://github.com/cube-js/cube.js/commit/8c13b2beb3fe75533a923128859731cab2da4bbc))
- **cubestore:** do not spam logs when no statsd server is listening ([44b8cad](https://github.com/cube-js/cube.js/commit/44b8cad96cdbb84dd830d255392152d3f20a4e23))
- **cubestore:** fix assertion failure (unsorted inputs to merge sort) ([534da14](https://github.com/cube-js/cube.js/commit/534da146a472e2d04cd1e97c0ab9825f02623551))
- **cubestore:** send at most one request per worker ([17e504a](https://github.com/cube-js/cube.js/commit/17e504a62c21961724dc2a17bf8a9a480cf3cf23))

### Features

- **@cubejs-client/playground:** cli connection wizard ([#2969](https://github.com/cube-js/cube.js/issues/2969)) ([77652d7](https://github.com/cube-js/cube.js/commit/77652d7e0121140b0d52912a862c25aed911ed9d))
- **@cubejs-client/vue:** support logical operator filters ([#2950](https://github.com/cube-js/cube.js/issues/2950)). Thanks to [@piktur](https://github.com/piktur)! ([1313dad](https://github.com/cube-js/cube.js/commit/1313dad6a1f4b9da7e6ca194415b1a756e715692))
- **mongobi-driver:** Ingest types from driver ([c630997](https://github.com/cube-js/cube.js/commit/c63099799cd1fc99921c630d3f413d5da0b34453))
- **mongobi-driver:** Migrate to TypeScript ([8ca7811](https://github.com/cube-js/cube.js/commit/8ca7811be9ce1f6d240d4f4d62a2bb93899fbbbd))
- **mongobi-driver:** Support streaming ([059368e](https://github.com/cube-js/cube.js/commit/059368e68de606a0aefe1368131e21c795578995))
- MySQL/PostgreSQL - make readOnly by default ([86eef1b](https://github.com/cube-js/cube.js/commit/86eef1b9ea1382e9c6cfe1aabb48888c2132f406))
- **mysql-driver:** Ingest types from driver ([89a48a8](https://github.com/cube-js/cube.js/commit/89a48a8c2107b6a3a31d017b56df3233d005de83))

## [0.27.34](https://github.com/cube-js/cube.js/compare/v0.27.33...v0.27.34) (2021-06-15)

### Bug Fixes

- TypeError: oldLogger is not a function, fix [#2940](https://github.com/cube-js/cube.js/issues/2940) ([#2958](https://github.com/cube-js/cube.js/issues/2958)) ([4854f01](https://github.com/cube-js/cube.js/commit/4854f0184f3b5267540902803fe8b40d2da35a93))

### Features

- **clickhouse-driver:** Migrate to TS & HydrationStream ([c9672c0](https://github.com/cube-js/cube.js/commit/c9672c0a14d141a9c3fc6ae75e4321a2211383e7))
- **clickhouse-driver:** Support streaming ([2df4b10](https://github.com/cube-js/cube.js/commit/2df4b10c4998db8bbc6cafe07d1a8a30d4b0c41f))
- **clickhouse-driver:** Support type ingestion from database ([dfde312](https://github.com/cube-js/cube.js/commit/dfde31292dedf6c7ee2326bf06ee8d754f932b83))

## [0.27.33](https://github.com/cube-js/cube.js/compare/v0.27.32...v0.27.33) (2021-06-15)

### Bug Fixes

- **api-gateway:** Extend Load Request Success event ([#2952](https://github.com/cube-js/cube.js/issues/2952)) ([37a3563](https://github.com/cube-js/cube.js/commit/37a356350b18eca442d0be362b7b9ceae2caadd6))
- **cubestore:** fix crash (merge not supported on Float64) ([78e6d36](https://github.com/cube-js/cube.js/commit/78e6d36ad289ab2cc778e0fdc6c1dedd4de4c8e7))
- **server-core:** Finding preAggregation by strict equal, Meta API ([#2956](https://github.com/cube-js/cube.js/issues/2956)) ([c044da6](https://github.com/cube-js/cube.js/commit/c044da6014543bbf59476f81dd4faee87abb00d2))

## [0.27.32](https://github.com/cube-js/cube.js/compare/v0.27.31...v0.27.32) (2021-06-12)

### Features

- **@cubejs-client/playground:** internal pre-agg warning ([#2943](https://github.com/cube-js/cube.js/issues/2943)) ([039270f](https://github.com/cube-js/cube.js/commit/039270f267774bb5a80d5434ba057164e565b08b))

## [0.27.31](https://github.com/cube-js/cube.js/compare/v0.27.30...v0.27.31) (2021-06-11)

### Bug Fixes

- Allow multi timezone filter for pre-aggregations Meta API ([#2912](https://github.com/cube-js/cube.js/issues/2912)) ([5a873db](https://github.com/cube-js/cube.js/commit/5a873db17b9e12e7409b956de506ad525b723240))
- **bigquery-driver:** Encode buffer as base64 for CSV (streaming) ([a311a9a](https://github.com/cube-js/cube.js/commit/a311a9ab21ad8cfd902545e2052020a9cd28fddc))
- **cubejs-api-gateway:** Proper end date when current month has less days than previous month ([#2824](https://github.com/cube-js/cube.js/issues/2824)) ([0f83356](https://github.com/cube-js/cube.js/commit/0f83356fac4f276be8c2e22b27fa9d86f9b113e9))
- Create schema directory in DEV_MODE, when server is ready for processing ([c7b886c](https://github.com/cube-js/cube.js/commit/c7b886c0bf54d3e6a7981286a6a0e2eebddb68dc))
- **cubestore:** finalize GCP configuration variable names ([116ddc5](https://github.com/cube-js/cube.js/commit/116ddc5f019715308b1a640e5e88b278b07ced3d))
- **cubestore:** optimize access to table metainformation ([e727c8b](https://github.com/cube-js/cube.js/commit/e727c8b9b223c96199e2bbfdef2cd29a4457be86))
- **cubestore:** remove backtraces from error messages ([89a2e28](https://github.com/cube-js/cube.js/commit/89a2e28c3c6aea29fdf7b628174b3e03071515f6))
- extDbType warning ([#2939](https://github.com/cube-js/cube.js/issues/2939)) ([0f014bf](https://github.com/cube-js/cube.js/commit/0f014bf701e07dd1d542529d4792c0e9fbdceb48))
- Fetch all partitions, pre-aggregations Meta API ([#2944](https://github.com/cube-js/cube.js/issues/2944)) ([b5585fb](https://github.com/cube-js/cube.js/commit/b5585fbbb677b1b9e76d10719e64531286175da3))
- Make missing externalDriverFactory error message more specific ([b5480f6](https://github.com/cube-js/cube.js/commit/b5480f6b86e4d727bc027b22914508f17fd1f88c))

### Features

- **@cubejs-client/playground:** query tabs, preserve query history ([#2915](https://github.com/cube-js/cube.js/issues/2915)) ([d794d9e](https://github.com/cube-js/cube.js/commit/d794d9ec1281a2bc66a9194496df0eeb97936217))
- **cubestore:** Bump rocksdb for bindgen -> libloading (compatiblity aaarch64) ([a09d399](https://github.com/cube-js/cube.js/commit/a09d3998e296f0ebc2183abda28b30ab945aa4d7))
- **snowflake-driver:** Support UNLOAD to GCS ([91691e9](https://github.com/cube-js/cube.js/commit/91691e9513742496793d0d06a356976fc43b24fd))
- Rename queryTransformer to queryRewrite (with compatibility) ([#2934](https://github.com/cube-js/cube.js/issues/2934)) ([458cd9d](https://github.com/cube-js/cube.js/commit/458cd9d388bffc5e66893844c1e93b3b06e05525))
- Suggest export/unload for large pre-aggregations (detect via streaming) ([b20cdbc](https://github.com/cube-js/cube.js/commit/b20cdbc0b9fa98785d3ea46443492037017da12f))
- Write preAggregations block in schema generation ([2c1e150](https://github.com/cube-js/cube.js/commit/2c1e150ce70787381a34b22da32bf5b1e9e26bc6))

## [0.27.30](https://github.com/cube-js/cube.js/compare/v0.27.29...v0.27.30) (2021-06-04)

### Bug Fixes

- **@cubejs-backend/query-orchestrator:** enum to generic type ([#2906](https://github.com/cube-js/cube.js/issues/2906)) ([1a4f745](https://github.com/cube-js/cube.js/commit/1a4f74575116ec14570f1245895fc8756a6a6ff4))
- **@cubejs-client/playground:** pre-agg status ([#2904](https://github.com/cube-js/cube.js/issues/2904)) ([b18685f](https://github.com/cube-js/cube.js/commit/b18685f55a5f2bde8060cc7345dfd38f762307e3))
- **@cubejs-client/react:** order reset ([#2901](https://github.com/cube-js/cube.js/issues/2901)) ([536819f](https://github.com/cube-js/cube.js/commit/536819f5551e2846131d3f0459a46eba1306492a))
- **snowflake-driver:** Unexpected random order for columns in pre-aggregations ([a99977a](https://github.com/cube-js/cube.js/commit/a99977a11a2ae3029d3c2436b7d1500f7186a687))
- pass timezone to pre-aggregation description ([#2884](https://github.com/cube-js/cube.js/issues/2884)) ([9cca41e](https://github.com/cube-js/cube.js/commit/9cca41ee18ee6bb0dbd0d6abe6f778d467a9a240))

### Features

- Make scheduledRefresh true by default (preview period) ([f3e648c](https://github.com/cube-js/cube.js/commit/f3e648c7a3d05bfe4719a8f820794f11611fb8c7))
- skipExternalCacheAndQueue for Cube Store ([dc6138e](https://github.com/cube-js/cube.js/commit/dc6138e27deb9c319bab2faadd4413fe318c18e2))
- **cubestore:** combine query results on worker ([d76c9fd](https://github.com/cube-js/cube.js/commit/d76c9fdfc54feede8f2e4e6a6f62aaacd4f8f8f9))
- Introduce lock for dropOrphanedTables (concurrency bug) ([29509fa](https://github.com/cube-js/cube.js/commit/29509fa6714f636ba28960119138fbc20d604b4f))
- large dataset warning ([#2848](https://github.com/cube-js/cube.js/issues/2848)) ([92edbe9](https://github.com/cube-js/cube.js/commit/92edbe96cdc27a7830ae1496ca8c0c226177ca72))
- **cross:** Upgrade, use llvm/clang 9 ([f046839](https://github.com/cube-js/cube.js/commit/f0468398ee1890e0a7bff8b42da975029341ada2))
- **cubestore:** support the 'coalesce' function, fix [#2887](https://github.com/cube-js/cube.js/issues/2887) ([017fd4b](https://github.com/cube-js/cube.js/commit/017fd4b0be6d3c236a85782e59933932f0a0a7cf))
- **cubestore:** Use NPM's proxy settings in post-installer ([0b4daec](https://github.com/cube-js/cube.js/commit/0b4daec01eb41ff67daf97918df664a3dbab300a))

## [0.27.29](https://github.com/cube-js/cube.js/compare/v0.27.27...v0.27.29) (2021-06-02)

### Bug Fixes

- Pass securityContext to contextSymbols in refresh scheduler ([886c276](https://github.com/cube-js/cube.js/commit/886c2764d72837899f0bbde8c6e4cd80c1cafc97))
- Resolve refresh key sql for pre-aggregations meta api ([#2881](https://github.com/cube-js/cube.js/issues/2881)) ([55383b6](https://github.com/cube-js/cube.js/commit/55383b6d1d9755ded6e8815fc04ba3070e675199))
- **cubestore:** clean up metastore if create table fails, fix [#2880](https://github.com/cube-js/cube.js/issues/2880) ([f2d5b1a](https://github.com/cube-js/cube.js/commit/f2d5b1af2d2bd8d3444eb70464f8e5bcd3511dab))
- **cubestore:** fix encoding of sparse HLL imported from Snowflake ([5ca48f4](https://github.com/cube-js/cube.js/commit/5ca48f4384e59ac56bbdfe85644f1730d5cbe011))
- **elasticsearch-driver:** Lock @elastic/elasticsearch to 7.12 for Node.js 10 support ([cedf108](https://github.com/cube-js/cube.js/commit/cedf1089ea6262a4e3e3fed18c3714fb8258fa93))

### Features

- **cubestore:** cubestore-specific env vars for GCS configuration ([6760c0e](https://github.com/cube-js/cube.js/commit/6760c0e0c8706332890f50c3eb85d8e0def5d8f5))
- **docker:** Install redshift-driver ([dc81be5](https://github.com/cube-js/cube.js/commit/dc81be526c11a32e3b45327dae98a477597438d9))
- **snowflake-driver:** Support HLL ([7b57840](https://github.com/cube-js/cube.js/commit/7b578401a5271a2cbe43266f0190b786b2191aaf))

## [0.27.28](https://github.com/cube-js/cube.js/compare/v0.27.27...v0.27.28) (2021-06-02)

### Bug Fixes

- Pass securityContext to contextSymbols in refresh scheduler ([886c276](https://github.com/cube-js/cube.js/commit/886c2764d72837899f0bbde8c6e4cd80c1cafc97))
- Resolve refresh key sql for pre-aggregations meta api ([#2881](https://github.com/cube-js/cube.js/issues/2881)) ([55383b6](https://github.com/cube-js/cube.js/commit/55383b6d1d9755ded6e8815fc04ba3070e675199))
- **cubestore:** clean up metastore if create table fails, fix [#2880](https://github.com/cube-js/cube.js/issues/2880) ([f2d5b1a](https://github.com/cube-js/cube.js/commit/f2d5b1af2d2bd8d3444eb70464f8e5bcd3511dab))
- **cubestore:** fix encoding of sparse HLL imported from Snowflake ([5ca48f4](https://github.com/cube-js/cube.js/commit/5ca48f4384e59ac56bbdfe85644f1730d5cbe011))
- **elasticsearch-driver:** Lock @elastic/elasticsearch to 7.12 for Node.js 10 support ([cedf108](https://github.com/cube-js/cube.js/commit/cedf1089ea6262a4e3e3fed18c3714fb8258fa93))

### Features

- **cubestore:** cubestore-specific env vars for GCS configuration ([6760c0e](https://github.com/cube-js/cube.js/commit/6760c0e0c8706332890f50c3eb85d8e0def5d8f5))
- **docker:** Install redshift-driver ([dc81be5](https://github.com/cube-js/cube.js/commit/dc81be526c11a32e3b45327dae98a477597438d9))
- **snowflake-driver:** Support HLL ([7b57840](https://github.com/cube-js/cube.js/commit/7b578401a5271a2cbe43266f0190b786b2191aaf))

## [0.27.27](https://github.com/cube-js/cube.js/compare/v0.27.26...v0.27.27) (2021-06-01)

### Bug Fixes

- **cubestore:** panic on compaction of decimals [#2868](https://github.com/cube-js/cube.js/issues/2868) ([a4eef83](https://github.com/cube-js/cube.js/commit/a4eef83602734b1e6c59e0666822f9c80eed3a90))
- Filter empty contexts for pre-aggregation Meta API ([#2873](https://github.com/cube-js/cube.js/issues/2873)) ([cec4dff](https://github.com/cube-js/cube.js/commit/cec4dff54b1e56f6670edf1041b0ff22ee2c25d1))

## [0.27.26](https://github.com/cube-js/cube.js/compare/v0.27.25...v0.27.26) (2021-06-01)

### Bug Fixes

- **redshift-driver:** publishConfig - public ([d72baf4](https://github.com/cube-js/cube.js/commit/d72baf4ef39d89be986f51941629def0975c989f))

## [0.27.25](https://github.com/cube-js/cube.js/compare/v0.27.24...v0.27.25) (2021-06-01)

### Bug Fixes

- **@cubejs-backend/mssql-driver:** Add column type mappings for MSSQL external pre-aggregations ([#2846](https://github.com/cube-js/cube.js/issues/2846)) ([7c1ef6d](https://github.com/cube-js/cube.js/commit/7c1ef6d69e9cc0ff8f19c6dd2b8a6f2328ec6456))
- **cubestore:** allow decimal and float type in index keys ([32d2f69](https://github.com/cube-js/cube.js/commit/32d2f691d7fba73bd36a8cfd75ed1ecebb046ef3))
- **cubestore:** Uncompress files with .gz ([5f8062a](https://github.com/cube-js/cube.js/commit/5f8062a272ed3d01d61c0de7847bd31363079d92))
- **mysql/aurora-mysql:** Unexpected index creation in loop ([#2828](https://github.com/cube-js/cube.js/issues/2828)) ([456aae7](https://github.com/cube-js/cube.js/commit/456aae7f1a21b18250511f2f8b5783e6e66dfb53))

### Features

- Introduce Redshift driver (based on postgres-driver) ([f999699](https://github.com/cube-js/cube.js/commit/f99969905ee4df1a797afd389177a72dd5a097c8))
- **cubestore:** Support import of Snowflake HLL ([61324e0](https://github.com/cube-js/cube.js/commit/61324e0784314bb7c6db67a45d5da35bdc5fee26))
- **redshift-driver:** Support UNLOAD (direct export to S3) ([d741027](https://github.com/cube-js/cube.js/commit/d7410278d3a27e692c41b222ab0eb66f7992fe21))
- Pre-aggregations Meta API, part 2 ([#2804](https://github.com/cube-js/cube.js/issues/2804)) ([84b6e70](https://github.com/cube-js/cube.js/commit/84b6e70ed81e80cff0ba8d0dd9ad507132bb1b24))
- time filter operators ([#2851](https://github.com/cube-js/cube.js/issues/2851)) ([5054249](https://github.com/cube-js/cube.js/commit/505424964d34ae16205485e5347409bb4c5a661d))

## [0.27.24](https://github.com/cube-js/cube.js/compare/v0.27.23...v0.27.24) (2021-05-29)

### Bug Fixes

- **@cubejs-client/core:** decompose type ([#2849](https://github.com/cube-js/cube.js/issues/2849)) ([60f2596](https://github.com/cube-js/cube.js/commit/60f25964f8f7aecd8a36cc0f1b811445ae12edc6))
- **cubestore:** Invalid cross-device link (os error 18) during streaming CREATE TABLE ([942f6d0](https://github.com/cube-js/cube.js/commit/942f6d0b1ee7635bf15c17b8d69467385fba4747))

## [0.27.23](https://github.com/cube-js/cube.js/compare/v0.27.22...v0.27.23) (2021-05-27)

### Bug Fixes

- **bigquery-driver:** Broken package ([8b8c10b](https://github.com/cube-js/cube.js/commit/8b8c10b94a99aca4ce26aab231d0379d434f88f6))
- **cubestore:** do not resolve aliases in having clause ([caca792](https://github.com/cube-js/cube.js/commit/caca79226d69ff2ca29b731eac9074137bdbb780))

## [0.27.22](https://github.com/cube-js/cube.js/compare/v0.27.21...v0.27.22) (2021-05-27)

### Bug Fixes

- **postgresql-driver:** Map bool/float4/float8 to generic type, use bigint for int8 ([ddc1739](https://github.com/cube-js/cube.js/commit/ddc17393dba3067ebe06248d817c8778859020a8))

### Features

- **@cubejs-client/vue3:** vue 3 support ([#2827](https://github.com/cube-js/cube.js/issues/2827)) ([6ac2c8c](https://github.com/cube-js/cube.js/commit/6ac2c8c938fee3001f78ef0f8782255799550514))
- **bigquery-driver:** Migrate to TypeScript ([7c5b254](https://github.com/cube-js/cube.js/commit/7c5b25459cd3265587ddd8ed6dd23c944094254c))
- **bigquery-driver:** Support CUBEJS_DB_EXPORT_BUCKET ([400c163](https://github.com/cube-js/cube.js/commit/400c1632e978de6c00b4c996088d1b61a9223404))
- **bigquery-driver:** Support streaming ([8ffeba2](https://github.com/cube-js/cube.js/commit/8ffeba20f9d647438504d735155b0f8cca29fa40))

## [0.27.21](https://github.com/cube-js/cube.js/compare/v0.27.20...v0.27.21) (2021-05-26)

### Bug Fixes

- Drop NodeJs namespace from @types/styled-components ([343fa8c](https://github.com/cube-js/cube.js/commit/343fa8c3556424461aaf600fc13246ffffa14473))
- Pre-aggregation warmup stucks if refresh range fetch takes too long ([fe9afd5](https://github.com/cube-js/cube.js/commit/fe9afd5007d2fc2a80fe08e792b3cdb1b29eedee))

## [0.27.20](https://github.com/cube-js/cube.js/compare/v0.27.19...v0.27.20) (2021-05-25)

### Bug Fixes

- **@cubejs-client/playground:** reorder reset ([#2810](https://github.com/cube-js/cube.js/issues/2810)) ([2d22683](https://github.com/cube-js/cube.js/commit/2d22683e7f02447f22a74db5fc2e0a122939f7e8))

### Features

- **@cubejs-client/playground:** sticky search bar, search improvements ([#2815](https://github.com/cube-js/cube.js/issues/2815)) ([343c52b](https://github.com/cube-js/cube.js/commit/343c52bf42fd1809360135243be9a5020beea19b))
- **cubestore:** support aliases for right side of joins ([42a7d41](https://github.com/cube-js/cube.js/commit/42a7d4120af19e45751d683389d8003691a807e7))
- **cubestore:** support SQL for rolling window queries ([03ff70a](https://github.com/cube-js/cube.js/commit/03ff70ac9be7805b9332d3382707b42ebf625be9))

## [0.27.19](https://github.com/cube-js/cube.js/compare/v0.27.18...v0.27.19) (2021-05-24)

### Bug Fixes

- Dont publish source files for snowflake/postgresql ([cd2c90f](https://github.com/cube-js/cube.js/commit/cd2c90f51bd02cd9b918c64e116207b75af95d3d))
- **snowflake-driver:** Handle UNLOAD for empty tables ([f5f69ff](https://github.com/cube-js/cube.js/commit/f5f69ff79371bdd33526ffa0cc8634a0f30fc9f4))

### Features

- Make rollup default for preAggregation.type ([4875fa1](https://github.com/cube-js/cube.js/commit/4875fa1be360372e7ed9dcaf9ded8b28bb348e2f))
- Pre-aggregations Meta API, part 1 ([#2801](https://github.com/cube-js/cube.js/issues/2801)) ([2245a77](https://github.com/cube-js/cube.js/commit/2245a7774666a3a8bd36703b2b4001b20789b943))
- **@cubejs-client/playground:** pre-agg helper ([#2807](https://github.com/cube-js/cube.js/issues/2807)) ([44f09c3](https://github.com/cube-js/cube.js/commit/44f09c39ce3b2a8c6a2ce2fb66b75a0c288eb1da))

## [0.27.18](https://github.com/cube-js/cube.js/compare/v0.27.17...v0.27.18) (2021-05-22)

### Bug Fixes

- **@cubejs-backend/postgres-driver:** Cannot find module './dist/src' ([d4cd657](https://github.com/cube-js/cube.js/commit/d4cd657c465875b1c609c0d0f1e05b6d50e7df96))

## [0.27.17](https://github.com/cube-js/cube.js/compare/v0.27.16...v0.27.17) (2021-05-22)

### Bug Fixes

- CUBEJS_JWT_KEY - allow string ([1bd9832](https://github.com/cube-js/cube.js/commit/1bd9832ca1f93dc685ff9633482ebe7c8537e11b))
- Pre-aggregation table is not found if warming up same pre-aggregation with a new index ([b980744](https://github.com/cube-js/cube.js/commit/b9807446174e310cc2981af0fa1bc6b54fa1669d))
- **cubestore:** fix [#2748](https://github.com/cube-js/cube.js/issues/2748), a crash in partition filter ([f6f0992](https://github.com/cube-js/cube.js/commit/f6f09923c065ac6d2b97c5cbbe42958e51dab251))
- **cubestore:** improve partition filter accuracy ([ef93d26](https://github.com/cube-js/cube.js/commit/ef93d26bfdb889c34fd267db599cbe071925d507))

### Features

- **@cubejs-client/core:** exporting CubejsApi class ([#2773](https://github.com/cube-js/cube.js/issues/2773)) ([03cfaff](https://github.com/cube-js/cube.js/commit/03cfaff83d2ca1c9e49b6088eab3948d081bb9a9))
- **mysql-driver:** Support streaming ([d694c91](https://github.com/cube-js/cube.js/commit/d694c91370ceb703a50aefdd42ae3a9834e9884c))
- **postgres-driver:** Introduce streaming ([7685ffd](https://github.com/cube-js/cube.js/commit/7685ffd45640c5ac7b403487476dd799a11db5a0))
- **postgres-driver:** Migrate driver to TypeScript ([38f4adb](https://github.com/cube-js/cube.js/commit/38f4adbc7d0405b99cada80cc844011dcc504d2f))
- **snowflake-driver:** Introduce streaming ([4119a79](https://github.com/cube-js/cube.js/commit/4119a79c20e710bb31c418d0ffd6bb6b8f736319))
- **snowflake-driver:** Migrate to TypeScript ([c63a0ae](https://github.com/cube-js/cube.js/commit/c63a0ae1cd446ff4a0e336dd71c1874f99a5dfdb))
- **snowflake-driver:** Support UNLOAD to S3 ([d984f97](https://github.com/cube-js/cube.js/commit/d984f973547d1ff339dbf6ba26f0e363bddf2e98))
- Dont introspect schema, if driver can detect types ([3467b44](https://github.com/cube-js/cube.js/commit/3467b4472e800d8345260a5765542486ed93647b))

## [0.27.16](https://github.com/cube-js/cube.js/compare/v0.27.15...v0.27.16) (2021-05-19)

### Bug Fixes

- Optimize high load Cube Store serve track: do not use Redis while fetching pre-aggregation tables ([#2776](https://github.com/cube-js/cube.js/issues/2776)) ([a3bc0b8](https://github.com/cube-js/cube.js/commit/a3bc0b8079373394e6a6a4fca12ddd46195d5b7d))

### Features

- **@cubejs-client/playground:** member search ([#2764](https://github.com/cube-js/cube.js/issues/2764)) ([8ba6eed](https://github.com/cube-js/cube.js/commit/8ba6eed02a0bfed877f89eb42c035ef71617b483))
- **cubestore:** Allow to query tables only when they imported and ready ([#2775](https://github.com/cube-js/cube.js/issues/2775)) ([02cf69a](https://github.com/cube-js/cube.js/commit/02cf69ac9477d87e898a34ac1b8acad829dd120e))
- **cubestore:** update datafusion and sqlparser-rs ([a020c07](https://github.com/cube-js/cube.js/commit/a020c070cea053983ccd28c5d47e3f805364b713))
- **snowflake-driver:** Support 'key-pair' authentication ([#2724](https://github.com/cube-js/cube.js/issues/2724)) ([4dc55b4](https://github.com/cube-js/cube.js/commit/4dc55b40e47f62e6b82d8a6549d2823dee0ed9cb))

## [0.27.15](https://github.com/cube-js/cube.js/compare/v0.27.14...v0.27.15) (2021-05-18)

### Bug Fixes

- upgrade @amcharts/amcharts4 from 4.10.10 to 4.10.17 ([#2707](https://github.com/cube-js/cube.js/issues/2707)) ([56c7f5a](https://github.com/cube-js/cube.js/commit/56c7f5af29daf3007e56bbd64e4506f23d6e6f59))
- upgrade @cubejs-client/core from 0.19.56 to 0.26.94 ([#2706](https://github.com/cube-js/cube.js/issues/2706)) ([93dd7d9](https://github.com/cube-js/cube.js/commit/93dd7d92181ad0f2ab60bf8dc07db4b2d2ee4ced))
- upgrade react-dropzone from 11.2.4 to 11.3.2 ([#2708](https://github.com/cube-js/cube.js/issues/2708)) ([a16a83b](https://github.com/cube-js/cube.js/commit/a16a83bc93598a4b5eb788f9a69429382b0e162b))
- **@cubejs-client/core:** compareDateRange pivot fix ([#2752](https://github.com/cube-js/cube.js/issues/2752)) ([653ad84](https://github.com/cube-js/cube.js/commit/653ad84f57cc7d5e004c92fe717246ce33d7dcad))
- **@cubejs-client/core:** Meta types fixes ([#2746](https://github.com/cube-js/cube.js/issues/2746)) ([cd17755](https://github.com/cube-js/cube.js/commit/cd17755a07fe19fbebe44d0193330d43a66b757d))
- **@cubejs-client/playground:** display error message on all tabs ([#2741](https://github.com/cube-js/cube.js/issues/2741)) ([0b9b597](https://github.com/cube-js/cube.js/commit/0b9b597f8e936c9daf287c5ed2e03ecdf5af2a0b))

### Features

- **@cubejs-client/playground:** member grouping ([#2736](https://github.com/cube-js/cube.js/issues/2736)) ([7659438](https://github.com/cube-js/cube.js/commit/76594383e08e44354c5966f8e60107d65e05ddab))
- Enable external pre-aggregations by default for new users ([22de035](https://github.com/cube-js/cube.js/commit/22de0358ec35017c45e6a716faaacf176c49c652))
- Replace diagnostic error for devPackages to warning ([b65c1f5](https://github.com/cube-js/cube.js/commit/b65c1f56329998cbcafa27512de3acc9da3b62c5))
- Start Cube Store on startup in devMode for official images ([5e216a8](https://github.com/cube-js/cube.js/commit/5e216a8df0e5df2168d007bac32102cf78ba26e3))

## [0.27.14](https://github.com/cube-js/cube.js/compare/v0.27.13...v0.27.14) (2021-05-13)

### Bug Fixes

- **@cubejs-client/playground:** stop processing query once it has changed ([#2731](https://github.com/cube-js/cube.js/issues/2731)) ([3aed740](https://github.com/cube-js/cube.js/commit/3aed7408d5fc43bd51356869956c5d0d18c6424f))
- **schema-compiler:** Time-series query with minute/second granularity ([c4a6044](https://github.com/cube-js/cube.js/commit/c4a6044702df39629044802b0b5d9e1636cc99d0))
- Add missing validators for configuration values ([6ac6350](https://github.com/cube-js/cube.js/commit/6ac6350d2e7a892122a3cbbe1cfae822ba483f3b))

### Features

- **@cubejs-client/core:** member sorting ([#2733](https://github.com/cube-js/cube.js/issues/2733)) ([fae3b29](https://github.com/cube-js/cube.js/commit/fae3b293a2ce84ea518567f38546f14acc587e0d))
- Compile Cube.ts configuration in-memory ([3490d1f](https://github.com/cube-js/cube.js/commit/3490d1f2a5b3fb9af617e3add8993f351e82f1b1))

## [0.27.13](https://github.com/cube-js/cube.js/compare/v0.27.12...v0.27.13) (2021-05-13)

### Bug Fixes

- **@cubejs-client/core:** response error handling ([#2703](https://github.com/cube-js/cube.js/issues/2703)) ([de31373](https://github.com/cube-js/cube.js/commit/de31373d9829a6924d7edc04b96464ffa417d920))
- **@cubejs-client/playground:** telemetry ([#2727](https://github.com/cube-js/cube.js/issues/2727)) ([366435a](https://github.com/cube-js/cube.js/commit/366435aeae5ed2d30d520cd0ced2fb6d6b6bbe29))

### Features

- **cubestore:** Use MSVC build for Windows ([d472bcd](https://github.com/cube-js/cube.js/commit/d472bcdbd2c19beb433f79fab8d6a7abc23c8c05))

## [0.27.12](https://github.com/cube-js/cube.js/compare/v0.27.11...v0.27.12) (2021-05-13)

### Bug Fixes

- Out of memory on intensive pre-aggregation warmup: use intervals instead of promises cycles ([51149cb](https://github.com/cube-js/cube.js/commit/51149cbd56281cdd89072cfc7a5e449422793bc4))

## [0.27.11](https://github.com/cube-js/cube.js/compare/v0.27.10...v0.27.11) (2021-05-12)

### Bug Fixes

- **clickhouse-driver:** Support ungrouped query, fix [#2717](https://github.com/cube-js/cube.js/issues/2717) ([#2719](https://github.com/cube-js/cube.js/issues/2719)) ([82efc98](https://github.com/cube-js/cube.js/commit/82efc987c574960c4989b41b92776ba928a2f22b))
- Compress archive with Cube Store for Windows ([f09589b](https://github.com/cube-js/cube.js/commit/f09589b8b4836f1ffa15a54ee6d848bf02f1671b))
- **cubestore:** Built-in in archive OpenSSL for Windows (msvc) ([dbdeb6d](https://github.com/cube-js/cube.js/commit/dbdeb6da1850b25ce094ffdf70b6f7ae27a4066a))
- **cubestore:** do not stop startup warmup on errors ([90350a3](https://github.com/cube-js/cube.js/commit/90350a34d7c7519d052174432fdcb5a3c07e4359))

### Features

- **cubestore:** import separate CSV files in parallel ([ca896b3](https://github.com/cube-js/cube.js/commit/ca896b3aa3e54d923a3054f55aaf7d4b5735a64d))
- Strict validatation for configuration ([eda24b8](https://github.com/cube-js/cube.js/commit/eda24b8f97fa1d406b75cf2496f77295af5e9e1a))

## [0.27.10](https://github.com/cube-js/cube.js/compare/v0.27.9...v0.27.10) (2021-05-11)

### Bug Fixes

- titleize case ([6acc100](https://github.com/cube-js/cube.js/commit/6acc100a6d50bf6970010843d0472e981399a602))

### Features

- Move External Cache And Queue serving to Cube Store ([#2702](https://github.com/cube-js/cube.js/issues/2702)) ([37e4268](https://github.com/cube-js/cube.js/commit/37e4268869a23c07f922a039873d349b733bf577))

## [0.27.9](https://github.com/cube-js/cube.js/compare/v0.27.8...v0.27.9) (2021-05-11)

### Bug Fixes

- **@cubejs-backend/schema-compiler:** titleize fix ([#2695](https://github.com/cube-js/cube.js/issues/2695)) ([d997f49](https://github.com/cube-js/cube.js/commit/d997f4978de9f4b25b606ba6c17b225f3e6a6cb1))
- **mssql-driver:** Cannot read property 'readOnly' of undefined, fix [#2659](https://github.com/cube-js/cube.js/issues/2659) ([d813594](https://github.com/cube-js/cube.js/commit/d81359498e5d57cc855354ef2dae41653dadbea2))

### Features

- **cubestore:** Support x86_64-pc-windows-msvc ([407f1bd](https://github.com/cube-js/cube.js/commit/407f1bdda7fdb24c7863163b6157624ab6c9b7ee))

## [0.27.8](https://github.com/cube-js/cube.js/compare/v0.27.7...v0.27.8) (2021-05-06)

### Bug Fixes

- **@cubejs-client/playground:** prevent the browser to freeze on large SQL highlighting ([3285008](https://github.com/cube-js/cube.js/commit/32850086901c65a375e4aca625cc649956d6161f))

## [0.27.7](https://github.com/cube-js/cube.js/compare/v0.27.6...v0.27.7) (2021-05-04)

### Bug Fixes

- ECONNREFUSED 127.0.0.1:3030 on health checks in production mode without Cube Store ([7ccfbe1](https://github.com/cube-js/cube.js/commit/7ccfbe1524f11b3b88c90aa2e49969596cb0aa48))
- TypeError: moment is not a function ([39662e4](https://github.com/cube-js/cube.js/commit/39662e468ed001a5b472a2c09fc9fa2d48af03d2))

## [0.27.6](https://github.com/cube-js/cube.js/compare/v0.27.5...v0.27.6) (2021-05-03)

### Bug Fixes

- **@cubejs-client/playground:** display server error message ([#2648](https://github.com/cube-js/cube.js/issues/2648)) ([c4d8936](https://github.com/cube-js/cube.js/commit/c4d89369db8796fb136af8370aee2111ac3d0316))

## [0.27.5](https://github.com/cube-js/cube.js/compare/v0.27.4...v0.27.5) (2021-05-03)

### Bug Fixes

- **cubestore:** create `metastore-current` atomically, do not send content-length to GCS ([a2a68a0](https://github.com/cube-js/cube.js/commit/a2a68a04ab89d4df30236fa175ddc1abde79503d))
- **cubestore:** support OFFSET clause ([30b7b68](https://github.com/cube-js/cube.js/commit/30b7b68647496c995c68bbcf7a6b98ebce213783))
- **query-orchestrator:** tableColumnTypes - compatibility for MySQL 8 ([a886876](https://github.com/cube-js/cube.js/commit/a88687606ce54272c8742503f62bcac8d2ca0441))
- cubejs-dev-server ([8a16b52](https://github.com/cube-js/cube.js/commit/8a16b5288f35423b0f0dd3960bd0af8a3d6c9e41))

### Features

- Use Cube Store as default EXTERNAL_DB, prefix variables ([30d52c4](https://github.com/cube-js/cube.js/commit/30d52c4f8f1ae1d6619ec4976d9db1eeb1e44140))
- **@cubejs-client/playground:** pre-aggregation status ([#2641](https://github.com/cube-js/cube.js/issues/2641)) ([f48f63f](https://github.com/cube-js/cube.js/commit/f48f63f2691ee7ee37dda8a89490d067143d879b))

## [0.27.4](https://github.com/cube-js/cube.js/compare/v0.27.3...v0.27.4) (2021-04-29)

### Bug Fixes

- **@cubejs-client/playground:** cache pane crash ([#2635](https://github.com/cube-js/cube.js/issues/2635)) ([405b80b](https://github.com/cube-js/cube.js/commit/405b80b4f9b14b98eb6d47803b0f1e519ce1e0c9))
- **@cubejs-client/playground:** pass field, win host ([e3144e9](https://github.com/cube-js/cube.js/commit/e3144e9110f4d572aca56cf9393a558e4c0817d1))
- **cubestore:** make top-k scan less batches ([486ee32](https://github.com/cube-js/cube.js/commit/486ee328f7625fd9fb2c490ec68e1fcd2c4c8a50))
- **cubestore-driver:** Ping connection only when it's OPEN ([d80e157](https://github.com/cube-js/cube.js/commit/d80e157e5865318c14be534a7f8a1bc39b0ad851))
- Show warning for deprecated variables only once ([fecbda4](https://github.com/cube-js/cube.js/commit/fecbda456b2b66005fb230e093b117db76c4919e))

## [0.27.3](https://github.com/cube-js/cube.js/compare/v0.27.2...v0.27.3) (2021-04-29)

### Bug Fixes

- **@cubejs-backend/cloud:** Missed dependency ([#2626](https://github.com/cube-js/cube.js/issues/2626)) ([0d396bf](https://github.com/cube-js/cube.js/commit/0d396bf7bd41c5d9acb8d84ae62bdd4ee33fc2a3))
- **@cubejs-server/core:** env file path ([#2622](https://github.com/cube-js/cube.js/issues/2622)) ([b9abb19](https://github.com/cube-js/cube.js/commit/b9abb195b5c73b3a0077801302721bf8ae2bdaa0))

## [0.27.2](https://github.com/cube-js/cube.js/compare/v0.27.1...v0.27.2) (2021-04-28)

### Bug Fixes

- Disable Scheduling when querying is not ready (before connection Wizard) ([b518ccd](https://github.com/cube-js/cube.js/commit/b518ccdd66a2f51af541bc54de2df816e138bac5))
- Error: ENOENT: no such file or directory, scandir '/cube/conf/schema' ([dc96084](https://github.com/cube-js/cube.js/commit/dc9608419f53dd823a7f59fcb18141b0f18f002b))
- Move Prettier & Jest to dev dep (reduce size) ([da59584](https://github.com/cube-js/cube.js/commit/da5958426b701b8a81506e8d74070b2977e3df56))
- **@cubejs-server/core:** config file check ([1c16af1](https://github.com/cube-js/cube.js/commit/1c16af18ea024239a66f096113157d72eb1065b0))

## [0.27.1](https://github.com/cube-js/cube.js/compare/v0.27.0...v0.27.1) (2021-04-27)

### Bug Fixes

- **cubestore:** simplify `trim_alloc` handling ([aa8e721](https://github.com/cube-js/cube.js/commit/aa8e721fb295e6748f220cb70dd1f318f0d113f8))
- Deprecate CUBEJS*REDIS_PASSWORD, similar to all REDIS* variables ([#2604](https://github.com/cube-js/cube.js/issues/2604)) ([ee54aeb](https://github.com/cube-js/cube.js/commit/ee54aebae9db828263feef0f4f5285754abcc5c2)), closes [#ch484](https://github.com/cube-js/cube.js/issues/ch484)

# [0.27.0](https://github.com/cube-js/cube.js/compare/v0.26.104...v0.27.0) (2021-04-26)

### Features

- Add CUBEJS\_ prefix for REDIS_URL/REDIS_TLS ([#2312](https://github.com/cube-js/cube.js/issues/2312)) ([b5e7099](https://github.com/cube-js/cube.js/commit/b5e7099da90d0f8c6def0af648b45b18c1e56569))

## [0.26.104](https://github.com/cube-js/cube.js/compare/v0.26.103...v0.26.104) (2021-04-26)

### Bug Fixes

- Original SQL table is not found when `useOriginalSqlPreAggregations` used together with `CUBE.sql()` reference ([#2603](https://github.com/cube-js/cube.js/issues/2603)) ([5fd8e42](https://github.com/cube-js/cube.js/commit/5fd8e42cbe361a66cf0ffe6542478b6beaad86c5))

## [0.26.103](https://github.com/cube-js/cube.js/compare/v0.26.102...v0.26.103) (2021-04-24)

### Bug Fixes

- **cubestore:** deploy datafusion fix, add test for failing top-k ([59bc127](https://github.com/cube-js/cube.js/commit/59bc127e401a03364622b9257e48db47b496caae))
- concurrency lock driver instance & release on error ([dd60f5d](https://github.com/cube-js/cube.js/commit/dd60f5d1be59012ed33958d5889276afcc0639af))
- Typo JWK_KEY -> JWT_KEY ([#2516](https://github.com/cube-js/cube.js/issues/2516)) ([7bdb576](https://github.com/cube-js/cube.js/commit/7bdb576e2130c61eec87e88575fdb96c55d6b4a6)), closes [#ch449](https://github.com/cube-js/cube.js/issues/ch449)
- **cubestore:** fix error on binary results of CASE ([72634e9](https://github.com/cube-js/cube.js/commit/72634e9d4e5a2cf66595895513f721032a64a0a5))
- **docker:** Dev images & broken playground after local build ([3d7a96d](https://github.com/cube-js/cube.js/commit/3d7a96dfae970623e2358369c8e7485992e6e3fd))
- **mssql-driver:** multi-part identifier `columns.data_type` ([487e8b2](https://github.com/cube-js/cube.js/commit/487e8b2fa8da761f32d10481bcc1d77eaf094ab4))
- **snowflake:** pre-aggregations & add support for number/timestamp_ntz -> generic type ([d66bac8](https://github.com/cube-js/cube.js/commit/d66bac82f5b61e33265b4bb257f3a58640dfa983))

### Features

- **docker:** Upgrade Node.js to 12.22.1 [security] ([087d762](https://github.com/cube-js/cube.js/commit/087d7629d69a5988c25ba8272aac44ea4275be78)), closes [#2021-04-06-version-12221](https://github.com/cube-js/cube.js/issues/2021-04-06-version-12221)

## [0.26.102](https://github.com/cube-js/cube.js/compare/v0.26.101...v0.26.102) (2021-04-22)

### Bug Fixes

- **@cubejs-client/core:** add type definition to the categories method ([#2585](https://github.com/cube-js/cube.js/issues/2585)) ([4112b2d](https://github.com/cube-js/cube.js/commit/4112b2ddf956537dd1fc3d08b249bef8d07f7645))
- **cubestore:** download data files on worker startup ([0a6caba](https://github.com/cube-js/cube.js/commit/0a6cabad2cec32ba25d995d99f65f9c1f874895b))
- **cubestore:** download only relevant partitions on workers ([7adfd62](https://github.com/cube-js/cube.js/commit/7adfd62b220ef2194a77d82101f93831a8e02c20))

### Features

- **@cubejs-playground:** localhost connection tipbox ([#2587](https://github.com/cube-js/cube.js/issues/2587)) ([bab3265](https://github.com/cube-js/cube.js/commit/bab3265265747266285a4ca3aa1a3b36bfab9783))
- **server:** Handle Ctrl + C (SIGINT) in Docker ([644dc46](https://github.com/cube-js/cube.js/commit/644dc461497d970c5a89a756e2b0edad41d2f8e8))

## [0.26.101](https://github.com/cube-js/cube.js/compare/v0.26.100...v0.26.101) (2021-04-20)

### Bug Fixes

- QueryQueue - broken queuing on any error (TimeoutError: ResourceRequest timed out) ([ebe2c71](https://github.com/cube-js/cube.js/commit/ebe2c716d4ec0aa5cbc60529567f39672c199720))

## [0.26.100](https://github.com/cube-js/cube.js/compare/v0.26.99...v0.26.100) (2021-04-20)

### Bug Fixes

- Make sure pre-aggregation warmup has a priority over Refresh Scheduler to avoid endless loop refreshes ([9df3506](https://github.com/cube-js/cube.js/commit/9df3506001fe8f0708f5fe85816d1b00f9b2573a))

### Features

- **api-gateway:** Allow to throw CubejsHandlerError in checkAuth ([559172a](https://github.com/cube-js/cube.js/commit/559172a2b57d2afc1e58d79abc018b96a6856d59))

## [0.26.99](https://github.com/cube-js/cube.js/compare/v0.26.98...v0.26.99) (2021-04-16)

### Bug Fixes

- Continuous refresh during Scheduled Refresh in case of both external and internal pre-aggregations are defined for the same data source ([#2566](https://github.com/cube-js/cube.js/issues/2566)) ([dcd2894](https://github.com/cube-js/cube.js/commit/dcd2894453d7afda3ea50b2592ae6610b231f5c5))

### Features

- **dreamio-driver:** Improve polling, similar to BigQuery driver ([5a4783a](https://github.com/cube-js/cube.js/commit/5a4783a62b62e54d960fc2498aecb47ad1142b3c))
- **dreamio-driver:** Up code quality ([9e1aafe](https://github.com/cube-js/cube.js/commit/9e1aafe3b214673f1934fae2e66c8b901e44575d))

## [0.26.98](https://github.com/cube-js/cube.js/compare/v0.26.97...v0.26.98) (2021-04-15)

### Bug Fixes

- **@cubejs-client/playground:** live preview check ([5b4b5b8](https://github.com/cube-js/cube.js/commit/5b4b5b818c1a8d095cd821494b3f2785428a60cb))
- **cubestore:** allow to disable top-k with env var ([9c2838a](https://github.com/cube-js/cube.js/commit/9c2838aecf2980fa3c076aa812f12fef05924344)), closes [#2559](https://github.com/cube-js/cube.js/issues/2559)
- **cubestore:** re-enable streaming for top-k ([c21b5f7](https://github.com/cube-js/cube.js/commit/c21b5f7690d5de7570034449d24a7842dfd097c6))
- **docker:** Build playground for dev images ([42a75db](https://github.com/cube-js/cube.js/commit/42a75db492663d2ba296601ed42842a88176d4da))
- **dreamio-driver:** Allow casting boolean/number/measure ([#2560](https://github.com/cube-js/cube.js/issues/2560)) ([4ff93fe](https://github.com/cube-js/cube.js/commit/4ff93fe44a79926044ec5e2cbe291276ca0e3235))
- **dreamio-driver:** Remove unexpected console.log ([#2561](https://github.com/cube-js/cube.js/issues/2561)) ([a3beee7](https://github.com/cube-js/cube.js/commit/a3beee76f4797a3a073762a2013362dfd88b136b))

### Features

- **ws-transport:** Introduce close() method ([47394c1](https://github.com/cube-js/cube.js/commit/47394c195fc7513c664c6e1e35b43a6883924491))

## [0.26.97](https://github.com/cube-js/cube.js/compare/v0.26.96...v0.26.97) (2021-04-14)

**Note:** Version bump only for package cubejs

## [0.26.96](https://github.com/cube-js/cube.js/compare/v0.26.95...v0.26.96) (2021-04-14)

### Bug Fixes

- **@cubejs-client/playground:** basePath, vue dashboard selection ([#2555](https://github.com/cube-js/cube.js/issues/2555)) ([5723786](https://github.com/cube-js/cube.js/commit/5723786056b1a42ead4554f817210ea7881c0ddb))

## [0.26.95](https://github.com/cube-js/cube.js/compare/v0.26.94...v0.26.95) (2021-04-13)

### Bug Fixes

- **playground:** Test connection in Connection Wizard ([715fa1c](https://github.com/cube-js/cube.js/commit/715fa1c0ab6667017e12851cbd97e97cf6807d60))
- post-install compatibility with yarn ([4641e81](https://github.com/cube-js/cube.js/commit/4641e814909a807ecf49e838e6dc471db6920392))

## [0.26.94](https://github.com/cube-js/cube.js/compare/v0.26.93...v0.26.94) (2021-04-13)

### Bug Fixes

- **@cubejs-client/core:** WebSocket response check ([7af1b45](https://github.com/cube-js/cube.js/commit/7af1b458a9f2d7390e80805241d108b895d5c2cc))

## [0.26.93](https://github.com/cube-js/cube.js/compare/v0.26.92...v0.26.93) (2021-04-12)

### Bug Fixes

- **@cubejs-client/vue:** make query reactive ([#2539](https://github.com/cube-js/cube.js/issues/2539)) ([9bce6ba](https://github.com/cube-js/cube.js/commit/9bce6ba964d71f0cba0e4d4e4e21a973309d77d4))
- **docker:** Lock yarn, workaround for PATH & post-installers ([907befa](https://github.com/cube-js/cube.js/commit/907befa4ae96cdba7f9d5ac167a74af9fb1fff18))

## [0.26.92](https://github.com/cube-js/cube.js/compare/v0.26.91...v0.26.92) (2021-04-12)

### Bug Fixes

- **cubestore:** temporarily disable streaming in top-k ([ff629d5](https://github.com/cube-js/cube.js/commit/ff629d51790bb54f719b38448acfbd7fb1eba67c))
- Do not refresh cube keys during pre-aggregations warmup ([f331d53](https://github.com/cube-js/cube.js/commit/f331d53dbfd3feef0a5fa71ffc89c72cef3ecbb0))
- post installers compatiblity with Windows [#2520](https://github.com/cube-js/cube.js/issues/2520) ([7e9bd7c](https://github.com/cube-js/cube.js/commit/7e9bd7c86df1032d53e752654fe4a446951480bb))

## [0.26.91](https://github.com/cube-js/cube.js/compare/v0.26.90...v0.26.91) (2021-04-11)

### Bug Fixes

- **@cubejs-backend/cubestore-driver:** Import stream stuck if it's big: handle bigger uploads using temp files ([08155bc](https://github.com/cube-js/cube.js/commit/08155bc3ccc03f9bdcfa54a2ab6a2b10cd9edb39))
- **@cubejs-backend/mysql-driver:** Add support for more int type definitions ([64f9cb7](https://github.com/cube-js/cube.js/commit/64f9cb7e601d8ada637fdf20011270766b9e676b))

## [0.26.90](https://github.com/cube-js/cube.js/compare/v0.26.89...v0.26.90) (2021-04-11)

### Bug Fixes

- **@cubejs-backend/cubestore-driver:** Import stream stuck if it's big ([2a41020](https://github.com/cube-js/cube.js/commit/2a41020027987a451818bbd8b5b750aec0dc6ace))
- Manage stream connection release by orchestrator instead of driver ([adf059e](https://github.com/cube-js/cube.js/commit/adf059ec52e31e3ebc055b60a1ac6236c57251f8))
- **@cubejs-client/core:** display request status code ([b6108c9](https://github.com/cube-js/cube.js/commit/b6108c9285ffe177439b2310c201d19f19819206))
- **@cubejs-client/vue:** error test fix ([9a97e7b](https://github.com/cube-js/cube.js/commit/9a97e7b97b1a300e89051810a421cb686f250faa))
- **cubestore:** Empty files on temp upload ([893e467](https://github.com/cube-js/cube.js/commit/893e467dcbb1461a3f769709197838773b9eccc0))

## [0.26.89](https://github.com/cube-js/cube.js/compare/v0.26.88...v0.26.89) (2021-04-10)

### Bug Fixes

- **cubestore:** File not found for S3 during uploads ([a1b0087](https://github.com/cube-js/cube.js/commit/a1b00876c64e3206a9e0cbfa39f0440a865125a2))
- **cubestore:** Return http errors as JSON ([fb52f7d](https://github.com/cube-js/cube.js/commit/fb52f7dc647840747b86640c1466bbce78cc3817))

## [0.26.88](https://github.com/cube-js/cube.js/compare/v0.26.87...v0.26.88) (2021-04-10)

### Features

- Mysql Cube Store streaming ingests ([#2528](https://github.com/cube-js/cube.js/issues/2528)) ([0b36a6f](https://github.com/cube-js/cube.js/commit/0b36a6faa184766873ec3792785eb1aa5ca582af))

## [0.26.87](https://github.com/cube-js/cube.js/compare/v0.26.86...v0.26.87) (2021-04-10)

### Bug Fixes

- **cubestore:** Something wrong with downloading Cube Store before running it. ([208dd31](https://github.com/cube-js/cube.js/commit/208dd31f20aa64a5c79e143d40055ac2658d0745))

## [0.26.86](https://github.com/cube-js/cube.js/compare/v0.26.85...v0.26.86) (2021-04-09)

### Bug Fixes

- **cubestore:** installer - compability with windows. fix [#2520](https://github.com/cube-js/cube.js/issues/2520) ([e05db81](https://github.com/cube-js/cube.js/commit/e05db81cc7b885046b08b2a0f034e472e22c8b3e))

## [0.26.85](https://github.com/cube-js/cube.js/compare/v0.26.84...v0.26.85) (2021-04-09)

### Bug Fixes

- **@cubejs-client/playground:** token refresh, Vue apiUrl ([#2523](https://github.com/cube-js/cube.js/issues/2523)) ([c1085f2](https://github.com/cube-js/cube.js/commit/c1085f21125f9b8537bf0856e0be9fccfb3ecbb9))

## [0.26.84](https://github.com/cube-js/cube.js/compare/v0.26.83...v0.26.84) (2021-04-09)

### Bug Fixes

- **@cubejs-client/playground:** make live preview optional for PlaygroundQueryBuilder ([#2513](https://github.com/cube-js/cube.js/issues/2513)) ([db7a7f5](https://github.com/cube-js/cube.js/commit/db7a7f5fe9783a0c4834d9ace9ffbe710bd266b0))

### Features

- Make java deps as optional (to allow failture in development) ([74966cd](https://github.com/cube-js/cube.js/commit/74966cd2988c151e6f9f4d32c3b945f94dd7fa5d))

## [0.26.83](https://github.com/cube-js/cube.js/compare/v0.26.82...v0.26.83) (2021-04-07)

### Bug Fixes

- **server:** Compatibility with CLI to run external commands ([#2510](https://github.com/cube-js/cube.js/issues/2510)) ([44b51dd](https://github.com/cube-js/cube.js/commit/44b51dd648e73897e341e408f4ae37db8e3fb0fc))

## [0.26.82](https://github.com/cube-js/cube.js/compare/v0.26.81...v0.26.82) (2021-04-07)

### Bug Fixes

- **cli:** @ dependency not found ([3c78dd3](https://github.com/cube-js/cube.js/commit/3c78dd3f735310dca37ca7ebcb7f96bb603a7252))

### Features

- **@cubejs-client/playground:** run query button, disable query auto triggering ([#2476](https://github.com/cube-js/cube.js/issues/2476)) ([92a5d45](https://github.com/cube-js/cube.js/commit/92a5d45eca00e88e925e547a12c3f69b05bfafa6))

## [0.26.81](https://github.com/cube-js/cube.js/compare/v0.26.80...v0.26.81) (2021-04-07)

### Features

- Dev mode and live preview ([#2440](https://github.com/cube-js/cube.js/issues/2440)) ([1a7cde8](https://github.com/cube-js/cube.js/commit/1a7cde816ee231ea00d1a952f5000dcc5a8c0ca4))
- **jdbc-driver:** Initial support for TS ([179dcf3](https://github.com/cube-js/cube.js/commit/179dcf307ed3a0e53c4f143d438b1abab1462410))
- Introduce databricks-jdbc-driver ([bb0b31f](https://github.com/cube-js/cube.js/commit/bb0b31fb333f2aa379f11f6733c4efc17ec12dde))

## [0.26.80](https://github.com/cube-js/cube.js/compare/v0.26.79...v0.26.80) (2021-04-07)

### Bug Fixes

- TypeError: Cannot read property 'preAggregation' of undefined on empty pre-aggregations ([91ba03e](https://github.com/cube-js/cube.js/commit/91ba03e91545f31b498aa0fa47090ccd35085ae6)), closes [#2505](https://github.com/cube-js/cube.js/issues/2505)

## [0.26.79](https://github.com/cube-js/cube.js/compare/v0.26.78...v0.26.79) (2021-04-06)

### Bug Fixes

- **cubestore:** reduce serialization time for record batches ([cea5fd2](https://github.com/cube-js/cube.js/commit/cea5fd21c721b0252b3a068e8f324100ebfff546))
- sqlAlias on non partitioned rollups ([0675925](https://github.com/cube-js/cube.js/commit/0675925efb61a6492344b28179b7647eabb01a1d))
- **cubestore:** stream results for topk ([d2c7485](https://github.com/cube-js/cube.js/commit/d2c7485807cd20d15f8da333fcf31035dab0d529))

### Features

- Check Node.js version in dev-server/server ([d20e9c2](https://github.com/cube-js/cube.js/commit/d20e9c217e0acd894ecbfbf91e24c3d12cfba1c1))
- Make apiSecret optional, when JWK_URL is specified ([1a94590](https://github.com/cube-js/cube.js/commit/1a94590292baa6984a049b1b1634b9a04ea9d99a))

## [0.26.78](https://github.com/cube-js/cube.js/compare/v0.26.77...v0.26.78) (2021-04-05)

### Bug Fixes

- **@cubejs-backend/snowflake-driver:** Cube.js doesn't see pre-aggregations ([c9279bf](https://github.com/cube-js/cube.js/commit/c9279bfcb04230684676d1db87c18ebdc6a0f9d2)), closes [#2487](https://github.com/cube-js/cube.js/issues/2487)
- **athena-driver:** Wrong escape, Use " for column names, ` for table/schema ([62d8fcf](https://github.com/cube-js/cube.js/commit/62d8fcfb145ac04de72086b354bd583279617481))

## [0.26.77](https://github.com/cube-js/cube.js/compare/v0.26.76...v0.26.77) (2021-04-04)

### Bug Fixes

- Reduce Redis traffic for Refresh Scheduler by introducing single pre-aggregations load cache for a whole refresh cycle ([bdb69d5](https://github.com/cube-js/cube.js/commit/bdb69d55ee15e7d9ecca69a8e237d40ee666d051))

## [0.26.76](https://github.com/cube-js/cube.js/compare/v0.26.75...v0.26.76) (2021-04-03)

### Bug Fixes

- Reduce Redis traffic while querying immutable pre-aggregation partitions by introducing LRU in memory cache for refresh keys ([#2484](https://github.com/cube-js/cube.js/issues/2484)) ([76ea3c1](https://github.com/cube-js/cube.js/commit/76ea3c1a5227cfa8eda5b0ad6a09f41b71826866))

## [0.26.75](https://github.com/cube-js/cube.js/compare/v0.26.74...v0.26.75) (2021-04-02)

### Bug Fixes

- **cli:** Use binary instead of path in templates (Windows issue) ([#2483](https://github.com/cube-js/cube.js/issues/2483)) ([5ec7af7](https://github.com/cube-js/cube.js/commit/5ec7af74d0e144c967d15e465f2c63adaa370137))

## [0.26.74](https://github.com/cube-js/cube.js/compare/v0.26.73...v0.26.74) (2021-04-01)

### Features

- **cubestore:** top-k query planning and execution ([#2464](https://github.com/cube-js/cube.js/issues/2464)) ([3607a3a](https://github.com/cube-js/cube.js/commit/3607a3a69537feb815de470cf0a2ec9dde351ae8))

## [0.26.73](https://github.com/cube-js/cube.js/compare/v0.26.72...v0.26.73) (2021-04-01)

### Bug Fixes

- Pre-aggregation warmup should wait for partitions to build ([#2474](https://github.com/cube-js/cube.js/issues/2474)) ([3f05d94](https://github.com/cube-js/cube.js/commit/3f05d94eacdd3d5cdb7d1a5edbf647e3c767fb60))

### Features

- Introduce ITransportResponse for HttpTransport/WSTransport, fix [#2439](https://github.com/cube-js/cube.js/issues/2439) ([756bcb8](https://github.com/cube-js/cube.js/commit/756bcb8ae9cd6075382c01a88e46415dd7d024b3))

## [0.26.72](https://github.com/cube-js/cube.js/compare/v0.26.71...v0.26.72) (2021-03-29)

### Bug Fixes

- Persist live token ([#2451](https://github.com/cube-js/cube.js/issues/2451)) ([2f52cac](https://github.com/cube-js/cube.js/commit/2f52cac61b073a7aa20a9ddc2916edff52e1ff08))
- **athena-driver:** Use correct quoteIdentifier, fix [#2363](https://github.com/cube-js/cube.js/issues/2363) ([968b3b7](https://github.com/cube-js/cube.js/commit/968b3b7363b0d2c6c8385c717f3d1f29300c5caf))
- **cubestore:** Detect gnu libc without warning ([03e01e5](https://github.com/cube-js/cube.js/commit/03e01e5a30f88acfd61b4285461b25c26ef9ecfe))

## [0.26.71](https://github.com/cube-js/cube.js/compare/v0.26.70...v0.26.71) (2021-03-26)

### Bug Fixes

- **cubestore:** Remove tracing from logs ([10a264c](https://github.com/cube-js/cube.js/commit/10a264c1261bad9ae3f04753ac8c49dfe30efa63))

### Features

- Allow saving state of a scheduled refresh for warmup operations ([#2445](https://github.com/cube-js/cube.js/issues/2445)) ([0c637b8](https://github.com/cube-js/cube.js/commit/0c637b882b655652214f8ee543aa1a0fc74089bc))

## [0.26.70](https://github.com/cube-js/cube.js/compare/v0.26.69...v0.26.70) (2021-03-26)

### Bug Fixes

- **@cubejs-backend/dremio-driver:** readOnly external pre-aggregations is not working ([#2443](https://github.com/cube-js/cube.js/issues/2443)) Thanks to [@rongfengliang](https://github.com/rongfengliang) ! ([eee9d95](https://github.com/cube-js/cube.js/commit/eee9d95e56b8ac14c628d539a55cf94308128644))

### Features

- Vue chart renderers ([#2428](https://github.com/cube-js/cube.js/issues/2428)) ([bc2cbab](https://github.com/cube-js/cube.js/commit/bc2cbab22fee860cfc846d1207f6a83899198dd8))

## [0.26.69](https://github.com/cube-js/cube.js/compare/v0.26.68...v0.26.69) (2021-03-25)

### Bug Fixes

- **@cubejs-client/core:** Updated totalRow within ResultSet ([#2410](https://github.com/cube-js/cube.js/issues/2410)) ([91e51be](https://github.com/cube-js/cube.js/commit/91e51be6e5690dfe6ba294f75e768406fcc9d4a1))

### Features

- Introduce @cubejs-backend/maven ([#2432](https://github.com/cube-js/cube.js/issues/2432)) ([6dc6034](https://github.com/cube-js/cube.js/commit/6dc6034c3cdcc8e2c2b0568c218228a18b64f44b))

## [0.26.68](https://github.com/cube-js/cube.js/compare/v0.26.67...v0.26.68) (2021-03-25)

### Bug Fixes

- **cubestore:** make get active partitions a read operation ([#2416](https://github.com/cube-js/cube.js/issues/2416)) ([a1981f3](https://github.com/cube-js/cube.js/commit/a1981f3eadeb7359ab5cabdedf7ee2e5cfe9cc00))

### Features

- **@cubejs-client/vue:** query load event ([6045e8f](https://github.com/cube-js/cube.js/commit/6045e8f060b3702512f138b5c571db5deb6448f2))
- Live preview watcher, refactor cube cloud package ([#2418](https://github.com/cube-js/cube.js/issues/2418)) ([a311843](https://github.com/cube-js/cube.js/commit/a3118439afa5b04d658f79a94d9021f4c7f09472))

## [0.26.67](https://github.com/cube-js/cube.js/compare/v0.26.66...v0.26.67) (2021-03-24)

### Bug Fixes

- Make requireCubeStoreDriver private (to make it internal in d.ts) ([c0e6fb5](https://github.com/cube-js/cube.js/commit/c0e6fb569c5fea65146da9dfcf52e75800a22392))

## [0.26.66](https://github.com/cube-js/cube.js/compare/v0.26.65...v0.26.66) (2021-03-24)

### Bug Fixes

- **cubestore:** choose inplace aggregate in more cases ([#2402](https://github.com/cube-js/cube.js/issues/2402)) ([9ab6559](https://github.com/cube-js/cube.js/commit/9ab65599ea2a900bf63c4cb5e0a2544e5766822f))
- this.logger is not a function. fix [#2431](https://github.com/cube-js/cube.js/issues/2431) ([d715dc0](https://github.com/cube-js/cube.js/commit/d715dc0929673dbd0e7d9694e6f6d09b61f8c090))

### Features

- **cubestore:** add 'no upload' mode ([#2405](https://github.com/cube-js/cube.js/issues/2405)) ([38999b0](https://github.com/cube-js/cube.js/commit/38999b05a41849cae690b8900319340a99177fdb))

## [0.26.65](https://github.com/cube-js/cube.js/compare/v0.26.64...v0.26.65) (2021-03-24)

### Bug Fixes

- Warning/skip Cube Store on unsupported platforms ([c187e11](https://github.com/cube-js/cube.js/commit/c187e119b8747e1f6bb3fe2bd84f66ae3822ac7d))
- **cubestore:** use less read and write locks during planning ([#2420](https://github.com/cube-js/cube.js/issues/2420)) ([2d5d963](https://github.com/cube-js/cube.js/commit/2d5d96343dd2ef9204cb68c7a3e897dd28fa0d52))
- Allow using sub query dimensions in join conditions ([#2419](https://github.com/cube-js/cube.js/issues/2419)) ([496a075](https://github.com/cube-js/cube.js/commit/496a0755308f376666e732ea09a4c7816682cfb0))

## [0.26.64](https://github.com/cube-js/cube.js/compare/v0.26.63...v0.26.64) (2021-03-22)

### Bug Fixes

- **cubestore-driver:** Download x86-darwin for arm64-apple (for Rosetta2) ([562ea1a](https://github.com/cube-js/cube.js/commit/562ea1aa2dd34dc6b282ad8b4216be6c09b4240e))
- **cubestore-driver:** Drop limit for table names ([9e1317c](https://github.com/cube-js/cube.js/commit/9e1317c4bfd051a384d7dd2f6ce6df011ae795af))

## [0.26.63](https://github.com/cube-js/cube.js/compare/v0.26.62...v0.26.63) (2021-03-22)

### Bug Fixes

- **@cubejs-backend/dremio-driver:** Fix ILIKE operator ([#2397](https://github.com/cube-js/cube.js/issues/2397)) Thanks [@rongfengliang](https://github.com/rongfengliang)! ([d9e3c8d](https://github.com/cube-js/cube.js/commit/d9e3c8d2fd1e106d7c91a0801f13aa051f42db2e))
- **@cubejs-client/ngx:** wrong type reference ([#2407](https://github.com/cube-js/cube.js/issues/2407)) ([d6c4183](https://github.com/cube-js/cube.js/commit/d6c41838df53d18eae23b2d38f86435626568ccf))
- **@cubejs-client/react:** stateChangeHeuristics type definition ([b983344](https://github.com/cube-js/cube.js/commit/b9833441c74ad8901d3115a8dbf409d5eb9fb620))
- **cubestore:** Http message processing isn't forked ([844dab2](https://github.com/cube-js/cube.js/commit/844dab24114508c0c6ddbe068aa81d0f609250be))
- **cubestore:** Introduce meta store lock acquire timeouts to avoid deadlocks ([24b87e4](https://github.com/cube-js/cube.js/commit/24b87e41e172ab04d02e65f2343b928e3806e6bd))
- **cubestore:** Narrow check point lock life time ([b8e9003](https://github.com/cube-js/cube.js/commit/b8e9003a243d17e6ce5fa2ea8eabbf097cb42835))
- **cubestore:** Remove upstream directory when runs locally ([d5975f1](https://github.com/cube-js/cube.js/commit/d5975f13d34c46c03224d584995de6862e82f7ef))

### Features

- **cubestore:** Make query planning for indices explicit ([#2400](https://github.com/cube-js/cube.js/issues/2400)) ([a3e6c5c](https://github.com/cube-js/cube.js/commit/a3e6c5ce98974ffcb0280295e1c6182c1a46a1f4))
- **mysql-aurora-driver:** Support options ([a8e76c3](https://github.com/cube-js/cube.js/commit/a8e76c34df8a0be6a56c64db7998ac7d55c109c8))

## [0.26.62](https://github.com/cube-js/cube.js/compare/v0.26.61...v0.26.62) (2021-03-18)

### Features

- **cubejs-cli:** Create - ability to select a specific JDBC driver ([#1149](https://github.com/cube-js/cube.js/issues/1149)) ([33dc144](https://github.com/cube-js/cube.js/commit/33dc1440e628855e51b1a383d8fe64b39763a19f))

## [0.26.61](https://github.com/cube-js/cube.js/compare/v0.26.60...v0.26.61) (2021-03-18)

### Features

- **@cubejs-client/vue:** vue query builder ([#1824](https://github.com/cube-js/cube.js/issues/1824)) ([06ee13f](https://github.com/cube-js/cube.js/commit/06ee13f72ef33372385567ed5e1795087b4f5f53))

## [0.26.60](https://github.com/cube-js/cube.js/compare/v0.26.59...v0.26.60) (2021-03-16)

### Bug Fixes

- **docker:** Drop lerna bootstrap, because we migrated to yarn workspaces ([d0e16ef](https://github.com/cube-js/cube.js/commit/d0e16eff2ae581cf174f0fbd9608715ad2f10cc7))

### Features

- **@cubejs-client/playground:** Playground components ([#2329](https://github.com/cube-js/cube.js/issues/2329)) ([489dc12](https://github.com/cube-js/cube.js/commit/489dc12d7e9bfa87bfb3c8ffabf76f238c86a2fe))
- introduce GET /cubejs-system/v1/context ([d97858d](https://github.com/cube-js/cube.js/commit/d97858d528f6efa65400bf54b81b3a8a4039ecb0))
- **druid-driver:** Support Basic auth via username/passwd ([#2386](https://github.com/cube-js/cube.js/issues/2386)) ([4a89635](https://github.com/cube-js/cube.js/commit/4a896358e43e39555d69f1f028a165a3e4eaed6d))

## [0.26.59](https://github.com/cube-js/cube.js/compare/v0.26.58...v0.26.59) (2021-03-15)

### Features

- introduce @cubejs-backend/testing ([c38ce23](https://github.com/cube-js/cube.js/commit/c38ce23f3658effb8ef26f14148bffab3aafb3e5))

## [0.26.58](https://github.com/cube-js/cube.js/compare/v0.26.56...v0.26.58) (2021-03-14)

### Bug Fixes

- lock file conflict preventing dashboard generation, always propagate chartType to viz state ([#2369](https://github.com/cube-js/cube.js/issues/2369)) ([d372fe9](https://github.com/cube-js/cube.js/commit/d372fe9ee69542f9227a5a524e66b99cd69d5dff))

## [0.26.57](https://github.com/cube-js/cube.js/compare/v0.26.56...v0.26.57) (2021-03-14)

### Bug Fixes

- lock file conflict preventing dashboard generation, always propagate chartType to viz state ([#2369](https://github.com/cube-js/cube.js/issues/2369)) ([d372fe9](https://github.com/cube-js/cube.js/commit/d372fe9ee69542f9227a5a524e66b99cd69d5dff))

## [0.26.56](https://github.com/cube-js/cube.js/compare/v0.26.55...v0.26.56) (2021-03-13)

### Bug Fixes

- Expected one parameter but nothing found ([#2362](https://github.com/cube-js/cube.js/issues/2362)) ([ce490d2](https://github.com/cube-js/cube.js/commit/ce490d2de60c200832966824e6b0300ba91cde41))

### Features

- **cubestore:** Tracing support ([be5ab9b](https://github.com/cube-js/cube.js/commit/be5ab9b66d2bdc65962b0e04622d1db1f8608791))

## [0.26.55](https://github.com/cube-js/cube.js/compare/v0.26.54...v0.26.55) (2021-03-12)

### Bug Fixes

- **playground:** Cannot read property 'extendMoment' of undefined ([42fd694](https://github.com/cube-js/cube.js/commit/42fd694f28782413c25356530d6b07db9ea091e0))

### Features

- **elasticsearch-driver:** Ability to use USERNAME, PASSWORD and SSL\_\* env variables ([13c90cd](https://github.com/cube-js/cube.js/commit/13c90cd097b24c6e823dc4de76ae497ebfecc06b))
- **elasticsearch-driver:** Implement release ([2b88ed7](https://github.com/cube-js/cube.js/commit/2b88ed7c4105f0506555bd7cec63f2634f3149f4))

## [0.26.54](https://github.com/cube-js/cube.js/compare/v0.26.53...v0.26.54) (2021-03-12)

### Bug Fixes

- **@cubejs-client/react:** Prevent calling onVizStateChanged twice unless needed ([#2351](https://github.com/cube-js/cube.js/issues/2351)) ([3719265](https://github.com/cube-js/cube.js/commit/371926532032bd998a8d2fc200e78883a32f172d))
- running Angular dashboard in Docker ([#2353](https://github.com/cube-js/cube.js/issues/2353)) ([d1e2e9e](https://github.com/cube-js/cube.js/commit/d1e2e9efec1f433a5869de3377d8869683be4c94))
- **cubestore:** fix crash on empty sort order, temporarily disable full hash aggregate optimization ([#2348](https://github.com/cube-js/cube.js/issues/2348)) ([7dfd51a](https://github.com/cube-js/cube.js/commit/7dfd51a633f1f39e95bf908164a0abc4feeab37d))

### Features

- Suggest to install cubestore-driver to get external pre-agg layer ([2059e57](https://github.com/cube-js/cube.js/commit/2059e57a2aa7caf81691c083949395e9697d2bcb))
- Suggest to use rollUp & pre-agg for to join across data sources ([2cf1a63](https://github.com/cube-js/cube.js/commit/2cf1a630a9abaa7248526c284441e65212e82259))

## [0.26.53](https://github.com/cube-js/cube.js/compare/v0.26.52...v0.26.53) (2021-03-11)

### Bug Fixes

- **@cubejs-backend/dremio-driver:** Use ILIKE instead of LIKE for contains operator ([fb6fa73](https://github.com/cube-js/cube.js/commit/fb6fa73944e6fd53905c2a42d0ed8bacac40de08))
- **cubestore:** fix crash on empty results from workers ([9efb2a4](https://github.com/cube-js/cube.js/commit/9efb2a46ef57d4d3d5bef91f61ba7848568e1154))
- **cubestore:** Malloc trim inside worker processes ([9962fa1](https://github.com/cube-js/cube.js/commit/9962fa1259c85826abe4527f47518e826a0bec94))
- **cubestore:** Node.js 10 support, switched to cli-progress ([032a6ab](https://github.com/cube-js/cube.js/commit/032a6abe25028c09a2947e36a58ffd94d4334dca))
- **cubestore:** update arrow, fix test merge sort over unions ([#2326](https://github.com/cube-js/cube.js/issues/2326)) ([2c02d8f](https://github.com/cube-js/cube.js/commit/2c02d8f9599e3e7131ada82bcd714d814ebd100f))
- **cubestore:** use merge sort exec when aggregations are required ([#2330](https://github.com/cube-js/cube.js/issues/2330)) ([9a4603a](https://github.com/cube-js/cube.js/commit/9a4603a857c55b868fe20e8d45536d1f1188cf44))

### Features

- **cubestore:** Support boolean expressions in partition filters ([#2322](https://github.com/cube-js/cube.js/issues/2322)) ([6fa38d3](https://github.com/cube-js/cube.js/commit/6fa38d39caa0a65beda64c1fce4ccbbff8b101da))

## [0.26.52](https://github.com/cube-js/cube.js/compare/v0.26.51...v0.26.52) (2021-03-07)

### Bug Fixes

- **@cubejs-backend/cubestore-driver:** Error: connect ECONNREFUSED 127.0.0.1:3030 ([74f4683](https://github.com/cube-js/cube.js/commit/74f468362b34f0decac67e48f52d3756ba4dc647))

## [0.26.51](https://github.com/cube-js/cube.js/compare/v0.26.50...v0.26.51) (2021-03-07)

### Bug Fixes

- **@cubejs-backend/cubstore-driver:** Missing files in package.json ([487f4cc](https://github.com/cube-js/cube.js/commit/487f4ccbc74356db3d1de1644d157bae3a3c6ba6))

## [0.26.50](https://github.com/cube-js/cube.js/compare/v0.26.49...v0.26.50) (2021-03-07)

### Bug Fixes

- **cubestore:** Group by without aggregates returns empty results ([82902dd](https://github.com/cube-js/cube.js/commit/82902ddb894dc0a0d30e88bde33b0308136789b9))

### Features

- **cubestore:** Web Socket transport client ([#2228](https://github.com/cube-js/cube.js/issues/2228)) ([4a86d89](https://github.com/cube-js/cube.js/commit/4a86d896a6e815b74695e0387357abc0764bfd6c))

## [0.26.49](https://github.com/cube-js/cube.js/compare/v0.26.48...v0.26.49) (2021-03-05)

### Bug Fixes

- **@cubejs-client/playground:** error fix ([7ac72e3](https://github.com/cube-js/cube.js/commit/7ac72e39a5ee9f1a31733a6516fb99f620102cba))
- **@cubejs-client/react:** QueryRenderer TS type fix ([7c002c9](https://github.com/cube-js/cube.js/commit/7c002c920732b7aca6a3e3f7f346447e6f5d1b4d))
- **@cubejs-client/react:** type fixes ([#2289](https://github.com/cube-js/cube.js/issues/2289)) ([df2b24c](https://github.com/cube-js/cube.js/commit/df2b24c74958858b192292cf22e69915c66a6d26))
- **cubestore:** fully execute a single-node query on a worker ([#2288](https://github.com/cube-js/cube.js/issues/2288)) ([00156d0](https://github.com/cube-js/cube.js/commit/00156d03b38becbb472f0b93bfb1617506caa941))
- **cubestore:** Merge aggregate performance improvements ([a0dbb1a](https://github.com/cube-js/cube.js/commit/a0dbb1ab492f5da40216435b8bf9b98f1ffda5e5))
- **cubestore:** update arrow, provide hints for default index of CubeTableExec ([#2304](https://github.com/cube-js/cube.js/issues/2304)) ([e27b8a4](https://github.com/cube-js/cube.js/commit/e27b8a4bb9b35b77625103a72a73f98ccca225e0))
- **server:** overide env variables only on reload, fix [#2267](https://github.com/cube-js/cube.js/issues/2267) ([e1ee222](https://github.com/cube-js/cube.js/commit/e1ee2226eaa1467965963089f7a380eba5d5ef38))

### Features

- **@cubejs-client/react:** Adding queryError to QueryBuilder for showing dryRun errors ([#2262](https://github.com/cube-js/cube.js/issues/2262)) ([61bac0b](https://github.com/cube-js/cube.js/commit/61bac0b0dcdd81be908143334bd726eacc7b6e49))
- **cubestore:** Merge aggregate ([#2297](https://github.com/cube-js/cube.js/issues/2297)) ([31ebbbc](https://github.com/cube-js/cube.js/commit/31ebbbcb8a1ca2bc145b55fac00838cdeca0ea87))
- **elasticsearch-driver:** Introduce typings ([c0e519d](https://github.com/cube-js/cube.js/commit/c0e519d1daab5e04c11ddd1a54fe2fdb213b91f7))
- **elasticsearch-driver:** Support for elastic.co & improve docs ([#2240](https://github.com/cube-js/cube.js/issues/2240)) ([d8557f6](https://github.com/cube-js/cube.js/commit/d8557f6487ea98c19c055cc94b94b284dd273835))
- Enable SQL cache by default ([4b00a4f](https://github.com/cube-js/cube.js/commit/4b00a4f81f82fdbb6952a922907ce6cecadb8af7))

## [0.26.48](https://github.com/cube-js/cube.js/compare/v0.26.47...v0.26.48) (2021-03-04)

### Bug Fixes

- **cubestore:** publish issue ([5bd1c3b](https://github.com/cube-js/cube.js/commit/5bd1c3bb74d49a4f6c363f18c6b5bb4822a543cc))

## [0.26.47](https://github.com/cube-js/cube.js/compare/v0.26.46...v0.26.47) (2021-03-04)

### Bug Fixes

- **@cubejs-client/playground:** missing chart renderers ([a76b39f](https://github.com/cube-js/cube.js/commit/a76b39ff76666e338d94c8d3aedd5bb3db2c8957))
- **cubestore:** post-install - compatbility with non bash env ([4b0c9ef](https://github.com/cube-js/cube.js/commit/4b0c9ef19b20d4cbfaee63337b7a0025bb31e6e9))

## [0.26.46](https://github.com/cube-js/cube.js/compare/v0.26.45...v0.26.46) (2021-03-04)

### Bug Fixes

- **@cubejs-client/playground:** React and Angular code generation ([#2285](https://github.com/cube-js/cube.js/issues/2285)) ([4313bc8](https://github.com/cube-js/cube.js/commit/4313bc8cbaef819b1b9fc318b0bf9bfc06c1b114))

## [0.26.45](https://github.com/cube-js/cube.js/compare/v0.26.44...v0.26.45) (2021-03-04)

### Bug Fixes

- **@cubejs-client:** react, core TypeScript fixes ([#2261](https://github.com/cube-js/cube.js/issues/2261)). Thanks to @SteffeyDev! ([4db93af](https://github.com/cube-js/cube.js/commit/4db93af984e737d7a6a448facbc8227907007c5d))
- **@cubejs-schema-compiler:** addInterval / subtractInterval for Mssql ([#2239](https://github.com/cube-js/cube.js/issues/2239)) Thanks to [@florian-fischer-swarm](https://github.com/florian-fischer-swarm)! ([0930e15](https://github.com/cube-js/cube.js/commit/0930e1526612b92db2d192e4444a2c2a1d2d15ce)), closes [#2237](https://github.com/cube-js/cube.js/issues/2237)
- **cubestore:** attempt to exit gracefully on sigint (ctrl+c) ([#2255](https://github.com/cube-js/cube.js/issues/2255)) ([2b006f8](https://github.com/cube-js/cube.js/commit/2b006f80428a7202da06a9bded1b42c3d2753ced))
- **schema-compiler:** Lock antlr4ts to 0.5.0-alpha.4, fix [#2264](https://github.com/cube-js/cube.js/issues/2264) ([37b3a0d](https://github.com/cube-js/cube.js/commit/37b3a0d61433ae1b3e41c1264298d1409b7f95b7))

### Features

- Fetch JWK in background only ([954ce30](https://github.com/cube-js/cube.js/commit/954ce30a8d85e51360340558468a5ea4e2e4ca68))
- **cubestore:** Extract transport to separate service ([#2236](https://github.com/cube-js/cube.js/issues/2236)) ([921786b](https://github.com/cube-js/cube.js/commit/921786b8a80bc0b2ed3e50d798a0c5bab435ec5c))

## [0.26.44](https://github.com/cube-js/cube.js/compare/v0.26.43...v0.26.44) (2021-03-02)

### Bug Fixes

- **schema-compiler:** @types/ramda is a dev dependecy, dont ship it ([0a87d11](https://github.com/cube-js/cube.js/commit/0a87d1152f454e0e4d9c30c3295ee975dd493d0d))

### Features

- **examples:** add simple example project for `asyncModule()` ([d6bbab7](https://github.com/cube-js/cube.js/commit/d6bbab7dc7c67307dbb44fdfd23da78d41bc7d4e))

## [0.26.43](https://github.com/cube-js/cube.js/compare/v0.26.42...v0.26.43) (2021-03-02)

### Bug Fixes

- **@cubejs-client/playground:** loading state colors ([#2252](https://github.com/cube-js/cube.js/issues/2252)) ([c7d1da4](https://github.com/cube-js/cube.js/commit/c7d1da45d9c258ffa6411c0fc31ea29c0f1d3fc1))
- **cubestore:** post-install - right path ([fc77c8f](https://github.com/cube-js/cube.js/commit/fc77c8f1672a36205dadd90e2f33cfdf89eb330c))

### Features

- **cli:** Install Cube Store driver ([6153add](https://github.com/cube-js/cube.js/commit/6153add82ebb6abdd29424d773f8f3256ae3508e))

## [0.26.42](https://github.com/cube-js/cube.js/compare/v0.26.41...v0.26.42) (2021-03-02)

### Bug Fixes

- **cubestore:** allow to prune partitions with unbounded min or max ([#2213](https://github.com/cube-js/cube.js/issues/2213)) ([1649c09](https://github.com/cube-js/cube.js/commit/1649c094e24f9cdd00bbaef9693e9e623cbdd523))
- **cubestore-driver:** host must be 127.0.0.1 for CubeStoreDevDriver ([435d4d6](https://github.com/cube-js/cube.js/commit/435d4d68ba4e4d136fda210da6eda997bc22a686))

### Features

- **cubestore:** post-install - improvements ([871cadb](https://github.com/cube-js/cube.js/commit/871cadb57109b79f1f792bc2843983aa0712e648))

## [0.26.41](https://github.com/cube-js/cube.js/compare/v0.26.40...v0.26.41) (2021-03-01)

### Bug Fixes

- **@cubejs-client/playground:** chart library title ([0f5884a](https://github.com/cube-js/cube.js/commit/0f5884a7659364250f2f4578dfc9718138b18162))
- **@cubejs-client/playground:** meta refresh refactoring ([#2242](https://github.com/cube-js/cube.js/issues/2242)) ([b3d5085](https://github.com/cube-js/cube.js/commit/b3d5085c5dea9a563f6420927ccf2c91ea2e6ec1))

### Features

- **sqlite-driver:** bump sqlite3 from 4.2.0 to 5.0.2 ([#2127](https://github.com/cube-js/cube.js/issues/2127)) ([81f64df](https://github.com/cube-js/cube.js/commit/81f64df7148a659982a4e804e5712b615e803440))
- improve compression for binaries ([356120f](https://github.com/cube-js/cube.js/commit/356120f14758d0f81d0c1b330bf4282e68232727))

## [0.26.40](https://github.com/cube-js/cube.js/compare/v0.26.39...v0.26.40) (2021-03-01)

### Bug Fixes

- **cubestore:** CubeStoreHandler - startup timeout, delay execution before start ([db9a8bd](https://github.com/cube-js/cube.js/commit/db9a8bd19f2b892151dcc051e962d9c5bedb4669))

### Features

- **cubestore:** Compress binary for linux-gnu ([f0edd1a](https://github.com/cube-js/cube.js/commit/f0edd1a2af727a9b441165b40775dd6946897df0))
- **docker:** built-in cubestore ([15a2599](https://github.com/cube-js/cube.js/commit/15a25990ebb172c4cb4066f591bbbf543bc082c5))

## [0.26.39](https://github.com/cube-js/cube.js/compare/v0.26.38...v0.26.39) (2021-02-28)

### Bug Fixes

- **cubestore:** Malloc trim is broken for docker ([#2223](https://github.com/cube-js/cube.js/issues/2223)) ([5702cc4](https://github.com/cube-js/cube.js/commit/5702cc432c63d8db19a45d0938f4bbc073d05542))
- **cubestore:** use `spawn_blocking` on potentially expensive operations ([#2219](https://github.com/cube-js/cube.js/issues/2219)) ([a0f92e3](https://github.com/cube-js/cube.js/commit/a0f92e378f3c9531d4112f69a997ba32b8d09187))

### Features

- **cubestore:** Build binary with static linking for OpenSSL ([5cd961f](https://github.com/cube-js/cube.js/commit/5cd961f4e18e84abb00650a28c0b546b21893972))
- **cubestore:** Bump OpenSSL to 1.1.1h ([a1d091e](https://github.com/cube-js/cube.js/commit/a1d091e411933ed68ee823e31d5bce8703c83d06))
- **cubestore:** Web Socket transport ([#2227](https://github.com/cube-js/cube.js/issues/2227)) ([8821b9e](https://github.com/cube-js/cube.js/commit/8821b9e1378c17c54441bfb54a9ab387ae1e7044))
- Use single instance for Cube Store handler ([#2229](https://github.com/cube-js/cube.js/issues/2229)) ([35c140c](https://github.com/cube-js/cube.js/commit/35c140cac864b5b588fa88e90fec3d8b7de6acda))

## [0.26.38](https://github.com/cube-js/cube.js/compare/v0.26.37...v0.26.38) (2021-02-26)

### Bug Fixes

- Optimize compute time during pre-aggregations load checks for queries with many partitions ([b9b590b](https://github.com/cube-js/cube.js/commit/b9b590b2891674020b23bc2e56ea2ab8dc02fa10))
- **cubestore:** Reduce too verbose logging on slow queries ([1d62a47](https://github.com/cube-js/cube.js/commit/1d62a470bacf6b254b5f04f80ad44f24e84d6fb7))

## [0.26.37](https://github.com/cube-js/cube.js/compare/v0.26.36...v0.26.37) (2021-02-26)

### Bug Fixes

- **@cubejs-backend/dremio-driver:** Typo in error message ([49f0af8](https://github.com/cube-js/cube.js/commit/49f0af84533ceaea0f0074d9ebfc10a1b4bd8274))

### Features

- Optional SQL cache ([4431520](https://github.com/cube-js/cube.js/commit/443152058d63fee0c699fd436f7d3cc38403e0dd))

## [0.26.36](https://github.com/cube-js/cube.js/compare/v0.26.35...v0.26.36) (2021-02-25)

### Bug Fixes

- this.externalDriverFactory is not a function ([#2216](https://github.com/cube-js/cube.js/issues/2216)) ([e47d152](https://github.com/cube-js/cube.js/commit/e47d1523c0278d1087eb8f209021e6a5097dd38a))
- **cubestore:** speed up ingestion ([#2205](https://github.com/cube-js/cube.js/issues/2205)) ([22685ea](https://github.com/cube-js/cube.js/commit/22685ea2d313893479ee9eaf88073158b0059c91))

## [0.26.35](https://github.com/cube-js/cube.js/compare/v0.26.34...v0.26.35) (2021-02-25)

### Features

- Use Cube Store as default external storage for CUBEJS_DEV_MODE ([e526676](https://github.com/cube-js/cube.js/commit/e52667617e5e687c92d383045fb1a8d5fd19cab6))

## [0.26.34](https://github.com/cube-js/cube.js/compare/v0.26.33...v0.26.34) (2021-02-25)

### Bug Fixes

- **@cubejs-client/playground:** loading state ([#2206](https://github.com/cube-js/cube.js/issues/2206)) ([9f21f44](https://github.com/cube-js/cube.js/commit/9f21f44aa9eec03d66b30345631972b3553ecbd5))
- **@cubejs-client/playground:** pre-aggregation loader ([6f60988](https://github.com/cube-js/cube.js/commit/6f609885227f0b790aa183349dfe90e0cc5c9891))
- **@cubejs-client/playground:** slow query, loading position ([#2207](https://github.com/cube-js/cube.js/issues/2207)) ([aa0090a](https://github.com/cube-js/cube.js/commit/aa0090a31d49ad10d5d71eaedda89d582ba14f22))

### Features

- **cubestore:** speed up import with faster timestamp parsing ([#2203](https://github.com/cube-js/cube.js/issues/2203)) ([18958aa](https://github.com/cube-js/cube.js/commit/18958aab4597930111211de3e8497040bce9432e))

## [0.26.33](https://github.com/cube-js/cube.js/compare/v0.26.32...v0.26.33) (2021-02-24)

### Bug Fixes

- **docker:** Move back to scretch + build linux (gnu) via cross ([4e48acc](https://github.com/cube-js/cube.js/commit/4e48acc626abce800be27b234651cc22778e1b9a))

### Features

- **cubestore:** Wait for processing loops and MySQL password support ([#2186](https://github.com/cube-js/cube.js/issues/2186)) ([f3649f5](https://github.com/cube-js/cube.js/commit/f3649f536ef7d645c686c0a5c30ca2e570790d73))

## [0.26.32](https://github.com/cube-js/cube.js/compare/v0.26.31...v0.26.32) (2021-02-24)

### Features

- **cubestore:** installer - detect musl + support windows ([9af0d34](https://github.com/cube-js/cube.js/commit/9af0d34512ef01c108ce843929009316eed51f4b))

## [0.26.31](https://github.com/cube-js/cube.js/compare/v0.26.30...v0.26.31) (2021-02-23)

### Features

- **cubestore:** Build binary for Linux (Musl) :feelsgood: ([594956c](https://github.com/cube-js/cube.js/commit/594956c9ec685d8939bfae0221c8ad6537194ab1))
- **cubestore:** Build binary for Windows :neckbeard: ([3e64d03](https://github.com/cube-js/cube.js/commit/3e64d0362f392a0461c9dc31ea7aac1d1ac0f901))

## [0.26.30](https://github.com/cube-js/cube.js/compare/v0.26.29...v0.26.30) (2021-02-22)

### Features

- **bigquery-driver:** Upgrade @google-cloud/bigquery 5.x ([dbbd29c](https://github.com/cube-js/cube.js/commit/dbbd29cb41fcb093b37526e9c1d00b9170f38f41))

## [0.26.29](https://github.com/cube-js/cube.js/compare/v0.26.28...v0.26.29) (2021-02-22)

### Bug Fixes

- **cubestore:** switch from string to float in table value ([#2175](https://github.com/cube-js/cube.js/issues/2175)) ([05dc7d2](https://github.com/cube-js/cube.js/commit/05dc7d2174bee767ecff26acc6d4047a82f5f70d))

### Features

- **cubestore:** Ability to control process in Node.js ([f45e875](https://github.com/cube-js/cube.js/commit/f45e87560139beff1fc013f4a82b4b6a16799c1e))
- **cubestore:** installer - extract on fly ([290e264](https://github.com/cube-js/cube.js/commit/290e264a935a81a3c8181ec9a79730bf580232be))
- **docker:** Alpine - initial support for Cube Store ([6a48bcd](https://github.com/cube-js/cube.js/commit/6a48bcdb681ab47fba7c6d3e47b740fa58f7ee1a))

## [0.26.28](https://github.com/cube-js/cube.js/compare/v0.26.27...v0.26.28) (2021-02-21)

### Features

- **docker:** Upgrade default image to Buster (required for Cube Store) ([5908d28](https://github.com/cube-js/cube.js/commit/5908d2807877ba1d77213a8667cba6afe7acbd2b))

## [0.26.27](https://github.com/cube-js/cube.js/compare/v0.26.26...v0.26.27) (2021-02-20)

### Bug Fixes

- **cubestore:** Check CUBESTORE_SKIP_POST_INSTALL before calling script ([fd2cebb](https://github.com/cube-js/cube.js/commit/fd2cebb8d4e0c91b22ffcd3f78ad15db225e6fad))

## [0.26.26](https://github.com/cube-js/cube.js/compare/v0.26.25...v0.26.26) (2021-02-20)

### Bug Fixes

- docker build ([8661acd](https://github.com/cube-js/cube.js/commit/8661acdff2b88eabeb855b25e8395815c9ecfa26))

## [0.26.25](https://github.com/cube-js/cube.js/compare/v0.26.24...v0.26.25) (2021-02-20)

### Features

- **bigquery-driver:** Support changing location, CUBEJS_DB_BQ_LOCATION env variable ([204c73c](https://github.com/cube-js/cube.js/commit/204c73c4290760234242b1d3eecdd498bf848e0f))

## [0.26.24](https://github.com/cube-js/cube.js/compare/v0.26.23...v0.26.24) (2021-02-20)

### Features

- **cubestore:** Improve installer error reporting ([76dd651](https://github.com/cube-js/cube.js/commit/76dd6515fec8e809cd3b188e4c1c437707ff79a4))

## [0.26.23](https://github.com/cube-js/cube.js/compare/v0.26.22...v0.26.23) (2021-02-20)

### Features

- **cubestore:** Download binary from GitHub release. ([#2167](https://github.com/cube-js/cube.js/issues/2167)) ([9f90d2b](https://github.com/cube-js/cube.js/commit/9f90d2b27e480231c119af7b4e7039d0659b7b75))

## [0.26.22](https://github.com/cube-js/cube.js/compare/v0.26.21...v0.26.22) (2021-02-20)

### Features

- Introduce CUBEJS_AGENT_FRAME_SIZE to tweak agent performance ([4550b64](https://github.com/cube-js/cube.js/commit/4550b6475536af016dd5b584adb0f8d3f583dd2a))
- **cubestore:** Return success for create table only after table has been warmed up ([991a538](https://github.com/cube-js/cube.js/commit/991a538968b59104729a95a5be5d6b55a0aa6dcc))

## [0.26.21](https://github.com/cube-js/cube.js/compare/v0.26.20...v0.26.21) (2021-02-19)

### Bug Fixes

- **cubestore:** Cleanup memory after selects as well ([d9fd460](https://github.com/cube-js/cube.js/commit/d9fd46004caabc68145fa916a30b22b08c486a29))

## [0.26.20](https://github.com/cube-js/cube.js/compare/v0.26.19...v0.26.20) (2021-02-19)

### Bug Fixes

- **cubestore:** publish package ([60496e5](https://github.com/cube-js/cube.js/commit/60496e52e63e12b96bf750b468dc02686ddcdf5e))

## [0.26.19](https://github.com/cube-js/cube.js/compare/v0.26.18...v0.26.19) (2021-02-19)

### Bug Fixes

- **@cubejs-client/core:** manage boolean filters for missing members ([#2139](https://github.com/cube-js/cube.js/issues/2139)) ([6fad355](https://github.com/cube-js/cube.js/commit/6fad355ea47c0e9dda1e733345a0de0a0d99e7cb))
- **@cubejs-client/react:** replace dimension with member ([#2142](https://github.com/cube-js/cube.js/issues/2142)) ([622f398](https://github.com/cube-js/cube.js/commit/622f3987ee5d0d1313008f92e9d96aa586ac25c1))
- **@cubejs-client/react:** type fixes ([#2140](https://github.com/cube-js/cube.js/issues/2140)) ([bca1ff7](https://github.com/cube-js/cube.js/commit/bca1ff72b0204a3cce1d4ee658396b3e22adb1cd))
- **@cubejs-schema-compilter:** MSSQL remove order by from subqueries ([75c1903](https://github.com/cube-js/cube.js/commit/75c19035e2732adfb7c4711197bba57245e9673e))
- **druid-driver:** now() is not supported ([#2151](https://github.com/cube-js/cube.js/issues/2151)) ([78a21d1](https://github.com/cube-js/cube.js/commit/78a21d1a3db61241d215b676a81cbad272cc3cfb))

## [0.26.18](https://github.com/cube-js/cube.js/compare/v0.26.17...v0.26.18) (2021-02-18)

### Bug Fixes

- **@cubejs-client/playground:** revert path alias ([e577040](https://github.com/cube-js/cube.js/commit/e57704096914ce07fcc0ea1ae07435046f08701f))

## [0.26.17](https://github.com/cube-js/cube.js/compare/v0.26.16...v0.26.17) (2021-02-18)

### Bug Fixes

- **@cubejs-client/playground:** error handling, refresh api ([#2126](https://github.com/cube-js/cube.js/issues/2126)) ([ca730ea](https://github.com/cube-js/cube.js/commit/ca730eaaad9fce49a5e9e45c60aee67d082867f7))

## [0.26.16](https://github.com/cube-js/cube.js/compare/v0.26.15...v0.26.16) (2021-02-18)

### Bug Fixes

- **cubestore:** allow conversion from string to int in partition pruning ([#2128](https://github.com/cube-js/cube.js/issues/2128)) ([af920ca](https://github.com/cube-js/cube.js/commit/af920caa00e665b38c53a37b00cdf8170744e308))
- **cubestore:** CSV import silently fails on empty "" strings ([c82583a](https://github.com/cube-js/cube.js/commit/c82583a16ada49af8fab4f4faf18c35a5907f4ed))
- **cubestore:** Do not block scheduler loop during event processing ([3a0875e](https://github.com/cube-js/cube.js/commit/3a0875e23b441e35e42495ba14581b1cda28cc43))
- **cubestore:** fix index schema used for partition pruning ([#2125](https://github.com/cube-js/cube.js/issues/2125)) ([99635be](https://github.com/cube-js/cube.js/commit/99635befce1df8f7cb4b4fdd4bd577175fd68bf3))
- **cubestore:** put temporary downloads into a separate directory ([33986e9](https://github.com/cube-js/cube.js/commit/33986e959ee714f830c0e1f46da11a9b87a0ee3d))
- **cubestore:** Remove information_schema lock access to avoid any possible tokio thread blocks ([e7450a9](https://github.com/cube-js/cube.js/commit/e7450a94fc995f05dc1ae0abf4c11f6a2a4bbe81))
- **cubestore:** warmup partitions only once ([#2123](https://github.com/cube-js/cube.js/issues/2123)) ([c4c009c](https://github.com/cube-js/cube.js/commit/c4c009c382c0f93472a80fd4e3a6ab2ecb6cf185))
- continueWaitTimeout is ignored: expose single centralized continueWaitTimeout option ([#2120](https://github.com/cube-js/cube.js/issues/2120)) ([2735d2c](https://github.com/cube-js/cube.js/commit/2735d2c1b60c0a31e3970c65b7dd1c293351614e)), closes [#225](https://github.com/cube-js/cube.js/issues/225)

### Features

- **cubestore:** Build Linux (gnu) ([#2088](https://github.com/cube-js/cube.js/issues/2088)) ([3ef9576](https://github.com/cube-js/cube.js/commit/3ef95766ab1cde8923de7a887a451b43b07a253a))
- **cubestore:** Introduce Dependency Injection ([#2111](https://github.com/cube-js/cube.js/issues/2111)) ([0d97357](https://github.com/cube-js/cube.js/commit/0d97357dbc8ee1eaf858387f7e7a8572ef76562f))
- **cubestore:** Schedule imports on worker nodes ([c0cb164](https://github.com/cube-js/cube.js/commit/c0cb16448b70299844c643c3c07d65cb999e3c30))
- **druid-driver:** Support CUBEJS_DB_SSL ([d8124d0](https://github.com/cube-js/cube.js/commit/d8124d0a91c926ce0e1ffd21d6d057c164b01e79))

### Reverts

- Revert "fix(cubestore): cleanup warmup downloads on partition removal (#2025)" (#2121) ([f9ae6af](https://github.com/cube-js/cube.js/commit/f9ae6af65fb8a1dfdd1317d3e272de71a02f9f6f)), closes [#2025](https://github.com/cube-js/cube.js/issues/2025) [#2121](https://github.com/cube-js/cube.js/issues/2121)

## [0.26.15](https://github.com/cube-js/cube.js/compare/v0.26.14...v0.26.15) (2021-02-16)

### Bug Fixes

- **cubestore:** prune partitions during initial planning ([#2100](https://github.com/cube-js/cube.js/issues/2100)) ([294928a](https://github.com/cube-js/cube.js/commit/294928a50be3a693e8098b2635399a591ba4f81d))

### Features

- Support JWK in authentication, improve JWT configuration([#1962](https://github.com/cube-js/cube.js/issues/1962)) ([6e5d2ac](https://github.com/cube-js/cube.js/commit/6e5d2ac0dc05757498b95f308be41d1be86fe206))
- support SSL ca/cert/key in .env files as multiline text ([#2072](https://github.com/cube-js/cube.js/issues/2072)) ([d9f5154](https://github.com/cube-js/cube.js/commit/d9f5154e38c14417aa332f9bcd3d38ef00553e5a))
- **clickhouse-driver:** HTTPS and readOnly support ([3d60ead](https://github.com/cube-js/cube.js/commit/3d60ead920635eb85c76b51a5c301e2f1fb08cb6))
- **cubestore:** cleanup local copies of files removed remotely ([#2110](https://github.com/cube-js/cube.js/issues/2110)) ([9b3fd2e](https://github.com/cube-js/cube.js/commit/9b3fd2ee333229a107b957ea10d70b3ed16aaac1))
- **cubestore:** handle IN SQL expression in partition pruning ([#2101](https://github.com/cube-js/cube.js/issues/2101)) ([a7871d2](https://github.com/cube-js/cube.js/commit/a7871d2e67a25f5a4c518cc7bf619bbb64a1d782))

## [0.26.14](https://github.com/cube-js/cube.js/compare/v0.26.13...v0.26.14) (2021-02-15)

### Bug Fixes

- **cubejs-cli:** Deploy token from env and malformed error ([#2089](https://github.com/cube-js/cube.js/issues/2089)) ([4d97399](https://github.com/cube-js/cube.js/commit/4d97399fba97a4e9e24501eeff90ac7d28da35d3))
- **cubestore:** do not wait in queue for already downloaded files ([#2085](https://github.com/cube-js/cube.js/issues/2085)) ([13f4319](https://github.com/cube-js/cube.js/commit/13f4319218b4c3f81b3863e8c055907c55a6165a))
- **cubestore:** Multiline CSV parsing ([860cf39](https://github.com/cube-js/cube.js/commit/860cf39cfc7a7302efb79d8e850aa793be264b00))
- **cubestore:** UTF CSV parsing ([de2c6c3](https://github.com/cube-js/cube.js/commit/de2c6c36a0572daaa41f954bcbdd5ca131d92489))

### Features

- Build Cube Store binary for MacOS ([#2075](https://github.com/cube-js/cube.js/issues/2075)) ([5ef3612](https://github.com/cube-js/cube.js/commit/5ef3612e7ea23c08ecada5423c27bd54a609ad75))

## [0.26.13](https://github.com/cube-js/cube.js/compare/v0.26.12...v0.26.13) (2021-02-12)

### Bug Fixes

- **@cubejs-client/core:** security token impovements ([#2069](https://github.com/cube-js/cube.js/issues/2069)) ([94c1431](https://github.com/cube-js/cube.js/commit/94c14316efbea2cd53df247c82ed36efd6a81adb))
- **@cubejs-client/playground:** context refactor, piechart fix ([#2076](https://github.com/cube-js/cube.js/issues/2076)) ([cef9251](https://github.com/cube-js/cube.js/commit/cef9251908cc259671c314694ac36ef01558d151))
- **@cubejs-client/playground:** Meta error handling ([#2077](https://github.com/cube-js/cube.js/issues/2077)) ([1e6d591](https://github.com/cube-js/cube.js/commit/1e6d591f2857311f0f885ec7d275d110f93bc08a))

### Features

- **@cubejs-client/playground:** handle missing members ([#2067](https://github.com/cube-js/cube.js/issues/2067)) ([348b245](https://github.com/cube-js/cube.js/commit/348b245f4086095fbe1515d7821d631047f0008c))
- **cubestore:** prune partitions based on sort order at query time ([#2068](https://github.com/cube-js/cube.js/issues/2068)) ([84b6c9b](https://github.com/cube-js/cube.js/commit/84b6c9be887582f17ebc988fc1c58284d33fa483))
- **schema-compiler:** Generate parser by antlr4ts ([d8e68c7](https://github.com/cube-js/cube.js/commit/d8e68c77f4649ddc056322f2848c769e5311c6b1))
- **schema-compiler:** Wrap new generated parser. fix [#1798](https://github.com/cube-js/cube.js/issues/1798) ([c5fde21](https://github.com/cube-js/cube.js/commit/c5fde21cb4bbcd675a4eeb735cd0c48d7a3ade6d))
- Support for extra params in generating schema for tables. ([#1990](https://github.com/cube-js/cube.js/issues/1990)) ([a9b3df2](https://github.com/cube-js/cube.js/commit/a9b3df222f8eaca86724ed2e1c24c348b38f718c))
- type detection - check overflows for int/bigint ([393948a](https://github.com/cube-js/cube.js/commit/393948ae5fabf1c4b46ce4ea2b2dc22e8f012ee1))

## [0.26.12](https://github.com/cube-js/cube.js/compare/v0.26.11...v0.26.12) (2021-02-11)

### Bug Fixes

- **cubestore:** ms timestamp format for BigQuery support ([bf70716](https://github.com/cube-js/cube.js/commit/bf70716c9844417c6b9162a309bc8bf8f32a4036))
- **query-orchestrator:** detect negative int/decimal as strings ([#2051](https://github.com/cube-js/cube.js/issues/2051)) ([2b8b549](https://github.com/cube-js/cube.js/commit/2b8b549b022e357d85b9f4549a4ff61c9d39fbeb))
- **snowflake-driver:** Reorder snowflake execute statement to check for error before attempting to access rows.length ([#2053](https://github.com/cube-js/cube.js/issues/2053)) ([783f003](https://github.com/cube-js/cube.js/commit/783f0033754843364c809df4539529c73c3e49ff))
- UnhandledPromiseRejectionWarning: Error: Continue wait. fix [#1873](https://github.com/cube-js/cube.js/issues/1873) ([7f113f6](https://github.com/cube-js/cube.js/commit/7f113f61bef5b197cf26a3948a80d052a9cda79d))

## [0.26.11](https://github.com/cube-js/cube.js/compare/v0.26.10...v0.26.11) (2021-02-10)

### Bug Fixes

- **@cubejs-client/playground:** react code gen ([22b077e](https://github.com/cube-js/cube.js/commit/22b077eab5b1a486ab1ea93a50fc897c76c26b55))
- **cubestore:** cleanup warmup downloads on partition removal ([#2025](https://github.com/cube-js/cube.js/issues/2025)) ([3e77143](https://github.com/cube-js/cube.js/commit/3e771432138f05fb08f48e860479340107d634b5))
- CUBEJS_SCHEDULED_REFRESH_TIMER, fix [#1972](https://github.com/cube-js/cube.js/issues/1972) ([#1975](https://github.com/cube-js/cube.js/issues/1975)) ([dac7e52](https://github.com/cube-js/cube.js/commit/dac7e52ee0d3a118c9d69c9d030e58a3c048cca1))
- hostname: command not found. fix [#1998](https://github.com/cube-js/cube.js/issues/1998) ([#2027](https://github.com/cube-js/cube.js/issues/2027)) ([8d03cee](https://github.com/cube-js/cube.js/commit/8d03ceec27b2ac3dd057fce4dfc21ff97c6f12a2))

### Features

- **cli:** generate - allow to specify -d (dataSource) ([51c6a1c](https://github.com/cube-js/cube.js/commit/51c6a1c45483f259dcd719f9177936bb9dbcee8f))

## [0.26.10](https://github.com/cube-js/cube.js/compare/v0.26.9...v0.26.10) (2021-02-09)

### Bug Fixes

- Using .end() without the flush parameter is deprecated and throws from v.3.0.0 ([7078f41](https://github.com/cube-js/cube.js/commit/7078f4146572a4eb447b9ed6f64e071b86e0aca2))
- **cubestore:** Speed up CSV parsing ([#2032](https://github.com/cube-js/cube.js/issues/2032)) ([39e95f0](https://github.com/cube-js/cube.js/commit/39e95f0347e068012e5e7d60060168c271edab98))

## [0.26.9](https://github.com/cube-js/cube.js/compare/v0.26.8...v0.26.9) (2021-02-09)

### Bug Fixes

- **@cubejs-client/playground:** closing tag ([b0777b3](https://github.com/cube-js/cube.js/commit/b0777b3cf07e563280dfe9a41e1c7628f21b6360))
- **@cubejs-client/playground:** tab switch ([ddd93f3](https://github.com/cube-js/cube.js/commit/ddd93f31fbe8dd90854cca782f72193860b777ba))

## [0.26.8](https://github.com/cube-js/cube.js/compare/v0.26.7...v0.26.8) (2021-02-09)

### Bug Fixes

- **@cubejs-client/playground:** allow to store multiple tokens ([bf3f49a](https://github.com/cube-js/cube.js/commit/bf3f49ae2357e9a7eff5027cfa5573f8290a110b))
- **@cubejs-client/playground:** angular chart renderer ([1132103](https://github.com/cube-js/cube.js/commit/1132103b8c1d9b2ad0f986518dbe48bc3ed4d377))
- **cubestore:** Increase job timeout to 10 minutes ([a874f60](https://github.com/cube-js/cube.js/commit/a874f6035bf374432dcfc775374ad373aeb5c118))

### Reverts

- Revert "fix(cubestore): skip WAL, partition data directly during ingestion (#2002)" ([c9a6527](https://github.com/cube-js/cube.js/commit/c9a6527375f1223d69f3163d18343a5d52bc4f05)), closes [#2002](https://github.com/cube-js/cube.js/issues/2002)

## [0.26.7](https://github.com/cube-js/cube.js/compare/v0.26.6...v0.26.7) (2021-02-09)

### Bug Fixes

- **cubestore:** skip WAL, partition data directly during ingestion ([#2002](https://github.com/cube-js/cube.js/issues/2002)) ([5442fad](https://github.com/cube-js/cube.js/commit/5442fad9e804615de6271f555b6084e4c5e45c28))

### Features

- **@cubejs-client/playground:** security context editing ([#1986](https://github.com/cube-js/cube.js/issues/1986)) ([90f2365](https://github.com/cube-js/cube.js/commit/90f2365eb21313fb5ea7a80583622e0ed742005c))
- Support for Redis Sentinel + IORedis driver. fix [#1769](https://github.com/cube-js/cube.js/issues/1769) ([a5e7972](https://github.com/cube-js/cube.js/commit/a5e7972485fa97faaf9965b9794b0cf48256f484))
- Use REDIS_URL for IORedis options (with santinels) ([988bfe5](https://github.com/cube-js/cube.js/commit/988bfe5526be3506fe7b773d247ad89b3287fad4))

## [0.26.6](https://github.com/cube-js/cube.js/compare/v0.26.5...v0.26.6) (2021-02-08)

### Bug Fixes

- **cubestore:** Increase default split thresholds as memory issues are fixed ([7771a86](https://github.com/cube-js/cube.js/commit/7771a869ae51d58a61dce2ffcbd3dd0f9dc8c483))
- **server-core:** add support for async driverFactory ([312f90b](https://github.com/cube-js/cube.js/commit/312f90b0c6d92f053f1033ecf15efea1c10a4c0a))
- **sqlite-driver:** Use workaround for FLOOR ([#1931](https://github.com/cube-js/cube.js/issues/1931)) ([fe64feb](https://github.com/cube-js/cube.js/commit/fe64febd1b970c4b8396d05a859f16b3d9e5a8a8))

### Features

- **@cubejs-client/playground:** Building pre-aggregations message ([#1984](https://github.com/cube-js/cube.js/issues/1984)) ([e1fff5d](https://github.com/cube-js/cube.js/commit/e1fff5de4584df1bd8ef518e2436e1dcb4962975))
- Block from uploading files and folders (recur) starting from "." ([d549fc4](https://github.com/cube-js/cube.js/commit/d549fc4ab6eff19b3c5273cafb7427be1cbaca98))
- Improve typings for extendContext ([8e9c3bc](https://github.com/cube-js/cube.js/commit/8e9c3bcafc3f9acbc8e1a53113202b4be19bb12c))
- Partitions warmup ([#1993](https://github.com/cube-js/cube.js/issues/1993)) ([200dab1](https://github.com/cube-js/cube.js/commit/200dab193eee43649b0a3e9f5240bc4bf3576fcc))
- **cubestore:** Distributed jobs implementation ([#2001](https://github.com/cube-js/cube.js/issues/2001)) ([064ca30](https://github.com/cube-js/cube.js/commit/064ca3056ac3c52f8514ac5fb21f23f6b6b43244))
- **server-core:** Correct typings for driverFactory/dialectFactory ([51fb117](https://github.com/cube-js/cube.js/commit/51fb117883d2e04c3a8fce4494ac48e0938a0097))

## [0.26.5](https://github.com/cube-js/cube.js/compare/v0.26.4...v0.26.5) (2021-02-03)

### Bug Fixes

- **cubestore:** Return physical memory to the system after compaction ([cdfec78](https://github.com/cube-js/cube.js/commit/cdfec78a43d7a2c3a25c2ee1842147c8060e3fb4))
- **cubestore:** return physical memory to the system at rest ([#1981](https://github.com/cube-js/cube.js/issues/1981)) ([7249a7d](https://github.com/cube-js/cube.js/commit/7249a7d0a96eaea3bb142f56f816690bec908618))

### Features

- **cubestore:** Multiple location load support ([#1982](https://github.com/cube-js/cube.js/issues/1982)) ([2b509ec](https://github.com/cube-js/cube.js/commit/2b509ec3c50be0688d613d2cda1ac3f53e80e093))

## [0.26.4](https://github.com/cube-js/cube.js/compare/v0.26.3...v0.26.4) (2021-02-02)

### Bug Fixes

- coerceForSqlQuery - dont mutate securityContext, fix [#1974](https://github.com/cube-js/cube.js/issues/1974) ([95e0536](https://github.com/cube-js/cube.js/commit/95e05364712b9539b564f948dccb44b7367abe26))

## [0.26.3](https://github.com/cube-js/cube.js/compare/v0.26.2...v0.26.3) (2021-02-02)

### Bug Fixes

- **@cubejs-client/playground:** table presentation ([09c953d](https://github.com/cube-js/cube.js/commit/09c953d54b0aebdf9174f00750652fde3787fc50))

## [0.26.2](https://github.com/cube-js/cube.js/compare/v0.26.1...v0.26.2) (2021-02-01)

### Bug Fixes

- **cubestore:** sort data in column order from the index ([#1956](https://github.com/cube-js/cube.js/issues/1956)) ([342491e](https://github.com/cube-js/cube.js/commit/342491e720c1d1c6239005edcb8423777aaafb83))
- Cannot create proxy with a non-object as target or handler ([790a3ba](https://github.com/cube-js/cube.js/commit/790a3ba8887ca00b4ec9ed3e31c7ff4875ae26c5))

### Features

- **cubestore:** filter rowgroups when reading parquet files ([#1957](https://github.com/cube-js/cube.js/issues/1957)) ([4df454c](https://github.com/cube-js/cube.js/commit/4df454c69542d72cc7846b69259d03c69d1a80c8))

## [0.26.1](https://github.com/cube-js/cube.js/compare/v0.26.0...v0.26.1) (2021-02-01)

### Bug Fixes

- **api-gateway:** Await checkAuth middleware ([b3b8ccb](https://github.com/cube-js/cube.js/commit/b3b8ccb86f7a882b30c6d3df407ae024d1c08670))

# [0.26.0](https://github.com/cube-js/cube.js/compare/v0.25.33...v0.26.0) (2021-02-01)

### Features

- Storing userContext inside payload.u is deprecated, moved to root ([559bd87](https://github.com/cube-js/cube.js/commit/559bd8757d9754ab486eed88d1fdb0c280b82dc9))
- USER_CONTEXT -> SECURITY_CONTEXT, authInfo -> securityInfo ([fa5d17c](https://github.com/cube-js/cube.js/commit/fa5d17c0bb703b087f442c41a5bf0a3dca1c5faa))
- Warning about Node.js 10 deprecation ([7d15099](https://github.com/cube-js/cube.js/commit/7d15099462e60cb666bd9342580583ddf325c2ab))

## [0.25.33](https://github.com/cube-js/cube.js/compare/v0.25.32...v0.25.33) (2021-01-30)

### Bug Fixes

- **cubestore:** min/max statistics on parquet writes ([#1925](https://github.com/cube-js/cube.js/issues/1925)) ([c7b5bbf](https://github.com/cube-js/cube.js/commit/c7b5bbf5add13eeb67e63dc76d0fe30304f54ab0))
- Use local dates for pre-aggregations to avoid timezone shift discrepancies on DST timezones for timezone unaware databases like MySQL ([#1941](https://github.com/cube-js/cube.js/issues/1941)) ([f138e6f](https://github.com/cube-js/cube.js/commit/f138e6fa3d97492c34527d0f04917e78c374eb57))
- **cubestore:** Correct `convert_tz` implementation ([f06d91e](https://github.com/cube-js/cube.js/commit/f06d91ed43d3b9d2a9398c03e30f2a86d70b64f6))
- **cubestore:** Correct `convert_tz` implementation: correct sign ([999e00a](https://github.com/cube-js/cube.js/commit/999e00a96e61b9420adcd04e94b329b93a8a03bb))
- **schema-compiler:** Wrong dayOffset in refreshKey for not UTC computers ([#1938](https://github.com/cube-js/cube.js/issues/1938)) ([5fe3431](https://github.com/cube-js/cube.js/commit/5fe3431a8f7320555fc3dba101c72547a0f41dac))

### Features

- Warning on unconfigured scheduledRefreshContexts in multitenancy mode, fix [#1904](https://github.com/cube-js/cube.js/issues/1904) ([cf1984b](https://github.com/cube-js/cube.js/commit/cf1984b754d804383a72733d895bbb3a42544f2a))

## [0.25.32](https://github.com/cube-js/cube.js/compare/v0.25.31...v0.25.32) (2021-01-29)

### Bug Fixes

- **@cubejs-client/playground:** base64 file upload ([#1915](https://github.com/cube-js/cube.js/issues/1915)) ([8ba70fd](https://github.com/cube-js/cube.js/commit/8ba70fdd1d3aa8907cb3dd4e0f4bce34ac4e6e70))
- **cubestore:** Revert back naive in list OR implementation ([99e9ca2](https://github.com/cube-js/cube.js/commit/99e9ca293555911497a9d8d45d05255e845b47c8))
- **shared:** Value True is not valid for CUBEJS_SCHEDULED_REFRESH_TIMER ([99a5759](https://github.com/cube-js/cube.js/commit/99a5759e619824666b48c589a5c98c82c1817025))

### Features

- **cubestore:** Rebase to datafusion 2021-01-27 version ([#1930](https://github.com/cube-js/cube.js/issues/1930)) ([309ce8e](https://github.com/cube-js/cube.js/commit/309ce8edee0cc49f1e4dc0536f0ef593ceaa428f))

## [0.25.31](https://github.com/cube-js/cube.js/compare/v0.25.30...v0.25.31) (2021-01-28)

### Bug Fixes

- **@cubejs-client/core:** propagate time dimension to the drill down query ([#1911](https://github.com/cube-js/cube.js/issues/1911)) ([59701da](https://github.com/cube-js/cube.js/commit/59701dad6f6cb6d78954d18b309716a9d51aa6b7))
- **cubestore:** Adjust default memory usage ([04c4bc8](https://github.com/cube-js/cube.js/commit/04c4bc850a801e5833641a4904144bb5e9f36ff8))
- **cubestore:** Bring back WAL removal ([1b2bd40](https://github.com/cube-js/cube.js/commit/1b2bd40131314afbb4e024867666615b199c80d3))
- **cubestore:** Drop temporary files on CSV import ([ab0affb](https://github.com/cube-js/cube.js/commit/ab0affb6ceed719444b3e0df6f8f56439d5b36a7))
- **cubestore:** Error processing event DeletePartition: No such object ([0208234](https://github.com/cube-js/cube.js/commit/0208234ee1f5f76efb7c7d2db6eee448af2f097d))
- **cubestore:** index out of bounds: the len is 0 but the index is 18446744073709551615 ([21bb226](https://github.com/cube-js/cube.js/commit/21bb226f911236353ed8625a674fe1fe1f1b7f51))
- **cubestore:** Limit memory usage on compaction -- zero compaction threshold case ([#1895](https://github.com/cube-js/cube.js/issues/1895)) ([fb516f5](https://github.com/cube-js/cube.js/commit/fb516f5f04790f51a8e37f24eb875e00704b1954))
- **cubestore:** Support single partition compactions ([c5eac36](https://github.com/cube-js/cube.js/commit/c5eac3655be86cfdf5d9035a5c98d72025521459))

### Features

- Ability to specify dataSource from request ([e8fe83a](https://github.com/cube-js/cube.js/commit/e8fe83abacfd2a47ad440fa2d52f3bf78d7a8c72))
- Disable graceful shutdown by default ([#1903](https://github.com/cube-js/cube.js/issues/1903)) ([19e2f54](https://github.com/cube-js/cube.js/commit/19e2f5491ba8f8b3aa76762382da98400fb71a1b))

## [0.25.30](https://github.com/cube-js/cube.js/compare/v0.25.29...v0.25.30) (2021-01-26)

### Bug Fixes

- **cubestore:** add custom type 'bytes', a synonym for 'varbinary' ([#1890](https://github.com/cube-js/cube.js/issues/1890)) ([4efc291](https://github.com/cube-js/cube.js/commit/4efc2914a0f9f0c960bf9af00a56c5562ac02bd4))
- **shared:** 1st interval unexpected call on onDuplicatedStateResolved ([6265503](https://github.com/cube-js/cube.js/commit/62655036d337e2ca491d0bda4f7f1b98a6811c4c))

### Features

- **cubestore:** allow to import base64-encoded bytes in CSV ([#1891](https://github.com/cube-js/cube.js/issues/1891)) ([2f43afa](https://github.com/cube-js/cube.js/commit/2f43afaa3776fb70526196734b1f3e97b942770e))

## [0.25.29](https://github.com/cube-js/cube.js/compare/v0.25.28...v0.25.29) (2021-01-26)

### Bug Fixes

- **cubestore:** CSV import escape sequence ([a3e118e](https://github.com/cube-js/cube.js/commit/a3e118e7be072d0763a2f0aa1044350e0a4ddd90))
- **cubestore:** More CSV import escaping cases ([9419128](https://github.com/cube-js/cube.js/commit/9419128d2653405c51d60c8b79c4d07971b54e0f))
- **cubestore:** Support NULL values in CSV import ([529e5ac](https://github.com/cube-js/cube.js/commit/529e5ac9f3d31fb8b7962c9dced6a5d8dd94c26a))

### Features

- **cubestore:** CUBESTORE_WAL_SPLIT_THRESHOLD env variable ([0d7e550](https://github.com/cube-js/cube.js/commit/0d7e550d825c129f5b21a6963182faebaa882132))
- Improve logs for RefreshScheduler and too long execution ([d0f1f1b](https://github.com/cube-js/cube.js/commit/d0f1f1bbc32473452c763d22ff8ee728c74f6462))

## [0.25.28](https://github.com/cube-js/cube.js/compare/v0.25.27...v0.25.28) (2021-01-25)

### Bug Fixes

- dependency version resolution ([f314ec5](https://github.com/cube-js/cube.js/commit/f314ec54d15c4c01b9eca602f5587d0896bdca23))
- **cubestore:** merge() and cardinality() now work on empty inputs ([#1875](https://github.com/cube-js/cube.js/issues/1875)) ([0e35861](https://github.com/cube-js/cube.js/commit/0e358612a133cdd0004d5e03b47e963a8dc66df6))

### Features

- **cubestore:** HyperLogLog++ support for BigQuery ([#1872](https://github.com/cube-js/cube.js/issues/1872)) ([357ecef](https://github.com/cube-js/cube.js/commit/357eceffcf56f46634b4f7de7550cfbe77911c2d))

## [0.25.27](https://github.com/cube-js/cube.js/compare/v0.25.26...v0.25.27) (2021-01-25)

### Bug Fixes

- **mongobi-driver:** authSwitchHandler api is deprecated, please use new authPlugins api ([5ee9349](https://github.com/cube-js/cube.js/commit/5ee93497972f1cbd0436f0179c2959867e4b3101))

### Features

- **server:** Dont accept new request(s) during shutdown ([#1855](https://github.com/cube-js/cube.js/issues/1855)) ([78f8f0b](https://github.com/cube-js/cube.js/commit/78f8f0ba395f061c5acb9055c2a83c2d573b950c))

## [0.25.26](https://github.com/cube-js/cube.js/compare/v0.25.25...v0.25.26) (2021-01-25)

### Features

- BigQuery CSV pre-aggregation download support ([#1867](https://github.com/cube-js/cube.js/issues/1867)) ([5a2ea3f](https://github.com/cube-js/cube.js/commit/5a2ea3f27058a01bf08f697495c8ccce5abf9fa2))

## [0.25.25](https://github.com/cube-js/cube.js/compare/v0.25.24...v0.25.25) (2021-01-24)

### Bug Fixes

- **cubestore:** Ignore CUBEJS_DB_SSL env ([86f06f7](https://github.com/cube-js/cube.js/commit/86f06f7955b3d230c9398953fec76c2569460701))

### Features

- **cubestore:** Migrate to tokio v1.0 and implement GCS support ([#1864](https://github.com/cube-js/cube.js/issues/1864)) ([803efd2](https://github.com/cube-js/cube.js/commit/803efd2a36d08f604af2ee31f14ddfeb2abe9468))

## [0.25.24](https://github.com/cube-js/cube.js/compare/v0.25.23...v0.25.24) (2021-01-22)

### Bug Fixes

- Non default data source cache key and table schema queries are forwarded to the default data source ([2f7c672](https://github.com/cube-js/cube.js/commit/2f7c67292468da60faea284751bf8c71d2e051f5))
- Non default data source cache key and table schema queries are forwarded to the default data source: broken test ([#1856](https://github.com/cube-js/cube.js/issues/1856)) ([8aad3f5](https://github.com/cube-js/cube.js/commit/8aad3f52f476836df4f93c266af96f30ceb57131))

## [0.25.23](https://github.com/cube-js/cube.js/compare/v0.25.22...v0.25.23) (2021-01-22)

### Bug Fixes

- Map int2/4/8 to generic int type. fix [#1796](https://github.com/cube-js/cube.js/issues/1796) ([78e20eb](https://github.com/cube-js/cube.js/commit/78e20eb304eda3086cda7dbc4ea5d33ef877facb))
- **api-gateway:** Validate a case when chrono can return empty array ([#1848](https://github.com/cube-js/cube.js/issues/1848)) ([e7349f7](https://github.com/cube-js/cube.js/commit/e7349f7bd71800e51a9c1d7cefecc8783bd886d6))
- **cubestore:** Increase queue buffer to avoid lagging on wait ([2605bdf](https://github.com/cube-js/cube.js/commit/2605bdf69cd4c19b80a51ec0f526c8d1dffb9681))
- **cubestore:** Queue uploads and downloads to avoid reads on unfinished S3 streams ([b94eb26](https://github.com/cube-js/cube.js/commit/b94eb266b98b2cbec494882931df8f3fbb40882a))
- **cubestore:** Speed up S3 uploads ([d7062c8](https://github.com/cube-js/cube.js/commit/d7062c825c96412c43e36b3fd09a2f630396117c))

### Features

- **schema-compiler:** Move some parts to TS ([2ad0e2e](https://github.com/cube-js/cube.js/commit/2ad0e2e377fce52f4967fc73ae2486d4365f3ac4))

## [0.25.22](https://github.com/cube-js/cube.js/compare/v0.25.21...v0.25.22) (2021-01-21)

### Bug Fixes

- **cubestore:** Add curl as a dependency for certs ([d364fc4](https://github.com/cube-js/cube.js/commit/d364fc454f013667001a9932ebd1e894c5a4b5fc))
- **cubestore:** Try to fix Invalid Parquet file on worker nodes ([aab87c8](https://github.com/cube-js/cube.js/commit/aab87c85ddaca156d20895efd29785b673bc5e2d))
- **playground:** Create schema directory on changing env ([f99f6cc](https://github.com/cube-js/cube.js/commit/f99f6cc658ffdd9f2ec58dcbfa3b2be67ca67bf8))
- **server:** Unexpected kill on graceful shutdown ([fc99239](https://github.com/cube-js/cube.js/commit/fc992398037719e5d7cc56b35f5e52e59d7c71f2))
- **server-core:** Clear refresh uncaughtException for DevServer ([1ea4882](https://github.com/cube-js/cube.js/commit/1ea4882c8afd8b13f7637bb641120dc104096515))

### Features

- Log warnings from createCancelableInterval ([44d09c4](https://github.com/cube-js/cube.js/commit/44d09c44da6ddfa845dd457bb766172698c8f334))
- **@cubejs-client/playground:** Database connection wizard ([#1671](https://github.com/cube-js/cube.js/issues/1671)) ([ba30883](https://github.com/cube-js/cube.js/commit/ba30883617c806c9f19ed6c879d0b0c2d656aae1))
- **cubestore:** Add column type for HLL ([#1827](https://github.com/cube-js/cube.js/issues/1827)) ([df97052](https://github.com/cube-js/cube.js/commit/df970523c5413a171578e14abdb792ce4c260fbe))
- **server:** Guard multiple restart in same time ([45f19b8](https://github.com/cube-js/cube.js/commit/45f19b84cd2eb2818e5053a4d5ae025b8aa2497c))

## [0.25.21](https://github.com/cube-js/cube.js/compare/v0.25.20...v0.25.21) (2021-01-19)

### Bug Fixes

- **@cubejs-backend/api-gateway:** readiness fix ([#1791](https://github.com/cube-js/cube.js/issues/1791)) ([d5dad60](https://github.com/cube-js/cube.js/commit/d5dad60e1dda655d67d5d8df4f4d6ee4345dbe42))
- **@cubejs-backend/query-orchestrator:** prevent generic pool infinite loop ([#1793](https://github.com/cube-js/cube.js/issues/1793)) ([d4129c4](https://github.com/cube-js/cube.js/commit/d4129c4d71b4afa66f62ae5d9666fcd9a08d9187))
- **@cubejs-client/playground:** avoid styles override ([4bdae02](https://github.com/cube-js/cube.js/commit/4bdae024d3d866acebb054e01106ed621a51a445))

### Features

- **cubestore:** Cluster support ([4846080](https://github.com/cube-js/cube.js/commit/48460807c9228a0b4db9193e6b36b5895a5d57b8))
- **cubestore:** S3 sub path support ([0cabd4c](https://github.com/cube-js/cube.js/commit/0cabd4c0820af8c4e4dbd338588bd49274e294c2))
- **schema-compiler:** Initial support for TS ([5926067](https://github.com/cube-js/cube.js/commit/5926067bf5314c7cbddfe59f26dd0ae3b8b60293))

## [0.25.20](https://github.com/cube-js/cube.js/compare/v0.25.19...v0.25.20) (2021-01-15)

### Bug Fixes

- Remove unnecessary `SELECT 1` during scheduled refresh. Fixes [#1592](https://github.com/cube-js/cube.js/issues/1592) ([#1786](https://github.com/cube-js/cube.js/issues/1786)) ([66f9d91](https://github.com/cube-js/cube.js/commit/66f9d91d12b1853b69903475af8338bfa586026b))

## [0.25.19](https://github.com/cube-js/cube.js/compare/v0.25.18...v0.25.19) (2021-01-14)

### Bug Fixes

- Do not renew historical refresh keys during scheduled refresh ([e5fbb12](https://github.com/cube-js/cube.js/commit/e5fbb120d5e848468999de59ba536b95be2e67e9))

### Features

- **cubestore:** Improve support for the binary data type ([#1759](https://github.com/cube-js/cube.js/issues/1759)) ([925f813](https://github.com/cube-js/cube.js/commit/925f81368494e1128eadd097462814d9a87493f8))

## [0.25.18](https://github.com/cube-js/cube.js/compare/v0.25.17...v0.25.18) (2021-01-14)

### Bug Fixes

- **@cubejs-client/playground:** ng number, crash ([#1770](https://github.com/cube-js/cube.js/issues/1770)) ([a2bce37](https://github.com/cube-js/cube.js/commit/a2bce37db86efe521410ddf7f30030c8c65b210b))
- **server:** Wrong path to typings. fix [#1766](https://github.com/cube-js/cube.js/issues/1766) ([59d279d](https://github.com/cube-js/cube.js/commit/59d279deef446fdfc6ebdd40c3bb9817b618fe88))

### Features

- **server:** Kill Cube.js if it's stuck in gracefull shutdown ([0874de8](https://github.com/cube-js/cube.js/commit/0874de8a1b7216d783d947914e2396b35f17d130))

## [0.25.17](https://github.com/cube-js/cube.js/compare/v0.25.16...v0.25.17) (2021-01-13)

### Reverts

- Revert "feat(server): Throw an exception when env file is not correct" (#1763) ([f899786](https://github.com/cube-js/cube.js/commit/f899786a424a56326bdff9a6c4c87bb160f994d6)), closes [#1763](https://github.com/cube-js/cube.js/issues/1763)

## [0.25.16](https://github.com/cube-js/cube.js/compare/v0.25.15...v0.25.16) (2021-01-13)

### Bug Fixes

- **cli:** Broken jdbc installation ([b37a134](https://github.com/cube-js/cube.js/commit/b37a134f121fd933bfb793462563486ff85273fe))
- **server:** configuration reload should overrite old env variables ([bbb5c4a](https://github.com/cube-js/cube.js/commit/bbb5c4aad249a8f62e0edccf29871abcd95feca6))
- **snowflake-driver:** Handle null values for numbers, dates. fix [#1741](https://github.com/cube-js/cube.js/issues/1741) ([51c2bb2](https://github.com/cube-js/cube.js/commit/51c2bb21d4d46daac89f21921f5c61982ab6547f))
- Pass dbType in DialectContext for dialectFactory ([#1756](https://github.com/cube-js/cube.js/issues/1756)) ([5cf88bf](https://github.com/cube-js/cube.js/commit/5cf88bf1eaaed1c70223631a76e3de77ecae46b7)), closes [#1728](https://github.com/cube-js/cube.js/issues/1728)

### Features

- **cubestore:** Filter mirroring push down optimization ([49685d3](https://github.com/cube-js/cube.js/commit/49685d3ca3a20b561d6fbfdb66a6b8cdd7b6b755))
- **server:** Throw an exception when env file is not correct ([abff7fc](https://github.com/cube-js/cube.js/commit/abff7fcc11b7c226346ecb9524b98548fdc4fe09))

## [0.25.15](https://github.com/cube-js/cube.js/compare/v0.25.14...v0.25.15) (2021-01-12)

### Bug Fixes

- Ensure agent events are delivered with a 50k backlog ([bf0b9ec](https://github.com/cube-js/cube.js/commit/bf0b9ec9f75b5e5f996fd4da855371ef6cd641f2))

### Features

- **@cubejs-client/playground:** display slow query warning ([#1649](https://github.com/cube-js/cube.js/issues/1649)) ([ce33f88](https://github.com/cube-js/cube.js/commit/ce33f8849b96ac25dd6f242b61f81e29600f511a))
- introduce graceful shutdown ([#1683](https://github.com/cube-js/cube.js/issues/1683)) ([118232f](https://github.com/cube-js/cube.js/commit/118232f56b6c66b7dff6ed11e914ccc107a25881))

## [0.25.14](https://github.com/cube-js/cube.js/compare/v0.25.13...v0.25.14) (2021-01-11)

### Bug Fixes

- **@cubejs-client/react:** useCubeQuery - clear resultSet on exception ([#1734](https://github.com/cube-js/cube.js/issues/1734)) ([a5d19ae](https://github.com/cube-js/cube.js/commit/a5d19aecffc6a613f6e0f0d9346143c4f2e335be))
- **cubestore:** filter pushdown optimization for aliased tables doesn't work ([decfa3a](https://github.com/cube-js/cube.js/commit/decfa3a9110fb4c82d125793838196eb0e0ac9a8))
- **cubestore:** Fix parquet-format dependency as new one isn't compatible with current arrow version ([f236314](https://github.com/cube-js/cube.js/commit/f2363147796f6724c3f53d6f62527a6ec93f8fa0))
- **cubestore:** Invalid argument error: Unable to get field named during merge resort ([031f4fe](https://github.com/cube-js/cube.js/commit/031f4fec492e821a5ef799e461785024aad09a6f))
- **cubestore:** Log 0.4.12 dependency is broken ([a484b12](https://github.com/cube-js/cube.js/commit/a484b12d21c7c445ec94687950efb29aba205ebf))
- **cubestore:** Merge sort early exit ([ddb292f](https://github.com/cube-js/cube.js/commit/ddb292fecc0db7f0a84199c9f932198d145a3170))
- **cubestore:** Merge sort seg fault on empty batch ([4eb1f28](https://github.com/cube-js/cube.js/commit/4eb1f2872cea59f5c5352c28e5980d7fb276d98d))
- **cubestore:** Remove debug output ([8706798](https://github.com/cube-js/cube.js/commit/8706798dc441895b3718167f4eac82e29b319298))
- **cubestore:** Union merge sort support ([8cd3994](https://github.com/cube-js/cube.js/commit/8cd3994bbaafdaeb2c1ccc77c6d786ad7b85c987))
- **gateway:** Allow healthchecks to be requested without auth ([95c0c57](https://github.com/cube-js/cube.js/commit/95c0c57d739e6ce46de958883d7dbfe04616a7a0))

### Features

- **cubestore:** Add CUBESTORE_DATA_DIR env variable ([3571916](https://github.com/cube-js/cube.js/commit/3571916c1e29ba84cd61f83aeb6611632a78b176))
- **cubestore:** Float column type support ([f427598](https://github.com/cube-js/cube.js/commit/f4275985fdfc0679b9ba89d86f7586b8c814d9dc))
- **cubestore:** Merge resort implementation to support three tables merge joins ([3fa675b](https://github.com/cube-js/cube.js/commit/3fa675bf9d7109c58847fe93219574e0cf287483))
- **docker:** Upgrade Node.js to Node v12.20.1 (security release) ([097a11a](https://github.com/cube-js/cube.js/commit/097a11a81402f26c90441d93bcdd8421f89bf2e8))

## [0.25.13](https://github.com/cube-js/cube.js/compare/v0.25.12...v0.25.13) (2021-01-07)

### Bug Fixes

- Guard from `undefined` dataSource in queue key ([6ae1fd6](https://github.com/cube-js/cube.js/commit/6ae1fd60a1e67bc73c0630b7de36b598397ce22b))
- **cubestore:** Root Cargo.toml isn't used for docker build ([8030fe3](https://github.com/cube-js/cube.js/commit/8030fe3796acc69e6dcd88c728430007c91dded6))
- **cubestore:** Set default scale to 5 for floats ([98d85eb](https://github.com/cube-js/cube.js/commit/98d85eb641e77225267a5e63351a4d72cf1c9531))
- **cubestore:** Support Utf8 to Boolean cast ([7ac9892](https://github.com/cube-js/cube.js/commit/7ac98921bb6c9999e1b59499fefb9a68578513fd))
- **cubestore:** Support Utf8 to Int64Decimal cast ([c523b46](https://github.com/cube-js/cube.js/commit/c523b4683c747d0cfcf9cc32c2319e83d56e7758))
- Reduce agent event queue on network failures ([548fb9a](https://github.com/cube-js/cube.js/commit/548fb9a23fe5fafa9d54c92c1d9425b83fafffbe))

### Features

- **cubestore:** Drop unused chunks and partitions after compaction and repartition ([94895a2](https://github.com/cube-js/cube.js/commit/94895a20bff4c6e2932e547f6c49fa5624644098))
- **cubestore:** Float with exp number support ([6e92c55](https://github.com/cube-js/cube.js/commit/6e92c5555efc8f76722994b2988d98850f9d10e9))

## [0.25.12](https://github.com/cube-js/cube.js/compare/v0.25.11...v0.25.12) (2021-01-05)

### Bug Fixes

- **@cubejs-client/react:** updated peer dependency version ([442a979](https://github.com/cube-js/cube.js/commit/442a979e9d5509ffcb71e48d42a4e4944eae98e1))
- **cubestore:** Join aliasing fails after rebase ([67ffd4d](https://github.com/cube-js/cube.js/commit/67ffd4df087d6a27855ab3cbe334125f3ff43293))

### Features

- **cubestore:** Distribute unions across workers the same way as partitions ([52f8a77](https://github.com/cube-js/cube.js/commit/52f8a771cc97d23e86e9e00a6fe7e5d5f291bda9))

## [0.25.11](https://github.com/cube-js/cube.js/compare/v0.25.10...v0.25.11) (2021-01-04)

### Bug Fixes

- **cubestore:** File not found if upstream mounted as a network volume ([68822ec](https://github.com/cube-js/cube.js/commit/68822ec8e0f2e7fe92ae80af1cb1b52cbbc22a61))
- **cubestore:** Fix write metastore locking ([cbbacce](https://github.com/cube-js/cube.js/commit/cbbacce44c4f8af734095d985d834071fb2f8b24))
- **cubestore:** Handle corrupted log files and discard them with error ([00a1c1a](https://github.com/cube-js/cube.js/commit/00a1c1a532be8cbb20a908c4bdb0e4f5eb802e71))
- **cubestore:** Handle corrupted upstream metastore ([d547677](https://github.com/cube-js/cube.js/commit/d547677c277c2233351266add966a2b019c38e3c))
- **cubestore:** Index repairs ([d5dc4cf](https://github.com/cube-js/cube.js/commit/d5dc4cf4f2313d1462a5d67cb5b7b7009680003a))
- **cubestore:** Set default worker pool timeout to 2 minutes ([139c8f6](https://github.com/cube-js/cube.js/commit/139c8f6844c9b923855a1d0a235aefd58288fd14))
- Declare Add missing externalQueueOptions for QueryCacheOptions ([563fcdc](https://github.com/cube-js/cube.js/commit/563fcdcb943622ad8ca391182652f2eb27000079))

### Features

- **cubestore:** Rebase arrow to 2020-01-02 version ([3cbb46d](https://github.com/cube-js/cube.js/commit/3cbb46d883445e2fbfb261d182e5cdaa6871bf2c))
- **cubestore:** Three tables join support ([b776398](https://github.com/cube-js/cube.js/commit/b776398ba45bc314f12a15a2f0861d5b01dcb90a))

## [0.25.10](https://github.com/cube-js/cube.js/compare/v0.25.9...v0.25.10) (2020-12-31)

### Bug Fixes

- **@cubejs-backend/cubestore-driver:** 2k batch size upload ([d1be31e](https://github.com/cube-js/cube.js/commit/d1be31e8adabd022a2be518405cbf403870b7f18))

## [0.25.9](https://github.com/cube-js/cube.js/compare/v0.25.8...v0.25.9) (2020-12-31)

### Bug Fixes

- **@cubejs-backend/cubestore-driver:** 10k batch size upload ([d863a10](https://github.com/cube-js/cube.js/commit/d863a10b1b025577ff302b73de15ff9d9f2fb9a6))

## [0.25.8](https://github.com/cube-js/cube.js/compare/v0.25.7...v0.25.8) (2020-12-31)

### Features

- **@cubejs-backend/mysql-driver:** More int and text types support for read only pre-aggregations ([5bb2a4f](https://github.com/cube-js/cube.js/commit/5bb2a4f40efa9b602a48f594052be0eb9484d31a))

## [0.25.7](https://github.com/cube-js/cube.js/compare/v0.25.6...v0.25.7) (2020-12-30)

### Bug Fixes

- **@cubejs-backend/mysql-driver:** Handle mediumint(9) type ([3d135b1](https://github.com/cube-js/cube.js/commit/3d135b16eee8fa4c35c584c28b8f18e47539fa54))

## [0.25.6](https://github.com/cube-js/cube.js/compare/v0.25.5...v0.25.6) (2020-12-30)

### Bug Fixes

- Allow CUBEJS_SCHEDULED_REFRESH_TIMER to be boolean ([4e80645](https://github.com/cube-js/cube.js/commit/4e80645259cbd3a5ad7d92f3d07a1d5a58a6c5ef))

## [0.25.5](https://github.com/cube-js/cube.js/compare/v0.25.4...v0.25.5) (2020-12-30)

### Features

- Allow to specify socket for PORT/TLS_PORT, fix [#1681](https://github.com/cube-js/cube.js/issues/1681) ([b9c4669](https://github.com/cube-js/cube.js/commit/b9c466987ffa41f31fa8b3bda88432175e57cd86))

## [0.25.4](https://github.com/cube-js/cube.js/compare/v0.25.3...v0.25.4) (2020-12-30)

### Bug Fixes

- **cubestore:** `next_table_seq` sanity check until transactions arrive ([f9b65ea](https://github.com/cube-js/cube.js/commit/f9b65eac837d102afb2b280a124dbe341a4cc058))
- **cubestore:** Atomic WAL activation ([0c64e69](https://github.com/cube-js/cube.js/commit/0c64e698253921973a7452cf2b0184c1a27553ef))
- **cubestore:** Migrate to memory sequence tracking until transactions arrive ([7308a63](https://github.com/cube-js/cube.js/commit/7308a632ddba43c9333b098eca34f71686922e4d))
- **cubestore:** Move to RocksDB Snapshot reading to ensure strong metastore read consistency ([68dac72](https://github.com/cube-js/cube.js/commit/68dac72cf6adff920c05c118cf297986e943a7f3))

### Features

- **@cubejs-backend/cubestore-driver:** Increase upload batch size to 50k ([1bebc1d](https://github.com/cube-js/cube.js/commit/1bebc1dd09845e547abea65dd24ace56a5cea40b))
- **server-core:** Compatibility shim, for legacy imports ([2116799](https://github.com/cube-js/cube.js/commit/21167995045d7a5c0d1056dc034b14ec18205277))
- **server-core:** Initial support for TS ([df45216](https://github.com/cube-js/cube.js/commit/df452164d8282074f926a980cbfe3284817e85a6))
- **server-core:** Introduce CUBEJS_PRE_AGGREGATIONS_SCHEMA, use dev_preaggregations/prod_preaggregations by default ([e5bdf3d](https://github.com/cube-js/cube.js/commit/e5bdf3dfbd28d5e1c1e775c554c275304a0941f3))
- **server-core:** Move to TS ([d7b7431](https://github.com/cube-js/cube.js/commit/d7b743156751dbc2202a7138bc7603dc6861f001))

## [0.25.3](https://github.com/cube-js/cube.js/compare/v0.25.2...v0.25.3) (2020-12-28)

### Bug Fixes

- `CUBEJS_SCHEDULED_REFRESH_CONCURRENCY` doesn't work ([1f6b505](https://github.com/cube-js/cube.js/commit/1f6b5054b1327547d86004fd95941b0f3099ca68))

## [0.25.2](https://github.com/cube-js/cube.js/compare/v0.25.1...v0.25.2) (2020-12-27)

### Bug Fixes

- **@cubejs-backend/query-orchestrator:** Throw an exception on empty pre-agg in readOnly mode, refs [#1597](https://github.com/cube-js/cube.js/issues/1597) ([17d5fdb](https://github.com/cube-js/cube.js/commit/17d5fdb82e0ce06d55e438913e32952f32db7923))
- **@cubejs-backend/schema-compiler:** MySQL double timezone conversion ([e5f1490](https://github.com/cube-js/cube.js/commit/e5f1490a897df4f0eac062dfabbc20aca2ea2f5b))
- **@cubejs-client/react:** prevent state updates on unmounted components ([#1684](https://github.com/cube-js/cube.js/issues/1684)) ([4f3796c](https://github.com/cube-js/cube.js/commit/4f3796c9f402a7b8b54311a08c632270be8e34c3))
- **api-gateway:** /readyz /healthz - correct response for partial outage ([1e5bdf5](https://github.com/cube-js/cube.js/commit/1e5bdf556f6f14698945a72c0332e0f6982ba8e7))

### Features

- Ability to set timeouts for polling in BigQuery/Athena ([#1675](https://github.com/cube-js/cube.js/issues/1675)) ([dc944b1](https://github.com/cube-js/cube.js/commit/dc944b1aaacc69dd74a9d9d31ceaf43e16d37ccd)), closes [#1672](https://github.com/cube-js/cube.js/issues/1672)
- Concurrency controls for scheduled refresh ([2132f0d](https://github.com/cube-js/cube.js/commit/2132f0dc7bb3aab994d559ea42dd0b0a934b1310))
- **api-gateway:** Support schema inside Authorization header, fix [#1297](https://github.com/cube-js/cube.js/issues/1297) ([2549004](https://github.com/cube-js/cube.js/commit/25490048661738e273629c73368ca03f821ee096))
- **cubestore:** Default decimal scale ([a79f98b](https://github.com/cube-js/cube.js/commit/a79f98b08c9be0688c0cea82b881230518575270))

## [0.25.1](https://github.com/cube-js/cube.js/compare/v0.25.0...v0.25.1) (2020-12-24)

### Bug Fixes

- **@cubejs-backend/schema-compiler:** Better error message for join member resolutions ([30cc3ab](https://github.com/cube-js/cube.js/commit/30cc3abc4e8c91e8d95b8794f892e1d1f2152798))
- **@cubejs-backend/schema-compiler:** Error: TypeError: R.eq is not a function -- existing joins in rollup support ([5f62aae](https://github.com/cube-js/cube.js/commit/5f62aaee88b7ecc281437601410b10ef04d7bbf3))
- **@cubejs-client/playground:** propagate cubejs token ([#1669](https://github.com/cube-js/cube.js/issues/1669)) ([f1fb563](https://github.com/cube-js/cube.js/commit/f1fb5634fa62b2f78cf6d8365c4a98094e114f6c))
- **cubestore:** Merge join empty side fixes ([5e65c3e](https://github.com/cube-js/cube.js/commit/5e65c3e251c7f9d7329a601bd467a4ef3b043463))
- **cubestore:** Non atomic primary key allocation conflicts ([073ac8c](https://github.com/cube-js/cube.js/commit/073ac8ce69cc294a15a5e59b11b2915a755ca81b))
- **cubestore:** Pass join on sort conditions explicitly. Avoid incorrectly selected sort keys. ([b6a2e4a](https://github.com/cube-js/cube.js/commit/b6a2e4a457bd38d40d8819443a2fc4fddf7465db))
- **playground:** Use basePath from configuration, fix [#377](https://github.com/cube-js/cube.js/issues/377) ([c94cbce](https://github.com/cube-js/cube.js/commit/c94cbce50e31617086ec458f934fefaf779b76f4))

### Features

- **@cubejs-backend/dremio-driver:** Add HTTPS support for Dremio ([#1666](https://github.com/cube-js/cube.js/issues/1666)), Thanks [@chipblox](https://github.com/chipblox) ([1143e9c](https://github.com/cube-js/cube.js/commit/1143e9cbdb78059a93e1419feff80c34ee29bdbf))
- **athena-driver:** Support readOnly option, add typings ([a519cb8](https://github.com/cube-js/cube.js/commit/a519cb880be2bb2b872c56b092f1273291fbd397))
- **elasticsearch-driver:** Support CUBEJS_DB_ELASTIC_QUERY_FORMAT, Thanks [@dylman79](https://github.com/dylman79) ([a7460f5](https://github.com/cube-js/cube.js/commit/a7460f5d45dc7e9d96b65f6cc36df810a5b9312e))

# [0.25.0](https://github.com/cube-js/cube.js/compare/v0.24.15...v0.25.0) (2020-12-21)

### Bug Fixes

- **@cubejs-client/playground:** chart renderer load ([#1658](https://github.com/cube-js/cube.js/issues/1658)) ([bbce716](https://github.com/cube-js/cube.js/commit/bbce71697a0d4c33a2d0bb277fd039cc5925f4ca))
- getQueryStage throws undefined is not a function ([0de1603](https://github.com/cube-js/cube.js/commit/0de1603293fc918c0da8ff8bd514b49f14de51d8))

### Features

- Allow cross data source joins ([a58336e](https://github.com/cube-js/cube.js/commit/a58336e3840f8ac02d83de43ec7661419bceb71c))
- Allow cross data source joins: Serverless support ([034cdc8](https://github.com/cube-js/cube.js/commit/034cdc8dbf8907988df0f999fd115b8acdb4990f))

## [0.24.15](https://github.com/cube-js/cube.js/compare/v0.24.14...v0.24.15) (2020-12-20)

### Bug Fixes

- **cubestore:** Atomic chunks repartition ([b1a23da](https://github.com/cube-js/cube.js/commit/b1a23dac8b82e2ab997ec060109948c355e37764))
- **cubestore:** Atomic index snapshotting ([8a50f34](https://github.com/cube-js/cube.js/commit/8a50f34c22db7cc9ddd13c4aa33c864a90e29b4f))

### Features

- Allow joins between data sources for external queries ([1dbfe2c](https://github.com/cube-js/cube.js/commit/1dbfe2cdc1b1904ce8567a7599b24e660c5047f3))
- **cubestore:** Support GROUP BY DECIMAL ([#1652](https://github.com/cube-js/cube.js/issues/1652)) ([4ad97dc](https://github.com/cube-js/cube.js/commit/4ad97dc8ae618fccb98020b50e335c5e8cf47459))

## [0.24.14](https://github.com/cube-js/cube.js/compare/v0.24.13...v0.24.14) (2020-12-19)

### Bug Fixes

- Rollup match results for rollupJoin ([0279b13](https://github.com/cube-js/cube.js/commit/0279b13a8696643ad95c374062ea059cea3b890b))
- **api-gateway:** Fix broken POST /v1/dry-run ([fa0cae0](https://github.com/cube-js/cube.js/commit/fa0cae01fa471e01d88d7db6f1d17046392167d0))

### Features

- Add HTTP Post to cubejs client core ([#1608](https://github.com/cube-js/cube.js/issues/1608)). Thanks to [@mnifakram](https://github.com/mnifakram)! ([1ebd6a0](https://github.com/cube-js/cube.js/commit/1ebd6a04ac97b31c6a51ef63bb1d4c040e524190))

## [0.24.13](https://github.com/cube-js/cube.js/compare/v0.24.12...v0.24.13) (2020-12-18)

### Bug Fixes

- **@cubejs-client/react:** reset the error on subsequent calls ([#1641](https://github.com/cube-js/cube.js/issues/1641)) ([2a65dae](https://github.com/cube-js/cube.js/commit/2a65dae8d1f327f47d387ff8bbf52193ebb7bf53))

### Features

- **api-gateway:** Dont run all health checks, when the one is down ([f5957f4](https://github.com/cube-js/cube.js/commit/f5957f4824372d5e22de25a23a3a1e78445df5d0))
- Rollup join implementation ([#1637](https://github.com/cube-js/cube.js/issues/1637)) ([bffd220](https://github.com/cube-js/cube.js/commit/bffd22095f58369f3d52474283951b4844657f2b))

## [0.24.11](https://github.com/cube-js/cube.js/compare/v0.24.10...v0.24.11) (2020-12-17)

**Note:** Version bump only for package cubejs

## [0.24.9](https://github.com/cube-js/cube.js/compare/v0.24.8...v0.24.9) (2020-12-16)

### Bug Fixes

- **@cubejs-backend/mysql-driver:** Revert back test on borrow with database pool error logging. ([2cdaf40](https://github.com/cube-js/cube.js/commit/2cdaf406a7d99116849f60e00e1b1bc25605e0d3))
- **docker:** Drop usage of VOLUME to protected unexpected behavior ([e3f20cd](https://github.com/cube-js/cube.js/commit/e3f20cdad7b72cb45b7a4eee5452dde918539df7))
- Warning about absolute import ([5f228bc](https://github.com/cube-js/cube.js/commit/5f228bc5e654ab9a4efba458b5c31614ac44a5aa))

### Features

- **@cubejs-client/playground:** Angular chart code generation support in Playground ([#1519](https://github.com/cube-js/cube.js/issues/1519)) ([4690e11](https://github.com/cube-js/cube.js/commit/4690e11f417ff65fea8426360f3f5a2b3acd2792)), closes [#1515](https://github.com/cube-js/cube.js/issues/1515) [#1612](https://github.com/cube-js/cube.js/issues/1612)
- **@cubejs-client/react:** dry run hook ([#1612](https://github.com/cube-js/cube.js/issues/1612)) ([9aea035](https://github.com/cube-js/cube.js/commit/9aea03556ae61f443598ed587538e60239a3be2d))

## [0.24.8](https://github.com/cube-js/cube.js/compare/v0.24.7...v0.24.8) (2020-12-15)

### Bug Fixes

- **@cubejs-backend/mysql-driver:** Use decimal(38,10) for external pre-aggregations, fix [#1563](https://github.com/cube-js/cube.js/issues/1563) ([3aec549](https://github.com/cube-js/cube.js/commit/3aec549f0344590185618427b854eef863d24287))
- **@cubejs-backend/schema-compiler:** CubeCheckDuplicatePropTranspiler - dont crash on not StringLiterals ([#1582](https://github.com/cube-js/cube.js/issues/1582)) ([a705a2e](https://github.com/cube-js/cube.js/commit/a705a2ed6885d5c08e654945682054a1421dfb51))
- **@cubejs-client/playground:** fix color name and change font to Inter ([010a106](https://github.com/cube-js/cube.js/commit/010a106442dfc39a2027733d5087ac6b7e2cdcb3))

### Features

- **@cubejs-backend/query-orchestrator:** Introduce AsyncRedisClient type ([728110e](https://github.com/cube-js/cube.js/commit/728110ed0ffe5697bd5e47e3920bf2e5377a0ffd))
- **@cubejs-backend/query-orchestrator:** Migrate createRedisClient to TS ([78e8422](https://github.com/cube-js/cube.js/commit/78e8422937e79457fdcec70535225bc9ccecfce8))
- **@cubejs-backend/query-orchestrator:** Move RedisPool to TS, export RedisPoolOptions ([8e8abde](https://github.com/cube-js/cube.js/commit/8e8abde85b9fa821d21f33fc286cfb2cc56891e4))
- **@cubejs-backend/query-orchestrator:** Set redis pool options from server config ([c1270d4](https://github.com/cube-js/cube.js/commit/c1270d4cfdc243b230ade0cb3a4c59171db70d20))
- **@cubejs-client/core:** Added pivotConfig option to alias series with a prefix ([#1594](https://github.com/cube-js/cube.js/issues/1594)). Thanks to @MattGson! ([a3342f7](https://github.com/cube-js/cube.js/commit/a3342f7fd0389ce3ad0bc62686c0e787de25f411))
- Set CUBEJS_SCHEDULED_REFRESH_TIMER default value to 30 seconds ([f69324c](https://github.com/cube-js/cube.js/commit/f69324c60ee4adfdfded67dddedab113fb5fdb95))

## [0.24.7](https://github.com/cube-js/cube.js/compare/v0.24.6...v0.24.7) (2020-12-14)

### Bug Fixes

- **@cubejs-backend/mysql-driver:** Do not validate connections in pool and expose all errors to clients ([b62f27f](https://github.com/cube-js/cube.js/commit/b62f27fb8319c5ea161d601586bd5cf0e3e940dd))

## [0.24.6](https://github.com/cube-js/cube.js/compare/v0.24.5...v0.24.6) (2020-12-13)

### Bug Fixes

- **@cubejs-backend/api-gateway:** SubscriptionServer - support dry-run ([#1581](https://github.com/cube-js/cube.js/issues/1581)) ([43fbc20](https://github.com/cube-js/cube.js/commit/43fbc20a66b4aad335ba198960cc1f626fb909a4))
- **cubejs-cli:** deploy --upload-env - filter CUBEJS_DEV_MODE ([81a835f](https://github.com/cube-js/cube.js/commit/81a835f033e44b945d0c3a6115491e337a7eddfd))

### Features

- **cubestore:** Explicit index selection for join ([290cab8](https://github.com/cube-js/cube.js/commit/290cab82586084ed464b50ecdc9ad8bfe1461c9e))
- Move index creation orchestration to the driver: allow to control drivers when to create indexes ([2a94e71](https://github.com/cube-js/cube.js/commit/2a94e710a89954ecedf4aa6f76b89578138e7aff))
- **cubestore:** String implicit casts. CREATE INDEX support. ([d42c199](https://github.com/cube-js/cube.js/commit/d42c1995f675c437812196c30d2ba08cd35f273a))

## [0.24.5](https://github.com/cube-js/cube.js/compare/v0.24.4...v0.24.5) (2020-12-09)

### Bug Fixes

- **@cubejs-backend/api-gateway:** Export UserError/CubejsHandlerError ([#1540](https://github.com/cube-js/cube.js/issues/1540)) ([20124ba](https://github.com/cube-js/cube.js/commit/20124ba26f8330801fd23e33c7c36a2005ae98e8))
- **@cubejs-client/playground:** fix user select on tab content ([7a0e4ef](https://github.com/cube-js/cube.js/commit/7a0e4ef10fb42597a402c69004c0d94178ce62ed))
- **cubestore:** Compaction fixes ([7441a26](https://github.com/cube-js/cube.js/commit/7441a267d382c126b9e567f59c2b06aed2ca34a5))
- **cubestore:** Partition range gap fix ([3610b61](https://github.com/cube-js/cube.js/commit/3610b612009016431859bdf18a3760ba029e8613))

### Features

- **@cubejs-backend/bigquery-driver:** Allow to make BigQueryDriver as readOnly, fix [#1028](https://github.com/cube-js/cube.js/issues/1028) ([d9395f6](https://github.com/cube-js/cube.js/commit/d9395f6df4e896c1b987ff5dfbf741829e3b51df))
- **@cubejs-backend/mysql-driver:** CAST all time dimensions with granularities to DATETIME in order to provide typing for rollup downloads. Add mediumtext and mediumint generic type conversions. ([3d8cb37](https://github.com/cube-js/cube.js/commit/3d8cb37d03716cd2768a0986643495e4a844cb8d))
- **cubejs-cli:** improve DX for docker ([#1457](https://github.com/cube-js/cube.js/issues/1457)) ([72ad782](https://github.com/cube-js/cube.js/commit/72ad782090c52e677b9e51e43818f1dca40db791))
- **cubestore:** CUBESTORE_PORT env variable ([11e36a7](https://github.com/cube-js/cube.js/commit/11e36a726b930a1952eb917868c93078e1a9308e))
- **cubestore:** IN Implementation ([945d8bc](https://github.com/cube-js/cube.js/commit/945d8bc3728b3ab462e7448b13d92d65d1581ac8))

## [0.24.4](https://github.com/cube-js/cube.js/compare/v0.24.3...v0.24.4) (2020-12-07)

### Bug Fixes

- **@cubejs-backend/server:** Versions inside error message ([1a8cc4f](https://github.com/cube-js/cube.js/commit/1a8cc4f9ec15c18744c1541499552fa2133484ac))
- **@cubejs-backend/server-core:** Allow to pass externalRefresh inside preAggregationsOptions, fix [#1524](https://github.com/cube-js/cube.js/issues/1524) ([a6959c9](https://github.com/cube-js/cube.js/commit/a6959c9f47d3751bdc6b5d132e858d55107d9a50))
- **@cubejs-client/playground:** always show scrollbars in menu if content is too big ([5e22a3a](https://github.com/cube-js/cube.js/commit/5e22a3a179fa38bdbd539ec00b09f2ca0e89b8b9))
- **cubestore:** Merge sort propagation fixes ([35125ad](https://github.com/cube-js/cube.js/commit/35125ad58296cef5a038dfce27a95941487c9ab0))
- **docker:** Add sqlite driver to built-in drivers ([3b7b0f7](https://github.com/cube-js/cube.js/commit/3b7b0f74a3474a561481fac80cb5bc4b9c8450c9))
- **docker:** Use latest snowflake driver ([f607ed0](https://github.com/cube-js/cube.js/commit/f607ed01366981f3f1b53ab0782cca867ed5d50c))

### Features

- **@cubejs-backend/api-gateway:** Migrate some parts to TS ([c1166d7](https://github.com/cube-js/cube.js/commit/c1166d744ccd562db492e5dedd01eab63e07bfd4))
- **@cubejs-backend/api-gateway:** Migrate to TS initial ([1edef6d](https://github.com/cube-js/cube.js/commit/1edef6d269fd1877f0bfcdcf17d2f780abd4404c))
- **@cubejs-backend/postgres-driver:** Support CUBEJS_DB_SSL_SERVERNAME ([f044372](https://github.com/cube-js/cube.js/commit/f04437236ca78cb23ef69f2a5de6be60006f2464))
- Ability to load SSL keys from FS ([#1512](https://github.com/cube-js/cube.js/issues/1512)) ([71da5bb](https://github.com/cube-js/cube.js/commit/71da5bb529294fabd92b3a914b1e8bceb464643c))
- **cubestore:** Decimal support ([6bdc68b](https://github.com/cube-js/cube.js/commit/6bdc68b4de96a050306044cb61e337961c76d898))
- **cubestore:** Left join support ([9d1fd09](https://github.com/cube-js/cube.js/commit/9d1fd0996dcb4838ff848d1905955d82132f1338))
- **cubestore:** Mediumint support ([f98540b](https://github.com/cube-js/cube.js/commit/f98540bb0db705ea53e5fb73dd242338c9145adc))

## [0.24.3](https://github.com/cube-js/cube.js/compare/v0.24.2...v0.24.3) (2020-12-01)

### Bug Fixes

- **cubestore:** Merge join support: not implemented: Merge join is not supported for data type Timestamp(Microsecond, None) ([6e3ebfc](https://github.com/cube-js/cube.js/commit/6e3ebfc10c87b7ff23901949f1caa0a6021202e2))
- **cubestore:** Unsupported data type Boolean. ([b286182](https://github.com/cube-js/cube.js/commit/b28618204b4e07507e5df0e822607900a3439ca4))

### Features

- **cubestore:** Hash join support ([8b1a5da](https://github.com/cube-js/cube.js/commit/8b1a5da50992fa784aa2da8bd0dd092162b5b853))
- **cubestore:** Merge join support ([d08d8e3](https://github.com/cube-js/cube.js/commit/d08d8e357ca7baeb113fb0a003f76e519162c3ee))
- **cubestore:** Update datafusion upstream to the version of 2020-11-27 ([b4685dd](https://github.com/cube-js/cube.js/commit/b4685dd5556f5a1448ef0bfbcae841fd7905f372))

## [0.24.2](https://github.com/cube-js/cube.js/compare/v0.24.1...v0.24.2) (2020-11-27)

### Bug Fixes

- add content-type to allowedHeaders ([d176269](https://github.com/cube-js/cube.js/commit/d176269fda12d7213c021026c02f7aec0df50ba6))
- **@cubejs-backend/server-core:** Allow to pass unknown options (such as http) ([f1e9402](https://github.com/cube-js/cube.js/commit/f1e9402ee5c1fa6695d44f8750602d0a2ccedd5f))

### Features

- **@cubejs-backend/query-orchestrator:** Initial move to TypeScript ([#1462](https://github.com/cube-js/cube.js/issues/1462)) ([101e8dc](https://github.com/cube-js/cube.js/commit/101e8dc90d4b1266c0327adb86cab3e3caa8d4d0))

## [0.24.1](https://github.com/cube-js/cube.js/compare/v0.24.0...v0.24.1) (2020-11-27)

### Bug Fixes

- Specifying `dateRange` in time dimension should produce same result as `inDateRange` in filter ([a7603d7](https://github.com/cube-js/cube.js/commit/a7603d724732a51301227f68c39ba699333c0e06)), closes [#962](https://github.com/cube-js/cube.js/issues/962)
- **cubejs-cli:** template/serverless - specify CORS ([#1449](https://github.com/cube-js/cube.js/issues/1449)) ([f8064d2](https://github.com/cube-js/cube.js/commit/f8064d292570804fb8d2ef04708d2f5c4e563be2))
- **cubestore:** Negative int insert support ([5f2ff55](https://github.com/cube-js/cube.js/commit/5f2ff552bc5042f4d0d87fc3678de8e21ff5424a))

### Features

- **cubestore:** Group by boolean ([fa1b1b2](https://github.com/cube-js/cube.js/commit/fa1b1b2a439d9dd98e3cbaf730a313033f39ad80))
- **cubestore:** Group by boolean ([45fe036](https://github.com/cube-js/cube.js/commit/45fe03677beb09ef7d83065566d1e0536543fea2))
- Specify CORS for server/serverless ([#1455](https://github.com/cube-js/cube.js/issues/1455)) ([8c371ad](https://github.com/cube-js/cube.js/commit/8c371add2821a851bc51e00fb24e7ad2d8620345))

# [0.24.0](https://github.com/cube-js/cube.js/compare/v0.23.15...v0.24.0) (2020-11-26)

### Bug Fixes

- Error: Type must be provided for null values. -- `null` parameter values are passed to BigQuery when used for dimensions that contain `?` ([6417e7d](https://github.com/cube-js/cube.js/commit/6417e7d120a95c4792557a4c4a0d6abb7c483db9))
- **cubejs-cli:** template/serverless - iamRoleStatements.Resource[0] unsupported configuration format ([9fbe683](https://github.com/cube-js/cube.js/commit/9fbe683d3d1464ab453d354331033775fe707dec))

### Features

- Make default refreshKey to be `every 10 seconds` and enable scheduled refresh in dev mode by default ([221003a](https://github.com/cube-js/cube.js/commit/221003aa73aa1ece3d649de9164a7379a4a690be))

### BREAKING CHANGES

- `every 10 seconds` refreshKey becomes a default refreshKey for all cubes.

## [0.23.15](https://github.com/cube-js/cube.js/compare/v0.23.14...v0.23.15) (2020-11-25)

### Bug Fixes

- Error: Cannot find module 'antlr4/index' ([0d2e330](https://github.com/cube-js/cube.js/commit/0d2e33040dfea3fb80df2a1af2ccff46db0f8673))
- **@cubejs-backend/server-core:** Correct type for orchestratorOptions ([#1422](https://github.com/cube-js/cube.js/issues/1422)) ([96c1691](https://github.com/cube-js/cube.js/commit/96c169150ccf2197812dafdebce8194dd2cf6294))

### Features

- **@cubejs-backend/postgres-driver:** Support CUBEJS_DB_SSL_KEY ([e6291fc](https://github.com/cube-js/cube.js/commit/e6291fcda283aa6ee22badec339a600db02a1ce9))
- **@cubejs-client/react:** support 'compareDateRange' when updating 'timeDimensions' ([#1426](https://github.com/cube-js/cube.js/issues/1426)). Thanks to @BeAnMo! ([6446a58](https://github.com/cube-js/cube.js/commit/6446a58c5d6c983f045dc2062732aacfd69d908a))

## [0.23.14](https://github.com/cube-js/cube.js/compare/v0.23.13...v0.23.14) (2020-11-22)

### Bug Fixes

- **@cubejs-backend/query-orchestrator:** Intermittent lags when pre-aggregation tables are refreshed ([4efe1fc](https://github.com/cube-js/cube.js/commit/4efe1fc006282d87ab2718918d1bdd174baa6be3))
- **@cubejs-backend/snowflake-driver:** Add keepConnectionAlive and release ([#1379](https://github.com/cube-js/cube.js/issues/1379)) ([f1acae5](https://github.com/cube-js/cube.js/commit/f1acae5e00e37ba1ab2c9fab0f5f94f8e7d20283))
- **@cubejs-client/core:** propagate segments to drillDown queries ([#1406](https://github.com/cube-js/cube.js/issues/1406)) ([d4ceb65](https://github.com/cube-js/cube.js/commit/d4ceb6502db9c62c0cf95f1e48879f95ea4544d7))
- **cubestore:** Error reporting in docker ([cba3c50](https://github.com/cube-js/cube.js/commit/cba3c50a9856e1fe6893e5e2a2c14f89ebc2ce41))
- **cubestore:** Tables are imported without location ([5e8cffb](https://github.com/cube-js/cube.js/commit/5e8cffb5cc5b0123157086b206cb565b0dca5bac))
- **examples:** Add deprecation warnings to Slack Vibe ([98783c6](https://github.com/cube-js/cube.js/commit/98783c6d0658e136912bbaf9d3c6da5385085738))
- **examples:** e-commerce backend 💸 ([dab7301](https://github.com/cube-js/cube.js/commit/dab7301b01eefd7d1c5c8cbf1f233ae9cc5cc4c8))
- **examples:** External Rollups 🗞 ([86172b7](https://github.com/cube-js/cube.js/commit/86172b752c18f0a785558aa5f4710d9155593208))
- **examples:** Migration to Netlify ([ad582a1](https://github.com/cube-js/cube.js/commit/ad582a144c3cc7d64ae55ff45bc684c8d967e98e))
- **examples:** React Dashboard ⚛️ ([eccae84](https://github.com/cube-js/cube.js/commit/eccae84bb8b76a3ee138445a2c648eeda11b3774))

### Features

- **cubestore:** Collect backtraces in docker ([d97bcb9](https://github.com/cube-js/cube.js/commit/d97bcb9f9b4d035d15192ba5bc559478cd850ff0))
- **cubestore:** Error reporting ([99ede83](https://github.com/cube-js/cube.js/commit/99ede8388699298c4bbe89462a1c1737a324ce53))
- **cubestore:** Table location support ([6b63ef8](https://github.com/cube-js/cube.js/commit/6b63ef8ac109cca40cca5d2787bc342938c56d7a))
- **docker:** Introduce alpine images ([#1413](https://github.com/cube-js/cube.js/issues/1413)) ([972c700](https://github.com/cube-js/cube.js/commit/972c7008c3dcf1febfdcb66af0dd674bedb04752))
- **docs-build:** add `gatsby-redirect-from` to allow redirects with SEO ([f3e680a](https://github.com/cube-js/cube.js/commit/f3e680a9542370a1efa126a85b86e8c425fcc8a3)), closes [#1395](https://github.com/cube-js/cube.js/issues/1395)
- Allow to run docker image without config file ([#1409](https://github.com/cube-js/cube.js/issues/1409)) ([bc53cd1](https://github.com/cube-js/cube.js/commit/bc53cd17296ea4fa53940b74eaa9e3c7823d1603))

## [0.23.13](https://github.com/cube-js/cube.js/compare/v0.23.12...v0.23.13) (2020-11-17)

### Bug Fixes

- **docker:** Use CMD instead of entrypoint for cubejs server ([d6066a8](https://github.com/cube-js/cube.js/commit/d6066a8049881ca5a53b5aa35b32c10f3adbd277))
- **docs:** fix broken link in 'Deployment Guide' page ([#1399](https://github.com/cube-js/cube.js/issues/1399)) ([4c01e2d](https://github.com/cube-js/cube.js/commit/4c01e2d9c548f0b2db9a19dc295dab5fe5179b68))

## [0.23.12](https://github.com/cube-js/cube.js/compare/v0.23.11...v0.23.12) (2020-11-17)

### Bug Fixes

- **@cubejs-client/core:** pivot should work well with null values ([#1386](https://github.com/cube-js/cube.js/issues/1386)). Thanks to [@mspiegel31](https://github.com/mspiegel31)! ([d4c2446](https://github.com/cube-js/cube.js/commit/d4c24469b8eea2d84f04c540b0a5f9a8d285ad1d))
- **cubestore:** CREATE SCHEMA IF NOT EXISTS support ([7c590b3](https://github.com/cube-js/cube.js/commit/7c590b30ca2c4bb3ef9ac6d9cbfc181f322de14c))

### Features

- Introduce CUBEJS_DEV_MODE & improve ENV variables experience ([#1356](https://github.com/cube-js/cube.js/issues/1356)) ([cc2aa92](https://github.com/cube-js/cube.js/commit/cc2aa92bbec87b21b147d5003fa546d4b1807185))
- **@cubejs-server:** Require the latest oclif packages to support Node.js 8 ([7019966](https://github.com/cube-js/cube.js/commit/70199662cc3370c0c8763bb69dcec045e4e52590))
- **cubestore:** Distributed query execution ([102c641](https://github.com/cube-js/cube.js/commit/102c64120e2488a6ba2eff960d674cd5aedb9e8f))

## [0.23.11](https://github.com/cube-js/cube.js/compare/v0.23.10...v0.23.11) (2020-11-13)

### Bug Fixes

- **@cubejs-backend/server-core:** Node.js 8 support (downgrade fs-extra to 8.x) ([#1367](https://github.com/cube-js/cube.js/issues/1367)) ([be10ac6](https://github.com/cube-js/cube.js/commit/be10ac6912ebbaa57d386625dd4b2e3c40808c48))
- **@cubejs-client/core:** annotation format type ([e5004f6](https://github.com/cube-js/cube.js/commit/e5004f6bf687e7df4b611bf1d772da278558759d))
- **@cubejs-client/ws-transport:** make auth optional ([#1368](https://github.com/cube-js/cube.js/issues/1368)) ([28a07bd](https://github.com/cube-js/cube.js/commit/28a07bdc0e7e506bbc60daa2ad621415c93b54e2))
- **@cubejs-playground:** boolean filters support ([#1269](https://github.com/cube-js/cube.js/issues/1269)) ([adda809](https://github.com/cube-js/cube.js/commit/adda809e4cd08436ffdf8f3396a6f35725f3dc22))
- **@cubejs-playground:** ng scaffolding support ([0444744](https://github.com/cube-js/cube.js/commit/0444744dda44250c35eb22c1a7e2da1f2183cbc6))
- **@cubejs-playground:** ng support notification, loader ([2f73f16](https://github.com/cube-js/cube.js/commit/2f73f16f49c1c7325ea2104f44c8e8e437bc1ab6))
- **ci:** Trigger on pull_request, not issue ([193dc81](https://github.com/cube-js/cube.js/commit/193dc81fbbc506141656c6b0cc879b7b241ad33b))
- CUBEJS_DB_SSL must be true to affect SSL ([#1252](https://github.com/cube-js/cube.js/issues/1252)) ([f2e9d9d](https://github.com/cube-js/cube.js/commit/f2e9d9db3f7b8fc5a7c5bbaaebca56f5331d4332)), closes [#1212](https://github.com/cube-js/cube.js/issues/1212)
- **cubejs-cli:** Generate/token should work inside docker ([67d7501](https://github.com/cube-js/cube.js/commit/67d7501a8419e9f5be6d39ae9116592134d99c91))
- **cubestore:** Endless upload loop ([0494122](https://github.com/cube-js/cube.js/commit/049412257688c3971449cff22a789697c1b5eb04))
- **cubestore:** Worker Pool graceful shutdown ([56377dc](https://github.com/cube-js/cube.js/commit/56377dca194e76d9be4ec1f8ad18055af041914b))
- **examples/real-time-dashboard:** Configure collect entrypoint by REACT_APP_COLLECT_URL ENV ([bde3ad8](https://github.com/cube-js/cube.js/commit/bde3ad8ef9f5d6dbe5b07a0e496709adedc8abf7))

### Features

- **@cubejs-backend/mysql-aurora-serverless-driver:** Add a new driver to support AWS Aurora Serverless MySql ([#1333](https://github.com/cube-js/cube.js/issues/1333)) Thanks to [@kcwinner](https://github.com/kcwinner)! ([154fab1](https://github.com/cube-js/cube.js/commit/154fab1a222685e1e83d5187a4f00f745c4613a3))
- **@cubejs-client/react:** Add minute and second granularities to React QueryBuilder ([#1332](https://github.com/cube-js/cube.js/issues/1332)). Thanks to [@danielnass](https://github.com/danielnass)! ([aa201ae](https://github.com/cube-js/cube.js/commit/aa201aecdc66d920e7a6f84a1043cf5964bc6cb9))
- **cubejs-cli:** .env file - add link to the docs ([b63405c](https://github.com/cube-js/cube.js/commit/b63405cf78acabf80faec5d910be7a53af8702b9))
- **cubejs-cli:** create - persist template name & version ([8555290](https://github.com/cube-js/cube.js/commit/8555290dda3f36bc3b185fecef2ad17fba5aae80))
- **cubejs-cli:** Share /dashboard-app directory by default ([#1380](https://github.com/cube-js/cube.js/issues/1380)) ([d571dcc](https://github.com/cube-js/cube.js/commit/d571dcc9ad5c14916cd33740c0a3dba85e8c8be2))
- **cubejs-cli:** Use index.js file instead of cube.js ([#1350](https://github.com/cube-js/cube.js/issues/1350)) ([9b6c593](https://github.com/cube-js/cube.js/commit/9b6c59359e10cba7ec37e8a5be2ac7cc7dabd9da))
- **cubestore:** Add avx2 target-feature for docker build ([68e5a8a](https://github.com/cube-js/cube.js/commit/68e5a8a4d14d8028b5060bb6825d391b3c7ce8e5))
- **cubestore:** CUBESTORE_SELECT_WORKERS env variable ([9e59b2d](https://github.com/cube-js/cube.js/commit/9e59b2dbd8bdb43327560320d863569e77ba507c))
- **cubestore:** Select worker process pool ([c282cdd](https://github.com/cube-js/cube.js/commit/c282cdd0c1f80b444991290ba5753f8ce9ac710c))
- **cubestore:** Slow query logging ([d854303](https://github.com/cube-js/cube.js/commit/d8543033d764157139f2ffe3b6c96adaac070940))
- **docs-build:** change code font to Source Code Pro ([#1338](https://github.com/cube-js/cube.js/issues/1338)) ([79ec3db](https://github.com/cube-js/cube.js/commit/79ec3db573739ca0bbe85e92a86493232fee2991)), closes [#1337](https://github.com/cube-js/cube.js/issues/1337)
- **examples/real-time-dashboard:** Automatically deploy ([54303d8](https://github.com/cube-js/cube.js/commit/54303d88604593f48661f5980fe105d1f1bea8b4))

## [0.23.10](https://github.com/cube-js/cube.js/compare/v0.23.9...v0.23.10) (2020-11-07)

### Bug Fixes

- **@cubejs-client/playground:** add horizontal scroll and sticky head for chart card ([#1256](https://github.com/cube-js/cube.js/issues/1256)) ([025f15d](https://github.com/cube-js/cube.js/commit/025f15dbee101e12f086ef3bbe4c6cceaf543670))
- **@cubejs-playground:** codesandbox dependencies ([1ed6309](https://github.com/cube-js/cube.js/commit/1ed63096d60b241b6966b4bc29cb455214a59ee5))
- **ci:** Force usinging /build directory for netlify deployment ([7ca10f0](https://github.com/cube-js/cube.js/commit/7ca10f05cfec6e85d8eaf3042d18bd15c33b84ce))
- **ci:** Install netlify-cli instead of netlify (api client) ([60bfaa2](https://github.com/cube-js/cube.js/commit/60bfaa2531efdecc3f76af50cfe53f89752c3ae2))
- **cubejs-cli:** scaffolding/ScaffoldingTemplate dependency not found. ([8f3e6c7](https://github.com/cube-js/cube.js/commit/8f3e6c7594a406b689ed43ba1c0dd004f0a14e3b))
- **examples/drill-down:** Automatically deploy ([b04148b](https://github.com/cube-js/cube.js/commit/b04148b6bf8a5a47b927b83771f7953ae2905631))
- **examples/drill-down:** Automatically deploy ([570b903](https://github.com/cube-js/cube.js/commit/570b90341e87b458ef12873ce43f01b630abc8ac))
- **examples/highcharts:** Switch configuration for production/development ([978eb89](https://github.com/cube-js/cube.js/commit/978eb89cb4179ce9d5e3eb4008b1744ead08041c))
- **examples/highcharts:** Warnings on build ([72bb74b](https://github.com/cube-js/cube.js/commit/72bb74b6a8493611b3e3f878a1e432c49e1961e6))
- **examples/react-dashboard:** Automatically deploy ([0036016](https://github.com/cube-js/cube.js/commit/0036016b5947d95d362a52e5fe8029ec3298c58d))
- update message in CLI template ([d5a24ba](https://github.com/cube-js/cube.js/commit/d5a24ba1fad5a9b8bb1e5abed09a30b7bc5a8751))
- Warnings on installation ([cecaa6e](https://github.com/cube-js/cube.js/commit/cecaa6e9797ef23c52964b3c3e76ace6fb567e8a))

### Features

- **@cubejs-backend/server:** dev-server/server - introduce project diagnostics ([#1330](https://github.com/cube-js/cube.js/issues/1330)) ([0606926](https://github.com/cube-js/cube.js/commit/0606926146abfd33edc707efc617460b6b77e006))
- **ci:** Automatically deploy examples/highcharts ([c227137](https://github.com/cube-js/cube.js/commit/c227137793f10914485d0c05f498d759e21e3ef6))
- **cubestore:** Upgrade datafusion to 3.0 ([85f2165](https://github.com/cube-js/cube.js/commit/85f216517c6c611aca39c5f775669749a9e74387))

## [0.23.9](https://github.com/cube-js/cube.js/compare/v0.23.8...v0.23.9) (2020-11-06)

**Note:** Version bump only for package cubejs

## [0.23.8](https://github.com/cube-js/cube.js/compare/v0.23.7...v0.23.8) (2020-11-06)

### Bug Fixes

- **@cubejs-playground:** undefined query ([7d87fa6](https://github.com/cube-js/cube.js/commit/7d87fa60f207c2fa3360405a05d84fb6ffaba4c7))
- **cubejs-cli:** proxyCommand - await external command run on try/catch ([dc84460](https://github.com/cube-js/cube.js/commit/dc84460d740eedfff3a874f13316c1c2dedb9135))

### Features

- **@cubejs-backend/server:** Init source-map-support for cubejs-server/cubejs-dev-server ([aed319a](https://github.com/cube-js/cube.js/commit/aed319a33e84ba924a21a1270ee18f2ab054b9d5))
- **@cubejs-client/ws-transport:** Move to TypeScript ([#1293](https://github.com/cube-js/cube.js/issues/1293)) ([e7e1100](https://github.com/cube-js/cube.js/commit/e7e1100ee2adc7e1e9f6368c2edc6208a8eea774))
- **docker:** Use --frozen-lockfile for docker image building ([60a0ca9](https://github.com/cube-js/cube.js/commit/60a0ca9e77a8f95c40cc501dbdfd8ae80c3f8481))

## [0.23.7](https://github.com/cube-js/cube.js/compare/v0.23.6...v0.23.7) (2020-11-04)

### Bug Fixes

- **docker:** Add missing MySQL and cubestore drivers to the docker ([a36e86e](https://github.com/cube-js/cube.js/commit/a36e86e4e2524602a2a8ac09e2703e89c72796f2))

### Features

- **@cubejs-backend/server:** Migrate WebSocketServer to TS ([#1295](https://github.com/cube-js/cube.js/issues/1295)) ([94c39df](https://github.com/cube-js/cube.js/commit/94c39dfb35c0e8bed81a77cde093fd346bcd5646))
- **cubejs-cli:** Completely move CLI to TypeScript ([#1281](https://github.com/cube-js/cube.js/issues/1281)) ([dd5f3e2](https://github.com/cube-js/cube.js/commit/dd5f3e2948c82713354743af4a2727becac81388))
- Generate source maps for client libraries ([#1292](https://github.com/cube-js/cube.js/issues/1292)) ([cb64118](https://github.com/cube-js/cube.js/commit/cb64118770dce58bf7f3a3e7181cf159b8f316d3))
- **@cubejs-backend/jdbc-driver:** Upgrade vendors ([#1282](https://github.com/cube-js/cube.js/issues/1282)) ([94b9b37](https://github.com/cube-js/cube.js/commit/94b9b37484c55a4155578a84ade409035d62e152))
- **cubejs-cli:** Use env_file to pass .env file instead of sharing inside volume ([#1287](https://github.com/cube-js/cube.js/issues/1287)) ([876f549](https://github.com/cube-js/cube.js/commit/876f549dd9c5a7a79664006f6614a72a836b63ca))

## [0.23.6](https://github.com/cube-js/cube.js/compare/v0.23.5...v0.23.6) (2020-11-02)

### Bug Fixes

- **cubejs-cli:** Incorrectly generated reference to `module.export` instead of `module.exports` ([7427d46](https://github.com/cube-js/cube.js/commit/7427d463e63f173d7069ee9d8065a77013c98c2b))

### Features

- **cubejs-cli:** Add --token option for deploy command ([#1279](https://github.com/cube-js/cube.js/issues/1279)) ([4fecd8c](https://github.com/cube-js/cube.js/commit/4fecd8ca2fe6f3f85defe0ecb20ccf9b3f9a7067))

## [0.23.5](https://github.com/cube-js/cube.js/compare/v0.23.4...v0.23.5) (2020-11-02)

### Bug Fixes

- **cubejs-cli:** Deploy and Windows-style for file hashes ([ac3f62a](https://github.com/cube-js/cube.js/commit/ac3f62afd8a1957eec7b265de5c3781b70faf76c))
- **cubestore:** File is not found during list_recursive ([1065875](https://github.com/cube-js/cube.js/commit/1065875599b33c953c7e0b77f5743477929c0dc2))

## [0.23.4](https://github.com/cube-js/cube.js/compare/v0.23.3...v0.23.4) (2020-11-02)

### Bug Fixes

- **cubejs-cli:** Deploy and Windows-style paths ([#1277](https://github.com/cube-js/cube.js/issues/1277)) ([aa02f01](https://github.com/cube-js/cube.js/commit/aa02f0183008d6b49941d53321a68c59b999254d))

## [0.23.3](https://github.com/cube-js/cube.js/compare/v0.23.2...v0.23.3) (2020-10-31)

### Bug Fixes

- **@cubejs-backend/schema-compiler:** deprecation warning ([#1272](https://github.com/cube-js/cube.js/issues/1272)) ([5515465](https://github.com/cube-js/cube.js/commit/5515465))
- **ci:** Update a Docker Hub repository description automatically ([4ad0b0d](https://github.com/cube-js/cube.js/commit/4ad0b0d))
- **cubejs-cli:** @cubejs-backend/server/dist/command/dev-server dependency not found. ([e552ee1](https://github.com/cube-js/cube.js/commit/e552ee1))

### Features

- **@cubejs-backend/query-orchestrator:** add support for MSSQL nvarchar ([#1260](https://github.com/cube-js/cube.js/issues/1260)) Thanks to @JoshMentzer! ([a9e9919](https://github.com/cube-js/cube.js/commit/a9e9919))
- Dynamic Angular template ([#1257](https://github.com/cube-js/cube.js/issues/1257)) ([86ba728](https://github.com/cube-js/cube.js/commit/86ba728))

## [0.23.2](https://github.com/cube-js/cube.js/compare/v0.23.1...v0.23.2) (2020-10-28)

### Bug Fixes

- Add default ports and fix dashboard creation fails in docker ([#1267](https://github.com/cube-js/cube.js/issues/1267)) ([2929dbb](https://github.com/cube-js/cube.js/commit/2929dbb))

## [0.23.1](https://github.com/cube-js/cube.js/compare/v0.23.0...v0.23.1) (2020-10-28)

### Bug Fixes

- Unavailable. @cubejs-backend/server inside current directory requires cubejs-cli (^0.22) ([#1265](https://github.com/cube-js/cube.js/issues/1265)) ([340746e](https://github.com/cube-js/cube.js/commit/340746e))

# [0.23.0](https://github.com/cube-js/cube.js/compare/v0.22.4...v0.23.0) (2020-10-28)

### Bug Fixes

- TypeError: CubejsServer.driverDependencies is not a function ([#1264](https://github.com/cube-js/cube.js/issues/1264)) ([9b1260a](https://github.com/cube-js/cube.js/commit/9b1260a))

## [0.22.4](https://github.com/cube-js/cube.js/compare/v0.22.3...v0.22.4) (2020-10-28)

### Bug Fixes

- **Web Analytics Guide:** add links ([065a637](https://github.com/cube-js/cube.js/commit/065a637))

### Features

- **@cubejs-backend/server:** Implement dev-server & server command ([#1227](https://github.com/cube-js/cube.js/issues/1227)) ([84c1eeb](https://github.com/cube-js/cube.js/commit/84c1eeb))
- Introduce Docker template ([#1243](https://github.com/cube-js/cube.js/issues/1243)) ([e0430bf](https://github.com/cube-js/cube.js/commit/e0430bf))

## [0.22.3](https://github.com/cube-js/cube.js/compare/v0.22.2...v0.22.3) (2020-10-26)

### Bug Fixes

- **@cubejs-backend/schema-compiler:** Dialect for 'undefined' is not found, fix [#1247](https://github.com/cube-js/cube.js/issues/1247) ([1069b47](https://github.com/cube-js/cube.js/commit/1069b47ff4f0a9d2e398ba194fe3eef5ad39f0d2))

## [0.22.2](https://github.com/cube-js/cube.js/compare/v0.22.1...v0.22.2) (2020-10-26)

### Bug Fixes

- Dialect class isn't looked up for external drivers ([b793f4a](https://github.com/cube-js/cube.js/commit/b793f4a))
- **@cubejs-client/core:** duplicate names in ResultSet.seriesNames() ([#1187](https://github.com/cube-js/cube.js/issues/1187)). Thanks to [@aviranmoz](https://github.com/aviranmoz)! ([8d9eb68](https://github.com/cube-js/cube.js/commit/8d9eb68))

### Features

- Short Cube Cloud auth token ([#1222](https://github.com/cube-js/cube.js/issues/1222)) ([7885089](https://github.com/cube-js/cube.js/commit/7885089))

## [0.22.1](https://github.com/cube-js/cube.js/compare/v0.22.0...v0.22.1) (2020-10-21)

### Bug Fixes

- **@cubejs-playground:** avoid unnecessary load calls, dryRun ([#1210](https://github.com/cube-js/cube.js/issues/1210)) ([aaf4911](https://github.com/cube-js/cube.js/commit/aaf4911))
- **cube-cli:** Missed deploy command ([4192e77](https://github.com/cube-js/cube.js/commit/4192e77))

### Features

- **cubejs-cli:** Check js files by tsc ([3b9f4a2](https://github.com/cube-js/cube.js/commit/3b9f4a2))
- **cubejs-cli:** Move deploy command to TS ([b38cb4a](https://github.com/cube-js/cube.js/commit/b38cb4a))

# [0.22.0](https://github.com/cube-js/cube.js/compare/v0.21.2...v0.22.0) (2020-10-20)

### Bug Fixes

- umd build default export ([#1219](https://github.com/cube-js/cube.js/issues/1219)) ([cc434eb](https://github.com/cube-js/cube.js/commit/cc434eb))
- **@cubejs-client/core:** Add parseDateMeasures field to CubeJSApiOptions (typings) ([e1a1ada](https://github.com/cube-js/cube.js/commit/e1a1ada))
- **@cubejs-client/vue:** Allow array props on query renderer to allow data blending usage ([#1213](https://github.com/cube-js/cube.js/issues/1213)). Thanks to [@richipargo](https://github.com/richipargo) ([2203a54](https://github.com/cube-js/cube.js/commit/2203a54))
- **ci:** Specify DOCKER_IMAGE ([59bf390](https://github.com/cube-js/cube.js/commit/59bf390))
- **docs-gen:** change signature generation ([e4703ad](https://github.com/cube-js/cube.js/commit/e4703ad))

### Features

- Cube Store driver ([85ca240](https://github.com/cube-js/cube.js/commit/85ca240))
- **@cubejs-backend/server:** Introduce external commands for CLI (demo) ([fed9285](https://github.com/cube-js/cube.js/commit/fed9285))
- **cubejs-cli:** adds USER_CONTEXT parameter to cli ([#1215](https://github.com/cube-js/cube.js/issues/1215)) Thanks to @TheSPD! ([66452b9](https://github.com/cube-js/cube.js/commit/66452b9))
- **cubejs-cli:** Improve external commands support ([c13a729](https://github.com/cube-js/cube.js/commit/c13a729))
- **cubejs-cli:** Move helpers to TypeScript ([06b5f01](https://github.com/cube-js/cube.js/commit/06b5f01))
- **cubejs-cli:** Run dev-server/server commands from @cubejs-backend/core ([a35244c](https://github.com/cube-js/cube.js/commit/a35244c))
- **cubejs-cli:** Run dev-server/server commands from @cubejs-backend/core ([a4d72fe](https://github.com/cube-js/cube.js/commit/a4d72fe))
- **cubejs-cli:** Use TypeScript ([009ff7a](https://github.com/cube-js/cube.js/commit/009ff7a))

## [0.21.2](https://github.com/cube-js/cube.js/compare/v0.21.1...v0.21.2) (2020-10-15)

### Bug Fixes

- **@cubejs-client/playground:** fix setting popovers ([#1209](https://github.com/cube-js/cube.js/issues/1209)) ([644bb9f](https://github.com/cube-js/cube.js/commit/644bb9f))

## [0.21.1](https://github.com/cube-js/cube.js/compare/v0.21.0...v0.21.1) (2020-10-15)

### Bug Fixes

- **@cubejs-client/react:** resultSet ts in for QueryBuilderRenderProps ([#1193](https://github.com/cube-js/cube.js/issues/1193)) ([7e15cf0](https://github.com/cube-js/cube.js/commit/7e15cf0))

### Features

- Introduce Official Docker Image ([#1201](https://github.com/cube-js/cube.js/issues/1201)) ([0647d1f](https://github.com/cube-js/cube.js/commit/0647d1f))

# [0.21.0](https://github.com/cube-js/cube.js/compare/v0.20.15...v0.21.0) (2020-10-09)

**Note:** Version bump only for package cubejs

## [0.20.15](https://github.com/cube-js/cube.js/compare/v0.20.14...v0.20.15) (2020-10-09)

**Note:** Version bump only for package cubejs

## [0.20.14](https://github.com/cube-js/cube.js/compare/v0.20.13...v0.20.14) (2020-10-09)

### Bug Fixes

- Filter values can't be changed in Playground -- revert back defaultHeuristic implementation ([30ee112](https://github.com/cube-js/cube.js/commit/30ee112))

## [0.20.13](https://github.com/cube-js/cube.js/compare/v0.20.12...v0.20.13) (2020-10-07)

### Bug Fixes

- **@cubejs-backend/mongobi-driver:** TypeError: v.toLowerCase is not a function ([16a15cb](https://github.com/cube-js/cube.js/commit/16a15cb))
- **@cubejs-schema-compilter:** MSSQL rollingWindow with granularity ([#1169](https://github.com/cube-js/cube.js/issues/1169)) Thanks to @JoshMentzer! ([16e6a9e](https://github.com/cube-js/cube.js/commit/16e6a9e))

## [0.20.12](https://github.com/cube-js/cube.js/compare/v0.20.11...v0.20.12) (2020-10-02)

### Bug Fixes

- respect npm proxy settings ([#1137](https://github.com/cube-js/cube.js/issues/1137)) ([c43adac](https://github.com/cube-js/cube.js/commit/c43adac))
- **@cubejs-client/playground:** prepublishOnly for exports ([#1171](https://github.com/cube-js/cube.js/issues/1171)) ([5b6b4dc](https://github.com/cube-js/cube.js/commit/5b6b4dc))

### Features

- angular query builder ([#1073](https://github.com/cube-js/cube.js/issues/1073)) ([ea088b3](https://github.com/cube-js/cube.js/commit/ea088b3))
- **@cubejs-client/playground:** Export playground components ([#1170](https://github.com/cube-js/cube.js/issues/1170)) ([fb22331](https://github.com/cube-js/cube.js/commit/fb22331))

## [0.20.11](https://github.com/cube-js/cube.js/compare/v0.20.10...v0.20.11) (2020-09-28)

### Bug Fixes

- **@cubejs-backend/prestodb-driver:** Wrong OFFSET/LIMIT order ([#1135](https://github.com/cube-js/cube.js/issues/1135)) ([3b94b2c](https://github.com/cube-js/cube.js/commit/3b94b2c)), closes [#988](https://github.com/cube-js/cube.js/issues/988) [#988](https://github.com/cube-js/cube.js/issues/988) [#988](https://github.com/cube-js/cube.js/issues/988)
- **example:** Material UI Dashboard link ([f4c4170](https://github.com/cube-js/cube.js/commit/f4c4170))
- propagate drill down parent filters ([#1143](https://github.com/cube-js/cube.js/issues/1143)) ([314985e](https://github.com/cube-js/cube.js/commit/314985e))

### Features

- Introduce Druid driver ([#1099](https://github.com/cube-js/cube.js/issues/1099)) ([2bfe20f](https://github.com/cube-js/cube.js/commit/2bfe20f))

## [0.20.10](https://github.com/cube-js/cube.js/compare/v0.20.9...v0.20.10) (2020-09-23)

### Bug Fixes

- **@cubejs-backend/server-core:** Allow initApp as server-core option ([#1115](https://github.com/cube-js/cube.js/issues/1115)) ([a9d06fd](https://github.com/cube-js/cube.js/commit/a9d06fd))
- **@cubejs-backend/server-core:** Allow processSubscriptionsInterval as an option ([#1122](https://github.com/cube-js/cube.js/issues/1122)) ([cf21d70](https://github.com/cube-js/cube.js/commit/cf21d70))
- drilling into any measure other than the first ([#1131](https://github.com/cube-js/cube.js/issues/1131)) ([e741a20](https://github.com/cube-js/cube.js/commit/e741a20))
- rollupOnlyMode option validation ([#1127](https://github.com/cube-js/cube.js/issues/1127)) ([89ee308](https://github.com/cube-js/cube.js/commit/89ee308))
- **@cubejs-backend/server-core:** Support apiSecret as option ([#1130](https://github.com/cube-js/cube.js/issues/1130)) ([9fbf544](https://github.com/cube-js/cube.js/commit/9fbf544))

## [0.20.9](https://github.com/cube-js/cube.js/compare/v0.20.8...v0.20.9) (2020-09-19)

### Bug Fixes

- Allow empty complex boolean filter arrays ([#1100](https://github.com/cube-js/cube.js/issues/1100)) ([80d112e](https://github.com/cube-js/cube.js/commit/80d112e))
- Allow scheduledRefreshContexts in server-core options validation ([#1105](https://github.com/cube-js/cube.js/issues/1105)) ([7e43276](https://github.com/cube-js/cube.js/commit/7e43276))
- **@cubejs-backend/server-core:** orchestratorOptions validation breaks serverless deployments ([#1113](https://github.com/cube-js/cube.js/issues/1113)) ([48ca5aa](https://github.com/cube-js/cube.js/commit/48ca5aa))

### Features

- **cubejs-cli:** Ask question about database, if user forget to specify it with -d flag ([#1096](https://github.com/cube-js/cube.js/issues/1096)) ([8b5b9d3](https://github.com/cube-js/cube.js/commit/8b5b9d3))
- `sqlAlias` attribute for `preAggregations` and short format for pre-aggregation table names ([#1068](https://github.com/cube-js/cube.js/issues/1068)) ([98ffad3](https://github.com/cube-js/cube.js/commit/98ffad3)), closes [#86](https://github.com/cube-js/cube.js/issues/86) [#907](https://github.com/cube-js/cube.js/issues/907)
- Share Node's version for debug purposes ([#1107](https://github.com/cube-js/cube.js/issues/1107)) ([26c0420](https://github.com/cube-js/cube.js/commit/26c0420))

## [0.20.8](https://github.com/cube-js/cube.js/compare/v0.20.7...v0.20.8) (2020-09-16)

### Bug Fixes

- **@cubejs-backend/athena-driver:** Show views in Playground for Athena ([#1090](https://github.com/cube-js/cube.js/issues/1090)) ([f8ce729](https://github.com/cube-js/cube.js/commit/f8ce729))
- validated query behavior ([#1085](https://github.com/cube-js/cube.js/issues/1085)) ([e93891b](https://github.com/cube-js/cube.js/commit/e93891b))
- **@cubejs-backend/elasticsearch-driver:** Respect `ungrouped` flag ([#1098](https://github.com/cube-js/cube.js/issues/1098)) Thanks to [@vignesh-123](https://github.com/vignesh-123)! ([995b8f9](https://github.com/cube-js/cube.js/commit/995b8f9))

### Features

- Add server-core options validate ([#1089](https://github.com/cube-js/cube.js/issues/1089)) ([5580018](https://github.com/cube-js/cube.js/commit/5580018))
- refreshKey every support for CRON format interval ([#1048](https://github.com/cube-js/cube.js/issues/1048)) ([3e55f5c](https://github.com/cube-js/cube.js/commit/3e55f5c))
- Strict cube schema parsing, show duplicate property name errors ([#1095](https://github.com/cube-js/cube.js/issues/1095)) ([d4ab530](https://github.com/cube-js/cube.js/commit/d4ab530))

## [0.20.7](https://github.com/cube-js/cube.js/compare/v0.20.6...v0.20.7) (2020-09-11)

### Bug Fixes

- member-dimension query normalization for queryTransformer and additional complex boolean logic tests ([#1047](https://github.com/cube-js/cube.js/issues/1047)) ([65ef327](https://github.com/cube-js/cube.js/commit/65ef327)), closes [#1007](https://github.com/cube-js/cube.js/issues/1007)

## [0.20.6](https://github.com/cube-js/cube.js/compare/v0.20.5...v0.20.6) (2020-09-10)

### Bug Fixes

- pivot control ([05ce626](https://github.com/cube-js/cube.js/commit/05ce626))

## [0.20.5](https://github.com/cube-js/cube.js/compare/v0.20.4...v0.20.5) (2020-09-10)

### Bug Fixes

- cube-client-core resolveMember return type ([#1051](https://github.com/cube-js/cube.js/issues/1051)). Thanks to @Aaronkala ([662cfe0](https://github.com/cube-js/cube.js/commit/662cfe0))
- improve TimeDimensionGranularity type ([#1052](https://github.com/cube-js/cube.js/issues/1052)). Thanks to [@joealden](https://github.com/joealden) ([1e9bd99](https://github.com/cube-js/cube.js/commit/1e9bd99))
- query logger ([e5d6ce9](https://github.com/cube-js/cube.js/commit/e5d6ce9))

## [0.20.4](https://github.com/cube-js/cube.js/compare/v0.20.3...v0.20.4) (2020-09-04)

### Bug Fixes

- **@cubejs-backend/dremio-driver:** CAST doesn't work on string timestamps: replace CAST to TO_TIMESTAMP ([#1057](https://github.com/cube-js/cube.js/issues/1057)) ([59da9ae](https://github.com/cube-js/cube.js/commit/59da9ae))

## [0.20.3](https://github.com/cube-js/cube.js/compare/v0.20.2...v0.20.3) (2020-09-03)

### Bug Fixes

- Export the TimeDimensionGranularity type ([#1044](https://github.com/cube-js/cube.js/issues/1044)). Thanks to [@gudjonragnar](https://github.com/gudjonragnar) ([26b329e](https://github.com/cube-js/cube.js/commit/26b329e))

### Features

- Complex boolean logic ([#1038](https://github.com/cube-js/cube.js/issues/1038)) ([a5b44d1](https://github.com/cube-js/cube.js/commit/a5b44d1)), closes [#259](https://github.com/cube-js/cube.js/issues/259)

## [0.20.2](https://github.com/cube-js/cube.js/compare/v0.20.1...v0.20.2) (2020-09-02)

### Bug Fixes

- subscribe option, new query types to work with ws ([dbf602e](https://github.com/cube-js/cube.js/commit/dbf602e))

### Features

- custom date range ([#1027](https://github.com/cube-js/cube.js/issues/1027)) ([304985f](https://github.com/cube-js/cube.js/commit/304985f))

## [0.20.1](https://github.com/cube-js/cube.js/compare/v0.20.0...v0.20.1) (2020-09-01)

### Bug Fixes

- data blending query support ([#1033](https://github.com/cube-js/cube.js/issues/1033)) ([20fc979](https://github.com/cube-js/cube.js/commit/20fc979))
- Error: TypeError: Cannot read property ‘externalPreAggregationQuery’ of null ([e23f302](https://github.com/cube-js/cube.js/commit/e23f302))

### Features

- Expose the progress response in the useCubeQuery hook ([#990](https://github.com/cube-js/cube.js/issues/990)). Thanks to [@anton164](https://github.com/anton164) ([01da1fd](https://github.com/cube-js/cube.js/commit/01da1fd))
- scheduledRefreshContexts CubejsServerCore option ([789a098](https://github.com/cube-js/cube.js/commit/789a098))

# [0.20.0](https://github.com/cube-js/cube.js/compare/v0.19.61...v0.20.0) (2020-08-26)

### Bug Fixes

- **@cubejs-backend/athena-driver:** Error: Queries of this type are not supported for incremental refreshKey ([2d3018d](https://github.com/cube-js/cube.js/commit/2d3018d)), closes [#404](https://github.com/cube-js/cube.js/issues/404)
- Check partitionGranularity requires timeDimensionReference for `originalSql` ([2a2b256](https://github.com/cube-js/cube.js/commit/2a2b256))
- Refresh Scheduler should respect `dataSource` ([d7e7a57](https://github.com/cube-js/cube.js/commit/d7e7a57))
- respect timezone in drillDown queries ([#1003](https://github.com/cube-js/cube.js/issues/1003)) ([c128417](https://github.com/cube-js/cube.js/commit/c128417))
- **@cubejs-backend/clickhouse-driver:** allow default compound indexes: add parentheses to the pre-aggregation sql definition ([#1009](https://github.com/cube-js/cube.js/issues/1009)) Thanks to [@gudjonragnar](https://github.com/gudjonragnar)! ([6535cb6](https://github.com/cube-js/cube.js/commit/6535cb6))
- TypeError: Cannot read property '1' of undefined -- Using scheduled cube refresh endpoint not working with Athena ([ed6c9aa](https://github.com/cube-js/cube.js/commit/ed6c9aa)), closes [#1000](https://github.com/cube-js/cube.js/issues/1000)

### Features

- add post method for the load endpoint ([#982](https://github.com/cube-js/cube.js/issues/982)). Thanks to @RusovDmitriy ([1524ede](https://github.com/cube-js/cube.js/commit/1524ede))
- Data blending ([#1012](https://github.com/cube-js/cube.js/issues/1012)) ([19fd00e](https://github.com/cube-js/cube.js/commit/19fd00e))
- date range comparison support ([#979](https://github.com/cube-js/cube.js/issues/979)) ([ca21cfd](https://github.com/cube-js/cube.js/commit/ca21cfd))
- Dremio driver ([#1008](https://github.com/cube-js/cube.js/issues/1008)) ([617225f](https://github.com/cube-js/cube.js/commit/617225f))
- Make the Filter type more specific. ([#915](https://github.com/cube-js/cube.js/issues/915)) Thanks to [@ylixir](https://github.com/ylixir) ([cecdb36](https://github.com/cube-js/cube.js/commit/cecdb36))
- query limit control ([#910](https://github.com/cube-js/cube.js/issues/910)) ([c6e086b](https://github.com/cube-js/cube.js/commit/c6e086b))

## [0.19.61](https://github.com/cube-js/cube.js/compare/v0.19.60...v0.19.61) (2020-08-11)

### Bug Fixes

- avoid opening connection to the source database when caching tables from external rollup db ([#929](https://github.com/cube-js/cube.js/issues/929)) Thanks to [@jcw](https://github.com/jcw)-! ([92cd0b3](https://github.com/cube-js/cube.js/commit/92cd0b3))
- readOnly originalSql pre-aggregations aren't working without writing rights ([cfa7c7d](https://github.com/cube-js/cube.js/commit/cfa7c7d))

### Features

- add support of array of tuples order format ([#973](https://github.com/cube-js/cube.js/issues/973)). Thanks to @RusovDmitriy ([0950b94](https://github.com/cube-js/cube.js/commit/0950b94))
- **mssql-driver:** add readonly aggregation for mssql sources ([#920](https://github.com/cube-js/cube.js/issues/920)) Thanks to @JoshMentzer! ([dfeccca](https://github.com/cube-js/cube.js/commit/dfeccca))

## [0.19.60](https://github.com/cube-js/cube.js/compare/v0.19.59...v0.19.60) (2020-08-08)

### Bug Fixes

- Intermittent errors with empty rollups or not ready metadata for Athena and MySQL: HIVE_CANNOT_OPEN_SPLIT errors. ([fa2cf45](https://github.com/cube-js/cube.js/commit/fa2cf45))

## [0.19.59](https://github.com/cube-js/cube.js/compare/v0.19.58...v0.19.59) (2020-08-05)

### Bug Fixes

- appying templates in a git repo ([#952](https://github.com/cube-js/cube.js/issues/952)) ([3556a74](https://github.com/cube-js/cube.js/commit/3556a74))

## [0.19.58](https://github.com/cube-js/cube.js/compare/v0.19.57...v0.19.58) (2020-08-05)

### Bug Fixes

- Error: Cannot find module 'axios' ([5fcfa87](https://github.com/cube-js/cube.js/commit/5fcfa87))

## [0.19.57](https://github.com/cube-js/cube.js/compare/v0.19.56...v0.19.57) (2020-08-05)

### Bug Fixes

- bizcharts incorrect geom type ([#941](https://github.com/cube-js/cube.js/issues/941)) ([7df66d8](https://github.com/cube-js/cube.js/commit/7df66d8))

### Features

- Playground templates separate repository open for third party contributions ([#903](https://github.com/cube-js/cube.js/issues/903)) ([fb57bda](https://github.com/cube-js/cube.js/commit/fb57bda))
- support first chance to define routes ([#931](https://github.com/cube-js/cube.js/issues/931)) Thanks to [@jsw](https://github.com/jsw)- ([69fdebc](https://github.com/cube-js/cube.js/commit/69fdebc))

## [0.19.56](https://github.com/cube-js/cube.js/compare/v0.19.55...v0.19.56) (2020-08-03)

### Bug Fixes

- allow renewQuery in dev mode with warning ([#868](https://github.com/cube-js/cube.js/issues/868)) Thanks to [@jcw](https://github.com/jcw)-! ([dbdbb5f](https://github.com/cube-js/cube.js/commit/dbdbb5f))
- CubeContext ts type missing ([#913](https://github.com/cube-js/cube.js/issues/913)) ([f5f72cd](https://github.com/cube-js/cube.js/commit/f5f72cd))
- membersForQuery return type ([#909](https://github.com/cube-js/cube.js/issues/909)) ([4976fcf](https://github.com/cube-js/cube.js/commit/4976fcf))
- readme examples updates ([#893](https://github.com/cube-js/cube.js/issues/893)) ([0458af8](https://github.com/cube-js/cube.js/commit/0458af8))
- using limit and offset together in MSSql ([9ba875c](https://github.com/cube-js/cube.js/commit/9ba875c))
- Various ClickHouse improvements ([6f40847](https://github.com/cube-js/cube.js/commit/6f40847))

## [0.19.55](https://github.com/cube-js/cube.js/compare/v0.19.54...v0.19.55) (2020-07-23)

### Bug Fixes

- ngx client installation ([#898](https://github.com/cube-js/cube.js/issues/898)) ([31ab9a0](https://github.com/cube-js/cube.js/commit/31ab9a0))

### Features

- expose loadResponse annotation ([#894](https://github.com/cube-js/cube.js/issues/894)) ([2875d47](https://github.com/cube-js/cube.js/commit/2875d47))

## [0.19.54](https://github.com/cube-js/cube.js/compare/v0.19.53...v0.19.54) (2020-07-23)

### Bug Fixes

- Orphaned queries in Redis queue during intensive load ([101b85f](https://github.com/cube-js/cube.js/commit/101b85f))

## [0.19.53](https://github.com/cube-js/cube.js/compare/v0.19.52...v0.19.53) (2020-07-20)

### Bug Fixes

- preserve order of sorted data ([#870](https://github.com/cube-js/cube.js/issues/870)) ([861db10](https://github.com/cube-js/cube.js/commit/861db10))

### Features

- More logging info for Orphaned Queries debugging ([99bf957](https://github.com/cube-js/cube.js/commit/99bf957))

## [0.19.52](https://github.com/cube-js/cube.js/compare/v0.19.51...v0.19.52) (2020-07-18)

### Bug Fixes

- Redis driver execAsync ignores watch directives ([ac67e5b](https://github.com/cube-js/cube.js/commit/ac67e5b))

## [0.19.51](https://github.com/cube-js/cube.js/compare/v0.19.50...v0.19.51) (2020-07-17)

**Note:** Version bump only for package cubejs

## [0.19.50](https://github.com/cube-js/cube.js/compare/v0.19.49...v0.19.50) (2020-07-16)

### Bug Fixes

- **cubejs-client-vue:** added deep watch at query props object in Vue QueryBuilder ([#818](https://github.com/cube-js/cube.js/issues/818)) ([32402e6](https://github.com/cube-js/cube.js/commit/32402e6))
- filter out falsy members ([65b19c9](https://github.com/cube-js/cube.js/commit/65b19c9))

### Features

- Generic readOnly external rollup implementation. MongoDB support. ([79d7bfd](https://github.com/cube-js/cube.js/commit/79d7bfd)), closes [#239](https://github.com/cube-js/cube.js/issues/239)
- ResultSet serializaion and deserializaion ([#836](https://github.com/cube-js/cube.js/issues/836)) ([80b8d41](https://github.com/cube-js/cube.js/commit/80b8d41))
- Rollup mode ([#843](https://github.com/cube-js/cube.js/issues/843)) Thanks to [@jcw](https://github.com/jcw)-! ([cc41f97](https://github.com/cube-js/cube.js/commit/cc41f97))

## [0.19.49](https://github.com/cube-js/cube.js/compare/v0.19.48...v0.19.49) (2020-07-11)

### Bug Fixes

- TypeError: exports.en is not a function ([ade2ccd](https://github.com/cube-js/cube.js/commit/ade2ccd))

## [0.19.48](https://github.com/cube-js/cube.js/compare/v0.19.47...v0.19.48) (2020-07-11)

### Bug Fixes

- **cubejs-client-core:** enums exported from declaration files are not accessible ([#810](https://github.com/cube-js/cube.js/issues/810)) ([3396fbe](https://github.com/cube-js/cube.js/commit/3396fbe))
- chrono-node upgrade changed `from 60 minutes ago to now` behavior ([e456829](https://github.com/cube-js/cube.js/commit/e456829))

## [0.19.46](https://github.com/cube-js/cube.js/compare/v0.19.45...v0.19.46) (2020-07-06)

### Features

- Report query usage for Athena and BigQuery ([697b53f](https://github.com/cube-js/cube.js/commit/697b53f))

## [0.19.45](https://github.com/cube-js/cube.js/compare/v0.19.44...v0.19.45) (2020-07-04)

### Bug Fixes

- Error: Error: Class constructor cannot be invoked without 'new' ([beb75df](https://github.com/cube-js/cube.js/commit/beb75df))
- TypeError: (queryOptions.dialectClass || ADAPTERS[dbType]) is not a constructor ([502480c](https://github.com/cube-js/cube.js/commit/502480c))

## [0.19.44](https://github.com/cube-js/cube.js/compare/v0.19.43...v0.19.44) (2020-07-04)

### Bug Fixes

- Error: Unsupported db type: function ([13d1b93](https://github.com/cube-js/cube.js/commit/13d1b93))

## [0.19.43](https://github.com/cube-js/cube.js/compare/v0.19.42...v0.19.43) (2020-07-04)

### Bug Fixes

- **cubejs-client-core:** Display the measure value when the y axis is empty ([#789](https://github.com/cube-js/cube.js/issues/789)) ([7ec6ac6](https://github.com/cube-js/cube.js/commit/7ec6ac6))
- **docs-gen:** Menu order ([#783](https://github.com/cube-js/cube.js/issues/783)) ([11d974a](https://github.com/cube-js/cube.js/commit/11d974a))

### Features

- `CUBEJS_EXT_DB_*` env variables support ([3a4c921](https://github.com/cube-js/cube.js/commit/3a4c921))
- Adjust client options to send credentials when needed ([#790](https://github.com/cube-js/cube.js/issues/790)) Thanks to [@colefichter](https://github.com/colefichter) ! ([5203f6c](https://github.com/cube-js/cube.js/commit/5203f6c)), closes [#788](https://github.com/cube-js/cube.js/issues/788)
- Pluggable dialects support ([f786fdd](https://github.com/cube-js/cube.js/commit/f786fdd)), closes [#590](https://github.com/cube-js/cube.js/issues/590)

## [0.19.42](https://github.com/cube-js/cube.js/compare/v0.19.41...v0.19.42) (2020-07-01)

### Bug Fixes

- **docs-gen:** generation fixes ([1598a9b](https://github.com/cube-js/cube.js/commit/1598a9b))
- **docs-gen:** titles ([12a1a5f](https://github.com/cube-js/cube.js/commit/12a1a5f))

### Features

- `CUBEJS_SCHEDULED_REFRESH_TIMEZONES` env variable ([d22e3f0](https://github.com/cube-js/cube.js/commit/d22e3f0))

## [0.19.41](https://github.com/cube-js/cube.js/compare/v0.19.40...v0.19.41) (2020-06-30)

### Bug Fixes

- **docs-gen:** generator fixes, docs updates ([c5b26d0](https://github.com/cube-js/cube.js/commit/c5b26d0))
- **docs-gen:** minor fixes ([#771](https://github.com/cube-js/cube.js/issues/771)) ([ae32519](https://github.com/cube-js/cube.js/commit/ae32519))
- scheduledRefreshTimer.match is not a function ([caecc51](https://github.com/cube-js/cube.js/commit/caecc51)), closes [#772](https://github.com/cube-js/cube.js/issues/772)

## [0.19.40](https://github.com/cube-js/cube.js/compare/v0.19.39...v0.19.40) (2020-06-30)

### Bug Fixes

- Querying empty Postgres table with 'time' dimension in a cube results in null value ([07d00f8](https://github.com/cube-js/cube.js/commit/07d00f8)), closes [#639](https://github.com/cube-js/cube.js/issues/639)

### Features

- CUBEJS_SCHEDULED_REFRESH_TIMER env variable ([6d0096e](https://github.com/cube-js/cube.js/commit/6d0096e))
- **docs-gen:** Typedoc generator ([#769](https://github.com/cube-js/cube.js/issues/769)) ([15373eb](https://github.com/cube-js/cube.js/commit/15373eb))

## [0.19.39](https://github.com/cube-js/cube.js/compare/v0.19.38...v0.19.39) (2020-06-28)

### Bug Fixes

- treat wildcard Elasticsearch select as simple asterisk select: include \* as part of RE to support elasticsearch indexes ([#760](https://github.com/cube-js/cube.js/issues/760)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar) ! ([099a888](https://github.com/cube-js/cube.js/commit/099a888))

### Features

- `refreshRangeStart` and `refreshRangeEnd` pre-aggregation params ([e4d2874](https://github.com/cube-js/cube.js/commit/e4d2874))

## [0.19.38](https://github.com/cube-js/cube.js/compare/v0.19.37...v0.19.38) (2020-06-28)

### Bug Fixes

- **cubejs-playground:** Long line ellipsis ([#761](https://github.com/cube-js/cube.js/issues/761)) ([4aee9dc](https://github.com/cube-js/cube.js/commit/4aee9dc))
- Refresh partitioned pre-aggregations sequentially to avoid excessive memory and Redis connection consumption ([38aab17](https://github.com/cube-js/cube.js/commit/38aab17))

## [0.19.37](https://github.com/cube-js/cube.js/compare/v0.19.36...v0.19.37) (2020-06-26)

### Bug Fixes

- **cubejs-client-core:** tableColumns empty data fix ([#750](https://github.com/cube-js/cube.js/issues/750)) ([0ac9b7a](https://github.com/cube-js/cube.js/commit/0ac9b7a))
- **cubejs-client-react:** order heuristic ([#758](https://github.com/cube-js/cube.js/issues/758)) ([498c10a](https://github.com/cube-js/cube.js/commit/498c10a))

### Features

- **cubejs-client-react:** Exposing updateQuery method ([#751](https://github.com/cube-js/cube.js/issues/751)) ([e2083c8](https://github.com/cube-js/cube.js/commit/e2083c8))
- query builder pivot config support ([#742](https://github.com/cube-js/cube.js/issues/742)) ([4e29057](https://github.com/cube-js/cube.js/commit/4e29057))

## [0.19.36](https://github.com/cube-js/cube.js/compare/v0.19.35...v0.19.36) (2020-06-24)

### Bug Fixes

- Avoid excessive pre-aggregation invalidation in presence of multiple structure versions ([fd5e602](https://github.com/cube-js/cube.js/commit/fd5e602))

## [0.19.35](https://github.com/cube-js/cube.js/compare/v0.19.34...v0.19.35) (2020-06-22)

### Bug Fixes

- **cubejs-client-core:** table pivot ([#672](https://github.com/cube-js/cube.js/issues/672)) ([70015f5](https://github.com/cube-js/cube.js/commit/70015f5))
- header ([#734](https://github.com/cube-js/cube.js/issues/734)) ([056275a](https://github.com/cube-js/cube.js/commit/056275a))
- Scheduler request annotation for `/v1/run-scheduled-refresh` ([8273544](https://github.com/cube-js/cube.js/commit/8273544))
- table ([#740](https://github.com/cube-js/cube.js/issues/740)) ([6f1a8e7](https://github.com/cube-js/cube.js/commit/6f1a8e7))

## [0.19.34](https://github.com/cube-js/cube.js/compare/v0.19.33...v0.19.34) (2020-06-10)

### Bug Fixes

- **cubejs-cli:** Check if correct directory is being deployed ([56b8319](https://github.com/cube-js/cube.js/commit/56b8319))

## [0.19.33](https://github.com/cube-js/cube.js/compare/v0.19.32...v0.19.33) (2020-06-10)

### Bug Fixes

- **cubejs-api-gateway:** fromEntries replacement ([#715](https://github.com/cube-js/cube.js/issues/715)) ([998c735](https://github.com/cube-js/cube.js/commit/998c735))

## [0.19.32](https://github.com/cube-js/cube.js/compare/v0.19.31...v0.19.32) (2020-06-10)

### Bug Fixes

- Cannot read property 'reorder' of undefined ([3f1d8d1](https://github.com/cube-js/cube.js/commit/3f1d8d1))

## [0.19.31](https://github.com/cube-js/cube.js/compare/v0.19.30...v0.19.31) (2020-06-10)

### Bug Fixes

- **cubejs-cli:** linter ([#712](https://github.com/cube-js/cube.js/issues/712)) ([53c053f](https://github.com/cube-js/cube.js/commit/53c053f))
- **cubejs-client-core:** Remove Content-Type header from requests in HttpTransport ([#709](https://github.com/cube-js/cube.js/issues/709)) ([f6e366c](https://github.com/cube-js/cube.js/commit/f6e366c))

### Features

- **cubejs-cli:** Save deploy credentials ([af7e930](https://github.com/cube-js/cube.js/commit/af7e930))
- add schema path as an environment variable. ([#711](https://github.com/cube-js/cube.js/issues/711)) ([5ee2e16](https://github.com/cube-js/cube.js/commit/5ee2e16)), closes [#695](https://github.com/cube-js/cube.js/issues/695)
- Query builder order by ([#685](https://github.com/cube-js/cube.js/issues/685)) ([d3c735b](https://github.com/cube-js/cube.js/commit/d3c735b))

## [0.19.30](https://github.com/cube-js/cube.js/compare/v0.19.29...v0.19.30) (2020-06-09)

### Bug Fixes

- **cubejs-cli:** Fix file hashing for Cube Cloud ([ce8e090](https://github.com/cube-js/cube.js/commit/ce8e090))

## [0.19.29](https://github.com/cube-js/cube.js/compare/v0.19.28...v0.19.29) (2020-06-09)

### Bug Fixes

- **cubejs-cli:** eslint fixes ([0aa8001](https://github.com/cube-js/cube.js/commit/0aa8001))

## [0.19.28](https://github.com/cube-js/cube.js/compare/v0.19.27...v0.19.28) (2020-06-09)

### Bug Fixes

- **cubejs-cli:** Correct missing auth error ([ceeaff7](https://github.com/cube-js/cube.js/commit/ceeaff7))

## [0.19.27](https://github.com/cube-js/cube.js/compare/v0.19.26...v0.19.27) (2020-06-09)

**Note:** Version bump only for package cubejs

## [0.19.26](https://github.com/cube-js/cube.js/compare/v0.19.25...v0.19.26) (2020-06-09)

**Note:** Version bump only for package cubejs

## [0.19.25](https://github.com/cube-js/cube.js/compare/v0.19.24...v0.19.25) (2020-06-09)

### Features

- **cubejs-cli:** Cube Cloud deploy implementation ([b34ba53](https://github.com/cube-js/cube.js/commit/b34ba53))

## [0.19.24](https://github.com/cube-js/cube.js/compare/v0.19.23...v0.19.24) (2020-06-06)

### Bug Fixes

- **@cubejs-backend/elasticsearch-driver:** respect ungrouped parameter ([#684](https://github.com/cube-js/cube.js/issues/684)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar)! ([27d0d49](https://github.com/cube-js/cube.js/commit/27d0d49))
- **@cubejs-backend/schema-compiler:** TypeError: methods.filter is not a function ([25c4ef6](https://github.com/cube-js/cube.js/commit/25c4ef6))

## [0.19.23](https://github.com/cube-js/cube.js/compare/v0.19.22...v0.19.23) (2020-06-02)

### Features

- drill down queries support ([#664](https://github.com/cube-js/cube.js/issues/664)) ([7e21545](https://github.com/cube-js/cube.js/commit/7e21545)), closes [#190](https://github.com/cube-js/cube.js/issues/190)

## [0.19.22](https://github.com/cube-js/cube.js/compare/v0.19.21...v0.19.22) (2020-05-26)

**Note:** Version bump only for package cubejs

## [0.19.21](https://github.com/cube-js/cube.js/compare/v0.19.20...v0.19.21) (2020-05-25)

### Bug Fixes

- **@cubejs-backend/sqlite-driver:** sqlite name and type extraction ([#659](https://github.com/cube-js/cube.js/issues/659)) Thanks to [@avin3sh](https://github.com/avin3sh) ! ([b1c179d](https://github.com/cube-js/cube.js/commit/b1c179d))
- **playground:** Dynamic dashboard templated doesn't work: graphql-tools version downgrade ([#665](https://github.com/cube-js/cube.js/issues/665)) ([f5dfe54](https://github.com/cube-js/cube.js/commit/f5dfe54)), closes [#661](https://github.com/cube-js/cube.js/issues/661)

## [0.19.20](https://github.com/cube-js/cube.js/compare/v0.19.19...v0.19.20) (2020-05-21)

### Bug Fixes

- **cubejs-playground:** header style ([8d0f6a9](https://github.com/cube-js/cube.js/commit/8d0f6a9))
- **cubejs-playground:** style fixes ([fadbdf2](https://github.com/cube-js/cube.js/commit/fadbdf2))
- **cubejs-postgres-driver:** updated pg version ([af758f6](https://github.com/cube-js/cube.js/commit/af758f6))

## [0.19.19](https://github.com/cube-js/cube.js/compare/v0.19.18...v0.19.19) (2020-05-15)

### Bug Fixes

- corejs version ([8bef3b2](https://github.com/cube-js/cube.js/commit/8bef3b2))
- **client-vue:** updateChartType fix ([#644](https://github.com/cube-js/cube.js/issues/644)) ([5c0e79c](https://github.com/cube-js/cube.js/commit/5c0e79c)), closes [#635](https://github.com/cube-js/cube.js/issues/635)

### Features

- ability to add custom meta data for measures, dimensions and segments ([#641](https://github.com/cube-js/cube.js/issues/641)) ([88d5c9b](https://github.com/cube-js/cube.js/commit/88d5c9b)), closes [#625](https://github.com/cube-js/cube.js/issues/625)

## [0.19.18](https://github.com/cube-js/cube.js/compare/v0.19.17...v0.19.18) (2020-05-11)

### Bug Fixes

- Offset doesn't affect actual queries ([1feaa38](https://github.com/cube-js/cube.js/commit/1feaa38)), closes [#636](https://github.com/cube-js/cube.js/issues/636)

## [0.19.17](https://github.com/cube-js/cube.js/compare/v0.19.16...v0.19.17) (2020-05-09)

### Bug Fixes

- Continue wait errors during tables fetch ([cafaa28](https://github.com/cube-js/cube.js/commit/cafaa28))

## [0.19.16](https://github.com/cube-js/cube.js/compare/v0.19.15...v0.19.16) (2020-05-07)

### Bug Fixes

- **@cubejs-client/react:** options dependency for useEffect: check if `subscribe` has been changed in `useCubeQuery` ([#632](https://github.com/cube-js/cube.js/issues/632)) ([13ab5de](https://github.com/cube-js/cube.js/commit/13ab5de))

### Features

- Update type defs for query transformer ([#619](https://github.com/cube-js/cube.js/issues/619)) Thanks to [@jcw](https://github.com/jcw)-! ([b396b05](https://github.com/cube-js/cube.js/commit/b396b05))

## [0.19.15](https://github.com/cube-js/cube.js/compare/v0.19.14...v0.19.15) (2020-05-04)

### Bug Fixes

- Max date measures incorrectly converted for MySQL ([e704867](https://github.com/cube-js/cube.js/commit/e704867))

### Features

- Include version in startup message ([#615](https://github.com/cube-js/cube.js/issues/615)) Thanks to jcw-! ([d2f1732](https://github.com/cube-js/cube.js/commit/d2f1732))
- More pre-aggregation info logging ([9d69f98](https://github.com/cube-js/cube.js/commit/9d69f98))
- Tweak server type definitions ([#623](https://github.com/cube-js/cube.js/issues/623)) Thanks to [@willhausman](https://github.com/willhausman)! ([23da279](https://github.com/cube-js/cube.js/commit/23da279))

## [0.19.14](https://github.com/cube-js/cube.js/compare/v0.19.13...v0.19.14) (2020-04-24)

### Bug Fixes

- More descriptive errors for download errors ([e834aba](https://github.com/cube-js/cube.js/commit/e834aba))
- Show Postgres params in logs ([a678ca7](https://github.com/cube-js/cube.js/commit/a678ca7))

### Features

- Postgres HLL improvements: always round to int ([#611](https://github.com/cube-js/cube.js/issues/611)) Thanks to [@milanbella](https://github.com/milanbella)! ([680a613](https://github.com/cube-js/cube.js/commit/680a613))

## [0.19.13](https://github.com/cube-js/cube.js/compare/v0.19.12...v0.19.13) (2020-04-21)

### Features

- Postgres Citus Data HLL plugin implementation ([#601](https://github.com/cube-js/cube.js/issues/601)) Thanks to [@milanbella](https://github.com/milanbella) ! ([be85ac6](https://github.com/cube-js/cube.js/commit/be85ac6)), closes [#563](https://github.com/cube-js/cube.js/issues/563)
- **react:** `resetResultSetOnChange` option for `QueryRenderer` and `useCubeQuery` ([c8c74d3](https://github.com/cube-js/cube.js/commit/c8c74d3))

## [0.19.12](https://github.com/cube-js/cube.js/compare/v0.19.11...v0.19.12) (2020-04-20)

### Bug Fixes

- Make date measure parsing optional ([d199cd5](https://github.com/cube-js/cube.js/commit/d199cd5)), closes [#602](https://github.com/cube-js/cube.js/issues/602)

## [0.19.11](https://github.com/cube-js/cube.js/compare/v0.19.10...v0.19.11) (2020-04-20)

### Bug Fixes

- Strict date range and rollup granularity alignment check ([deb62b6](https://github.com/cube-js/cube.js/commit/deb62b6)), closes [#103](https://github.com/cube-js/cube.js/issues/103)

## [0.19.10](https://github.com/cube-js/cube.js/compare/v0.19.9...v0.19.10) (2020-04-18)

### Bug Fixes

- Recursive pre-aggregation description generation: support propagateFiltersToSubQuery with partitioned originalSql ([6a2b9dd](https://github.com/cube-js/cube.js/commit/6a2b9dd))

## [0.19.9](https://github.com/cube-js/cube.js/compare/v0.19.8...v0.19.9) (2020-04-16)

### Features

- add await when invoking schemaVersion -- support async schemaVersion ([#557](https://github.com/cube-js/cube.js/issues/557)) Thanks to [@barakcoh](https://github.com/barakcoh)! ([964c6d8](https://github.com/cube-js/cube.js/commit/964c6d8))
- Added support for websocketsBasePath ([#584](https://github.com/cube-js/cube.js/issues/584)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar)! ([0fa7349](https://github.com/cube-js/cube.js/commit/0fa7349)), closes [#583](https://github.com/cube-js/cube.js/issues/583)
- Allow persisting multiple pre-aggregation structure versions to support staging pre-aggregation warm-up environments and multiple timezones ([ab9539a](https://github.com/cube-js/cube.js/commit/ab9539a))
- Parse dates on client side ([#522](https://github.com/cube-js/cube.js/issues/522)) Thanks to [@richipargo](https://github.com/richipargo)! ([11c1106](https://github.com/cube-js/cube.js/commit/11c1106))

## [0.19.8](https://github.com/cube-js/cube.js/compare/v0.19.7...v0.19.8) (2020-04-15)

### Bug Fixes

- Dead queries added to queue in serverless ([eca3d0c](https://github.com/cube-js/cube.js/commit/eca3d0c))

## [0.19.7](https://github.com/cube-js/cube.js/compare/v0.19.6...v0.19.7) (2020-04-14)

### Bug Fixes

- Associate Queue storage error with requestId ([ec2750e](https://github.com/cube-js/cube.js/commit/ec2750e))

### Features

- Including format and type in tableColumns ([#587](https://github.com/cube-js/cube.js/issues/587)) Thanks to [@danpanaite](https://github.com/danpanaite)! ([3f7d74f](https://github.com/cube-js/cube.js/commit/3f7d74f)), closes [#585](https://github.com/cube-js/cube.js/issues/585)

## [0.19.6](https://github.com/cube-js/cube.js/compare/v0.19.5...v0.19.6) (2020-04-14)

### Bug Fixes

- Consistent queryKey logging ([5f1a632](https://github.com/cube-js/cube.js/commit/5f1a632))

## [0.19.5](https://github.com/cube-js/cube.js/compare/v0.19.4...v0.19.5) (2020-04-13)

### Bug Fixes

- Broken query and pre-aggregation cancel ([aa82256](https://github.com/cube-js/cube.js/commit/aa82256))
- Include data transformation in Load Request time ([edf2461](https://github.com/cube-js/cube.js/commit/edf2461))
- RefreshScheduler refreshes pre-aggregations during cache key refresh ([51d1214](https://github.com/cube-js/cube.js/commit/51d1214))

### Features

- Log queue state on Waiting for query ([395c63c](https://github.com/cube-js/cube.js/commit/395c63c))

## [0.19.4](https://github.com/cube-js/cube.js/compare/v0.19.3...v0.19.4) (2020-04-12)

### Bug Fixes

- **serverless-aws:** cubejsProcess agent doesn't collect all events after process has been finished ([939e25a](https://github.com/cube-js/cube.js/commit/939e25a))

## [0.19.3](https://github.com/cube-js/cube.js/compare/v0.19.2...v0.19.3) (2020-04-12)

### Bug Fixes

- Handle invalid lambda process events ([37fc43f](https://github.com/cube-js/cube.js/commit/37fc43f))

## [0.19.2](https://github.com/cube-js/cube.js/compare/v0.19.1...v0.19.2) (2020-04-12)

### Bug Fixes

- Do not DoS agent with huge payloads ([7886130](https://github.com/cube-js/cube.js/commit/7886130))
- TypeError: Cannot read property 'timeDimensions' of null ([7d3329b](https://github.com/cube-js/cube.js/commit/7d3329b))

## [0.19.1](https://github.com/cube-js/cube.js/compare/v0.19.0...v0.19.1) (2020-04-11)

### Bug Fixes

- TypeError: Cannot read property 'dataSource' of null ([5bef81b](https://github.com/cube-js/cube.js/commit/5bef81b))
- TypeError: Cannot read property 'path' of undefined -- Case when partitioned originalSql is resolved for query without time dimension and incremental refreshKey is used ([ca0f1f6](https://github.com/cube-js/cube.js/commit/ca0f1f6))

### Features

- **postgres-driver:** Allow usage of CUBEJS_DB_SSL_CA parameter in postgres Driver. ([#582](https://github.com/cube-js/cube.js/issues/582)) Thanks to [@gauravlanjekar](https://github.com/gauravlanjekar)! ([158bd10](https://github.com/cube-js/cube.js/commit/158bd10))
- Provide status messages for ``/cubejs-api/v1/run-scheduled-refresh` API ([fb6623f](https://github.com/cube-js/cube.js/commit/fb6623f))
- Renamed OpenDistro to AWSElasticSearch. Added `elasticsearch` dialect ([#577](https://github.com/cube-js/cube.js/issues/577)) Thanks to [@chad-codeworkshop](https://github.com/chad-codeworkshop)! ([a4e41cb](https://github.com/cube-js/cube.js/commit/a4e41cb))

# [0.19.0](https://github.com/cube-js/cube.js/compare/v0.18.32...v0.19.0) (2020-04-09)

### Features

- Multi-level query structures in-memory caching ([38aa32d](https://github.com/cube-js/cube.js/commit/38aa32d))

## [0.18.32](https://github.com/cube-js/cube.js/compare/v0.18.31...v0.18.32) (2020-04-07)

### Bug Fixes

- **mysql-driver:** Special characters in database name for readOnly database lead to Error: ER_PARSE_ERROR: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ([1464326](https://github.com/cube-js/cube.js/commit/1464326))

## [0.18.31](https://github.com/cube-js/cube.js/compare/v0.18.30...v0.18.31) (2020-04-07)

### Bug Fixes

- Pass query options such as timezone ([#570](https://github.com/cube-js/cube.js/issues/570)) Thanks to [@jcw](https://github.com/jcw)-! ([089f307](https://github.com/cube-js/cube.js/commit/089f307))
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

## [0.18.27](https://github.com/cube-js/cube.js/compare/v0.18.26...v0.18.27) (2020-04-03)

### Bug Fixes

- TypeError: date.match is not a function at BaseTimeDimension.formatFromDate ([4ac7307](https://github.com/cube-js/cube.js/commit/4ac7307))

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

## [0.18.22](https://github.com/cube-js/cube.js/compare/v0.18.21...v0.18.22) (2020-03-29)

### Features

- **mysql-driver:** Read only pre-aggregations support ([2e7cf58](https://github.com/cube-js/cube.js/commit/2e7cf58))

## [0.18.21](https://github.com/cube-js/cube.js/compare/v0.18.20...v0.18.21) (2020-03-29)

### Bug Fixes

- **mysql-driver:** Remove debug output ([3cd0bf3](https://github.com/cube-js/cube.js/commit/3cd0bf3))

## [0.18.20](https://github.com/cube-js/cube.js/compare/v0.18.19...v0.18.20) (2020-03-29)

### Features

- **mysql-driver:** `loadPreAggregationWithoutMetaLock` option ([a5bae69](https://github.com/cube-js/cube.js/commit/a5bae69))

## [0.18.19](https://github.com/cube-js/cube.js/compare/v0.18.18...v0.18.19) (2020-03-29)

### Bug Fixes

- Empty default `originalSql` refreshKey ([dd8536b](https://github.com/cube-js/cube.js/commit/dd8536b))
- incorrect WHERE for refreshKey every ([bf8b648](https://github.com/cube-js/cube.js/commit/bf8b648))
- Return single table for one partition queries ([54083ef](https://github.com/cube-js/cube.js/commit/54083ef))

### Features

- `propagateFiltersToSubQuery` flag ([6b253c0](https://github.com/cube-js/cube.js/commit/6b253c0))
- Partitioned `originalSql` support ([133857e](https://github.com/cube-js/cube.js/commit/133857e))

## [0.18.18](https://github.com/cube-js/cube.js/compare/v0.18.17...v0.18.18) (2020-03-28)

### Bug Fixes

- **postgres-driver:** Clean-up deprecation warning ([#531](https://github.com/cube-js/cube.js/issues/531)) ([ed1e8da](https://github.com/cube-js/cube.js/commit/ed1e8da))

### Features

- Executing SQL logging message that shows final SQL ([26b8758](https://github.com/cube-js/cube.js/commit/26b8758))

## [0.18.17](https://github.com/cube-js/cube.js/compare/v0.18.16...v0.18.17) (2020-03-24)

### Bug Fixes

- Unknown function NOW for Snowflake -- Incorrect now timestamp implementation ([036f68a](https://github.com/cube-js/cube.js/commit/036f68a)), closes [#537](https://github.com/cube-js/cube.js/issues/537)

### Features

- More places to fetch `readOnly` pre-aggregations flag from ([9877037](https://github.com/cube-js/cube.js/commit/9877037))

## [0.18.16](https://github.com/cube-js/cube.js/compare/v0.18.15...v0.18.16) (2020-03-24)

### Features

- Log canUseTransformedQuery ([5b2ab90](https://github.com/cube-js/cube.js/commit/5b2ab90))

## [0.18.15](https://github.com/cube-js/cube.js/compare/v0.18.14...v0.18.15) (2020-03-24)

### Bug Fixes

- Athena -> MySQL segmentReferences rollup support ([fd3f3d6](https://github.com/cube-js/cube.js/commit/fd3f3d6))

## [0.18.14](https://github.com/cube-js/cube.js/compare/v0.18.13...v0.18.14) (2020-03-24)

### Bug Fixes

- MySQL segment references support ([be42298](https://github.com/cube-js/cube.js/commit/be42298))

### Features

- **postgres-driver:** `CUBEJS_DB_MAX_POOL` env variable ([#528](https://github.com/cube-js/cube.js/issues/528)) Thanks to [@chaselmann](https://github.com/chaselmann)! ([fb0d34b](https://github.com/cube-js/cube.js/commit/fb0d34b))

## [0.18.13](https://github.com/cube-js/cube.js/compare/v0.18.12...v0.18.13) (2020-03-21)

### Bug Fixes

- Overriding of orchestratorOptions results in no usage of process cloud function -- deep merge Handlers options ([c879cb6](https://github.com/cube-js/cube.js/commit/c879cb6)), closes [#519](https://github.com/cube-js/cube.js/issues/519)
- Various cleanup errors ([538f6d0](https://github.com/cube-js/cube.js/commit/538f6d0)), closes [#525](https://github.com/cube-js/cube.js/issues/525)

## [0.18.12](https://github.com/cube-js/cube.js/compare/v0.18.11...v0.18.12) (2020-03-19)

### Bug Fixes

- **types:** Fix index.d.ts errors in cubejs-server. ([#521](https://github.com/cube-js/cube.js/issues/521)) Thanks to jwalton! ([0b01fd6](https://github.com/cube-js/cube.js/commit/0b01fd6))

### Features

- Add duration to error logging ([59a4255](https://github.com/cube-js/cube.js/commit/59a4255))

## [0.18.11](https://github.com/cube-js/cube.js/compare/v0.18.10...v0.18.11) (2020-03-18)

### Bug Fixes

- Orphaned pre-aggregation tables aren't dropped because LocalCacheDriver doesn't expire keys ([393af3d](https://github.com/cube-js/cube.js/commit/393af3d))

## [0.18.10](https://github.com/cube-js/cube.js/compare/v0.18.9...v0.18.10) (2020-03-18)

### Features

- **mysql-driver:** `CUBEJS_DB_MAX_POOL` env variable ([e67e0c7](https://github.com/cube-js/cube.js/commit/e67e0c7))
- **mysql-driver:** Provide a way to define pool options ([2dbf302](https://github.com/cube-js/cube.js/commit/2dbf302))

## [0.18.9](https://github.com/cube-js/cube.js/compare/v0.18.8...v0.18.9) (2020-03-18)

### Bug Fixes

- **mysql-driver:** use utf8mb4 charset for columns to fix ER_TRUNCATED_WRONG_VALUE_FOR_FIELD ([b68a7a8](https://github.com/cube-js/cube.js/commit/b68a7a8))

## [0.18.8](https://github.com/cube-js/cube.js/compare/v0.18.7...v0.18.8) (2020-03-18)

### Bug Fixes

- Publish index.d.ts for @cubejs-backend/server. ([#518](https://github.com/cube-js/cube.js/issues/518)) Thanks to [@jwalton](https://github.com/jwalton)! ([7e9861f](https://github.com/cube-js/cube.js/commit/7e9861f))
- **mysql-driver:** use utf8mb4 charset as default to fix ER_TRUNCATED_WRONG_VALUE_FOR_FIELD for string types ([17e084e](https://github.com/cube-js/cube.js/commit/17e084e))

## [0.18.7](https://github.com/cube-js/cube.js/compare/v0.18.6...v0.18.7) (2020-03-17)

### Bug Fixes

- Error: ER_TRUNCATED_WRONG_VALUE_FOR_FIELD for string types ([c2ee5ee](https://github.com/cube-js/cube.js/commit/c2ee5ee))

### Features

- Log `requestId` in compiling schema events ([4c457c9](https://github.com/cube-js/cube.js/commit/4c457c9))

## [0.18.6](https://github.com/cube-js/cube.js/compare/v0.18.5...v0.18.6) (2020-03-16)

### Bug Fixes

- Waiting for query isn't logged for Local Queue when query is already in progress ([e7be6d1](https://github.com/cube-js/cube.js/commit/e7be6d1))

## [0.18.5](https://github.com/cube-js/cube.js/compare/v0.18.4...v0.18.5) (2020-03-15)

### Bug Fixes

- **@cubejs-client/core:** make `progressCallback` optional ([#497](https://github.com/cube-js/cube.js/issues/497)) Thanks to [@hassankhan](https://github.com/hassankhan)! ([a41cf9a](https://github.com/cube-js/cube.js/commit/a41cf9a))
- `requestId` isn't propagating to all pre-aggregations messages ([650dd6e](https://github.com/cube-js/cube.js/commit/650dd6e))

## [0.18.4](https://github.com/cube-js/cube.js/compare/v0.18.3...v0.18.4) (2020-03-09)

### Bug Fixes

- Request span for WebSocketTransport is incorrectly set ([54ba5da](https://github.com/cube-js/cube.js/commit/54ba5da))
- results not converted to timezone unless granularity is set: value fails to match the required pattern ([715ba71](https://github.com/cube-js/cube.js/commit/715ba71)), closes [#443](https://github.com/cube-js/cube.js/issues/443)

### Features

- Add API gateway request logging support ([#475](https://github.com/cube-js/cube.js/issues/475)) ([465471e](https://github.com/cube-js/cube.js/commit/465471e))
- Use options pattern in constructor ([#468](https://github.com/cube-js/cube.js/issues/468)) Thanks to [@jcw](https://github.com/jcw)-! ([ff20167](https://github.com/cube-js/cube.js/commit/ff20167))

## [0.18.3](https://github.com/cube-js/cube.js/compare/v0.18.2...v0.18.3) (2020-03-02)

### Bug Fixes

- antd 4 support for dashboard ([84bb164](https://github.com/cube-js/cube.js/commit/84bb164)), closes [#463](https://github.com/cube-js/cube.js/issues/463)
- CUBEJS_REDIS_POOL_MAX=0 env variable setting isn't respected ([75f6889](https://github.com/cube-js/cube.js/commit/75f6889))
- Duration string is not printed for all messages -- Load Request SQL case ([e0d3aff](https://github.com/cube-js/cube.js/commit/e0d3aff))

## [0.18.2](https://github.com/cube-js/cube.js/compare/v0.18.1...v0.18.2) (2020-03-01)

### Bug Fixes

- Limit pre-aggregations fetch table requests using queue -- handle HA for pre-aggregations ([75833b1](https://github.com/cube-js/cube.js/commit/75833b1))

## [0.18.1](https://github.com/cube-js/cube.js/compare/v0.18.0...v0.18.1) (2020-03-01)

### Bug Fixes

- Remove user facing errors for pre-aggregations refreshes ([d15c551](https://github.com/cube-js/cube.js/commit/d15c551))

# [0.18.0](https://github.com/cube-js/cube.js/compare/v0.17.10...v0.18.0) (2020-03-01)

### Bug Fixes

- Error: client.readOnly is not a function ([6069499](https://github.com/cube-js/cube.js/commit/6069499))
- External rollup type conversions: cast double to decimal for postgres ([#421](https://github.com/cube-js/cube.js/issues/421)) Thanks to [@sandeepravi](https://github.com/sandeepravi)! ([a19410a](https://github.com/cube-js/cube.js/commit/a19410a))
- **athena-driver:** Remove debug output ([f538135](https://github.com/cube-js/cube.js/commit/f538135))
- Handle missing body-parser error ([b90dd89](https://github.com/cube-js/cube.js/commit/b90dd89))
- Handle multiple occurrences in the first event of a funnel: conversion percent discrepancies. ([0989482](https://github.com/cube-js/cube.js/commit/0989482))
- Handle primaryKey shown: false pitfall error ([5bbf5f0](https://github.com/cube-js/cube.js/commit/5bbf5f0))
- Redis query queue locking redesign ([a2eb9b2](https://github.com/cube-js/cube.js/commit/a2eb9b2)), closes [#459](https://github.com/cube-js/cube.js/issues/459)
- TypeError: Cannot read property 'queryKey' of null under load ([0c996d8](https://github.com/cube-js/cube.js/commit/0c996d8))

### Features

- Add role parameter to driver options ([#448](https://github.com/cube-js/cube.js/issues/448)) Thanks to [@smbkr](https://github.com/smbkr)! ([9bfb71d](https://github.com/cube-js/cube.js/commit/9bfb71d)), closes [#447](https://github.com/cube-js/cube.js/issues/447)
- COMPILE_CONTEXT and async driverFactory support ([160f931](https://github.com/cube-js/cube.js/commit/160f931))
- Redis connection pooling ([#433](https://github.com/cube-js/cube.js/issues/433)) Thanks to [@jcw](https://github.com/jcw)! ([cf133a9](https://github.com/cube-js/cube.js/commit/cf133a9)), closes [#104](https://github.com/cube-js/cube.js/issues/104)

## [0.17.10](https://github.com/cube-js/cube.js/compare/v0.17.9...v0.17.10) (2020-02-20)

### Bug Fixes

- Revert "feat: Bump corejs ([#378](https://github.com/cube-js/cube.js/issues/378))" ([b21cbe6](https://github.com/cube-js/cube.js/commit/b21cbe6)), closes [#418](https://github.com/cube-js/cube.js/issues/418)
- uuidv4 upgrade ([c46c721](https://github.com/cube-js/cube.js/commit/c46c721))

### Features

- **cubejs-cli:** Add node_modules to .gitignore ([207544b](https://github.com/cube-js/cube.js/commit/207544b))
- Support external rollups from readonly source ([#395](https://github.com/cube-js/cube.js/issues/395)) ([b17e841](https://github.com/cube-js/cube.js/commit/b17e841))

## [0.17.9](https://github.com/cube-js/cube.js/compare/v0.17.8...v0.17.9) (2020-02-18)

### Features

- Add .gitignore with .env content to templates.js ([#403](https://github.com/cube-js/cube.js/issues/403)) ([c0d1a76](https://github.com/cube-js/cube.js/commit/c0d1a76)), closes [#402](https://github.com/cube-js/cube.js/issues/402)
- Bump corejs ([#378](https://github.com/cube-js/cube.js/issues/378)) ([cb8d51c](https://github.com/cube-js/cube.js/commit/cb8d51c))
- Enhanced trace logging ([1fdd8e9](https://github.com/cube-js/cube.js/commit/1fdd8e9))
- Extend meta response with aggregation type ([#394](https://github.com/cube-js/cube.js/issues/394)) Thanks to [@pyrooka](https://github.com/pyrooka)! ([06eed0b](https://github.com/cube-js/cube.js/commit/06eed0b))
- Request id trace span ([880f65e](https://github.com/cube-js/cube.js/commit/880f65e))

## [0.17.8](https://github.com/cube-js/cube.js/compare/v0.17.7...v0.17.8) (2020-02-14)

### Bug Fixes

- typings export ([#373](https://github.com/cube-js/cube.js/issues/373)) Thanks to [@lvauvillier](https://github.com/lvauvillier)! ([f4ea839](https://github.com/cube-js/cube.js/commit/f4ea839))
- Wrong interval functions for BigQuery ([#367](https://github.com/cube-js/cube.js/issues/367)) Thanks to [@lvauvillier](https://github.com/lvauvillier)! ([0e09d4d](https://github.com/cube-js/cube.js/commit/0e09d4d))
- **@cubejs-backend/oracle-driver:** a pre-built node-oracledb binary was not found for Node.js v12.16.0 ([#375](https://github.com/cube-js/cube.js/issues/375)) ([fd66bb6](https://github.com/cube-js/cube.js/commit/fd66bb6)), closes [#370](https://github.com/cube-js/cube.js/issues/370)
- **@cubejs-client/core:** improve types ([#376](https://github.com/cube-js/cube.js/issues/376)) Thanks to [@hassankhan](https://github.com/hassankhan)! ([cfb65a2](https://github.com/cube-js/cube.js/commit/cfb65a2))

### Features

- Athena HLL support ([45c7b83](https://github.com/cube-js/cube.js/commit/45c7b83))

## [0.17.7](https://github.com/cube-js/cube.js/compare/v0.17.6...v0.17.7) (2020-02-12)

### Bug Fixes

- Invalid Date: Incorrect MySQL minutes granularity ([dc553b9](https://github.com/cube-js/cube.js/commit/dc553b9))
- Respect MySQL TIMESTAMP strict mode on rollup downloads ([c72ab07](https://github.com/cube-js/cube.js/commit/c72ab07))
- Wrong typings ([c32fb0e](https://github.com/cube-js/cube.js/commit/c32fb0e))

### Features

- add bigquery-driver typings ([0c5e0f7](https://github.com/cube-js/cube.js/commit/0c5e0f7))
- add postgres-driver typings ([364d9bf](https://github.com/cube-js/cube.js/commit/364d9bf))
- add sqlite-driver typings ([4446eba](https://github.com/cube-js/cube.js/commit/4446eba))
- Cube.js agent ([35366aa](https://github.com/cube-js/cube.js/commit/35366aa))
- improve server-core typings ([9d59300](https://github.com/cube-js/cube.js/commit/9d59300))
- Set warn to be default log level for production logging ([c4298ea](https://github.com/cube-js/cube.js/commit/c4298ea))

## [0.17.6](https://github.com/cube-js/cube.js/compare/v0.17.5...v0.17.6) (2020-02-10)

### Bug Fixes

- `sqlAlias` isn't used for pre-aggregation table names ([b757175](https://github.com/cube-js/cube.js/commit/b757175))
- Multiplied measures rollup select case and leaf measure additive exact match ([c897dec](https://github.com/cube-js/cube.js/commit/c897dec))

## [0.17.5](https://github.com/cube-js/cube.js/compare/v0.17.4...v0.17.5) (2020-02-07)

### Bug Fixes

- Sanity check for silent truncate name problem during pre-aggregation creation ([e7fb2f2](https://github.com/cube-js/cube.js/commit/e7fb2f2))

## [0.17.4](https://github.com/cube-js/cube.js/compare/v0.17.3...v0.17.4) (2020-02-06)

### Bug Fixes

- Don't fetch schema twice when generating in Playground. Big schemas take a lot of time to fetch. ([3eeb73a](https://github.com/cube-js/cube.js/commit/3eeb73a))

## [0.17.3](https://github.com/cube-js/cube.js/compare/v0.17.2...v0.17.3) (2020-02-06)

### Bug Fixes

- Fix typescript type definition ([66e2fe5](https://github.com/cube-js/cube.js/commit/66e2fe5))

### Features

- Pre-aggregation indexes support ([d443585](https://github.com/cube-js/cube.js/commit/d443585))

## [0.17.2](https://github.com/cube-js/cube.js/compare/v0.17.1...v0.17.2) (2020-02-04)

### Bug Fixes

- Funnel step names cannot contain spaces ([aff1891](https://github.com/cube-js/cube.js/commit/aff1891)), closes [#359](https://github.com/cube-js/cube.js/issues/359)

## [0.17.1](https://github.com/cube-js/cube.js/compare/v0.17.0...v0.17.1) (2020-02-04)

### Bug Fixes

- TypeError: Cannot read property 'map' of undefined ([a12610d](https://github.com/cube-js/cube.js/commit/a12610d))

# [0.17.0](https://github.com/cube-js/cube.js/compare/v0.16.0...v0.17.0) (2020-02-04)

**Note:** Version bump only for package cubejs

# [0.16.0](https://github.com/cube-js/cube.js/compare/v0.15.4...v0.16.0) (2020-02-04)

### Bug Fixes

- Do not pad `last 24 hours` interval to day ([6554611](https://github.com/cube-js/cube.js/commit/6554611)), closes [#361](https://github.com/cube-js/cube.js/issues/361)

### Features

- Allow `null` filter values ([9e339f7](https://github.com/cube-js/cube.js/commit/9e339f7)), closes [#362](https://github.com/cube-js/cube.js/issues/362)

## [0.15.4](https://github.com/cube-js/cube.js/compare/v0.15.3...v0.15.4) (2020-02-02)

### Features

- Return `shortTitle` in `tableColumns()` result ([810c812](https://github.com/cube-js/cube.js/commit/810c812))

## [0.15.3](https://github.com/cube-js/cube.js/compare/v0.15.2...v0.15.3) (2020-01-26)

### Bug Fixes

- TypeError: Cannot read property 'title' of undefined ([3f76066](https://github.com/cube-js/cube.js/commit/3f76066))

## [0.15.2](https://github.com/cube-js/cube.js/compare/v0.15.1...v0.15.2) (2020-01-25)

### Bug Fixes

- **@cubejs-client/core:** improve types ([55edf85](https://github.com/cube-js/cube.js/commit/55edf85)), closes [#350](https://github.com/cube-js/cube.js/issues/350)
- Time dimension ResultSet backward compatibility to allow work newer client with old server ([b6834b1](https://github.com/cube-js/cube.js/commit/b6834b1)), closes [#356](https://github.com/cube-js/cube.js/issues/356)

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
- Skip contents for huge queries in dev logs ([c873a83](https://github.com/cube-js/cube.js/commit/c873a83))

## [0.14.2](https://github.com/cube-js/cube.js/compare/v0.14.1...v0.14.2) (2020-01-17)

### Bug Fixes

- TypeError: Cannot read property 'evaluateSymbolSqlWithContext' of undefined ([125afd7](https://github.com/cube-js/cube.js/commit/125afd7))

## [0.14.1](https://github.com/cube-js/cube.js/compare/v0.14.0...v0.14.1) (2020-01-17)

### Features

- Default refreshKey implementations for mutable and immutable pre-aggregations. ([bef0626](https://github.com/cube-js/cube.js/commit/bef0626))

# [0.14.0](https://github.com/cube-js/cube.js/compare/v0.13.12...v0.14.0) (2020-01-16)

### Bug Fixes

- Cannot read property 'requestId' of null ([d087837](https://github.com/cube-js/cube.js/commit/d087837)), closes [#347](https://github.com/cube-js/cube.js/issues/347)
- dateRange gets translated to incorrect value ([71d07e6](https://github.com/cube-js/cube.js/commit/71d07e6)), closes [#348](https://github.com/cube-js/cube.js/issues/348)
- Time dimension can't be selected twice within same query with and without granularity ([aa65129](https://github.com/cube-js/cube.js/commit/aa65129))

### Features

- Scheduled refresh for pre-aggregations ([c87b525](https://github.com/cube-js/cube.js/commit/c87b525))
- Scheduled Refresh REST API ([472a0c3](https://github.com/cube-js/cube.js/commit/472a0c3))

## [0.13.12](https://github.com/cube-js/cube.js/compare/v0.13.11...v0.13.12) (2020-01-12)

**Note:** Version bump only for package cubejs

## [0.13.11](https://github.com/cube-js/cube.js/compare/v0.13.10...v0.13.11) (2020-01-03)

### Bug Fixes

- Can't parse /node_modules/.bin/sha.js during dashboard creation ([e13ad50](https://github.com/cube-js/cube.js/commit/e13ad50))

## [0.13.10](https://github.com/cube-js/cube.js/compare/v0.13.9...v0.13.10) (2020-01-03)

### Bug Fixes

- More details for parsing errors during dashboard creation ([a8cb9d3](https://github.com/cube-js/cube.js/commit/a8cb9d3))

## [0.13.9](https://github.com/cube-js/cube.js/compare/v0.13.8...v0.13.9) (2020-01-03)

### Bug Fixes

- define context outside try-catch ([3075624](https://github.com/cube-js/cube.js/commit/3075624))

### Features

- **@cubejs-client/core:** add types ([abdf089](https://github.com/cube-js/cube.js/commit/abdf089))
- Improve logging ([8a692c1](https://github.com/cube-js/cube.js/commit/8a692c1))
- **mysql-driver:** Increase external pre-aggregations upload batch size ([741e26c](https://github.com/cube-js/cube.js/commit/741e26c))

## [0.13.8](https://github.com/cube-js/cube.js/compare/v0.13.7...v0.13.8) (2019-12-31)

### Bug Fixes

- UnhandledPromiseRejectionWarning: TypeError: Converting circular structure to JSON ([44c5065](https://github.com/cube-js/cube.js/commit/44c5065))

## [0.13.7](https://github.com/cube-js/cube.js/compare/v0.13.6...v0.13.7) (2019-12-31)

### Bug Fixes

- ER_TRUNCATED_WRONG_VALUE: Truncated incorrect datetime value ([fcbbe84](https://github.com/cube-js/cube.js/commit/fcbbe84)), closes [#309](https://github.com/cube-js/cube.js/issues/309)
- schemaVersion called with old context ([#293](https://github.com/cube-js/cube.js/issues/293)) ([da10e39](https://github.com/cube-js/cube.js/commit/da10e39)), closes [#294](https://github.com/cube-js/cube.js/issues/294)
- **client-core:** Uncaught TypeError: cubejs is not a function ([b5c32cd](https://github.com/cube-js/cube.js/commit/b5c32cd))

### Features

- Extendable context ([#299](https://github.com/cube-js/cube.js/issues/299)) ([38e33ce](https://github.com/cube-js/cube.js/commit/38e33ce)), closes [#295](https://github.com/cube-js/cube.js/issues/295) [#296](https://github.com/cube-js/cube.js/issues/296)
- Health check methods ([#308](https://github.com/cube-js/cube.js/issues/308)) Thanks to [@willhausman](https://github.com/willhausman)! ([49ca36b](https://github.com/cube-js/cube.js/commit/49ca36b))

## [0.13.6](https://github.com/cube-js/cube.js/compare/v0.13.5...v0.13.6) (2019-12-19)

### Bug Fixes

- Date parser returns 31 days for `last 30 days` date range ([bedbe9c](https://github.com/cube-js/cube.js/commit/bedbe9c)), closes [#303](https://github.com/cube-js/cube.js/issues/303)
- **elasticsearch-driver:** TypeError: Cannot convert undefined or null to object ([2dc570f](https://github.com/cube-js/cube.js/commit/2dc570f))

## [0.13.5](https://github.com/cube-js/cube.js/compare/v0.13.4...v0.13.5) (2019-12-17)

### Features

- Elasticsearch driver preview ([d6a6a07](https://github.com/cube-js/cube.js/commit/d6a6a07))
- Return key in the resultSet.series alongside title ([#291](https://github.com/cube-js/cube.js/issues/291)) ([6144a86](https://github.com/cube-js/cube.js/commit/6144a86))

## [0.13.4](https://github.com/cube-js/cube.js/compare/v0.13.3...v0.13.4) (2019-12-16)

**Note:** Version bump only for package cubejs

## [0.13.3](https://github.com/cube-js/cube.js/compare/v0.13.2...v0.13.3) (2019-12-16)

### Bug Fixes

- **sqlite-driver:** Fixed table schema parsing: support for escape characters ([#289](https://github.com/cube-js/cube.js/issues/289)). Thanks to [@philippefutureboy](https://github.com/philippefutureboy)! ([42026fb](https://github.com/cube-js/cube.js/commit/42026fb))
- Logging failing when pre-aggregations are built ([22f77a6](https://github.com/cube-js/cube.js/commit/22f77a6))

### Features

- d3-charts template package ([f9bd3fb](https://github.com/cube-js/cube.js/commit/f9bd3fb))
- **sqlite-driver:** Pre-aggregations support ([5ffb3d2](https://github.com/cube-js/cube.js/commit/5ffb3d2))

## [0.13.2](https://github.com/cube-js/cube.js/compare/v0.13.1...v0.13.2) (2019-12-13)

### Features

- Error type for returning specific http status codes ([#288](https://github.com/cube-js/cube.js/issues/288)). Thanks to [@willhausman](https://github.com/willhausman)! ([969e609](https://github.com/cube-js/cube.js/commit/969e609))
- hooks for dynamic schemas ([#287](https://github.com/cube-js/cube.js/issues/287)). Thanks to [@willhausman](https://github.com/willhausman)! ([47b256d](https://github.com/cube-js/cube.js/commit/47b256d))
- Propagate `requestId` for trace logging ([24d7b41](https://github.com/cube-js/cube.js/commit/24d7b41))

## [0.13.1](https://github.com/cube-js/cube.js/compare/v0.13.0...v0.13.1) (2019-12-10)

### Bug Fixes

- **api-gateway:** getTime on undefined call in case of web socket auth error ([9807b1e](https://github.com/cube-js/cube.js/commit/9807b1e))

# [0.13.0](https://github.com/cube-js/cube.js/compare/v0.12.3...v0.13.0) (2019-12-10)

### Bug Fixes

- cube validation from updating BasePreAggregation ([#285](https://github.com/cube-js/cube.js/issues/285)). Thanks to [@ferrants](https://github.com/ferrants)! ([f4bda4e](https://github.com/cube-js/cube.js/commit/f4bda4e))
- Errors during web socket subscribe returned with status 200 code ([6df008e](https://github.com/cube-js/cube.js/commit/6df008e))

### Features

- Minute and second granularities support ([34c5d4c](https://github.com/cube-js/cube.js/commit/34c5d4c))
- Sqlite driver implementation ([f9b43d3](https://github.com/cube-js/cube.js/commit/f9b43d3))

## [0.12.3](https://github.com/cube-js/cube.js/compare/v0.12.2...v0.12.3) (2019-12-02)

**Note:** Version bump only for package cubejs

## [0.12.2](https://github.com/cube-js/cube.js/compare/v0.12.1...v0.12.2) (2019-12-02)

### Bug Fixes

- this.versionEntries typo ([#279](https://github.com/cube-js/cube.js/issues/279)) ([743f9fb](https://github.com/cube-js/cube.js/commit/743f9fb))
- **cli:** update list of supported db based on document ([#281](https://github.com/cube-js/cube.js/issues/281)). Thanks to [@lanphan](https://github.com/lanphan)! ([8aa5a2e](https://github.com/cube-js/cube.js/commit/8aa5a2e))

### Features

- support REDIS_PASSWORD env variable ([#280](https://github.com/cube-js/cube.js/issues/280)). Thanks to [@lanphan](https://github.com/lanphan)! ([5172745](https://github.com/cube-js/cube.js/commit/5172745))

## [0.12.1](https://github.com/cube-js/cube.js/compare/v0.12.0...v0.12.1) (2019-11-26)

### Features

- Show used pre-aggregations and match rollup results in Playground ([4a67346](https://github.com/cube-js/cube.js/commit/4a67346))

# [0.12.0](https://github.com/cube-js/cube.js/compare/v0.11.25...v0.12.0) (2019-11-25)

### Features

- Show `refreshKey` values in Playground ([b49e184](https://github.com/cube-js/cube.js/commit/b49e184))

## [0.11.25](https://github.com/cube-js/cube.js/compare/v0.11.24...v0.11.25) (2019-11-23)

### Bug Fixes

- **playground:** Multiple conflicting packages applied at the same time: check for creation state before applying ([35f6325](https://github.com/cube-js/cube.js/commit/35f6325))

### Features

- playground receipes - update copy and previews ([b11a8c3](https://github.com/cube-js/cube.js/commit/b11a8c3))

## [0.11.24](https://github.com/cube-js/cube.js/compare/v0.11.23...v0.11.24) (2019-11-20)

### Bug Fixes

- Material UI template doesn't work ([deccca1](https://github.com/cube-js/cube.js/commit/deccca1))

## [0.11.23](https://github.com/cube-js/cube.js/compare/v0.11.22...v0.11.23) (2019-11-20)

### Features

- Enable web sockets by default in Express template ([815fb2c](https://github.com/cube-js/cube.js/commit/815fb2c))

## [0.11.22](https://github.com/cube-js/cube.js/compare/v0.11.21...v0.11.22) (2019-11-20)

### Bug Fixes

- Error: Router element is not found: Template Gallery source enumeration returns empty array ([459a4a7](https://github.com/cube-js/cube.js/commit/459a4a7))

## [0.11.21](https://github.com/cube-js/cube.js/compare/v0.11.20...v0.11.21) (2019-11-20)

### Features

- **schema-compiler:** Upgrade babel and support `objectRestSpread` for schema generation ([ac97c44](https://github.com/cube-js/cube.js/commit/ac97c44))
- Template gallery ([#272](https://github.com/cube-js/cube.js/issues/272)) ([f5ac516](https://github.com/cube-js/cube.js/commit/f5ac516))

## [0.11.20](https://github.com/cube-js/cube.js/compare/v0.11.19...v0.11.20) (2019-11-18)

### Bug Fixes

- Fix postgres driver timestamp parsing by using pg per-query type parser ([#269](https://github.com/cube-js/cube.js/issues/269)) Thanks to [@berndartmueller](https://github.com/berndartmueller)! ([458c0c9](https://github.com/cube-js/cube.js/commit/458c0c9)), closes [#265](https://github.com/cube-js/cube.js/issues/265)

### Features

- support for pre-aggregation time hierarchies ([#258](https://github.com/cube-js/cube.js/issues/258)) Thanks to @Justin-ZS! ([ea78c84](https://github.com/cube-js/cube.js/commit/ea78c84)), closes [#246](https://github.com/cube-js/cube.js/issues/246)
- per cube `dataSource` support ([6dc3fef](https://github.com/cube-js/cube.js/commit/6dc3fef))

## [0.11.19](https://github.com/cube-js/cube.js/compare/v0.11.18...v0.11.19) (2019-11-16)

### Bug Fixes

- Merge back `sqlAlias` support ([80b312f](https://github.com/cube-js/cube.js/commit/80b312f))

## [0.11.18](https://github.com/cube-js/cube.js/compare/v0.11.17...v0.11.18) (2019-11-09)

**Note:** Version bump only for package cubejs

## [0.11.17](https://github.com/cube-js/cube.js/compare/v0.11.16...v0.11.17) (2019-11-08)

### Bug Fixes

- **server-core:** the schemaPath option does not work when generating schema ([#255](https://github.com/cube-js/cube.js/issues/255)) ([92f17b2](https://github.com/cube-js/cube.js/commit/92f17b2))
- Default Express middleware security check is ignored in production ([4bdf6bd](https://github.com/cube-js/cube.js/commit/4bdf6bd))

### Features

- Default root path message for servers running in production ([5b7ef41](https://github.com/cube-js/cube.js/commit/5b7ef41))

## [0.11.16](https://github.com/cube-js/cube.js/compare/v0.11.15...v0.11.16) (2019-11-04)

### Bug Fixes

- **vue:** Error: Invalid query format: "order" is not allowed ([e6a738a](https://github.com/cube-js/cube.js/commit/e6a738a))
- Respect timezone for natural language date parsing and align custom date ranges to dates by default to ensure backward compatibility ([af6f3c2](https://github.com/cube-js/cube.js/commit/af6f3c2))
- Respect timezone for natural language date parsing and align custom date ranges to dates by default to ensure backward compatibility ([2104492](https://github.com/cube-js/cube.js/commit/2104492))
- Use `node index.js` for `npm run dev` where available to ensure it starts servers with changed code ([527e274](https://github.com/cube-js/cube.js/commit/527e274))

## [0.11.15](https://github.com/cube-js/cube.js/compare/v0.11.14...v0.11.15) (2019-11-01)

### Bug Fixes

- Reduce output for logging ([aaf55e0](https://github.com/cube-js/cube.js/commit/aaf55e0))

## [0.11.14](https://github.com/cube-js/cube.js/compare/v0.11.13...v0.11.14) (2019-11-01)

### Bug Fixes

- Catch unhandled rejections on server starts ([fd9d872](https://github.com/cube-js/cube.js/commit/fd9d872))

### Features

- pretty default logger and log levels ([#244](https://github.com/cube-js/cube.js/issues/244)) ([b1302d2](https://github.com/cube-js/cube.js/commit/b1302d2))

## [0.11.13](https://github.com/cube-js/cube.js/compare/v0.11.12...v0.11.13) (2019-10-30)

### Features

- **playground:** Static dashboard template ([2458aad](https://github.com/cube-js/cube.js/commit/2458aad))

## [0.11.12](https://github.com/cube-js/cube.js/compare/v0.11.11...v0.11.12) (2019-10-29)

### Bug Fixes

- Playground shouldn't be run in serverless environment by default ([41cd46c](https://github.com/cube-js/cube.js/commit/41cd46c))
- **react:** Refetch hook only actual query changes ([10b8988](https://github.com/cube-js/cube.js/commit/10b8988))

## [0.11.11](https://github.com/cube-js/cube.js/compare/v0.11.10...v0.11.11) (2019-10-26)

### Bug Fixes

- **postgres-driver:** `CUBEJS_DB_SSL=false` should disable SSL ([85064bc](https://github.com/cube-js/cube.js/commit/85064bc))

## [0.11.10](https://github.com/cube-js/cube.js/compare/v0.11.9...v0.11.10) (2019-10-25)

### Features

- client headers for CubejsApi ([#242](https://github.com/cube-js/cube.js/issues/242)). Thanks to [@ferrants](https://github.com/ferrants)! ([2f75ef3](https://github.com/cube-js/cube.js/commit/2f75ef3)), closes [#241](https://github.com/cube-js/cube.js/issues/241)

## [0.11.9](https://github.com/cube-js/cube.js/compare/v0.11.8...v0.11.9) (2019-10-23)

### Bug Fixes

- Support `apiToken` to be an async function: first request sends incorrect token ([a2d0c77](https://github.com/cube-js/cube.js/commit/a2d0c77))

## [0.11.8](https://github.com/cube-js/cube.js/compare/v0.11.7...v0.11.8) (2019-10-22)

### Bug Fixes

- Pass `checkAuth` option to API Gateway ([d3d690e](https://github.com/cube-js/cube.js/commit/d3d690e))

## [0.11.7](https://github.com/cube-js/cube.js/compare/v0.11.6...v0.11.7) (2019-10-22)

### Features

- dynamic case label ([#236](https://github.com/cube-js/cube.js/issues/236)) ([1a82605](https://github.com/cube-js/cube.js/commit/1a82605)), closes [#235](https://github.com/cube-js/cube.js/issues/235)
- Support `apiToken` to be an async function ([3a3b5f5](https://github.com/cube-js/cube.js/commit/3a3b5f5))

## [0.11.6](https://github.com/cube-js/cube.js/compare/v0.11.5...v0.11.6) (2019-10-17)

### Bug Fixes

- Postgres driver with redis in non UTC timezone returns timezone shifted results ([f1346da](https://github.com/cube-js/cube.js/commit/f1346da))
- TypeError: Cannot read property 'table_name' of undefined: Drop orphaned tables implementation drops recent tables in cluster environments ([84ea78a](https://github.com/cube-js/cube.js/commit/84ea78a))
- Yesterday date range doesn't work ([6c81a02](https://github.com/cube-js/cube.js/commit/6c81a02))

## [0.11.5](https://github.com/cube-js/cube.js/compare/v0.11.4...v0.11.5) (2019-10-17)

### Bug Fixes

- **api-gateway:** TypeError: res.json is not a function ([7f3f0a8](https://github.com/cube-js/cube.js/commit/7f3f0a8))

## [0.11.4](https://github.com/cube-js/cube.js/compare/v0.11.3...v0.11.4) (2019-10-16)

### Bug Fixes

- Remove legacy scaffolding comments ([123a929](https://github.com/cube-js/cube.js/commit/123a929))
- TLS redirect is failing if cube.js listening on port other than 80 ([0fe92ec](https://github.com/cube-js/cube.js/commit/0fe92ec))

## [0.11.3](https://github.com/cube-js/cube.js/compare/v0.11.2...v0.11.3) (2019-10-15)

### Bug Fixes

- `useCubeQuery` doesn't reset error and resultSet on query change ([805d5b1](https://github.com/cube-js/cube.js/commit/805d5b1))

## [0.11.2](https://github.com/cube-js/cube.js/compare/v0.11.1...v0.11.2) (2019-10-15)

### Bug Fixes

- Error: ENOENT: no such file or directory, open 'Orders.js' ([74a8875](https://github.com/cube-js/cube.js/commit/74a8875))
- Incorrect URL generation in HttpTransport ([7e7020b](https://github.com/cube-js/cube.js/commit/7e7020b))

## [0.11.1](https://github.com/cube-js/cube.js/compare/v0.11.0...v0.11.1) (2019-10-15)

### Bug Fixes

- Error: Cannot find module './WebSocketServer' ([df3b074](https://github.com/cube-js/cube.js/commit/df3b074))

# [0.11.0](https://github.com/cube-js/cube.js/compare/v0.10.62...v0.11.0) (2019-10-15)

### Bug Fixes

- TypeError: Cannot destructure property authInfo of 'undefined' or 'null'. ([1886d13](https://github.com/cube-js/cube.js/commit/1886d13))

### Features

- Read schema subfolders ([#230](https://github.com/cube-js/cube.js/issues/230)). Thanks to [@lksilva](https://github.com/lksilva)! ([aa736b1](https://github.com/cube-js/cube.js/commit/aa736b1))
- Sockets Preview ([#231](https://github.com/cube-js/cube.js/issues/231)) ([89fc762](https://github.com/cube-js/cube.js/commit/89fc762)), closes [#221](https://github.com/cube-js/cube.js/issues/221)

## [0.10.62](https://github.com/cube-js/cube.js/compare/v0.10.61...v0.10.62) (2019-10-11)

### Features

- **vue:** Add order, renewQuery, and reactivity to Vue component ([#229](https://github.com/cube-js/cube.js/issues/229)). Thanks to @TCBroad ([9293f13](https://github.com/cube-js/cube.js/commit/9293f13))
- `ungrouped` queries support ([c6ac873](https://github.com/cube-js/cube.js/commit/c6ac873))

## [0.10.61](https://github.com/cube-js/cube.js/compare/v0.10.60...v0.10.61) (2019-10-10)

### Bug Fixes

- Override incorrect button color in playground ([6b7d964](https://github.com/cube-js/cube.js/commit/6b7d964))
- playground scaffolding include antd styles via index.css ([881084e](https://github.com/cube-js/cube.js/commit/881084e))
- **playground:** Chart type doesn't switch in Dashboard App ([23f604f](https://github.com/cube-js/cube.js/commit/23f604f))

### Features

- Scaffolding Updates React ([#228](https://github.com/cube-js/cube.js/issues/228)) ([552fd9c](https://github.com/cube-js/cube.js/commit/552fd9c))
- **react:** Introduce `useCubeQuery` react hook and `CubeProvider` cubejsApi context provider ([19b6fac](https://github.com/cube-js/cube.js/commit/19b6fac))
- **schema-compiler:** Allow access raw data in `USER_CONTEXT` using `unsafeValue()` method ([52ef146](https://github.com/cube-js/cube.js/commit/52ef146))

## [0.10.60](https://github.com/cube-js/cube.js/compare/v0.10.59...v0.10.60) (2019-10-08)

### Bug Fixes

- **client-ngx:** Support Observables for config: runtime token change case ([0e30773](https://github.com/cube-js/cube.js/commit/0e30773))

## [0.10.59](https://github.com/cube-js/cube.js/compare/v0.10.58...v0.10.59) (2019-10-08)

### Bug Fixes

- hostname: command not found ([8ca1f21](https://github.com/cube-js/cube.js/commit/8ca1f21))
- Rolling window returns dates in incorrect time zone for Postgres ([71a3baa](https://github.com/cube-js/cube.js/commit/71a3baa)), closes [#216](https://github.com/cube-js/cube.js/issues/216)

## [0.10.58](https://github.com/cube-js/cube.js/compare/v0.10.57...v0.10.58) (2019-10-04)

### Bug Fixes

- **playground:** Fix recharts height ([cd75409](https://github.com/cube-js/cube.js/commit/cd75409))
- `continueWaitTimout` option is ignored in LocalQueueDriver ([#224](https://github.com/cube-js/cube.js/issues/224)) ([4f72a52](https://github.com/cube-js/cube.js/commit/4f72a52))

## [0.10.57](https://github.com/cube-js/cube.js/compare/v0.10.56...v0.10.57) (2019-10-04)

### Bug Fixes

- **react:** Evade unnecessary heavy chart renders ([b1eb63f](https://github.com/cube-js/cube.js/commit/b1eb63f))

## [0.10.56](https://github.com/cube-js/cube.js/compare/v0.10.55...v0.10.56) (2019-10-04)

### Bug Fixes

- **react:** Evade unnecessary heavy chart renders ([bdcc569](https://github.com/cube-js/cube.js/commit/bdcc569))

## [0.10.55](https://github.com/cube-js/cube.js/compare/v0.10.54...v0.10.55) (2019-10-03)

### Bug Fixes

- **client-core:** can't read property 'title' of undefined ([4b48c7f](https://github.com/cube-js/cube.js/commit/4b48c7f))
- **playground:** Dashboard item name edit performance issues ([73df3c7](https://github.com/cube-js/cube.js/commit/73df3c7))
- **playground:** PropTypes validations ([3d5faa1](https://github.com/cube-js/cube.js/commit/3d5faa1))
- **playground:** Recharts fixes ([bce0313](https://github.com/cube-js/cube.js/commit/bce0313))

## [0.10.54](https://github.com/cube-js/cube.js/compare/v0.10.53...v0.10.54) (2019-10-02)

**Note:** Version bump only for package cubejs

## [0.10.53](https://github.com/cube-js/cube.js/compare/v0.10.52...v0.10.53) (2019-10-02)

### Bug Fixes

- **playground:** antd styles are added as part of table scaffolding ([8a39c9d](https://github.com/cube-js/cube.js/commit/8a39c9d))
- **playground:** Can't delete dashboard item name in dashboard app ([0cf546f](https://github.com/cube-js/cube.js/commit/0cf546f))
- **playground:** Recharts extra code ([950541c](https://github.com/cube-js/cube.js/commit/950541c))

### Features

- **client-react:** provide isQueryPresent() as static API method ([59dc5ce](https://github.com/cube-js/cube.js/commit/59dc5ce))
- **playground:** Make dashboard loading errors permanent ([155380d](https://github.com/cube-js/cube.js/commit/155380d))
- **playground:** Recharts code generation support ([c8c8230](https://github.com/cube-js/cube.js/commit/c8c8230))

## [0.10.52](https://github.com/cube-js/cube.js/compare/v0.10.51...v0.10.52) (2019-10-01)

### Bug Fixes

- **client-ngx:** client.ts is missing from the TypeScript compilation. Fix files ([f4885b4](https://github.com/cube-js/cube.js/commit/f4885b4))

## [0.10.51](https://github.com/cube-js/cube.js/compare/v0.10.50...v0.10.51) (2019-10-01)

### Bug Fixes

- **client-ngx:** client.ts is missing from the TypeScript compilation. Fix files ([8fe80f6](https://github.com/cube-js/cube.js/commit/8fe80f6))

## [0.10.50](https://github.com/cube-js/cube.js/compare/v0.10.49...v0.10.50) (2019-10-01)

### Bug Fixes

- **client-ngx:** client.ts is missing from the TypeScript compilation. Fix files ([ae5c2df](https://github.com/cube-js/cube.js/commit/ae5c2df))

## [0.10.49](https://github.com/cube-js/cube.js/compare/v0.10.48...v0.10.49) (2019-10-01)

### Bug Fixes

- **client-ngx:** client.ts is missing from the TypeScript compilation ([65a30cf](https://github.com/cube-js/cube.js/commit/65a30cf))

## [0.10.48](https://github.com/cube-js/cube.js/compare/v0.10.47...v0.10.48) (2019-10-01)

### Bug Fixes

- **client-ngx:** client.ts is missing from the TypeScript compilation ([ffab1a1](https://github.com/cube-js/cube.js/commit/ffab1a1))

## [0.10.47](https://github.com/cube-js/cube.js/compare/v0.10.46...v0.10.47) (2019-10-01)

### Bug Fixes

- **client-ngx:** client.ts is missing from the TypeScript compilation ([7dfc071](https://github.com/cube-js/cube.js/commit/7dfc071))

## [0.10.46](https://github.com/cube-js/cube.js/compare/v0.10.45...v0.10.46) (2019-09-30)

### Features

- Restructure Dashboard scaffolding to make it more user friendly and reliable ([78ba3bc](https://github.com/cube-js/cube.js/commit/78ba3bc))

## [0.10.45](https://github.com/cube-js/cube.js/compare/v0.10.44...v0.10.45) (2019-09-27)

### Bug Fixes

- TypeError: "listener" argument must be a function ([5cfc61e](https://github.com/cube-js/cube.js/commit/5cfc61e))

## [0.10.44](https://github.com/cube-js/cube.js/compare/v0.10.43...v0.10.44) (2019-09-27)

### Bug Fixes

- npm installs old dependencies on dashboard creation ([a7d519c](https://github.com/cube-js/cube.js/commit/a7d519c))
- **playground:** use default 3000 port for dashboard app as it's more appropriate ([ec4f3f4](https://github.com/cube-js/cube.js/commit/ec4f3f4))

### Features

- **cubejs-server:** Integrated support for TLS ([#213](https://github.com/cube-js/cube.js/issues/213)) ([66fe156](https://github.com/cube-js/cube.js/commit/66fe156))
- **playground:** Rename Explore to Build ([ce067a9](https://github.com/cube-js/cube.js/commit/ce067a9))
- **playground:** Show empty dashboard note ([ef559e5](https://github.com/cube-js/cube.js/commit/ef559e5))
- **playground:** Support various chart libraries for dashboard generation ([a4ba9c5](https://github.com/cube-js/cube.js/commit/a4ba9c5))

## [0.10.43](https://github.com/cube-js/cube.js/compare/v0.10.42...v0.10.43) (2019-09-27)

### Bug Fixes

- empty array reduce error in `stackedChartData` ([#211](https://github.com/cube-js/cube.js/issues/211)) ([1dc44bb](https://github.com/cube-js/cube.js/commit/1dc44bb))

### Features

- Dynamic dashboards ([#218](https://github.com/cube-js/cube.js/issues/218)) ([2c6cdc9](https://github.com/cube-js/cube.js/commit/2c6cdc9))

## [0.10.42](https://github.com/cube-js/cube.js/compare/v0.10.41...v0.10.42) (2019-09-16)

### Bug Fixes

- **client-ngx:** Function calls are not supported in decorators but 'ɵangular_packages_core_core_a' was called. ([65871f9](https://github.com/cube-js/cube.js/commit/65871f9))

## [0.10.41](https://github.com/cube-js/cube.js/compare/v0.10.40...v0.10.41) (2019-09-13)

### Bug Fixes

- support for deep nested watchers on 'QueryRenderer' ([#207](https://github.com/cube-js/cube.js/issues/207)) ([8d3a500](https://github.com/cube-js/cube.js/commit/8d3a500))

### Features

- Provide date filter with hourly granularity ([e423d82](https://github.com/cube-js/cube.js/commit/e423d82)), closes [#179](https://github.com/cube-js/cube.js/issues/179)

## [0.10.40](https://github.com/cube-js/cube.js/compare/v0.10.39...v0.10.40) (2019-09-09)

### Bug Fixes

- missed Vue.js build ([1cf22d5](https://github.com/cube-js/cube.js/commit/1cf22d5))

## [0.10.39](https://github.com/cube-js/cube.js/compare/v0.10.38...v0.10.39) (2019-09-09)

### Bug Fixes

- Requiring local node files is restricted: adding test for relative path resolvers ([f328d07](https://github.com/cube-js/cube.js/commit/f328d07))

## [0.10.38](https://github.com/cube-js/cube.js/compare/v0.10.37...v0.10.38) (2019-09-09)

### Bug Fixes

- Requiring local node files is restricted ([ba3c390](https://github.com/cube-js/cube.js/commit/ba3c390))

## [0.10.37](https://github.com/cube-js/cube.js/compare/v0.10.36...v0.10.37) (2019-09-09)

### Bug Fixes

- **client-ngx:** Omit warnings for Angular import: Use cjs module as main ([97e8d48](https://github.com/cube-js/cube.js/commit/97e8d48))

## [0.10.36](https://github.com/cube-js/cube.js/compare/v0.10.35...v0.10.36) (2019-09-09)

### Bug Fixes

- all queries forwarded to external DB instead of original one for zero pre-aggregation query ([2c230f4](https://github.com/cube-js/cube.js/commit/2c230f4))

## [0.10.35](https://github.com/cube-js/cube.js/compare/v0.10.34...v0.10.35) (2019-09-09)

### Bug Fixes

- LocalQueueDriver key interference for multitenant deployment ([aa860e4](https://github.com/cube-js/cube.js/commit/aa860e4))

### Features

- **mysql-driver:** Faster external pre-aggregations upload ([b6e3ee6](https://github.com/cube-js/cube.js/commit/b6e3ee6))
- `originalSql` external pre-aggregations support ([0db2282](https://github.com/cube-js/cube.js/commit/0db2282))
- Serve pre-aggregated data right from external database without hitting main one if pre-aggregation is available ([931fb7c](https://github.com/cube-js/cube.js/commit/931fb7c))

## [0.10.34](https://github.com/cube-js/cube.js/compare/v0.10.33...v0.10.34) (2019-09-06)

### Bug Fixes

- Athena timezone conversion issue for non-UTC server ([7085d2f](https://github.com/cube-js/cube.js/commit/7085d2f))

## [0.10.33](https://github.com/cube-js/cube.js/compare/v0.10.32...v0.10.33) (2019-09-06)

### Bug Fixes

- Revert to default queue concurrency for external pre-aggregations as driver pools expect this be aligned with default pool size ([c695ddd](https://github.com/cube-js/cube.js/commit/c695ddd))

## [0.10.32](https://github.com/cube-js/cube.js/compare/v0.10.31...v0.10.32) (2019-09-06)

### Bug Fixes

- In memory queue driver drop state if rollups are building too long ([ad4c062](https://github.com/cube-js/cube.js/commit/ad4c062))

### Features

- Speedup PG external pre-aggregations ([#201](https://github.com/cube-js/cube.js/issues/201)) ([7abf504](https://github.com/cube-js/cube.js/commit/7abf504)), closes [#200](https://github.com/cube-js/cube.js/issues/200)
- vue limit, offset and measure filters support ([#194](https://github.com/cube-js/cube.js/issues/194)) ([33f365a](https://github.com/cube-js/cube.js/commit/33f365a)), closes [#188](https://github.com/cube-js/cube.js/issues/188)

## [0.10.31](https://github.com/cube-js/cube.js/compare/v0.10.30...v0.10.31) (2019-08-27)

### Bug Fixes

- **athena-driver:** TypeError: Cannot read property 'map' of undefined ([478c6c6](https://github.com/cube-js/cube.js/commit/478c6c6))

## [0.10.30](https://github.com/cube-js/cube.js/compare/v0.10.29...v0.10.30) (2019-08-26)

### Bug Fixes

- Athena doesn't support `_` in contains filter ([d330be4](https://github.com/cube-js/cube.js/commit/d330be4))
- Athena doesn't support `'` in contains filter ([40a36d5](https://github.com/cube-js/cube.js/commit/40a36d5))

### Features

- `REDIS_TLS=true` env variable support ([55858cf](https://github.com/cube-js/cube.js/commit/55858cf))

## [0.10.29](https://github.com/cube-js/cube.js/compare/v0.10.28...v0.10.29) (2019-08-21)

### Bug Fixes

- MS SQL segment pre-aggregations support ([f8e37bf](https://github.com/cube-js/cube.js/commit/f8e37bf)), closes [#186](https://github.com/cube-js/cube.js/issues/186)

## [0.10.28](https://github.com/cube-js/cube.js/compare/v0.10.27...v0.10.28) (2019-08-19)

### Bug Fixes

- BigQuery to Postgres external rollup doesn't work ([feccdb5](https://github.com/cube-js/cube.js/commit/feccdb5)), closes [#178](https://github.com/cube-js/cube.js/issues/178)
- Presto error messages aren't showed correctly ([5f41afe](https://github.com/cube-js/cube.js/commit/5f41afe))
- Show dev server errors in console ([e8c3af9](https://github.com/cube-js/cube.js/commit/e8c3af9))

## [0.10.27](https://github.com/cube-js/cube.js/compare/v0.10.26...v0.10.27) (2019-08-18)

### Features

- Make `preAggregationsSchema` an option of CubejsServerCore - missed option propagation ([60d5704](https://github.com/cube-js/cube.js/commit/60d5704)), closes [#96](https://github.com/cube-js/cube.js/issues/96)

## [0.10.26](https://github.com/cube-js/cube.js/compare/v0.10.25...v0.10.26) (2019-08-18)

### Features

- Make `preAggregationsSchema` an option of CubejsServerCore ([3b1b082](https://github.com/cube-js/cube.js/commit/3b1b082)), closes [#96](https://github.com/cube-js/cube.js/issues/96)

## [0.10.25](https://github.com/cube-js/cube.js/compare/v0.10.24...v0.10.25) (2019-08-17)

### Bug Fixes

- MS SQL has unusual CREATE SCHEMA syntax ([16b8c87](https://github.com/cube-js/cube.js/commit/16b8c87)), closes [#185](https://github.com/cube-js/cube.js/issues/185)

## [0.10.24](https://github.com/cube-js/cube.js/compare/v0.10.23...v0.10.24) (2019-08-16)

### Bug Fixes

- MS SQL has unusual CTAS syntax ([1a00e4a](https://github.com/cube-js/cube.js/commit/1a00e4a)), closes [#185](https://github.com/cube-js/cube.js/issues/185)

## [0.10.23](https://github.com/cube-js/cube.js/compare/v0.10.22...v0.10.23) (2019-08-14)

### Bug Fixes

- Unexpected string literal Bigquery ([8768895](https://github.com/cube-js/cube.js/commit/8768895)), closes [#182](https://github.com/cube-js/cube.js/issues/182)

## [0.10.22](https://github.com/cube-js/cube.js/compare/v0.10.21...v0.10.22) (2019-08-09)

### Bug Fixes

- **clickhouse-driver:** Empty schema when CUBEJS_DB_NAME is provided ([7117e89](https://github.com/cube-js/cube.js/commit/7117e89))

## [0.10.21](https://github.com/cube-js/cube.js/compare/v0.10.20...v0.10.21) (2019-08-05)

### Features

- Offset pagination support ([7fb1715](https://github.com/cube-js/cube.js/commit/7fb1715)), closes [#117](https://github.com/cube-js/cube.js/issues/117)

## [0.10.20](https://github.com/cube-js/cube.js/compare/v0.10.19...v0.10.20) (2019-08-03)

### Features

- **playground:** Various dashboard hints ([eed2b55](https://github.com/cube-js/cube.js/commit/eed2b55))

## [0.10.19](https://github.com/cube-js/cube.js/compare/v0.10.18...v0.10.19) (2019-08-02)

### Bug Fixes

- **postgres-driver:** ERROR: type "string" does not exist ([d472e89](https://github.com/cube-js/cube.js/commit/d472e89)), closes [#176](https://github.com/cube-js/cube.js/issues/176)

## [0.10.18](https://github.com/cube-js/cube.js/compare/v0.10.17...v0.10.18) (2019-07-31)

### Bug Fixes

- BigQuery external rollup compatibility: use `__` separator for member aliases. Fix missed override. ([c1eb113](https://github.com/cube-js/cube.js/commit/c1eb113))

## [0.10.17](https://github.com/cube-js/cube.js/compare/v0.10.16...v0.10.17) (2019-07-31)

### Bug Fixes

- BigQuery external rollup compatibility: use `__` separator for member aliases. Fix all tests. ([723359c](https://github.com/cube-js/cube.js/commit/723359c))
- Moved joi dependency to it's new availability ([#171](https://github.com/cube-js/cube.js/issues/171)) ([1c20838](https://github.com/cube-js/cube.js/commit/1c20838))

### Features

- **playground:** Show editable files hint ([2dffe6c](https://github.com/cube-js/cube.js/commit/2dffe6c))
- **playground:** Slack and Docs links ([3270e70](https://github.com/cube-js/cube.js/commit/3270e70))

## [0.10.16](https://github.com/cube-js/cube.js/compare/v0.10.15...v0.10.16) (2019-07-20)

### Bug Fixes

- Added correct string concat for Mysql. ([#162](https://github.com/cube-js/cube.js/issues/162)) ([287411b](https://github.com/cube-js/cube.js/commit/287411b))
- remove redundant hacks: primaryKey filter for method dimensionColumns ([#161](https://github.com/cube-js/cube.js/issues/161)) ([f910a56](https://github.com/cube-js/cube.js/commit/f910a56))

### Features

- BigQuery external rollup support ([10c635c](https://github.com/cube-js/cube.js/commit/10c635c))
- Lean more on vue slots for state ([#148](https://github.com/cube-js/cube.js/issues/148)) ([e8af88d](https://github.com/cube-js/cube.js/commit/e8af88d))

## [0.10.15](https://github.com/cube-js/cube.js/compare/v0.10.14...v0.10.15) (2019-07-13)

**Note:** Version bump only for package cubejs

## [0.10.14](https://github.com/cube-js/cube.js/compare/v0.10.13...v0.10.14) (2019-07-13)

### Features

- **playground:** Show Query ([dc45fcb](https://github.com/cube-js/cube.js/commit/dc45fcb))
- Oracle driver ([#160](https://github.com/cube-js/cube.js/issues/160)) ([854ebff](https://github.com/cube-js/cube.js/commit/854ebff))

## [0.10.13](https://github.com/cube-js/cube.js/compare/v0.10.12...v0.10.13) (2019-07-08)

### Bug Fixes

- **bigquery-driver:** Error with Cube.js pre-aggregations in BigQuery ([01815a1](https://github.com/cube-js/cube.js/commit/01815a1)), closes [#158](https://github.com/cube-js/cube.js/issues/158)
- **cli:** update mem dependency security alert ([06a07a2](https://github.com/cube-js/cube.js/commit/06a07a2))

### Features

- **playground:** Copy code to clipboard ([30a2528](https://github.com/cube-js/cube.js/commit/30a2528))

## [0.10.12](https://github.com/cube-js/cube.js/compare/v0.10.11...v0.10.12) (2019-07-06)

### Bug Fixes

- Empty array for BigQuery in serverless GCP deployment ([#155](https://github.com/cube-js/cube.js/issues/155)) ([045094c](https://github.com/cube-js/cube.js/commit/045094c)), closes [#153](https://github.com/cube-js/cube.js/issues/153)
- QUERIES_undefined redis key for QueryQueue ([4c44886](https://github.com/cube-js/cube.js/commit/4c44886))

### Features

- **playground:** Links to Vanilla, Angular and Vue.js docs ([184495c](https://github.com/cube-js/cube.js/commit/184495c))

## [0.10.11](https://github.com/statsbotco/cube.js/compare/v0.10.10...v0.10.11) (2019-07-02)

### Bug Fixes

- TypeError: Cannot read property 'startsWith' of undefined at tableDefinition.filter.column: support uppercase databases ([995b115](https://github.com/statsbotco/cube.js/commit/995b115))

## [0.10.10](https://github.com/statsbotco/cube.js/compare/v0.10.9...v0.10.10) (2019-07-02)

### Bug Fixes

- **mongobi-driver:** accessing password field of undefined ([#147](https://github.com/statsbotco/cube.js/issues/147)) ([bdd9580](https://github.com/statsbotco/cube.js/commit/bdd9580))

## [0.10.9](https://github.com/statsbotco/cube.js/compare/v0.10.8...v0.10.9) (2019-06-30)

### Bug Fixes

- Syntax error during parsing: Unexpected token, expected: escaping back ticks ([9638a1a](https://github.com/statsbotco/cube.js/commit/9638a1a))

### Features

- **playground:** Chart.js charting library support ([40bb5d0](https://github.com/statsbotco/cube.js/commit/40bb5d0))

## [0.10.8](https://github.com/statsbotco/cube.js/compare/v0.10.7...v0.10.8) (2019-06-28)

### Features

- More readable compiling schema log message ([246805b](https://github.com/statsbotco/cube.js/commit/246805b))
- Presto driver ([1994083](https://github.com/statsbotco/cube.js/commit/1994083))

## [0.10.7](https://github.com/statsbotco/cube.js/compare/v0.10.6...v0.10.7) (2019-06-27)

### Bug Fixes

- config provided password not passed to server ([#145](https://github.com/statsbotco/cube.js/issues/145)) ([4b1afb1](https://github.com/statsbotco/cube.js/commit/4b1afb1))
- Module not found: Can't resolve 'react' ([a00e588](https://github.com/statsbotco/cube.js/commit/a00e588))

## [0.10.6](https://github.com/statsbotco/cube.js/compare/v0.10.5...v0.10.6) (2019-06-26)

### Bug Fixes

- Update version to fix audit warnings ([1bce587](https://github.com/statsbotco/cube.js/commit/1bce587))

## [0.10.5](https://github.com/statsbotco/cube.js/compare/v0.10.4...v0.10.5) (2019-06-26)

### Bug Fixes

- Update version to fix audit warnings ([f8f5225](https://github.com/statsbotco/cube.js/commit/f8f5225))

## [0.10.4](https://github.com/statsbotco/cube.js/compare/v0.10.3...v0.10.4) (2019-06-26)

### Bug Fixes

- Gray screen for Playground on version update ([b08333f](https://github.com/statsbotco/cube.js/commit/b08333f))

### Features

- More descriptive error for SyntaxError ([f6d12d3](https://github.com/statsbotco/cube.js/commit/f6d12d3))

## [0.10.3](https://github.com/statsbotco/cube.js/compare/v0.10.2...v0.10.3) (2019-06-26)

### Bug Fixes

- Snowflake driver config var typo ([d729b9d](https://github.com/statsbotco/cube.js/commit/d729b9d))

## [0.10.2](https://github.com/statsbotco/cube.js/compare/v0.10.1...v0.10.2) (2019-06-26)

### Bug Fixes

- Snowflake driver missing dependency ([b7620b3](https://github.com/statsbotco/cube.js/commit/b7620b3))

## [0.10.1](https://github.com/statsbotco/cube.js/compare/v0.10.0...v0.10.1) (2019-06-26)

### Features

- **cli:** Revert back concise next steps ([f4fa1e1](https://github.com/statsbotco/cube.js/commit/f4fa1e1))
- Snowflake driver ([35861b5](https://github.com/statsbotco/cube.js/commit/35861b5)), closes [#142](https://github.com/statsbotco/cube.js/issues/142)

# [0.10.0](https://github.com/statsbotco/cube.js/compare/v0.9.24...v0.10.0) (2019-06-21)

### Features

- **api-gateway:** `queryTransformer` security hook ([a9c41b2](https://github.com/statsbotco/cube.js/commit/a9c41b2))
- **playground:** App layout for dashboard ([f5578dd](https://github.com/statsbotco/cube.js/commit/f5578dd))
- **schema-compiler:** `asyncModules` and Node.js `require()`: support loading cube definitions from DB and other async sources ([397cceb](https://github.com/statsbotco/cube.js/commit/397cceb)), closes [#141](https://github.com/statsbotco/cube.js/issues/141)

## [0.9.24](https://github.com/statsbotco/cube.js/compare/v0.9.23...v0.9.24) (2019-06-17)

### Bug Fixes

- **mssql-driver:** Fix domain passed as an empty string case: ConnectionError: Login failed. The login is from an untrusted domain and cannot be used with Windows authentication ([89383dc](https://github.com/statsbotco/cube.js/commit/89383dc))
- Fix dev server in production mode message ([7586ad5](https://github.com/statsbotco/cube.js/commit/7586ad5))

### Features

- **mssql-driver:** Support query cancellation ([22a4bba](https://github.com/statsbotco/cube.js/commit/22a4bba))

## [0.9.23](https://github.com/statsbotco/cube.js/compare/v0.9.22...v0.9.23) (2019-06-17)

### Bug Fixes

- **hive:** Fix count when id is not defined ([5a5fffd](https://github.com/statsbotco/cube.js/commit/5a5fffd))
- **hive-driver:** SparkSQL compatibility ([1f20225](https://github.com/statsbotco/cube.js/commit/1f20225))

## [0.9.22](https://github.com/statsbotco/cube.js/compare/v0.9.21...v0.9.22) (2019-06-16)

### Bug Fixes

- **hive-driver:** Incorrect default Hive version ([379bff2](https://github.com/statsbotco/cube.js/commit/379bff2))

## [0.9.21](https://github.com/statsbotco/cube.js/compare/v0.9.20...v0.9.21) (2019-06-16)

### Features

- Hive dialect for simple queries ([30d4a30](https://github.com/statsbotco/cube.js/commit/30d4a30))

## [0.9.20](https://github.com/statsbotco/cube.js/compare/v0.9.19...v0.9.20) (2019-06-16)

### Bug Fixes

- **api-gateway:** Unexpected token u in JSON at position 0 at JSON.parse ([f95cea8](https://github.com/statsbotco/cube.js/commit/f95cea8))

### Features

- Pure JS Hive Thrift Driver ([4ca169e](https://github.com/statsbotco/cube.js/commit/4ca169e))

## [0.9.19](https://github.com/statsbotco/cube.js/compare/v0.9.18...v0.9.19) (2019-06-13)

### Bug Fixes

- **api-gateway:** handle can't parse date: Cannot read property 'end' of undefined ([a61b0da](https://github.com/statsbotco/cube.js/commit/a61b0da))
- **serverless:** remove redundant CUBEJS_API_URL env variable: Serverless offline framework support ([84a20b3](https://github.com/statsbotco/cube.js/commit/84a20b3)), closes [#121](https://github.com/statsbotco/cube.js/issues/121)
- Handle requests for hidden members: TypeError: Cannot read property 'type' of undefined at R.pipe.R.map.p ([5cdf71b](https://github.com/statsbotco/cube.js/commit/5cdf71b))
- Handle rollingWindow queries without dateRange: TypeError: Cannot read property '0' of undefined at BaseTimeDimension.dateFromFormatted ([409a238](https://github.com/statsbotco/cube.js/commit/409a238))
- issue with query generator for Mongobi for nested fields in document ([907b234](https://github.com/statsbotco/cube.js/commit/907b234)), closes [#56](https://github.com/statsbotco/cube.js/issues/56)
- More descriptive SyntaxError messages ([acd17ad](https://github.com/statsbotco/cube.js/commit/acd17ad))

### Features

- Add Typescript typings for server-core ([#111](https://github.com/statsbotco/cube.js/issues/111)) ([b1b895e](https://github.com/statsbotco/cube.js/commit/b1b895e))

## [0.9.18](https://github.com/statsbotco/cube.js/compare/v0.9.17...v0.9.18) (2019-06-12)

### Bug Fixes

- **mssql-driver:** Set default request timeout to 10 minutes ([c411484](https://github.com/statsbotco/cube.js/commit/c411484))

## [0.9.17](https://github.com/statsbotco/cube.js/compare/v0.9.16...v0.9.17) (2019-06-11)

### Bug Fixes

- **cli:** jdbc-driver fail hides db type not supported errors ([6f7c675](https://github.com/statsbotco/cube.js/commit/6f7c675))

### Features

- **mssql-driver:** Add domain env variable ([bb4c4a8](https://github.com/statsbotco/cube.js/commit/bb4c4a8))

## [0.9.16](https://github.com/statsbotco/cube.js/compare/v0.9.15...v0.9.16) (2019-06-10)

### Bug Fixes

- force escape cubeAlias to work with restricted column names such as "case" ([#128](https://github.com/statsbotco/cube.js/issues/128)) ([b8a59da](https://github.com/statsbotco/cube.js/commit/b8a59da))
- **playground:** Do not cache index.html to prevent missing resource errors on version upgrades ([4f20955](https://github.com/statsbotco/cube.js/commit/4f20955)), closes [#116](https://github.com/statsbotco/cube.js/issues/116)

### Features

- **cli:** Edit .env after app create help instruction ([f039c01](https://github.com/statsbotco/cube.js/commit/f039c01))
- **playground:** Go to explore modal after schema generation ([5325c2d](https://github.com/statsbotco/cube.js/commit/5325c2d))

## [0.9.15](https://github.com/statsbotco/cube.js/compare/v0.9.14...v0.9.15) (2019-06-07)

### Bug Fixes

- **schema-compiler:** subquery in FROM must have an alias -- fix Redshift rollingWindow ([70b752f](https://github.com/statsbotco/cube.js/commit/70b752f))

## [0.9.14](https://github.com/statsbotco/cube.js/compare/v0.9.13...v0.9.14) (2019-06-07)

### Features

- Add option to run in production without redis ([a7de417](https://github.com/statsbotco/cube.js/commit/a7de417)), closes [#110](https://github.com/statsbotco/cube.js/issues/110)
- Added SparkSQL and Hive support to the JDBC driver. ([#127](https://github.com/statsbotco/cube.js/issues/127)) ([659c24c](https://github.com/statsbotco/cube.js/commit/659c24c))
- View Query SQL in Playground ([8ef28c8](https://github.com/statsbotco/cube.js/commit/8ef28c8))

## [0.9.13](https://github.com/statsbotco/cube.js/compare/v0.9.12...v0.9.13) (2019-06-06)

### Bug Fixes

- Schema generation with joins having case sensitive table and column names ([#124](https://github.com/statsbotco/cube.js/issues/124)) ([c7b706a](https://github.com/statsbotco/cube.js/commit/c7b706a)), closes [#120](https://github.com/statsbotco/cube.js/issues/120) [#120](https://github.com/statsbotco/cube.js/issues/120)

## [0.9.12](https://github.com/statsbotco/cube.js/compare/v0.9.11...v0.9.12) (2019-06-03)

### Bug Fixes

- **api-gateway:** Unexpected token u in JSON at position 0 at JSON.parse ([91ca994](https://github.com/statsbotco/cube.js/commit/91ca994))
- **client-core:** Update normalizePivotConfig method to not to fail if x or y are missing ([ee20863](https://github.com/statsbotco/cube.js/commit/ee20863)), closes [#10](https://github.com/statsbotco/cube.js/issues/10)
- **schema-compiler:** cast parameters for IN filters ([28f3e48](https://github.com/statsbotco/cube.js/commit/28f3e48)), closes [#119](https://github.com/statsbotco/cube.js/issues/119)

## [0.9.11](https://github.com/statsbotco/cube.js/compare/v0.9.10...v0.9.11) (2019-05-31)

### Bug Fixes

- **client-core:** ResultSet series returns a series with no data ([715e170](https://github.com/statsbotco/cube.js/commit/715e170)), closes [#38](https://github.com/statsbotco/cube.js/issues/38)
- **schema-compiler:** TypeError: Cannot read property 'filterToWhere' of undefined ([6b399ea](https://github.com/statsbotco/cube.js/commit/6b399ea))

## [0.9.10](https://github.com/statsbotco/cube.js/compare/v0.9.9...v0.9.10) (2019-05-29)

### Bug Fixes

- **cli:** @cubejs-backend/schema-compiler/scaffolding/ScaffoldingTemplate dependency not found ([4296204](https://github.com/statsbotco/cube.js/commit/4296204))

## [0.9.9](https://github.com/statsbotco/cube.js/compare/v0.9.8...v0.9.9) (2019-05-29)

### Bug Fixes

- **cli:** missing package files ([81e8549](https://github.com/statsbotco/cube.js/commit/81e8549))

## [0.9.8](https://github.com/statsbotco/cube.js/compare/v0.9.7...v0.9.8) (2019-05-29)

### Features

- **cubejs-cli:** add token generation ([#67](https://github.com/statsbotco/cube.js/issues/67)) ([2813fed](https://github.com/statsbotco/cube.js/commit/2813fed))
- **postgres-driver:** SSL error hint for Heroku users ([0e9b9cb](https://github.com/statsbotco/cube.js/commit/0e9b9cb))

## [0.9.7](https://github.com/statsbotco/cube.js/compare/v0.9.6...v0.9.7) (2019-05-27)

### Features

- **postgres-driver:** support CUBEJS_DB_SSL option ([67a767e](https://github.com/statsbotco/cube.js/commit/67a767e))

## [0.9.6](https://github.com/statsbotco/cube.js/compare/v0.9.5...v0.9.6) (2019-05-24)

### Bug Fixes

- contains filter does not work with MS SQL Server database ([35210f6](https://github.com/statsbotco/cube.js/commit/35210f6)), closes [#113](https://github.com/statsbotco/cube.js/issues/113)

### Features

- better npm fail message in Playground ([545a020](https://github.com/statsbotco/cube.js/commit/545a020))
- **playground:** better add to dashboard error messages ([94e8dbf](https://github.com/statsbotco/cube.js/commit/94e8dbf))

## [0.9.5](https://github.com/statsbotco/cube.js/compare/v0.9.4...v0.9.5) (2019-05-22)

### Features

- Propagate `renewQuery` option from API to orchestrator ([9c640ba](https://github.com/statsbotco/cube.js/commit/9c640ba)), closes [#112](https://github.com/statsbotco/cube.js/issues/112)

## [0.9.4](https://github.com/statsbotco/cube.js/compare/v0.9.3...v0.9.4) (2019-05-22)

### Features

- Add `refreshKeyRenewalThreshold` option ([aa69449](https://github.com/statsbotco/cube.js/commit/aa69449)), closes [#112](https://github.com/statsbotco/cube.js/issues/112)

## [0.9.3](https://github.com/statsbotco/cube.js/compare/v0.9.2...v0.9.3) (2019-05-21)

### Bug Fixes

- **playground:** revert back create-react-app to npx as there're much more problems with global npm ([e434939](https://github.com/statsbotco/cube.js/commit/e434939))

## [0.9.2](https://github.com/statsbotco/cube.js/compare/v0.9.1...v0.9.2) (2019-05-11)

### Bug Fixes

- External rollups serverless implementation ([6d13370](https://github.com/statsbotco/cube.js/commit/6d13370))

## [0.9.1](https://github.com/statsbotco/cube.js/compare/v0.9.0...v0.9.1) (2019-05-11)

### Bug Fixes

- update BaseDriver dependencies ([a7aef2b](https://github.com/statsbotco/cube.js/commit/a7aef2b))

# [0.9.0](https://github.com/statsbotco/cube.js/compare/v0.8.7...v0.9.0) (2019-05-11)

### Features

- External rollup implementation ([d22a809](https://github.com/statsbotco/cube.js/commit/d22a809))

## [0.8.7](https://github.com/statsbotco/cube.js/compare/v0.8.6...v0.8.7) (2019-05-09)

### Bug Fixes

- **cubejs-react:** add core-js dependency ([#107](https://github.com/statsbotco/cube.js/issues/107)) ([0e13ffe](https://github.com/statsbotco/cube.js/commit/0e13ffe))
- **query-orchestrator:** Athena got swamped by fetch schema requests ([d8b5440](https://github.com/statsbotco/cube.js/commit/d8b5440))

## [0.8.6](https://github.com/statsbotco/cube.js/compare/v0.8.5...v0.8.6) (2019-05-05)

### Bug Fixes

- **cli:** Update Slack Community Link ([#101](https://github.com/statsbotco/cube.js/issues/101)) ([c5fd43f](https://github.com/statsbotco/cube.js/commit/c5fd43f))
- **playground:** Update Slack Community Link ([#102](https://github.com/statsbotco/cube.js/issues/102)) ([61a9bb0](https://github.com/statsbotco/cube.js/commit/61a9bb0))

### Features

- Replace codesandbox by running dashboard react-app directly ([861c817](https://github.com/statsbotco/cube.js/commit/861c817))

## [0.8.5](https://github.com/statsbotco/cube.js/compare/v0.8.4...v0.8.5) (2019-05-02)

### Bug Fixes

- **clickhouse-driver:** merging config with custom queryOptions which were not passing along the database ([#100](https://github.com/statsbotco/cube.js/issues/100)) ([dedc279](https://github.com/statsbotco/cube.js/commit/dedc279))

## [0.8.4](https://github.com/statsbotco/cube.js/compare/v0.8.3...v0.8.4) (2019-05-02)

### Features

- Angular client ([#99](https://github.com/statsbotco/cube.js/issues/99)) ([640e6de](https://github.com/statsbotco/cube.js/commit/640e6de))

## [0.8.3](https://github.com/statsbotco/cube.js/compare/v0.8.2...v0.8.3) (2019-05-01)

### Features

- clickhouse dialect implementation ([#98](https://github.com/statsbotco/cube.js/issues/98)) ([7236e29](https://github.com/statsbotco/cube.js/commit/7236e29)), closes [#93](https://github.com/statsbotco/cube.js/issues/93)

## [0.8.2](https://github.com/statsbotco/cube.js/compare/v0.8.1...v0.8.2) (2019-04-30)

### Bug Fixes

- Wrong variables when creating new BigQuery backed project ([bae6348](https://github.com/statsbotco/cube.js/commit/bae6348)), closes [#97](https://github.com/statsbotco/cube.js/issues/97)

## [0.8.1](https://github.com/statsbotco/cube.js/compare/v0.8.0...v0.8.1) (2019-04-30)

### Bug Fixes

- add the missing @cubejs-client/vue package ([#95](https://github.com/statsbotco/cube.js/issues/95)) ([9e8c4be](https://github.com/statsbotco/cube.js/commit/9e8c4be))

### Features

- Driver for ClickHouse database support ([#94](https://github.com/statsbotco/cube.js/issues/94)) ([0f05321](https://github.com/statsbotco/cube.js/commit/0f05321)), closes [#1](https://github.com/statsbotco/cube.js/issues/1)
- Serverless Google Cloud Platform in CLI support ([392ba1e](https://github.com/statsbotco/cube.js/commit/392ba1e))

# [0.8.0](https://github.com/statsbotco/cube.js/compare/v0.7.10...v0.8.0) (2019-04-29)

### Features

- Serverless Google Cloud Platform support ([89ec0ec](https://github.com/statsbotco/cube.js/commit/89ec0ec))

## [0.7.10](https://github.com/statsbotco/cube.js/compare/v0.7.9...v0.7.10) (2019-04-25)

### Bug Fixes

- **client-core:** Table pivot incorrectly behaves with multiple measures ([adb2270](https://github.com/statsbotco/cube.js/commit/adb2270))
- **client-core:** use ',' as standard axisValue delimiter ([e889955](https://github.com/statsbotco/cube.js/commit/e889955)), closes [#19](https://github.com/statsbotco/cube.js/issues/19)

## [0.7.9](https://github.com/statsbotco/cube.js/compare/v0.7.8...v0.7.9) (2019-04-24)

### Features

- **schema-compiler:** Allow to pass functions to USER_CONTEXT ([b489090](https://github.com/statsbotco/cube.js/commit/b489090)), closes [#88](https://github.com/statsbotco/cube.js/issues/88)

## [0.7.8](https://github.com/statsbotco/cube.js/compare/v0.7.7...v0.7.8) (2019-04-24)

### Bug Fixes

- **playground:** Dashboard doesn't work on Windows ([48a2ec4](https://github.com/statsbotco/cube.js/commit/48a2ec4)), closes [#82](https://github.com/statsbotco/cube.js/issues/82)

## [0.7.7](https://github.com/statsbotco/cube.js/compare/v0.7.6...v0.7.7) (2019-04-24)

### Bug Fixes

- **playground:** Dashboard doesn't work on Windows ([7c48aa4](https://github.com/statsbotco/cube.js/commit/7c48aa4)), closes [#82](https://github.com/statsbotco/cube.js/issues/82)

## [0.7.6](https://github.com/statsbotco/cube.js/compare/v0.7.5...v0.7.6) (2019-04-23)

### Bug Fixes

- **playground:** Cannot read property 'content' of undefined at e.value ([7392feb](https://github.com/statsbotco/cube.js/commit/7392feb))
- Use cross-fetch instead of isomorphic-fetch to allow React-Native builds ([#92](https://github.com/statsbotco/cube.js/issues/92)) ([79150f4](https://github.com/statsbotco/cube.js/commit/79150f4))
- **query-orchestrator:** add RedisFactory and promisify methods manually ([#89](https://github.com/statsbotco/cube.js/issues/89)) ([cdfcd87](https://github.com/statsbotco/cube.js/commit/cdfcd87)), closes [#84](https://github.com/statsbotco/cube.js/issues/84)

### Features

- Support member key in filters in query ([#91](https://github.com/statsbotco/cube.js/issues/91)) ([e1fccc0](https://github.com/statsbotco/cube.js/commit/e1fccc0))
- **schema-compiler:** Athena rollingWindow support ([f112c69](https://github.com/statsbotco/cube.js/commit/f112c69))

## [0.7.5](https://github.com/statsbotco/cube.js/compare/v0.7.4...v0.7.5) (2019-04-18)

### Bug Fixes

- **schema-compiler:** Athena, Mysql and BigQuery doesn't respect multiple contains filter ([0a8f324](https://github.com/statsbotco/cube.js/commit/0a8f324))

## [0.7.4](https://github.com/statsbotco/cube.js/compare/v0.7.3...v0.7.4) (2019-04-17)

### Bug Fixes

- Make dashboard app creation explicit. Show error messages if dashboard failed to create. ([3b2a22b](https://github.com/statsbotco/cube.js/commit/3b2a22b))
- **api-gateway:** measures is always required ([04adb7d](https://github.com/statsbotco/cube.js/commit/04adb7d))
- **mongobi-driver:** fix ssl configuration ([#78](https://github.com/statsbotco/cube.js/issues/78)) ([ddc4dff](https://github.com/statsbotco/cube.js/commit/ddc4dff))

## [0.7.3](https://github.com/statsbotco/cube.js/compare/v0.7.2...v0.7.3) (2019-04-16)

### Bug Fixes

- Allow SSR: use isomorphic-fetch instead of whatwg-fetch. ([902e581](https://github.com/statsbotco/cube.js/commit/902e581)), closes [#1](https://github.com/statsbotco/cube.js/issues/1)

## [0.7.2](https://github.com/statsbotco/cube.js/compare/v0.7.1...v0.7.2) (2019-04-15)

### Bug Fixes

- Avoid 502 for Playground in serverless: minimize babel ([f9d3171](https://github.com/statsbotco/cube.js/commit/f9d3171))

### Features

- MS SQL database driver ([48fbe66](https://github.com/statsbotco/cube.js/commit/48fbe66)), closes [#76](https://github.com/statsbotco/cube.js/issues/76)

## [0.7.1](https://github.com/statsbotco/cube.js/compare/v0.7.0...v0.7.1) (2019-04-15)

### Bug Fixes

- **serverless:** `getApiHandler` called on undefined ([0ee5121](https://github.com/statsbotco/cube.js/commit/0ee5121))
- Allow Playground to work in Serverless mode ([2c0c89c](https://github.com/statsbotco/cube.js/commit/2c0c89c))

# [0.7.0](https://github.com/statsbotco/cube.js/compare/v0.6.2...v0.7.0) (2019-04-15)

### Features

- App multi-tenancy support in single ServerCore instance ([6f0220f](https://github.com/statsbotco/cube.js/commit/6f0220f))

## [0.6.2](https://github.com/statsbotco/cube.js/compare/v0.6.1...v0.6.2) (2019-04-12)

### Features

- Natural language date range support ([b962e80](https://github.com/statsbotco/cube.js/commit/b962e80))
- **api-gateway:** Order support ([670237b](https://github.com/statsbotco/cube.js/commit/670237b))

## [0.6.1](https://github.com/statsbotco/cube.js/compare/v0.6.0...v0.6.1) (2019-04-11)

### Bug Fixes

- Get Playground API_URL from window.location until provided explicitly in env. Remote server playground case. ([7b1a0ff](https://github.com/statsbotco/cube.js/commit/7b1a0ff))

### Features

- Disable authentication checks in developer mode ([bc09eba](https://github.com/statsbotco/cube.js/commit/bc09eba))
- Formatted error logging in developer mode ([3376a50](https://github.com/statsbotco/cube.js/commit/3376a50))

# [0.6.0](https://github.com/statsbotco/cube.js/compare/v0.5.2...v0.6.0) (2019-04-09)

### Bug Fixes

- **playground:** no such file or directory, scandir 'dashboard-app/src' ([64ec481](https://github.com/statsbotco/cube.js/commit/64ec481))

### Features

- query validation added in api-gateway ([#73](https://github.com/statsbotco/cube.js/issues/73)) ([21f6176](https://github.com/statsbotco/cube.js/commit/21f6176)), closes [#39](https://github.com/statsbotco/cube.js/issues/39)
- QueryBuilder heuristics. Playground area, table and number implementation. ([c883a48](https://github.com/statsbotco/cube.js/commit/c883a48))
- Vue.js reactivity on query update ([#70](https://github.com/statsbotco/cube.js/issues/70)) ([167fdbf](https://github.com/statsbotco/cube.js/commit/167fdbf))

## [0.5.2](https://github.com/statsbotco/cube.js/compare/v0.5.1...v0.5.2) (2019-04-05)

### Features

- Add redshift to postgres driver link ([#71](https://github.com/statsbotco/cube.js/issues/71)) ([4797588](https://github.com/statsbotco/cube.js/commit/4797588))
- Playground UX improvements ([6760a1d](https://github.com/statsbotco/cube.js/commit/6760a1d))

## [0.5.1](https://github.com/statsbotco/cube.js/compare/v0.5.0...v0.5.1) (2019-04-02)

### Features

- BigQuery driver ([654edac](https://github.com/statsbotco/cube.js/commit/654edac))
- Vue package improvements and docs ([fc38e69](https://github.com/statsbotco/cube.js/commit/fc38e69)), closes [#68](https://github.com/statsbotco/cube.js/issues/68)

# [0.5.0](https://github.com/statsbotco/cube.js/compare/v0.4.6...v0.5.0) (2019-04-01)

### Bug Fixes

- **schema-compiler:** joi@10.6.0 upgrade to joi@14.3.1 ([#59](https://github.com/statsbotco/cube.js/issues/59)) ([f035531](https://github.com/statsbotco/cube.js/commit/f035531))
- mongobi issue with parsing schema file with nested fields ([eaf1631](https://github.com/statsbotco/cube.js/commit/eaf1631)), closes [#55](https://github.com/statsbotco/cube.js/issues/55)

### Features

- add basic vue support ([#65](https://github.com/statsbotco/cube.js/issues/65)) ([f45468b](https://github.com/statsbotco/cube.js/commit/f45468b))
- use local queue and cache for local dev server instead of Redis one ([50f1bbb](https://github.com/statsbotco/cube.js/commit/50f1bbb))

## [0.4.6](https://github.com/statsbotco/cube.js/compare/v0.4.5...v0.4.6) (2019-03-27)

### Features

- Dashboard Generator for Playground ([28a42ee](https://github.com/statsbotco/cube.js/commit/28a42ee))

## [0.4.5](https://github.com/statsbotco/cube.js/compare/v0.4.4...v0.4.5) (2019-03-21)

### Bug Fixes

- client-react - query prop now has default blank value ([#54](https://github.com/statsbotco/cube.js/issues/54)) ([27e7090](https://github.com/statsbotco/cube.js/commit/27e7090))

### Features

- Make API path namespace configurable ([#53](https://github.com/statsbotco/cube.js/issues/53)) ([b074a3d](https://github.com/statsbotco/cube.js/commit/b074a3d))
- Playground filters implementation ([de4315d](https://github.com/statsbotco/cube.js/commit/de4315d))

## [0.4.4](https://github.com/statsbotco/cube.js/compare/v0.4.3...v0.4.4) (2019-03-17)

### Bug Fixes

- Postgres doesn't show any data for queries with time dimension. ([e95e6fe](https://github.com/statsbotco/cube.js/commit/e95e6fe))

### Features

- Introduce Schema generation UI in Playground ([349c7d0](https://github.com/statsbotco/cube.js/commit/349c7d0))

## [0.4.3](https://github.com/statsbotco/cube.js/compare/v0.4.2...v0.4.3) (2019-03-15)

### Bug Fixes

- **mongobi-driver:** implement `convert_tz` as a simple hour shift ([c97e451](https://github.com/statsbotco/cube.js/commit/c97e451)), closes [#50](https://github.com/statsbotco/cube.js/issues/50)

## [0.4.2](https://github.com/statsbotco/cube.js/compare/v0.4.1...v0.4.2) (2019-03-14)

### Bug Fixes

- **mongobi-driver:** Fix Server does not support secure connnection on connection to localhost ([3202508](https://github.com/statsbotco/cube.js/commit/3202508))

## [0.4.1](https://github.com/statsbotco/cube.js/compare/v0.4.0...v0.4.1) (2019-03-14)

### Bug Fixes

- concat called on undefined for empty MongoDB password ([7d75b1e](https://github.com/statsbotco/cube.js/commit/7d75b1e))

### Features

- Allow to use custom checkAuth middleware ([19d5cd8](https://github.com/statsbotco/cube.js/commit/19d5cd8)), closes [#42](https://github.com/statsbotco/cube.js/issues/42)

# [0.4.0](https://github.com/statsbotco/cube.js/compare/v0.3.5-alpha.0...v0.4.0) (2019-03-13)

### Features

- Add MongoBI connector and schema adapter support ([3ebbbf0](https://github.com/statsbotco/cube.js/commit/3ebbbf0))

## [0.3.5-alpha.0](https://github.com/statsbotco/cube.js/compare/v0.3.5...v0.3.5-alpha.0) (2019-03-12)

**Note:** Version bump only for package cubejs
