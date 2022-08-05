# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.30.45](https://github.com/cube-js/cube.js/compare/v0.30.44...v0.30.45) (2022-08-05)


### Features

* **cubesql:** Support binary bitwise operators (>>, <<) ([7363879](https://github.com/cube-js/cube.js/commit/7363879184395b3c499f9b678da7152362226ea0))
* **cubesql:** Support svv_tables table (Redshift) ([#5060](https://github.com/cube-js/cube.js/issues/5060)) ([d3ed3ac](https://github.com/cube-js/cube.js/commit/d3ed3aca798d41fe4e1919c9fde2f7610435168c))





## [0.30.44](https://github.com/cube-js/cube.js/compare/v0.30.43...v0.30.44) (2022-08-01)


### Bug Fixes

* **cubesql:** Ignore IO's UnexpectedEof|BrokenPipe on handling error ([98deb73](https://github.com/cube-js/cube.js/commit/98deb7362bf772816af88173e6669bf486c328a9))





## [0.30.43](https://github.com/cube-js/cube.js/compare/v0.30.42...v0.30.43) (2022-07-28)


### Bug Fixes

* **cubesq:** Ignore BrokenPipe/UnexpectedEOF as error in pg-wire ([4ec01d2](https://github.com/cube-js/cube.js/commit/4ec01d269f2216f74b841ebe2fd96d3b8597fdcc))


### Features

* **cubesql:** Security Context switching (Row Access) ([731e1ab](https://github.com/cube-js/cube.js/commit/731e1ab6d9362fb9a1857f5276e22a565f79781c))





## [0.30.42](https://github.com/cube-js/cube.js/compare/v0.30.41...v0.30.42) (2022-07-27)


### Features

* **cubesql:** Metabase - support Summarize by week of year ([#5000](https://github.com/cube-js/cube.js/issues/5000)) ([37589a9](https://github.com/cube-js/cube.js/commit/37589a9e58b0c8f14922041432647b814759f22a))





## [0.30.38](https://github.com/cube-js/cube.js/compare/v0.30.37...v0.30.38) (2022-07-25)


### Features

* **cubesql:** Define standard_conforming_strings (SQLAlchemy compatibility) ([8fbc046](https://github.com/cube-js/cube.js/commit/8fbc0467c2e3e37fa9c4b320630dc9200884f3ee)), closes [#L2994](https://github.com/cube-js/cube.js/issues/L2994)
* **cubesql:** Support Cast(expr as Regclass) ([e3cafe4](https://github.com/cube-js/cube.js/commit/e3cafe4a0a291d61545e8855425b8755f3629a4e))
* **cubesql:** Support for new introspection query in SQLAlchemy ([0dbc9e6](https://github.com/cube-js/cube.js/commit/0dbc9e6551016d12155bba27a57b9a17e13dbd02)), closes [#L3381](https://github.com/cube-js/cube.js/issues/L3381)
* **cubesql:** Support pg_catalog.pg_sequence table ([fe057bf](https://github.com/cube-js/cube.js/commit/fe057bf256b8744a9c3f407908808cefa6cd6d8c))





## [0.30.37](https://github.com/cube-js/cube.js/compare/v0.30.36...v0.30.37) (2022-07-20)


### Bug Fixes

* **cubesql:** Correct UDTF behavior with no batch sections ([f52c89a](https://github.com/cube-js/cube.js/commit/f52c89a1baedd9e5a259b663f9427e02ade9fb10))


### Features

* **cubesql:** Add `pg_constraint` pg_type ([e9beb5f](https://github.com/cube-js/cube.js/commit/e9beb5fd875e8f2d181aec45035849e503a61e6b))





## [0.30.36](https://github.com/cube-js/cube.js/compare/v0.30.35...v0.30.36) (2022-07-18)


### Features

* **cubesql:** Metabase - support between numbers queries ([#4916](https://github.com/cube-js/cube.js/issues/4916)) ([52a34fd](https://github.com/cube-js/cube.js/commit/52a34fd563aed43448908e9c5efb3fd55d82de74))
* **cubesql:** Metabase - support Summarize's Bins ([#4926](https://github.com/cube-js/cube.js/issues/4926)) ([8fcdf1a](https://github.com/cube-js/cube.js/commit/8fcdf1a0d1730dbab2871ed6acb99d5added8df1))
* **cubesql:** Metabase string contains / not contains filters ([#4922](https://github.com/cube-js/cube.js/issues/4922)) ([e5abc09](https://github.com/cube-js/cube.js/commit/e5abc09747c5b7d1855b373236b8c682ce278710))
* **cubesql:** Support `has_schema_privilege` UDF ([7ba3148](https://github.com/cube-js/cube.js/commit/7ba3148532568da23934e60abf5919f0e85c8956))
* **cubesql:** Support `pg_catalog.pg_statio_user_tables` meta table ([a4d9050](https://github.com/cube-js/cube.js/commit/a4d9050f02b14a04374520256dabee29e5a4c226))
* **cubesql:** Support `pg_total_relation_size` UDF ([cfca8ee](https://github.com/cube-js/cube.js/commit/cfca8eeec3bd83ba343dbeadfb2ad063d8058ec7))
* **cubesql:** Support minus, multiply, division for binary expression in projection ([#4899](https://github.com/cube-js/cube.js/issues/4899)) ([1fc653b](https://github.com/cube-js/cube.js/commit/1fc653bd6cd83d6d023b51cf4141f9649e6b00da))





## [0.30.35](https://github.com/cube-js/cube.js/compare/v0.30.34...v0.30.35) (2022-07-14)


### Bug Fixes

* **cubesql:** Binary operations with dates and intervals ([#4908](https://github.com/cube-js/cube.js/issues/4908)) ([a2a0cba](https://github.com/cube-js/cube.js/commit/a2a0cba9c3ea0507bd81c684d120d897579f2b90))


### Features

* **cubesql:** Metabase - datetime filters with 'starting from' flag support ([#4882](https://github.com/cube-js/cube.js/issues/4882)) ([4cc01f1](https://github.com/cube-js/cube.js/commit/4cc01f1750b141ad851081efafb1833133420885))
* **cubesql:** Support `PREPARE` queries in pg-wire ([#4906](https://github.com/cube-js/cube.js/issues/4906)) ([2e2ae63](https://github.com/cube-js/cube.js/commit/2e2ae6347692ae5ae77fcf6f921c97b5c5bd10f1))





## [0.30.34](https://github.com/cube-js/cube.js/compare/v0.30.33...v0.30.34) (2022-07-12)


### Features

* **cubesql:** Metabase - BETWEEN filters support ([#4852](https://github.com/cube-js/cube.js/issues/4852)) ([b191120](https://github.com/cube-js/cube.js/commit/b19112079f0f9a51d6703e37afaa121d09ce31e4))
* **cubesql:** Metabase - filters with relative dates support ([#4851](https://github.com/cube-js/cube.js/issues/4851)) ([423be2f](https://github.com/cube-js/cube.js/commit/423be2f33d40ccd5681c47201586ac93944ac9dd))
* **cubesql:** Support Extract(DAY/DOW), Binary (?expr + ?literal_expr) for rewriting (Metabase) ([#4887](https://github.com/cube-js/cube.js/issues/4887)) ([2565705](https://github.com/cube-js/cube.js/commit/2565705fcff6a3d3dc4ff5ac2dcd819d8ad040db))
* **cubesql:** Support Substring for rewriting (Metabase) ([#4881](https://github.com/cube-js/cube.js/issues/4881)) ([8fadebd](https://github.com/cube-js/cube.js/commit/8fadebd7670e9f461a16e51e5114812933722ddd))





## [0.30.32](https://github.com/cube-js/cube.js/compare/v0.30.31...v0.30.32) (2022-07-07)


### Bug Fixes

* **cubesql:** Correct portal pagination (use PortalSuspended) in pg-wire ([#4872](https://github.com/cube-js/cube.js/issues/4872)) ([63aad19](https://github.com/cube-js/cube.js/commit/63aad191ea2be58291b0ce8709e1352a62cbd8a4))


### Features

* **cubesql:** Support grant tables (columns, tables) ([a3d9493](https://github.com/cube-js/cube.js/commit/a3d949324e1ac879606b45faed4812b30b07173b))





## [0.30.31](https://github.com/cube-js/cube.js/compare/v0.30.30...v0.30.31) (2022-07-07)


### Features

* **cubesql:** Initial support for canceling queries in pg-wire ([#4847](https://github.com/cube-js/cube.js/issues/4847)) ([bce0f99](https://github.com/cube-js/cube.js/commit/bce0f994d59a48f221cce3d21e3c2f3244e5f3a1))





## [0.30.30](https://github.com/cube-js/cube.js/compare/v0.30.29...v0.30.30) (2022-07-05)


### Bug Fixes

* **cubesql:** Invalid argument error: all columns in a record batch must have the same length ([895f8cf](https://github.com/cube-js/cube.js/commit/895f8cf301a951907aa4cd3ea190ea1cfeb3be73))


### Features

* **cubesql:** Superset ILIKE support for Search all filter options feature ([2532040](https://github.com/cube-js/cube.js/commit/2532040792faa9ed0a151d85cead1c1bd425d3ce))
* **cubesql:** Support for metabase literal queries ([#4843](https://github.com/cube-js/cube.js/issues/4843)) ([6d45d55](https://github.com/cube-js/cube.js/commit/6d45d558e0c58c37c515d07cae367eed5624cb3a))
* **cubesql:** Support Interval type for pg-wire ([4c8a82c](https://github.com/cube-js/cube.js/commit/4c8a82caf3b64c295bf7606e6a694f6cda50491c))





## [0.30.29](https://github.com/cube-js/cube.js/compare/v0.30.28...v0.30.29) (2022-07-01)


### Bug Fixes

* **cubesql:** Can't find rewrite due to timeout reached for bigger ORDER BY queries ([b765838](https://github.com/cube-js/cube.js/commit/b765838ff6c27ae34272feec00f1b60e7932b2c7))


### Features

* **cubesql:** Initial support for DBeaver ([#4831](https://github.com/cube-js/cube.js/issues/4831)) ([0a63152](https://github.com/cube-js/cube.js/commit/0a6315210fd7115f4649ec12a68a2d9b1479a23f))
* **cubesql:** Send parameters at once (initial handshake) for pg-wire ([#4812](https://github.com/cube-js/cube.js/issues/4812)) ([645253f](https://github.com/cube-js/cube.js/commit/645253f9b21ef08f7fc908e6577878f97b3ef6b0))
* **cubesql:** Support Date type in pg-wire (Date32, Date64) ([d0d08cf](https://github.com/cube-js/cube.js/commit/d0d08cf8ee848903a3b49849cace34046371a90f))
* **pg-srv:** Introduce ToProtocolValue trait (encoding) ([#4818](https://github.com/cube-js/cube.js/issues/4818)) ([4e35aee](https://github.com/cube-js/cube.js/commit/4e35aeec993cdeecab9d64fdb0392c33c35913e4))





## [0.30.28](https://github.com/cube-js/cube.js/compare/v0.30.27...v0.30.28) (2022-06-27)


### Bug Fixes

* **cubesql:** Correct sync behaviour for extended query in pg-wire ([#4815](https://github.com/cube-js/cube.js/issues/4815)) ([ee1362f](https://github.com/cube-js/cube.js/commit/ee1362f19fe1c36569109fc474c86f7ac9292ee5))





## [0.30.27](https://github.com/cube-js/cube.js/compare/v0.30.26...v0.30.27) (2022-06-24)


### Bug Fixes

* **cubesql:** Correct TransactionStatus for Sync in pg-wire ([90c6265](https://github.com/cube-js/cube.js/commit/90c62658fe076060161e6384e0b3dcc8e7e94dd4))
* **cubesql:** Return error on execute for unknown portal in pg-wire ([0b87261](https://github.com/cube-js/cube.js/commit/0b872614f30f5fd9b22c88916ad4edba604f8d02))
* **cubesql:** thread 'tokio-runtime-worker' panicked at 'called `Option::unwrap()` on a `None` value' in case of MEASURE() called on a dimension ([5d62c5a](https://github.com/cube-js/cube.js/commit/5d62c5af1562696ccb192c800ed2047b8345f8f8))


### Features

* **cubesql:** Metabase interval date range filter support ([#4763](https://github.com/cube-js/cube.js/issues/4763)) ([221715a](https://github.com/cube-js/cube.js/commit/221715adee2876585c639e8918dc0f171ad91a86))
* **cubesql:** Support Numeric type (text + binary) in pg-wire ([db7ec5c](https://github.com/cube-js/cube.js/commit/db7ec5c2d0a726b99daf014a70cdee8c15d3721b))
* **cubesql:** Workaround for Metabase introspection query ([ee7b3cf](https://github.com/cube-js/cube.js/commit/ee7b3cfd7401882bf802d668e5709e4f02c64be3))





## [0.30.26](https://github.com/cube-js/cube.js/compare/v0.30.25...v0.30.26) (2022-06-20)


### Features

* **cubesql:** Correct implementation for placeholder binder/finder in pg-wire ([fa018bd](https://github.com/cube-js/cube.js/commit/fa018bd62fd0d7f66c8aa0b68b43cc37d73d65ac))
* **cubesql:** Replace timestamptz CAST with timestamp ([9e7c1bd](https://github.com/cube-js/cube.js/commit/9e7c1bd69adae367a65f77339087194e7e1bc5fe))
* **cubesql:** Support Int8 for Bind + binary in pg-wire ([f28fbd5](https://github.com/cube-js/cube.js/commit/f28fbd5049e0a72adbb0f078e45728d60b481ca2))
* **cubesql:** Support placeholders in `WITH` and `LIMIT` ([#4768](https://github.com/cube-js/cube.js/issues/4768)) ([d444c0f](https://github.com/cube-js/cube.js/commit/d444c0fda31b3cdf824e85c3f03d76d8a3f47211))
* **cubesql:** Workaround CTEs with subqueries (Sigma) ([#4767](https://github.com/cube-js/cube.js/issues/4767)) ([d99a02f](https://github.com/cube-js/cube.js/commit/d99a02f508418c9a054977572da0985f627acfc3))





## [0.30.25](https://github.com/cube-js/cube.js/compare/v0.30.24...v0.30.25) (2022-06-16)


### Features

* logging cubesql queries errors ([#4550](https://github.com/cube-js/cube.js/issues/4550)) ([10021c3](https://github.com/cube-js/cube.js/commit/10021c34f28348183fd30584d8bb97a97103b91e))
* **cubesql:** PowerBI support for wrapped queries ([#4752](https://github.com/cube-js/cube.js/issues/4752)) ([fc129d4](https://github.com/cube-js/cube.js/commit/fc129d4364ea89ea32aa903cda9499133959fdbe))





## [0.30.20](https://github.com/cube-js/cube.js/compare/v0.30.19...v0.30.20) (2022-06-11)


### Bug Fixes

* **cubesql:** Send `Empty Query` message on empty query ([88e966d](https://github.com/cube-js/cube.js/commit/88e966d12e31e6277ac02bf9a1b44cd7c8722311))


### Features

* **cubesql:** Support pg_catalog.pg_roles table ([eed0727](https://github.com/cube-js/cube.js/commit/eed0727fe70b9fddfaddf8c32821fc721c911ae8))
* **cubesql:** Support pg_my_temp_schema, pg_is_other_temp_schema UDFs ([c843491](https://github.com/cube-js/cube.js/commit/c843491c834204231424a21bc8a89b18336cc68a))





## [0.30.18](https://github.com/cube-js/cube.js/compare/v0.30.17...v0.30.18) (2022-06-10)


### Bug Fixes

* **cubesql:** Simple query: fetch in pg-wire (ODBC) ([fc7c0e0](https://github.com/cube-js/cube.js/commit/fc7c0e0f46000c68a64a1c6d1c635a56ab84d51e))





## [0.30.17](https://github.com/cube-js/cube.js/compare/v0.30.16...v0.30.17) (2022-06-09)


### Bug Fixes

* **cubesql:** Simple query: commit/rollback in pg-wire ([#4743](https://github.com/cube-js/cube.js/issues/4743)) ([3e03870](https://github.com/cube-js/cube.js/commit/3e03870545fb916d434a610d69c0a56a597d7e70))


### Features

* **cubesql:** Add Postgres `pg_database` meta layer table ([64e65eb](https://github.com/cube-js/cube.js/commit/64e65eb622241f77c150dbfca687186d05f3432e))
* **cubesql:** add support public compount identifier in filters ([#4742](https://github.com/cube-js/cube.js/issues/4742)) ([74aaef6](https://github.com/cube-js/cube.js/commit/74aaef6c114e5ab4918d54c215b0ed05c27999a4))
* **cubesql:** Workarounds for Tableau Desktop (ODBC) ([951c4b5](https://github.com/cube-js/cube.js/commit/951c4b5c807c28818b76aa6fc26880b4654bff0a))





## [0.30.16](https://github.com/cube-js/cube.js/compare/v0.30.15...v0.30.16) (2022-06-08)


### Bug Fixes

* **cubesql:** Allow binary encoding for all types in pg-wire ([d456745](https://github.com/cube-js/cube.js/commit/d4567451c40c168076ef86ed055052f5490723c4))
* **cubesql:** TIMESTAMP/TZ was wrong in some BIs (pg-wire) ([dfdb5ff](https://github.com/cube-js/cube.js/commit/dfdb5ffe611d0978258a5ae3eb3354366cd1f346))





## [0.30.14](https://github.com/cube-js/cube.js/compare/v0.30.13...v0.30.14) (2022-06-06)


### Features

* **cubesql:** Auto-closing hold cursos on transaction end (simple query) ([79725ec](https://github.com/cube-js/cube.js/commit/79725ec3b02abde6d9cf3f5d3e45e60518a8386f))
* **cubesql:** cast DECIMAL with default precision and scale ([#4709](https://github.com/cube-js/cube.js/issues/4709)) ([771d179](https://github.com/cube-js/cube.js/commit/771d1797f4084fff68f0291c55a37b25b32fb5e2))
* **cubesql:** Support CAST for name, int2/4/8 ([#4711](https://github.com/cube-js/cube.js/issues/4711)) ([36fe891](https://github.com/cube-js/cube.js/commit/36fe891fd102c165eb28b0c5561151934751f143))
* **cubesql:** Support CLOSE [name | ALL] (cursors) for pg-wire ([#4712](https://github.com/cube-js/cube.js/issues/4712)) ([91048bd](https://github.com/cube-js/cube.js/commit/91048bd48ddf755b436f41a1bdfff8b24d4bf5f5))
* **cubesql:** Support Metabase pg_type introspection query ([2401dbf](https://github.com/cube-js/cube.js/commit/2401dbf9e5b5de75c5f7cf31e3586135a0a016e5))





## [0.30.13](https://github.com/cube-js/cube.js/compare/v0.30.12...v0.30.13) (2022-06-05)


### Features

* **cubesql:** PowerBI is not empty filter ([e31ffdc](https://github.com/cube-js/cube.js/commit/e31ffdcd762236fb54d454ede7e892acb54bdcee))





## [0.30.11](https://github.com/cube-js/cube.js/compare/v0.30.10...v0.30.11) (2022-06-03)


### Bug Fixes

* **cubesql:** array_lower, array_upper - correct behaviour ([#4677](https://github.com/cube-js/cube.js/issues/4677)) ([a3f29d4](https://github.com/cube-js/cube.js/commit/a3f29d4df9fc85e53406101bb73b7a7281a60846))


### Features

* **cubesql:** Add `pg_catalog.pg_matviews` meta layer table ([2fbc5f4](https://github.com/cube-js/cube.js/commit/2fbc5f43de312a85967dbd8be79bd92ee04141a7))
* **cubesql:** PowerBI contains filter support ([#4646](https://github.com/cube-js/cube.js/issues/4646)) ([3cbd753](https://github.com/cube-js/cube.js/commit/3cbd753b47dc1a20f3fede11bf0c01b784504869))
* **cubesql:** Support `[NOT] ILIKE` operator ([96b05c8](https://github.com/cube-js/cube.js/commit/96b05c843588aa96a935e5491667d77b3f456b82))
* **cubesql:** Support ArrayIndex for scalars ([419689e](https://github.com/cube-js/cube.js/commit/419689e7d341e287596455d5c94b8225d627798b))





## [0.30.10](https://github.com/cube-js/cube.js/compare/v0.30.9...v0.30.10) (2022-06-01)


### Bug Fixes

* **cubesql:** Handle `Flush` pg-wire message ([f779e75](https://github.com/cube-js/cube.js/commit/f779e75fb4e6ba5a12d7b751c5f53313d33753bc))
* **cubesql:** Store description on Portal in Finished state ([f5f6566](https://github.com/cube-js/cube.js/commit/f5f65663cb01fdbc222e38ec8d3fb6813d5466ae))


### Features

* **cubesql:** information_schema.constraint_column_usage meta table ([1fe8312](https://github.com/cube-js/cube.js/commit/1fe83127b2601bf7ef9f3b63ff63b4026958e8c8))
* **cubesql:** information_schema.views meta table ([490d721](https://github.com/cube-js/cube.js/commit/490d721b4bcd90ae059996bdac177d707935f58e))
* **cubesql:** Support ANY expressions ([77e0672](https://github.com/cube-js/cube.js/commit/77e06727a2a4039d7297538d9bace9498a0fc1a2))
* **cubesql:** Support current_database(), current_schema(), current_user for pg-wire ([a18f68c](https://github.com/cube-js/cube.js/commit/a18f68c8a6538c38c8985b512996a9fec2292da2))
* **cubesql:** Support string for NULLIF (metabase pg_class query) ([#4638](https://github.com/cube-js/cube.js/issues/4638)) ([ef962e7](https://github.com/cube-js/cube.js/commit/ef962e71fe9955c359a044ea83736cac1748c4a4))
* Initial support for FETCH/DECLARE (cursors) for simple query in pg-wire ([#4601](https://github.com/cube-js/cube.js/issues/4601)) ([b160773](https://github.com/cube-js/cube.js/commit/b160773d9a208c2b794a34e6e36f4ce73a83a53e))





## [0.30.9](https://github.com/cube-js/cube.js/compare/v0.30.8...v0.30.9) (2022-05-31)


### Bug Fixes

* **cubesql:** Allow `CASE` with `pg_attribute.atttypmod` offset ([fc09160](https://github.com/cube-js/cube.js/commit/fc091609e6f3512d5a078501279e8b9064048b54))


### Features

* **cubesql:** Support comparison between strings and booleans ([#4618](https://github.com/cube-js/cube.js/issues/4618)) ([e4352c3](https://github.com/cube-js/cube.js/commit/e4352c3930e6c948e98bae764920f5d6e21103e8))





## [0.30.8](https://github.com/cube-js/cube.js/compare/v0.30.7...v0.30.8) (2022-05-30)


### Bug Fixes

* **cubesql:** Empty results on `JOIN` with `AND` + `OR` in `WHERE` ([#4608](https://github.com/cube-js/cube.js/issues/4608)) ([96c2f15](https://github.com/cube-js/cube.js/commit/96c2f157f03b95106b509b677fc3d4d6af36b0a2))
* **cubesql:** fix log error standalone ([#4606](https://github.com/cube-js/cube.js/issues/4606)) ([3e3e010](https://github.com/cube-js/cube.js/commit/3e3e010403dc83ca34f7b2ca95c7b46a2a2f1e2d))


### Features

* **cubesql:** Allow `::information_schema.cardinal_number` casting ([b198fb3](https://github.com/cube-js/cube.js/commit/b198fb3a70b3d075ccdfaff638dc8f36e6530944))
* **cubesql:** excel subquery column with same name ([#4602](https://github.com/cube-js/cube.js/issues/4602)) ([ea3a0bc](https://github.com/cube-js/cube.js/commit/ea3a0bc4a944cd724672056f5885110c7cee90cd))
* **cubesql:** PowerBI basic queries support ([455ae07](https://github.com/cube-js/cube.js/commit/455ae076880f305ed73d1d217a87f908837070f5))
* **cubesql:** Support array_upper, array_lower UDFs ([5a3b6bb](https://github.com/cube-js/cube.js/commit/5a3b6bb31c5af920c706b56a8e3c5046f272f8ca))
* **cubesql:** Support to_char UDF ([#4600](https://github.com/cube-js/cube.js/issues/4600)) ([48077a9](https://github.com/cube-js/cube.js/commit/48077a95fccf48309085e6f1f9b2652c581ab3a3))





## [0.30.7](https://github.com/cube-js/cube.js/compare/v0.30.6...v0.30.7) (2022-05-26)


### Bug Fixes

* **cubesql:** Correct command completion for SET in pg-wire ([ab42e54](https://github.com/cube-js/cube.js/commit/ab42e54b2c49aea63d4db75e9332655159fa73e6))


### Features

* **cubesql:** Support escaped string literals, E'str' ([ef9700d](https://github.com/cube-js/cube.js/commit/ef9700d8f7a1ccd0a31aeece70fdcecee092eb9f))
* **cubesql:** Support multiple stmts for simple query in pg-wire ([0f645cb](https://github.com/cube-js/cube.js/commit/0f645cbd0a4bf25d0a03a14d366607ae716fc792))





## [0.30.6](https://github.com/cube-js/cube.js/compare/v0.30.5...v0.30.6) (2022-05-24)


### Bug Fixes

* **cubesql:** Normalize column names for joins and aliased columns ([7faadc9](https://github.com/cube-js/cube.js/commit/7faadc9c96d4cb80b7318a1955cd01e854ca2272))


### Features

* **cubesql:** Support `_pg_truetypid`, `_pg_truetypmod` UDFs ([1436a76](https://github.com/cube-js/cube.js/commit/1436a76c71e7cec8a62149def9fc2de39a48acef))





## [0.30.4](https://github.com/cube-js/cube.js/compare/v0.30.3...v0.30.4) (2022-05-20)


### Bug Fixes

* **cubesql:** Skip returning of schema for special queries in pg-wire ([479ec78](https://github.com/cube-js/cube.js/commit/479ec78836cc095dda8c3725e1378b9f60f56233))
* **cubesql:** Wrong format in RowDescription, support i16/i32/f32 ([0c52cd6](https://github.com/cube-js/cube.js/commit/0c52cd6180e7cf43aeb735ec901da07508ff4598))


### Features

* **cubesql:** Allow ::oid casting ([bb31838](https://github.com/cube-js/cube.js/commit/bb318383028ce9557ccd45ae03cd33f05705bff2))
* **cubesql:** Initial support for type receivers ([452f504](https://github.com/cube-js/cube.js/commit/452f504b7c57d6c669de2eabba935c7a398aa7d2))
* **cubesql:** Support ||, correct schema/catalog/ordinal_position ([6d6cbf5](https://github.com/cube-js/cube.js/commit/6d6cbf5ee743e527d8b9f64008cdc0d12103abf6))
* **cubesql:** Support DISCARD [ALL | PLANS | SEQUENCES | TEMPORARY |… ([#4560](https://github.com/cube-js/cube.js/issues/4560)) ([390c764](https://github.com/cube-js/cube.js/commit/390c764a98fb58fc294cdfe08ed224f2318e1b31))
* **cubesql:** Support IS TRUE|FALSE ([4d227b1](https://github.com/cube-js/cube.js/commit/4d227b11cbe93352d735c81b50da79f256266bb9))





## [0.30.3](https://github.com/cube-js/cube.js/compare/v0.30.2...v0.30.3) (2022-05-17)


### Bug Fixes

* **cubesql:** Add support for all types to `pg_catalog.format_type` UDF ([c49c55a](https://github.com/cube-js/cube.js/commit/c49c55a213efba8da49f2e53cc36a8c8fd9cd64e))
* **cubesql:** Coerce empty subquery result to `NULL` ([e59d2fb](https://github.com/cube-js/cube.js/commit/e59d2fb367f99deea3463316d87ee9eb5ae59463))
* **cubesql:** Fix several UDFs to return correct row amount ([f1e0223](https://github.com/cube-js/cube.js/commit/f1e02239962965f6d246eed53a81c756cbc3a24d))


### Features

* **cubesql:** Ignore `pg_catalog` schema for UDFs ([ab2a0da](https://github.com/cube-js/cube.js/commit/ab2a0da0cdf2ec3cd9974dcb1a532c2ccfad4851))





## [0.30.2](https://github.com/cube-js/cube.js/compare/v0.30.1...v0.30.2) (2022-05-16)


### Features

* **cubesql:** Superset Postgres protocol support ([#4535](https://github.com/cube-js/cube.js/issues/4535)) ([394248f](https://github.com/cube-js/cube.js/commit/394248fa8a10dfd568721405e4a8f392d236d551))





## [0.30.1](https://github.com/cube-js/cube.js/compare/v0.30.0...v0.30.1) (2022-05-14)


### Features

* **cubesql:** Add CUBEJS_PG_SQL_PORT env support and SQL API reference docs ([#4531](https://github.com/cube-js/cube.js/issues/4531)) ([de60d71](https://github.com/cube-js/cube.js/commit/de60d71c360be47e3231e7eafa349b9a0fddd244))
* **cubesql:** Provide specific error messages for not matched expressions ([e035780](https://github.com/cube-js/cube.js/commit/e0357801bd39269585dd31d6ad932b32287a05af))
* **cubesql:** Support `quarter` field in `date_part` SQL function ([7fdf4ac](https://github.com/cube-js/cube.js/commit/7fdf4acf6ce60387d3fa716c572e1611a77c205b))





# [0.30.0](https://github.com/cube-js/cube.js/compare/v0.29.57...v0.30.0) (2022-05-11)


### Features

* **cubesql:** Support dynamic key in ArrayIndex expression ([#4504](https://github.com/cube-js/cube.js/issues/4504)) ([115dd55](https://github.com/cube-js/cube.js/commit/115dd55ed390b8617d592add832b1aefde636265))





## [0.29.57](https://github.com/cube-js/cube.js/compare/v0.29.56...v0.29.57) (2022-05-11)


### Bug Fixes

* **cubesql:** Fix format_type udf usage with tables ([a49b2b4](https://github.com/cube-js/cube.js/commit/a49b2b44c10e4da42443cfd948404d2bc60671ec))
* **cubesql:** Reject `SELECT INTO` queries gracefully ([8b67ff7](https://github.com/cube-js/cube.js/commit/8b67ff7d0de1c5ca0b3852a342931d348ec2422c))





## [0.29.56](https://github.com/cube-js/cube.js/compare/v0.29.55...v0.29.56) (2022-05-06)


### Features

* **cubesql:** Correct support for regclass in CAST expr ([#4499](https://github.com/cube-js/cube.js/issues/4499)) ([cdab58a](https://github.com/cube-js/cube.js/commit/cdab58abeb4251c45e0365ff2a8584c9094f6d4d))
* **cubesql:** More descriptive error messages ([812db77](https://github.com/cube-js/cube.js/commit/812db772a651e0df1f7bc0d1dba97192c65ea834))
* **cubesql:** Partial support for Tableau's table_cat query ([#4466](https://github.com/cube-js/cube.js/issues/4466)) ([f1956d3](https://github.com/cube-js/cube.js/commit/f1956d3240bf067e1ecbee0997303ae76ab3fcaa))
* **cubesql:** Support pg_catalog.pg_enum postgres table ([2db445a](https://github.com/cube-js/cube.js/commit/2db445a120832390dd2577192597e30768b29918))
* **cubesql:** Support pg_get_constraintdef UDF ([#4487](https://github.com/cube-js/cube.js/issues/4487)) ([7a3018d](https://github.com/cube-js/cube.js/commit/7a3018d24326124b5e9257264a11fa09bc565f57))
* **cubesql:** Support pg_type_is_visible postgres udf ([47fc285](https://github.com/cube-js/cube.js/commit/47fc285c07c9633bee2482ef246f5436dd79dff3))





## [0.29.55](https://github.com/cube-js/cube.js/compare/v0.29.54...v0.29.55) (2022-05-04)


### Bug Fixes

* **cubesql:** Correct handling for boolean type ([cff6c8b](https://github.com/cube-js/cube.js/commit/cff6c8b4d69c9b8bade7ce1e6e2f2502a44f3918))
* **cubesql:** Tableau new regclass query fast fix ([2a7ff1e](https://github.com/cube-js/cube.js/commit/2a7ff1e20fc79dccd9cff94e6225d657569ed06e))


### Features

* **cubesql:** Tableau cubes without count measure support ([931e2f5](https://github.com/cube-js/cube.js/commit/931e2f5fb5fa29b19347b7858a8b4f892162f169))





## [0.29.54](https://github.com/cube-js/cube.js/compare/v0.29.53...v0.29.54) (2022-05-03)


### Bug Fixes

* **cubesql:** Using same alias on column yields Option.unwrap() panic ([a674c5f](https://github.com/cube-js/cube.js/commit/a674c5f98f8c643ed407fcf1cac528c797c43746))


### Features

* **cubesql:** Tableau boolean filters support ([33aa5f1](https://github.com/cube-js/cube.js/commit/33aa5f138b44ccf60afc6e562b9bf71c2fe6257c))
* **cubesql:** Tableau cast projection queries support ([71ec644](https://github.com/cube-js/cube.js/commit/71ec64444e182a0a1c92818d655b40f78e463684))
* **cubesql:** Tableau contains support ([71dcad0](https://github.com/cube-js/cube.js/commit/71dcad091dc8e60958c717bd01e07db050abf8af))
* **cubesql:** Tableau min max number dimension support ([2abe13e](https://github.com/cube-js/cube.js/commit/2abe13e3155ad03ec3837da38bd465fbee0eb2f9))
* **cubesql:** Tableau not null filter support ([d48d0e0](https://github.com/cube-js/cube.js/commit/d48d0e03d05559413ddcff0ce980f6cf96cd24bc))
* **cubesql:** Tableau week support ([6d987ea](https://github.com/cube-js/cube.js/commit/6d987ea6062a90843084b72b254d068f46e26601))





## [0.29.53](https://github.com/cube-js/cube.js/compare/v0.29.52...v0.29.53) (2022-04-29)


### Bug Fixes

* **cubesql:** fix pg_constraint confkey type ([#4462](https://github.com/cube-js/cube.js/issues/4462)) ([82c25fd](https://github.com/cube-js/cube.js/commit/82c25fd98961a4130607cd1b93049d9b6f3093e7))


### Features

* **cubesql:** Aggregate aggregate split to support Tableau extract date part queries ([532b4ee](https://github.com/cube-js/cube.js/commit/532b4eece185dce8bfd5de46325105b45d50f621))
* **cubesql:** Projection aggregate split to support Tableau casts ([#4435](https://github.com/cube-js/cube.js/issues/4435)) ([1550774](https://github.com/cube-js/cube.js/commit/1550774acf2dd208d7222bb7b4742dcc64ca4b89))
* **cubesql:** Support for pg_get_userbyid, pg_table_is_visible UDFs ([64f8885](https://github.com/cube-js/cube.js/commit/64f8885806d9034cb55b828d37193d5540829a6a))
* **cubesql:** Support generate_subscripts UDTF ([a29551a](https://github.com/cube-js/cube.js/commit/a29551a402f323541a1b10523f3478f9ae284989))
* **cubesql:** Support get_expr query for Pg/Tableau ([#4421](https://github.com/cube-js/cube.js/issues/4421)) ([4d4918f](https://github.com/cube-js/cube.js/commit/4d4918fd9ff73c4d642416c74d720e5a85e2a87a))
* **cubesql:** Support information_schema._pg_expandarray postgres UDTF ([#4439](https://github.com/cube-js/cube.js/issues/4439)) ([1af4290](https://github.com/cube-js/cube.js/commit/1af4290a9d35a67e62c21acc3edc0536ce15c694))
* **cubesql:** Support pg_catalog.pg_am table ([24b231d](https://github.com/cube-js/cube.js/commit/24b231d45d355c0c01425157a41db1f7ac65b80a))
* **cubesql:** Support Timestamp, TimestampTZ for pg-wire ([0b38b3d](https://github.com/cube-js/cube.js/commit/0b38b3d594999bf5f165295ba9643998004beb81))
* **cubesql:** Support unnest UDTF ([110bdf8](https://github.com/cube-js/cube.js/commit/110bdf8de390bf82c604aeab0dacafaae4b0eda8))
* **cubesql:** Tableau default having support ([4d432c0](https://github.com/cube-js/cube.js/commit/4d432c0b12d2ed75488d723304aa999554f7ee54))
* **cubesql:** Tableau Min, Max timestamp queries support ([48ee34e](https://github.com/cube-js/cube.js/commit/48ee34efb9c7a1a3feaae8fa0e091a84c18b4736))
* **cubesql:** Tableau range of dates support ([ef56133](https://github.com/cube-js/cube.js/commit/ef5613307996cf5b3973af366f625ca78bcb2dbd))
* **cubesql:** Tableau relative date range support ([87a3817](https://github.com/cube-js/cube.js/commit/87a381705dcfaa3e3c3841bdb66b2b6f0535d8ca))
* **cubesql:** Unwrap filter casts for Tableau ([0a39420](https://github.com/cube-js/cube.js/commit/0a3942038d12a357d9af13941311af7cbcc87830))





## [0.29.51](https://github.com/cube-js/cube.js/compare/v0.29.50...v0.29.51) (2022-04-22)


### Bug Fixes

* **cubesql:** Bool encoding for text format in pg-wire ([7faf34b](https://github.com/cube-js/cube.js/commit/7faf34b4dee421202528aa2e9985acbfcc8da6b9))
* **cubesql:** current_schema() UDF ([69a75dc](https://github.com/cube-js/cube.js/commit/69a75dc3fe29be97eecf2f0eeb97a642a2328212))
* **cubesql:** Proper handling for Postgresql table reference ([35f5635](https://github.com/cube-js/cube.js/commit/35f56350f39f22665e71fa53a1e6fc5d7bb02262))


### Features

* **cubesql:** Correlated subqueries support for introspection queries ([#4408](https://github.com/cube-js/cube.js/issues/4408)) ([1f02b2c](https://github.com/cube-js/cube.js/commit/1f02b2c363becb046ae5b94833a46a7091e572ad))
* **cubesql:** Implement rewrites for SELECT * FROM WHERE 1=0 ([#4427](https://github.com/cube-js/cube.js/issues/4427)) ([0c9abd1](https://github.com/cube-js/cube.js/commit/0c9abd1bde7c5492c42340f75e020dc09228908b))
* **cubesql:** Support arrays in pg-wire ([b7925ba](https://github.com/cube-js/cube.js/commit/b7925ba703d245115321fb6b399eb71efec71cab))
* **cubesql:** Support generate_series UDTF ([#4416](https://github.com/cube-js/cube.js/issues/4416)) ([3321925](https://github.com/cube-js/cube.js/commit/33219254319b13e2d7ef97fd81eedb01a198123c))
* **cubesql:** Support GetIndexedFieldExpr rewrites ([#4424](https://github.com/cube-js/cube.js/issues/4424)) ([8dca8b5](https://github.com/cube-js/cube.js/commit/8dca8b50ea67f2e5e562e1bb69b5375de55a3b48))
* **cubesql:** Support information_schema._pg_datetime_precision UDF ([4d20ee6](https://github.com/cube-js/cube.js/commit/4d20ee61410d439fc17ddc204afb9e855705c7b7))
* **cubesql:** Support information_schema._pg_numeric_precision UDF ([6fc6c0a](https://github.com/cube-js/cube.js/commit/6fc6c0a22c57f4fa38ace1f5183e2d9e3eb7afde))
* **cubesql:** Support information_schema._pg_numeric_scale UDF ([398d1db](https://github.com/cube-js/cube.js/commit/398d1dba9736ff1059bc328c3ab881cdb9ad1650))
* **cubesql:** Support lc_collate for PostgreSQL ([120ce31](https://github.com/cube-js/cube.js/commit/120ce3145447e1df034ac412fa20471ff674c893))
* **cubesql:** Support NoData response for empty response in pg-wire ([6711c8a](https://github.com/cube-js/cube.js/commit/6711c8aa39bbcff0e36db763c4c7f1a37b838a5c))
* **cubesql:** Support pg_get_expr UDF ([#4425](https://github.com/cube-js/cube.js/issues/4425)) ([2b51d70](https://github.com/cube-js/cube.js/commit/2b51d70e4aafb5e2531df2a39293803cbf33b195))
* **cubesql:** Support pg_get_userbyid UDF ([c6efef8](https://github.com/cube-js/cube.js/commit/c6efef83736f2b1733f40f07f315d51574f6d371))
* **cubesql:** Use proper command completion tags for pg-wire ([3e777ec](https://github.com/cube-js/cube.js/commit/3e777ec2926d3e6c2f174ff94b6ac50ae5e2593a))





## [0.29.50](https://github.com/cube-js/cube.js/compare/v0.29.49...v0.29.50) (2022-04-18)


### Features

* **cubesql:** Initial support for Binary format in pg-wire ([a36845c](https://github.com/cube-js/cube.js/commit/a36845c5edcb6bd77172de2cebcd67a700df5224))
* **cubesql:** Support Describe(Portal) for pg-wire ([34cf111](https://github.com/cube-js/cube.js/commit/34cf111249c3fede986c3633fe8d5f0cade3ed91))
* **cubesql:** Support pg_depend postgres table ([ceb35d4](https://github.com/cube-js/cube.js/commit/ceb35d4825cd2ac76ad191eef950ab3be126c3de))





## [0.29.48](https://github.com/cube-js/cube.js/compare/v0.29.47...v0.29.48) (2022-04-14)


### Bug Fixes

* **cubesql:** Support pg_catalog.format_type through fully qualified name ([9eafae0](https://github.com/cube-js/cube.js/commit/9eafae0c4eafc2ad1d8517be9dbf292c1650c64a))


### Features

* **cubesql:** Initial support for prepared statements in pg-wire ([#4244](https://github.com/cube-js/cube.js/issues/4244)) ([912b52a](https://github.com/cube-js/cube.js/commit/912b52a5cb8d72820c68843e15a2ef83233b952f))
* **cubesql:** Postgres Apache Superset connection flow support ([ab256d9](https://github.com/cube-js/cube.js/commit/ab256d9fc31fd4d2bc08c969b374cec449e34bae))





## [0.29.47](https://github.com/cube-js/cube.js/compare/v0.29.46...v0.29.47) (2022-04-12)


### Bug Fixes

* **cubesql:** Correct MySQL types in response headers ([#4362](https://github.com/cube-js/cube.js/issues/4362)) ([c507f82](https://github.com/cube-js/cube.js/commit/c507f82fbdd92363d27c4b3c8b41957bd62a3d87))
* **cubesql:** Special handling for bool as string ([3ba27bf](https://github.com/cube-js/cube.js/commit/3ba27bf7ee91aef69eb75c33580abf18b21bd29e))
* **cubesql:** Support boolean (ColumnType) for MySQL protocol ([23f8367](https://github.com/cube-js/cube.js/commit/23f8367f6657b8d7f31e4e34a4547d30c3c34c79))





## [0.29.46](https://github.com/cube-js/cube.js/compare/v0.29.45...v0.29.46) (2022-04-11)


### Bug Fixes

* **cubesql:** Rewrite engine decimal measure support ([8a0fa98](https://github.com/cube-js/cube.js/commit/8a0fa981b87b67281867c6073903fa9bb6826570))


### Features

* **cubesql:** Support format_type UDF for Postgres ([#4325](https://github.com/cube-js/cube.js/issues/4325)) ([8b972ca](https://github.com/cube-js/cube.js/commit/8b972ca9bfd46cc8d43a93ce04e696624838fbde))





## [0.29.45](https://github.com/cube-js/cube.js/compare/v0.29.44...v0.29.45) (2022-04-09)


### Bug Fixes

* **cubesql:** Rewrite engine datafusion after rebase regressions: mismatched to_day_interval signature, projection aliases, order by date. ([8310f7e](https://github.com/cube-js/cube.js/commit/8310f7e1d4b7c2c28b6d2e7f0fb683114c837282))





## [0.29.44](https://github.com/cube-js/cube.js/compare/v0.29.43...v0.29.44) (2022-04-07)

**Note:** Version bump only for package @cubejs-backend/cubesql





## [0.29.43](https://github.com/cube-js/cube.js/compare/v0.29.42...v0.29.43) (2022-04-07)


### Bug Fixes

* **cubesql:** Rewrites don't respect projection column order ([cfe35a7](https://github.com/cube-js/cube.js/commit/cfe35a7b65390db43f1e7c68ac54c82c2ec8af49))


### Features

* **cubesql:** Rewrite engine error handling ([3fba823](https://github.com/cube-js/cube.js/commit/3fba823bc561d7a985c89c4cf437a6595ef88a7c))
* **cubesql:** Upgrade rust to 1.61.0-nightly (2022-02-22) ([c836065](https://github.com/cube-js/cube.js/commit/c8360658ccb8e5e3e6cfcd62da2d156b44ee8456))





## [0.29.42](https://github.com/cube-js/cube.js/compare/v0.29.41...v0.29.42) (2022-04-04)


### Bug Fixes

* **cubesql:** Allow quoted variables with SHOW <variable> syntax ([#4313](https://github.com/cube-js/cube.js/issues/4313)) ([3eece0e](https://github.com/cube-js/cube.js/commit/3eece0e70817b2b72406b146a95a5757cdfb994c))


### Features

* **cubesql:** Rewrite engine segments support ([48b0767](https://github.com/cube-js/cube.js/commit/48b0767aa880a2373d6a6fa15b2e0a1815bb1865))





## [0.29.40](https://github.com/cube-js/cube.js/compare/v0.29.39...v0.29.40) (2022-04-03)


### Bug Fixes

* **cubesql:** Table columns should take precedence over projection to mimic MySQL and Postgres behavior ([60d6e45](https://github.com/cube-js/cube.js/commit/60d6e4511267b132df204fe7b637953d81e5f980))





## [0.29.38](https://github.com/cube-js/cube.js/compare/v0.29.37...v0.29.38) (2022-04-01)


### Bug Fixes

* **cubesql:** Deallocate statement on specific command in MySQL protocol ([ab3f36c](https://github.com/cube-js/cube.js/commit/ab3f36c182f603f348115463926e1cbe8ee40fd6))
* **cubesql:** Enable EXPLAIN support for postgres ([c0244d1](https://github.com/cube-js/cube.js/commit/c0244d10bd43647c6e6f562be3e30ec3d1ae66d9))


### Features

* **cubesql:** Initial support for current_schemas() postgres function ([e0907ff](https://github.com/cube-js/cube.js/commit/e0907ffa0a03b985ac2fe1cb9d592a47a6141d20))
* **cubesql:** Postgres pg_catalog.pg_class MetaLayer table ([#4287](https://github.com/cube-js/cube.js/issues/4287)) ([d70da08](https://github.com/cube-js/cube.js/commit/d70da08a345f857b09e60333d324e722e6619684))
* **cubesql:** Support binding values for prepared statements (MySQL only) ([ad26dc5](https://github.com/cube-js/cube.js/commit/ad26dc55274cc060300525bfd6238df82ffd782c))
* **cubesql:** Support current_schema() postgres function ([44b64ce](https://github.com/cube-js/cube.js/commit/44b64ce97d46f94eff057d0f7fd182e969bbbea0))
* **cubesql:** Support information_schema.character_sets (postgres) table ([1804b79](https://github.com/cube-js/cube.js/commit/1804b792381b044a5ee41b60cd72d4d02c1cf945))
* **cubesql:** Support information_schema.key_column_usage (postgres) table ([84cf2c1](https://github.com/cube-js/cube.js/commit/84cf2c1791f56dee0f240faa23e70ae4dd4e013a))
* **cubesql:** Support information_schema.referential_constraints (postgres) table ([eeb42be](https://github.com/cube-js/cube.js/commit/eeb42bec4d8635a647c638857bfa12f5f3518f1c))
* **cubesql:** Support information_schema.table_constraints (postgres) table ([2d6bfee](https://github.com/cube-js/cube.js/commit/2d6bfee1ee8d7bdc9750f32e85ec6f027ef988f0))
* **cubesql:** Support pg_catalog.pg_description and pg_catalog.pg_constraint MetaLayer tables ([#4292](https://github.com/cube-js/cube.js/issues/4292)) ([0ea9699](https://github.com/cube-js/cube.js/commit/0ea969930c5821589033595cb749d8acd64d991b))
* MySQL SET variables / Postgres SHOW SET variables ([#4266](https://github.com/cube-js/cube.js/issues/4266)) ([88ec3cc](https://github.com/cube-js/cube.js/commit/88ec3ccdf6582129d164bfcd3b0486b7f90cd923))
* **cubesql:** Postgres pg_catalog.pg_proc MetaLayer table ([#4289](https://github.com/cube-js/cube.js/issues/4289)) ([b3613d0](https://github.com/cube-js/cube.js/commit/b3613d08e4b906f61f5433c3a40a03eab1f0b297))
* **cubesql:** Support pg_catalog.pg_attrdef table ([d6aae8d](https://github.com/cube-js/cube.js/commit/d6aae8da32de339fefc232ba83818a15c4b01872))
* **cubesql:** Support pg_catalog.pg_attribute table ([d5f7d0c](https://github.com/cube-js/cube.js/commit/d5f7d0cab2df0b4b5eb6e1308b4347552ff3494d))
* **cubesql:** Support pg_catalog.pg_index table ([a621532](https://github.com/cube-js/cube.js/commit/a6215327a61439eb4293b7182de148ec425716fe))





## [0.29.37](https://github.com/cube-js/cube.js/compare/v0.29.36...v0.29.37) (2022-03-29)


### Bug Fixes

* **cubesql:** Dropping session on close for pg-wire ([#4280](https://github.com/cube-js/cube.js/issues/4280)) ([c4442be](https://github.com/cube-js/cube.js/commit/c4442be153160b864fafd34a4f0769dce9117fa4))
* **cubesql:** Rewrite engine can't parse `db` prefixed table names ([b7d9382](https://github.com/cube-js/cube.js/commit/b7d93827750b8d72e871abd527f1c0a649e5e6c2))
* **cubesql:** Rewrite engine: support for stacked time series charts ([c1add2c](https://github.com/cube-js/cube.js/commit/c1add2c9c52d1cd884dfefe4db978a853a76c83e))


### Features

* **cubesql:** Global Meta Tables ([88db9ea](https://github.com/cube-js/cube.js/commit/88db9eab3854a89cd93cfdce3a9fad9a180f3b45))
* **cubesql:** Global Meta Tables - add tests ([42e9517](https://github.com/cube-js/cube.js/commit/42e9517d1ac1673622bb9b03352af94f8ec968ba))
* **cubesql:** Global Meta Tables - cargo fmt ([c8336d9](https://github.com/cube-js/cube.js/commit/c8336d92a7867fd2b78e1542d61b42519fa9e3f2))
* **cubesql:** Support pg_catalog.pg_range table ([625c03a](https://github.com/cube-js/cube.js/commit/625c03ae965b2730d924812a7d16aec3fbdf5369))





## [0.29.36](https://github.com/cube-js/cube.js/compare/v0.29.35...v0.29.36) (2022-03-27)


### Features

* **cubesql:** Improve Postgres, MySQL meta layer ([#4228](https://github.com/cube-js/cube.js/issues/4228)) ([5c8d002](https://github.com/cube-js/cube.js/commit/5c8d002d1efc8cb6a57849389b87e7cb4ec187f0))
* **cubesql:** Rewrite engine first steps ([#4132](https://github.com/cube-js/cube.js/issues/4132)) ([84c51ed](https://github.com/cube-js/cube.js/commit/84c51eda4bf989a46f95fe683ea2732814dde28f))
* **cubesql:** Support pg_catalog.pg_namespace table ([66e41da](https://github.com/cube-js/cube.js/commit/66e41dacdaf3d0dc24f866c3d29ddb76b79a292a))
* **cubesql:** Support pg_catalog.pg_type table ([d792bb9](https://github.com/cube-js/cube.js/commit/d792bb9949b48e0dceb3aa5d02500258533cfb66))





## [0.29.35](https://github.com/cube-js/cube.js/compare/v0.29.34...v0.29.35) (2022-03-24)


### Bug Fixes

* **cubesql:** Fix decoding for messages without body in pg-wire protocol ([f7aa6ed](https://github.com/cube-js/cube.js/commit/f7aa6ed5438888edce2b413529d79996a912aac3))
* **cubesql:** Specify required parameters on startup for pg-wire ([b79088b](https://github.com/cube-js/cube.js/commit/b79088b01d328082378c6c66d5ca103997955e42))


### Features

* **cubesql:** Split variables to session / server for MySQL ([#4255](https://github.com/cube-js/cube.js/issues/4255)) ([f78b539](https://github.com/cube-js/cube.js/commit/f78b5396e217d9fdf6cf970bd837f767c5b8a2f5))





## [0.29.34](https://github.com/cube-js/cube.js/compare/v0.29.33...v0.29.34) (2022-03-21)


### Bug Fixes

* **cubesql:** Disable MySQL specific functions/statements for pg-wire protocol ([#4222](https://github.com/cube-js/cube.js/issues/4222)) ([21f6cde](https://github.com/cube-js/cube.js/commit/21f6cde31537e515daedd7266e958e7b259f0ace))


### Features

* **cubesql:** Correct response for SslRequest in pg-wire ([#4238](https://github.com/cube-js/cube.js/issues/4238)) ([bd1468a](https://github.com/cube-js/cube.js/commit/bd1468aa0a5851c9bcddb81dfd0d1da5c080972f))





## [0.29.33](https://github.com/cube-js/cube.js/compare/v0.29.32...v0.29.33) (2022-03-17)


### Bug Fixes

* **cubesql:** Add numeric_scale field for information_schema.columns ([2e2877a](https://github.com/cube-js/cube.js/commit/2e2877ab8a1d144f529661481b7fc6ddef7d3c85))


### Features

* **cubesql:** Enable PostgresServer via env variable ([39b6528](https://github.com/cube-js/cube.js/commit/39b6528d91b569bdae90362e1a693404a4eef958))
* **cubesql:** Initial support for pg-wire protocol ([1b87c8c](https://github.com/cube-js/cube.js/commit/1b87c8cc67055ab0be0c208505d2bd50b7abffc8))
* **cubesql:** Support meta layer and dialect for Postgres service ([#4215](https://github.com/cube-js/cube.js/issues/4215)) ([46af90d](https://github.com/cube-js/cube.js/commit/46af90d6d41d147b33f9e9eed24e830857243967))
* **cubesql:** Support PLAIN authentication method to pg-wire ([#4229](https://github.com/cube-js/cube.js/issues/4229)) ([c4fbd8c](https://github.com/cube-js/cube.js/commit/c4fbd8c9f12ffed396754712f912868f147c697a))
* **cubesql:** Support SHOW processlist ([0194098](https://github.com/cube-js/cube.js/commit/0194098af10e77c84ef141dc372f3abc46b3b514))





## [0.29.32](https://github.com/cube-js/cube.js/compare/v0.29.31...v0.29.32) (2022-03-10)


### Features

* **cubesql:** Support information_schema.processlist ([#4185](https://github.com/cube-js/cube.js/issues/4185)) ([4179fb0](https://github.com/cube-js/cube.js/commit/4179fb006104275ba0d7074d681cd937efb0a8fc))





## [0.29.28](https://github.com/cube-js/cube.js/compare/v0.29.27...v0.29.28) (2022-02-10)


### Bug Fixes

* **cubesql:** Allow to pass measure as an argument in COUNT function ([#4063](https://github.com/cube-js/cube.js/issues/4063)) ([c48c7ea](https://github.com/cube-js/cube.js/commit/c48c7ea1c86a64463a84a9ffc1c06aa605c6331c))





## [0.29.27](https://github.com/cube-js/cube.js/compare/v0.29.26...v0.29.27) (2022-02-09)


### Bug Fixes

* **cubesql:** Unique filtering for measures/dimensions/segments in Request ([552c87b](https://github.com/cube-js/cube.js/commit/552c87bf38479133e2c8dac20ac1c29eb034c762))


### Features

* **cubesql:** Move execution to Query Engine ([2d84b6b](https://github.com/cube-js/cube.js/commit/2d84b6b98fc03d84f858bd152f2359232e9ea8ed))





## [0.29.26](https://github.com/cube-js/cube.js/compare/v0.29.25...v0.29.26) (2022-02-07)


### Bug Fixes

* **cubesql:** Ignore case sensitive search for usage of identifiers ([a50f8a2](https://github.com/cube-js/cube.js/commit/a50f8a25e8064f98eb7931c643d2ce67be340ad0))


### Features

* **cubesql:** Support information_schema.COLLATIONS table ([#4018](https://github.com/cube-js/cube.js/issues/4018)) ([262314d](https://github.com/cube-js/cube.js/commit/262314dd939b57851c264f038e4f032d8b98bab8))
* **cubesql:** Support prepared statements in MySQL protocol ([#4005](https://github.com/cube-js/cube.js/issues/4005)) ([6b2f61c](https://github.com/cube-js/cube.js/commit/6b2f61cafbcf4758bba1d16a344871a84d0767f3))
* **cubesql:** Support SHOW COLLATION ([#4025](https://github.com/cube-js/cube.js/issues/4025)) ([95b5d0e](https://github.com/cube-js/cube.js/commit/95b5d0ee8af9054c64e5dac50a89db7bb6d8a5fc))





## [0.29.24](https://github.com/cube-js/cube.js/compare/v0.29.23...v0.29.24) (2022-02-01)


### Bug Fixes

* **cubesql:** Ignore @@ global prefix for system defined variables ([80caef0](https://github.com/cube-js/cube.js/commit/80caef0f2eb145a4405d8edcb4d650179b22c593))


### Features

* **cubesql:** Support binary expression for measures ([#4009](https://github.com/cube-js/cube.js/issues/4009)) ([475a614](https://github.com/cube-js/cube.js/commit/475a6148aa8d87183e7680888d27737c0290e401))
* **cubesql:** Support COUNT(1) ([#4004](https://github.com/cube-js/cube.js/issues/4004)) ([df33d89](https://github.com/cube-js/cube.js/commit/df33d89b1a19c452b1a97b49e640c4ed1a53e1ad))
* **cubesql:** Support SHOW COLUMNS ([#3995](https://github.com/cube-js/cube.js/issues/3995)) ([bbf7e6c](https://github.com/cube-js/cube.js/commit/bbf7e6c232d9c91ccd9421f01f4fdda07ef82998))
* **cubesql:** Support SHOW TABLES via QE ([#4001](https://github.com/cube-js/cube.js/issues/4001)) ([bac2aaa](https://github.com/cube-js/cube.js/commit/bac2aaae130c0863790b2178884a157dcdf0c55d))
* **cubesql:** Support USE 'db' (success reply) ([bd945fb](https://github.com/cube-js/cube.js/commit/bd945fbc12a9250a90f240127ad1ac9910011a01))





## [0.29.23](https://github.com/cube-js/cube.js/compare/v0.29.22...v0.29.23) (2022-01-26)


### Features

* **cubesql:** Setup more system variables ([97fe231](https://github.com/cube-js/cube.js/commit/97fe231b36d1d2497e0a913b2ae35f3f41f98e53))





## [0.29.22](https://github.com/cube-js/cube.js/compare/v0.29.21...v0.29.22) (2022-01-21)


### Features

* **cubesql:** Execute SHOW VARIABLES [LIKE 'pattern'] via QE instead of hardcoding ([#3960](https://github.com/cube-js/cube.js/issues/3960)) ([48c0d77](https://github.com/cube-js/cube.js/commit/48c0d774b8c206dee3f2280fadcbeb832d695dc9))





## [0.29.21](https://github.com/cube-js/cube.js/compare/v0.29.20...v0.29.21) (2022-01-17)


### Features

* **cubesql:** Improve error messages ([#3829](https://github.com/cube-js/cube.js/issues/3829)) ([8293e52](https://github.com/cube-js/cube.js/commit/8293e52a4a509e8559949d8af6446ef8a04e33f5))





## [0.29.20](https://github.com/cube-js/cube.js/compare/v0.29.19...v0.29.20) (2022-01-10)


### Bug Fixes

* **cubesql:** Alias binding problem with escapes (<expr> as '') ([8b2c002](https://github.com/cube-js/cube.js/commit/8b2c002537e5151b51328e041828153ac77bf231))





## [0.29.18](https://github.com/cube-js/cube.js/compare/v0.29.17...v0.29.18) (2022-01-09)

**Note:** Version bump only for package @cubejs-backend/cubesql





## [0.29.15](https://github.com/cube-js/cube.js/compare/v0.29.14...v0.29.15) (2021-12-30)

**Note:** Version bump only for package @cubejs-backend/cubesql





## [0.29.12](https://github.com/cube-js/cube.js/compare/v0.29.11...v0.29.12) (2021-12-29)


### Features

* **cubesql:** Ignore KILL statement without error ([20590f3](https://github.com/cube-js/cube.js/commit/20590f39bc1931f5b23b14d81aa48562e373c95b))





## [0.29.11](https://github.com/cube-js/cube.js/compare/v0.29.10...v0.29.11) (2021-12-24)

**Note:** Version bump only for package @cubejs-backend/cubesql





## [0.29.10](https://github.com/cube-js/cube.js/compare/v0.29.9...v0.29.10) (2021-12-22)

**Note:** Version bump only for package @cubejs-backend/cubesql
