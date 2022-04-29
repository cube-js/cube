# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

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
