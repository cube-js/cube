# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

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
