# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.28.62](https://github.com/cube-js/cube.js/compare/v0.28.61...v0.28.62) (2021-12-02)


### Features

* **cubesql:** Specify transaction_isolation, transaction_read_only ([81a8f2d](https://github.com/cube-js/cube.js/commit/81a8f2d7c791938f01b56572b757edf1630b724e))





## [0.28.61](https://github.com/cube-js/cube.js/compare/v0.28.60...v0.28.61) (2021-11-30)


### Bug Fixes

* **cubesql:** Ignore SET NAMES on AST level ([495f245](https://github.com/cube-js/cube.js/commit/495f245b2652f7bf688c9f8d253c7cdf4b96edcc))


### Features

* **cubesql:** Skip SET {GLOBAL|SESSION} TRANSACTION isolation on AST level ([3afe2b1](https://github.com/cube-js/cube.js/commit/3afe2b1741f7d37a0976a57b3a905f4760f6833c))
* **cubesql:** Specify max_allowed_packet, auto_increment_increment ([dd4a22d](https://github.com/cube-js/cube.js/commit/dd4a22d713181df3e5862fd457bff32221f8eaa1))





## [0.28.60](https://github.com/cube-js/cube.js/compare/v0.28.59...v0.28.60) (2021-11-25)


### Bug Fixes

* **cubesql:** MySQL CLI connection error with COM_FIELD_LIST ([#3728](https://github.com/cube-js/cube.js/issues/3728)) ([aef1401](https://github.com/cube-js/cube.js/commit/aef14014fce87b8b47dc41ead066a7647f6fe225))
* **native:** Return promise for registerInterface ([be97a84](https://github.com/cube-js/cube.js/commit/be97a8433dd0d2e400734c6ab3d3110d59ad4d1a))


### Features

* **cubesql:** Enable unicode_expression (required for LEFT) ([4059a17](https://github.com/cube-js/cube.js/commit/4059a1785fece8293461323ac1de5940a8b9876d))
* **native:** Support Node.js 17 ([91f5d51](https://github.com/cube-js/cube.js/commit/91f5d517d59067fc5e643219c2f4f83ec915259c))





## [0.28.58](https://github.com/cube-js/cube.js/compare/v0.28.57...v0.28.58) (2021-11-18)

**Note:** Version bump only for package @cubejs-backend/native





## [0.28.57](https://github.com/cube-js/cube.js/compare/v0.28.56...v0.28.57) (2021-11-16)


### Features

* **cubesql:** Initial support for INFORMATION_SCHEMA ([d1fac9e](https://github.com/cube-js/cube.js/commit/d1fac9e75cb01cbf6a1207b6e69a999e9d755d1e))





## [0.28.55](https://github.com/cube-js/cube.js/compare/v0.28.54...v0.28.55) (2021-11-12)


### Features

* Introduce checkSqlAuth (auth hook for SQL API) ([3191b73](https://github.com/cube-js/cube.js/commit/3191b73816cd63d242349041c54a7037e9027c1a))





## [0.28.53](https://github.com/cube-js/cube.js/compare/v0.28.52...v0.28.53) (2021-11-04)


### Features

* **cubesql:** Specify MySQL version as 8.0.25 in protocol ([eb7e73e](https://github.com/cube-js/cube.js/commit/eb7e73eac5819f8549f51e841f2f4fdc90ba7f32))





## [0.28.52](https://github.com/cube-js/cube.js/compare/v0.28.51...v0.28.52) (2021-11-03)


### Features

* **cubeclient:** Granularity is an optional field ([c381570](https://github.com/cube-js/cube.js/commit/c381570b786d27c49deb701c43858cd6e2facf02))





## [0.28.51](https://github.com/cube-js/cube.js/compare/v0.28.50...v0.28.51) (2021-10-30)


### Bug Fixes

* **native:** warning - is missing a bundled dependency node-pre-gyp ([0bee2f7](https://github.com/cube-js/cube.js/commit/0bee2f7f1776eb8e11cfed003f2e4741c73b1f48))


### Features

* **cubesql:** Use real Query Engine for simple queries ([cc907d3](https://github.com/cube-js/cube.js/commit/cc907d3e2b35462a789427e084989c2ee4a693db))





## [0.28.50](https://github.com/cube-js/cube.js/compare/v0.28.49...v0.28.50) (2021-10-28)


### Bug Fixes

* **native:** Correct logging level for native module ([c1a8439](https://github.com/cube-js/cube.js/commit/c1a843909d6681c718e3634f60684705cdc32f29))


### Features

* Validate return type for dbType/driverFactory/externalDriverFactory in runtime ([#2657](https://github.com/cube-js/cube.js/issues/2657)) ([10e269f](https://github.com/cube-js/cube.js/commit/10e269f9febe26902838a2d7fa611a0f1d375d3e))





## [0.28.46](https://github.com/cube-js/cube.js/compare/v0.28.45...v0.28.46) (2021-10-20)


### Bug Fixes

* **native:** Catch errors in authentication handshake (msql_srv) ([#3560](https://github.com/cube-js/cube.js/issues/3560)) ([9012399](https://github.com/cube-js/cube.js/commit/90123990fa5713fc1351ba0540776a9f7cd78dce))





## [0.28.44](https://github.com/cube-js/cube.js/compare/v0.28.43...v0.28.44) (2021-10-18)


### Features

* **native:** Enable logger ([f0e2812](https://github.com/cube-js/cube.js/commit/f0e2812491302770b1e62ac4a87d50c58551bea3))





## [0.28.43](https://github.com/cube-js/cube.js/compare/v0.28.42...v0.28.43) (2021-10-17)


### Bug Fixes

* **native:** Allow to install Cube.js on unsupported systems ([71ce6a4](https://github.com/cube-js/cube.js/commit/71ce6a4eaa78870a3716bf8c9f1e091d08639753))
* **native:** Split musl/libc packages (musl is unsupported for now) ([836bd5f](https://github.com/cube-js/cube.js/commit/836bd5f3a2125326144819831c6b04962bdc0565))





## [0.28.42](https://github.com/cube-js/cube.js/compare/v0.28.41...v0.28.42) (2021-10-15)


### Features

* **native:** CubeSQL - support auth via JWT (from user) ([#3536](https://github.com/cube-js/cube.js/issues/3536)) ([a10bd59](https://github.com/cube-js/cube.js/commit/a10bd5921627712182a67fda1e2b170e0373102c))
* Integrate SQL Connector to Cube.js ([#3544](https://github.com/cube-js/cube.js/issues/3544)) ([f90de4c](https://github.com/cube-js/cube.js/commit/f90de4c9283178962f501826a8a64abb674c37d1))





## [0.28.41](https://github.com/cube-js/cube.js/compare/v0.28.40...v0.28.41) (2021-10-12)


### Features

* Introduce @cubejs-backend/native 🦀  ([#3531](https://github.com/cube-js/cube.js/issues/3531)) ([5fd511e](https://github.com/cube-js/cube.js/commit/5fd511e8804c26d06bdc166df05d630c650f23fc))
