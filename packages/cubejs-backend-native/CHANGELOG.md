# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

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
