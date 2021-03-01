# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

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
