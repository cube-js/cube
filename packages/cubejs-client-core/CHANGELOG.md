# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.29.33](https://github.com/cube-js/cube.js/compare/v0.29.32...v0.29.33) (2022-03-17)


### Features

* **playground:** non-additive measures message ([#4236](https://github.com/cube-js/cube.js/issues/4236)) ([ae18bbc](https://github.com/cube-js/cube.js/commit/ae18bbcb9030d0eef03c74410c25902602ec6d43))





## [0.29.29](https://github.com/cube-js/cube.js/compare/v0.29.28...v0.29.29) (2022-03-03)


### Features

* Compact JSON array based response data format support ([#4046](https://github.com/cube-js/cube.js/issues/4046)) ([e74d73c](https://github.com/cube-js/cube.js/commit/e74d73c140f56e71a24c35a5f03e9af63022bced)), closes [#1](https://github.com/cube-js/cube.js/issues/1)





## [0.29.23](https://github.com/cube-js/cube.js/compare/v0.29.22...v0.29.23) (2022-01-26)


### Bug Fixes

* **@cubejs-client/core:** restore support for Angular by removing dependency on `@cubejs-client/dx` ([#3972](https://github.com/cube-js/cube.js/issues/3972)) ([13d30dc](https://github.com/cube-js/cube.js/commit/13d30dc98a08c6ef93808adaf1be6c2aa10c664a))
* **client-core:** apiToken nullable check ([3f93f68](https://github.com/cube-js/cube.js/commit/3f93f68170ff87a50bd6bbf768e1cd36c478c20c))





## [0.29.8](https://github.com/cube-js/cube.js/compare/v0.29.7...v0.29.8) (2021-12-21)


### Bug Fixes

* **@cubejs-client/core:** Add 'meta' field to typescript TCubeMember type [#3682](https://github.com/cube-js/cube.js/issues/3682) ([#3815](https://github.com/cube-js/cube.js/issues/3815)) Thanks @System-Glitch! ([578c0a7](https://github.com/cube-js/cube.js/commit/578c0a75ec2830f48b5d6156370f9343b2fd8b6b))





## [0.29.5](https://github.com/cube-js/cube.js/compare/v0.29.4...v0.29.5) (2021-12-17)


### Features

* **@cubejs-client/dx:** introduce new dependency for Cube.js Client ([5bfaf1c](https://github.com/cube-js/cube.js/commit/5bfaf1cf99d68dfcdddb04f2b3151ad451657ff9))





# [0.29.0](https://github.com/cube-js/cube.js/compare/v0.28.67...v0.29.0) (2021-12-14)


### Bug Fixes

* **client-core:** nullish measure values ([#3664](https://github.com/cube-js/cube.js/issues/3664)) ([f28f803](https://github.com/cube-js/cube.js/commit/f28f803521c9020ce639f68612143c2e962975ea))


### BREAKING CHANGES

* **client-core:** All undefined/null measure values will now be converted to 0





## [0.28.52](https://github.com/cube-js/cube.js/compare/v0.28.51...v0.28.52) (2021-11-03)


### Bug Fixes

* **client-core:** dayjs global locale conflict ([#3606](https://github.com/cube-js/cube.js/issues/3606)) Thanks @LvtLvt! ([de7471d](https://github.com/cube-js/cube.js/commit/de7471dfecd1c49f2e9554c92307d3f7c5b8eb9a))





## [0.28.45](https://github.com/cube-js/cube.js/compare/v0.28.44...v0.28.45) (2021-10-19)

**Note:** Version bump only for package @cubejs-client/core





## [0.28.38](https://github.com/cube-js/cube.js/compare/v0.28.37...v0.28.38) (2021-09-20)


### Bug Fixes

* **playground:** member visibility filter ([958fad1](https://github.com/cube-js/cube.js/commit/958fad1661d75fa7f387d837c209e5494ca94af4))


### Features

* **playground:** time zone for cron expressions ([#3441](https://github.com/cube-js/cube.js/issues/3441)) ([b27f509](https://github.com/cube-js/cube.js/commit/b27f509c690c7970ea5443650a141a1bbfcc947b))





## [0.28.37](https://github.com/cube-js/cube.js/compare/v0.28.36...v0.28.37) (2021-09-17)


### Features

* **playground:** add rollup button ([#3424](https://github.com/cube-js/cube.js/issues/3424)) ([a5db7f1](https://github.com/cube-js/cube.js/commit/a5db7f1905d1eb50bb6e78b4c6c54e03ba7499c9))





## [0.28.35](https://github.com/cube-js/cube.js/compare/v0.28.34...v0.28.35) (2021-09-13)


### Bug Fixes

* **cubejs-client-core:** keep order of elements within tableColumns ([#3368](https://github.com/cube-js/cube.js/issues/3368)) ([b9a0f47](https://github.com/cube-js/cube.js/commit/b9a0f4744543d2d5facdbb191638f05790d21070))





## [0.28.25](https://github.com/cube-js/cube.js/compare/v0.28.24...v0.28.25) (2021-08-20)


### Bug Fixes

* **@cubejs-client/core:** prevent redundant auth updates ([#3288](https://github.com/cube-js/cube.js/issues/3288)) ([5ebf211](https://github.com/cube-js/cube.js/commit/5ebf211fde56aeee1a59f0cabc9f7ae3f8e9ffaa))





## [0.28.24](https://github.com/cube-js/cube.js/compare/v0.28.23...v0.28.24) (2021-08-19)


### Features

* Added Quarter to the timeDimensions of ([3f62b2c](https://github.com/cube-js/cube.js/commit/3f62b2c125b2b7b752e370b65be4c89a0c65a623))
* Support quarter granularity ([4ad1356](https://github.com/cube-js/cube.js/commit/4ad1356ac2d2c4d479c25e60519b0f7b4c1605bb))





## [0.28.17](https://github.com/cube-js/cube.js/compare/v0.28.16...v0.28.17) (2021-08-11)


### Bug Fixes

* **@cubejs-client/core:** client hangs when loading big responses (node) ([#3216](https://github.com/cube-js/cube.js/issues/3216)) ([33a16f9](https://github.com/cube-js/cube.js/commit/33a16f983f5bbbb88e62f737ee2dc0670f3710c7))





## [0.28.15](https://github.com/cube-js/cube.js/compare/v0.28.14...v0.28.15) (2021-08-06)


### Bug Fixes

* **@cubejs-client/core:** do not filter out time dimensions ([#3201](https://github.com/cube-js/cube.js/issues/3201)) ([0300093](https://github.com/cube-js/cube.js/commit/0300093e0af29b87e7a9018dc8159c1299e3cd85))





## [0.28.10](https://github.com/cube-js/cube.js/compare/v0.28.9...v0.28.10) (2021-07-30)

**Note:** Version bump only for package @cubejs-client/core





## [0.28.9](https://github.com/cube-js/cube.js/compare/v0.28.8...v0.28.9) (2021-07-29)


### Bug Fixes

* **@cubejs-client/core:** data blending without date range ([#3161](https://github.com/cube-js/cube.js/issues/3161)) ([cc7c140](https://github.com/cube-js/cube.js/commit/cc7c1401b1d4e7d66fa4215997cfa4f19f8a5707))





## [0.28.6](https://github.com/cube-js/cube.js/compare/v0.28.5...v0.28.6) (2021-07-22)


### Features

* **@cubejs-client/ngx:** async CubejsClient initialization ([#2876](https://github.com/cube-js/cube.js/issues/2876)) ([bba3a01](https://github.com/cube-js/cube.js/commit/bba3a01d2a072093509633f2d26e8df9677f940c))





## [0.28.1](https://github.com/cube-js/cube.js/compare/v0.28.0...v0.28.1) (2021-07-19)

**Note:** Version bump only for package @cubejs-client/core





# [0.28.0](https://github.com/cube-js/cube.js/compare/v0.27.53...v0.28.0) (2021-07-17)


### Features

* Move partition range evaluation from Schema Compiler to Query Orchestrator to allow unbounded queries on partitioned pre-aggregations ([8ea654e](https://github.com/cube-js/cube.js/commit/8ea654e93b57014fb2409e070b3a4c381985a9fd))





## [0.27.51](https://github.com/cube-js/cube.js/compare/v0.27.50...v0.27.51) (2021-07-13)


### Bug Fixes

* **@cubejs-client/core:** incorrect types for logical and/or in query filters ([#3083](https://github.com/cube-js/cube.js/issues/3083)) ([d7014a2](https://github.com/cube-js/cube.js/commit/d7014a21add8d264d92987a3c840d98d09545457))





## [0.27.48](https://github.com/cube-js/cube.js/compare/v0.27.47...v0.27.48) (2021-07-08)


### Bug Fixes

* **@cubejs-client/core:** Long Query 413 URL too large ([#3072](https://github.com/cube-js/cube.js/issues/3072)) ([67de4bc](https://github.com/cube-js/cube.js/commit/67de4bc3de69a4da86d4c8d241abe5d921d0e658))
* **@cubejs-client/core:** week granularity ([#3076](https://github.com/cube-js/cube.js/issues/3076)) ([80812ea](https://github.com/cube-js/cube.js/commit/80812ea4027a929729187b096088f38829e9fa27))


### Performance Improvements

* **@cubejs-client/core:** speed up the pivot implementaion ([#3075](https://github.com/cube-js/cube.js/issues/3075)) ([d6d7a85](https://github.com/cube-js/cube.js/commit/d6d7a858ea8e3940b034cd12ed1630c53e55ea6d))





## [0.27.47](https://github.com/cube-js/cube.js/compare/v0.27.46...v0.27.47) (2021-07-06)


### Features

* **@cubejs-client/playground:** rollup designer v2 ([#3018](https://github.com/cube-js/cube.js/issues/3018)) ([07e2427](https://github.com/cube-js/cube.js/commit/07e2427bb8050a74bae3a4d9206a7cfee6944022))





## [0.27.41](https://github.com/cube-js/cube.js/compare/v0.27.40...v0.27.41) (2021-06-25)

**Note:** Version bump only for package @cubejs-client/core





## [0.27.35](https://github.com/cube-js/cube.js/compare/v0.27.34...v0.27.35) (2021-06-18)

**Note:** Version bump only for package @cubejs-client/core





## [0.27.32](https://github.com/cube-js/cube.js/compare/v0.27.31...v0.27.32) (2021-06-12)


### Features

* **@cubejs-client/playground:** internal pre-agg warning ([#2943](https://github.com/cube-js/cube.js/issues/2943)) ([039270f](https://github.com/cube-js/cube.js/commit/039270f267774bb5a80d5434ba057164e565b08b))





## [0.27.31](https://github.com/cube-js/cube.js/compare/v0.27.30...v0.27.31) (2021-06-11)


### Bug Fixes

* extDbType warning ([#2939](https://github.com/cube-js/cube.js/issues/2939)) ([0f014bf](https://github.com/cube-js/cube.js/commit/0f014bf701e07dd1d542529d4792c0e9fbdceb48))





## [0.27.26](https://github.com/cube-js/cube.js/compare/v0.27.25...v0.27.26) (2021-06-01)

**Note:** Version bump only for package @cubejs-client/core





## [0.27.25](https://github.com/cube-js/cube.js/compare/v0.27.24...v0.27.25) (2021-06-01)


### Features

* time filter operators ([#2851](https://github.com/cube-js/cube.js/issues/2851)) ([5054249](https://github.com/cube-js/cube.js/commit/505424964d34ae16205485e5347409bb4c5a661d))





## [0.27.24](https://github.com/cube-js/cube.js/compare/v0.27.23...v0.27.24) (2021-05-29)


### Bug Fixes

* **@cubejs-client/core:** decompose type ([#2849](https://github.com/cube-js/cube.js/issues/2849)) ([60f2596](https://github.com/cube-js/cube.js/commit/60f25964f8f7aecd8a36cc0f1b811445ae12edc6))





## [0.27.22](https://github.com/cube-js/cube.js/compare/v0.27.21...v0.27.22) (2021-05-27)


### Features

* **@cubejs-client/vue3:** vue 3 support ([#2827](https://github.com/cube-js/cube.js/issues/2827)) ([6ac2c8c](https://github.com/cube-js/cube.js/commit/6ac2c8c938fee3001f78ef0f8782255799550514))





## [0.27.19](https://github.com/cube-js/cube.js/compare/v0.27.18...v0.27.19) (2021-05-24)


### Features

* **@cubejs-client/playground:** pre-agg helper ([#2807](https://github.com/cube-js/cube.js/issues/2807)) ([44f09c3](https://github.com/cube-js/cube.js/commit/44f09c39ce3b2a8c6a2ce2fb66b75a0c288eb1da))





## [0.27.17](https://github.com/cube-js/cube.js/compare/v0.27.16...v0.27.17) (2021-05-22)


### Features

* **@cubejs-client/core:** exporting CubejsApi class ([#2773](https://github.com/cube-js/cube.js/issues/2773)) ([03cfaff](https://github.com/cube-js/cube.js/commit/03cfaff83d2ca1c9e49b6088eab3948d081bb9a9))





## [0.27.15](https://github.com/cube-js/cube.js/compare/v0.27.14...v0.27.15) (2021-05-18)


### Bug Fixes

* **@cubejs-client/core:** compareDateRange pivot fix ([#2752](https://github.com/cube-js/cube.js/issues/2752)) ([653ad84](https://github.com/cube-js/cube.js/commit/653ad84f57cc7d5e004c92fe717246ce33d7dcad))
* **@cubejs-client/core:** Meta types fixes ([#2746](https://github.com/cube-js/cube.js/issues/2746)) ([cd17755](https://github.com/cube-js/cube.js/commit/cd17755a07fe19fbebe44d0193330d43a66b757d))


### Features

* **@cubejs-client/playground:** member grouping ([#2736](https://github.com/cube-js/cube.js/issues/2736)) ([7659438](https://github.com/cube-js/cube.js/commit/76594383e08e44354c5966f8e60107d65e05ddab))





## [0.27.14](https://github.com/cube-js/cube.js/compare/v0.27.13...v0.27.14) (2021-05-13)


### Features

* **@cubejs-client/core:** member sorting ([#2733](https://github.com/cube-js/cube.js/issues/2733)) ([fae3b29](https://github.com/cube-js/cube.js/commit/fae3b293a2ce84ea518567f38546f14acc587e0d))





## [0.27.13](https://github.com/cube-js/cube.js/compare/v0.27.12...v0.27.13) (2021-05-13)


### Bug Fixes

* **@cubejs-client/core:** response error handling ([#2703](https://github.com/cube-js/cube.js/issues/2703)) ([de31373](https://github.com/cube-js/cube.js/commit/de31373d9829a6924d7edc04b96464ffa417d920))





## [0.27.6](https://github.com/cube-js/cube.js/compare/v0.27.5...v0.27.6) (2021-05-03)


### Bug Fixes

* **@cubejs-client/playground:** display server error message ([#2648](https://github.com/cube-js/cube.js/issues/2648)) ([c4d8936](https://github.com/cube-js/cube.js/commit/c4d89369db8796fb136af8370aee2111ac3d0316))





## [0.27.5](https://github.com/cube-js/cube.js/compare/v0.27.4...v0.27.5) (2021-05-03)

**Note:** Version bump only for package @cubejs-client/core





# [0.27.0](https://github.com/cube-js/cube.js/compare/v0.26.104...v0.27.0) (2021-04-26)

**Note:** Version bump only for package @cubejs-client/core





## [0.26.102](https://github.com/cube-js/cube.js/compare/v0.26.101...v0.26.102) (2021-04-22)


### Bug Fixes

* **@cubejs-client/core:** add type definition to the categories method ([#2585](https://github.com/cube-js/cube.js/issues/2585)) ([4112b2d](https://github.com/cube-js/cube.js/commit/4112b2ddf956537dd1fc3d08b249bef8d07f7645))





## [0.26.94](https://github.com/cube-js/cube.js/compare/v0.26.93...v0.26.94) (2021-04-13)


### Bug Fixes

* **@cubejs-client/core:** WebSocket response check ([7af1b45](https://github.com/cube-js/cube.js/commit/7af1b458a9f2d7390e80805241d108b895d5c2cc))





## [0.26.93](https://github.com/cube-js/cube.js/compare/v0.26.92...v0.26.93) (2021-04-12)


### Bug Fixes

* **@cubejs-client/vue:** make query reactive ([#2539](https://github.com/cube-js/cube.js/issues/2539)) ([9bce6ba](https://github.com/cube-js/cube.js/commit/9bce6ba964d71f0cba0e4d4e4e21a973309d77d4))





## [0.26.90](https://github.com/cube-js/cube.js/compare/v0.26.89...v0.26.90) (2021-04-11)


### Bug Fixes

* **@cubejs-client/core:** display request status code ([b6108c9](https://github.com/cube-js/cube.js/commit/b6108c9285ffe177439b2310c201d19f19819206))





## [0.26.82](https://github.com/cube-js/cube.js/compare/v0.26.81...v0.26.82) (2021-04-07)


### Features

* **@cubejs-client/playground:** run query button, disable query auto triggering ([#2476](https://github.com/cube-js/cube.js/issues/2476)) ([92a5d45](https://github.com/cube-js/cube.js/commit/92a5d45eca00e88e925e547a12c3f69b05bfafa6))





## [0.26.73](https://github.com/cube-js/cube.js/compare/v0.26.72...v0.26.73) (2021-04-01)


### Features

* Introduce ITransportResponse for HttpTransport/WSTransport, fix [#2439](https://github.com/cube-js/cube.js/issues/2439) ([756bcb8](https://github.com/cube-js/cube.js/commit/756bcb8ae9cd6075382c01a88e46415dd7d024b3))





## [0.26.70](https://github.com/cube-js/cube.js/compare/v0.26.69...v0.26.70) (2021-03-26)


### Features

* Vue chart renderers ([#2428](https://github.com/cube-js/cube.js/issues/2428)) ([bc2cbab](https://github.com/cube-js/cube.js/commit/bc2cbab22fee860cfc846d1207f6a83899198dd8))





## [0.26.69](https://github.com/cube-js/cube.js/compare/v0.26.68...v0.26.69) (2021-03-25)


### Bug Fixes

* **@cubejs-client/core:** Updated totalRow within ResultSet ([#2410](https://github.com/cube-js/cube.js/issues/2410)) ([91e51be](https://github.com/cube-js/cube.js/commit/91e51be6e5690dfe6ba294f75e768406fcc9d4a1))





## [0.26.63](https://github.com/cube-js/cube.js/compare/v0.26.62...v0.26.63) (2021-03-22)


### Bug Fixes

* **@cubejs-client/ngx:** wrong type reference ([#2407](https://github.com/cube-js/cube.js/issues/2407)) ([d6c4183](https://github.com/cube-js/cube.js/commit/d6c41838df53d18eae23b2d38f86435626568ccf))





## [0.26.61](https://github.com/cube-js/cube.js/compare/v0.26.60...v0.26.61) (2021-03-18)


### Features

* **@cubejs-client/vue:** vue query builder ([#1824](https://github.com/cube-js/cube.js/issues/1824)) ([06ee13f](https://github.com/cube-js/cube.js/commit/06ee13f72ef33372385567ed5e1795087b4f5f53))





## [0.26.60](https://github.com/cube-js/cube.js/compare/v0.26.59...v0.26.60) (2021-03-16)


### Features

* **@cubejs-client/playground:** Playground components ([#2329](https://github.com/cube-js/cube.js/issues/2329)) ([489dc12](https://github.com/cube-js/cube.js/commit/489dc12d7e9bfa87bfb3c8ffabf76f238c86a2fe))





## [0.26.55](https://github.com/cube-js/cube.js/compare/v0.26.54...v0.26.55) (2021-03-12)


### Bug Fixes

* **playground:** Cannot read property 'extendMoment' of undefined ([42fd694](https://github.com/cube-js/cube.js/commit/42fd694f28782413c25356530d6b07db9ea091e0))





## [0.26.54](https://github.com/cube-js/cube.js/compare/v0.26.53...v0.26.54) (2021-03-12)

**Note:** Version bump only for package @cubejs-client/core





## [0.26.53](https://github.com/cube-js/cube.js/compare/v0.26.52...v0.26.53) (2021-03-11)

**Note:** Version bump only for package @cubejs-client/core





## [0.26.45](https://github.com/cube-js/cube.js/compare/v0.26.44...v0.26.45) (2021-03-04)


### Bug Fixes

* **@cubejs-client:** react, core TypeScript fixes ([#2261](https://github.com/cube-js/cube.js/issues/2261)). Thanks to @SteffeyDev! ([4db93af](https://github.com/cube-js/cube.js/commit/4db93af984e737d7a6a448facbc8227907007c5d))





## [0.26.19](https://github.com/cube-js/cube.js/compare/v0.26.18...v0.26.19) (2021-02-19)


### Bug Fixes

* **@cubejs-client/core:** manage boolean filters for missing members ([#2139](https://github.com/cube-js/cube.js/issues/2139)) ([6fad355](https://github.com/cube-js/cube.js/commit/6fad355ea47c0e9dda1e733345a0de0a0d99e7cb))
* **@cubejs-client/react:** type fixes ([#2140](https://github.com/cube-js/cube.js/issues/2140)) ([bca1ff7](https://github.com/cube-js/cube.js/commit/bca1ff72b0204a3cce1d4ee658396b3e22adb1cd))





## [0.26.13](https://github.com/cube-js/cube.js/compare/v0.26.12...v0.26.13) (2021-02-12)


### Features

* **@cubejs-client/playground:** handle missing members ([#2067](https://github.com/cube-js/cube.js/issues/2067)) ([348b245](https://github.com/cube-js/cube.js/commit/348b245f4086095fbe1515d7821d631047f0008c))





# [0.26.0](https://github.com/cube-js/cube.js/compare/v0.25.33...v0.26.0) (2021-02-01)

**Note:** Version bump only for package @cubejs-client/core





## [0.25.31](https://github.com/cube-js/cube.js/compare/v0.25.30...v0.25.31) (2021-01-28)


### Bug Fixes

* **@cubejs-client/core:** propagate time dimension to the drill down query ([#1911](https://github.com/cube-js/cube.js/issues/1911)) ([59701da](https://github.com/cube-js/cube.js/commit/59701dad6f6cb6d78954d18b309716a9d51aa6b7))





# [0.25.0](https://github.com/cube-js/cube.js/compare/v0.24.15...v0.25.0) (2020-12-21)

**Note:** Version bump only for package @cubejs-client/core





## [0.24.14](https://github.com/cube-js/cube.js/compare/v0.24.13...v0.24.14) (2020-12-19)


### Features

* Add HTTP Post to cubejs client core ([#1608](https://github.com/cube-js/cube.js/issues/1608)). Thanks to [@mnifakram](https://github.com/mnifakram)! ([1ebd6a0](https://github.com/cube-js/cube.js/commit/1ebd6a04ac97b31c6a51ef63bb1d4c040e524190))





## [0.24.13](https://github.com/cube-js/cube.js/compare/v0.24.12...v0.24.13) (2020-12-18)

**Note:** Version bump only for package @cubejs-client/core





## [0.24.12](https://github.com/cube-js/cube.js/compare/v0.24.11...v0.24.12) (2020-12-17)


### Bug Fixes

* **@cubejs-core:** Add type definition for compareDateRange, fix [#1621](https://github.com/cube-js/cube.js/issues/1621) ([#1623](https://github.com/cube-js/cube.js/issues/1623)). Thanks to [@cbroome](https://github.com/cbroome)! ([d8b7f36](https://github.com/cube-js/cube.js/commit/d8b7f3654c705cc020ccfa548b180b56b5422f60))





## [0.24.9](https://github.com/cube-js/cube.js/compare/v0.24.8...v0.24.9) (2020-12-16)


### Features

* **@cubejs-client/playground:** Angular chart code generation support in Playground ([#1519](https://github.com/cube-js/cube.js/issues/1519)) ([4690e11](https://github.com/cube-js/cube.js/commit/4690e11f417ff65fea8426360f3f5a2b3acd2792)), closes [#1515](https://github.com/cube-js/cube.js/issues/1515) [#1612](https://github.com/cube-js/cube.js/issues/1612)





## [0.24.8](https://github.com/cube-js/cube.js/compare/v0.24.7...v0.24.8) (2020-12-15)


### Features

* **@cubejs-client/core:** Added pivotConfig option to alias series with a prefix ([#1594](https://github.com/cube-js/cube.js/issues/1594)). Thanks to @MattGson! ([a3342f7](https://github.com/cube-js/cube.js/commit/a3342f7fd0389ce3ad0bc62686c0e787de25f411))





## [0.24.4](https://github.com/cube-js/cube.js/compare/v0.24.3...v0.24.4) (2020-12-07)

**Note:** Version bump only for package @cubejs-client/core





# [0.24.0](https://github.com/cube-js/cube.js/compare/v0.23.15...v0.24.0) (2020-11-26)

**Note:** Version bump only for package @cubejs-client/core





## [0.23.14](https://github.com/cube-js/cube.js/compare/v0.23.13...v0.23.14) (2020-11-22)


### Bug Fixes

* **@cubejs-client/core:** propagate segments to drillDown queries ([#1406](https://github.com/cube-js/cube.js/issues/1406)) ([d4ceb65](https://github.com/cube-js/cube.js/commit/d4ceb6502db9c62c0cf95f1e48879f95ea4544d7))





## [0.23.12](https://github.com/cube-js/cube.js/compare/v0.23.11...v0.23.12) (2020-11-17)


### Bug Fixes

* **@cubejs-client/core:** pivot should work well with null values ([#1386](https://github.com/cube-js/cube.js/issues/1386)). Thanks to [@mspiegel31](https://github.com/mspiegel31)! ([d4c2446](https://github.com/cube-js/cube.js/commit/d4c24469b8eea2d84f04c540b0a5f9a8d285ad1d))





## [0.23.11](https://github.com/cube-js/cube.js/compare/v0.23.10...v0.23.11) (2020-11-13)


### Bug Fixes

* **@cubejs-client/core:** annotation format type ([e5004f6](https://github.com/cube-js/cube.js/commit/e5004f6bf687e7df4b611bf1d772da278558759d))
* **@cubejs-playground:** boolean filters support ([#1269](https://github.com/cube-js/cube.js/issues/1269)) ([adda809](https://github.com/cube-js/cube.js/commit/adda809e4cd08436ffdf8f3396a6f35725f3dc22))





## [0.23.6](https://github.com/cube-js/cube.js/compare/v0.23.5...v0.23.6) (2020-11-02)

**Note:** Version bump only for package @cubejs-client/core





# [0.23.0](https://github.com/cube-js/cube.js/compare/v0.22.4...v0.23.0) (2020-10-28)

**Note:** Version bump only for package @cubejs-client/core





## [0.22.2](https://github.com/cube-js/cube.js/compare/v0.22.1...v0.22.2) (2020-10-26)


### Bug Fixes

* **@cubejs-client/core:** duplicate names in ResultSet.seriesNames() ([#1187](https://github.com/cube-js/cube.js/issues/1187)). Thanks to [@aviranmoz](https://github.com/aviranmoz)! ([8d9eb68](https://github.com/cube-js/cube.js/commit/8d9eb68))





## [0.22.1](https://github.com/cube-js/cube.js/compare/v0.22.0...v0.22.1) (2020-10-21)


### Bug Fixes

* **@cubejs-playground:** avoid unnecessary load calls, dryRun ([#1210](https://github.com/cube-js/cube.js/issues/1210)) ([aaf4911](https://github.com/cube-js/cube.js/commit/aaf4911))





# [0.22.0](https://github.com/cube-js/cube.js/compare/v0.21.2...v0.22.0) (2020-10-20)


### Bug Fixes

* umd build default export ([#1219](https://github.com/cube-js/cube.js/issues/1219)) ([cc434eb](https://github.com/cube-js/cube.js/commit/cc434eb))
* **@cubejs-client/core:** Add parseDateMeasures field to CubeJSApiOptions (typings) ([e1a1ada](https://github.com/cube-js/cube.js/commit/e1a1ada))





## [0.21.1](https://github.com/cube-js/cube.js/compare/v0.21.0...v0.21.1) (2020-10-15)

**Note:** Version bump only for package @cubejs-client/core





# [0.21.0](https://github.com/cube-js/cube.js/compare/v0.20.15...v0.21.0) (2020-10-09)

**Note:** Version bump only for package @cubejs-client/core





## [0.20.12](https://github.com/cube-js/cube.js/compare/v0.20.11...v0.20.12) (2020-10-02)


### Features

* angular query builder ([#1073](https://github.com/cube-js/cube.js/issues/1073)) ([ea088b3](https://github.com/cube-js/cube.js/commit/ea088b3))





## [0.20.11](https://github.com/cube-js/cube.js/compare/v0.20.10...v0.20.11) (2020-09-28)


### Bug Fixes

* propagate drill down parent filters ([#1143](https://github.com/cube-js/cube.js/issues/1143)) ([314985e](https://github.com/cube-js/cube.js/commit/314985e))





## [0.20.10](https://github.com/cube-js/cube.js/compare/v0.20.9...v0.20.10) (2020-09-23)


### Bug Fixes

* drilling into any measure other than the first ([#1131](https://github.com/cube-js/cube.js/issues/1131)) ([e741a20](https://github.com/cube-js/cube.js/commit/e741a20))





## [0.20.9](https://github.com/cube-js/cube.js/compare/v0.20.8...v0.20.9) (2020-09-19)

**Note:** Version bump only for package @cubejs-client/core





## [0.20.6](https://github.com/cube-js/cube.js/compare/v0.20.5...v0.20.6) (2020-09-10)

**Note:** Version bump only for package @cubejs-client/core





## [0.20.5](https://github.com/cube-js/cube.js/compare/v0.20.4...v0.20.5) (2020-09-10)


### Bug Fixes

* cube-client-core resolveMember return type ([#1051](https://github.com/cube-js/cube.js/issues/1051)). Thanks to @Aaronkala ([662cfe0](https://github.com/cube-js/cube.js/commit/662cfe0))
* improve TimeDimensionGranularity type ([#1052](https://github.com/cube-js/cube.js/issues/1052)). Thanks to [@joealden](https://github.com/joealden) ([1e9bd99](https://github.com/cube-js/cube.js/commit/1e9bd99))





## [0.20.3](https://github.com/cube-js/cube.js/compare/v0.20.2...v0.20.3) (2020-09-03)


### Bug Fixes

* Export the TimeDimensionGranularity type ([#1044](https://github.com/cube-js/cube.js/issues/1044)). Thanks to [@gudjonragnar](https://github.com/gudjonragnar) ([26b329e](https://github.com/cube-js/cube.js/commit/26b329e))





## [0.20.2](https://github.com/cube-js/cube.js/compare/v0.20.1...v0.20.2) (2020-09-02)


### Bug Fixes

* subscribe option, new query types to work with ws ([dbf602e](https://github.com/cube-js/cube.js/commit/dbf602e))


### Features

* custom date range ([#1027](https://github.com/cube-js/cube.js/issues/1027)) ([304985f](https://github.com/cube-js/cube.js/commit/304985f))





## [0.20.1](https://github.com/cube-js/cube.js/compare/v0.20.0...v0.20.1) (2020-09-01)

**Note:** Version bump only for package @cubejs-client/core





# [0.20.0](https://github.com/cube-js/cube.js/compare/v0.19.61...v0.20.0) (2020-08-26)


### Bug Fixes

* respect timezone in drillDown queries ([#1003](https://github.com/cube-js/cube.js/issues/1003)) ([c128417](https://github.com/cube-js/cube.js/commit/c128417))


### Features

* Data blending ([#1012](https://github.com/cube-js/cube.js/issues/1012)) ([19fd00e](https://github.com/cube-js/cube.js/commit/19fd00e))
* date range comparison support ([#979](https://github.com/cube-js/cube.js/issues/979)) ([ca21cfd](https://github.com/cube-js/cube.js/commit/ca21cfd))
* Make the Filter type more specific. ([#915](https://github.com/cube-js/cube.js/issues/915)) Thanks to [@ylixir](https://github.com/ylixir) ([cecdb36](https://github.com/cube-js/cube.js/commit/cecdb36))





## [0.19.56](https://github.com/cube-js/cube.js/compare/v0.19.55...v0.19.56) (2020-08-03)


### Bug Fixes

* membersForQuery return type ([#909](https://github.com/cube-js/cube.js/issues/909)) ([4976fcf](https://github.com/cube-js/cube.js/commit/4976fcf))





## [0.19.55](https://github.com/cube-js/cube.js/compare/v0.19.54...v0.19.55) (2020-07-23)


### Features

* expose loadResponse annotation ([#894](https://github.com/cube-js/cube.js/issues/894)) ([2875d47](https://github.com/cube-js/cube.js/commit/2875d47))





## [0.19.54](https://github.com/cube-js/cube.js/compare/v0.19.53...v0.19.54) (2020-07-23)

**Note:** Version bump only for package @cubejs-client/core





## [0.19.53](https://github.com/cube-js/cube.js/compare/v0.19.52...v0.19.53) (2020-07-20)


### Bug Fixes

* preserve order of sorted data ([#870](https://github.com/cube-js/cube.js/issues/870)) ([861db10](https://github.com/cube-js/cube.js/commit/861db10))





## [0.19.51](https://github.com/cube-js/cube.js/compare/v0.19.50...v0.19.51) (2020-07-17)

**Note:** Version bump only for package @cubejs-client/core





## [0.19.50](https://github.com/cube-js/cube.js/compare/v0.19.49...v0.19.50) (2020-07-16)


### Features

* ResultSet serializaion and deserializaion ([#836](https://github.com/cube-js/cube.js/issues/836)) ([80b8d41](https://github.com/cube-js/cube.js/commit/80b8d41))





## [0.19.48](https://github.com/cube-js/cube.js/compare/v0.19.47...v0.19.48) (2020-07-11)


### Bug Fixes

* **cubejs-client-core:** enums exported from declaration files are not accessible ([#810](https://github.com/cube-js/cube.js/issues/810)) ([3396fbe](https://github.com/cube-js/cube.js/commit/3396fbe))





## [0.19.43](https://github.com/cube-js/cube.js/compare/v0.19.42...v0.19.43) (2020-07-04)


### Bug Fixes

* **cubejs-client-core:** Display the measure value when the y axis is empty ([#789](https://github.com/cube-js/cube.js/issues/789)) ([7ec6ac6](https://github.com/cube-js/cube.js/commit/7ec6ac6))


### Features

* Adjust client options to send credentials when needed ([#790](https://github.com/cube-js/cube.js/issues/790)) Thanks to [@colefichter](https://github.com/colefichter) ! ([5203f6c](https://github.com/cube-js/cube.js/commit/5203f6c)), closes [#788](https://github.com/cube-js/cube.js/issues/788)





## [0.19.42](https://github.com/cube-js/cube.js/compare/v0.19.41...v0.19.42) (2020-07-01)


### Bug Fixes

* **docs-gen:** generation fixes ([1598a9b](https://github.com/cube-js/cube.js/commit/1598a9b))
* **docs-gen:** titles ([12a1a5f](https://github.com/cube-js/cube.js/commit/12a1a5f))





## [0.19.41](https://github.com/cube-js/cube.js/compare/v0.19.40...v0.19.41) (2020-06-30)


### Bug Fixes

* **docs-gen:** generator fixes, docs updates ([c5b26d0](https://github.com/cube-js/cube.js/commit/c5b26d0))





## [0.19.40](https://github.com/cube-js/cube.js/compare/v0.19.39...v0.19.40) (2020-06-30)


### Features

* **docs-gen:** Typedoc generator ([#769](https://github.com/cube-js/cube.js/issues/769)) ([15373eb](https://github.com/cube-js/cube.js/commit/15373eb))





## [0.19.37](https://github.com/cube-js/cube.js/compare/v0.19.36...v0.19.37) (2020-06-26)


### Bug Fixes

* **cubejs-client-core:** tableColumns empty data fix ([#750](https://github.com/cube-js/cube.js/issues/750)) ([0ac9b7a](https://github.com/cube-js/cube.js/commit/0ac9b7a))


### Features

* query builder pivot config support ([#742](https://github.com/cube-js/cube.js/issues/742)) ([4e29057](https://github.com/cube-js/cube.js/commit/4e29057))





## [0.19.35](https://github.com/cube-js/cube.js/compare/v0.19.34...v0.19.35) (2020-06-22)


### Bug Fixes

* **cubejs-client-core:** table pivot ([#672](https://github.com/cube-js/cube.js/issues/672)) ([70015f5](https://github.com/cube-js/cube.js/commit/70015f5))
* table ([#740](https://github.com/cube-js/cube.js/issues/740)) ([6f1a8e7](https://github.com/cube-js/cube.js/commit/6f1a8e7))





## [0.19.31](https://github.com/cube-js/cube.js/compare/v0.19.30...v0.19.31) (2020-06-10)


### Bug Fixes

* **cubejs-client-core:** Remove Content-Type header from requests in HttpTransport ([#709](https://github.com/cube-js/cube.js/issues/709)) ([f6e366c](https://github.com/cube-js/cube.js/commit/f6e366c))


### Features

* Query builder order by ([#685](https://github.com/cube-js/cube.js/issues/685)) ([d3c735b](https://github.com/cube-js/cube.js/commit/d3c735b))





## [0.19.23](https://github.com/cube-js/cube.js/compare/v0.19.22...v0.19.23) (2020-06-02)


### Features

* drill down queries support ([#664](https://github.com/cube-js/cube.js/issues/664)) ([7e21545](https://github.com/cube-js/cube.js/commit/7e21545)), closes [#190](https://github.com/cube-js/cube.js/issues/190)





## [0.19.22](https://github.com/cube-js/cube.js/compare/v0.19.21...v0.19.22) (2020-05-26)

**Note:** Version bump only for package @cubejs-client/core





## [0.19.19](https://github.com/cube-js/cube.js/compare/v0.19.18...v0.19.19) (2020-05-15)


### Bug Fixes

* corejs version ([8bef3b2](https://github.com/cube-js/cube.js/commit/8bef3b2))


### Features

* ability to add custom meta data for measures, dimensions and segments ([#641](https://github.com/cube-js/cube.js/issues/641)) ([88d5c9b](https://github.com/cube-js/cube.js/commit/88d5c9b)), closes [#625](https://github.com/cube-js/cube.js/issues/625)





## [0.19.16](https://github.com/cube-js/cube.js/compare/v0.19.15...v0.19.16) (2020-05-07)

**Note:** Version bump only for package @cubejs-client/core





## [0.19.14](https://github.com/cube-js/cube.js/compare/v0.19.13...v0.19.14) (2020-04-24)

**Note:** Version bump only for package @cubejs-client/core





## [0.19.13](https://github.com/cube-js/cube.js/compare/v0.19.12...v0.19.13) (2020-04-21)


### Features

* **react:** `resetResultSetOnChange` option for `QueryRenderer` and `useCubeQuery` ([c8c74d3](https://github.com/cube-js/cube.js/commit/c8c74d3))





## [0.19.12](https://github.com/cube-js/cube.js/compare/v0.19.11...v0.19.12) (2020-04-20)


### Bug Fixes

* Make date measure parsing optional ([d199cd5](https://github.com/cube-js/cube.js/commit/d199cd5)), closes [#602](https://github.com/cube-js/cube.js/issues/602)





## [0.19.9](https://github.com/cube-js/cube.js/compare/v0.19.8...v0.19.9) (2020-04-16)


### Features

* Parse dates on client side ([#522](https://github.com/cube-js/cube.js/issues/522)) Thanks to [@richipargo](https://github.com/richipargo)! ([11c1106](https://github.com/cube-js/cube.js/commit/11c1106))





## [0.19.7](https://github.com/cube-js/cube.js/compare/v0.19.6...v0.19.7) (2020-04-14)


### Features

* Including format and type in tableColumns ([#587](https://github.com/cube-js/cube.js/issues/587)) Thanks to [@danpanaite](https://github.com/danpanaite)! ([3f7d74f](https://github.com/cube-js/cube.js/commit/3f7d74f)), closes [#585](https://github.com/cube-js/cube.js/issues/585)





# [0.19.0](https://github.com/cube-js/cube.js/compare/v0.18.32...v0.19.0) (2020-04-09)

**Note:** Version bump only for package @cubejs-client/core





## [0.18.18](https://github.com/cube-js/cube.js/compare/v0.18.17...v0.18.18) (2020-03-28)

**Note:** Version bump only for package @cubejs-client/core





## [0.18.7](https://github.com/cube-js/cube.js/compare/v0.18.6...v0.18.7) (2020-03-17)

**Note:** Version bump only for package @cubejs-client/core





## [0.18.5](https://github.com/cube-js/cube.js/compare/v0.18.4...v0.18.5) (2020-03-15)


### Bug Fixes

* **@cubejs-client/core:** make `progressCallback` optional ([#497](https://github.com/cube-js/cube.js/issues/497)) Thanks to [@hassankhan](https://github.com/hassankhan)! ([a41cf9a](https://github.com/cube-js/cube.js/commit/a41cf9a))





## [0.18.4](https://github.com/cube-js/cube.js/compare/v0.18.3...v0.18.4) (2020-03-09)


### Bug Fixes

* Request span for WebSocketTransport is incorrectly set ([54ba5da](https://github.com/cube-js/cube.js/commit/54ba5da))





# [0.18.0](https://github.com/cube-js/cube.js/compare/v0.17.10...v0.18.0) (2020-03-01)

**Note:** Version bump only for package @cubejs-client/core





## [0.17.10](https://github.com/cube-js/cube.js/compare/v0.17.9...v0.17.10) (2020-02-20)


### Bug Fixes

* Revert "feat: Bump corejs ([#378](https://github.com/cube-js/cube.js/issues/378))" ([b21cbe6](https://github.com/cube-js/cube.js/commit/b21cbe6)), closes [#418](https://github.com/cube-js/cube.js/issues/418)





## [0.17.9](https://github.com/cube-js/cube.js/compare/v0.17.8...v0.17.9) (2020-02-18)


### Features

* Bump corejs ([#378](https://github.com/cube-js/cube.js/issues/378)) ([cb8d51c](https://github.com/cube-js/cube.js/commit/cb8d51c))
* Request id trace span ([880f65e](https://github.com/cube-js/cube.js/commit/880f65e))





## [0.17.8](https://github.com/cube-js/cube.js/compare/v0.17.7...v0.17.8) (2020-02-14)


### Bug Fixes

* **@cubejs-client/core:** improve types ([#376](https://github.com/cube-js/cube.js/issues/376)) Thanks to [@hassankhan](https://github.com/hassankhan)! ([cfb65a2](https://github.com/cube-js/cube.js/commit/cfb65a2))





# [0.17.0](https://github.com/cube-js/cube.js/compare/v0.16.0...v0.17.0) (2020-02-04)

**Note:** Version bump only for package @cubejs-client/core





# [0.16.0](https://github.com/cube-js/cube.js/compare/v0.15.4...v0.16.0) (2020-02-04)


### Bug Fixes

* Do not pad `last 24 hours` interval to day ([6554611](https://github.com/cube-js/cube.js/commit/6554611)), closes [#361](https://github.com/cube-js/cube.js/issues/361)





## [0.15.4](https://github.com/cube-js/cube.js/compare/v0.15.3...v0.15.4) (2020-02-02)


### Features

* Return `shortTitle` in `tableColumns()` result ([810c812](https://github.com/cube-js/cube.js/commit/810c812))





## [0.15.2](https://github.com/cube-js/cube.js/compare/v0.15.1...v0.15.2) (2020-01-25)


### Bug Fixes

* **@cubejs-client/core:** improve types ([55edf85](https://github.com/cube-js/cube.js/commit/55edf85)), closes [#350](https://github.com/cube-js/cube.js/issues/350)
* Time dimension ResultSet backward compatibility to allow work newer client with old server ([b6834b1](https://github.com/cube-js/cube.js/commit/b6834b1)), closes [#356](https://github.com/cube-js/cube.js/issues/356)





# [0.15.0](https://github.com/cube-js/cube.js/compare/v0.14.3...v0.15.0) (2020-01-18)

**Note:** Version bump only for package @cubejs-client/core





# [0.14.0](https://github.com/cube-js/cube.js/compare/v0.13.12...v0.14.0) (2020-01-16)


### Bug Fixes

* Time dimension can't be selected twice within same query with and without granularity ([aa65129](https://github.com/cube-js/cube.js/commit/aa65129))





## [0.13.9](https://github.com/cube-js/cube.js/compare/v0.13.8...v0.13.9) (2020-01-03)


### Features

* **@cubejs-client/core:** add types ([abdf089](https://github.com/cube-js/cube.js/commit/abdf089))





## [0.13.7](https://github.com/cube-js/cube.js/compare/v0.13.6...v0.13.7) (2019-12-31)


### Bug Fixes

* **client-core:** Uncaught TypeError: cubejs is not a function ([b5c32cd](https://github.com/cube-js/cube.js/commit/b5c32cd))





## [0.13.5](https://github.com/cube-js/cube.js/compare/v0.13.4...v0.13.5) (2019-12-17)


### Features

* Return key in the resultSet.series alongside title ([#291](https://github.com/cube-js/cube.js/issues/291)) ([6144a86](https://github.com/cube-js/cube.js/commit/6144a86))





# [0.13.0](https://github.com/cube-js/cube.js/compare/v0.12.3...v0.13.0) (2019-12-10)

**Note:** Version bump only for package @cubejs-client/core





# [0.12.0](https://github.com/cube-js/cube.js/compare/v0.11.25...v0.12.0) (2019-11-25)

**Note:** Version bump only for package @cubejs-client/core





## [0.11.18](https://github.com/cube-js/cube.js/compare/v0.11.17...v0.11.18) (2019-11-09)

**Note:** Version bump only for package @cubejs-client/core





## [0.11.10](https://github.com/statsbotco/cubejs-client/compare/v0.11.9...v0.11.10) (2019-10-25)


### Features

* client headers for CubejsApi ([#242](https://github.com/statsbotco/cubejs-client/issues/242)). Thanks to [@ferrants](https://github.com/ferrants)! ([2f75ef3](https://github.com/statsbotco/cubejs-client/commit/2f75ef3)), closes [#241](https://github.com/statsbotco/cubejs-client/issues/241)





## [0.11.9](https://github.com/statsbotco/cubejs-client/compare/v0.11.8...v0.11.9) (2019-10-23)


### Bug Fixes

* Support `apiToken` to be an async function: first request sends incorrect token ([a2d0c77](https://github.com/statsbotco/cubejs-client/commit/a2d0c77))





## [0.11.7](https://github.com/statsbotco/cubejs-client/compare/v0.11.6...v0.11.7) (2019-10-22)


### Features

* Support `apiToken` to be an async function ([3a3b5f5](https://github.com/statsbotco/cubejs-client/commit/3a3b5f5))





## [0.11.2](https://github.com/statsbotco/cubejs-client/compare/v0.11.1...v0.11.2) (2019-10-15)


### Bug Fixes

* Incorrect URL generation in HttpTransport ([7e7020b](https://github.com/statsbotco/cubejs-client/commit/7e7020b))





# [0.11.0](https://github.com/statsbotco/cubejs-client/compare/v0.10.62...v0.11.0) (2019-10-15)


### Features

* Sockets Preview ([#231](https://github.com/statsbotco/cubejs-client/issues/231)) ([89fc762](https://github.com/statsbotco/cubejs-client/commit/89fc762)), closes [#221](https://github.com/statsbotco/cubejs-client/issues/221)





## [0.10.61](https://github.com/statsbotco/cubejs-client/compare/v0.10.60...v0.10.61) (2019-10-10)

**Note:** Version bump only for package @cubejs-client/core





## [0.10.59](https://github.com/statsbotco/cubejs-client/compare/v0.10.58...v0.10.59) (2019-10-08)

**Note:** Version bump only for package @cubejs-client/core





## [0.10.56](https://github.com/statsbotco/cubejs-client/compare/v0.10.55...v0.10.56) (2019-10-04)

**Note:** Version bump only for package @cubejs-client/core





## [0.10.55](https://github.com/statsbotco/cubejs-client/compare/v0.10.54...v0.10.55) (2019-10-03)


### Bug Fixes

* **client-core:** can't read property 'title' of undefined ([4b48c7f](https://github.com/statsbotco/cubejs-client/commit/4b48c7f))





## [0.10.16](https://github.com/statsbotco/cubejs-client/compare/v0.10.15...v0.10.16) (2019-07-20)

**Note:** Version bump only for package @cubejs-client/core





## [0.10.15](https://github.com/statsbotco/cubejs-client/compare/v0.10.14...v0.10.15) (2019-07-13)

**Note:** Version bump only for package @cubejs-client/core





## [0.10.14](https://github.com/statsbotco/cubejs-client/compare/v0.10.13...v0.10.14) (2019-07-13)


### Features

* **playground:** Show Query ([dc45fcb](https://github.com/statsbotco/cubejs-client/commit/dc45fcb))





# [0.10.0](https://github.com/statsbotco/cubejs-client/compare/v0.9.24...v0.10.0) (2019-06-21)

**Note:** Version bump only for package @cubejs-client/core





## [0.9.12](https://github.com/statsbotco/cubejs-client/compare/v0.9.11...v0.9.12) (2019-06-03)


### Bug Fixes

* **client-core:** Update normalizePivotConfig method to not to fail if x or y are missing ([ee20863](https://github.com/statsbotco/cubejs-client/commit/ee20863)), closes [#10](https://github.com/statsbotco/cubejs-client/issues/10)





## [0.9.11](https://github.com/statsbotco/cubejs-client/compare/v0.9.10...v0.9.11) (2019-05-31)


### Bug Fixes

* **client-core:** ResultSet series returns a series with no data ([715e170](https://github.com/statsbotco/cubejs-client/commit/715e170)), closes [#38](https://github.com/statsbotco/cubejs-client/issues/38)





# [0.9.0](https://github.com/statsbotco/cubejs-client/compare/v0.8.7...v0.9.0) (2019-05-11)

**Note:** Version bump only for package @cubejs-client/core





## [0.8.7](https://github.com/statsbotco/cubejs-client/compare/v0.8.6...v0.8.7) (2019-05-09)

**Note:** Version bump only for package @cubejs-client/core





## [0.8.4](https://github.com/statsbotco/cubejs-client/compare/v0.8.3...v0.8.4) (2019-05-02)

**Note:** Version bump only for package @cubejs-client/core





## [0.8.1](https://github.com/statsbotco/cubejs-client/compare/v0.8.0...v0.8.1) (2019-04-30)

**Note:** Version bump only for package @cubejs-client/core





# [0.8.0](https://github.com/statsbotco/cubejs-client/compare/v0.7.10...v0.8.0) (2019-04-29)

**Note:** Version bump only for package @cubejs-client/core





## [0.7.10](https://github.com/statsbotco/cubejs-client/compare/v0.7.9...v0.7.10) (2019-04-25)


### Bug Fixes

* **client-core:** Table pivot incorrectly behaves with multiple measures ([adb2270](https://github.com/statsbotco/cubejs-client/commit/adb2270))
* **client-core:** use ',' as standard axisValue delimiter ([e889955](https://github.com/statsbotco/cubejs-client/commit/e889955)), closes [#19](https://github.com/statsbotco/cubejs-client/issues/19)





## [0.7.6](https://github.com/statsbotco/cubejs-client/compare/v0.7.5...v0.7.6) (2019-04-23)


### Bug Fixes

* Use cross-fetch instead of isomorphic-fetch to allow React-Native builds ([#92](https://github.com/statsbotco/cubejs-client/issues/92)) ([79150f4](https://github.com/statsbotco/cubejs-client/commit/79150f4))





## [0.7.3](https://github.com/statsbotco/cubejs-client/compare/v0.7.2...v0.7.3) (2019-04-16)


### Bug Fixes

* Allow SSR: use isomorphic-fetch instead of whatwg-fetch. ([902e581](https://github.com/statsbotco/cubejs-client/commit/902e581)), closes [#1](https://github.com/statsbotco/cubejs-client/issues/1)





# [0.7.0](https://github.com/statsbotco/cubejs-client/compare/v0.6.2...v0.7.0) (2019-04-15)

**Note:** Version bump only for package @cubejs-client/core





# [0.6.0](https://github.com/statsbotco/cubejs-client/compare/v0.5.2...v0.6.0) (2019-04-09)


### Features

* QueryBuilder heuristics. Playground area, table and number implementation. ([c883a48](https://github.com/statsbotco/cubejs-client/commit/c883a48))





## [0.5.2](https://github.com/statsbotco/cubejs-client/compare/v0.5.1...v0.5.2) (2019-04-05)


### Features

* Playground UX improvements ([6760a1d](https://github.com/statsbotco/cubejs-client/commit/6760a1d))





## [0.5.1](https://github.com/statsbotco/cubejs-client/compare/v0.5.0...v0.5.1) (2019-04-02)

**Note:** Version bump only for package @cubejs-client/core





# [0.5.0](https://github.com/statsbotco/cubejs-client/compare/v0.4.6...v0.5.0) (2019-04-01)

**Note:** Version bump only for package @cubejs-client/core





## [0.4.5](https://github.com/statsbotco/cubejs-client/compare/v0.4.4...v0.4.5) (2019-03-21)


### Features

* Playground filters implementation ([de4315d](https://github.com/statsbotco/cubejs-client/commit/de4315d))





## [0.4.4](https://github.com/statsbotco/cubejs-client/compare/v0.4.3...v0.4.4) (2019-03-17)


### Features

* Introduce Schema generation UI in Playground ([349c7d0](https://github.com/statsbotco/cubejs-client/commit/349c7d0))





## [0.4.1](https://github.com/statsbotco/cubejs-client/compare/v0.4.0...v0.4.1) (2019-03-14)

**Note:** Version bump only for package @cubejs-client/core





## [0.3.5-alpha.0](https://github.com/statsbotco/cubejs-client/compare/v0.3.5...v0.3.5-alpha.0) (2019-03-12)

**Note:** Version bump only for package @cubejs-client/core
