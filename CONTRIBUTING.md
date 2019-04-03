# Contributing to Cube.js

Thanks for taking the time for contribution to Cube.js!
We're very welcoming community and while it's very much appreciated if you follow these guidelines it's not a requirement.

## Code of Conduct
This project and everyone participating in it is governed by the [Cube.js Code of Conduct](./CODE_OF_CONDUCT.md).
By participating, you are expected to uphold this code. Please report unacceptable behavior to info@statsbot.co.

## Contribution Prerequisites

Cube.js works with Node.js 8+ and uses yarn as a package manager.

## Development Workflow
### Cube.js Client

1. After cloning Cube.js repository run `$ yarn` in `packages/cubejs-client-core` and `packages/cubejs-react` to install dependencies.
2. Use `$ yarn link` to add these packages to link registry.
3. Perform required code changes.
4. Use `$ yarn build` in the repository root to build CommonJS and UMD modules.
5. Use `$ yarn link @cubejs-client/core` and/or `$ yarn link @cubejs-client/react` in your project to test changes applied.
6. Use `$ yarn test` where available to test your changes.
7. Ensure commit CommonJS and UMD modules as part of your commit.

### Implementing Driver

1. Copy existing driver package structure and name it in `@cubejs-backend/<db-name>-driver` format.
`@cubejs-backend/mysql-driver` is very good candidate for copying this structure.
2. Please do not copy *CHANGELOG.md*.
3. Name driver class and adjust package.json, README.md accordingly.
4. As a rule of thumb please use only Pure JS libraries as a dependencies where possible.
It increases driver adoption rate a lot.
5. Typically you need to implement only `query()` and `testConnection()` methods of driver.
The rest will be done by `BaseDriver` class.
6. If db requires connection pooling prefer use `generic-pool` implementation with settings similar to other db packages.
7. Make sure your driver has `release()` method in case DB expects graceful shutdowns for connections.
8. Please use yarn to add any dependencies and run `$ yarn` within the package before committing to ensure right `yarn.lock` is in place.
