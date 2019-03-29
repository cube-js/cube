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
