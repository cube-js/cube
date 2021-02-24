---
title: Deploys
permalink: /cloud/deploys
category: Deploys
menuOrder: 1
---

This guide covers features and tools you can use to deploy your Cube.js project to Cube Cloud.

## Deploy with Git
Continuous deployment works by connecting a Git repository to a Cube Cloud deployment and keeping the two in sync.

## Deploy with CLI

You can use the CLI to set up continuous deployment for a Git repository. You can also use the CLI to manual deploys changes without continuous deployment.

### Manual Deploys

You can deploy your Cube.js project manually. This method uploads data schema and configuration files directly from your local project directory. 

You can obtain Cube Cloud deploy token from your deployment **Settings** page.

```bash
$ npx cubejs-cli deploy --token TOKEN
```

### Continuous Deployment

You can use Cube.js CLI with your continuous integration tool.

[[info |]]
| You can use `CUBE_CLOUD_DEPLOY_AUTH` environment variable to pass Cube Cloud deploy token to Cube.js CLI.


Below is the example configuration for Github Actions.

```bash
name: My Cube.js App
on:
  push:
    paths:
      - '**'
    branches:
      - 'master'
jobs:
  deploy:
    name: Deploy My Cube.js App
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - name: Checkout
        uses: actions/checkout@v2
      - name: Use Node.js 14.x
        uses: actions/setup-node@v1
        with:
          node-version: 14.x
      - name: Deploy to Cube Cloud
        run: npx cubejs-cli deploy
        env:
          CUBE_CLOUD_DEPLOY_AUTH: ${{ secrets.CUBE_CLOUD_DEPLOY_AUTH }}
```
