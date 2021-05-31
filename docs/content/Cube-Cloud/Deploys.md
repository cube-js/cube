---
title: Deploys
permalink: /cloud/deploys
category: Deploys
menuOrder: 1
---

This guide covers features and tools you can use to deploy your Cube.js project
to Cube Cloud.

## Deploy with Git

Continuous deployment works by connecting a Git repository to a Cube Cloud
deployment and keeping the two in sync.

First, go to **Settings > Build & Deploy** to make sure your deployment is
configured to deploy with Git. Then click **Generate Git credentials** to obtain
Git credentials:

<div
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/deploy-with-git-creds.png"
  style="border: none"
  width="100%"
  />
</div>

The instructions to set up Cube Cloud as a Git remote are also available on the
same screen:

```bash
$ git config credential.helper store
$ git remote add cubecloud <YOUR-CUBE-CLOUD-GIT-URL>
$ git push cubecloud master
```

## Deploy with GitHub

First, ensure your deployment is configured to deploy with Git. Then connect
your GitHub repository to your deployment by clicking the **Connect to GitHub**
button, and selecting your repository.

<div
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/deploy-with-github.png"
  style="border: none"
  width="100%"
  />
</div>

Cube Cloud will automatically deploy from the specified production branch
(**master** by default).

## Deploy with CLI

You can use the CLI to set up continuous deployment for a Git repository. You
can also use the CLI to manually deploy changes without continuous deployment.

### Manual Deploys

You can deploy your Cube.js project manually. This method uploads data schema
and configuration files directly from your local project directory.

You can obtain Cube Cloud deploy token from your deployment **Settings** page.

```bash
$ npx cubejs-cli deploy --token TOKEN
```

### Continuous Deployment

You can use Cube.js CLI with your continuous integration tool.

<!-- prettier-ignore-start -->
[[info |]]
| You can use the `CUBE_CLOUD_DEPLOY_AUTH` environment variable to pass the Cube
| Cloud deploy token to Cube.js CLI.
<!-- prettier-ignore-end -->

Below is an example configuration for GitHub Actions:

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
