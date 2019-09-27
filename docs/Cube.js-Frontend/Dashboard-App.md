---
title: Dashboard App
permalink: /dashboard-app
category: Cube.js Frontend
menuOrder: 3
---

You can generate a Dashboard App in the Cube.js developer playground. The
Dashboard App wires multiple frontend components together into single
app. It setups all the structure and configuration to work with Cube.js Backend, while giving you full control to customize it however you want.

The main purpose of the Dashboard App is to easily get the project up and
running, while keeping it fully customizable. Since it is just a frontend app it
is easy to embed into an existing architecture or deploy to any static website hosting.

_Currently Dashboard App is generated based on [React](https://reactjs.org/) JS library and [Ant Design](https://ant.design/) UI framework. If you'd like to see more options with
 different JS and CSS frameworks please [open a Github issue](https://github.com/cube-js/cube.js/issues/new?assignees=&labels=&template=feature_request.md&title=) or [ping us in
 Slack.](https://slack.cube.dev)_

## Overview

Dashboard App is generated in the `dashboard-app` folder within your project folder. To start the app you either navigate to the "Dashboard App" tab in the playground and click "Start dashboard app" or run `$ npm run start` in `your-project-folder/dashboard-app` folder. Dashboard App runs on `3050` port. You can access it directly by going to [http://localhost:3050](http://localhost:3050) or inside the playground under the "Dashboard App" tab.

## Customization

## Deployment

### Netlify 

Since Dashboard App is based on the `create-react-app` it is extremely easy to deploy to Netlify.

First install Netlify CLI:

```bash
$ npm install netlify-cli -g
```

Then build and deploy your Dashboard App:

```bash
$ cd my-cubejs-app/dashboard-app
$ npm run build
$ netlify deploy
```

Next follow command line prompts and choose yes for new project and ./build as your deploy folder.
