---
title: Dashboard App
permalink: /dashboard-app
category: Cube.js Frontend
menuOrder: 6
---

You can generate a Dashboard App in the Cube.js developer playground. The
Dashboard App wires multiple frontend components together into single
app. It setups all the structure and configuration to work with Cube.js Backend, while giving you full control to customize it however you want.

The main purpose of the Dashboard App is to easily get the project up and
running, while keeping it fully customizable. Since it is just a frontend app it
is easy to embed into an existing architecture or deploy to any static website hosting.

_Currently Dashboard App is generated based on [React](https://reactjs.org/) JS library and [Ant Design](https://ant.design/) UI framework. If you'd like to see more options with
 different JS and CSS frameworks please [open a Github issue](https://github.com/cube-js/cube.js/issues/new) or [ping us in
 Slack.](https://slack.cube.dev)_

## Overview

Dashboard App is generated in the `dashboard-app` folder within your project folder. To start the app you either navigate to the "Dashboard App" tab in the playground and click "Start dashboard app" or run `$ npm run start` in `your-project-folder/dashboard-app` folder. Dashboard App runs on `3000` port. You can access it directly by going to [http://localhost:3050](http://localhost:3050) or inside the playground under the "Dashboard App" tab.

Dashboard App uses Cube.js backend to power the query builder and the dashboard. It also uses Apollo GraphQL with local storage to save meta data such as dashboard items titles and locations on the dashboard. You can easily switch from local storage to your own or hosted GraphQL backend.

## Customization Guides

* [Dynamic React Dashboard](https://react-dashboard.cube.dev/)
* [Real Time Dashboard](https://real-time-dashboard.cube.dev/)

## Deployment

`npm run build` creates a build directory with a production build of your dashboard app. There a lot of options to deploy your static applications. You can serve it with your favorite HTTP server ot just select one of the popular cloud providers. Below you can find quick guides for [Netlify](https://www.netlify.com/) and [ZEIT Now](https://zeit.co/). Also, you can refer to the [create-react-app deployment](https://create-react-app.dev/docs/deployment) guide for additional deployment options.

### Netlify 

Install Netlify CLI:

```bash
$ npm install netlify-cli -g
```

Then build and deploy your Dashboard App:

```bash
$ cd my-cubejs-app/dashboard-app
$ npm run build
$ netlify deploy
```

Next follow command line prompts and choose yes for new project and `build` as your deploy folder.

### ZEIT

Install ZEIT Now CLI:

```bash
$ npm install now -g
```

Run `now` command in the root directory of the app.

```bash
$ cd my-cubejs-app/dashboard-app
$ now
```
