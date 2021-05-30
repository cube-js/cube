---
title: Cube IDE
permalink: /cloud/dev-tools/cube-ide
category: Developer Tools
menuOrder: 1
redirect_from:
  - /cloud/cube-ide
---

With the Cube IDE, you can write and test and your Cube.js data schemas from
your browser. Cube Cloud can create branch-based development API instances to
quickly test changes in the data schema in your frontend applications before
pushing them into production.

<div class="block-video" style="position: relative; padding-bottom: 60.504201680672274%; height: 0;">
  <iframe src="https://www.loom.com/embed/101b6291b0ba4d1d8982faa3b8c5bd55" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe>
</div>

## Development Mode

In development mode, you can safely make changes to your project without
affecting production deployment. Development mode uses a separate Git branch and
allows testing your changes in Playground or via a separate API endpoint
specific to this branch. This development API hot-reloads your schema changes,
allowing you to quickly test API changes from your applications.

To enter development mode, navigate to the **Schema** page and click **Enter
Development Mode**.

<div
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/enter-dev-mode.png"
  style="border: none"
  width="100%"
  />
</div>

When development mode is active, a grey bar will be visible at the top of the
screen. It provides several useful controls and indicators:

- The name of the current development Git branch
- The status of the development API. After any changes to the project, the API
  will hot-reload, and the API status will indicate when it's ready.
- 'Copy API URL' will copy the API URL to the clipboard for the current
  development branch.

You can exit development mode by clicking **Exit** button in the grey banner.

<div
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/dev-mode-bar.png"
  style="border: none"
  width="100%"
  />
</div>
