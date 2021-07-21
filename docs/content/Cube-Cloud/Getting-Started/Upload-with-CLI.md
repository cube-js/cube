---
title: Getting Started with Cube Cloud - Upload with CLI
permalink: /cloud/getting-started/cli
---

This guide walks you through setting up Cube Cloud and connecting to your
database.

<div class="block-video" style="position: relative; padding-bottom: 56.25%; height: 0;">
  <iframe src="https://www.loom.com/embed/8ad76276b9d74e8283b7c319a22e4411" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe>
</div>

## Step 1: Create Deployment

Click Create Deployment. Then give it a name and select the cloud provider and
region of your choice.

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Create Deployment Screen"
  src="https://cube.dev/downloads/images/cube-cloud-quickstart-1.png"
  style="border: none"
  width="100%"
  />
</div>

## Step 2: Upload your Cube.js project

The next step is to upload your existing Cube.js project to the Cube Cloud.

You can do it by running the following command from terminal in your Cube.js
project directory.

```bash
npx cubejs-cli deploy --token <TOKEN>
```

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Upload Project Screen"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/getting-started-2.png"
  style="border: none"
  width="100%"
  />
</div>

## Step 3: Connect your Database

Enter credentials to connect to your database. Consult [connecting to
databases][ref-cloud-connecting-to-databases] guide for more details.

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Setup Database Screen"
  src="https://cube.dev/downloads/images/cube-cloud-quickstart-3.png"
  style="border: none"
  width="100%"
  />

</div>

## Step 4: Try out Cube Cloud

Now you can navigate to [the Playground][ref-cloud-playground] to try out your queries or connect your
application to Cube Cloud API.

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Playground"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/getting-started-4.png"
  style="border: none"
  width="100%"
  />

</div>

[ref-cloud-connecting-to-databases]:
  /cloud/configuration/connecting-to-databases
[ref-cloud-playground]: /cloud/dev-tools/dev-playground
