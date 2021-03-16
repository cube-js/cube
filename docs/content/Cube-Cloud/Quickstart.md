---
title: Quickstart
permalink: /cloud/quickstart
category: Quickstart
menuOrder: 1
---

This guide walks you through setting up Cube Cloud and connecting to your
database.

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
  src="https://cube.dev/downloads/images/cube-cloud-quickstart-2.png"
  style="border: none"
  width="100%"
  />
</div>

## Step 3: Connect your Database

Enter credentials to connect to your database. Consult [connecting to
databases][link-connecting-to-databases] guide for more details.

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

Now you can navigate to Playground to try out your queries or connect your
application to Cube Cloud API.

[link-connecting-to-databases]: /cloud/configuration/connecting-to-databases
