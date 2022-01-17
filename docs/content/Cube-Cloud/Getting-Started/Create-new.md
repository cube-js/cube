---
title: "Getting Started with Cube Cloud: Create a project"
permalink: /cloud/getting-started/create
---

This guide walks you through creating a deployment on Cube Cloud and connecting your Cube project to your
database.

## Step 1: Create a new Deployment

The first step of creating a Cube App from scratch in Cube Cloud is to create a deployment.

Click the `create deployment` button. This is the first step in the deployment creation. Give it a name and select the cloud provider and
region of your choice.

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Create Deployment Screen"
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/f5b73cc7-ac72-49ff-a3cd-491c6ab89bbc.png"
  style="border: none"
  width="100%"
  />
</div>

## Step 2: Set up the Cube project from scratch

Next up, the second step in creating a Cube App from scratch in Cube Cloud is to click the `+ create` button.

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Upload Project Screen"
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/17f4f303-45cf-4a70-be60-d40cca99ab5e.png"
  style="border: none"
  width="100%"
  />
</div>

## Step 3: Connect your Database

Enter your credentials to connect to your database. Check the [connecting to
databases][link-connecting-to-databases] guide for more details.

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Setup Database Screen"
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/1375f9f1-0860-412a-a436-e2e775ec10fa.png"
  style="border: none"
  width="100%"
  />
</div>

### Why don't you try out some sample data?

We have a sample database where you can try out Cube Cloud.

```json
Hostname
demo-db.cube.dev

Port
5432

Database
ecom

Username
cube

Password
12345
```

In the UI it'll look exactly like the image below.

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Setup Database Screen"
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/031bb948-d706-412c-b714-5bf28df01312.png"
  style="border: none"
  width="100%"
  />
</div>

If you run into issues here, make sure to allow the Cube Cloud IPs to access your database. This means you need to enable these IPs in your firewall. If you are using AWS, this would mean adding a security group with allowed IPs.

## Step 4: Generate the Data Schema

Step four in this case consists of generating a data schema. Start by selecting the database tables to generate the data schema from, then hit `generate`.

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Setup Database Screen"
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/a906434b-c4da-414a-adb3-f010b1fa45d1.png"
  style="border: none"
  width="100%"
  />
</div>

Cube Cloud will generate the data schema and spin up your Cube deployment. With this, you're done. You've created a Cube deployment, configured a database connection, and generated a data schema!

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Setup Database Screen"
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/b6addada-cc77-4940-aa0c-a0a9c3df6fd1.png"
  style="border: none"
  width="100%"
  />
</div>

You're ready for the last step, running queries in the Playground.

## Step 5: Try out Cube Cloud

Now you can navigate to Playground to try out your queries or connect your
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

[link-connecting-to-databases]: /cloud/configuration/connecting-to-databases
