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
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/f2455031-14b9-49ca-b449-eb113a8deda8.png"
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
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/0fe2ae4e-d596-499d-b26b-5f62f5780683.png"
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
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/81442713-0261-424c-bb09-17b1601c10e0.png"
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
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/a0c30616-0a8e-4c85-8f79-4f9ba849eb63.png"
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
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/a959643f-4e0f-4f62-9dc9-4d48a0405002.png"
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
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/4cd2de24-098f-465e-b7ee-dbadd3e82ab9.png"
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
