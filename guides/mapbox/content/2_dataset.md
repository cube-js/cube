---
order: 2
title: "Dataset and API"
---

Original [Stack Overflow dataset](https://www.kaggle.com/stackoverflow/stackoverflow) contains locations as strings of text. However, Mapbox best works with locations encoded as [GeoJSON](https://en.wikipedia.org/wiki/GeoJSON), an open standard for geographical features based (surprise!) on JSON. 

![](/images/so-dataset.png)

That's why we've used [Mapbox Search API](https://docs.mapbox.com/#search) to perform *geocoding*. As the geocoding procedure has nothing to do with map data visualization, we're just providing the ready to use dataset with embedded GeoJSON data.

# Setting Up a Database 🐘

We'll be using PostgreSQL, a great open-source database, to store the Stack Overflow dataset. Please make sure to have PostgreSQL [installed](https://www.postgresql.org/download/) on your system.

First, download the [dataset](https://cubedev-guides-mapbox.s3.amazonaws.com/so-dataset.sql) ⬇️ (the file size is about 600 MB).

Then, create the `stackoverflow__example` database with the following commands:

```shell
$ createdb stackoverflow__example
$ psql --dbname stackoverflow__example -f so-dataset.sql
```

# Setting Up an API 📦

Let's use [Cube.js](https://cube.dev), an open-source analytical API platform, to serve this dataset over an API. Run this command:

```shell
$ npx cubejs-cli create stackoverflow__example -d postgres
```

Cube.js uses environment variables for configuration. To set up the connection to our database, we need to specify the database type and name.

In the newly created `stackoverflow__example` folder, please replace the contents of the .env file with the following:

```yaml
CUBEJS_DEVELOPER_MODE=true
CUBEJS_API_SECRET=SECRET
CUBEJS_DB_TYPE=postgres
CUBEJS_DB_NAME=stackoverflow__example
CUBEJS_DB_USER=postgres
CUBEJS_DB_PASS=postgres
```

Now we're ready to start the API with this simple command:

```shell
$ npm run dev
```

To check if the API works, please navigate to [http://localhost:4000](http://localhost:4000/) in your browser. You'll see Cube.js Developer Playground, a powerful tool which greatly simplifies data exploration and query building.

![](/images/playground.png)

The last thing left to make the API work is to define the [data schema](https://cube.dev/docs/getting-started-cubejs-schema): it describes what kind of data we have in our dataset and what should be available at our application.

Let’s go to the [data schema page](http://localhost:4000/#/schema) and check all tables from our database. Then, please click on the plus icon and press the “generate schema” button. Voila! 🎉

Now you can spot a number of new `*.js` files in the `schema` folder.

So, our API is set up, and we're ready to create map data visualizations with Mapbox!