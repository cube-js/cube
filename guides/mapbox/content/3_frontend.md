---
order: 3
title: "Frontend and Mapbox"
---

Okay, now it's time to write some JavaScript and create the front-end part of our map data visualization. As with the data schema, we can easily scaffold it using Cube.js Developer Playground. 

Navigate to the [templates page](http://localhost:4000/#/template-gallery) and choose one of predefined templates or click "Create your own". In this guide, we'll be using React, so choose accordingly.

After a few minutes spent to install all dependencies (oh, these `node_modules`) you'll have the new `dashboard-app` folder. Run this app with the following commands:

```shell
$ cd dashboard-app
$ npm start 
```
Great! Now we're ready to add Mapbox to our front-end app.

# Setting Up Mapbox 🗺

We'll be using the [react-map-gl](http://visgl.github.io/react-map-gl/) wrapper to work with Mapbox. Actually, you can find some plugins for React, Angular, and other frameworks in [Mapbox documentation](https://docs.mapbox.com/mapbox-gl-js/plugins/).

Let's install `react-map-gl` with this command:

```jsx
npm install --save react-map-gl
```

To connect this package to our front-end app, replace the `src/App.jsx` with the following:

```jsx
import * as React from 'react';
import { useState } from 'react';
import MapGL from 'react-map-gl';

const MAPBOX_TOKEN = 'MAPBOX_TOKEN';

function App() {
  const [ viewport, setViewport ] = useState({
    latitude: 34,
    longitude: 5,
    zoom: 1.5,
  });

  return (
    <MapGL
      {...viewport}
      onViewportChange={(viewport) => {
        setViewport(viewport)
      }}
      width='100%'
      height='100%'
      mapboxApiAccessToken={MAPBOX_TOKEN}
    />
  );
}
```

You can see that `MAPBOX_TOKEN` needs to be obtained from Mapbox and put in this file.

Please see the [Mapbox documentation](https://docs.mapbox.com/help/how-mapbox-works/access-tokens/#how-access-tokens-work) or, if you already have a Mapbox account, just generate it at the [account page](https://account.mapbox.com/access-tokens/).

At this point we have an empty world map and can start to visualize data. Hurray!

# Planning the Map Data Visualization 🔢

Here's how you can *any map data visualization* using Mapbox and Cube.js:

- load data to the front-end with Cube.js
- transform data to GeoJSON format
- load data to Mapbox layers
- optionally, customize the map using the `properties` object to set up data-driven styling and manipulations

In this guide, we'll follow this path and create four independent map data visualizations:

- a heatmap layer based on users' location data
- a points layer with data-driven styling and dynamically updated data source
- a points layer with click events
- a choropleth layer based on different calculations and data-driven styling

Let's get hacking! 😎