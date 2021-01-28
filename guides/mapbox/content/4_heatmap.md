---
order: 4
title: "Heatmap Visualization"
---

Okay, let's create our first map data visualization! 1️⃣

Heatmap layer is a suitable way to show data distribution and density. That's why we'll use it to show where Stack Overflow users live.

# Data Schema

This component needs quite a simple schema, because we need only such [dimension](https://cube.dev/docs/dimensions) as “users locations coordinates” and such [measure](https://cube.dev/docs/measures) as “count”.

However, some Stack Overflow users have amazing locations like "in the cloud",  "Interstellar Transport Station", or "on a server far far away". Surprisingly, we can't translate all these fancy locations to GeoJSON, so we're using the SQL `WHERE` clause to select only users from the Earth. 🌎

Here's how the `schema/Users.js` file should look like:

```jsx
cube(`Users`, {
  sql: `SELECT * FROM public.Users WHERE geometry is not null`,
  
  measures: {
    count: {
      type: `count`
    }
  },
  
  dimensions: {
    geometry: {
      sql: 'geometry',
      type: 'string'
    }
  }
});
```

# Web Component

Also, we'll need the `dashboard-app/src/components/Heatmap.js` component with the following [source code](https://github.com/cube-js/cube.js/blob/master/examples/mapbox/dashboard-app/src/components/Heatmap.js). Let's break down its contents!

First, we're loading data to the front-end with a convenient [Cube.js hook](https://cube.dev/docs/@cubejs-client-react#use-cube-query):

```jsx
const { resultSet } = useCubeQuery({ 
  measures: ['Users.count'],
  dimensions: ['Users.geometry'],
});
```

To make map rendering faster, with this query we're grouping users by their locations.

Then, we transform query results to GeoJSON format:

```jsx
let data = {
  type: 'FeatureCollection',
  features: [],
};

if (resultSet) {
  resultSet.tablePivot().map((item) => {
    data['features'].push({
      type: 'Feature',
      properties: {
        value: parseInt(item['Users.count']),
      },
      geometry: JSON.parse(item['Users.geometry']),
    });
  });
}
```

After that, we feed this data to Mapbox. With `react-map-gl`, we can do it this way:

```jsx
  return (
    <MapGL
      width='100%'
      height='100%'
      mapboxApiAccessToken={MAPBOX_TOKEN}>
      <Source type='geojson' data={data}>
        <Layer {...{
          type: 'heatmap',
          paint: {
            'heatmap-intensity': intensity,
            'heatmap-radius': radius,
            'heatmap-weight': [ 'interpolate', [ 'linear' ], [ 'get', 'value' ], 0, 0, 6, 2 ],
            'heatmap-opacity': 1,
          },
        }} />
      </Source>
    </MapGL>
  );
}
```

Note that here we use Mapbox data-driven styling: we defined the `heatmap-weight` property as an expression and it depends on the "properties.value":

```jsx
'heatmap-weight': [ 'interpolate', ['linear'], ['get', 'value'], 0, 0, 6, 2]
```

You can find more information about expressions in [Mapbox docs](https://docs.mapbox.com/mapbox-gl-js/style-spec/expressions/).

Here's the heatmap we've built:

![](/images/heatmap.gif)

## Useful links

- [Heatmap layer example at Mapbox documentation](https://docs.mapbox.com/help/tutorials/make-a-heatmap-with-mapbox-gl-js/)
- [Heatmap layers params descriptions](https://docs.mapbox.com/mapbox-gl-js/style-spec/layers/#heatmap)
- [Some theory about heatmap layers settings, palettes](https://blog.mapbox.com/introducing-heatmaps-in-mapbox-gl-js-71355ada9e6c)