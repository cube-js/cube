---
order: 5
title: "Dynamic Points Visualization"
---

The next question was: is there any correlation between Stack Overflow users' locations and their ratings? 2️⃣

Spoiler alert: no, there isn't 😜. But it's a good question to understand how dynamic data loading works and to dive deep into Cube.js filters.

# Data Schema

We need to tweak the `schema/User.js` data schema to look like this:

```jsx
cube('Users', {
  sql: 'SELECT * FROM public.Users WHERE geometry is not null',
  
  measures: {
    max: {
      sql: 'reputation',
      type: 'max',
    },

    min: {
      sql: 'reputation',
      type: 'min',
    }
  },

  dimensions: {
    value: {
      sql: 'reputation',
      type: 'number'

    },

    geometry: {
      sql: 'geometry',
      type: 'string'
    }
  }
});
```

# Web Component

Also, we'll need the `dashboard-app/src/components/Points.js` component with the following [source code](https://github.com/cube-js/cube.js/blob/master/examples/mapbox/dashboard-app/src/components/Points.js). Let's break down its contents!

First, we needed to query the API to find out an initial range of users reputations:

```jsx
const { resultSet: range } = useCubeQuery({
    measures: ['Users.max', 'Users.min']
});

useEffect(() => {
  if (range) {
    setInitMax(range.tablePivot()[0]['Users.max']);
    setInitMin(range.tablePivot()[0]['Users.min']);
    setMax(range.tablePivot()[0]['Users.max']);
    setMin(range.tablePivot()[0]['Users.max'] * 0.4);
  }
}, [range]);
```

Then, we create a `Slider` component from [Ant Design](https://ant.design), a great open source UI toolkit.  On every chnage to this Slider's value, the front-end will make a request to the database:

```jsx
const { resultSet: points } = useCubeQuery({
  measures: ['Users.max'],
  dimensions: ['Users.geometry'],
  filters: [
    {
      member: "Users.value",
      operator: "lte",
      values: [ max.toString() ]
    },
    {
      member: "Users.value",
      operator: "gte",
      values: [ min.toString() ]
    }
  ]
});
```

To make maps rendering faster, with this query we're grouping users by their locations and showing only the user with the maximum rating.

Then, like in the previous example, we transform query results to GeoJSON format:

```jsx
const data = {
  type: 'FeatureCollection',
  features: [],
};

if (points) {
  points.tablePivot().map((item) => {
    data['features'].push({
      type: 'Feature',
      properties: {
        value: parseInt(item['Users.max']),
      },
      geometry: JSON.parse(item['Users.geometry']),
    });
  });
}
```

Please note that we've also applied a data-driven styling at the layer properties, and now points' radius depends on the rating value.

```jsx
'circle-radius': { 
  property: 'value', 
  stops: [ 
    [{ zoom: 0, value: 10000 }, 2], 
    [{ zoom: 0, value: 2000000 }, 20]
  ] 
}
```

When the data volume is moderate, it's also possible to use only [Mapbox filters](https://docs.mapbox.com/mapbox-gl-js/style-spec/other/#other-filter) and still achieve desired performance. We can load data with Cube.js once and then filter rendered data with these layer settings:

```jsx
filter: [ 
  "all", 
  [">", max, ["get", "value"]], 
  ["<", min, ["get", "value"]] 
],
```

Here's the visualization we've built:

![](/images/points.gif)