---
order: 7
title: "Choropleth Visualization"
---

Finally, choropleth. This type of map chart is suitable for regional statistics, so we're going to use it to visualize total and average users’ rankings by country. 4️⃣

# Data Schema

To accomplish this, we'll need to complicate our schema a bit with a few [transitive joins](https://cube.dev/docs/joins#transitive-joins).

First, let's update the `schema/Users.js` file:

```jsx
 cube('Users', {
  sql: 'SELECT * FROM public.Users',
  joins: {
    Mapbox: {
      sql: '${CUBE}.country = ${Mapbox}.geounit',
      relationship: 'belongsTo',
    },
  },
  measures: {
    total: {
      sql: 'reputation',
      type: 'sum',
    }
  },

  dimensions: {
    value: {
      sql: 'reputation',
      type: 'number'
    },

    country: {
      sql: 'country',
      type: 'string'
    }
  }
});
```

The next file is `schema/Mapbox.js`, it contains country codes and names:

```jsx
cube(`Mapbox`, {
  sql: `SELECT * FROM public.Mapbox`,

  joins: {
    MapboxCoords: {
      sql: `${CUBE}.iso_a3 = ${MapboxCoords}.iso_a3`,
      relationship: `belongsTo`,
    },
  },

  dimensions: {
    name: {
      sql: 'name_long',
      type: 'string',
    },

    geometry: {
      sql: 'geometry',
      type: 'string',
    },
  },
});
```

Then comes `schema/MapboxCoords.js` which, obviously, hold polygon coordinates for map rendering:

```jsx
cube(`MapboxCoords`, {
  sql: `SELECT * FROM public.MapboxCoords`,
  
  dimensions: {
    coordinates: {
      sql: `coordinates`,
      type: 'string',
      primaryKey: true,
      shown: true,
    },
  },
});
```

Please note that we have a join in `schema/Mapbox.js`:

```jsx
MapboxCoords: {
  sql: `${CUBE}.iso_a3 = ${MapboxCoords}.iso_a3`, 
  relationship: `belongsTo`,
},
```

And another one in `schema/User.js`:

```jsx
Mapbox: {
  sql: `${CUBE}.country = ${Mapbox}.geounit`,
  relationship: `belongsTo`,
}
```

With the Stack Overflow dataset, our most suitable column in the `Mapbox` table is `geounit`, but in other cases, postal codes, or `iso_a3`/`iso_a2` could work better.

That's all in regard to the data schema. You don't need to join the `Users` cube with the `MapboxCoords` cube directly. Cube.js will make all the joins for you.

# Web Component

The [source code](https://github.com/cube-js/cube.js/blob/master/examples/mapbox/dashboard-app/src/components/Choropleth.js) is contained in the `dashboard-app/src/components/Choropleth.js` component. Breaking it down for the last time:

The query is quite simple: we have a measure that calculates the sum of users’ rankings.

```jsx
const { resultSet } = useCubeQuery({
  measures: [ `Users.total` ],
  dimensions: [ 'Users.country', 'MapboxCoords.coordinates' ]
});
```

Then we need to transform the result to geoJSON:

```jsx
if (resultSet) {
  resultSet
    .tablePivot()
    .filter((item) => item['MapboxCoords.coordinates'] != null)
    .map((item) => {
      data['features'].push({
        type: 'Feature',
        properties: {
          name: item['Users.country'],
          value: parseInt(item[`Users.total`])
        },
        geometry: {
          type: 'Polygon',
          coordinates: [ item['MapboxCoords.coordinates'].split(';').map((item) => item.split(',')) ]
        }
      });
    });
}
```

After that we define a few data-driven styles to render the choropleth layer with a chosen color palette:

```jsx
'fill-color': { 
  property: 'value',
  stops: [ 
    [1000000, `rgba(255,100,146,0.1)`], 
    [10000000, `rgba(255,100,146,0.4)`], 
    [50000000, `rgba(255,100,146,0.8)`], 
    [100000000, `rgba(255,100,146,1)`]
  ],
}
```

And that's basically it!

Here's what we're going to behold once we're done:

![](/images/choropleth.gif)

Looks beautiful, right?

# The glorious end

So, here our attempt to build a map data visualization comes to its end.

![](/images/demo.gif)

We hope that you liked this guide. If you have any feedback or questions, feel free to join Cube.js community on [Slack](http://slack.cube.dev/) — we'll be happy to assist you.

Also, if you liked the way the data was queries via Cube.js API — visit [Cube.js website](https://cube.dev) and give it a shot.  Cheers! 🎉
