---
order: 7
title: "Choropleth Visualization"
---

# Choropleth layer

This type of map chart suits well for regions statistics, so we used it to count total and average country users’ reputation.

![Post%2034cc8f909c8b47c6a5250189e2deb6d7/Untitled%203.png](Post%2034cc8f909c8b47c6a5250189e2deb6d7/Untitled%203.png)

- [the Choropleth.js code](https://github.com/cube-js/cube.js/blob/master/examples/mapbox/dashboard-app/src/components/Choropleth.js)

This example demonstrates an usage of [transitive joins](https://cube.dev/docs/joins#transitive-joins) and has the most complicated schema:

- Users.js (contains data)

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

- Mapbox.js (contains countries codes, names)

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

- MapboxCoords.js (contains polygons coordinates for map rendering)

```jsx
cube(`MapboxCoords`, {
  sql: `SELECT * FROM public.MapboxCoords`,
  joins: {},
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

At Mapbox.js we created a join:

```jsx
MapboxCoords: {
	sql: `${CUBE}.iso_a3 = ${MapboxCoords}.iso_a3`,
	relationship: `belongsTo`,
},
```

At User.js we created a one more join:

```jsx
Mapbox: {
	sql: `${CUBE}.country = ${Mapbox}.geounit`,
	relationship: `belongsTo`,
}
```

In the case with Stack Overflow example, the most suitable column in Mapbox table is geounit, but in other cases, postal codes, or iso_a3/iso_a2 could be better.

So that's all. You don't need to join Users cube with MapboxCoords cube directly. Cube.js make it for you.

The query is quite simple: we have a measure that sum users’ reputations in our scheme.

```jsx
const { resultSet } = useCubeQuery({
   measures: [`Users.total`],
   dimensions: ['Users.country', 'MapboxCoords.coordinates']
});
```

Then we need just parse a result to geoJSON:

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
            coordinates: [item['MapboxCoords.coordinates'].split(';').map((item) => item.split(','))]
          }
        });
      });
  }
```

After that we define data-driven styles to render choropleth layer with a chosen color palette:

```jsx
'fill-color': { 
property: 'value',
stops: [
	[1000000, `rgba(255,100,146,0.1)`],
	[10000000, `rgba(255,100,146,0.4)`],
	[50000000, `rgba(255,100,146,0.8)`],
	[100000000, `rgba(255,100,146,1)`]
]}
```

The full component source:

```jsx
import React, { useState } from 'react';
import { useCubeQuery } from "@cubejs-client/react";
import MapGL from 'react-map-gl';

function App() {
  const { resultSet } = useCubeQuery({
    measures: [`Users.total`],
    dimensions: ['Users.country', 'MapboxCoords.coordinates']
  });

	const data = {
    type: 'FeatureCollection',
    features: []
  };

	if(resultSet) {
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
            coordinates: [item['MapboxCoords.coordinates'].split(';').map((item) => item.split(','))]
          }
        });
      });
  }

  return (
	    <MapGL
	      width='100%'
	      height='100%'
	      mapboxApiAccessToken='YOUR_TOKEN'>
					<Source type="geojson" data={data}>
	          <Layer beforeId="country-label" id="countries" type="fill" paint={options['total'][0]} />
	          <Layer {...options['total'][1]} />
	        </Source>
	    </MapGL>
  );
}
```

## **Useful links:**

- [Documentation for fill layer, its parameters and settings](https://docs.mapbox.com/mapbox-gl-js/style-spec/layers/#fill)
- [Choropleth layer example at Mapbox.Documentation](https://docs.mapbox.com/mapbox-gl-js/example/updating-choropleth/)

That's the finish for our small research, if you have any feedback or questions about this tutorial or about Cube.js in general — feel free to use our [Slack Cube.js community](http://slack.cube.dev/) or DM me at Slack.


