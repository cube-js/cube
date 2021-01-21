---
order: 2
title: "Dataset and API"
---

### Some links:

- [Demo](https://mapbox-example.cubecloudapp.dev/).
- [Source code](https://github.com/cube-js/cube.js/tree/master/examples/mapbox).
- [Mapbox API](https://www.mapbox.com/) (we used it as a map tool).
- [Stack Overflow original dataset](https://console.cloud.google.com/marketplace/details/stack-exchange/stack-overflow) and the normalised dataset[link] for this example.


At Cube.js side in this example we’ve worked with: [transitive joins](https://cube.dev/docs/joins#transitive-joins), pre-aggregations, filters, at Mapbox side we’ve used heat map, geoJSON, maps events, data-driven styling.

# Dataset

To work with Mapbox API, we need data in geoJSON format. Originally, Stack Overflow dataset contains only locations as a strings,  so we’ve normalised it with Mapbox GeoCoding API: in our PostgreSQL dataset we added column with geometry value, that is directly used in the example.

- [Link on the Stack Overflow dataset that is used in this example]()


# Setting up a Backend

To create the `stackoverflow__example` database, please, use the following commands.

```jsx
$ createdb stackoverflow__example
$ psql --dbname stackoverflow__example -f stackoverflow-dump.sql
```

Next, install Cube.js CLI if you don’t have it already and generate a new application.

```jsx
$ npm install -g cubejs-cli
$ cubejs create mapbox-example -d postgres
```

Cube.js uses environment variables for configuration. To configure the connection to our database, we need to specify the DB type and name. In the Cube.js project folder, replace the contents of the .env file with the following:

```jsx
CUBEJS_API_SECRET = SECRET;
CUBEJS_DB_TYPE = postgres;
CUBEJS_DB_NAME = mapbox__example;
CUBEJS_DB_USER = postgres;
CUBEJS_DB_PASS = postgres;
```

Now, start the development server and open the [localhost:4000](https://localhost:4000/) in your browser.

```jsx
$ npm run dev
```

When the environment for Cube.JS is ready, let's define our schema[https://cube.dev/docs/getting-started-cubejs-schema]: it describes what kind of data we have in our dataset and what should be available at our application.

Let’s go to http://localhost:4000/#/schema and check all tables from our database. Then please click on the plus icon and the “generate schema” button.

After this step you can see a ‘schema’ folder inside your project with several .js files.

We need all tables from our database for this whole project, but if you want to implement only part of it, please, go to a specific section: we described a schema for each part accordingly.


# **Setting up frontend**

In this example there are 4 independent map charts:

- a heatmap layer based on users locations data.
- a points layer with data-driven styling and dynamically updated data source.
- points with click events.
- a choropleth layer based on different calculations and data-driven styling.

So after we set backend for our project, let's generate our frontend part. You could do it with our playground: [http://localhost:4000/#/template-gallery](http://localhost:4000/#/template-gallery). Just click "Create your own" or choose one of predefined templates.
It will take several minutes to setup Dashboard App and install all the dependencies.

Then type at your terminal

```jsx
cd dashboard-app
npm start 
```
At this point you can see the running development server and your sample application.

# **Adding Mapbox to the project**

In this example, we used [react-map-gl](http://visgl.github.io/react-map-gl/) as a tool for working with Mapbox maps.. You can find some other plugins for React, Angular and other frameworks at [Mapbox Documentation](https://docs.mapbox.com/mapbox-gl-js/plugins/).

But to follow this tutorial, let's install react-map-gl:

```jsx
npm install --save react-map-gl
```

and add it to our App:

```jsx
import * as React from 'react';
import { useState } from 'react';
import MapGL from 'react-map-gl';

function App() {
  const [viewport, setViewport] = useState({
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
        mapboxApiAccessToken='YOUR_TOKEN'
      />
  );
}
```

You may find information about Mapbox token at [documentation](https://docs.mapbox.com/help/how-mapbox-works/access-tokens/#how-access-tokens-work) or if you already have an Mapbox account you can just generate it at the [Account page](https://account.mapbox.com/access-tokens/).

At this point we have an empty world map and can start to visualize data.

**The main idea of working with from Cube.JS at Mapbox is to load data with cube.js → transform it to geoJSON format → load it as a source to Mapbox layers. Furthermore you can define the "properties" object, that allows you to use data-driven styling and manipulations.**

# [Heatmap layer]
![Post%2034cc8f909c8b47c6a5250189e2deb6d7/Untitled.png](Post%2034cc8f909c8b47c6a5250189e2deb6d7/Untitled.png)

Heatmap layer is a suitable way to show data distribution and density so we chose it to show where Stack Overflow users live.

- [Heatmap.js source code](https://github.com/cube-js/cube.js/blob/master/examples/mapbox/dashboard-app/src/components/Heatmap.js)

This component needs quite simple schema, because we need only such dimension as “users locations coordinates” and such measure as “count”.

Furthermoe some of SO users have an amazing locations as: "in the cloud",  "Interstellar Transport Station", "on a server far far away" and unfortunately we couldn't calculate geometry column for it, so in this query, we chose only Earthy users within a  SQL filter.

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

After that we can load data to our app with [Cube.js hooks](https://cube.dev/docs/@cubejs-client-react#use-cube-query):

```jsx
const { resultSet } = useCubeQuery({
		measures: ['Users.count'],
		dimensions: ['Users.geometry']
	}
);
```


To make data rendering faster, we grouped our users by location and rendered heat map based on user amount in each location.

In such way we transformed query results to geoJSON format:

```jsx
let data = {
	type: 'FeatureCollection',
	features: [],
};

if(resultSet) {
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

After that all that we need is to set this data as a source. In react-map-gl, we can do it this way:

```jsx
import React, { useState } from 'react';
import { useCubeQuery } from "@cubejs-client/react";
import MapGL from 'react-map-gl';

function App() {
	const { resultSet } = useCubeQuery({
    measures: ['Users.count'],
    dimensions: [
      'Users.geometry',
    ],
    filters: [{
      member: "Users.geometry",
      operator: "set"
    }],
    limit: 50000
  });

	const data = {
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

  return (
    <MapGL
      width='100%'
      height='100%'
      mapboxApiAccessToken='YOUR_TOKEN'>
	      <Source type='geojson' data={data}>
	        <Layer {...{
	          type: 'heatmap',
	          paint: {
	            'heatmap-intensity': intensity,
	            'heatmap-radius': radius,
	            'heatmap-weight': ['interpolate', ['linear'], ['get', 'value'], 0, 0, 6, 2],
	            'heatmap-opacity': 1,
	          },
	        }} />
	      </Source>
    </MapGL>
  );
}
```
Here we met Mapbox data-driven styling:

- we defined heatmap-weight property as an expression and it depends on our "properties.value":

```jsx
'heatmap-weight': [ 'interpolate', ['linear'], ['get', 'value'], 0, 0, 6, 2]
```

You can find more information about expressions here:

- [What does mean expressions [‘interpolate’,[‘linear’] …]](https://docs.mapbox.com/mapbox-gl-js/style-spec/expressions/)

At this point you can see the rendered heatmap.

## Could be also useful:

- [Heatmap layer example at Mapbox.Documentation](https://docs.mapbox.com/help/tutorials/make-a-heatmap-with-mapbox-gl-js/)
- [Heatmap layers params descriptions](https://docs.mapbox.com/mapbox-gl-js/style-spec/layers/#heatmap)
- [Some theory about heatmap layers settings, palettes](https://blog.mapbox.com/introducing-heatmaps-in-mapbox-gl-js-71355ada9e6c)

# Points distribution **layer**

The next question was: if there is any dependency from user location and user rating. As we can see from our visualisation - no =) But it's a good example to understand how dynamic data loading works and to dive deeply into Cube.js filters.

![Post%2034cc8f909c8b47c6a5250189e2deb6d7/Untitled%201.png](Post%2034cc8f909c8b47c6a5250189e2deb6d7/Untitled%201.png)

- [the Points.js source code](https://github.com/cube-js/cube.js/blob/master/examples/mapbox/dashboard-app/src/components/Points.js)

Here we also needed only User.js cube with such settings as:

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

Firstly, we needed to define an initial range of users reputations.

We used measures defined at our Users.js cube.

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

Then we passed it to our  [Ant.design](http://ant.design) default component Slider. On a Slider change event the app makes a request to the database. According the [Cube.js documentation](https://cube.dev/docs/query-format#filters-format), values should be strings:

```jsx
const { resultSet: points } = useCubeQuery({
    measures: [
      'Users.max'
    ],
    dimensions: [
      'Users.geometry',
    ],
    filters: [
      {
        member: "Users.value",
        operator: "lte",
        values: [max.toString()]
      },
      {
        member: "Users.value",
        operator: "gte",
        values: [min.toString()]
      }
    ]
  });
```

To make maps rendering faster, we grouped users by location and showed only a user with a maximum rating.

After that we parsed result to the geoJSON format, like in previous example

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

Also we applied a data-driven styling at the layer properties, and now circle radius depends on the rating value.

```jsx
'circle-radius': 
	{ 
			property: 'value', 
			stops: [ 
				[{ zoom: 0, value: 10000 }, 2], 
				[{ zoom: 0, value: 2000000 }, 20]
			] 
	}
```

Note: in some cases it's enough to use only Mapbox API filters. We can load data with Cube.js once and then filter rendered data with these layer settings:

```jsx
filter: [ 
	"all", 
	[">", max, ["get", "value"]], 
	["<", min, ["get", "value"]] 
],
```

[Here](https://docs.mapbox.com/mapbox-gl-js/style-spec/other/#other-filter) you can find more information about Mapbox API filters.

The full component source code:

```jsx
import React, { useState } from 'react';
import { useCubeQuery } from "@cubejs-client/react";
import MapGL from 'react-map-gl';

function App() {
  const [initMin, setInitMin] = useState(0);
  const [initMax, setInitMax] = useState(0);
  const [min, setMin] = useState(0);
  const [max, setMax] = useState(0);

  const { resultSet: range } = useCubeQuery({
    measures: ['Users.max', 'Users.min'],
    filters: [{
      member: "Users.geometry",
      operator: "set"
    }],
  });

  const { resultSet: points } = useCubeQuery({
    measures: [
      'Users.max'
    ],
    dimensions: [
      'Users.geometry',
    ],
    filters: [
      {
        member: "Users.value",
        operator: "lte",
        values: [max.toString()]
      },
      {
        member: "Users.value",
        operator: "gte",
        values: [min.toString()]
      }
    ]
  });

useEffect(() => {
    if (range) {
      setInitMax(range.tablePivot()[0]['Users.max']);
      setInitMin(range.tablePivot()[0]['Users.min']);
      setMax(range.tablePivot()[0]['Users.max']);
      setMin(range.tablePivot()[0]['Users.max'] * 0.4);
    }
  }, [range]);

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

	const onChange = (value) => {
    setMin(value[0]);
    setMax(value[1]);
  }

  return (
		<React.Fragment>
	    <MapGL
	      width='100%'
	      height='100%'
	      mapboxApiAccessToken='YOUR_TOKEN'>
		      <Source type='geojson' data={data}>
	            <Layer {...{
	              type: 'circle',
	              paint: {
	                'circle-radius': {
	                  property: 'value',
	                  stops: [
	                    [{ zoom: 0, value: 10000 }, 2],
	                    [{ zoom: 0, value: 2000000 }, 20],
	                  ]
	                },
	                'circle-stroke-width': 0,
	                'circle-opacity': 0.6,
	                'circle-color': '#FF6492'
	              },
	            }
	            } />
	         </Source>
	    </MapGL>
			<Slider
	        range
	        min={initMin}
	        max={initMax}
	        step={1}
	        defaultValue={[initMax, initMax]}
	        value={[min, max]}
	        onChange={onChange}
	      />
			</React.Fragment>
  );
}
```

# Adding events to Mapbox

![Post%2034cc8f909c8b47c6a5250189e2deb6d7/Untitled%202.png](Post%2034cc8f909c8b47c6a5250189e2deb6d7/Untitled%202.png)

- [the ClickEvents.js source code](https://github.com/cube-js/cube.js/blob/master/examples/mapbox/dashboard-app/src/components/ClickEvent.js)

Here we wanted to show the distribution of answers and questions, so we rendered most viewable Stack Overflow questions and most rated answers.

We render popup with information about a question on a point click event.

Due to the dataset structure, we haven't user geometry inside Questions table, so we needed to use [joins](https://cube.dev/docs/joins) in our Schema at Questions.js.

It's one to many [relationship](https://cube.dev/docs/joins#parameters-relationship), that means that one user can leave many questions.

```jsx
joins: {

Users: { 
	sql: ${CUBE}.owner_user_id = ${Users}.id, 
	relationship: belongsTo 
},
}
```

Here the query to get a questions data:

```jsx
{ 
	measures: [ 'Questions.count' ], 
	dimensions: [ 'Users.geometry']
}
```

Then we used a typical code to transform the data into geoJSON:

```jsx
const data = { 
	type: 'FeatureCollection', 
	features: [], 
}; 

resultSet.tablePivot().map((item) => { 
	data['features'].push({ 
		type: 'Feature', 
		properties: { 
			count: item['Questions.count'], 
			geometry: item['Users.geometry'], }, 
		geometry: JSON.parse(item['Users.geometry']) 
	}); 
}); 
```

The next step is to catch click event and load points data on it. The next code is specific to react-map-gl wrapper, but the logic is to listen map clicks and filter it by layer id:

```jsx

const [selectedPoint, setSelectedPoint] = useState(null);

//Here we used the ‘[skip](https://cube.dev/docs/@cubejs-client-react#use-cube-query-use-cube-query-options)’ param, because if nothing is selected then the selectedPoint is equal to null and we don’t want to make an empty request to our database.

const { resultSet: popupSet } = useCubeQuery({
    dimensions: [
      'Users.geometry',
      'Questions.title',
      'Questions.views',
      'Questions.tags'
    ],
    filters: [{
      member: "Users.geometry",
      operator: "contains",
      values: [selectedPoint]
    }],
  }, { skip: selectedPoint == null });


const onClickMap = (event) => {
    setSelectedPoint(null);
    if (typeof event.features != 'undefined') {
      const feature = event.features.find(
        (f) => f.layer.id == 'questions-point'
      );
      if (feature) {
        setSelectedPoint(feature.properties.geometry);
      }
    }
 }
```

When we catch a click event on some point,  we request a questions data filtered by point location and update the popup.

The full source code for this part is:

```jsx
import React, { useState } from 'react';
import { useCubeQuery } from "@cubejs-client/react";
import MapGL from 'react-map-gl';

function App() {
  const [viewport, setViewport] = useState({
    latitude: 34,
    longitude: 5,
    zoom: 2
  });
  const [selectedPoint, setSelectedPoint] = useState(null);
  const { resultSet: questionsSet } = useCubeQuery({
    measures: [
      'Questions.count'
    ],
    dimensions: [
      'Users.geometry',
    ],
    order: {
      'Questions.views': 'desc',
    }
  });

  const { resultSet: popupSet } = useCubeQuery({
    dimensions: [
      'Users.geometry',
      'Questions.title',
      'Questions.views',
      'Questions.tags'
    ],
    filters: [{
      member: "Users.geometry",
      operator: "contains",
      values: [selectedPoint]
    }],
  }, { skip: selectedPoint == null });

  const dataQuestions = {
    type: 'FeatureCollection',
    features: [],
  };

  if (questionsSet) {
    questionsSet.tablePivot().map((item) => {
      dataQuestions['features'].push({
        type: 'Feature',
        properties: {
          count: item['Questions.count'],
          geometry: item['Users.geometry'],
          id: item['Users.id'],
        },
        geometry: JSON.parse(item['Users.geometry'])
      });
    });
  } 
  
  let renderPopup = null;
  if (popupSet && selectedPoint) {
    renderPopup = (
      <Popup
        className='mapbox__popup'
        closeButton={false}
        tipSize={5}
        anchor='top'
        longitude={JSON.parse(selectedPoint).coordinates[0]}
        latitude={JSON.parse(selectedPoint).coordinates[1]}
        captureScroll={true}
      >
        <Scrollbars
          autoHeight
          autoHeightMin={0}
          autoHeightMax={300}
        >
          {popupSet.tablePivot().map((item, i) => (
            <div className="mapbox__popup__item" key={i}>
              <h3>{item['Questions.title']}</h3>
              <div>
                Views count: {item['Questions.views']}<br />
          Tags: {item['Questions.tags'].replace(/\|/g, ', ')}
              </div>
            </div>
          ))}
        </Scrollbars>
      </Popup>
    );
  } 

  const onClickMap = (event) => {
    setSelectedPoint(null);
    if (typeof event.features != 'undefined') {
      const feature = event.features.find(
        (f) => f.layer.id == 'questions-point'
      );
      if (feature) {
        setSelectedPoint(feature.properties.geometry);
      }
    }
  }


  return (
    <div className='mapbox__container'>
      <MapGL
        {...viewport}
        onViewportChange={(viewport) => {
          setViewport(viewport)
        }}
        width='100%'
        height='100%'
        onClick={onClickMap}
        interactiveLayerIds={['questions-point']}
        mapStyle='mapbox://styles/kalipsik/ckb2fyfqu123n1ilb5yi7uyns/draft'
        mapboxApiAccessToken='pk.eyJ1Ijoia2FsaXBzaWsiLCJhIjoiY2p3Z3JrdjQ4MDRjdDQzcGFyeXBlN3ZtZiJ9.miVaze_snePdEvitucFWSQ'
      >
        <Source type='geojson' data={dataQuestions}>
          <Layer {...{
            id: 'questions-point',
            type: 'circle',
            filter: (mode != 'ans') ? ['!', ['has', 'non_exist']] : ['has', ['get', 'id']],
            paint: {
              'circle-radius': ['interpolate', ['linear'], ['zoom'], 0, 1, 12, 15],
              'circle-stroke-width': 0,
              'circle-opacity': 0.7,
              'circle-color': '#FF6492',
            }
          }} />
        </Source>
        {renderPopup}
      </MapGL>
    </div>
  )
}
```




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


