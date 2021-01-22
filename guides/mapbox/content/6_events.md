---
order: 6
title: "Points and Events Visualization"
---

Here we wanted to show the distribution of answers and questions by countries, so we rendered most viewable Stack Overflow questions and most rated answers. 3️⃣

When a point is clicked, we render a popup with information about a question.

# Data Schema

Due to the dataset structure, we don't have the user geometry info in the `Questions` table.

That's why we need to use [joins](https://cube.dev/docs/joins) in our data schema. It's a [one-to-many relationship](https://cube.dev/docs/joins#parameters-relationship) which means that one user can leave many questions.

We need to add the following code to the `schema/Questions.js` file:

```jsx
joins: {
  Users: { 
    sql: `${CUBE}.owner_user_id = ${Users}.id`, 
    relationship: `belongsTo` 
  },
},
```

# Web Component

- [the ClickEvents.js source code](https://github.com/cube-js/cube.js/blob/master/examples/mapbox/dashboard-app/src/components/ClickEvent.js)


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