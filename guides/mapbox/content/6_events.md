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

Then, we need to have the `dashboard-app/src/components/ClickEvents.js` component to contain the following [source code](https://github.com/cube-js/cube.js/blob/master/examples/mapbox/dashboard-app/src/components/ClickEvent.js). Here are the most important highlights!

The query to get questions data:

```jsx
{
  measures: [ 'Questions.count' ],
  dimensions: [ 'Users.geometry']
}
```

Then we use some pretty straightforward code to transform the data into geoJSON:

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
      geometry: item['Users.geometry'],
    },
    geometry: JSON.parse(item['Users.geometry'])
  });
}); 
```

The next step is to catch the click event and load the point data. The following code is specific to the `react-map-gl` wrapper, but the logic is just to listen to map clicks and filter by layer id:

```jsx

const [selectedPoint, setSelectedPoint] = useState(null);

const { resultSet: popupSet } = useCubeQuery({
  dimensions: [
    'Users.geometry',
    'Questions.title',
    'Questions.views',
    'Questions.tags'
  ],
  filters: [ {
    member: "Users.geometry",
    operator: "contains",
    values: [ selectedPoint ]
  } ],
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

When we catch a click event on some point, we request questions data filtered by point location and update the popup.

So, here's our glorious result:

![](/images/events.gif)