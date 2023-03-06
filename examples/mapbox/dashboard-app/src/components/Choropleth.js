import React, { useState } from 'react';
import { Radio } from 'antd';
import MapGL, { NavigationControl, Source, Layer } from 'react-map-gl';
import { useCubeQuery } from "@cubejs-client/react";

const options = [
  {
    'fill-color': {
      property: 'value',
      stops: [
        [1000000, `rgba(255,100,146,0.1)`],
        [10000000, `rgba(255,100,146,0.4)`],
        [50000000, `rgba(255,100,146,0.8)`],
        [100000000, `rgba(255,100,146,1)`]
      ]
    }
  },
  {
    type: 'symbol',
    layout: {
      'text-field': ['number-format', ['get', 'value'], { 'min-fraction-digits': 0, 'max-fraction-digits': 0 }],
      'text-font': ['Open Sans Semibold', 'Arial Unicode MS Bold'],
      'text-size': {
        property: 'value',
        stops: [
          [{ zoom: 0, value: 100000 }, 9],
          [{ zoom: 0, value: 1000000 }, 10],
          [{ zoom: 0, value: 50000000 }, 12],
          [{ zoom: 0, value: 100000000 }, 14]
        ]
      }
    },
    paint: {
      'text-color': ['case', ['<', ['get', 'value'], 100000000], '#43436B', '#43436B'],
      'text-halo-color': '#FFFFFF',
      'text-halo-width': 1
    }
  }
];

const Choropleth = () => {
  const [viewport, setViewport] = useState({
    latitude: 34,
    longitude: 5,
    zoom: 1.5
  });
  const [mode, setMode] = useState('total');

  const data = {
    type: 'FeatureCollection',
    features: []
  };

  const { resultSet } = useCubeQuery({
    measures: [`Users.${mode}`],
    dimensions: ['Users.country', 'MapboxCoords.coordinates']
  });

  if (resultSet) {
    resultSet
      .tablePivot()
      .filter((item) => item['MapboxCoords.coordinates'] != null)
      .forEach((item) => {
        data['features'].push({
          type: 'Feature',
          properties: {
            name: item['Users.country'],
            value: parseInt(item[`Users.${mode}`])
          },
          geometry: {
            type: 'Polygon',
            coordinates: [item['MapboxCoords.coordinates'].split(';').map((item) => item.split(','))]
          }
        });
      });
  }

  const onChangeMode = (e) => {
    setMode(e.target.value);
  };

  return (
    <div className="mapbox__container">
      <div className="mapbox__legend">
        <Radio.Group onChange={onChangeMode} defaultValue="total">
          <Radio.Button value="total">total</Radio.Button>
          <Radio.Button value="avg">average</Radio.Button>
        </Radio.Group>
      </div>
      <MapGL
        {...viewport}
        onViewportChange={(viewport) => {
          setViewport(viewport);
        }}
        width="100%"
        height="100%"
        mapStyle="mapbox://styles/kalipsik/ckb2fyfqu123n1ilb5yi7uyns"
        mapboxAccessToken={process.env.REACT_APP_MAPBOX_KEY}
      >
        <div className="mapbox__navi">
          <NavigationControl />
        </div>
        <Source type="geojson" data={data}>
          <Layer beforeId="country-label" id="countries" type="fill" paint={options[0]} />
          <Layer {...options[1]} />
        </Source>
      </MapGL>
    </div>
  );
};

export default Choropleth;
