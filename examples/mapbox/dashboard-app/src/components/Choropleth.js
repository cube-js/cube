import React, { useState, useEffect } from 'react';
import MapGL, { NavigationControl, Source, Layer } from 'react-map-gl';
import { Radio } from 'antd';

const options = {
  total: [
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
  ],
  avg: [
    {
      'fill-color': {
        property: 'value',
        stops: [
          [1000, `rgba(255,100,146,0.1)`],
          [5000, `rgba(255,100,146,0.4)`],
          [12000, `rgba(255,100,146,0.8)`],
          [13000, `rgba(255,100,146,1)`]
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
            [{ zoom: 0, value: 1000 }, 9],
            [{ zoom: 0, value: 5000 }, 10],
            [{ zoom: 0, value: 10000 }, 12],
            [{ zoom: 0, value: 15000 }, 14]
          ]
        }
      },
      paint: {
        'text-color': ['case', ['<', ['get', 'value'], 15000], '#43436B', '#43436B'],
        'text-halo-color': '#FFFFFF',
        'text-halo-width': 1
      }
    }
  ]
};

export default (props) => {
  const [viewport, setViewport] = useState({
    latitude: 34,
    longitude: 5,
    zoom: 1.5
  });

  const [mode, setMode] = useState('total');
  const [data, setData] = useState(null);

  const onChangeMode = (e) => {
    setMode(e.target.value);
  };

  const getData = () => {
    props.cubejsApi
      .load({
        measures: [`Users.${mode}`],
        dimensions: ['Users.country', 'MapboxCoords.coordinates']
      })
      .then((resultSet) => {
        let data = {
          type: 'FeatureCollection',
          features: []
        };
        resultSet
          .tablePivot()
          .filter((item) => item['MapboxCoords.coordinates'] != null)
          .map((item) => {
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
        setData(data);
      });
  };

  useEffect(() => {
    getData();
  }, [mode]);

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
        mapStyle="mapbox://styles/kalipsik/ckb2fyfqu123n1ilb5yi7uyns/draft"
        mapboxApiAccessToken="pk.eyJ1Ijoia2FsaXBzaWsiLCJhIjoiY2p3Z3JrdjQ4MDRjdDQzcGFyeXBlN3ZtZiJ9.miVaze_snePdEvitucFWSQ"
      >
        <div className="mapbox__navi">
          <NavigationControl />
        </div>
        <Source type="geojson" data={data}>
          <Layer beforeId="country-label" id="countries" type="fill" paint={options['total'][0]} />
          <Layer {...options['total'][1]} />
        </Source>
      </MapGL>
    </div>
  );
};
