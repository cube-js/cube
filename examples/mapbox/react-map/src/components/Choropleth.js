import React, { useState, useEffect } from 'react';
import * as moment from 'moment';

import { Spin } from 'antd';

import MapGL, { Source, Layer } from 'react-map-gl';

import { colors } from './../helpers/variables';

export default (props) => {
  const [choroplethData, setChoroplethData] = useState(null);
  const [totalData, setTotalData] = useState(null);

  useEffect(() => {
    props.cubejsApi
      .load({
        measures: ['stats.total'],
        dimensions: [
          'stats.countryterritorycode',
          'mapbox.name',
          'mapbox__coords.coordinates',
        ],
        timeDimensions: [
          {
            dimension: 'stats.date',
            granularity: 'day',
          },
        ],
        limit: 20000,
      })
      .then((resultSet) => {
        let data = {};
        resultSet
          .tablePivot()
          .filter((item) => item['mapbox__coords.coordinates'] != null)
          .map((s) => {
            let date = moment(s['stats.date.day']).format('YYYY-MM-DD');

            if (!data[date]) {
              data[date] = {
                type: 'FeatureCollection',
                features: [],
              };
            }
            data[date]['features'].push({
              type: 'Feature',
              properties: {
                name: s['mapbox.name'],
                cases: parseInt(s['stats.total']),
              },
              geometry: {
                type: 'Polygon',
                coordinates: [
                  s['mapbox__coords.coordinates']
                    .split(';')
                    .map((item) => item.split(',')),
                ],
              },
            });
          });
        setChoroplethData(data);
      });

    props.cubejsApi
      .load({
        measures: ['stats.total'],
        timeDimensions: [
          {
            dimension: 'stats.date',
            granularity: 'day',
          },
        ],
      })
      .then((resultSet) => {
        let data = {};
        resultSet.tablePivot().map((s) => {
          const date = moment(s['stats.date.day']).format('YYYY-MM-DD');
          data[date] = s['stats.total'];
        });
        setTotalData(data);
      });
  }, []);

  const _renderLegend = colors.levels.map((item) => (
    <div className='map__legend__item'>
      <span style={{ 'background-color': item[1] }}></span>
      {item[0]}
    </div>
  ));

  if (!choroplethData && !totalData) {
    return (
      <div className='mapbox__container'>
        <Spin />
      </div>
    );
  }

  return (
    <div className='mapbox__container'>
      <MapGL
        zoom={1}
        latitude={34}
        longitude={5}
        width='100%'
        height='600px'
        mapStyle='mapbox://styles/kalipsik/ck9601tuk5ky81ik55yadcebu'
        mapboxApiAccessToken='pk.eyJ1Ijoia2FsaXBzaWsiLCJhIjoiY2p3Z3JrdjQ4MDRjdDQzcGFyeXBlN3ZtZiJ9.miVaze_snePdEvitucFWSQ'
      >
        <Source
          type='geojson'
          data={choroplethData != null ? choroplethData[props.date] : null}
        >
          <Layer
            beforeId='country-label'
            id='countries'
            type='fill'
            paint={{
              'fill-color': {
                property: 'cases',
                stops: colors.levels,
              },
            }}
          />
        </Source>
      </MapGL>
      <div className='map__state'>
        <h4>Date:</h4>
        <p>{props.date}</p>
        <h4>Daily cases:</h4>
        <p>{!totalData ? null : totalData[props.date]}</p>
      </div>
      <div className='map__legend'>
        <h4>Legend:</h4>
        {_renderLegend}
      </div>
    </div>
  );
};
