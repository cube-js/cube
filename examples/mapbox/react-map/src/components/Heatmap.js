import React, { useState, useEffect } from 'react';
import { Spin } from 'antd';
import MapGL, { Source, Layer, Popup } from 'react-map-gl';

/* Layers settings */
const clusterLayer = {
  id: 'clusters',
  type: 'circle',
  filter: ['has', 'point_count'],
  paint: {
    'circle-color': '#fff',
    'circle-opacity': 0.6,
    'circle-radius': ['step', ['get', 'point_count'], 12, 80, 16, 150, 20],
    'circle-stroke-width': 2,
    'circle-stroke-color': [
      'step',
      ['get', 'point_count'],
      '#51bbd6',
      80,
      '#f1f075',
      150,
      '#f28cb1',
    ],
  },
};
const clusterCountLayer = {
  id: 'cluster-count',
  type: 'symbol',
  filter: ['has', 'point_count'],
  layout: {
    'text-field': '{point_count_abbreviated}',
    'text-font': ['DIN Pro Medium'],
    'text-size': 12,
  },
};

const unclusteredPointLayer = {
  id: 'unclustered-point',
  type: 'circle',
  filter: ['!', ['has', 'point_count']],
  paint: {
    'circle-color': '#11b4da',
    'circle-radius': 5,
    'circle-stroke-width': 1,
    'circle-stroke-color': '#fff',
  },
};

const heatmapLayer = {
  type: 'heatmap',
  paint: {
    'heatmap-radius': ['interpolate', ['linear'], ['zoom'], 1, 1, 12, 15],
    'heatmap-opacity': ['interpolate', ['linear'], ['zoom'], 14, 1, 20, 0],
    'heatmap-weight': [
      'interpolate',
      ['linear'],
      ['get', 'rating'],
      0,
      0,
      6,
      1,
    ],
    'heatmap-intensity': ['interpolate', ['linear'], ['zoom'], 6, 5, 12, 5],
    'heatmap-color': [
      'interpolate',
      ['linear'],
      ['heatmap-density'],
      0,
      'rgba(129,210,238,0)',
      0.05,
      'rgb(108,193,238)',
      0.4,
      'rgb(211,244,78)',
      0.6,
      'rgb(255,186,81)',
      0.8,
      'rgb(242,129,35)',
      0.9,
      'rgb(249,64,64)',
    ],
    //
    //h
  },
};

export default (props) => {
  const [viewport, setViewport] = useState({
    latitude: 48.86,
    longitude: 2.33,
    zoom: 11,
  });

  const [heatData, setHeatData] = useState(null);
  const [clusterData, setClusterData] = useState(null);
  const [popupInfo, setPopupInfo] = useState(null);

  useEffect(() => {
    props.cubejsApi
      .load({
        dimensions: [
          'paris__poi.name',
          'paris__poi.rating',
          'paris__poi.lat',
          'paris__poi.lng',
        ],
      })
      .then((resultSet) => {
        let data = {
          type: 'FeatureCollection',
          features: [],
        };
        resultSet.tablePivot().map((s) => {
          data['features'].push({
            type: 'Feature',
            properties: {
              name: s['paris__poi.name'],
              rating: parseFloat(s['paris__poi.rating']),
            },
            geometry: {
              type: 'Point',
              coordinates: [s['paris__poi.lng'], s['paris__poi.lat']],
            },
          });
        });
        setHeatData(data);
      });

    props.cubejsApi
      .load({
        dimensions: [
          'paris__accomodation.name',
          'paris__accomodation.address',
          'paris__accomodation.lat',
          'paris__accomodation.lng',
        ],
        limit: 180, // set limit for demonstration purposes
      })
      .then((resultSet) => {
        let data = {
          type: 'FeatureCollection',
          features: [],
        };
        resultSet.tablePivot().map((s) => {
          data['features'].push({
            type: 'Feature',
            properties: {
              name: s['paris__accomodation.name'],
              address: s['paris__accomodation.address'],
            },
            geometry: {
              type: 'Point',
              coordinates: [
                s['paris__accomodation.lng'],
                s['paris__accomodation.lat'],
              ],
            },
          });
        });
        setClusterData(data);
      });
  }, []);

  const _onClick = (event) => {
    const feature = event.features.find(
      (f) => f.layer.id === 'unclustered-point'
    );

    if (feature) {
      setPopupInfo(feature);
    }
  };

  const _renderPopup = () => {
    return popupInfo == null ? (
      <React.Fragment />
    ) : (
      <Popup
        tipSize={5}
        anchor='top'
        longitude={popupInfo.geometry.coordinates[0]}
        latitude={popupInfo.geometry.coordinates[1]}
        closeOnClick={false}
        onClose={() => setPopupInfo(null)}
      >
        <h3>{popupInfo.properties.name}</h3>
        <div>
          <strong>Address: </strong>
          {popupInfo.properties.address}
        </div>
      </Popup>
    );
  };

  if (!heatData && !clusterData) {
    return (
      <div className='mapbox__container'>
        <Spin />
      </div>
    );
  }

  return (
    <div className='mapbox__container'>
      <MapGL
        {...viewport}
        width='100%'
        height='600px'
        mapStyle='mapbox://styles/kalipsik/ck9601tuk5ky81ik55yadcebu'
        onViewportChange={(viewport) => {
          setViewport(viewport);
        }}
        onClick={_onClick}
        mapboxApiAccessToken='pk.eyJ1Ijoia2FsaXBzaWsiLCJhIjoiY2p3Z3JrdjQ4MDRjdDQzcGFyeXBlN3ZtZiJ9.miVaze_snePdEvitucFWSQ'
      >
        <Source type='geojson' data={heatData}>
          <Layer {...heatmapLayer} />
        </Source>
        <Source
          type='geojson'
          data={clusterData}
          cluster={true}
          clusterMaxZoom={12}
          clusterRadius={40}
        >
          <Layer {...clusterLayer} />
          <Layer {...clusterCountLayer} />
          <Layer {...unclusteredPointLayer} />
          {_renderPopup()}
        </Source>
      </MapGL>
      <div className='map__legend'>
        <h4>Legend:</h4>
        <div className='map__legend__item'>
          <span
            style={{
              background:
                'radial-gradient(rgb(249,64,64), rgb(243,200,29), rgba(108,193,238,.5), rgba(255,186,81,0))',
            }}
          ></span>
          heatmap layer <small>based on sightseing points with rating</small>
        </div>
        <div className='map__legend__item'>
          <span
            style={{
              'background-color': 'rgba(255,255,255,.5)',
              border: '2px solid #4fbad5',
            }}
          ></span>
          cluster layer <small>based on accomodation points</small>
        </div>
      </div>
    </div>
  );
};
