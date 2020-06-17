import React, { useState, useEffect } from 'react';
import MapGL, { Source, Layer, Popup, NavigationControl } from 'react-map-gl';
import { Radio } from "antd";


export default (props) => {
  const [dataAnswers, setDataAnswers] = useState(null);
  const [dataQuestions, setDataQuestions] = useState(null);

  const [viewport, setViewport] = useState({
    latitude: 34,
    longitude: 5,
    zoom: 1.5,
  });
  const [popupInfo, setPopupInfo] = useState(null);
  const [mode, setMode] = useState('both');

  useEffect(() => {
    props.cubejsApi
      .load({
        dimensions: [
          'Questions.title',
          'Questions.views',
          'Questions.tags',
          'Users.geometry',
        ],
        filters: [{
          member: "Users.geometry",
          operator: "set"
        }],
        order: {
          'Questions.views': 'desc',
        },
        limit: 50000,
      })
      .then((resultSet) => {
        let data = {
          type: 'FeatureCollection',
          features: [],
        };
        resultSet.tablePivot().map((item) => {
          data['features'].push({
            type: 'Feature',
            properties: {
              tags: item['Questions.tags'],
              views: item['Questions.views'],
              title: item['Questions.title'],
            },
            geometry: JSON.parse(item['Users.geometry'])
          });
        });
        setDataQuestions(data);
      });
    props.cubejsApi
      .load({
        measures: ['Answers.count'],
        dimensions: [
          'Users.geometry',
        ],
        filters: [{
          member: "Users.geometry",
          operator: "set"
        }],
      })
      .then((resultSet) => {
        let data = {
          type: 'FeatureCollection',
          features: [],
        };
        resultSet.tablePivot().map((item) => {
          data['features'].push({
            type: 'Feature',
            geometry: JSON.parse(item['Users.geometry']),
          });
        });
        setDataAnswers(data)
      });


  }, [])

  const onChangeMode = (e) => {
    setMode(e.target.value)
  }

  const onHoverMap = (event) => {
    if (typeof event.features != 'undefined') {
      const feature = event.features.find(
        (f) => f.layer.id === 'questions-point'
      );
      if (feature) {
        setPopupInfo(feature);
      }
      else {
        setPopupInfo(null);
      }
    }
  };

  const renderPopup = () => {
    return popupInfo == null ? (
      <React.Fragment />
    ) : (
        <Popup
          className='mapbox__popup'
          closeButton={false}
          tipSize={5}
          anchor='top'
          longitude={popupInfo.geometry.coordinates[0]}
          latitude={popupInfo.geometry.coordinates[1]}
        >
          <h3>{popupInfo.properties.title}</h3>
          <div>
            Views count: {popupInfo.properties.views}<br />
            Tags: {popupInfo.properties.tags.replace(/\|/g, ', ')}
          </div>
        </Popup>
      );
  };

  return (
    <div className='mapbox__container'>
      <div className="mapbox__legend">
        <Radio.Group onChange={onChangeMode} defaultValue="both">
          <Radio.Button value="qu"><i className="mapbox__icon__questions"></i>questions</Radio.Button>
          <Radio.Button value="ans"><i className="mapbox__icon__answers"></i>answers</Radio.Button>
          <Radio.Button value="both">both</Radio.Button>
        </Radio.Group>
      </div>
      <MapGL
        {...viewport}
        onViewportChange={(viewport) => {
          setViewport(viewport)
        }}
        width='100%'
        height='100%'
        onHover={onHoverMap}
        mapStyle='mapbox://styles/kalipsik/ckb2fyfqu123n1ilb5yi7uyns/draft'
        mapboxApiAccessToken='pk.eyJ1Ijoia2FsaXBzaWsiLCJhIjoiY2p3Z3JrdjQ4MDRjdDQzcGFyeXBlN3ZtZiJ9.miVaze_snePdEvitucFWSQ'
      >
        <div className='mapbox__navi'>
          <NavigationControl />
        </div>
        <Source type='geojson' data={dataAnswers}>
          <Layer {...{
            id: 'answers-point',
            type: 'circle',
            filter: (mode != 'qu') ? ['!', ['has', 'non_exist']] : ['has', ['get', 'id']],
            paint: {
              'circle-radius': ['interpolate', ['linear'], ['zoom'], 0, 1, 12, 8],
              'circle-stroke-width': 0,
              'circle-opacity': 0.7,
              'circle-color': '#E1FFAF',
            }
          }} />
        </Source>
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
        {renderPopup()}
      </MapGL>
    </div>
  )
}