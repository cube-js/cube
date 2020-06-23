import React, { useState } from 'react';
import { useCubeQuery } from "@cubejs-client/react";
import MapGL, { Source, Layer, Popup, NavigationControl } from 'react-map-gl';
import { Radio } from "antd";
import { Scrollbars } from 'react-custom-scrollbars';

export default () => {
  const [viewport, setViewport] = useState({
    latitude: 34,
    longitude: 5,
    zoom: 2,
  });
  const [mode, setMode] = useState('both');
  const [selectedPoint, setSelectedPoint] = useState(null);

  const { resultSet: questionsSet } = useCubeQuery({
    measures: [
      'Questions.count'
    ],
    dimensions: [
      'Users.geometry',
    ],
    filters: [{
      member: "Users.geometry",
      operator: "set"
    }],
    order: {
      'Questions.views': 'desc',
    }
  });

  const { resultSet: answersSet } = useCubeQuery({
    measures: ['Answers.count'],
    dimensions: [
      'Users.geometry',
    ],
    filters: [{
      member: "Users.geometry",
      operator: "set"
    }],
  });

  const { resultSet: popupSet } = useCubeQuery({
    dimensions: [
      'Questions.title',
      'Questions.views',
      'Questions.tags'
    ],
    filters: [{
      member: "Users.geometry",
      operator: "contains",
      values: [selectedPoint]
    }],
    order: {
      'Questions.views': 'desc',
    }
  });


  let dataQuestions = {
    type: 'FeatureCollection',
    features: [],
  };


  let dataAnswers = {
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

  if (answersSet) {
    answersSet.tablePivot().map((item) => {
      dataAnswers['features'].push({
        type: 'Feature',
        geometry: JSON.parse(item['Users.geometry']),
      });
    });
  }



  let renderPopup = null;
  if (popupSet && selectedPoint) {
    renderPopup =
      (
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
            {
              popupSet.tablePivot().map((item, i) => (
                <div className="mapbox__popup__item" key={i}>
                  <h3>{item['Questions.title']}</h3>
                  <div>
                    Views count: {item['Questions.views']}<br />
                    Tags: {item['Questions.tags'].replace(/\|/g, ', ')}
                  </div>
                </div>
              ))
            }
          </Scrollbars>
        </Popup>
      )
  }

  const onChangeMode = (e) => {
    setMode(e.target.value);
    setSelectedPoint(null);
  }

  const onClickMap = (event) => {
    if (typeof event.features != 'undefined') {
      const feature = event.features.find(
        (f) => f.layer.id == 'questions-point'
      );
      if (feature) {
        setSelectedPoint(feature.properties.geometry);
      }
      else {
        setSelectedPoint(null);
      }
    }
    else {
      setSelectedPoint(null);
    }
  }

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
        onClick={onClickMap}
        interactiveLayerIds={['questions-point']}
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
        {renderPopup}
      </MapGL>
    </div>
  )
}