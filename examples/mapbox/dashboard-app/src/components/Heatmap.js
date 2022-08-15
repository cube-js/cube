import React, { useState } from 'react';
import { useCubeQuery } from "@cubejs-client/react";
import { Slider, Button } from 'antd';
import MapGL, { NavigationControl, Source, Layer } from 'react-map-gl';

const sample = [
  [
    {
      step: 0,
      color: "rgba(255, 102, 148, 0)"
    },
    {
      step: 0.33,
      color: "rgba(255, 102, 148, 0.2)"
    },
    {
      step: 0.66,
      color: "#FFF4E0"
    },
    {
      step: 1,
      color: "#FF6694"
    }
  ],
  [
    {
      step: 0,
      color: "hsla(328, 0%, 100%, 0)"
    },
    {
      step: 0.33,
      color: "hsl(275, 18%, 60%)"
    },
    {
      step: 0.66,
      color: "hsl(350, 68%, 52%)"
    },
    {
      step: 1,
      color: "hsl(47, 100%, 87%)"
    }
  ],
  [
    {
      step: 0,
      color: "hsla(328, 0%, 100%, 0)"
    },
    {
      step: 0.33,
      color: "#F3F3FB"
    },
    {
      step: 0.66,
      color: "hsl(200, 74%, 52%)"
    },
    {
      step: 1,
      color: "hsl(173, 81%, 96%)"
    }
  ]
]

const Heatmap = () => {
  const [viewport, setViewport] = useState({
    latitude: 34,
    longitude: 5,
    zoom: 1.5,
  })

  const [intensity, setIntensity] = useState(0.1);
  const [weight, setWeight] = useState(2);
  const [radius, setRadius] = useState(11);

  const [colorState, setColorState] = useState(0);


  const { resultSet } = useCubeQuery({
    measures: ['Users.count'],
    dimensions: [
      'Users.geometry',
    ],
    limit: 50000
  });


  const data = {
    type: 'FeatureCollection',
    features: [],
  };

  if (resultSet) {
    resultSet.tablePivot().forEach((item) => {
      data['features'].push({
        type: 'Feature',
        properties: {
          value: parseInt(item['Users.count']),
        },
        geometry: JSON.parse(item['Users.geometry']),
      });
    });
  }

  const renderButtons = sample.map((item, i) => (
    <Button key={i} className={colorState === i ? 'mapbox__sample__button mapbox__sample__button--active' : 'mapbox__sample__button'} onClick={() => { setColorState(i) }}>
      <span style={{ background: `linear-gradient(90deg, ${item[3]['color']} 0%, ${item[2]['color']} 50%,${item[1]['color']} 100%)` }}></span>
    </Button>
  ));


  return (
    <div className='mapbox__container'>
      <MapGL
        {...viewport}
        onViewportChange={(viewport) => {
          setViewport(viewport)
        }}
        width='100%'
        height='100%'
        mapStyle='mapbox://styles/kalipsik/ckb2fyfqu123n1ilb5yi7uyns/'
        mapboxAccessToken={process.env.REACT_APP_MAPBOX_KEY}
      >
        <div className='mapbox__navi'>
          <NavigationControl />
        </div>
        <Source type='geojson' data={data}>
          <Layer {...{
            type: 'heatmap',
            paint: {
              'heatmap-intensity': intensity,
              'heatmap-radius': radius,
              'heatmap-weight': ['interpolate', ['linear'], ['get', 'value'], 0, 0, 6, weight],
              'heatmap-color': [
                "interpolate", ["linear"], ["heatmap-density"],
                sample[colorState][0].step, sample[colorState][0].color,
                sample[colorState][1].step, sample[colorState][1].color,
                sample[colorState][2].step, sample[colorState][2].color,
                sample[colorState][3].step, sample[colorState][3].color,
              ],
              'heatmap-opacity': 1,
            },
          }} />
        </Source>
      </MapGL>
      <div className='mapbox__legend__range'>
        <div className='mapbox__legend__row'>
          <label><span>heatmap-intensity</span><span>{intensity}</span></label>
          <Slider min={0} max={2} defaultValue={intensity} tooltipVisible={false} step={0.1} onChange={(value) => { setIntensity(value) }} />
        </div>

        <div className='mapbox__legend__row'>
          <label><span>heatmap-weight</span><span>{weight}</span></label>
          <Slider min={0} max={10} defaultValue={weight} step={0.5} tooltipVisible={false} onChange={(value) => { setWeight(value) }} />
        </div>

        <div className='mapbox__legend__row'>
          <label><span>heatmap-radius</span><span>{radius}</span></label>
          <Slider min={0} max={20} defaultValue={radius} step={1} tooltipVisible={false} onChange={(value) => { setRadius(value) }} />
        </div>

        <div className='mapbox__legend__row'>
          <label><span>sample palletes</span></label>
          {renderButtons}
        </div>

      </div>
    </div>);
}

export default Heatmap;
