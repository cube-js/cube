import React, { useState, useEffect } from 'react';
import { Slider, Button } from 'antd';
import MapGL, { NavigationControl, Source, Layer } from 'react-map-gl';
const sample = [
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
  ],
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
  ]
]

export default (props) => {
  const [viewport, setViewport] = useState({
    latitude: 34,
    longitude: 5,
    zoom: 1.5,
  })
  const [intensity, setIntensity] = useState(0.1);
  const [weight, setWeight] = useState(2);
  const [radius, setRadius] = useState(11);
  const [data, setData] = useState(null);
  const [color, setColor] = useState([
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
    },
  ]);

  useEffect(() => {
    props.
      cubejsApi
      .load({
        measures: ['Users.count'],
        dimensions: [
          'Users.geometry',
        ],
        filters: [{
          member: "Users.geometry",
          operator: "set"
        }],
        limit: 50000
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
              value: parseInt(item['Users.count']),
            },
            geometry: JSON.parse(item['Users.geometry']),
          });
        });
        setData(data);
      })

  }, [])

  return (
    <div className='mapbox__container'>
      <MapGL
        {...viewport}
        onViewportChange={(viewport) => {
          setViewport(viewport)
        }}
        width='100%'
        height='100%'
        mapStyle='mapbox://styles/kalipsik/ckb2fyfqu123n1ilb5yi7uyns/draft'
        mapboxApiAccessToken='pk.eyJ1Ijoia2FsaXBzaWsiLCJhIjoiY2p3Z3JrdjQ4MDRjdDQzcGFyeXBlN3ZtZiJ9.miVaze_snePdEvitucFWSQ'
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
                color[0].step, color[0].color,
                color[1].step, color[1].color,
                color[2].step, color[2].color,
                color[3].step, color[3].color,
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
          <Button className="mapbox__sample__button" onClick={() => { setColor(sample[0]) }}>
            <span style={{ background: 'linear-gradient(90deg, hsl(47, 100%, 87%) 0%, hsl(350, 68%, 52%) 50%, hsl(275, 18%, 60%) 100%)' }}></span>
          </Button>
          <Button className="mapbox__sample__button" onClick={() => { setColor(sample[1]) }}>
            <span style={{ background: 'linear-gradient(90deg, hsl(173, 81%, 96%) 0%, hsl(200, 74%, 52%) 50%, #F3F3FB 100%)' }}></span>
          </Button>
          <Button className="mapbox__sample__button" onClick={() => { setColor(sample[2]) }}>
            <span style={{ background: 'linear-gradient(90deg, #ff6694 0%, #fff4e0 50%,rgba(255, 102, 148, 0.2) 100%)' }}></span>
          </Button>
        </div>

      </div>
    </div >)
}