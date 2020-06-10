import React, { useState } from 'react';
import { Slider, Tooltip, Button, Input, InputNumber } from 'antd';
import { ChromePicker } from 'react-color';
import MapGL, { Source, Layer } from 'react-map-gl';
const sample = [
    [
        {
            step: 0,
            color: "rgba(0, 0, 255, 0)"
        },
        {
            step: 0.1,
            color: "rgba(0,255,141,1)"
        },
        {
            step: 0.3,
            color: "rgba(4,133,203,1)"
        },
        {
            step: 0.5,
            color: "rgba(18,93,184,1)"
        },
        {
            step: 0.7,
            color: "rgba(42,28,152,1)"
        },
        {
            step: 1,
            color: "rgba(9,9,121,1)"
        }
    ],
    [
        {
            step: 0,
            color: "rgba(0, 0, 255, 0)"
        },
        {
            step: 0.1,
            color: "rgba(86,58,180,1)"
        },
        {
            step: 0.3,
            color: "rgba(140,48,191,1)"
        },
        {
            step: 0.5,
            color: "rgba(174,59,227,1)"
        },
        {
            step: 0.7,
            color: "rgba(255,161,29,1)"
        },
        {
            step: 1,
            color: "rgba(253,29,29,1)"
        }
    ],
    [
        {
            step: 0,
            color: "rgba(0, 0, 255, 0)"
        },
        {
            step: 0.1,
            color: "royalblue"
        },
        {
            step: 0.3,
            color: "cyan"
        },
        {
            step: 0.5,
            color: "lime"
        },
        {
            step: 0.7,
            color: "yellow"
        },
        {
            step: 1,
            color: "red"
        }
    ]
]

export default (props) => {
    const [viewport, setViewport] = useState({
        latitude: 34,
        longitude: 5,
        zoom: 1,
    })
    const [intensity, setIntensity] = useState(0.1);
    const [weight, setWeight] = useState(1);
    const [radius, setRadius] = useState(11);

    const [color, setColor] = useState([
        {
            step: 0,
            color: "rgba(0, 0, 255, 0)"
        },
        {
            step: 0.1,
            color: "royalblue"
        },
        {
            step: 0.3,
            color: "cyan"
        },
        {
            step: 0.5,
            color: "lime"
        },
        {
            step: 0.7,
            color: "yellow"
        },
        {
            step: 1,
            color: "red"
        }
    ]);


    const renderColors = () => {
        return color.map((item, i) =>
            (
                <div className='mapbox__input__row' key={i}>
                    <p>{item.step}:</p>
                    <Tooltip placement="left" title={
                        (<ChromePicker
                            color={item.color}
                            onChangeComplete={(value) => {
                                let newColor = [...color];
                                newColor[i].color = `rgba(${value.rgb.r}, ${value.rgb.g}, ${value.rgb.b}, ${value.rgb.a})`;
                                setColor(newColor);
                            }}
                            width='200'
                        />)}>
                        <Input value={item.color} />
                    </Tooltip>
                </div>
            )
        )
    }
    return (
        <div className='mapbox__container'>

            <MapGL
                {...viewport}
                onViewportChange={(viewport) => {
                    setViewport(viewport)
                }}
                width='100%'
                height='600px'
                mapStyle='mapbox://styles/kalipsik/ckb2fyfqu123n1ilb5yi7uyns/draft'
                mapboxApiAccessToken='pk.eyJ1Ijoia2FsaXBzaWsiLCJhIjoiY2p3Z3JrdjQ4MDRjdDQzcGFyeXBlN3ZtZiJ9.miVaze_snePdEvitucFWSQ'
            >
                <Source type='geojson' data={props.data}>
                    <Layer {...{
                        type: 'heatmap',
                        paint: {
                            'heatmap-intensity': intensity,
                            'heatmap-weight': weight,
                            'heatmap-radius': radius,
                            'heatmap-color': [
                                "interpolate", ["linear"], ["heatmap-density"],
                                color[0].step, color[0].color,
                                color[1].step, color[1].color,
                                color[2].step, color[2].color,
                                color[3].step, color[3].color,
                                color[4].step, color[4].color,
                                color[5].step, color[5].color,
                            ],
                            'heatmap-opacity': 1,
                        },
                    }} />
                </Source>
            </MapGL>
            <div className='mapbox__legend__range'>
                <div className='mapbox__legend__row'>
                    <label>heatmap-color</label>
                    {renderColors()}
                </div>
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
                        <span style={{ background: 'linear-gradient(90deg, rgba(9,9,121,1) 0%, rgba(42,28,152,1) 15%, rgba(18,93,184,1) 37%, rgba(4,133,203,1) 61%, rgba(2,228,233,1) 83%, rgba(0,255,141,1) 100%)' }}></span>
                    </Button>
                    <Button className="mapbox__sample__button" onClick={() => { setColor(sample[1]) }}>
                        <span style={{ background: 'linear-gradient(90deg, rgba(253,29,29,1) 0%, rgba(255,161,29,1) 20%, rgba(254,190,25,1) 36%, rgba(174,59,227,1) 51%, rgba(140,48,191,1) 73%, rgba(86,58,180,1) 90%)' }}></span>
                    </Button>
                    <Button className="mapbox__sample__button" onClick={() => { setColor(sample[2]) }}>
                        <span style={{ background: 'linear-gradient(90deg, red 0%, yellow 15%, lime 37%, rgba(4,133,203,1) 61%, cyan 83%, royalblue 100%)' }}></span>
                    </Button>
                </div>

            </div>
        </div >)
}