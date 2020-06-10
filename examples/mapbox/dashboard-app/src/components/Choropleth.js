import React, { useState } from 'react';
import MapGL, { Source, Layer } from 'react-map-gl';


export default (props) => {
    const [viewport, setViewport] = useState({
        latitude: 34,
        longitude: 5,
        zoom: 1,
    })

    return (<React.Fragment>
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
                <Layer
                    beforeId='country-label'
                    id='countries'
                    type='fill'
                    paint={props.options}
                />
                <Layer {...props.text} />
            </Source>
        </MapGL></React.Fragment>)
}