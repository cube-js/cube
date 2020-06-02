import { Component, OnInit } from '@angular/core';
import * as moment from 'moment';
import * as mapboxgl from 'mapbox-gl';

import cubejs from '@cubejs-client/core';
const API_URL = 'http://localhost:4000';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1ODY2MTg3NDcsImV4cCI6MTU4NjcwNTE0N30.1M3LWja51cQJ8Hgoja8joBU-Z9o6vbhtqnV72WsTAic';
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});
@Component({
  selector: 'app-choropleth',
  templateUrl: './choropleth.component.html',
  styleUrls: ['./choropleth.component.scss']
})
export class ChoroplethComponent implements OnInit {
  map: mapboxgl.Map;
  style = 'mapbox://styles/mapbox/streets-v11';
  /*fill = {
    'fill-color': {
      property: 'cases',
      stops: [
        [0, '#ebeded'],
        [1000, '#ecc1b8'],
        [5000, '#e7aba7'],
        [10000, '#e29494'],
        [50000, '#dd7a7a'],
        [100000, '#ce6567'],
        [200000, '#bb5656'],
        [300000, '#be4545'],
        [500000, '#af3636']
      ]
    }
  };
*/
  geojson = {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        properties: { name: 'Italy', cases: 7375 },
        geometry: {
          type: 'Polygon',
          coordinates: [
            [
              ['15.520376010813834', '38.23115509699147'],
              ['15.160242954171736', '37.44404551853782'],
              ['15.309897902089006', '37.1342194687318'],
              ['15.09998823411945', '36.6199872909954'],
              ['14.335228712632016', '36.996630967754754'],
              ['13.82673261887993', '37.1045313583802'],
              ['12.431003859108813', '37.61294993748382'],
              ['12.570943637755136', '38.12638113051969'],
              ['13.741156447004585', '38.03496552179536'],
              ['14.76124922044616', '38.143873602850505'],
              ['15.520376010813834', '38.23115509699147']
            ]
          ]
        }
      }
    ]
  };
  constructor() {}
  ngOnInit(): void {
    //mapboxgl.accessToken = 'pk.eyJ1Ijoia2FsaXBzaWsiLCJhIjoiY2p3Z3JrdjQ4MDRjdDQzcGFyeXBlN3ZtZiJ9.miVaze_snePdEvitucFWSQ';
    this.map = new mapboxgl.Map({
      container: 'map',
      style: this.style,
      zoom: 13,
      center: [34, 5]
    });
    // Add map controls
    this.map.addControl(new mapboxgl.NavigationControl());
  }
}
