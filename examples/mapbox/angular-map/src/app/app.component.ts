import { Component } from '@angular/core';
import * as moment from 'moment';

import cubejs from '@cubejs-client/core';
const API_URL = 'http://localhost:4000';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1ODY2MTg3NDcsImV4cCI6MTU4NjcwNTE0N30.1M3LWja51cQJ8Hgoja8joBU-Z9o6vbhtqnV72WsTAic';
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  dates = {
    min: 0,
    max: 0,
    current: 0
  };

  tipFormatter = value => {
    return moment.unix(value).format('YYYY-MM-DD');
  };

  ngOnInit() {
    cubejsApi
      .load({
        measures: ['stats.startDate', 'stats.endDate']
      })
      .then(resultSet => {
        this.dates = {
          min: resultSet.tablePivot()[0]['stats.startDate'],
          max: resultSet.tablePivot()[0]['stats.endDate'],
          current: resultSet.tablePivot()[0]['stats.endDate']
        };
      });
  }
}
