import http from 'k6/http';

export const options = {
  vus: 10,
  duration: '5s',
  summaryTrendStats: [ 'min', 'med', 'p(95)', 'max' ],
  summaryTimeUnit: 'ms'
};

export default function() {
  const url = 'http://localhost:4000/cubejs-api/v1/load';

  const params = {
    headers: {
      'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.OHZOpOBVKr-sCwn8sbZ5UFsqI3uCs6e4omT7P6WVMFw',
      'Content-Type': 'application/json'
    },
  };

  const payload = {
    query: {
      measures: [
        'Orders.count'
      ],
      timeDimensions: [ {
        dimension: 'Orders.created_at',
        granularity: 'month'
      } ]
    }
  };
	
  http.post(url, JSON.stringify(payload), params);
}