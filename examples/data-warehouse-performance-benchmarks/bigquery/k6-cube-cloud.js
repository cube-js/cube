import http from 'k6/http';
import { sleep } from 'k6';

export default function () {
  const cubeUrl = 'https://irish-idalia.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1/load'
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2Mjc5ODk5NTd9.IQoJWnqtscvFe8r-dELM0ev2Rds_Rxe2h0F7-rUpES0',
    },
  };
  const payload = '{"query": {"measures": ["Events.count"],"timeDimensions": [],"order": {"Events.count": "desc"},"dimensions": ["Events.repositoryName","Events.type"],"filters": [{"member": "Events.type","operator": "equals","values": ["WatchEvent"]}],"limit": 20} }'
  http.post(cubeUrl, payload, params);
  sleep(1);
}
