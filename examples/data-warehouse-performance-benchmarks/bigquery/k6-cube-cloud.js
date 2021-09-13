import http from 'k6/http';
import { sleep } from 'k6';

function getRandomInRange(min, max) {
	const rand = min + Math.round(Math.random() * (max - min));
  return rand < 10 ? '0'.concat(rand) : String(rand);
}

function pad(n, width, z) {
  z = z || '0';
  n = n + '';
  return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
}

const cubeQueries = {
  generate: {
    data: () => {
      return {
        month1: pad(getRandomInRange(4, 7), 2),
        month2: pad(getRandomInRange(4, 7), 2),
        day1: pad(getRandomInRange(1, 28), 2),
        day2: pad(getRandomInRange(1, 28), 2),
      }
    },
    query: ({ month1, month2, day1, day2 }) => {
      return `{"measures": ["Events.count"],"timeDimensions": [{"dimension": "Events.createdAt","granularity": "day","dateRange": ["2012-${month1}-${day1}","2012-${month2}-${day2}"]}],"order": {"Events.count": "desc"},"dimensions": ["Events.repositoryName"],"filters": [{"member": "Events.type","operator": "equals","values": ["WatchEvent"]}],"limit": 1000}`;
    },
  }
};

export default function () {
  const cubeUrl = 'https://irish-idalia.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1/load'
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2Mjc5ODk5NTd9.IQoJWnqtscvFe8r-dELM0ev2Rds_Rxe2h0F7-rUpES0',
    },
  };

  const generatedData = cubeQueries.generate.data()
  const generatedQuery = cubeQueries.generate.query(generatedData)

  const payload = `{"query": ${generatedQuery} }`
  http.post(cubeUrl, payload, params);
  sleep(1);
}
