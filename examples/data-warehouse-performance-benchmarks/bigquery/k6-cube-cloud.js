import http from 'k6/http';
const vus = 200;
export let options = {
  vus: vus,
  duration: '10s',
};

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
      const year = Number(getRandomInRange(1998, 1999))

      return {
        year1: year,
        year2: year + 1,
        month1: pad(getRandomInRange(1, 12), 2),
        month2: pad(getRandomInRange(1, 12), 2),
        day1: pad(getRandomInRange(1, 28), 2),
        day2: pad(getRandomInRange(1, 28), 2),
      }
    },
    query: ({ year1, year2, month1, month2, day1, day2 }) => {
      return `{"measures": ["Orders.count"],"timeDimensions": [{"dimension": "Orders.oOrderdate","granularity": "day","dateRange": ["${year1}-${month1}-${day1}","${year2}-${month2}-${day2}"]}],"order": {"Orders.count": "desc"},"dimensions": ["Orders.oOrderstatus"],"limit": 10000}`;
    },
  }
};

export default function () {
  const cubeUrl = 'https://forward-wrightstown.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1/load'
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2MzE2Mjc5OTAsImV4cCI6MTYzNDIxOTk5MH0.-lzwkP76khbyq31M2fKI9YwYYkQBR0obcS4TRwuk7Tc',
    },
  };

  const generatedData = cubeQueries.generate.data()
  const generatedQuery = cubeQueries.generate.query(generatedData)

  const payload = `{"query": ${generatedQuery} }`

  http.post(cubeUrl, payload, params);
}

/* 

curl \
  -H "Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2MzE2MjgzMjJ9.EbqKN7BOTuo9z9jqoxI6ZXdyUGQ4PvNQs1N4SRuta14" \
  -G \
  --data-urlencode 'query={"measures": ["Orders.count"],"timeDimensions": [{"dimension": "Orders.oOrderdate","granularity": "day","dateRange": ["1993-03-08","2000-04-21"]}],"order": {"Orders.count": "desc"},"dimensions": ["Orders.oOrderstatus"],"limit": 10000}' \
  https://forward-wrightstown.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1/load

*/
