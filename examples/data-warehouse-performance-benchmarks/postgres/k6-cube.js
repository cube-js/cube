import http from 'k6/http';

const vus = __ENV.vus || 10;
export let options = {
  vus,
  duration: '5s',
};

export default function () {
  const url = 'http://localhost:9090/';
  http.get(url);
}

