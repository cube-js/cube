import http from 'k6/http';
export let options = {
  vus: 200,
  duration: '10s',
};

export default function () {
  const url = 'http://localhost:9090/';
  http.get(url);
}
