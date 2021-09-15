import http from 'k6/http';
const vus = 200;
export let options = {
  vus: vus,
  duration: '10s',
};

export default function () {
  const url = 'http://localhost:8080/';
  http.get(url);
}
