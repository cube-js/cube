import http from 'k6/http';
import { sleep } from 'k6';
const vus = 30;
export let options = {
  vus: vus,
  duration: '10s',
};

export default function () {
  const url = 'http://localhost:8080/';
  http.get(url);
}
