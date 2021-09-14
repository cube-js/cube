import http from 'k6/http';
import { sleep } from 'k6';
export let options = {
  vus: 30,
  duration: '10s',
};

export default function () {
  const url = 'http://localhost:9090/';
  http.get(url);
}
