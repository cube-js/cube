import http from 'k6/http';
import { sleep } from 'k6';

export default function () {
  const url = 'http://host.docker.internal:9090/';
  http.get(url);
  sleep(1);
}
