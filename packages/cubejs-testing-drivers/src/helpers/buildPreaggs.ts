import * as http from 'http';
import { clearInterval } from 'timers';

export async function postRequest(
  port: number,
  path: string,
  tkn: string,
  data: unknown,
): Promise<http.IncomingMessage> {
  return new Promise(
    (resolve: (res: http.IncomingMessage) => void, reject) => {
      const options = {
        hostname: 'localhost',
        port,
        path,
        method: 'POST',
        headers: {
          authorization: tkn,
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(JSON.stringify(data)),
        },
      };
      const req = http.request(options, (res) => {
        resolve(res);
      });
      req.on('error', (e) => {
        reject(`Problem with request: ${e.message}.`);
      });
      req.write(JSON.stringify(data));
      req.end();
    },
  );
}

export async function readData(
  res: http.IncomingMessage,
): Promise<Buffer> {
  return new Promise((resolve) => {
    let buffer = Buffer.from([]);
    res.on('data', (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, Buffer.from(chunk)]);
    });
    res.on('end', () => {
      resolve(buffer);
    });
  });
}

export async function buildPreaggs(
  port: number,
  token: string,
  selector: unknown,
) {
  return new Promise((resolve, reject) => {
    postRequest(
      port,
      '/cubejs-api/v1/pre-aggregations/jobs',
      token,
      { action: 'post', selector },
    ).then((post) => {
      readData(post).then((_jobs) => {
        const jobs = <string[]>JSON.parse(_jobs.toString());
        if (jobs.length === 0) {
          resolve(true);
        } else {
          const interval = setInterval(async () => {
            let missingOnly = true;
            const inProcess = [];
            const get = await postRequest(
              port,
              '/cubejs-api/v1/pre-aggregations/jobs',
              token,
              { action: 'get', resType: 'object', tokens: jobs },
            );
            const statuses = JSON.parse((await readData(get)).toString());
            Object.keys(statuses).forEach((t: string) => {
              const { status } = statuses[t];
              if (status.indexOf('failure') >= 0) {
                reject(`Cube pre-aggregations build failed: ${status}`);
              }
              if (status !== 'missing_partition') {
                missingOnly = false;
              }
              if (status !== 'done') {
                inProcess.push(t);
              }
              if (missingOnly) {
                reject('Cube pre-aggregations build failed: missing partitions.');
              }
            });
            if (inProcess.length === 0) {
              clearInterval(interval);
              resolve(true);
            }
          }, 1000);
          setTimeout(() => {
            clearInterval(interval);
            reject('Cube pre-aggregations build failed: timeout.');
          }, 10000);
        }
      });
    });
  });
}
