import { notification } from 'antd';
import fetch, { RequestInit, Response } from 'node-fetch';

import { PlaygroundEvent } from './types';

const bootstrapDefinition = {
  'angular-cli': {
    files: {
      'src/polyfills.ts': {
        content: `import 'core-js/proposals/reflect-metadata';
import 'zone.js/dist/zone';`,
      },
      'src/main.ts': {
        content: `import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';

import { AppModule } from './app/app.module';

platformBrowserDynamic().bootstrapModule(AppModule)
  .catch(err => console.error(err));`,
      },
      'src/index.html': {
        content: `<!DOCTYPE html>
        <html lang="en">
          <head>
            <meta charset="utf-8" />
            <title>AngularCharts</title>
            <base href="/" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <link rel="icon" type="image/x-icon" href="favicon.ico" />
          </head>
          <body>
            <app-root></app-root>
          </body>
        </html>
        `,
      },
      '.angular-cli.json': {
        content: `{
          "apps": [
            {
              "root": "src",
              "outDir": "dist",
              "index": "index.html",
              "main": "main.ts",
              "polyfills": "polyfills.ts",
              "styles": [],
              "scripts": []
            }
          ]
        }`,
      },
    },
    dependencies: {
      'zone.js': 'latest',
      '@angular/platform-browser-dynamic': 'latest',
      '@angular/platform-browser': 'latest',
      '@angular/compiler': 'latest',
      rxjs: 'latest',
      '@angular/common': 'latest',
    },
  },
  'create-react-app': {
    dependencies: {
      'react-dom': 'latest',
    },
  },
};

export function codeSandboxDefinition(template, files, dependencies = []) {
  return {
    files: {
      ...bootstrapDefinition[template]?.files,
      ...Object.entries(files)
        .map(([fileName, content]) => ({ [fileName]: { content } }))
        .reduce((a, b) => ({ ...a, ...b }), {}),
      'package.json': {
        content: {
          dependencies: {
            ...bootstrapDefinition[template]?.dependencies,
            ...dependencies.reduce((memo, d) => {
              const [name, version] = Array.isArray(d) ? d : [d, 'latest'];
              return { ...memo, [name]: version };
            }, {}),
          },
        },
      },
    },
    template,
  };
}

export function dispatchPlaygroundEvent(
  document: Document | null,
  eventType: PlaygroundEvent,
  detail: Record<string, unknown> = {}
) {
  if (!document) {
    return;
  }

  const myEvent = new CustomEvent('__cubejsPlaygroundEvent', {
    bubbles: true,
    composed: true,
    detail: {
      ...detail,
      eventType,
    },
  });

  document.dispatchEvent(myEvent);
}

type FetchPollCallbackArgs = {
  response: Response;
  retries: number;
  cancel: () => void;
};

export function fetchPoll(
  url: string,
  timeout: number,
  callback: (args: FetchPollCallbackArgs) => void,
  fetchOptions?: RequestInit
) {
  let retries: number = 0;
  let canceled: boolean = false;

  function cancel() {
    canceled = true;
  }

  async function request() {
    const response = await fetch(url, fetchOptions);

    if (!canceled) {
      callback({
        response,
        cancel,
        retries,
      });

      setTimeout(request, timeout);
    }

    retries++;
  }

  request();

  return {
    cancel,
    retries,
  };
}

export function fetchWithTimeout(
  url: string,
  options: RequestInit,
  timeout: number
): Promise<Response> {
  return Promise.race([
    fetch(url, options),
    new Promise<Response>((_, reject) =>
      setTimeout(() => reject(new Error('timeout')), timeout)
    ),
  ]);
}

export async function copyToClipboard(value, message = 'Copied to clipboard') {
  if (!navigator.clipboard) {
    notification.error({
      message: "Your browser doesn't support copy to clipboard",
    });
  }

  try {
    await navigator.clipboard.writeText(value);
    notification.success({
      message,
    });
  } catch (e) {
    notification.error({
      message: "Can't copy to clipboard",
      description: e,
    });
  }
}

export function formatNumber(num: number): string {
  return num.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,');
}
