export function codeSandboxDefinition(template, files, dependencies = []) {
  return {
    files: {
      'src/polyfills.ts': {
        content: `import 'zone.js/dist/zone';`,
      },
      'src/main.ts': {
        content: `import { enableProdMode } from '@angular/core';
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';

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

      ...Object.entries(files)
        .map(([fileName, content]) => ({ [fileName]: { content } }))
        .reduce((a, b) => ({ ...a, ...b }), {}),
      'package.json': {
        content: {
          dependencies: {
            // 'react-dom': 'latest',

            'zone.js': 'latest',
            '@angular/platform-browser-dynamic': 'latest',
            '@angular/platform-browser': 'latest',
            '@angular/compiler': 'latest',
            rxjs: 'latest',
            '@angular/common': 'latest',

            ...dependencies.reduce(
              (memo, d) => ({ ...memo, [d]: 'latest' }),
              {}
            ),
          },
        },
      },
    },
    template: 'angular-cli',
  };
}
