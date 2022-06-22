import { enableProdMode } from '@angular/core';
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';

import { AppModule } from './app/app.module';
import { environment } from './environments/environment';

import createExampleWrapper from "cube-example-wrapper"
const exampleDescription = {
  title: "Angular Dashboard with Material",
  text: `
  <p>This live demo shows a Material dashboard created with Angular and Cube.</p>
  <p>Follow the <a href="https://angular-dashboard.cube.dev/">tutorial</a> or explore the
    <a href="https://github.com/cube-js/cube.js/tree/master/examples/angular-dashboard-with-material-ui">source code</a>
    to learn more.
  </p>`
}

createExampleWrapper(exampleDescription)

if (environment.production) {
  enableProdMode();
}

platformBrowserDynamic().bootstrapModule(AppModule)
  .catch(err => console.error(err));
