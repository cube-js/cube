import { enableProdMode } from '@angular/core';
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';

import { AppModule } from './app/app.module';
import { environment } from './environments/environment';

import createExampleWrapper from "cube-example-wrapper";

const exampleDescription = {
  title: "Angular Dashboard",
  text: "This example shows Angular Dashboard",
  tutorialLabel: "tutorial",
  tutorialSrc: "https://angular-dashboard.cube.dev/",
  sourceCodeSrc: "https://github.com/cube-js/cube.js/tree/master/examples/angular-dashboard-with-material-ui",
};
createExampleWrapper(exampleDescription)

if (environment.production) {
  enableProdMode();
}

platformBrowserDynamic().bootstrapModule(AppModule)
  .catch(err => console.error(err));
