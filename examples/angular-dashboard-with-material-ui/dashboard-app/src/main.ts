import { enableProdMode } from '@angular/core';
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';

import { AppModule } from './app/app.module';
import { environment } from './environments/environment';

import Wrapper from "cube-example-wrapper";

const root = document.querySelector("app-root")
const exampleDescription = {
  title: "Angular Dashboard",
  text: "This example shows Angular Dashboard",
  tutorialLabel: "tutorial",
  tutorialSrc: "https://angular-dashboard.cube.dev/",
  sourceCodeSrc: "https://github.com/cube-js/cube.js/tree/master/examples/angular-dashboard-with-material-ui",
};
const cubeExampleWrapper = new Wrapper(exampleDescription)
cubeExampleWrapper.render(root)


if (environment.production) {
  enableProdMode();
}

platformBrowserDynamic().bootstrapModule(AppModule)
  .catch(err => console.error(err));
