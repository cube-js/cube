const { TargetSource } = require('@cubejs-templates/core');
const t = require('@babel/types');
const traverse = require('@babel/traverse').default;
const generator = require('@babel/generator').default;

// const codesandboxFiles = [
//   /\/app\/app\.module\.ts/,
//   /\/app\/app\.component\.ts/,
//   /\/app\/app\.component\.html/,
// ];

const commonFiles = {
  '/app/app.module.ts': `import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { CubejsClientModule } from '@cubejs-client/ngx';
import { ChartsModule } from 'ng2-charts';

import { AppComponent } from './app.component';
import { AngularNg2Charts } from './angular-ng2-charts/chart-renderer.component';
import { AngularTestCharts } from './angular-test-charts/chart-renderer.component';

const cubejsOptions = {
  token: '\${props.token}',
  options: {
    apiUrl: '\${props.apiUrl}',
  },
};

@NgModule({
  declarations: [
    AppComponent,
    AngularNg2Charts,
    AngularTestCharts,
  ],
  imports: [
    BrowserModule,
    CubejsClientModule.forRoot(cubejsOptions),
    ChartsModule,
  ],
  providers: [],
  bootstrap: [AppComponent],
})
export class AppModule {}`,
  '/app/app.component.ts': `import { Component, OnInit } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

@Component({
  selector: 'app',
  templateUrl: './app.component.html',
  styles: [],
})
export class AppComponent implements OnInit {
  cubeQuery = new BehaviorSubject(null);
  chartType = new BehaviorSubject(null);
  pivotConfig = new BehaviorSubject(null);

  ngOnInit() {    
    this.cubeQuery.next(\${props.query});
    this.chartType.next(\${props.chartType});
    this.pivotConfig.next(\${props.pivotConfig});
  }
}
`,
  '/app/app.component.html': '<p>to do...</p>',
};

function generateCodeChunks(
  { chartingLibraries, chartingLibraryDependencies },
  props
) {
  const commonDependencies = Object.entries(commonFiles)
    .map(([fileName, content]) => {
      const ts = new TargetSource(
        fileName,
        Array.from(content.match(/import\s(.*)/g) || []).join('\n')
      );
      return ts.getImportDependencies();
    })
    .reduce((a, b) => [...a, ...b], []);

  // const allDependencies = [
    // ...commonDependencies,
    // ...chartingLibraryDependencies,
  // ];

  const chartingLibraryFiles = {};

  return `
    const chartingLibraryFiles = ${JSON.stringify(chartingLibraryFiles)};
  
    function getFiles(props) {
      return { 
        ${Object.entries(commonFiles)
          .map(([name, content]) => `'${name}': \`${content}\``)
          .join(',')} 
      }; 
    }
    
    export function getCodesandboxFiles(chartingLibrary, props) {
      let files = {
        ...getFiles(props),
        ...chartingLibraryFiles[chartingLibrary]
      };
      
      return files;
    }
    
    const commonDependencies = ${JSON.stringify(commonDependencies)};
    const chartingLibraryDependencies = ${JSON.stringify(chartingLibraryDependencies)};
    
    export function getDependencies(chartingLibrary) {
      return Array.from(new Set([
        ...commonDependencies,
        ...(chartingLibraryDependencies[chartingLibrary] || [])
      ]));
    }
  `;
}

module.exports = {
  generateCodeChunks,
};
