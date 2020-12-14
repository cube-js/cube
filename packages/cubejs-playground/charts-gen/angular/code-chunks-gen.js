const { TargetSource } = require('@cubejs-templates/core');
// const t = require('@babel/types');
// const traverse = require('@babel/traverse').default;
// const generator = require('@babel/generator').default;

const commonFiles = {
  'src/app/app.module.ts': `import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgxSpinnerModule } from 'ngx-spinner';
import { NgModule } from '@angular/core';
import { MatTableModule } from '@angular/material/table';
import { CubejsClientModule } from '@cubejs-client/ngx';
import { ChartsModule } from 'ng2-charts';

import { AppComponent } from './app.component';
import { QueryRendererComponent } from './query-renderer/query-renderer.component';

const cubejsOptions = {
  token: '\${props.cubejsToken}',
  options: {
    apiUrl: '\${props.apiUrl}',
  },
};

@NgModule({
  declarations: [
    AppComponent,
    QueryRendererComponent,
  ],
  imports: [
    BrowserModule,
    MatTableModule,
    CubejsClientModule.forRoot(cubejsOptions),
    ChartsModule,
    BrowserAnimationsModule,
    NgxSpinnerModule,
  ],
  providers: [],
  bootstrap: [AppComponent],
})
export class AppModule {}`,
  'src/app/app.component.ts': `import { Component, OnInit } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

@Component({
  selector: 'app-root',
  template: \\\`
    <query-renderer
      [chartType]="chartType.asObservable()"
      [cubeQuery]="cubeQuery.asObservable()"
      [pivotConfig]="pivotConfig.asObservable()"
    >\\\`,
  styles: [],
})
export class AppComponent implements OnInit {
  cubeQuery = new BehaviorSubject(null);
  chartType = new BehaviorSubject(null);
  pivotConfig = new BehaviorSubject(null);

  ngOnInit() {    
    this.cubeQuery.next(\${props.query});
    this.chartType.next('\${props.chartType}');
    this.pivotConfig.next(\${props.pivotConfig});
  }
}
`,
};

function generateCodeChunks({
  chartingLibraryDependencies,
  chartingLibraryFiles,
}) {
  const commonDependencies = Object.entries(commonFiles)
    .map(([fileName, content]) => {
      const ts = new TargetSource(
        fileName,
        Array.from(content.match(/import\s(.*)/g) || []).join('\n')
      );
      return ts.getImportDependencies();
    })
    .reduce((a, b) => [...a, ...b], [])
    .concat(['@angular/cdk', '@angular/material', '@angular/animations']);

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
      return {
        ...getFiles(props),
        ...chartingLibraryFiles[chartingLibrary]
      };
    }
    
    const commonDependencies = ${JSON.stringify(commonDependencies)};
    const chartingLibraryDependencies = ${JSON.stringify(
      chartingLibraryDependencies
    )};
    
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
