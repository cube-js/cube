(window["webpackJsonp"] = window["webpackJsonp"] || []).push([["main"],{

/***/ 0:
/*!***************************!*\
  !*** multi ./src/main.ts ***!
  \***************************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

module.exports = __webpack_require__(/*! /Users/alex/Projects/cube.js/packages/cubejs-playground/charts-dist/angular/angular-charts/src/main.ts */"zUnb");


/***/ }),

/***/ "6WBr":
/*!****************************!*\
  !*** ./src/code-chunks.ts ***!
  \****************************/
/*! exports provided: getCodesandboxFiles, getDependencies */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
__webpack_require__.r(__webpack_exports__);
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "getCodesandboxFiles", function() { return getCodesandboxFiles; });
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "getDependencies", function() { return getDependencies; });
const chartingLibraryFiles = { "angular-ng2-charts": { "src/app/query-renderer/query-renderer.component.html": "<ngx-spinner\n  bdColor=\"rgba(0,0,0,0)\"\n  size=\"default\"\n  color=\"#000\"\n  type=\"ball-spin\"\n  [fullScreen]=\"false\"\n></ngx-spinner>\n\n<div *ngIf=\"error\">{{ error }}</div>\n\n<div *ngIf=\"isQueryPresent && !loading\" style=\"height: 100%\">\n  <div\n    *ngIf=\"chartType !== 'table' && chartType !== 'number'\"\n    class=\"chart-container\"\n  >\n    <canvas\n      *ngIf=\"chartType === 'line'\"\n      baseChart\n      legend=\"true\"\n      chartType=\"line\"\n      [datasets]=\"chartData\"\n      [labels]=\"chartLabels\"\n      [options]=\"noFillChartOptions\"\n    >\n    </canvas>\n\n    <canvas\n      *ngIf=\"chartType === 'area'\"\n      baseChart\n      legend=\"true\"\n      chartType=\"line\"\n      [datasets]=\"chartData\"\n      [labels]=\"chartLabels\"\n      [options]=\"chartOptions\"\n    >\n    </canvas>\n\n    <canvas\n      *ngIf=\"chartType === 'bar'\"\n      baseChart\n      legend=\"true\"\n      chartType=\"bar\"\n      [datasets]=\"chartData\"\n      [labels]=\"chartLabels\"\n      [options]=\"chartOptions\"\n    >\n    </canvas>\n\n    <canvas\n      *ngIf=\"chartType === 'pie'\"\n      baseChart\n      legend=\"true\"\n      chartType=\"pie\"\n      [datasets]=\"chartData\"\n      [labels]=\"chartLabels\"\n      [options]=\"chartOptions\"\n    >\n    </canvas>\n  </div>\n\n  <table\n    *ngIf=\"chartType === 'table'\"\n    mat-table\n    [dataSource]=\"tableData\"\n    style=\"width: 100%\"\n  >\n    <ng-container\n      *ngFor=\"let column of displayedColumns; let index = index\"\n      [matColumnDef]=\"column\"\n    >\n      <th mat-header-cell *matHeaderCellDef>\n        {{ columnTitles[index] }}\n      </th>\n      <td mat-cell *matCellDef=\"let element\">{{ element[column] }}</td>\n    </ng-container>\n\n    <tr mat-header-row *matHeaderRowDef=\"displayedColumns\"></tr>\n    <tr mat-row *matRowDef=\"let row; columns: displayedColumns\"></tr>\n  </table>\n\n  <div class=\"numeric-container\" *ngIf=\"chartType === 'number'\">\n    <h2 *ngFor=\"let value of numericValues\">\n      {{ value | number }}\n    </h2>\n  </div>\n</div>\n", "src/app/query-renderer/query-renderer.component.ts": "import { Component, Input } from '@angular/core';\nimport { isQueryPresent, ResultSet } from '@cubejs-client/core';\nimport { CubejsClient, TChartType } from '@cubejs-client/ngx';\nimport { BehaviorSubject, combineLatest, of, merge } from 'rxjs';\nimport { catchError, switchMap } from 'rxjs/operators';\nimport { ChartDataSets, ChartOptions } from 'chart.js';\nimport { Label } from 'ng2-charts';\nimport { getDisplayedColumns, flattenColumns } from './utils';\nimport { NgxSpinnerService } from 'ngx-spinner';\n\n@Component({\n  selector: 'query-renderer',\n  templateUrl: './query-renderer.component.html',\n  styles: [\n    `\n      .chart-container {\n        position: relative;\n        height: calc(100% - 52px);\n        min-height: 400px;\n      }\n\n      .numeric-container {\n        display: flex;\n        align-items: center;\n        justify-content: center;\n        flex-direction: column;\n        height: 100%;\n      }\n\n      :host {\n        position: absolute;\n        top: 0;\n        bottom: 0;\n        left: 0;\n        right: 0;\n      }\n    `,\n  ],\n})\nexport class QueryRendererComponent {\n  @Input('cubeQuery')\n  cubeQuery$: BehaviorSubject<any>;\n\n  @Input('pivotConfig')\n  pivotConfig$: any;\n\n  @Input('chartType')\n  chartType$: any;\n\n  chartType: any = null;\n  isQueryPresent = false;\n  error: string | null = null;\n\n  displayedColumns: string[] = [];\n  tableData: any[] = [];\n  columnTitles: string[] = [];\n  chartData: ChartDataSets[] = [];\n  chartLabels: Label[] = [];\n  chartOptions: ChartOptions = {\n    responsive: true,\n    maintainAspectRatio: false,\n  };\n  noFillChartOptions: ChartOptions = {\n    responsive: true,\n    maintainAspectRatio: false,\n    elements: {\n      line: {\n        fill: false,\n      },\n    },\n  };\n  numericValues: number[] = [];\n  loading = false;\n\n  constructor(\n    private cubejsClient: CubejsClient,\n    private spinner: NgxSpinnerService\n  ) {}\n\n  ngOnInit() {\n    combineLatest([\n      this.cubeQuery$.pipe(\n        switchMap((cubeQuery) => {\n          return of(isQueryPresent(cubeQuery || {}));\n        })\n      ),\n      this.cubeQuery$.pipe(\n        switchMap((cubeQuery) => {\n          this.error = null;\n          if (!isQueryPresent(cubeQuery || {})) {\n            return of(null);\n          }\n          this.loading = true;\n          this.spinner.show();\n\n          return merge(\n            of(null),\n            this.cubejsClient.load(cubeQuery).pipe(\n              catchError((error) => {\n                this.spinner.hide();\n                this.error = error.toString();\n                console.error(error);\n                return of(null);\n              })\n            )\n          );\n        })\n      ),\n      this.pivotConfig$,\n      this.chartType$,\n    ]).subscribe(\n      ([isQueryPresent, resultSet, pivotConfig, chartType]: [\n        boolean,\n        ResultSet,\n        any,\n        TChartType\n      ]) => {\n        this.chartType = chartType;\n        this.isQueryPresent = isQueryPresent;\n\n        if (resultSet != null) {\n          this.spinner.hide();\n          this.loading = false;\n\n          const { onQueryLoad } =\n            window.parent.window['__cubejsPlayground'] || {};\n          if (typeof onQueryLoad === 'function') {\n            onQueryLoad(resultSet);\n          }\n        }\n\n        if (resultSet) {\n          if (this.chartType === 'table') {\n            this.updateTableData(resultSet, pivotConfig);\n          } else if (this.chartType === 'number') {\n            this.updateNumericData(resultSet);\n          } else {\n            this.updateChartData(resultSet, pivotConfig);\n          }\n        }\n      }\n    );\n  }\n\n  updateChartData(resultSet, pivotConfig) {\n    this.chartData = resultSet.series(pivotConfig).map((item) => {\n      return {\n        label: item.title,\n        data: item.series.map(({ value }) => value),\n        stack: 'a',\n      };\n    });\n    this.chartLabels = resultSet.chartPivot(pivotConfig).map((row) => row.x);\n  }\n\n  updateTableData(resultSet, pivotConfig) {\n    this.tableData = resultSet.tablePivot(pivotConfig);\n    this.displayedColumns = getDisplayedColumns(\n      resultSet.tableColumns(pivotConfig)\n    );\n    this.columnTitles = flattenColumns(resultSet.tableColumns(pivotConfig));\n  }\n\n  updateNumericData(resultSet) {\n    this.numericValues = resultSet\n      .seriesNames()\n      .map((s) => resultSet.totalRow()[s.key]);\n  }\n}\n", "src/app/query-renderer/utils.ts": "import { TableColumn } from '@cubejs-client/core';\n\nexport function getDisplayedColumns(tableColumns: TableColumn[]): string[] {\n  const queue = tableColumns;\n  const columns = [];\n\n  while (queue.length) {\n    const column = queue.shift();\n    if (column.dataIndex) {\n      columns.push(column.dataIndex);\n    }\n    if ((column.children || []).length) {\n      column.children.map((child) => queue.push(child));\n    }\n  }\n\n  return columns;\n}\n\nexport function flattenColumns(columns: TableColumn[] = []) {\n  return columns.reduce((memo, column) => {\n    const titles = flattenColumns(column.children);\n    return [\n      ...memo,\n      ...(titles.length\n        ? titles.map((title) => column.shortTitle + ', ' + title)\n        : [column.shortTitle]),\n    ];\n  }, []);\n}\n" }, "angular-test-charts": { "src/app/query-renderer/query-renderer.component.html": "<div>angular-test-charts</div>\n", "src/app/query-renderer/query-renderer.component.ts": "import { Component, Input } from '@angular/core';\nimport { CubejsClient } from '@cubejs-client/ngx';\nimport { ChartDataSets, ChartOptions } from 'chart.js';\nimport { Label } from 'ng2-charts';\nimport { BehaviorSubject } from 'rxjs';\n\n@Component({\n  selector: 'query-renderer',\n  templateUrl: './query-renderer.component.html',\n  styles: [],\n})\nexport class QueryRendererComponent {\n  @Input('cubeQuery')\n  cubeQuery$: BehaviorSubject<any>;\n\n  @Input('pivotConfig')\n  pivotConfig$: any;\n\n  @Input('chartType')\n  chartType$: any;\n\n  chartType: any = null;\n  isQueryPresent = false;\n\n  displayedColumns: string[] = [];\n  tableData: any[] = [];\n  columnTitles: string[] = [];\n  chartData: ChartDataSets[] = [];\n  chartLabels: Label[] = [];\n  chartOptions: ChartOptions = {\n    responsive: true,\n    maintainAspectRatio: false,\n  };\n  noFillChartOptions: ChartOptions = {\n    responsive: true,\n    maintainAspectRatio: false,\n    elements: {\n      line: {\n        fill: false,\n      },\n    },\n  };\n\n  constructor(private cubejsClient: CubejsClient) {}\n\n  ngOnInit() {}\n\n  updateChartData(resultSet, pivotConfig) {\n    if (!resultSet) {\n      return;\n    }\n  }\n}\n" } };
function getFiles(props) {
    return {
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
  token: '${props.cubejsToken}',
  options: {
    apiUrl: '${props.apiUrl}',
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
export class AppModule {}`, 'src/app/app.component.ts': `import { Component, OnInit } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

@Component({
  selector: 'app-root',
  template: \`
    <query-renderer
      [chartType]="chartType.asObservable()"
      [cubeQuery]="cubeQuery.asObservable()"
      [pivotConfig]="pivotConfig.asObservable()"
    >\`,
  styles: [],
})
export class AppComponent implements OnInit {
  cubeQuery = new BehaviorSubject(null);
  chartType = new BehaviorSubject(null);
  pivotConfig = new BehaviorSubject(null);

  ngOnInit() {    
    this.cubeQuery.next(${props.query});
    this.chartType.next('${props.chartType}');
    this.pivotConfig.next(${props.pivotConfig});
  }
}
`
    };
}
function getCodesandboxFiles(chartingLibrary, props) {
    return Object.assign(Object.assign({}, getFiles(props)), chartingLibraryFiles[chartingLibrary]);
}
const commonDependencies = ["@angular/platform-browser", "@angular/platform-browser", "ngx-spinner", "@angular/core", "@angular/material", "@cubejs-client/ngx", "ng2-charts", "@angular/core", "rxjs", "@angular/cdk", "@angular/material", "@angular/animations"];
const chartingLibraryDependencies = { "angular-ng2-charts": ["@angular/core", "@cubejs-client/core", "@cubejs-client/ngx", "rxjs", "rxjs", "chart.js", "ng2-charts", "ngx-spinner", "@cubejs-client/core"], "angular-test-charts": ["@angular/core", "@cubejs-client/ngx", "chart.js", "ng2-charts", "rxjs"] };
function getDependencies(chartingLibrary) {
    return Array.from(new Set([
        ...commonDependencies,
        ...(chartingLibraryDependencies[chartingLibrary] || [])
    ]));
}


/***/ }),

/***/ "AytR":
/*!*****************************************!*\
  !*** ./src/environments/environment.ts ***!
  \*****************************************/
/*! exports provided: environment */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
__webpack_require__.r(__webpack_exports__);
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "environment", function() { return environment; });
// This file can be replaced during build by using the `fileReplacements` array.
// `ng build --prod` replaces `environment.ts` with `environment.prod.ts`.
// The list of file replacements can be found in `angular.json`.
const environment = {
    production: false,
};
/*
 * For easier debugging in development mode, you can import the following file
 * to ignore zone related error stack frames such as `zone.run`, `zoneDelegate.invokeTask`.
 *
 * This import should be commented out in production mode because it will have a negative impact
 * on performance if an error is thrown.
 */
// import 'zone.js/dist/zone-error';  // Included with Angular CLI.


/***/ }),

/***/ "CflG":
/*!*****************************************************************!*\
  !*** ./src/app/angular-test-charts/query-renderer.component.ts ***!
  \*****************************************************************/
/*! exports provided: AngularTestCharts */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
__webpack_require__.r(__webpack_exports__);
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "AngularTestCharts", function() { return AngularTestCharts; });
/* harmony import */ var _angular_core__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! @angular/core */ "fXoL");
/* harmony import */ var _cubejs_client_ngx__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! @cubejs-client/ngx */ "lC/H");


class AngularTestCharts {
    constructor(cubejsClient) {
        this.cubejsClient = cubejsClient;
        this.chartType = null;
        this.isQueryPresent = false;
        this.displayedColumns = [];
        this.tableData = [];
        this.columnTitles = [];
        this.chartData = [];
        this.chartLabels = [];
        this.chartOptions = {
            responsive: true,
            maintainAspectRatio: false,
        };
        this.noFillChartOptions = {
            responsive: true,
            maintainAspectRatio: false,
            elements: {
                line: {
                    fill: false,
                },
            },
        };
    }
    ngOnInit() { }
    updateChartData(resultSet, pivotConfig) {
        if (!resultSet) {
            return;
        }
    }
}
AngularTestCharts.ɵfac = function AngularTestCharts_Factory(t) { return new (t || AngularTestCharts)(_angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵdirectiveInject"](_cubejs_client_ngx__WEBPACK_IMPORTED_MODULE_1__["CubejsClient"])); };
AngularTestCharts.ɵcmp = _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵdefineComponent"]({ type: AngularTestCharts, selectors: [["angular-test-charts"]], inputs: { cubeQuery$: ["cubeQuery", "cubeQuery$"], pivotConfig$: ["pivotConfig", "pivotConfig$"], chartType$: ["chartType", "chartType$"] }, decls: 2, vars: 0, template: function AngularTestCharts_Template(rf, ctx) { if (rf & 1) {
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](0, "div");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵtext"](1, "angular-test-charts");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
    } }, encapsulation: 2 });


/***/ }),

/***/ "DSVJ":
/*!****************************************************************!*\
  !*** ./src/app/angular-ng2-charts/query-renderer.component.ts ***!
  \****************************************************************/
/*! exports provided: AngularNg2Charts */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
__webpack_require__.r(__webpack_exports__);
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "AngularNg2Charts", function() { return AngularNg2Charts; });
/* harmony import */ var _cubejs_client_core__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! @cubejs-client/core */ "Fkrf");
/* harmony import */ var _cubejs_client_core__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(_cubejs_client_core__WEBPACK_IMPORTED_MODULE_0__);
/* harmony import */ var rxjs__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! rxjs */ "qCKp");
/* harmony import */ var rxjs_operators__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! rxjs/operators */ "kU1M");
/* harmony import */ var _utils__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(/*! ./utils */ "Hpwl");
/* harmony import */ var _angular_core__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(/*! @angular/core */ "fXoL");
/* harmony import */ var _cubejs_client_ngx__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(/*! @cubejs-client/ngx */ "lC/H");
/* harmony import */ var ngx_spinner__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(/*! ngx-spinner */ "JqCM");
/* harmony import */ var _angular_common__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(/*! @angular/common */ "ofXK");
/* harmony import */ var ng2_charts__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__(/*! ng2-charts */ "LPYB");
/* harmony import */ var _angular_material_table__WEBPACK_IMPORTED_MODULE_9__ = __webpack_require__(/*! @angular/material/table */ "+0xr");










function AngularNg2Charts_div_1_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementStart"](0, "div");
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtext"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementEnd"]();
} if (rf & 2) {
    const ctx_r0 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"]();
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtextInterpolate"](ctx_r0.error);
} }
function AngularNg2Charts_div_2_div_1_canvas_1_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelement"](0, "canvas", 11);
} if (rf & 2) {
    const ctx_r5 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"](3);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("datasets", ctx_r5.chartData)("labels", ctx_r5.chartLabels)("options", ctx_r5.noFillChartOptions);
} }
function AngularNg2Charts_div_2_div_1_canvas_2_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelement"](0, "canvas", 11);
} if (rf & 2) {
    const ctx_r6 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"](3);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("datasets", ctx_r6.chartData)("labels", ctx_r6.chartLabels)("options", ctx_r6.chartOptions);
} }
function AngularNg2Charts_div_2_div_1_canvas_3_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelement"](0, "canvas", 12);
} if (rf & 2) {
    const ctx_r7 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"](3);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("datasets", ctx_r7.chartData)("labels", ctx_r7.chartLabels)("options", ctx_r7.chartOptions);
} }
function AngularNg2Charts_div_2_div_1_canvas_4_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelement"](0, "canvas", 13);
} if (rf & 2) {
    const ctx_r8 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"](3);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("datasets", ctx_r8.chartData)("labels", ctx_r8.chartLabels)("options", ctx_r8.chartOptions);
} }
function AngularNg2Charts_div_2_div_1_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementStart"](0, "div", 7);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](1, AngularNg2Charts_div_2_div_1_canvas_1_Template, 1, 3, "canvas", 8);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](2, AngularNg2Charts_div_2_div_1_canvas_2_Template, 1, 3, "canvas", 8);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](3, AngularNg2Charts_div_2_div_1_canvas_3_Template, 1, 3, "canvas", 9);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](4, AngularNg2Charts_div_2_div_1_canvas_4_Template, 1, 3, "canvas", 10);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementEnd"]();
} if (rf & 2) {
    const ctx_r2 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"](2);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("ngIf", ctx_r2.chartType === "line");
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("ngIf", ctx_r2.chartType === "area");
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("ngIf", ctx_r2.chartType === "bar");
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("ngIf", ctx_r2.chartType === "pie");
} }
function AngularNg2Charts_div_2_table_2_ng_container_1_th_1_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementStart"](0, "th", 21);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtext"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementEnd"]();
} if (rf & 2) {
    const index_r13 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"]().index;
    const ctx_r14 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"](3);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtextInterpolate1"](" ", ctx_r14.columnTitles[index_r13], " ");
} }
function AngularNg2Charts_div_2_table_2_ng_container_1_td_2_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementStart"](0, "td", 22);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtext"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementEnd"]();
} if (rf & 2) {
    const element_r17 = ctx.$implicit;
    const column_r12 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"]().$implicit;
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtextInterpolate"](element_r17[column_r12]);
} }
function AngularNg2Charts_div_2_table_2_ng_container_1_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementContainerStart"](0, 18);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](1, AngularNg2Charts_div_2_table_2_ng_container_1_th_1_Template, 2, 1, "th", 19);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](2, AngularNg2Charts_div_2_table_2_ng_container_1_td_2_Template, 2, 1, "td", 20);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementContainerEnd"]();
} if (rf & 2) {
    const column_r12 = ctx.$implicit;
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("matColumnDef", column_r12);
} }
function AngularNg2Charts_div_2_table_2_tr_2_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelement"](0, "tr", 23);
} }
function AngularNg2Charts_div_2_table_2_tr_3_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelement"](0, "tr", 24);
} }
function AngularNg2Charts_div_2_table_2_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementStart"](0, "table", 14);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](1, AngularNg2Charts_div_2_table_2_ng_container_1_Template, 3, 1, "ng-container", 15);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](2, AngularNg2Charts_div_2_table_2_tr_2_Template, 1, 0, "tr", 16);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](3, AngularNg2Charts_div_2_table_2_tr_3_Template, 1, 0, "tr", 17);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementEnd"]();
} if (rf & 2) {
    const ctx_r3 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"](2);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("dataSource", ctx_r3.tableData);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("ngForOf", ctx_r3.displayedColumns);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("matHeaderRowDef", ctx_r3.displayedColumns);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("matRowDefColumns", ctx_r3.displayedColumns);
} }
function AngularNg2Charts_div_2_div_3_h2_1_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementStart"](0, "h2");
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtext"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵpipe"](2, "number");
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementEnd"]();
} if (rf & 2) {
    const value_r21 = ctx.$implicit;
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtextInterpolate1"](" ", _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵpipeBind1"](2, 1, value_r21), " ");
} }
function AngularNg2Charts_div_2_div_3_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementStart"](0, "div", 25);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](1, AngularNg2Charts_div_2_div_3_h2_1_Template, 3, 3, "h2", 26);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementEnd"]();
} if (rf & 2) {
    const ctx_r4 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"](2);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("ngForOf", ctx_r4.numericValues);
} }
function AngularNg2Charts_div_2_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementStart"](0, "div", 3);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](1, AngularNg2Charts_div_2_div_1_Template, 5, 4, "div", 4);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](2, AngularNg2Charts_div_2_table_2_Template, 4, 4, "table", 5);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](3, AngularNg2Charts_div_2_div_3_Template, 2, 1, "div", 6);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelementEnd"]();
} if (rf & 2) {
    const ctx_r1 = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵnextContext"]();
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("ngIf", ctx_r1.chartType !== "table" && ctx_r1.chartType !== "number");
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("ngIf", ctx_r1.chartType === "table");
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
    _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("ngIf", ctx_r1.chartType === "number");
} }
class AngularNg2Charts {
    constructor(cubejsClient, spinner) {
        this.cubejsClient = cubejsClient;
        this.spinner = spinner;
        this.chartType = null;
        this.isQueryPresent = false;
        this.error = null;
        this.displayedColumns = [];
        this.tableData = [];
        this.columnTitles = [];
        this.chartData = [];
        this.chartLabels = [];
        this.chartOptions = {
            responsive: true,
            maintainAspectRatio: false,
        };
        this.noFillChartOptions = {
            responsive: true,
            maintainAspectRatio: false,
            elements: {
                line: {
                    fill: false,
                },
            },
        };
        this.numericValues = [];
        this.loading = false;
    }
    ngOnInit() {
        Object(rxjs__WEBPACK_IMPORTED_MODULE_1__["combineLatest"])([
            this.cubeQuery$.pipe(Object(rxjs_operators__WEBPACK_IMPORTED_MODULE_2__["switchMap"])((cubeQuery) => {
                return Object(rxjs__WEBPACK_IMPORTED_MODULE_1__["of"])(Object(_cubejs_client_core__WEBPACK_IMPORTED_MODULE_0__["isQueryPresent"])(cubeQuery || {}));
            })),
            this.cubeQuery$.pipe(Object(rxjs_operators__WEBPACK_IMPORTED_MODULE_2__["switchMap"])((cubeQuery) => {
                this.error = null;
                if (!Object(_cubejs_client_core__WEBPACK_IMPORTED_MODULE_0__["isQueryPresent"])(cubeQuery || {})) {
                    return Object(rxjs__WEBPACK_IMPORTED_MODULE_1__["of"])(null);
                }
                this.loading = true;
                this.spinner.show();
                return Object(rxjs__WEBPACK_IMPORTED_MODULE_1__["merge"])(Object(rxjs__WEBPACK_IMPORTED_MODULE_1__["of"])(null), this.cubejsClient.load(cubeQuery).pipe(Object(rxjs_operators__WEBPACK_IMPORTED_MODULE_2__["catchError"])((error) => {
                    this.spinner.hide();
                    this.error = error.toString();
                    console.error(error);
                    return Object(rxjs__WEBPACK_IMPORTED_MODULE_1__["of"])(null);
                })));
            })),
            this.pivotConfig$,
            this.chartType$,
        ]).subscribe(([isQueryPresent, resultSet, pivotConfig, chartType]) => {
            this.chartType = chartType;
            this.isQueryPresent = isQueryPresent;
            if (resultSet != null) {
                this.spinner.hide();
                this.loading = false;
                const { onQueryLoad } = window.parent.window['__cubejsPlayground'] || {};
                if (typeof onQueryLoad === 'function') {
                    onQueryLoad(resultSet);
                }
            }
            if (resultSet) {
                if (this.chartType === 'table') {
                    this.updateTableData(resultSet, pivotConfig);
                }
                else if (this.chartType === 'number') {
                    this.updateNumericData(resultSet);
                }
                else {
                    this.updateChartData(resultSet, pivotConfig);
                }
            }
        });
    }
    updateChartData(resultSet, pivotConfig) {
        this.chartData = resultSet.series(pivotConfig).map((item) => {
            return {
                label: item.title,
                data: item.series.map(({ value }) => value),
                stack: 'a',
            };
        });
        this.chartLabels = resultSet.chartPivot(pivotConfig).map((row) => row.x);
    }
    updateTableData(resultSet, pivotConfig) {
        this.tableData = resultSet.tablePivot(pivotConfig);
        this.displayedColumns = Object(_utils__WEBPACK_IMPORTED_MODULE_3__["getDisplayedColumns"])(resultSet.tableColumns(pivotConfig));
        this.columnTitles = Object(_utils__WEBPACK_IMPORTED_MODULE_3__["flattenColumns"])(resultSet.tableColumns(pivotConfig));
    }
    updateNumericData(resultSet) {
        this.numericValues = resultSet
            .seriesNames()
            .map((s) => resultSet.totalRow()[s.key]);
    }
}
AngularNg2Charts.ɵfac = function AngularNg2Charts_Factory(t) { return new (t || AngularNg2Charts)(_angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵdirectiveInject"](_cubejs_client_ngx__WEBPACK_IMPORTED_MODULE_5__["CubejsClient"]), _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵdirectiveInject"](ngx_spinner__WEBPACK_IMPORTED_MODULE_6__["NgxSpinnerService"])); };
AngularNg2Charts.ɵcmp = _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵdefineComponent"]({ type: AngularNg2Charts, selectors: [["angular-ng2-charts"]], inputs: { cubeQuery$: ["cubeQuery", "cubeQuery$"], pivotConfig$: ["pivotConfig", "pivotConfig$"], chartType$: ["chartType", "chartType$"] }, decls: 3, vars: 3, consts: [["bdColor", "rgba(0,0,0,0)", "size", "default", "color", "#000", "type", "ball-spin", 3, "fullScreen"], [4, "ngIf"], ["style", "height: 100%", 4, "ngIf"], [2, "height", "100%"], ["class", "chart-container", 4, "ngIf"], ["mat-table", "", "style", "width: 100%", 3, "dataSource", 4, "ngIf"], ["class", "numeric-container", 4, "ngIf"], [1, "chart-container"], ["baseChart", "", "legend", "true", "chartType", "line", 3, "datasets", "labels", "options", 4, "ngIf"], ["baseChart", "", "legend", "true", "chartType", "bar", 3, "datasets", "labels", "options", 4, "ngIf"], ["baseChart", "", "legend", "true", "chartType", "pie", 3, "datasets", "labels", "options", 4, "ngIf"], ["baseChart", "", "legend", "true", "chartType", "line", 3, "datasets", "labels", "options"], ["baseChart", "", "legend", "true", "chartType", "bar", 3, "datasets", "labels", "options"], ["baseChart", "", "legend", "true", "chartType", "pie", 3, "datasets", "labels", "options"], ["mat-table", "", 2, "width", "100%", 3, "dataSource"], [3, "matColumnDef", 4, "ngFor", "ngForOf"], ["mat-header-row", "", 4, "matHeaderRowDef"], ["mat-row", "", 4, "matRowDef", "matRowDefColumns"], [3, "matColumnDef"], ["mat-header-cell", "", 4, "matHeaderCellDef"], ["mat-cell", "", 4, "matCellDef"], ["mat-header-cell", ""], ["mat-cell", ""], ["mat-header-row", ""], ["mat-row", ""], [1, "numeric-container"], [4, "ngFor", "ngForOf"]], template: function AngularNg2Charts_Template(rf, ctx) { if (rf & 1) {
        _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵelement"](0, "ngx-spinner", 0);
        _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](1, AngularNg2Charts_div_1_Template, 2, 1, "div", 1);
        _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵtemplate"](2, AngularNg2Charts_div_2_Template, 4, 3, "div", 2);
    } if (rf & 2) {
        _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("fullScreen", false);
        _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
        _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("ngIf", ctx.error);
        _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵadvance"](1);
        _angular_core__WEBPACK_IMPORTED_MODULE_4__["ɵɵproperty"]("ngIf", ctx.isQueryPresent && !ctx.loading);
    } }, directives: [ngx_spinner__WEBPACK_IMPORTED_MODULE_6__["NgxSpinnerComponent"], _angular_common__WEBPACK_IMPORTED_MODULE_7__["NgIf"], ng2_charts__WEBPACK_IMPORTED_MODULE_8__["BaseChartDirective"], _angular_material_table__WEBPACK_IMPORTED_MODULE_9__["MatTable"], _angular_common__WEBPACK_IMPORTED_MODULE_7__["NgForOf"], _angular_material_table__WEBPACK_IMPORTED_MODULE_9__["MatHeaderRowDef"], _angular_material_table__WEBPACK_IMPORTED_MODULE_9__["MatRowDef"], _angular_material_table__WEBPACK_IMPORTED_MODULE_9__["MatColumnDef"], _angular_material_table__WEBPACK_IMPORTED_MODULE_9__["MatHeaderCellDef"], _angular_material_table__WEBPACK_IMPORTED_MODULE_9__["MatCellDef"], _angular_material_table__WEBPACK_IMPORTED_MODULE_9__["MatHeaderCell"], _angular_material_table__WEBPACK_IMPORTED_MODULE_9__["MatCell"], _angular_material_table__WEBPACK_IMPORTED_MODULE_9__["MatHeaderRow"], _angular_material_table__WEBPACK_IMPORTED_MODULE_9__["MatRow"]], pipes: [_angular_common__WEBPACK_IMPORTED_MODULE_7__["DecimalPipe"]], styles: [".chart-container[_ngcontent-%COMP%] {\n        position: relative;\n        height: calc(100% - 52px);\n        min-height: 400px;\n      }\n\n      .numeric-container[_ngcontent-%COMP%] {\n        display: flex;\n        align-items: center;\n        justify-content: center;\n        flex-direction: column;\n        height: 100%;\n      }\n\n      [_nghost-%COMP%] {\n        position: absolute;\n        top: 0;\n        bottom: 0;\n        left: 0;\n        right: 0;\n      }"] });


/***/ }),

/***/ "Hpwl":
/*!*********************************************!*\
  !*** ./src/app/angular-ng2-charts/utils.ts ***!
  \*********************************************/
/*! exports provided: getDisplayedColumns, flattenColumns */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
__webpack_require__.r(__webpack_exports__);
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "getDisplayedColumns", function() { return getDisplayedColumns; });
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "flattenColumns", function() { return flattenColumns; });
function getDisplayedColumns(tableColumns) {
    const queue = tableColumns;
    const columns = [];
    while (queue.length) {
        const column = queue.shift();
        if (column.dataIndex) {
            columns.push(column.dataIndex);
        }
        if ((column.children || []).length) {
            column.children.map((child) => queue.push(child));
        }
    }
    return columns;
}
function flattenColumns(columns = []) {
    return columns.reduce((memo, column) => {
        const titles = flattenColumns(column.children);
        return [
            ...memo,
            ...(titles.length
                ? titles.map((title) => column.shortTitle + ', ' + title)
                : [column.shortTitle]),
        ];
    }, []);
}


/***/ }),

/***/ "RnhZ":
/*!**************************************************!*\
  !*** ./node_modules/moment/locale sync ^\.\/.*$ ***!
  \**************************************************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

var map = {
	"./af": "K/tc",
	"./af.js": "K/tc",
	"./ar": "jnO4",
	"./ar-dz": "o1bE",
	"./ar-dz.js": "o1bE",
	"./ar-kw": "Qj4J",
	"./ar-kw.js": "Qj4J",
	"./ar-ly": "HP3h",
	"./ar-ly.js": "HP3h",
	"./ar-ma": "CoRJ",
	"./ar-ma.js": "CoRJ",
	"./ar-sa": "gjCT",
	"./ar-sa.js": "gjCT",
	"./ar-tn": "bYM6",
	"./ar-tn.js": "bYM6",
	"./ar.js": "jnO4",
	"./az": "SFxW",
	"./az.js": "SFxW",
	"./be": "H8ED",
	"./be.js": "H8ED",
	"./bg": "hKrs",
	"./bg.js": "hKrs",
	"./bm": "p/rL",
	"./bm.js": "p/rL",
	"./bn": "kEOa",
	"./bn-bd": "loYQ",
	"./bn-bd.js": "loYQ",
	"./bn.js": "kEOa",
	"./bo": "0mo+",
	"./bo.js": "0mo+",
	"./br": "aIdf",
	"./br.js": "aIdf",
	"./bs": "JVSJ",
	"./bs.js": "JVSJ",
	"./ca": "1xZ4",
	"./ca.js": "1xZ4",
	"./cs": "PA2r",
	"./cs.js": "PA2r",
	"./cv": "A+xa",
	"./cv.js": "A+xa",
	"./cy": "l5ep",
	"./cy.js": "l5ep",
	"./da": "DxQv",
	"./da.js": "DxQv",
	"./de": "tGlX",
	"./de-at": "s+uk",
	"./de-at.js": "s+uk",
	"./de-ch": "u3GI",
	"./de-ch.js": "u3GI",
	"./de.js": "tGlX",
	"./dv": "WYrj",
	"./dv.js": "WYrj",
	"./el": "jUeY",
	"./el.js": "jUeY",
	"./en-au": "Dmvi",
	"./en-au.js": "Dmvi",
	"./en-ca": "OIYi",
	"./en-ca.js": "OIYi",
	"./en-gb": "Oaa7",
	"./en-gb.js": "Oaa7",
	"./en-ie": "4dOw",
	"./en-ie.js": "4dOw",
	"./en-il": "czMo",
	"./en-il.js": "czMo",
	"./en-in": "7C5Q",
	"./en-in.js": "7C5Q",
	"./en-nz": "b1Dy",
	"./en-nz.js": "b1Dy",
	"./en-sg": "t+mt",
	"./en-sg.js": "t+mt",
	"./eo": "Zduo",
	"./eo.js": "Zduo",
	"./es": "iYuL",
	"./es-do": "CjzT",
	"./es-do.js": "CjzT",
	"./es-mx": "tbfe",
	"./es-mx.js": "tbfe",
	"./es-us": "Vclq",
	"./es-us.js": "Vclq",
	"./es.js": "iYuL",
	"./et": "7BjC",
	"./et.js": "7BjC",
	"./eu": "D/JM",
	"./eu.js": "D/JM",
	"./fa": "jfSC",
	"./fa.js": "jfSC",
	"./fi": "gekB",
	"./fi.js": "gekB",
	"./fil": "1ppg",
	"./fil.js": "1ppg",
	"./fo": "ByF4",
	"./fo.js": "ByF4",
	"./fr": "nyYc",
	"./fr-ca": "2fjn",
	"./fr-ca.js": "2fjn",
	"./fr-ch": "Dkky",
	"./fr-ch.js": "Dkky",
	"./fr.js": "nyYc",
	"./fy": "cRix",
	"./fy.js": "cRix",
	"./ga": "USCx",
	"./ga.js": "USCx",
	"./gd": "9rRi",
	"./gd.js": "9rRi",
	"./gl": "iEDd",
	"./gl.js": "iEDd",
	"./gom-deva": "qvJo",
	"./gom-deva.js": "qvJo",
	"./gom-latn": "DKr+",
	"./gom-latn.js": "DKr+",
	"./gu": "4MV3",
	"./gu.js": "4MV3",
	"./he": "x6pH",
	"./he.js": "x6pH",
	"./hi": "3E1r",
	"./hi.js": "3E1r",
	"./hr": "S6ln",
	"./hr.js": "S6ln",
	"./hu": "WxRl",
	"./hu.js": "WxRl",
	"./hy-am": "1rYy",
	"./hy-am.js": "1rYy",
	"./id": "UDhR",
	"./id.js": "UDhR",
	"./is": "BVg3",
	"./is.js": "BVg3",
	"./it": "bpih",
	"./it-ch": "bxKX",
	"./it-ch.js": "bxKX",
	"./it.js": "bpih",
	"./ja": "B55N",
	"./ja.js": "B55N",
	"./jv": "tUCv",
	"./jv.js": "tUCv",
	"./ka": "IBtZ",
	"./ka.js": "IBtZ",
	"./kk": "bXm7",
	"./kk.js": "bXm7",
	"./km": "6B0Y",
	"./km.js": "6B0Y",
	"./kn": "PpIw",
	"./kn.js": "PpIw",
	"./ko": "Ivi+",
	"./ko.js": "Ivi+",
	"./ku": "JCF/",
	"./ku.js": "JCF/",
	"./ky": "lgnt",
	"./ky.js": "lgnt",
	"./lb": "RAwQ",
	"./lb.js": "RAwQ",
	"./lo": "sp3z",
	"./lo.js": "sp3z",
	"./lt": "JvlW",
	"./lt.js": "JvlW",
	"./lv": "uXwI",
	"./lv.js": "uXwI",
	"./me": "KTz0",
	"./me.js": "KTz0",
	"./mi": "aIsn",
	"./mi.js": "aIsn",
	"./mk": "aQkU",
	"./mk.js": "aQkU",
	"./ml": "AvvY",
	"./ml.js": "AvvY",
	"./mn": "lYtQ",
	"./mn.js": "lYtQ",
	"./mr": "Ob0Z",
	"./mr.js": "Ob0Z",
	"./ms": "6+QB",
	"./ms-my": "ZAMP",
	"./ms-my.js": "ZAMP",
	"./ms.js": "6+QB",
	"./mt": "G0Uy",
	"./mt.js": "G0Uy",
	"./my": "honF",
	"./my.js": "honF",
	"./nb": "bOMt",
	"./nb.js": "bOMt",
	"./ne": "OjkT",
	"./ne.js": "OjkT",
	"./nl": "+s0g",
	"./nl-be": "2ykv",
	"./nl-be.js": "2ykv",
	"./nl.js": "+s0g",
	"./nn": "uEye",
	"./nn.js": "uEye",
	"./oc-lnc": "Fnuy",
	"./oc-lnc.js": "Fnuy",
	"./pa-in": "8/+R",
	"./pa-in.js": "8/+R",
	"./pl": "jVdC",
	"./pl.js": "jVdC",
	"./pt": "8mBD",
	"./pt-br": "0tRk",
	"./pt-br.js": "0tRk",
	"./pt.js": "8mBD",
	"./ro": "lyxo",
	"./ro.js": "lyxo",
	"./ru": "lXzo",
	"./ru.js": "lXzo",
	"./sd": "Z4QM",
	"./sd.js": "Z4QM",
	"./se": "//9w",
	"./se.js": "//9w",
	"./si": "7aV9",
	"./si.js": "7aV9",
	"./sk": "e+ae",
	"./sk.js": "e+ae",
	"./sl": "gVVK",
	"./sl.js": "gVVK",
	"./sq": "yPMs",
	"./sq.js": "yPMs",
	"./sr": "zx6S",
	"./sr-cyrl": "E+lV",
	"./sr-cyrl.js": "E+lV",
	"./sr.js": "zx6S",
	"./ss": "Ur1D",
	"./ss.js": "Ur1D",
	"./sv": "X709",
	"./sv.js": "X709",
	"./sw": "dNwA",
	"./sw.js": "dNwA",
	"./ta": "PeUW",
	"./ta.js": "PeUW",
	"./te": "XLvN",
	"./te.js": "XLvN",
	"./tet": "V2x9",
	"./tet.js": "V2x9",
	"./tg": "Oxv6",
	"./tg.js": "Oxv6",
	"./th": "EOgW",
	"./th.js": "EOgW",
	"./tk": "Wv91",
	"./tk.js": "Wv91",
	"./tl-ph": "Dzi0",
	"./tl-ph.js": "Dzi0",
	"./tlh": "z3Vd",
	"./tlh.js": "z3Vd",
	"./tr": "DoHr",
	"./tr.js": "DoHr",
	"./tzl": "z1FC",
	"./tzl.js": "z1FC",
	"./tzm": "wQk9",
	"./tzm-latn": "tT3J",
	"./tzm-latn.js": "tT3J",
	"./tzm.js": "wQk9",
	"./ug-cn": "YRex",
	"./ug-cn.js": "YRex",
	"./uk": "raLr",
	"./uk.js": "raLr",
	"./ur": "UpQW",
	"./ur.js": "UpQW",
	"./uz": "Loxo",
	"./uz-latn": "AQ68",
	"./uz-latn.js": "AQ68",
	"./uz.js": "Loxo",
	"./vi": "KSF8",
	"./vi.js": "KSF8",
	"./x-pseudo": "/X5v",
	"./x-pseudo.js": "/X5v",
	"./yo": "fzPg",
	"./yo.js": "fzPg",
	"./zh-cn": "XDpg",
	"./zh-cn.js": "XDpg",
	"./zh-hk": "SatO",
	"./zh-hk.js": "SatO",
	"./zh-mo": "OmwH",
	"./zh-mo.js": "OmwH",
	"./zh-tw": "kOpN",
	"./zh-tw.js": "kOpN"
};


function webpackContext(req) {
	var id = webpackContextResolve(req);
	return __webpack_require__(id);
}
function webpackContextResolve(req) {
	if(!__webpack_require__.o(map, req)) {
		var e = new Error("Cannot find module '" + req + "'");
		e.code = 'MODULE_NOT_FOUND';
		throw e;
	}
	return map[req];
}
webpackContext.keys = function webpackContextKeys() {
	return Object.keys(map);
};
webpackContext.resolve = webpackContextResolve;
module.exports = webpackContext;
webpackContext.id = "RnhZ";

/***/ }),

/***/ "Sy1n":
/*!**********************************!*\
  !*** ./src/app/app.component.ts ***!
  \**********************************/
/*! exports provided: AppComponent */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
__webpack_require__.r(__webpack_exports__);
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "AppComponent", function() { return AppComponent; });
/* harmony import */ var _angular_core__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! @angular/core */ "fXoL");

class AppComponent {
    constructor() {
        this.title = 'angular-charts';
    }
}
AppComponent.ɵfac = function AppComponent_Factory(t) { return new (t || AppComponent)(); };
AppComponent.ɵcmp = _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵdefineComponent"]({ type: AppComponent, selectors: [["app-root"]], decls: 21, vars: 2, consts: [[1, "content", 2, "text-align", "center"], [2, "display", "block"], ["width", "300", "alt", "Angular Logo", "src", "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNTAgMjUwIj4KICAgIDxwYXRoIGZpbGw9IiNERDAwMzEiIGQ9Ik0xMjUgMzBMMzEuOSA2My4ybDE0LjIgMTIzLjFMMTI1IDIzMGw3OC45LTQzLjcgMTQuMi0xMjMuMXoiIC8+CiAgICA8cGF0aCBmaWxsPSIjQzMwMDJGIiBkPSJNMTI1IDMwdjIyLjItLjFWMjMwbDc4LjktNDMuNyAxNC4yLTEyMy4xTDEyNSAzMHoiIC8+CiAgICA8cGF0aCAgZmlsbD0iI0ZGRkZGRiIgZD0iTTEyNSA1Mi4xTDY2LjggMTgyLjZoMjEuN2wxMS43LTI5LjJoNDkuNGwxMS43IDI5LjJIMTgzTDEyNSA1Mi4xem0xNyA4My4zaC0zNGwxNy00MC45IDE3IDQwLjl6IiAvPgogIDwvc3ZnPg=="], ["target", "_blank", "rel", "noopener", "href", "https://angular.io/tutorial"], ["target", "_blank", "rel", "noopener", "href", "https://angular.io/cli"], ["target", "_blank", "rel", "noopener", "href", "https://blog.angular.io/"]], template: function AppComponent_Template(rf, ctx) { if (rf & 1) {
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](0, "div", 0);
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](1, "h1");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵtext"](2);
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](3, "span", 1);
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵtext"](4);
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelement"](5, "img", 2);
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](6, "h2");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵtext"](7, "Here are some links to help you start: ");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](8, "ul");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](9, "li");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](10, "h2");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](11, "a", 3);
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵtext"](12, "Tour of Heroes");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](13, "li");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](14, "h2");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](15, "a", 4);
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵtext"](16, "CLI Documentation");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](17, "li");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](18, "h2");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementStart"](19, "a", 5);
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵtext"](20, "Angular blog");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵelementEnd"]();
    } if (rf & 2) {
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵadvance"](2);
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵtextInterpolate1"](" Welcome to ", ctx.title, "! ");
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵadvance"](2);
        _angular_core__WEBPACK_IMPORTED_MODULE_0__["ɵɵtextInterpolate1"]("", ctx.title, " app is running!");
    } }, encapsulation: 2 });


/***/ }),

/***/ "ZAI4":
/*!*******************************!*\
  !*** ./src/app/app.module.ts ***!
  \*******************************/
/*! exports provided: AppModule */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
__webpack_require__.r(__webpack_exports__);
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "AppModule", function() { return AppModule; });
/* harmony import */ var _angular_platform_browser__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! @angular/platform-browser */ "jhN1");
/* harmony import */ var _angular_platform_browser_animations__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! @angular/platform-browser/animations */ "R1ws");
/* harmony import */ var _cubejs_client_ngx__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! @cubejs-client/ngx */ "lC/H");
/* harmony import */ var ng2_charts__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(/*! ng2-charts */ "LPYB");
/* harmony import */ var _angular_material_table__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(/*! @angular/material/table */ "+0xr");
/* harmony import */ var ngx_spinner__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(/*! ngx-spinner */ "JqCM");
/* harmony import */ var _main_main_component__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(/*! ./main/main.component */ "wlho");
/* harmony import */ var _app_component__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(/*! ./app.component */ "Sy1n");
/* harmony import */ var _angular_ng2_charts_query_renderer_component__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__(/*! ./angular-ng2-charts/query-renderer.component */ "DSVJ");
/* harmony import */ var _angular_test_charts_query_renderer_component__WEBPACK_IMPORTED_MODULE_9__ = __webpack_require__(/*! ./angular-test-charts/query-renderer.component */ "CflG");
/* harmony import */ var _angular_core__WEBPACK_IMPORTED_MODULE_10__ = __webpack_require__(/*! @angular/core */ "fXoL");












const API_URL = 'http://localhost:4000/cubejs-api/v1';
const CUBEJS_TOKEN = 'secret';
const { apiUrl, token } = window.parent.window['__cubejsPlayground'] || {};
const cubejsOptions = {
    token: token || CUBEJS_TOKEN,
    options: {
        apiUrl: apiUrl || API_URL,
    },
};
class AppModule {
}
AppModule.ɵmod = _angular_core__WEBPACK_IMPORTED_MODULE_10__["ɵɵdefineNgModule"]({ type: AppModule, bootstrap: [_main_main_component__WEBPACK_IMPORTED_MODULE_6__["MainComponent"]] });
AppModule.ɵinj = _angular_core__WEBPACK_IMPORTED_MODULE_10__["ɵɵdefineInjector"]({ factory: function AppModule_Factory(t) { return new (t || AppModule)(); }, providers: [], imports: [[
            _angular_platform_browser__WEBPACK_IMPORTED_MODULE_0__["BrowserModule"],
            _cubejs_client_ngx__WEBPACK_IMPORTED_MODULE_2__["CubejsClientModule"].forRoot(cubejsOptions),
            ng2_charts__WEBPACK_IMPORTED_MODULE_3__["ChartsModule"],
            _angular_material_table__WEBPACK_IMPORTED_MODULE_4__["MatTableModule"],
            _angular_platform_browser_animations__WEBPACK_IMPORTED_MODULE_1__["BrowserAnimationsModule"],
            ngx_spinner__WEBPACK_IMPORTED_MODULE_5__["NgxSpinnerModule"],
        ]] });
(function () { (typeof ngJitMode === "undefined" || ngJitMode) && _angular_core__WEBPACK_IMPORTED_MODULE_10__["ɵɵsetNgModuleScope"](AppModule, { declarations: [_main_main_component__WEBPACK_IMPORTED_MODULE_6__["MainComponent"],
        _app_component__WEBPACK_IMPORTED_MODULE_7__["AppComponent"],
        _angular_ng2_charts_query_renderer_component__WEBPACK_IMPORTED_MODULE_8__["AngularNg2Charts"],
        _angular_test_charts_query_renderer_component__WEBPACK_IMPORTED_MODULE_9__["AngularTestCharts"]], imports: [_angular_platform_browser__WEBPACK_IMPORTED_MODULE_0__["BrowserModule"], _cubejs_client_ngx__WEBPACK_IMPORTED_MODULE_2__["CubejsClientModule"], ng2_charts__WEBPACK_IMPORTED_MODULE_3__["ChartsModule"],
        _angular_material_table__WEBPACK_IMPORTED_MODULE_4__["MatTableModule"],
        _angular_platform_browser_animations__WEBPACK_IMPORTED_MODULE_1__["BrowserAnimationsModule"],
        ngx_spinner__WEBPACK_IMPORTED_MODULE_5__["NgxSpinnerModule"]] }); })();


/***/ }),

/***/ "wlho":
/*!****************************************!*\
  !*** ./src/app/main/main.component.ts ***!
  \****************************************/
/*! exports provided: MainComponent */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
__webpack_require__.r(__webpack_exports__);
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "MainComponent", function() { return MainComponent; });
/* harmony import */ var rxjs__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! rxjs */ "qCKp");
/* harmony import */ var _code_chunks__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ../../code-chunks */ "6WBr");
/* harmony import */ var _angular_core__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! @angular/core */ "fXoL");
/* harmony import */ var _angular_common__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(/*! @angular/common */ "ofXK");
/* harmony import */ var _angular_ng2_charts_query_renderer_component__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(/*! ../angular-ng2-charts/query-renderer.component */ "DSVJ");
/* harmony import */ var _angular_test_charts_query_renderer_component__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(/*! ../angular-test-charts/query-renderer.component */ "CflG");






function MainComponent_angular_ng2_charts_0_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵelement"](0, "angular-ng2-charts", 2);
} if (rf & 2) {
    const ctx_r0 = _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵnextContext"]();
    _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵproperty"]("cubeQuery", ctx_r0.query$)("pivotConfig", ctx_r0.pivotConfig$)("chartType", ctx_r0.chartType$);
} }
function MainComponent_angular_test_charts_1_Template(rf, ctx) { if (rf & 1) {
    _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵelement"](0, "angular-test-charts", 3);
} if (rf & 2) {
    const ctx_r1 = _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵnextContext"]();
    _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵproperty"]("chartType", ctx_r1.chartType$.asObservable());
} }
window['__cubejsPlayground'] = {
    getDependencies: _code_chunks__WEBPACK_IMPORTED_MODULE_1__["getDependencies"],
    getCodesandboxFiles: _code_chunks__WEBPACK_IMPORTED_MODULE_1__["getCodesandboxFiles"],
};
class MainComponent {
    constructor() {
        this.chartingLibrary = null;
        this.query$ = new rxjs__WEBPACK_IMPORTED_MODULE_0__["BehaviorSubject"]({});
        this.pivotConfig$ = new rxjs__WEBPACK_IMPORTED_MODULE_0__["BehaviorSubject"](null);
        this.chartType$ = new rxjs__WEBPACK_IMPORTED_MODULE_0__["BehaviorSubject"]('line');
    }
    ngOnInit() {
        const { onChartRendererReady } = window.parent.window['__cubejsPlayground'] || {};
        if (typeof onChartRendererReady === 'function') {
            onChartRendererReady();
        }
    }
    onCubejsEvent(event) {
        const { query, pivotConfig, chartType, chartingLibrary, eventType, } = event.detail;
        if (eventType === 'chart') {
            if (query !== undefined) {
                this.query$.next(query);
            }
            if (pivotConfig !== undefined) {
                this.pivotConfig$.next(pivotConfig);
            }
            if (chartType) {
                this.chartType$.next(chartType);
            }
            if (chartingLibrary) {
                this.chartingLibrary = chartingLibrary;
            }
        }
    }
}
MainComponent.ɵfac = function MainComponent_Factory(t) { return new (t || MainComponent)(); };
MainComponent.ɵcmp = _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵdefineComponent"]({ type: MainComponent, selectors: [["app-root"]], hostBindings: function MainComponent_HostBindings(rf, ctx) { if (rf & 1) {
        _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵlistener"]("__cubejsPlaygroundEvent", function MainComponent___cubejsPlaygroundEvent_HostBindingHandler($event) { return ctx.onCubejsEvent($event); }, false, _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵresolveWindow"]);
    } }, decls: 2, vars: 2, consts: [[3, "cubeQuery", "pivotConfig", "chartType", 4, "ngIf"], [3, "chartType", 4, "ngIf"], [3, "cubeQuery", "pivotConfig", "chartType"], [3, "chartType"]], template: function MainComponent_Template(rf, ctx) { if (rf & 1) {
        _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵtemplate"](0, MainComponent_angular_ng2_charts_0_Template, 1, 3, "angular-ng2-charts", 0);
        _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵtemplate"](1, MainComponent_angular_test_charts_1_Template, 1, 1, "angular-test-charts", 1);
    } if (rf & 2) {
        _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵproperty"]("ngIf", ctx.chartingLibrary === "angular-ng2-charts");
        _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵadvance"](1);
        _angular_core__WEBPACK_IMPORTED_MODULE_2__["ɵɵproperty"]("ngIf", ctx.chartingLibrary === "angular-test-charts");
    } }, directives: [_angular_common__WEBPACK_IMPORTED_MODULE_3__["NgIf"], _angular_ng2_charts_query_renderer_component__WEBPACK_IMPORTED_MODULE_4__["AngularNg2Charts"], _angular_test_charts_query_renderer_component__WEBPACK_IMPORTED_MODULE_5__["AngularTestCharts"]], encapsulation: 2 });


/***/ }),

/***/ "zUnb":
/*!*********************!*\
  !*** ./src/main.ts ***!
  \*********************/
/*! no exports provided */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
__webpack_require__.r(__webpack_exports__);
/* harmony import */ var _angular_platform_browser__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! @angular/platform-browser */ "jhN1");
/* harmony import */ var _angular_core__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! @angular/core */ "fXoL");
/* harmony import */ var _app_app_module__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! ./app/app.module */ "ZAI4");
/* harmony import */ var _environments_environment__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(/*! ./environments/environment */ "AytR");




if (_environments_environment__WEBPACK_IMPORTED_MODULE_3__["environment"].production) {
    Object(_angular_core__WEBPACK_IMPORTED_MODULE_1__["enableProdMode"])();
}
_angular_platform_browser__WEBPACK_IMPORTED_MODULE_0__["platformBrowser"]().bootstrapModule(_app_app_module__WEBPACK_IMPORTED_MODULE_2__["AppModule"])
    .catch(err => console.error(err));


/***/ }),

/***/ "zn8P":
/*!******************************************************!*\
  !*** ./$$_lazy_route_resource lazy namespace object ***!
  \******************************************************/
/*! no static exports found */
/***/ (function(module, exports) {

function webpackEmptyAsyncContext(req) {
	// Here Promise.resolve().then() is used instead of new Promise() to prevent
	// uncaught exception popping up in devtools
	return Promise.resolve().then(function() {
		var e = new Error("Cannot find module '" + req + "'");
		e.code = 'MODULE_NOT_FOUND';
		throw e;
	});
}
webpackEmptyAsyncContext.keys = function() { return []; };
webpackEmptyAsyncContext.resolve = webpackEmptyAsyncContext;
module.exports = webpackEmptyAsyncContext;
webpackEmptyAsyncContext.id = "zn8P";

/***/ })

},[[0,"runtime","vendor"]]]);
//# sourceMappingURL=main.js.map