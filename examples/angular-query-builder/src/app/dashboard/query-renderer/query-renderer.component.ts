import { Component, Input, OnInit } from '@angular/core';
import { MatSnackBar } from '@angular/material/snack-bar';
import {
  isQueryPresent,
  PivotConfig as TPivotConfig,
  ResultSet,
} from '@cubejs-client/core';
import {
  CubejsClient,
  QueryBuilderService,
  TChartType,
} from '@cubejs-client/ngx';
import { ChartDataSets, ChartOptions } from 'chart.js';
import { Label } from 'ng2-charts';
import { combineLatest, of } from 'rxjs';
import { catchError, debounceTime, switchMap } from 'rxjs/operators';

import { flattenColumns, getDisplayedColumns } from './utils';

@Component({
  selector: 'query-renderer',
  templateUrl: './query-renderer.component.html',
  styleUrls: ['./query-renderer.component.css'],
})
export class QueryRendererComponent implements OnInit {
  resultSet: ResultSet;
  chartType: TChartType = 'line';
  isQueryPresent: boolean;
  displayedColumns: string[] = [];
  tableData: any[] = [];
  columnTitles: string[] = [];
  chartData: ChartDataSets[] = [];
  chartLabels: Label[] = [];
  chartOptions: ChartOptions = {
    responsive: true,
  };
  noFillChartOptions: ChartOptions = {
    responsive: true,
    elements: {
      line: {
        fill: false,
      },
    },
  };

  @Input()
  resetResultSetOnChange: boolean = false;

  @Input()
  queryBuilder: QueryBuilderService;

  constructor(
    private cubejsClient: CubejsClient,
    private snakBar: MatSnackBar
  ) {}

  async ngOnInit() {
    const query = await this.queryBuilder.query;

    combineLatest([
      query.subject.pipe(
        switchMap((cubeQuery) => {
          if (!isQueryPresent(cubeQuery)) {
            return of(null);
          }
          return this.cubejsClient.load(cubeQuery).pipe(
            catchError((error) => {
              this.snakBar.open(error.message || 'Request error', null, {
                duration: 2000,
              });
              return of(null);
            })
          );
        })
      ),
      this.queryBuilder.pivotConfig.subject,
      this.queryBuilder.chartType.subject,
    ])
      .pipe(debounceTime(300))
      .subscribe(
        ([resultSet, pivotConfig, chartType]: [
          ResultSet,
          TPivotConfig,
          TChartType
        ]) => {
          this.chartType = chartType;
          if (resultSet != null || this.resetResultSetOnChange) {
            this.resultSet = resultSet;
          }
          this.isQueryPresent = resultSet != null;
          this.updateChart(resultSet, pivotConfig);
        }
      );
  }

  updateChart(resultSet: ResultSet | null, pivotConfig: TPivotConfig) {
    if (!resultSet) {
      return;
    }

    if (this.queryBuilder.chartType.get() === 'table') {
      this.tableData = resultSet.tablePivot(pivotConfig);
      this.displayedColumns = getDisplayedColumns(
        resultSet.tableColumns(pivotConfig)
      );
      this.columnTitles = flattenColumns(resultSet.tableColumns(pivotConfig));
    } else {
      this.chartData = resultSet.series(pivotConfig).map((item) => {
        return {
          label: item.title,
          data: item.series.map(({ value }) => value),
          stack: 'a',
        };
      });
      this.chartLabels = resultSet.chartPivot(pivotConfig).map((row) => row.x);
    }
  }
}
