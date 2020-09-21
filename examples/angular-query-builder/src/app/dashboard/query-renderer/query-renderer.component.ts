import { Component, Input, OnInit } from '@angular/core';
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
import { Color, Label } from 'ng2-charts';
import { combineLatest, of } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

const ELEMENT_DATA: any[] = [];

function getDisplayedColumns(tableColumns: any[]) {
  const queue = tableColumns;
  const columns = [];

  while (queue.length) {
    const column = queue.pop();
    if (column.dataIndex) {
      columns.push(column.dataIndex);
    }
    if ((column.children || []).length) {
      column.children.map((child) => queue.push(child));
    }
  }

  return columns;
}

@Component({
  selector: 'query-renderer',
  templateUrl: './query-renderer.component.html',
  styleUrls: ['./query-renderer.component.css'],
})
export class QueryRendererComponent implements OnInit {
  private _resultSet: ResultSet;

  chartType: TChartType = 'line';
  isQueryPresent: boolean;
  displayedColumns: string[] = [];
  dataSource = ELEMENT_DATA;
  tableData = [];
  tableColumns = [];
  resultSet: ResultSet;

  @Input()
  resetResultSetOnChange: boolean = false;

  @Input()
  queryBuilder: QueryBuilderService;

  @Input()
  pivotConfig: TPivotConfig;

  chartData: ChartDataSets[] = [];
  chartLabels: Label[] = [];
  chartOptions: ChartOptions & { responsive: boolean } = {
    responsive: true,
  };
  chartColors: Color[] = [
    {
      borderColor: 'none',
      borderWidth: 1,
      backgroundColor: 'rgba(255,0,0,0.3)',
    },
  ];

  constructor(private cubejsClient: CubejsClient) {}

  async ngOnInit() {
    const query = await this.queryBuilder.query;

    combineLatest([
      query.subject.pipe(
        mergeMap((cubeQuery) => {
          if (!isQueryPresent(cubeQuery)) {
            return of(null);
          }
          return this.cubejsClient.load(cubeQuery);
        })
      ),
      this.queryBuilder.pivotConfig.subject,
      this.queryBuilder.chartType.subject,
    ]).subscribe(
      ([resultSet, pivotConfig, chartType]: [
        ResultSet,
        TPivotConfig,
        TChartType
      ]) => {
        if (resultSet != null || this.resetResultSetOnChange) {
          this._resultSet = resultSet;
        }
        this.chartType = chartType;
        this.isQueryPresent = resultSet != null;
        this.updateChart(this._resultSet, pivotConfig);
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
      this.tableColumns = this.displayedColumns.map((column) => {});
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

    console.log({
      tablePivot: resultSet.tablePivot(pivotConfig),
      tableColumns: resultSet.tableColumns(pivotConfig),
      _tableData: this.tableData,
      _dislayed: this.displayedColumns,
      __: getDisplayedColumns(resultSet.tableColumns(pivotConfig)),
      // series: resultSet.series(pivotConfig),
      _chartData: this.chartData,
      _chartLables: this.chartLabels,
    });
  }
}
