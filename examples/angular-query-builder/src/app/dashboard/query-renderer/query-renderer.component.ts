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

function getMatColumnsMeta(tableColumns: any[]): [any, any] {
  const queue = tableColumns;
  const columns = [];
  const dataIndexToTitle = {};

  while (queue.length) {
    const column = queue.shift();
    if (column.dataIndex) {
      columns.push(column.dataIndex);
      dataIndexToTitle[column.dataIndex] = column.title;
    }
    if ((column.children || []).length) {
      column.children.map((child) => queue.push(child));
    }
  }

  return [columns, dataIndexToTitle];
}

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
  dataIndexToTitle: any;
  dataSource = [];
  tableData = [];
  tableColumns = [];
  chartData: ChartDataSets[] = [];
  chartLabels: Label[] = [];
  chartOptions: ChartOptions = {
    responsive: true,
  };
  chartColors: Color[] = [
    {
      borderColor: 'none',
      borderWidth: 1,
      backgroundColor: 'rgba(255,0,0,0.3)',
    },
  ];

  @Input()
  resetResultSetOnChange: boolean = false;

  @Input()
  queryBuilder: QueryBuilderService;

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
        this.chartType = chartType;
        if (resultSet != null || this.resetResultSetOnChange) {
          this.resultSet = resultSet;
        }
        this.isQueryPresent = resultSet != null;
        setTimeout(() => this.updateChart(resultSet, pivotConfig), 0);
      }
    );
  }

  updateChart(resultSet: ResultSet | null, pivotConfig: TPivotConfig) {
    if (!resultSet) {
      return;
    }

    if (this.queryBuilder.chartType.get() === 'table') {
      this.tableData = resultSet.tablePivot(pivotConfig);
      const [displayedColumns, dataIndexToTitle] = getMatColumnsMeta(
        resultSet.tableColumns(pivotConfig)
      );

      this.displayedColumns = displayedColumns;
      this.dataIndexToTitle = dataIndexToTitle;
    } else {
      this.chartData = resultSet.series(pivotConfig).map((item) => {
        return {
          label: item.title,
          data: item.series.map(({ value }) => value),
          queue: 'a',
        };
      });
      this.chartLabels = resultSet.chartPivot(pivotConfig).map((row) => row.x);
    }
  }
}
