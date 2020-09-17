import { Component, Input, OnInit } from '@angular/core';
import { PivotConfig as TPivotConfig, ResultSet } from '@cubejs-client/core';
import {
  CubejsClient,
  QueryBuilderService,
  TChartType,
} from '@cubejs-client/ngx';
import { ChartDataSets, ChartOptions } from 'chart.js';
import { Color, Label } from 'ng2-charts';
import { combineLatest } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

const ELEMENT_DATA: any[] = [
];

@Component({
  selector: 'query-renderer',
  templateUrl: './query-renderer.component.html',
  styleUrls: ['./query-renderer.component.css'],
})
export class QueryRendererComponent implements OnInit {
  private _pivotConfig: TPivotConfig;
  private _resultSet: ResultSet;
  private _chartType: TChartType = 'line';

  isQueryPresent: boolean;
  displayedColumns: string[] = ['Sales.ts.day'];
  dataSource = ELEMENT_DATA;
  tableData = [];
  resultSet: ResultSet;

  @Input()
  queryBuilder: QueryBuilderService;

  @Input()
  set chartType(value) {
    this._chartType = value;
    this.updateChart(this._resultSet, this._pivotConfig);
  }

  get chartType() {
    return this._chartType;
  }

  @Input()
  pivotConfig: TPivotConfig;

  chartData: ChartDataSets[] = [
    // { data: [65, 59, 80, 81, 56, 55, 40], label: 'Series A' },
  ];
  chartLabels: Label[] = [];
  chartOptions: ChartOptions & { responsive: boolean } = {
    responsive: true,
  };
  chartColors: Color[] = [
    {
      borderColor: 'black',
      borderWidth: 1,
      backgroundColor: 'rgba(255,0,0,0.3)',
    },
  ];

  constructor(private cubejsClient: CubejsClient) {}

  async ngOnInit() {
    const query = await this.queryBuilder.query;

    combineLatest([
      query.subject.pipe(
        mergeMap((cubeQuery) => this.cubejsClient.load(cubeQuery))
      ),
      this.queryBuilder.pivotConfig.subject,
    ]).subscribe(([resultSet, pivotConfig]: [ResultSet, TPivotConfig]) => {
      this._resultSet = resultSet;
      this._pivotConfig = pivotConfig;
      this.updateChart(resultSet, pivotConfig);
    });
  }

  updateChart(resultSet: ResultSet | null, pivotConfig: TPivotConfig) {
    if (!resultSet) {
      return;
    }

    console.log({
      tablePivot: resultSet.tablePivot(pivotConfig),
      tableColumns: resultSet.tableColumns(pivotConfig),
    });

    if (this.queryBuilder.chartType === 'table') {
      this.tableData = resultSet.tablePivot(pivotConfig);
    } else {
      this.chartData = resultSet.series().map((item) => {
        return {
          label: item.title,
          data: item.series.map(({ value }) => value),
        };
      });
      this.chartLabels = resultSet.chartPivot().map((row) => row.x);
    }
  }
}
