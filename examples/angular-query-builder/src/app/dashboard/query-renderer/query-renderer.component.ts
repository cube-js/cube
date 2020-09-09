import { Component, OnInit, Input } from '@angular/core';
import { ChartDataSets, ChartOptions } from 'chart.js';
import { Color, Label } from 'ng2-charts';

@Component({
  selector: 'query-renderer',
  templateUrl: './query-renderer.component.html',
  styleUrls: ['./query-renderer.component.css'],
})
export class QueryRendererComponent implements OnInit {
  private _resultSet;

  @Input()
  set resultSet(resultSet: any) {
    this._resultSet = resultSet;
    this.updateChart();
  }
  get resultSet() {
    return this._resultSet;
  }

  @Input()
  chartType: any = 'line';

  public lineChartData: ChartDataSets[] = [{ data: [65, 59, 80, 81, 56, 55, 40], label: 'Series A' }];
  public lineChartLabels: Label[] = ['January', 'February', 'March', 'April', 'May', 'June', 'July'];
  public lineChartOptions: ChartOptions & { responsive: boolean } = {
    responsive: true,
  };
  public lineChartColors: Color[] = [
    {
      borderColor: 'black',
      borderWidth: 1,
      backgroundColor: 'rgba(255,0,0,0.3)',
    },
  ];
  public lineChartLegend = true;
  public lineChartType = 'line';
  public lineChartPlugins = [];

  constructor() {}

  ngOnInit() {}

  updateChart() {
    if (this.resultSet) {
      this.lineChartData = this.resultSet.series().map(item => {
        return {
          label: item.title,
          data: item.series.map(({ value }) => value)
        }
      });
      this.lineChartLabels = this.resultSet.chartPivot().map((row) => row.x);
    }
  }
}
