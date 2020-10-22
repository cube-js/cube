import { Component, Input, OnInit } from "@angular/core";
import { CubejsClient } from "@cubejs-client/ngx";

@Component({
  selector: 'app-kpi-card',
  templateUrl: './kpi-card.component.html',
  styleUrls: ['./kpi-card.component.scss']
})
export class KpiCardComponent implements OnInit {
  @Input() query: object;
  @Input() title: string;
  @Input() duration: number;
  @Input() progress: boolean;
  @Input() difference: string;
  constructor(private cubejs:CubejsClient){}
  public result = 0;
  public postfix = null;
  public prefix = null;
  public diffValue = null;

  ngOnInit(): void {
    this.cubejs.load(this.query).subscribe(
      resultSet => {
        resultSet.series().map((s) => {
          this.result = s['series'][0]['value'].toFixed(1);
          const measureKey = resultSet.seriesNames()[0].key;
          const annotations = resultSet.tableColumns().find((tableColumn) => tableColumn.key === measureKey);
          const format = annotations.format || (annotations.meta && annotations.meta.format);
          if (format === 'percent') {
            this.postfix = '%';
          } else if (format === 'currency') {
            this.prefix = '$';
          }
        })
      },
      err => console.log('HTTP Error', err)
    );
    if (this.difference) {
      this.cubejs.load({...this.query, timeDimensions: [
          {
            dimension: `${this.difference}.createdAt`,
            granularity: null,
            dateRange: 'This year',
          },
        ],}).subscribe(
        resultSet => {
          this.diffValue = ((parseInt(resultSet.rawData()[0][`${this.difference}.count`]) / this.result) * 100).toFixed(1);
          // this.diffValue = (resultSet.totalRow()[resultSet.measures[0]] / this.result) * 100;
        },
        err => console.log('HTTP Error', err)
      );
    }
  }

}
