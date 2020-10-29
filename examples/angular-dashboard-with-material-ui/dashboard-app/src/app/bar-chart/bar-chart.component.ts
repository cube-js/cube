import { Component, OnInit, Input } from "@angular/core";
import { CubejsClient } from '@cubejs-client/ngx';
import {formatDate, registerLocaleData} from "@angular/common"
import localeEn from '@angular/common/locales/en';

registerLocaleData(localeEn);

@Component({
  selector: "app-bar-chart",
  templateUrl: "./bar-chart.component.html",
  styleUrls: ["./bar-chart.component.scss"]
})

export class BarChartComponent implements OnInit {
  @Input() query: Object;
  constructor(private cubejs:CubejsClient){}

  public barChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    legend: { display: false },
    cornerRadius: 50,
    tooltips: {
      enabled: true,
      mode: 'index',
      intersect: false,
      borderWidth: 1,
      borderColor: "#eeeeee",
      backgroundColor: "#ffffff",
      titleFontColor: "#43436B",
      bodyFontColor: "#A1A1B5",
      footerFontColor: "#A1A1B5",
    },
    layout: { padding: 0 },
    scales: {
      xAxes: [
        {
          barThickness: 12,
          maxBarThickness: 10,
          barPercentage: 0.5,
          categoryPercentage: 0.5,
          ticks: {
            fontColor: "#A1A1B5",
          },
          gridLines: {
            display: false,
            drawBorder: false,
          },
        },
      ],
      yAxes: [
        {
          ticks: {
            fontColor: "#A1A1B5",
            beginAtZero: true,
            min: 0,
          },
          gridLines: {
            borderDash: [2],
            borderDashOffset: [2],
            color: "#eeeeee",
            drawBorder: false,
            zeroLineBorderDash: [2],
            zeroLineBorderDashOffset: [2],
            zeroLineColor: "#eeeeee",
          },
        },
      ],
    },
  };

  public barChartLabels = [];
  public barChartType = "bar";
  public barChartLegend = true;
  public barChartData = [];

  ngOnInit() {
    this.cubejs.load(this.query).subscribe(
      resultSet => {
        const COLORS_SERIES = ['#FF6492', '#F3F3FB', '#FFA2BE'];
        this.barChartLabels = resultSet.chartPivot().map((c) => formatDate(c.category, 'longDate', 'en'));
        this.barChartData = resultSet.series().map((s, index) => ({
          label: s.title,
          data: s.series.map((r) => r.value),
          backgroundColor: COLORS_SERIES[index],
          fill: false,
        }));
      },
      err => console.log('HTTP Error', err)
    );
  }
}
