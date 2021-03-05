import { Component, Input, OnInit } from "@angular/core";
import { CubejsClient } from "@cubejs-client/ngx";

@Component({
  selector: "app-doughnut-chart",
  templateUrl: "./doughnut-chart.component.html",
  styleUrls: ["./doughnut-chart.component.scss"]
})
export class DoughnutChartComponent implements OnInit {
  @Input() query: Object;

  public barChartOptions = {
    legend: {
      display: false
    },
    responsive: true,
    maintainAspectRatio: true,
    cutoutPercentage: 80,
    layout: { padding: 0 },
    tooltips: {
      enabled: true,
      mode: "index",
      intersect: false,
      borderWidth: 1,
      borderColor: "#eeeeee",
      backgroundColor: "#ffffff",
      titleFontColor: "#43436B",
      bodyFontColor: "#A1A1B5",
      footerFontColor: "#A1A1B5"
    }
  };

  public barChartLabels = [];
  public barChartType = "doughnut";
  public barChartLegend = true;
  public barChartData = [];
  public value = 0;
  public labels = [];

  constructor(private cubejs: CubejsClient) {
  }

  ngOnInit() {
    this.cubejs.load(this.query).subscribe(
      resultSet => {
        const COLORS_SERIES = ["#FF6492", "#F3F3FB", "#FFA2BE"];
        this.barChartLabels = resultSet.chartPivot().map((c) => c.category);
        this.barChartData = resultSet.series().map((s) => ({
          label: s.title,
          data: s.series.map((r) => r.value),
          backgroundColor: COLORS_SERIES,
          hoverBackgroundColor: COLORS_SERIES
        }));
        resultSet.series().map(s => {
          this.labels = s.series;
          this.value = s.series.reduce((sum, current) => {
            return sum.value ? sum.value + current.value : sum + current.value
          });
        });
      },
      err => console.log("HTTP Error", err)
    );
  }

}
