import { Component, OnInit } from "@angular/core";
import { BehaviorSubject } from "rxjs";

@Component({
  selector: "app-dashboard-page",
  templateUrl: "./dashboard-page.component.html",
  styleUrls: ["./dashboard-page.component.scss"]
})
export class DashboardPageComponent implements OnInit {
  private query = new BehaviorSubject({
    measures: ["Orders.count"],
    timeDimensions: [{ dimension: "Orders.createdAt", granularity: "month", dateRange: "This year" }],
    dimensions: ["Orders.status"],
    filters: [{ dimension: "Orders.status", operator: "notEquals", values: ["completed"] }]
  });
  changeDateRange = (value) => {
    this.query.next({
      ...this.query.value,
      timeDimensions: [{ dimension: "Orders.createdAt", granularity: "month", dateRange: value }]
    });
  };
  cards = [];

  ngOnInit() {
    this.query.subscribe(data => {
      this.cards[0] = {
        chart: "bar", cols: 2, rows: 1,
        query: data
      };
    });
  }
}
