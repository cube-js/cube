import { Component } from "@angular/core";
import { map } from "rxjs/operators";
import { Breakpoints, BreakpointObserver } from "@angular/cdk/layout";

@Component({
  selector: "app-dashboard-page",
  templateUrl: "./dashboard-page.component.html",
  styleUrls: ["./dashboard-page.component.scss"]
})
export class DashboardPageComponent {
  /** Based on the screen size, switch from standard to one column per row */
  cards = this.breakpointObserver.observe(Breakpoints.Handset).pipe(
    map(({ matches }) => {
      if (matches) {
        return [
          {
            chart: "bar", cols: 1, rows: 1,
            query: {
              measures: ["Orders.count"],
              timeDimensions: [{ dimension: "Orders.createdAt", granularity: "month", dateRange: "This year" }],
              dimensions: ["Orders.status"],
              filters: [{ dimension: "Orders.status", operator: "notEquals", values: ["completed"] }]
            }
          },
          { cols: 1, rows: 1 },
          { cols: 1, rows: 1 },
          { cols: 1, rows: 1 }
        ];
      }

      return [
        {
          chart: "bar", cols: 2, rows: 1,
          query: {
            measures: ["Orders.count"],
            timeDimensions: [{ dimension: "Orders.createdAt", granularity: "month", dateRange: "This year" }],
            dimensions: ["Orders.status"],
            filters: [{ dimension: "Orders.status", operator: "notEquals", values: ["completed"] }]
          }
        },
        { cols: 1, rows: 1 },
        { cols: 1, rows: 2 },
        { cols: 1, rows: 1 }
      ];
    })
  );

  constructor(private breakpointObserver: BreakpointObserver) {
  }
}
