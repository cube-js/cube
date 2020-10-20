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
  public cards = [];
  public KPICards = [
    {
      title: 'ORDERS',
      query: { measures: ['Orders.count'] },
      difference: 'Orders',
      progress: false,
      duration: 2.25,
    },
    {
      title: 'TOTAL USERS',
      query: { measures: ['Users.count'] },
      difference: 'Users',
      progress: false,
      duration: 2.5,
    },
    {
      title: 'COMPLETED ORDERS',
      query: { measures: ['Orders.percentOfCompletedOrders'] },
      progress: true,
      duration: 2.75,
    },
    {
      title: 'TOTAL PROFIT',
      query: { measures: ['LineItems.price'] },
      progress: false,
      duration: 3.25,
    },
  ];

  ngOnInit() {
    this.query.subscribe(data => {
      this.cards[0] = {
        chart: "bar", cols: 3, rows: 1,
        query: data
      };
      this.cards[1] = {
        chart: "bar", cols: 2, rows: 1,
        query: data
      };
    });
  }
}
