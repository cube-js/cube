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
  private doughnutQuery = new BehaviorSubject({
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
      },
    ],
    filters: [],
    dimensions: ['Orders.status'],
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
      difference: false,
      progress: true,
      duration: 2.75,
    },
    {
      title: 'TOTAL PROFIT',
      query: { measures: ['LineItems.price'] },
      difference: false,
      progress: false,
      duration: 3.25,
    },
  ];

  ngOnInit() {
    this.query.subscribe(data => {
      this.cards[0] = {
        hasDatePick: true,
        title: 'Last Sales',
        chart: "bar", cols: 3, rows: 1,
        query: data
      };
    });
    this.doughnutQuery.subscribe(data => {
      this.cards[1] = {
        hasDatePick: false,
        title: 'Users by Device',
        chart: "doughnut", cols: 2, rows: 1,
        query: data
      };
    });
  }
}
