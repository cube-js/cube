import { Component, OnInit } from "@angular/core";
import { BehaviorSubject } from "rxjs";
import { Meta, Title } from '@angular/platform-browser';
import { BreakpointObserver, Breakpoints } from '@angular/cdk/layout';

@Component({
  selector: "app-dashboard-page",
  templateUrl: "./dashboard-page.component.html",
  styleUrls: ["./dashboard-page.component.scss"]
})
export class DashboardPageComponent implements OnInit {
  cols : number;
  chartCols: number;

  gridByBreakpoint = {
    xl: 4,
    lg: 4,
    md: 2,
    sm: 2,
    xs: 1
  }
  chartGridByBreakpoint = {
    xl: 5,
    lg: 5,
    md: 3,
    sm: 3,
    xs: 3
  }
  doughnutCols = 2;
  constructor(private meta: Meta, private title: Title, private breakpointObserver: BreakpointObserver) {
    this.title.setTitle('Angular Dashboard with Material');
    this.meta.addTag({ name: 'description', content: 'How to build Angular Material Dashboard with Cube.js' });
    this.meta.addTag({ name: 'keywords', content: 'Angular, Cube.js, Dashboard, Material UI' });

    this.breakpointObserver.observe([
      Breakpoints.XSmall,
      Breakpoints.Small,
      Breakpoints.Medium,
      Breakpoints.Large,
      Breakpoints.XLarge,
    ]).subscribe(result => {
      if (result.matches) {
        if (result.breakpoints[Breakpoints.XSmall]) {
          this.cols = this.gridByBreakpoint.xs;
          this.chartCols = this.chartGridByBreakpoint.xs;
          this.doughnutCols = 3;
        }
        if (result.breakpoints[Breakpoints.Small]) {
          this.cols = this.gridByBreakpoint.sm;
          this.chartCols = this.chartGridByBreakpoint.sm;
          this.doughnutCols = 3;
        }
        if (result.breakpoints[Breakpoints.Medium]) {
          this.cols = this.gridByBreakpoint.md;
          this.chartCols = this.chartGridByBreakpoint.md;
          this.doughnutCols = 3;
        }
        if (result.breakpoints[Breakpoints.Large]) {
          this.cols = this.gridByBreakpoint.lg;
          this.chartCols = this.chartGridByBreakpoint.lg;
        }
        if (result.breakpoints[Breakpoints.XLarge]) {
          this.cols = this.gridByBreakpoint.xl;
          this.chartCols = this.chartGridByBreakpoint.xl;
        }
      }
    });
  }
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
        chart: "doughnut", cols: this.doughnutCols, rows: 1,
        query: data
      };
    });
  }
}
