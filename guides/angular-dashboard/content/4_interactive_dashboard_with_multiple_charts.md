---
order: 4
title: "Interactive Dashboard with Multiple Charts"
---

In the previous part, we've created an analytical backend and a basic dashboard with the first chart. Now we're going to expand the dashboard so it provides the view of key performance indicators of our e-commerce company.

## Custom Date Range

As the first step, we'll let users change the date range of the existing chart.

For that, we'll need to make a change to the `dashboard-page.component.ts` file: 

```diff
// ...

export class DashboardPageComponent implements OnInit {
  private query = new BehaviorSubject({
    measures: ["Orders.count"],
    timeDimensions: [{ dimension: "Orders.createdAt", granularity: "month", dateRange: "This year" }],
    dimensions: ["Orders.status"],
    filters: [{ dimension: "Orders.status", operator: "notEquals", values: ["completed"] }]
  });
+  changeDateRange = (value) => {
+    this.query.next({
+      ...this.query.value,
+      timeDimensions: [{ dimension: "Orders.createdAt", granularity: "month", dateRange: value }]
+    });
+  };

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
```

And another one to the `dashobard-page.component.html` file:

```diff
<div class="grid-container">
  <h1 class="mat-h1">Dashboard</h1>
  <mat-grid-list cols="3" rowHeight="450px">
    <mat-grid-tile *ngFor="let card of cards" [colspan]="card.cols" [rowspan]="card.rows">
      <mat-card class="dashboard-card">
        <mat-card-header>
          <mat-card-title>
            <button mat-icon-button class="more-button" [matMenuTriggerFor]="menu" aria-label="Toggle menu">
              <mat-icon>more_vert</mat-icon>
            </button>
            <mat-menu #menu="matMenu" xPosition="before">
+            <button mat-menu-item  (click)="changeDateRange('This year')">This year</button>
+            <button mat-menu-item  (click)="changeDateRange('Last year')">Last year</button>
            </mat-menu>
          </mat-card-title>
        </mat-card-header>
        <mat-card-content class="dashboard-card-content">
          <div>
            <app-bar-chart [query]="card.query" *ngIf="card.chart === 'bar'"></app-bar-chart>
          </div>
        </mat-card-content>
      </mat-card>
    </mat-grid-tile>
  </mat-grid-list>
</div>
```

Well done! 🎉 Here's what our dashboard application looks like:

![](/images/image-48.png)

## KPI Chart

The KPI chart can be used to display business indicators that provide information about the current performance of our e-commerce company. The chart will consist of a grid of tiles, where each tile will display a single numeric KPI value for a certain category.

First, let's add the `countUp` package to add the count-up animation to the values on the KPI chart. Run the following command in the dashboard-app folder:

```bash
npm i ngx-countup @angular/material/progress-bar
```

We weed to import these modules:

```diff
+ import { CountUpModule } from 'ngx-countup';
+ import { MatProgressBarModule } from '@angular/material/progress-bar'

@NgModule({
  imports: [

//    ...

+    CountUpModule,
+    MatProgressBarModule

  ],
  ...
})
```

Second, let's add an array of cards we're going to display to the `dashboard-page.component.ts` file:

```diff
export class DashboardPageComponent implements OnInit {

// ...

+  public KPICards = [
+    {
+      title: 'ORDERS',
+      query: { measures: ['Orders.count'] },
+      difference: 'Orders',
+      duration: 1.25,
+    },
+    {
+      title: 'TOTAL USERS',
+      query: { measures: ['Users.count'] },
+      difference: 'Users',
+      duration: 1.5,
+    },
+    {
+      title: 'COMPLETED ORDERS',
+      query: { measures: ['Orders.percentOfCompletedOrders'] },
+      progress: true,
+      duration: 1.75,
+    },
+    {
+      title: 'TOTAL PROFIT',
+      query: { measures: ['LineItems.price'] },
+      duration: 2.25,
+    },
+  ];

// ...

}
```

The next step is create the KPI Card component. Run:

```bash
ng generate component kpi-card
```

Edit this component's code:

```javascript
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
  constructor(private cubejs:CubejsClient){}
  public result = 0;
  public postfix = null;
  public prefix = null;

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
  }

}
```

And the component's template:

```html
<mat-card class="dashboard-card">
  <mat-card-header class="dashboard-card__header">
    <mat-card-title>
      <h3 class="kpi-title">{{title}}</h3>
    </mat-card-title>
  </mat-card-header>
  <mat-card-content class="dashboard-card-content kpi-result">
    <span>{{prefix}}</span>
    <span [countUp]="result" [options]="{duration: duration}">0</span>
    <span>{{postfix}}</span>
    <mat-progress-bar [color]="'primary'" class="kpi-progress" *ngIf="progress" value="{{result}}"></mat-progress-bar>
  </mat-card-content>
</mat-card>
```

The final step is to add this component to our dashboard page. To do so, open `dashboard-page.component.html` and replace the code:

```html
<div class="grid-container">
  <div class="kpi-wrap">
    <mat-grid-list cols="4" rowHeight="131px">
      <mat-grid-tile *ngFor="let card of KPICards" [colspan]="1" [rowspan]="1">
        <app-kpi-card class="kpi-card"
                      [query]="card.query"
                      [title]="card.title"
                      [duration]="card.duration"
                      [progress]="card.progress"
        ></app-kpi-card>
      </mat-grid-tile>
    </mat-grid-list>
  </div>
  <div>
    <mat-grid-list cols="5" rowHeight="510px">
      <mat-grid-tile *ngFor="let card of cards" [colspan]="card.cols" [rowspan]="card.rows">
        <mat-card class="dashboard-card">
          <mat-card-header class="dashboard-card__header">
            <mat-card-title>
              <h3>Last sales</h3>
              <button mat-icon-button class="more-button" [matMenuTriggerFor]="menu" aria-label="Toggle menu">
                <mat-icon>more_vert</mat-icon>
              </button>
              <mat-menu #menu="matMenu" xPosition="before">
                <button mat-menu-item  (click)="changeDateRange('This year')">This year</button>
                <button mat-menu-item  (click)="changeDateRange('Last year')">Last year</button>
              </mat-menu>
            </mat-card-title>
          </mat-card-header>
          <mat-card-content class="dashboard-card-content">
            <div>
              <app-bar-chart [query]="card.query" *ngIf="card.chart === 'bar'"></app-bar-chart>
            </div>
          </mat-card-content>
        </mat-card>
      </mat-grid-tile>
    </mat-grid-list>
  </div>
</div>
```

The only thing left is to adjust the Cube.js schema. While doing it, we'll learn an important aspect of Cube.js...

**Let's learn how to create custom measures in the data schema and display their values.** In the e-commerce business, it's crucial to know the share of completed orders. To enable our users to monitor this metric, we'll want to display it on the KPI chart. So, we will modify the data schema by [adding a custom measure](https://cube.dev/docs/measures/) (`percentOfCompletedOrders`) which will calculate the share based on another measure (`completedCount`).

Let's customize the "Orders" schema. Open the `schema/Orders.js` file in the root folder of the Cube.js project and make the following changes: 

- add the `completedCount` measure
- add the `percentOfCompletedOrders` measure

```diff
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  // ...

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    },
    number: {
      sql: `number`,
      type: `sum`
    },
+    completedCount: {
+      sql: `id`,
+      type: `count`,
+      filters: [
+        { sql: `${CUBE}.status = 'completed'` }
+      ]
+    },
+    percentOfCompletedOrders: {
+      sql: `${completedCount} * 100.0 / ${count}`,
+      type: `number`,
+      format: `percent`
+    }
  },

  // ...
```

Great! 🎉 Now our dashboard has a row of nice and informative KPI metrics:

![](/images/image-18.png)

## Doughnut Chart

Now, using the KPI chart, our users are able to monitor the share of completed orders. However, there are two more kinds of orders: "processed" orders (ones that were acknowledged but not yet shipped) and "shipped" orders (essentially, ones that were taken for delivery but not yet completed).

To enable our users to monitor all these kinds of orders, we'll want to add one final chart to our dashboard. It's best to use the Doughnut chart for that, because it's quite useful to visualize the distribution of a certain metric between several states (e.g., all kinds of orders).

First, let's create the `DoughnutChart` component. Run:

```bash
ng generate component doughnut-chart
```

Then edit the `doughnut-chart.component.ts` file:

```javascript
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
    maintainAspectRatio: false,
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
```

And the template in the `doughnut-chart.component.html` file:

```html
<div>
  <canvas baseChart
          height="215"
          [datasets]="barChartData"
          [labels]="barChartLabels"
          [options]="barChartOptions"
          [legend]="barChartLegend"
          [chartType]="barChartType">
  </canvas>
  <mat-grid-list cols="3">
    <mat-grid-tile *ngFor="let card of labels" [colspan]="1" [rowspan]="1">
      <div>
        <h3 class="doughnut-label">{{card.category}}</h3>
        <h2 class="doughnut-number">{{((card.value/value) * 100).toFixed(1)}}%</h2>
      </div>
    </mat-grid-tile>
  </mat-grid-list>
</div>
```

The next step is to add this card to the `dashboard-page.component.ts` file:

```diff
export class DashboardPageComponent implements OnInit {

// ...

+  private doughnutQuery = new BehaviorSubject({
+    measures: ['Orders.count'],
+    timeDimensions: [
+      {
+        dimension: 'Orders.createdAt',
+      },
+    ],
+    filters: [],
+    dimensions: ['Orders.status'],
+  });
 
  ngOnInit() {
    ...
+    this.doughnutQuery.subscribe(data => {
+      this.cards[1] = {
+        hasDatePick: false,
+        title: 'Users by Device',
+        chart: "doughnut", cols: 2, rows: 1,
+        query: data
+      };
+    });
  }
}
```

And the last step is to use this template in the `dashboard-page.component.html` file:

```diff
<div class="grid-container">

// ...

    <mat-grid-list cols="5" rowHeight="510px">
      <mat-grid-tile *ngFor="let card of cards" [colspan]="card.cols" [rowspan]="card.rows">
        <mat-card class="dashboard-card">
          <mat-card-header class="dashboard-card__header">
            <mat-card-title>
              <h3>{{card.title}}</h3>
+             <div *ngIf="card.hasDatePick">
                <button mat-icon-button class="more-button" [matMenuTriggerFor]="menu" aria-label="Toggle menu">
                  <mat-icon>more_vert</mat-icon>
                </button>
                <mat-menu #menu="matMenu" xPosition="before">
                  <button mat-menu-item  (click)="changeDateRange('This year')">This year</button>
                  <button mat-menu-item  (click)="changeDateRange('Last year')">Last year</button>
                </mat-menu>
+             </div>
            </mat-card-title>
          </mat-card-header>
          <mat-card-content class="dashboard-card-content">
            <div>
              <app-bar-chart [query]="card.query" *ngIf="card.chart === 'bar'"></app-bar-chart>
+              <app-doughnut-chart [query]="card.query" *ngIf="card.chart === 'doughnut'"></app-doughnut-chart>
            </div>
          </mat-card-content>
        </mat-card>
      </mat-grid-tile>
    </mat-grid-list>
  </div>
</div>
```

Awesome! 🎉 Now the first page of our dashboard is complete:

![](/images/image-53.png)
