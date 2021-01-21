---
order: 3
title: "Frontend application"
---

Creating a complex dashboard from scratch usually takes time and effort. Fortunately, Angular provides a tool that helps to create an application boilerplate code with just a few commands. Adding the Material library and Cube.js as an analytical API is also very easy.

## Installing the libraries

So, let's use Angular CLI and create the frontend application inside the `angular-dashboard` folder:

```bash
npm install -g @angular/cli  # Install Angular CLI
ng new dashboard-app         # Create an app
cd dashboard-app             # Change the folder
ng serve                     # Run the app
```

Congratulations! Now we have the `dashboard-app` folder in our project. This folder contains the frontend code that we're going to modify and evolve to build our analytical dashboard.

**Now it's time to add the Material library.** To install the Material library to our application, run:

```bash
ng add @angular/material
```

Choose a custom theme and the following options:
- Set up global Angular Material typography styles? - **Yes**
- Set up browser animations for Angular Material? - **Yes**

Great! We'll also need a charting library to add charts to the dashboard. [Chart.js](https://www.chartjs.org) is the most popular charting library, it's stable and feature-rich. So...

**It's time to add the Chart.js library.** To install it, run:

```bash
npm install ng2-charts
npm install chart.js
```

Also, to be able to make use of `ng2-charts` directives in our Angular application we need to import `ChartsModule`. For that, we add the following import statement in the `app.module.ts` file:

```diff
+ import { ChartsModule } from 'ng2-charts';
```

The second step is to add `ChartsModule` to the imports array of the `@NgModule` decorator as well:

```diff
@NgModule({
  declarations: [
    AppComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
+    ChartsModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
```

**Finally, it's time to add Cube.js.** This is the final step that will let our application access the data in our database via an analytical API is to install Cube.js client libraries for Angular. Run:

```bash
npm install --save @cubejs-client/ngx
npm install --save @cubejs-client/core
```

Now we can add `CubejsClientModule` to your `app.module.ts` file:

```diff
...
+ import { CubejsClientModule } from '@cubejs-client/ngx';

+ const cubejsOptions = {
+   token: 'YOUR-CUBEJS-API-TOKEN',
+   options: { apiUrl: 'http://localhost:4200/cubejs-api/v1' }
+ };

@NgModule({
  ...
  imports: [
     ...
+    CubejsClientModule.forRoot(cubejsOptions)
  ],
  ...
})
export class AppModule { }
```

`CubejsClientModule` provides `CubejsClient` which you can inject into your components or services to make API calls and retrieve data:

```javascript
import { CubejsClient } from '@cubejs-client/ngx';

export class AppComponent {
  constructor(private cubejs:CubejsClient){}

  ngOnInit(){
    this.cubejs.load({
      measures: ["some_measure"]
    }).subscribe(
      resultSet => {
        this.data = resultSet.chartPivot();
      },
      err => console.log('HTTP Error', err)
    );
  }
}
```

So far so good! Let's make it live.

## Creating the first chart

Let's create a generic `bar-chart` component using Angular CLI. Run:

```bash
$ ng g c bar-chart  # Oh these single-letter commands!
```

This command will add four new files to our app because this is what Angular uses for its components:
- `src/app/bar-chart/bar-chart.component.html`
- `src/app/bar-chart/bar-chart.component.ts`
- `src/app/bar-chart/bar-chart.component.scss`
- `src/app/bar-chart/bar-chart.component.spec.ts`

Open `bar-chart.component.html` and replace the content of that file with the following code:

```html
<div>
  <div style="display: block">
    <canvas baseChart
						height="320"
            [datasets]="barChartData"
            [labels]="barChartLabels"
            [options]="barChartOptions"
            [legend]="barChartLegend"
            [chartType]="barChartType">
    </canvas>
  </div>
</div>
```

Here we’re using the `baseChart` directive which is added to a canvas element. Furthermore, the `datasets`, `labels`, `options`, `legend`, and `chartType` attributes are bound to class members which are added to the implementation of the `BarChartComponent` class in `bar-chart-component.ts`:

```javascript
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
```

Okay, we have the code for our chart, let's show it in the app. We can use an Angular command to generate a base grid. Run:

```bash
ng generate @angular/material:dashboard dashboard-page
```

So, now we have a folder with the `dashboard-page` component. Open `app.component.html` and insert this code:

```html
<app-dashboard-page></app-dashboard-page>
```

Now it's time to open `dashboard-page/dashboard-page.component.html` and add our component like this:

```diff
<div class="grid-container">
  <h1 class="mat-h1">Dashboard</h1>
+  <mat-grid-list cols="2" rowHeight="450px">
-    <mat-grid-tile *ngFor="let card of cards | async" [colspan]="card.cols" [rowspan]="card.rows">
+    <mat-grid-tile *ngFor="let card of cards" [colspan]="card.cols" [rowspan]="card.rows">
      <mat-card class="dashboard-card">
        <mat-card-header>
          <mat-card-title>
            <button mat-icon-button class="more-button" [matMenuTriggerFor]="menu" aria-label="Toggle menu">
              <mat-icon>more_vert</mat-icon>
            </button>
            <mat-menu #menu="matMenu" xPosition="before">
              <button mat-menu-item>Expand</button>
              <button mat-menu-item>Remove</button>
            </mat-menu>
          </mat-card-title>
        </mat-card-header>
        <mat-card-content class="dashboard-card-content">
          <div>
+            <app-bar-chart [query]="card.query" *ngIf="card.chart === 'bar'"></app-bar-chart>
          </div>
        </mat-card-content>
      </mat-card>
    </mat-grid-tile>
  </mat-grid-list>
</div>
```

And the last edit will be in `dashboard-page.component.ts`:

```javascript
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

Nice work! 🎉 That's all we need to display our first chart with the data loaded from Postgres via Cube.js. 

![](/images/image-51.png)

In the next part, we'll make this chart interactive by letting users change the date range from "This year" to other predefined values.
