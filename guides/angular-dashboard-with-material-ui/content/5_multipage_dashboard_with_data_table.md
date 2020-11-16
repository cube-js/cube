---
order: 5
title: "Multi-Page Dashboard with Data Table"
---

Now we have a single-page dashboard that displays aggregated business metrics and provides at-a-glance view of several KPIs. However, there's no way to get information about a particular order or a range of orders.

We're going to fix it by adding a second page to our dashboard with the information about all orders. However, we'll need a way to navigate between two pages. So, let's add a navigation side bar.

## Navigation Side Bar

Now we need a router, so let's add a module for this. Run:

```bash
ng generate module app-routing --flat --module=app
```

And then edit the `app-routing.module.ts` file:

```javascript
import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { DashboardPageComponent } from './dashboard-page/dashboard-page.component';
import { TablePageComponent } from './table-page/table-page.component';

const routes: Routes = [
  { path: '', component: DashboardPageComponent },
  { path: 'table', component: TablePageComponent },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
```

Now we need to add new modules to the `app.module.ts` file:

```diff
// ...

import { CountUpModule } from 'ngx-countup';
import { DoughnutChartComponent } from './doughnut-chart/doughnut-chart.component';
+ import { AppRoutingModule } from './app-routing.module';
+ import { MatListModule } from '@angular/material/list';

// ...

    CountUpModule,
    MatProgressBarModule,
+    AppRoutingModule,
+    MatListModule,
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
```

The last step is to set the `app.component.html` file to this code:

```html
<style>
  * {
    box-sizing: border-box;
  }
  .toolbar {
    position: relative;
    top: 0;
    left: 0;
    right: 0;
    height: 60px;
    display: flex;
    align-items: center;
    background-color: #43436B;
    color: #D5D5E2;
    font-size: 16px;
    font-style: normal;
    font-weight: 400;
    line-height: 26px;
    letter-spacing: 0.02em;
    text-align: left;
    padding: 0 1rem;
  }
  .spacer {
    flex: 1;
  }

  .toolbar img {
    margin: 0 16px;
  }
  .root {
    width: 100%;
    display: flex;
    position: relative;
  }
  .component {
    width: 82.2%;
    min-height: 100vh;
    padding-top: 1rem;
    background: #F3F3FB;
  }
  .divider {
    width: 17.8%;
    background: #fff;
    padding: 1rem;
  }
  .nav-link {
    text-decoration: none;
    color: #A1A1B5;
  }
  .nav-link:hover .mat-list-item {
    background-color: rgba(67, 67, 107, 0.04);
  }
  .nav-link .mat-list-item {
    color: #A1A1B5;
  }
  .nav-link.active-link .mat-list-item {
    color: #7A77FF;
  }
</style>
<!-- Toolbar -->
<div class="toolbar" role="banner">
  <span>Angular Dashboard with Material</span>
  <div class="spacer"></div>
  <div class="links">
    <a
      aria-label="Cube.js on github"
      target="_blank"
      rel="noopener"
      href="https://github.com/cube-js/cube.js/tree/master/examples/angular-dashboard-with-material-ui"
      title="Cube.js on GitHub"
    >GitHub</a>
    <a
      aria-label="Cube.js on Slack"
      target="_blank"
      rel="noopener"
      href="https://slack.cube.dev/"
      title="Cube.js on Slack"
    >Slack</a>
  </div>
</div>
<div class="root">
  <div class="divider">
    <mat-list>
      <a class="nav-link"
         routerLinkActive="active-link"
         [routerLinkActiveOptions]="{exact: true}"
         *ngFor="let link of links" [routerLink]="[link.href]"
      >
        <mat-list-item>
          <mat-icon mat-list-icon>{{link.icon}}</mat-icon>
          <div mat-line>{{link.name}}</div>
        </mat-list-item>
      </a>
    </mat-list>
  </div>
  <div class="component">
    <router-outlet class="content"></router-outlet>
  </div>
</div>
```

To make everything finally work, let's add links to our `app.component.ts`:

```diff
import { Component } from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})
export class AppComponent {
+  public links = [
+    {name: 'Dashboard', href: '/', icon: 'dashboard'},
+    {name: 'Orders', href: '/table', icon: 'assignment'}
+    ];
  title = 'dashboard-app';
}
```

Wow! 🎉 Here's our navigation side bar which can be used to switch between different pages of the dashboard:

![](/images/image-31.png)

## Data Table for Orders

To fetch data for the Data Table, we'll need to customize the data schema and define a number of new metrics: amount of items in an order (its size), an order's price, and a user's full name.

First, let's add the full name in the "Users" schema in the `schema/Users.js` file:

```diff
cube(`Users`, {
  sql: `SELECT * FROM public.users`,

	// ...

  dimensions: {    
		
		// ...

    firstName: {
      sql: `first_name`,
      type: `string`
    },

    lastName: {
      sql: `last_name`,
      type: `string`
    },

+    fullName: {
+      sql: `CONCAT(${firstName}, ' ', ${lastName})`,
+      type: `string`
+    },

    age: {
      sql: `age`,
      type: `number`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
```

Then, let's add other measures to the "Orders" schema in the `schema/Orders.js` file.

For these measures, we're going to use the [subquery](https://cube.dev/docs/subquery#top) feature of Cube.js. You can use subquery dimensions to reference measures from other cubes inside a dimension. Here's how to defined such dimensions:

```diff
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
+      shown: true
    },

    status: {
      sql: `status`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    },

    completedAt: {
      sql: `completed_at`,
      type: `time`
    },

+    size: {
+      sql: `${LineItems.count}`,
+      subQuery: true,
+      type: 'number'
+    },
+
+    price: {
+      sql: `${LineItems.price}`,
+      subQuery: true,
+      type: 'number'
+    }
  }
});
```

Now we're ready to add a new page. Let's creating the `table-page` component. Run:

```bash
ng generate component table-page
```

Edit the `table-page.module.ts` file:

```javascript
import { Component, OnInit } from '@angular/core';
import { BehaviorSubject } from "rxjs";

@Component({
  selector: 'app-table-page',
  templateUrl: './table-page.component.html',
  styleUrls: ['./table-page.component.scss']
})
export class TablePageComponent implements OnInit {
  public _query = new BehaviorSubject({
    "limit": 500,
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
        "granularity": "day"
      }
    ],
    "dimensions": [
      "Users.id",
      "Orders.id",
      "Orders.size",
      "Users.fullName",
      "Users.city",
      "Orders.price",
      "Orders.status",
      "Orders.createdAt"
    ]
  });
  public query = {};

  constructor() { }

  ngOnInit(): void {
    this._query.subscribe(query => {
      this.query = query;
    });
  }

}
```

And set the template to these contents:

```html
<div class="table-warp">
  <app-material-table [query]="query"></app-material-table>
</div>
```

Note that this component contains a Cube.js query. Later, we'll modify this query to enable the filtering of the data.

Also, let's create the `material-table` component. Run:

```bash
ng generate component material-table
```

Add it to the `app.module.ts` file:

```diff
+ import { MatTableModule } from '@angular/material/table'

  imports: [

// ...

+    MatTableModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
```

And edit the `material-table.module.ts` file:

```jsx
import { Component, OnInit, Input } from "@angular/core";
import { CubejsClient } from "@cubejs-client/ngx";

@Component({
  selector: "app-material-table",
  templateUrl: "./material-table.component.html",
  styleUrls: ["./material-table.component.scss"]
})
export class MaterialTableComponent implements OnInit {
  @Input() query: object;

  constructor(private cubejs: CubejsClient) {
  }
  public dataSource = [];
  displayedColumns = ['id', 'size', 'name', 'city', 'price', 'status', 'date'];

  ngOnInit(): void {
    this.cubejs.load(this.query).subscribe(
      resultSet => {
        this.dataSource = resultSet.tablePivot();
      },
      err => console.log("HTTP Error", err)
    );
  }

}
```

Then set its template to these contents:

```html
<table style="width: 100%; box-shadow: none"
       mat-table
       matSort
       [dataSource]="dataSource"
       class="table mat-elevation-z8"
>

  <ng-container matColumnDef="id">
    <th mat-header-cell *matHeaderCellDef mat-sort-header> Order ID</th>
    <td mat-cell *matCellDef="let element"> {{element['Orders.id']}} </td>
  </ng-container>

  <ng-container matColumnDef="size">
    <th mat-header-cell *matHeaderCellDef> Orders size</th>
    <td mat-cell *matCellDef="let element"> {{element['Orders.size']}} </td>
  </ng-container>

  <ng-container matColumnDef="name">
    <th mat-header-cell *matHeaderCellDef> Full Name</th>
    <td mat-cell *matCellDef="let element"> {{element['Users.fullName']}} </td>
  </ng-container>

  <ng-container matColumnDef="city">
    <th mat-header-cell *matHeaderCellDef> User city</th>
    <td mat-cell *matCellDef="let element"> {{element['Users.city']}} </td>
  </ng-container>

  <ng-container matColumnDef="price">
    <th mat-header-cell *matHeaderCellDef> Order price</th>
    <td mat-cell *matCellDef="let element"> {{element['Orders.price']}} </td>
  </ng-container>

  <ng-container matColumnDef="status">
    <th mat-header-cell *matHeaderCellDef> Status</th>
    <td mat-cell *matCellDef="let element"> {{element['Orders.status']}} </td>
  </ng-container>

  <ng-container matColumnDef="date">
    <th mat-header-cell *matHeaderCellDef> Created at</th>
    <td mat-cell *matCellDef="let element"> {{element['Orders.createdAt'] | date}} </td>
  </ng-container>

  <tr mat-header-row *matHeaderRowDef="displayedColumns"></tr>
  <tr mat-row *matRowDef="let row; columns: displayedColumns;"></tr>
  <!--<mat-paginator [pageSizeOptions]="[5, 10, 25, 100]"></mat-paginator>-->
</table>
```

Time to add pagination!

Again, let's add modules to `app.module.ts`:

```diff
+ import {MatPaginatorModule} from "@angular/material/paginator";
+ import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";

@NgModule({
  ...
  imports: [
+    MatPaginatorModule,
+    MatProgressSpinnerModule
  ],
  ...
})
export class AppModule { }
```

Then, let's edit the template:

```diff
+ <div class="example-loading-shade"
+      *ngIf="loading">
+   <mat-spinner></mat-spinner>
+ </div>

+ <div class="example-table-container">
  <table style="width: 100%; box-shadow: none"
         mat-table
         matSort
         [dataSource]="dataSource"
         class="table mat-elevation-z8"
  >

// ...

  </table>
+ </div>
+ <mat-paginator [length]="length"
+               [pageSize]="pageSize"
+               [pageSizeOptions]="pageSizeOptions"
+               (page)="pageEvent.emit($event)"
+ ></mat-paginator>
```

The styles...

```scss
/* Structure */
.example-container {
  position: relative;
  min-height: 200px;
}

.example-table-container {
  position: relative;
  max-height: 75vh;
  overflow: auto;
}

table {
  width: 100%;
}

.example-loading-shade {
  position: absolute;
  top: 0;
  left: 0;
  bottom: 56px;
  right: 0;
  background: rgba(0, 0, 0, 0.15);
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: center;
}

.example-rate-limit-reached {
  color: #980000;
  max-width: 360px;
  text-align: center;
}

/* Column Widths */
.mat-column-number,
.mat-column-state {
  max-width: 64px;
}

.mat-column-created {
  max-width: 124px;
}

.table th {
  background: #F8F8FC;
  color: #43436B;
  font-weight: 500;
  line-height: 1.5rem;
  border-bottom: 1px solid #eeeeee;
  &:hover {
    color: #7A77FF;
    cursor: pointer;
  }
}
.table thead {
  background: #F8F8FC;
}
```

And the component:

```scss
import { Component, Input, Output } from "@angular/core";
import { CubejsClient } from "@cubejs-client/ngx";
import { EventEmitter } from '@angular/core';

@Component({
  selector: "app-material-table",
  templateUrl: "./material-table.component.html",
  styleUrls: ["./material-table.component.scss"]
})
export class MaterialTableComponent {
  constructor(private cubejs: CubejsClient) {}
  @Input() set query(query: object) {
    this.loading = true;
    this.cubejs.load(query).subscribe(
      resultSet => {
        this.dataSource = resultSet.tablePivot();
        this.loading = false;
      },
      err => console.log("HTTP Error", err)
    );
    this.cubejs.load({...query, limit: 50000, offset: 0}).subscribe(
      resultSet => {
        this.length = resultSet.tablePivot().length;
      },
      err => console.log("HTTP Error", err)
    );
  };
  @Input() limit: number;
  @Output() pageEvent = new EventEmitter();
  loading = true;
  length = 0;
  pageSize = 10;
  pageSizeOptions: number[] = [5, 10, 25, 100];
  dataSource = [];
  displayedColumns = ['id', 'size', 'name', 'city', 'price', 'status', 'date'];
}
```

The last edits will be to the `table-page-component.ts` file:

```jsx
import { Component, OnInit } from '@angular/core';
import { BehaviorSubject } from "rxjs";

@Component({
  selector: 'app-table-page',
  templateUrl: './table-page.component.html',
  styleUrls: ['./table-page.component.scss']
})
export class TablePageComponent implements OnInit {
  public limit = 50;
  public page = 0;
  public _query = new BehaviorSubject({
    "limit": this.limit,
    "offset": this.page * this.limit,
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
        "granularity": "day"
      }
    ],
    "dimensions": [
      "Users.id",
      "Orders.id",
      "Orders.size",
      "Users.fullName",
      "Users.city",
      "Orders.price",
      "Orders.status",
      "Orders.createdAt"
    ],
    filters: []
  });
  public query = null;
  public changePage = (obj) => {
    this._query.next({
      ...this._query.value,
      "limit": obj.pageSize,
      "offset": obj.pageIndex * obj.pageSize,
    });
  };
  public statusChanged(value) {
    this._query.next({...this._query.value,
      "filters": this.getFilters(value)});
  };
  private getFilters = (value) => {
    return [
      {
        "dimension": "Orders.status",
        "operator": value === 'all' ? "set" : "equals",
        "values": [
          value
        ]
      }
    ]
  };

  constructor() { }

  ngOnInit(): void {
    this._query.subscribe(query => {
      this.query = query;
    });
  }

}
```

And the related template:

```html
<div class="table-warp">
  <app-material-table [query]="query" [limit]="limit" (pageEvent)="changePage($event)"></app-material-table>
</div>
```

Voila! 🎉 Now we have a table which displays information about all orders: 

![](/images/image-47.png)

However, its hard to explore this orders using only the controls provided. To fix this, we'll add a comprehensive toolbar with filters and make our table interactive.

For this, let's create the `table-filters` component. Run:

```bash
ng generate component table-filters
```

Set the module contents:

```jsx
import { Component, EventEmitter, OnInit, Output } from "@angular/core";

@Component({
  selector: 'app-table-filters',
  templateUrl: './table-filters.component.html',
  styleUrls: ['./table-filters.component.scss']
})
export class TableFiltersComponent implements OnInit {
  @Output() statusChanged = new EventEmitter();
  statusChangedFunc = (obj) => {
    this.statusChanged.emit(obj.value);
  };

  constructor() { }

  ngOnInit(): void {
  }

}
```

And the template...

```html
<mat-button-toggle-group class="table-filters"
                         (change)="statusChangedFunc($event)">
  <mat-button-toggle value="all">All</mat-button-toggle>
  <mat-button-toggle value="shipped">Shipped</mat-button-toggle>
  <mat-button-toggle value="processing">Processing</mat-button-toggle>
  <mat-button-toggle value="completed">Completed</mat-button-toggle>
</mat-button-toggle-group>
```

With styles...

```scss
.table-filters {
  margin-bottom: 2rem;
  .mat-button-toggle-appearance-standard {
    background: transparent;
    color: #43436b;
  }
}
.mat-button-toggle-standalone.mat-button-toggle-appearance-standard, .mat-button-toggle-group-appearance-standard.table-filters {
  border: none;
  -webkit-border-radius: 0;
  -moz-border-radius: 0;
  border-radius: 0;
  border-bottom: 1px solid #7A77FF;
}
.mat-button-toggle-checked {
  border-bottom: 2px solid #7A77FF;
}
.mat-button-toggle-group-appearance-standard .mat-button-toggle + .mat-button-toggle {
  border-left: none;
}
```

The last step will be to add it to the `table-page.component.html` file:

```diff
 <div class="table-warp">
+  <app-table-filters (statusChanged)="statusChanged($event)"></app-table-filters>
  <app-material-table [query]="query" [limit]="limit" (pageEvent)="changePage($event)"></app-material-table>
 </div>
```

Perfect! 🎉 Now the data table has a filter which switches between different types of orders:

![](/images/image-1.gif)

However, orders have other parameters such as price and dates. Let's create filters for these parameters and enable sorting in the table. 

Edit the `table-filters` component:

```jsx
import { Component, EventEmitter, OnInit, Output } from "@angular/core";

@Component({
  selector: 'app-table-filters',
  templateUrl: './table-filters.component.html',
  styleUrls: ['./table-filters.component.scss']
})
export class TableFiltersComponent implements OnInit {
  @Output() statusChanged = new EventEmitter();
  @Output() dateChange = new EventEmitter();
  @Output() sliderChanged = new EventEmitter();

  statusChangedFunc = (obj) => {
    this.statusChanged.emit(obj.value);
  };
  changeDate(number, date) {
    this.dateChange.emit({number, date});
  };
  formatLabel(value: number) {
    if (value >= 1000) {
      return Math.round(value / 1000) + 'k';
    }
    return value;
  }
  sliderChange(value) {
    this.sliderChanged.emit(value);
  }

  constructor() { }

  ngOnInit(): void {
  }

}
```

And its template:

```html
<mat-grid-list cols="4" rowHeight="131px">

  <mat-grid-tile>
    <mat-button-toggle-group class="table-filters"
                             (change)="statusChangedFunc($event)">
      <mat-button-toggle value="all">All</mat-button-toggle>
      <mat-button-toggle value="shipped">Shipped</mat-button-toggle>
      <mat-button-toggle value="processing">Processing</mat-button-toggle>
      <mat-button-toggle value="completed">Completed</mat-button-toggle>
    </mat-button-toggle-group>
  </mat-grid-tile>

  <mat-grid-tile>
    <mat-form-field class="table-filters__date-form" color="primary" (change)="changeDate(0, $event)">
      <mat-label>Start date</mat-label>
      <input #ref matInput [matDatepicker]="picker1" (dateChange)="changeDate(0, ref.value)">
      <mat-datepicker-toggle matSuffix [for]="picker1"></mat-datepicker-toggle>
      <mat-datepicker #picker1></mat-datepicker>
    </mat-form-field>
  </mat-grid-tile>

  <mat-grid-tile>
    <mat-form-field class="table-filters__date-form" color="primary" (change)="changeDate(1, $event)">
      <mat-label>Finish date</mat-label>
      <input #ref1 matInput [matDatepicker]="picker2" (dateChange)="changeDate(1, ref1.value)">
      <mat-datepicker-toggle matSuffix [for]="picker2"></mat-datepicker-toggle>
      <mat-datepicker #picker2></mat-datepicker>
    </mat-form-field>
  </mat-grid-tile>

  <mat-grid-tile>
    <div>
      <mat-label class="price-label">Price range</mat-label>
      <mat-slider
        color="primary"
        thumbLabel
        (change)="sliderChange($event)"
        [displayWith]="formatLabel"
        tickInterval="10"
        min="1"
        max="1200"></mat-slider>
    </div>
  </mat-grid-tile>
</mat-grid-list>
```

Again, add plenty of modules to the `app.module.ts` file:

```diff
// ...

import { TableFiltersComponent } from "./table-filters/table-filters.component";
import { MatButtonToggleModule } from "@angular/material/button-toggle";
+ import { MatDatepickerModule } from "@angular/material/datepicker";
+ import { MatFormFieldModule } from "@angular/material/form-field";
+ import { MatNativeDateModule } from "@angular/material/core";
+ import { MatInputModule } from "@angular/material/input";
+ import {MatSliderModule} from "@angular/material/slider";

// ...

    MatProgressSpinnerModule,
    MatButtonToggleModule,
+    MatDatepickerModule,
+    MatFormFieldModule,
+    MatNativeDateModule,
+    MatInputModule,
+    MatSliderModule
  ],
+  providers: [MatDatepickerModule],
  bootstrap: [AppComponent]
})
export class AppModule {
}
```

Edit the `table-page.component.html` file:

```diff
 <div class="table-warp">
  <app-table-filters (statusChanged)="statusChanged($event)"
                     (dateChange)="dateChanged($event)"
                     (sliderChanged)="sliderChanged($event)"
  ></app-table-filters>
  <app-material-table [query]="query"
                      [limit]="limit"
                      (pageEvent)="changePage($event)"
+                      (sortingChanged)="sortingChanged($event)"></app-material-table>
 </div>
```

And the `table-page` component:

```diff
import { Component, OnInit } from "@angular/core";
import { BehaviorSubject } from "rxjs";

@Component({
  selector: "app-table-page",
  templateUrl: "./table-page.component.html",
  styleUrls: ["./table-page.component.scss"]
})
export class TablePageComponent implements OnInit {
...
+  public limit = 50;
+  public page = 0;
+  public sorting = ['Orders.createdAt', 'desc'];
+  public startDate = "01/1/2019";
+  public finishDate = "01/1/2022";
+  private minPrice = 0;
  public _query = new BehaviorSubject({
+    "limit": this.limit,
+    "offset": this.page * this.limit,
+    order: {
+      [`${this.sorting[0]}`]: this.sorting[1],
+    },
+    "timeDimensions": [
+      {
+        "dimension": "Orders.createdAt",
+        "dateRange" : [this.startDate, this.finishDate],
+        "granularity": "day"
+      }
+    ],
    "dimensions": [
      "Users.id",
      "Orders.id",
      "Orders.size",
      "Users.fullName",
      "Users.city",
      "Orders.price",
      "Orders.status",
      "Orders.createdAt"
    ],
+    filters: []
  });
+  public changePage = (obj) => {
+    this._query.next({
+      ...this._query.value,
+      "limit": obj.pageSize,
+      "offset": obj.pageIndex * obj.pageSize
+    });
+  };

+  public sortingChanged(value) {
+    if (value === this.sorting[0] && this.sorting[1] === 'desc') {
+      this.sorting[0] = value;
+      this.sorting[1] = 'asc'
+    } else if (value === this.sorting[0] && this.sorting[1] === 'asc') {
+      this.sorting[0] = value;
+      this.sorting[1] = 'desc'
+    } else {
+      this.sorting[0] = value;
+    }
+    this.sorting[0] = value;
+    this._query.next({
+      ...this._query.value,
+      order: {
+        [`${this.sorting[0]}`]: this.sorting[1],
+      },
+    });
+  }

+  public dateChanged(value) {
+    if (value.number === 0) {
+      this.startDate = value.date
+    }
+    if (value.number === 1) {
+      this.finishDate = value.date
+    }
+    this._query.next({
+      ...this._query.value,
+      timeDimensions: [
+        {
+          dimension: "Orders.createdAt",
+          dateRange: [this.startDate, this.finishDate],
+          granularity: null
+        }
+      ]
+    });
+  }

+  public statusChanged(value) {
+    this.status = value;
+    this._query.next({
+      ...this._query.value,
+      "filters": this.getFilters(this.status, this.minPrice)
+    });
+  };

+  public sliderChanged(obj) {
+    this.minPrice = obj.value;
+    this._query.next({
+      ...this._query.value,
+      "filters": this.getFilters(this.status, this.minPrice)
+    });
+  };

+  private getFilters = (status, price) => {
+    let filters = [];
+    if (status) {
+      filters.push(
+        {
+          "dimension": "Orders.status",
+          "operator": status === "all" ? "set" : "equals",
+          "values": [
+            status
+          ]
+        }
+      );
+    }
+    if (price) {
+      filters.push(
+        {
+          dimension: 'Orders.price',
+          operator: 'gt',
+          values: [`${price}`],
+        },
+      );
+    }
+    return filters;
+  };
	
 ...
}
```

Now we need to propagate the changes to the `material-table` component:

```diff
// ...

export class MaterialTableComponent {

// ...

+  @Output() sortingChanged = new EventEmitter();

// ...

+  changeSorting(value) {
+    this.sortingChanged.emit(value)
+  }
}
``` 

And its template:

```diff
// ...

    <ng-container matColumnDef="id">
      <th matSort mat-header-cell *matHeaderCellDef mat-sort-header 
+.    (click)="changeSorting('Orders.id')"> Order ID</th>
      <td mat-cell *matCellDef="let element"> {{element['Orders.id']}} </td>
    </ng-container>

    <ng-container matColumnDef="size">
      <th mat-header-cell *matHeaderCellDef mat-sort-header 
+     (click)="changeSorting('Orders.size')"> Orders size</th>
      <td mat-cell *matCellDef="let element"> {{element['Orders.size']}} </td>
    </ng-container>

    <ng-container matColumnDef="name">
      <th mat-header-cell *matHeaderCellDef mat-sort-header 
+     (click)="changeSorting('Users.fullName')"> Full Name</th>
      <td mat-cell *matCellDef="let element"> {{element['Users.fullName']}} </td>
    </ng-container>

    <ng-container matColumnDef="city">
      <th mat-header-cell *matHeaderCellDef mat-sort-header 
+    (click)="changeSorting('Users.city')"> User city</th>
      <td mat-cell *matCellDef="let element"> {{element['Users.city']}} </td>
    </ng-container>

    <ng-container matColumnDef="price">
      <th mat-header-cell *matHeaderCellDef mat-sort-header 
+    (click)="changeSorting('Orders.price')"> Order price</th>
      <td mat-cell *matCellDef="let element"> {{element['Orders.price']}} </td>
    </ng-container>

    <ng-container matColumnDef="status">
      <th mat-header-cell *matHeaderCellDef mat-sort-header 
+    (click)="changeSorting('Orders.status')"> Status</th>
      <td mat-cell *matCellDef="let element"> {{element['Orders.status']}} </td>
    </ng-container>

    <ng-container matColumnDef="date">
      <th mat-header-cell *matHeaderCellDef mat-sort-header 
+     (click)="changeSorting('Orders.createdAt')"> Created at</th>
      <td mat-cell *matCellDef="let element"> {{element['Orders.createdAt'] | date}} </td>
    </ng-container>

// ...
```

Wonderful! 🎉 Now we have the data table that fully supports filtering and sorting: 

![](/images/image-7.gif)

And that's all! 😇 Congratulations on completing this guide! 🎉

Also, check the [live demo](https://angular-dashboard-demo.cube.dev) and the [full source code](https://github.com/cube-js/cube.js/tree/master/examples/angular-dashboard-with-material-ui) available on GitHub.

Now you should be able to create comprehensive analytical dashboards powered by Cube.js using Angular and Material to display aggregate metrics and detailed information.

Feel free to explore [other examples](https://github.com/cube-js/cube.js/#examples) of what can be done with Cube.js such as the [Real-Time Dashboard Guide](https://real-time-dashboard.cube.dev) and the [Open Source Web Analytics Platform Guide](https://web-analytics.cube.dev).
