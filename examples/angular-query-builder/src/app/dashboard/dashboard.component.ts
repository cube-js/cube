import { Component, OnInit } from '@angular/core';
import {
  CubejsClient,
  BuilderMeta,
  QueryBuilderService,
  Query,
} from '@cubejs-client/ngx';
import { ResultSet } from '@cubejs-client/core';
import { MatDialog } from '@angular/material/dialog';
import { SettingsDialogComponent } from '../settings-dialog/settings-dialog.component';

@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.css'],
})
export class DashboardComponent implements OnInit {
  query: Query;
  builderMeta: BuilderMeta;
  resultSet: ResultSet;
  chartTypeToIcon = [
    {
      chartType: 'line',
      icon: 'multiline_chart',
    },
    {
      chartType: 'bar',
      icon: 'bar_chart',
    },
    {
      chartType: 'pie',
      icon: 'pie_chart',
    },
    {
      chartType: 'table',
      icon: 'table_chart',
    },
  ];
  chartTypeMap = {};
  filterMembers: any[] = [];

  constructor(
    public cubejsClient: CubejsClient,
    public queryBuilder: QueryBuilderService,
    public dialog: MatDialog
  ) {
    queryBuilder.setCubejsClient(cubejsClient);
    // queryBuilder.disableHeuristics();
    this.chartTypeMap = this.chartTypeToIcon.reduce(
      (memo, { chartType, icon }) => ({ ...memo, [chartType]: icon }),
      {}
    );
  }

  async ngOnInit() {
    this.queryBuilder.deserialize({
      query: {
        measures: ['Sales.count', 'Sales.amount'],
        dimensions: ['Users.gender'],
        timeDimensions: [{
          dimension: 'Sales.ts',
          granularity: 'month',
        }]
        // filters: [
        //   {
        //     dimension: 'Sales.title',
        //     operator: 'contains',
        //     values: ['test'],
        //   },
        // ],
      },
      pivotConfig: {
        x: ['Sales.ts.month'],
        y: ['Users.gender', 'measures'],
      },
      chartType: 'table',
    });

    this.builderMeta = await this.queryBuilder.builderMeta;
    this.query = await this.queryBuilder.query;

    this.query.subject.subscribe(() => {
      this.filterMembers = this.query.filters.asArray();
    }); 
  }

  openDialog(): void {
    const dialogRef = this.dialog.open(SettingsDialogComponent, {
      width: '500px',
      data: {
        pivotConfig: this.queryBuilder.pivotConfig,
        query: this.query,
      },
    });

    dialogRef.updatePosition({
      top: '10%',
    });
  }

  // todo: remove (testing only)
  setQuery() {
    this.query.setQuery(
      Object.keys(this.query.asCubeQuery()).length
        ? {}
        : {
            measures: ['Sales.amount', 'Sales.count'],
            dimensions: ['Users.gender'],
            timeDimensions: [
              {
                dimension: 'Sales.ts',
                granularity: 'month',
                dateRange: 'This year',
              },
            ],
          }
    );
  }

  debug() {}
}
