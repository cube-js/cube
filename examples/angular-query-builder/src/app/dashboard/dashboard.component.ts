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

  constructor(
    public cubejsClient: CubejsClient,
    public queryBuilder: QueryBuilderService,
    public dialog: MatDialog
  ) {
    queryBuilder.setCubejsClient(cubejsClient);
    this.chartTypeMap = this.chartTypeToIcon.reduce(
      (memo, { chartType, icon }) => ({ ...memo, [chartType]: icon }),
      {}
    );
  }

  async ngOnInit() {
    this.queryBuilder.deserialize({
      query: {
        measures: ['Sales.count'],
        dimensions: ['Users.country', 'Users.gender'],
      },
      // pivotConfig: {
      // x: ['Users.country'],
      // y: ['measures'],
      // },
      chartType: 'table',
    });

    this.queryBuilder.builderMeta.subscribe((builderMeta) => {
      this.builderMeta = builderMeta;
    });

    this.query = await this.queryBuilder.query;
    // this.queryBuilder.state.subscribe((vizState) =>
    // console.log('vizState', JSON.stringify(vizState))
    // );
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
    dialogRef.afterClosed().subscribe(() => {
      console.log('closed');
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

  debug(event: any) {
    // this.query.setPartialQuery({
    // order: [['Users.country', 'desc'], ['Users.gender', 'asc']]
    // order: {'Users.country': 'desc','Users.gender': 'asc'}
    // })
    // this.query.order.setMemberOrder('Sales.amount', 'desc');
    // this.query.setLimit(50);
    console.log(event.target.value);
  }
}
