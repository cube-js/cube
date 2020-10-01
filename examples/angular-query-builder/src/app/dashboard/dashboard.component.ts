import { Component, OnInit } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { ResultSet } from '@cubejs-client/core';
import {
  BuilderMeta,
  CubejsClient,
  Query,
  QueryBuilderService,
} from '@cubejs-client/ngx';

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
      chartType: 'area',
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
  timeDimensionMembers: any[] = [];

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
    this.builderMeta = await this.queryBuilder.builderMeta;
    this.query = await this.queryBuilder.query;

    this.query.subject.subscribe(() => {
      this.filterMembers = this.query.filters.asArray();
      this.timeDimensionMembers = this.query.timeDimensions.asArray();
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
}
