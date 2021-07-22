import { Component, Inject, OnDestroy, OnInit } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { ActivatedRoute } from '@angular/router';
import { ResultSet } from '@cubejs-client/core';
import {
  BuilderMeta,
  CubejsClient,
  Query,
  QueryBuilderService,
} from '@cubejs-client/ngx';

import { SettingsDialogComponent } from '../settings-dialog/settings-dialog.component';
import { AddToDashboardDialogComponent } from './add-to-dashboard-dialog/add-to-dashboard-dialog.component';

@Component({
  selector: 'app-explore',
  templateUrl: './explore.component.html',
  styleUrls: ['./explore.component.css'],
})
export class ExploreComponent implements OnInit, OnDestroy {
  itemId: number | null = null;
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
    @Inject(CubejsClient) public cubejsClient: CubejsClient,
    public queryBuilder: QueryBuilderService,
    public dialog: MatDialog,
    private route: ActivatedRoute
  ) {
    cubejsClient.ready$.subscribe(
      (ready) => ready && queryBuilder.setCubejsClient(cubejsClient)
    );

    this.chartTypeMap = this.chartTypeToIcon.reduce(
      (memo, { chartType, icon }) => ({ ...memo, [chartType]: icon }),
      {}
    );
  }

  async ngOnInit(): Promise<void> {
    this.builderMeta = await this.queryBuilder.builderMeta;
    this.query = await this.queryBuilder.query;

    this.route.queryParams.subscribe((params) => {
      this.itemId = params.id;

      if (params.query) {
        this.queryBuilder.deserialize({
          query: params.query && JSON.parse(params.query),
          pivotConfig:
            (params.pivotConfig && JSON.parse(params.pivotConfig)) || null,
          chartType: params.chartType,
        });
      }
    });

    this.query.subject.subscribe(() => {
      this.filterMembers = this.query.filters.asArray();
      this.timeDimensionMembers = this.query.timeDimensions.asArray();
    });
  }

  ngOnDestroy(): void {
    this.queryBuilder.deserialize({
      query: {},
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

  openAddToDashboardDialog(): void {
    const dialogRef = this.dialog.open(AddToDashboardDialogComponent, {
      width: '500px',
      data: {
        itemId: this.itemId,
        cubeQuery: this.query.get(),
        pivotConfig: this.queryBuilder.pivotConfig.get(),
        chartType: this.queryBuilder.chartType.get(),
      },
    });

    dialogRef.updatePosition({
      top: '10%',
    });
  }
}
