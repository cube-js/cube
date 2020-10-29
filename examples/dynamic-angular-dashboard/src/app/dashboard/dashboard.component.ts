import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Apollo, gql } from 'apollo-angular';
import { BehaviorSubject, of } from 'rxjs';
import {
  CompactType,
  DisplayGrid,
  Draggable,
  GridsterConfig,
  GridType,
  PushDirections,
  Resizable,
} from 'angular-gridster2';

interface Safe extends GridsterConfig {
  draggable: Draggable;
  resizable: Resizable;
  pushDirections: PushDirections;
}

@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.css'],
})
export class DashboardComponent implements OnInit {
  dashboardItems = new BehaviorSubject<any[]>([]);
  options: Safe;

  changedOptions(): void {
    if (this.options.api && this.options.api.optionsChanged) {
      this.options.api.optionsChanged();
    }
  }

  constructor(private apollo: Apollo, private router: Router) {}

  ngOnInit() {
    this.options = {
      gridType: GridType.ScrollVertical,
      compactType: CompactType.None,
      margin: 10,
      outerMargin: true,
      outerMarginTop: null,
      outerMarginRight: null,
      outerMarginBottom: null,
      outerMarginLeft: null,
      useTransformPositioning: true,
      mobileBreakpoint: 640,
      minCols: 1,
      maxCols: 100,
      minRows: 1,
      maxRows: 100,
      maxItemCols: 100,
      minItemCols: 1,
      maxItemRows: 100,
      minItemRows: 1,
      maxItemArea: 2500,
      minItemArea: 1,
      defaultItemCols: 1,
      defaultItemRows: 1,
      fixedColWidth: 105,
      fixedRowHeight: 105,
      keepFixedHeightInMobile: false,
      keepFixedWidthInMobile: false,
      scrollSensitivity: 10,
      scrollSpeed: 20,
      enableEmptyCellClick: false,
      enableEmptyCellContextMenu: false,
      enableEmptyCellDrop: false,
      enableEmptyCellDrag: false,
      enableOccupiedCellDrop: false,
      emptyCellDragMaxCols: 50,
      emptyCellDragMaxRows: 50,
      ignoreMarginInRow: false,
      draggable: {
        enabled: true,
        stop: this.handleLayoutChange.bind(this)
      },
      resizable: {
        enabled: true,
        stop: this.handleLayoutChange.bind(this)
      },
      swap: false,
      pushItems: true,
      disablePushOnDrag: false,
      disablePushOnResize: false,
      pushDirections: {
        north: true,
        east: true,
        south: true,
        west: true,
      },
      pushResizeItems: false,
      displayGrid: DisplayGrid.OnDragAndResize,
      disableWindowResize: false,
      disableWarnings: false,
      scrollToNewItems: false,
      setGridSize: true
    };

    this.apollo
      .query({
        query: gql`
          query DashboardItems {
            dashboardItems {
              id
              name
              vizState
            }
          }
        `,
        fetchPolicy: 'network-only',
      })
      .subscribe((result: any) => {
        this.dashboardItems.next(
          (result?.data?.dashboardItems || []).map((item, index) => {
            const vizState = JSON.parse(item.vizState);
            return {
              id: item.id,
              name: item.name,
              cubeQuery: of(vizState.query),
              chartType: of(vizState.chartType || 'line'),
              pivotConfig: of(vizState.pivotConfig || null),
              plain: {
                ...vizState,
                grid: {
                  id: Number(item.id),
                  cols: 6,
                  rows: 2,
                  y: index,
                  x: 0,
                  minItemRows: 2
                },
              },
            };
          })
        );
      });
  }
  
  handleLayoutChange(event) {
    // todo
    console.log(event)
  }

  deleteItem(id: number) {
    this.apollo
      .mutate({
        mutation: gql`
          mutation DeleteItem($id: String!) {
            deleteDashboardItem(id: $id) {
              id
            }
          }
        `,
        variables: {
          id,
        },
      })
      .subscribe((result: any) => {
        this.dashboardItems.next(
          this.dashboardItems.value.filter(
            ({ id }) => id != result?.data?.deleteDashboardItem.id
          )
        );
      });
  }

  editItem(id: number, { query, pivotConfig, chartType }: any) {
    this.router.navigate(['/explore'], {
      queryParams: {
        id,
        query: JSON.stringify(query),
        pivotConfig: JSON.stringify(pivotConfig),
        chartType: chartType,
      },
    });
  }
}
