import { Component, OnInit } from '@angular/core';
import { CubejsClient, BuilderMeta, QueryBuilderService, Query } from '@cubejs-client/ngx';
import { ResultSet } from '@cubejs-client/core';

// import { BuilderMeta, QueryBuilderService } from '../../query-builder-service/query-builder.service';
// import { Query } from '../../query-builder-service/query';

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
  ];

  constructor(public cubejsClient: CubejsClient, public queryBuilder: QueryBuilderService) {
    queryBuilder.setCubejsClient(cubejsClient);
  }

  ngOnInit() {
    this.queryBuilder.builderMeta.subscribe((builderMeta) => {
      this.builderMeta = builderMeta;
    });
    
    this.queryBuilder.query.subscribe((query) => {
      // Setting the initial query.
      // query.setQuery({
      //   measures: ['Sales.count'],
      //   timeDimensions: [
      //     {
      //       dimension: 'Sales.ts',
      //       granularity: 'day',
      //     },
      //   ],
      // });

      this.query = query;
      this.query.query.subscribe((cubeQuery) => this.onQueryChange(cubeQuery));
    });
  }

  onQueryChange(query) {
    // todo: isQueryPresent
    if (Object.keys(this.query.asCubeQuery()).length) {
      this.cubejsClient.load(query).subscribe((resultSet: any) => (this.resultSet = resultSet));
    }
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
}
