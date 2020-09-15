import { Component, OnInit } from '@angular/core';
import { CubejsClient, BuilderMeta, QueryBuilderService, Query } from '@cubejs-client/ngx';
import { ResultSet, isQueryPresent } from '@cubejs-client/core';

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
    this.queryBuilder.deserialize({
      query: {
        measures: ['Sales.count'],
        dimensions: ['Users.country', 'Users.gender'],
      },
      pivotConfig: {
        x: ['Users.country'],
        y: ['measures'],
      },
      chartType: 'line',
    });

    this.queryBuilder.builderMeta.subscribe((builderMeta) => {
      this.builderMeta = builderMeta;
    });

    this.queryBuilder.query.then((query) => {
      query.subject.subscribe((cubeQuery) => {
        this.onQueryChange(cubeQuery);
      });
      // query.order.orderMembers.subscribe((orderMembers) => console.log({ orderMembers }));

      this.query = query;
    });

    // this.queryBuilder.state.subscribe((vizState) => console.log('vizState', JSON.stringify(vizState)));
  }

  onQueryChange(query) {
    if (isQueryPresent(query)) {
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

  debug() {
    // this.query.setPartialQuery({
    // order: [['Users.country', 'desc'], ['Users.gender', 'asc']]
    // order: {'Users.country': 'desc','Users.gender': 'asc'}
    // })
    this.query.order.setMemberOrder('Sales.amount', 'desc');
  }
}
