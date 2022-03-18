import { countWithTimedimenionQuery, tableQuery } from '../queries';
import crypto from 'crypto';

context('Playground: Chart Renderers', () => {
  before(() => {
    cy.viewport(3840, 2160);
  });

  describe('Chart Renderers', () => {
    // const chartTypeByQuery = [
    //   [countWithTimedimenionQuery, ['line', 'area', 'bar']],
    //   [tableQuery, ['pie', 'table', 'number']],
    // ];
    const chartTypeByQuery = [];

    const uiFrameworks = [
      {
        name: 'React',
        chartingLibraries: ['Bizcharts', 'Recharts', 'D3', 'Chart.js'],
      },
      {
        name: 'Angular',
        chartingLibraries: ['ng2'],
      },
      {
        name: 'Vue',
        chartingLibraries: ['Chartkick'],
      },
    ];

    it('opens the explore page', () => {
      cy.setQuery(countWithTimedimenionQuery);
    });

    chartTypeByQuery.forEach(([query, chartTypes]) => {
      const queryHash = crypto.createHash('md5').update(JSON.stringify(query)).digest('hex').substr(0, 5);

      it(`opens the explore page: query hash ${queryHash}`, () => {
        cy.log(`QUERY: ${JSON.stringify(query)}`);
        cy.setQuery(query);
      });

      uiFrameworks.forEach((framework) => {
        framework.chartingLibraries.forEach((name) => {
          chartTypes.forEach((chartType) => {
            it(`framework: ${framework.name}, charting library: ${name}, chart type: ${chartType}`, () => {
              const snapshotName = `${framework.name}-${name}-${chartType}-${queryHash}`.toLowerCase();

              function runQueryIfButtonExists() {
                cy.get('body').then((body) => {
                  if (body.find('button[data-testid=run-query-btn]').length > 0) {
                    cy.runQuery();
                  }
                });
              }

              cy.getByTestId('framework-btn').click();
              cy.getByTestId('framework-dropdown').contains(framework.name).click();
              cy.getByTestId('cube-loader', { timeout: 5 * 1000 }).should('not.exist');

              cy.getByTestId('charting-library-btn').click();
              cy.getByTestId('charting-library-dropdown').contains(name).click();

              cy.setChartType(chartType);
              cy.wait(100);

              // Some chart types change the query, so we need to run it again
              runQueryIfButtonExists();
              cy.get('body').click();

              // Workaround:
              // Taking a screenshot before the chart renderer screenshot
              // to wait for any unfinished animation
              cy.screenshot(`tmp/${snapshotName}`, {
                log: false,
              });

              cy.getByTestId('chart-renderer').matchImageSnapshot(snapshotName, {
                failureThreshold: 0.1,
                failureThresholdType: 'percent',
              });
            });
          });
        });
      });
    });
  });
});
