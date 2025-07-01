import { countWithTimedimenionQuery, tableQuery } from "../queries";
import crypto from "crypto";

context("Playground: Chart Renderers", () => {
  before(() => {
    cy.viewport(3840, 2160);
  });

  describe.skip("Chart Renderers", () => {
    // const chartTypeByQuery = [
    //   [countWithTimedimenionQuery, ['line', 'area', 'bar']],
    //   [tableQuery, ['pie', 'table', 'number']],
    // ];
    const chartTypeByQuery = [];

    it("opens the explore page", () => {
      cy.setQuery(countWithTimedimenionQuery);
    });

    chartTypeByQuery.forEach(([query, chartTypes]) => {
      const queryHash = crypto
        .createHash("md5")
        .update(JSON.stringify(query))
        .digest("hex")
        .slice(0, 5);

      it(`opens the explore page: query hash ${queryHash}`, () => {
        cy.log(`QUERY: ${JSON.stringify(query)}`);
        cy.setQuery(query);
      });

      chartTypes.forEach((chartType) => {
        it(`chart type: ${chartType}`, () => {
          const snapshotName = `${chartType}-${queryHash}`.toLowerCase();

          function runQueryIfButtonExists() {
            cy.get("body").then((body) => {
              if (body.find("button[data-qa=RunQueryButton]").length > 0) {
                cy.runQuery();
              }
            });
          }

          cy.setChartType(chartType);
          cy.wait(100);

          // Some chart types change the query, so we need to run it again
          runQueryIfButtonExists();
          cy.get("body").click();

          // Workaround:
          // Taking a screenshot before the chart renderer screenshot
          // to wait for any unfinished animation
          cy.screenshot(`tmp/${snapshotName}`, {
            log: false,
          });

          cy.getByTestId("chart-renderer").matchImageSnapshot(snapshotName, {
            failureThreshold: 0.1,
            failureThresholdType: "percent",
          });
        });
      });
    });
  });
});
