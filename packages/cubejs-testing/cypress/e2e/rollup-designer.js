/* eslint-disable */
import { ordersCountQuery } from "../queries";

context("Playground: Rollup Designer", () => {
  describe("Opens the Rollup Designer modal", () => {
    it("opens the Rollup Designer without running a query", () => {
      cy.setQuery(ordersCountQuery);

      cy.getByQa("QueryBuilder", { timeout: 30 * 1000 }).should("exist");
      cy.getByTestId("rd-btn").click();
      cy.wait(1000);
      cy.getByTestId("rd-modal").should("be.visible");
      cy.wait(1000);
      cy.getByTestId("rd-query-tab").should("exist");

      cy.getByTestId("member-tag-Orders.Count").should("exist");
      cy.getByTestId("rd-query-tab").click({ timeout: 120 * 1000 });
      cy.getByTestId("rd-incompatible-query").should("not.exist");

      cy.getByTestId("member-tag-Orders.Count").find(".anticon-close").click();
      cy.getByTestId("member-tag-Orders.Count").should("not.exist");
      cy.getByTestId("rd-incompatible-query").should("exist");

      cy.getByTestId("rd-match-rollup-btn").click();
      cy.getByTestId("member-tag-Orders.Count").should("exist");
      cy.getByTestId("rd-incompatible-query").should("not.exist");
    });

    it("opens the Rollup Designer with an empty query", () => {
      cy.setQuery({});

      cy.getByQa("QueryBuilder", { timeout: 30 * 1000 }).should("exist");
      cy.getByTestId("rd-btn").click();
      cy.wait(1000);
      cy.getByTestId("rd-modal").should("be.visible");
      cy.wait(1000);
      cy.getByTestId("rd-query-tab").should("not.exist");
    });

    it("opens the Rollup Designer after running a query", () => {
      cy.setQuery(ordersCountQuery);
      cy.getByQa("QueryBuilder", { timeout: 30 * 1000 }).should("exist");
      cy.runQuery();

      cy.getByTestId("not-pre-agg-query-btn").click();
      cy.getByTestId("rd-modal").should("be.visible");

      cy.getByTestId("member-tag-Orders.Count").should("exist");
      cy.getByTestId("rd-query-tab").click({
        timeout: 60 * 1000,
        force: true,
      });
    });

    it("applies settings", () => {
      cy.setQuery(ordersCountQuery);

      cy.getByQa("QueryBuilder", { timeout: 30 * 1000 }).should("exist");
      cy.getByTestId("rd-btn").click();
      cy.wait(500);
      cy.getByTestId("rd-settings-tab").click();
      cy.getByTestId("prism-code").should("contain.text", "main: ");
      cy.getByTestId("rd-input-every").clear().type("3");
      cy.getByTestId("rd-select-every-granularity")
        // This crazy chain of commands is needed to avoid crashing
        .find("input")
        .focus({ force: true })
        .wait(500)
        .click({ force: true })
        .wait(500)
        .type("Day{enter}", { force: true })
        .wait(500);
      cy.getByTestId("prism-code").should("contain.text", "every: `3 day`");
      cy.getByTestId("rd-add-btn")
        .should("be.visible")
        .should("not.be.disabled");
    });
  });
});
