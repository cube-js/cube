/// <reference types="cypress" />
import "cypress-wait-until";

import { eventsCountQuery } from "../queries";

context("Playground: Connection Wizard", () => {
  let shouldStartConnectionWizardFlow = true;

  beforeEach(() => {
    cy.intercept("/playground/context", (req) => {
      delete req.headers["if-none-match"];
      req.reply((res) => {
        res.body = {
          ...res.body,
          isDocker: true,
          shouldStartConnectionWizardFlow,
        };
      });
    });
  });

  it("copies values of the localhost tip box", () => {
    cy.visit("/");
    cy.getByTestId("wizard-db-card", { timeout: 20000 })
      .contains("PostgreSQL")
      .click();
    ["mac", "windows", "linux"].forEach((os) => {
      cy.getByTestId(`localhost-tipbox-${os}-copy-btn`).click();
      cy.getByTestId(`localhost-tipbox-${os}-input`)
        .invoke("val")
        .then((val) => {
          cy.getByTestId("CUBEJS_DB_HOST").should(($input) => {
            expect($input.val()).to.eq(val);
          });
        });
    });
  });

  describe("PostgreSQL connection flow", () => {
    it("resets the error", () => {
      cy.visit("/");
      cy.getByTestId("wizard-db-card").contains("PostgreSQL").click();
      cy.getByTestId("wizard-form-submit-btn").click();
      cy.getByTestId("wizard-connection-error").should("exist");

      cy.getByTestId("wizard-change-db-btn").click();
      cy.getByTestId("wizard-db-card").contains("MySQL").click();
      cy.getByTestId("wizard-connection-error").should("not.exist");
    });

    it("opens the DB connection page", () => {
      cy.visit("/");
      cy.getByTestId("wizard-db-card").contains("PostgreSQL").click();
      cy.getByTestId("wizard-localhost-tipbox").should("exist");
    });

    it("fails to connect to the DB with wrong credentials", () => {
      cy.visit("/");
      cy.getByTestId("wizard-db-card").contains("PostgreSQL").click();
      cy.fixture("databases.json").then(({ postgresql }) => {
        Object.entries(postgresql.credentials.invalid).forEach(
          ([key, value]) => {
            cy.getByTestId(key).type(value);
          }
        );
      });
      cy.getByTestId("wizard-form-submit-btn").click();
      cy.getByTestId("wizard-connection-error").should("exist");
    });

    it("connects to the DB", () => {
      cy.visit("/");
      cy.getByTestId("wizard-db-card").contains("PostgreSQL").click();
      cy.fixture("databases.json").then(({ postgresql }) => {
        postgresql.cubejsEnvVars.forEach((key) => {
          const value = Cypress.env(key) || postgresql.credentials.valid[key];

          cy.log(
            JSON.stringify({
              key,
              value,
              cypress: Cypress.env("CUBEJS_DB_HOST"),
            })
          );
          cy.getByTestId(key).type(value);
        });
      });
      cy.getByTestId("wizard-form-submit-btn").click();
      cy.getByTestId("wizard-test-connection-spinner").should("not.exist");
      cy.getByTestId("wizard-connection-error").should("not.exist");

      cy.location().should((location) => {
        expect(location.hash).to.eq("#/schema");
      });
      cy.getByTestId("cube-loader").should("not.exist");
      cy.getByTestId("schema-error").should("not.exist");
    });

    it("executes a query after a successful connection", () => {
      cy.visit("/");
      shouldStartConnectionWizardFlow = false;
      cy.setQuery(eventsCountQuery);
      cy.wait(300);
      cy.runQuery();
      cy.wait(2000);
      cy.getByQa("ResultsTableContainer")
        .find(
          '[data-row="0"][data-name="Events.count"][data-element=NumberCell]'
        )
        .contains("171,334");
    });
  });
});
