/// <reference path="../global.d.ts" />

// ***********************************************
// This example commands.js shows you how to
// create various custom commands and overwrite
// existing commands.
//
// For more comprehensive examples of custom
// commands please read more here:
// https://on.cypress.io/custom-commands
// ***********************************************
//
//
// -- This is a parent command --
// Cypress.Commands.add("login", (email, password) => { ... })
//
//
// -- This is a child command --
// Cypress.Commands.add("drag", { prevSubject: 'element'}, (subject, options) => { ... })
//
//
// -- This is a dual command --
// Cypress.Commands.add("dismiss", { prevSubject: 'optional'}, (subject, options) => { ... })
//
//
// -- This will overwrite an existing command --
// Cypress.Commands.overwrite("visit", (originalFn, url, options) => { ... })

Cypress.on(
  "uncaught:exception",
  (err) => !err.message.includes("ResizeObserver")
);

Cypress.Commands.add("getByTestId", (selector, ...args) =>
  cy.get(`[data-testid='${selector}']`, ...args)
);
Cypress.Commands.add("getByQa", (selector, ...args) =>
  cy.get(`[data-qa='${selector}']`, ...args)
);

Cypress.Commands.add("setQuery", (query, ...args) => {
  cy.clearLocalStorage(/queryTabs/);
  cy.visit(`/#/build?query=${JSON.stringify(query)}`, ...args);
  cy.wait(100);
});

Cypress.Commands.add("setChartType", (chartType) => {
  cy.get("[data-qa=QueryBuilderChart] > div[aria-expanded]").then(($title) => {
    const expanded = $title.attr("aria-expanded");

    if (expanded && expanded.includes("false")) {
      return cy.wrap($title).click();
    }

    return cy.wrap($title);
  });
  cy.wait(500);
  cy.getByQa("ChartType")
    .getByQa("RadioWrapper")
    .contains(chartType, { matchCase: false })
    .click();
});

Cypress.Commands.add("runQuery", () => {
  // it's currently not possible to wait for iframe requests to load
  // cy.intercept('get', '/cubejs-api/v1/load').as('load');
  // cy.wait(['@load']);

  cy.getByQa("RunQueryButton", { timeout: 10 * 1000 })
    .should("be.visible")
    .click();
  cy.getByQa("RunQueryButton", { timeout: 10 * 1000 }).should(
    "not.have.attr",
    "disabled"
  );
  cy.wait(100);
});

Cypress.Commands.add("addMeasure", (name) => {
  cy.get("body").then(($body) => {
    const $button = $body.find("[data-qa=ToggleMembersButton]");

    if ($button.length) {
      const expanded = $button.attr("data-qaval");

      if (expanded && expanded.includes("used")) {
        cy.wrap($button).click();
      }
    }

    const $cube = $body.find(
      `[data-qa=CubeButton][data-qaval=${
        name.split(".")[0]
      }]:not([data-is-open])`
    );

    if ($cube.length > 0) {
      cy.wrap($cube).click();
    }

    return cy.wrap($body);
  });
  cy.get(`[data-member="measure"][data-qaval="${name}"]`).click();
});

Cypress.Commands.add("addDimension", (name) => {
  cy.get("body").then(($body) => {
    const $button = $body.find("[data-qa=ToggleMembersButton]");

    if ($button.length) {
      const expanded = $button.attr("data-qaval");

      if (expanded && expanded.includes("used")) {
        cy.wrap($button).click();
      }
    }

    const $cube = $body.find(
      `[data-qa=CubeButton][data-qaval=${
        name.split(".")[0]
      }]:not([data-is-open])`
    );

    if ($cube.length > 0) {
      cy.wrap($cube).click();
    }

    return cy.wrap($body);
  });
  cy.get(`[data-member="dimension"][data-qaval="${name}"]`).click();
});
