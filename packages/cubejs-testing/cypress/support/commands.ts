///<reference path="../global.d.ts" />

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

Cypress.Commands.add('getByTestId', (selector, ...args) => {
  return cy.get(`[data-testid=${selector}]`, ...args);
});

Cypress.Commands.add('setQuery', (query, ...args) => {
  cy.visit(`/#/build?query=${JSON.stringify(query)}`, ...args);
});

Cypress.Commands.add('setChartType', (chartType) => {
  cy.getByTestId('chart-type-btn').click();
  cy.getByTestId('chart-type-dropdown').contains(chartType, { matchCase: false }).click();
});

Cypress.Commands.add('runQuery', () => {
  // it's currently not possible to wait for iframe requests to load
  // cy.intercept('get', '/cubejs-api/v1/load').as('load');
  // cy.wait(['@load']);

  cy.getByTestId('run-query-btn', { timeout: 2000 }).should('be.visible').click();
  cy.getByTestId('cube-loader', { timeout: 5 * 1000 }).should('not.exist');
  cy.wait(100);
});

Cypress.Commands.add('addMeasure', (name) => {
  cy.getByTestId('Measure').click();
  cy.get('body').contains(name).click();
});

Cypress.Commands.add('addDimension', (name) => {
  cy.getByTestId('Dimension').click();
  cy.get('body').contains(name).click();
});
