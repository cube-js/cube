// ***********************************************************
// This example support/index.js is processed and
// loaded automatically before your test files.
//
// This is a great place to put global configuration and
// behavior that modifies Cypress.
//
// You can change the location of this file or turn off
// automatically serving support files with the
// 'supportFile' configuration option.
//
// You can read more here:
// https://on.cypress.io/configuration
// ***********************************************************

// Import commands.js using ES2015 syntax:
// import 'cypress-plugin-snapshots/commands';

import 'cypress-localstorage-commands';
import '@4tw/cypress-drag-drop';
import { addMatchImageSnapshotCommand } from 'cypress-image-snapshot/command';

import './commands';

addMatchImageSnapshotCommand({
  capture: 'viewport',
});

after(() => {
  cy.exec(`rm -rf cypress/screenshots/playground-explore.spec.js/tmp`);
});

const resizeObserverLoopErrRe = /ResizeObserver loop limit exceeded/;

Cypress.on('uncaught:exception', (err) => {
  if (resizeObserverLoopErrRe.test(err.message)) {
    // returning false here prevents Cypress from
    // failing the test
    return false;
  }
});
