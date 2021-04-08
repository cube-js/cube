/// <reference types="cypress" />
import 'cypress-wait-until';

context('QueryBuilder', () => {
  it('successfully loads', async () => {
    cy.visit('/');

    const measureBox = cy.getByTestId('Measure');
    measureBox.click();

    cy.get('body').contains(`Orders Count`).click();

    cy.getByTestId('query-execute-button').click();

    cy.get('iframe')
      .its('0.contentDocument.body')
      .contains('[data-test-id="cube-loader"]').should('not.exist');

    cy.get('iframe').matchImageSnapshot();
  });
})
