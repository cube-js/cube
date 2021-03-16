/// <reference types="cypress" />
import 'cypress-wait-until';

context('QueryBuilder', () => {
  it('successfully loads', async () => {
    cy.visit('/')

    await cy.waitUntil(
      () => cy.contains('Choose a measure or dimension to get started'),
      {
        errorMsg: `React didnt render anything.`,
        timeout: 10 * 1000,
        verbose: true,
      }
    )
  })
})
