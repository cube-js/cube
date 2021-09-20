/* eslint-disable */
import { ordersCountQuery } from '../queries';

context('Playground: Rollup Designer', () => {
  beforeEach(() => {
    cy.restoreLocalStorage();
  });

  afterEach(() => {
    cy.saveLocalStorage();
  });

  describe('Opens the Rollup Designer modal', () => {
    it('opens the Rollup Designer without running a query', () => {
      cy.setQuery(ordersCountQuery);

      cy.getByTestId('rd-btn').click();
      cy.getByTestId('rd-modal').should('be.visible');
      cy.getByTestId('rd-query-tab').should('exist');

      cy.getByTestId('member-tag-Orders.Count').should('exist');
      cy.getByTestId('rd-query-tab').click();
      cy.getByTestId('rd-incompatible-query').should('not.exist');

      cy.getByTestId('member-tag-Orders.Count').find('.anticon-close').click();
      cy.getByTestId('member-tag-Orders.Count').should('not.exist');
      cy.getByTestId('rd-incompatible-query').should('exist');

      cy.getByTestId('rd-match-rollup-btn').click();
      cy.getByTestId('member-tag-Orders.Count').should('exist');
      cy.getByTestId('rd-incompatible-query').should('not.exist');
    });

    it('opens the Rollup Designer with an empty query', () => {
      cy.setQuery({});

      cy.getByTestId('rd-btn').click();
      cy.getByTestId('rd-modal').should('be.visible');

      cy.getByTestId('rd-query-tab').should('not.exist');
    });

    it('opens the Rollup Designer after running a query', () => {
      cy.setQuery(ordersCountQuery);
      cy.runQuery();

      cy.getByTestId('not-pre-agg-query-btn').click();
      cy.getByTestId('rd-modal').should('be.visible');

      cy.getByTestId('member-tag-Orders.Count').should('exist');
      cy.getByTestId('rd-query-tab').click();
    });

    it('applies settings', () => {
      cy.setQuery(ordersCountQuery);

      cy.getByTestId('rd-btn').click();
      cy.getByTestId('rd-settings-tab').click();
      cy.getByTestId('prism-code').should('contain.text', 'main: ');
      cy.getByTestId('rd-input-every').clear().type('3');
      cy.getByTestId('rd-select-every-granularity').find('input').type('Day{enter}', { force: true });
      cy.getByTestId('prism-code').should('contain.text', 'every: `3 day`');
      cy.getByTestId('rd-add-btn').should('be.visible').should('not.be.disabled');
    });
  });
});
