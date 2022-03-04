/// <reference types="cypress" />
import 'cypress-wait-until';

import { ordersCountQuery, tableQuery } from '../queries';

context('Playground: Explore Page', () => {
  beforeEach(() => {
    cy.restoreLocalStorage();
  });

  afterEach(() => {
    cy.saveLocalStorage();
  });

  // @todo Fix...
  // it('copies the query', () => {
  //   cy.setQuery(ordersCountQuery);
  //   cy.runQuery();
  //   cy.getByTestId('json-query-btn').click();
  //   cy.getByTestId('copy-cube-query-btn').click();
  //
  //   cy.window().then(async (win) => {
  //     const text = await win.navigator.clipboard.readText();
  //     assert.equal(JSON.stringify(JSON.parse(text)), JSON.stringify(ordersCountQuery));
  //   });
  // });

  describe('Overview', () => {
    it('has the Add To Dashboard button', () => {
      cy.setQuery(ordersCountQuery);
      cy.runQuery();
      cy.getByTestId('add-to-dashboard-btn').should('exist');
    })
  });

  describe('Tabs', () => {
    it('opens the code tab', () => {
      cy.setQuery(ordersCountQuery);
      cy.runQuery();
      cy.getByTestId('code-btn').click();
    });

    it('opens the sql tab', () => {
      cy.setQuery(ordersCountQuery);
      cy.runQuery();
      cy.getByTestId('sql-btn').click();
      cy.getByTestId('prism-code').should('contain.text', 'SELECT');
    });
  });

  it('applies default heuristics', () => {
    cy.intercept('/playground/context').as('context');
    cy.intercept('/playground/files').as('files');

    cy.visit('/');
    cy.wait(['@context', '@files']);

    cy.wait(500);
    cy.url().should('include', '/build');

    cy.addMeasure('Events.count');
    cy.wait(300);
    cy.getByTestId('TimeDimension').contains('Events Created at');
  });

  describe('Live preview', () => {
    it('respects livePreview option', () => {
      cy.intercept('get', '/playground/context', (req) => {
        delete req.headers['if-none-match'];

        req.reply((res) => {
          res.body = {
            ...res.body,
            livePreview: true,
          };
        });
      }).as('context');

      cy.setQuery(ordersCountQuery);
      cy.wait(['@context']);
      cy.getByTestId('live-preview-btn').should('exist');
    });

    it('does now show the Live Preview button when livePreview is disabled', () => {
      cy.intercept('get', '/playground/context', (req) => {
        delete req.headers['if-none-match'];

        req.reply((res) => {
          res.body = {
            ...res.body,
            livePreview: undefined,
          };
        });
      }).as('context');

      cy.setQuery(ordersCountQuery);
      cy.wait(['@context']);
      cy.getByTestId('live-preview-btn').should('not.exist');
    });
  });

  describe('Security Context', () => {
    it('has no a cubejs token initially', () => {
      cy.intercept('get', '/playground/context', (req) => {
        delete req.headers['if-none-match'];

        req.reply((res) => {
          res.body = {
            ...res.body,
            identifier: ''
          };
        });
      }).as('context');

      cy.clearLocalStorage(/cubejsToken/);

      cy.visit('/');
      cy.wait('@context');

      cy.wait(500);
      cy.url().should('include', '/build');

      cy.getByTestId('security-context-btn').contains('Add').should('exist');
      cy.getLocalStorage('cubejsToken').should('be.null');
    });

    // @todo Fix...
    // it('saves a token', () => {
    //   cy.intercept('post', '/playground/token').as('token');
    //
    //   cy.visit('/');
    //   cy.getByTestId('security-context-btn').click();
    //   cy.getByTestId('security-context-modal').should('exist');
    //
    //   cy.getByTestId('security-context-textarea').should('be.empty');
    //   cy.getByTestId('security-context-textarea').type('{invalid value', { parseSpecialCharSequences: false });
    //   cy.getByTestId('save-security-context-payload-btn').should('be.disabled');
    //
    //   cy.getByTestId('security-context-textarea').clear().type('{"userId": 100}', { parseSpecialCharSequences: false });
    //   cy.getByTestId('save-security-context-payload-btn').should('not.be.disabled').click();
    //   cy.wait(['@token']);
    //   cy.getLocalStorage('cubejsToken').should('not.be.null');
    //
    //   cy.getByTestId('security-context-btn').click();
    //   cy.getByTestId('security-context-modal').find('.ant-tabs-tab').eq(1).click();
    //   cy.getByTestId('security-context-token-input').should(($input) => {
    //     expect(jwtDecode($input.val())).to.include({ userId: 100 });
    //   })
    // });
  });

  describe('Order', () => {
    it('applies order', () => {
      cy.setQuery(tableQuery);
      cy.setChartType('table');
      cy.runQuery();

      // todo: fix and uncomment
      // cy.getByTestId('chart-renderer').matchImageSnapshot('default-order', {
      //   failureThreshold: 0.1,
      //   failureThresholdType: 'percent',
      // });

      cy.getByTestId('order-btn').click();
      cy.getByTestId('order-popover').contains('Events Count').closest('div[data-testid=order-item]').click();

      // todo: fix and uncomment
      // cy.runQuery();
      // cy.getByTestId('chart-renderer').matchImageSnapshot('applied-order', {
      //   failureThreshold: 0.1,
      //   failureThresholdType: 'percent',
      // });
    });
  });
});
