/// <reference types="cypress" />

export function blockAllAnalytics() {
  const blacklist = [
    'track.cube.dev',
    's.intercomcdn.com',
    'api-iam.intercom.io',
    'ingest.sentry.io',
    'fullstory.com',
    'widget.intercom.io',
    'edge.fullstory.com',
    'o56139.ingest.sentry.io',
    'nexus-websocket-a.intercom.io',
  ];

  blacklist.forEach((host) => {
    // on cypress 6.0 route2 will be renamed to intercept
    cy.intercept(host, (req) => {
      req.reply('');
    });
  });
}
