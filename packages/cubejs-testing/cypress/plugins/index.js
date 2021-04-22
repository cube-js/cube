/// <reference types="cypress" />
// ***********************************************************
// This example plugins/index.js can be used to load plugins
//
// You can change the location of this file or turn off loading
// the plugins file with the 'pluginsFile' configuration option.
//
// You can read more here:
// https://on.cypress.io/plugins-guide
// ***********************************************************

// This function is called when a project is opened or re-opened (e.g. due to
// the project's config changing)

// const { initPlugin } = require('cypress-plugin-snapshots/plugin');
const { addMatchImageSnapshotPlugin } = require('cypress-image-snapshot/plugin');

/**
 * @type {Cypress.PluginConfig}
 */
module.exports = (on, config) => {
  addMatchImageSnapshotPlugin(on, config);

  return config;
};
