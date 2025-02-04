import { defineConfig } from 'cypress'

export default defineConfig({
  chromeWebSecurity: false,
  viewportWidth: 1600,
  viewportHeight: 1400,
  projectId: 'zv1vfg',
  blockHosts: ['*.cube.dev'],
  retries: {
    runMode: 2,
    openMode: 0,
  },
  e2e: {
    // We've imported your old cypress plugins here.
    // You may want to clean this up later by importing these.
    setupNodeEvents(on, config) {
      return require('./cypress/plugins/index.js')(on, config)
    },
    baseUrl: 'http://localhost:3080',
    specPattern: 'cypress/e2e/**/*.{js,jsx,ts,tsx}',
  },
});
