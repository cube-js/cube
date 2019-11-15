/* globals window */
import fetch from './playgroundFetch';

const fetchWithRetry = (url, options, retries) => fetch(url, { ...options, retries });

class DashboardSource {
  async load(createApp, templatePackages) {
    this.loadError = null;
    if (createApp) {
      await this.applyTemplatePackages(
        templatePackages
      );
    }
    const res = await fetchWithRetry('/playground/dashboard-app-create-status', undefined, 10);
    const result = await res.json();
    if (result.error) {
      this.loadError = result.error;
    } else {
      this.dashboardCreated = result.status === 'created';
      this.installedTemplates = result.installedTemplates;
    }
  }

  async applyTemplatePackages(templatePackages, templateConfig) {
    if (!this.playgroundContext) {
      this.playgroundContext = await this.loadContext();
    }
    return fetchWithRetry('/playground/apply-template-packages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        templatePackages,
        templateConfig: templateConfig || {
          credentials: this.playgroundContext
        }
      })
    });
  }

  async loadContext() {
    const res = await fetch('/playground/context');
    const result = await res.json();
    return {
      cubejsToken: result.cubejsToken,
      apiUrl: result.apiUrl || window.location.href.split('#')[0].replace(/\/$/, '')
    };
  }

  get templatePackages() { // TODO load
    return [
      { name: 'react-antd-dynamic', description: 'React Antd Dynamic' },
      { name: 'react-antd-static', description: 'React Antd Static' },
      { name: 'react-material-static', description: 'React Material UI Static' }
    ];
  }

  async canAddChart() {
    await this.load();
    if (this.loadError) {
      return this.loadError;
    }
    return !!Object.keys(this.installedTemplates).find(template => template.match(/-static$/)); // TODO
  }

  async addChart(chartCode) {
    await this.load();
    if (this.loadError) {
      return;
    }
    await this.applyTemplatePackages([
      'create-react-app',
      Object.keys(this.installedTemplates).find(template => template.match(/-static$/)), // TODO
      'static-chart'
    ], {
      'static-chart': {
        chartCode
      }
    });
  }
}

export default DashboardSource;
