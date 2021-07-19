import { playgroundFetch } from './shared/helpers';

const playgroundFetchWithRetry = (url, options, retries) =>
  playgroundFetch(url, { ...options, retries });

class DashboardSource {
  public loadError: Error | null = null;

  public dashboardCreated: boolean = false;

  public installedTemplates: any;

  protected playgroundContext: any;

  async load(instant = false) {
    const res = await playgroundFetchWithRetry(
      `/playground/dashboard-app-create-status${
        instant ? '?instant=true' : ''
      }`,
      undefined,
      10
    );
    const result = await res.json();
    if (result.error) {
      this.loadError = result.error;
    } else {
      this.dashboardCreated = result.status === 'created';
      this.installedTemplates = result.installedTemplates;
    }
  }

  async applyTemplatePackages(toApply: string[], templateConfig: any = null) {
    if (!this.playgroundContext) {
      this.playgroundContext = await this.loadContext();
    }
    return playgroundFetch('/playground/apply-template-packages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        toApply,
        templateConfig: templateConfig || {
          credentials: this.playgroundContext,
        },
      }),
    });
  }

  async loadContext() {
    const res = await playgroundFetch('/playground/context');
    const result = await res.json();
    return {
      cubejsToken: result.cubejsToken,
      apiUrl:
        result.apiUrl || window.location.href.split('#')[0].replace(/\/$/, ''),
    };
  }

  templatePackages(framework = 'react') {
    // TODO load
    if (framework.toLowerCase() === 'react') {
      return [
        { name: 'react-antd-dynamic', description: 'React Antd Dynamic' },
        { name: 'react-antd-static', description: 'React Antd Static' },
        {
          name: 'react-material-static',
          description: 'React Material UI Static',
        },
      ];
    } else if (framework.toLowerCase() === 'angular') {
      return [
        {
          name: 'ng-material-dynamic',
          description: 'Angular Material UI Dynamic',
        },
      ];
    } else if (framework.toLowerCase() === 'vue') {
      return [
        {
          name: 'vue-vuetify-dynamic',
          description: 'Vue Vuetify UI Dynamic',
        },
      ];
    }

    return [];
  }

  async canAddChart() {
    await this.load();
    if (this.loadError) {
      return this.loadError;
    }
    return !!Object.keys(this.installedTemplates).find((template) =>
      template.match(/-static$/)
    ); // TODO
  }

  async addChart(chartCode) {
    await this.load();
    if (this.loadError) {
      return;
    }
    await this.applyTemplatePackages(
      [
        'create-react-app',
        Object.keys(this.installedTemplates).find((template) =>
          template.match(/-static$/)
        ) as string, // TODO
        'static-chart',
      ],
      { chartCode }
    );
  }

  async templates() {
    const { templates } = await (await playgroundFetch('/playground/manifest')).json();
    return templates;
  }
}

export default DashboardSource;
