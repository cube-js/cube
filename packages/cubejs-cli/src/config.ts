import inquirer from 'inquirer';
import { Config } from '@cubejs-backend/cloud';

export class ConfigCli extends Config {
  public async deployAuth(url?: string) {
    const config = await this.loadConfig();

    if (process.env.CUBE_CLOUD_DEPLOY_AUTH) {
      return (await this.addAuthToken(process.env.CUBE_CLOUD_DEPLOY_AUTH, config)).auth;
    }

    if (config.auth) {
      return config.auth;
    }

    const auth = await inquirer.prompt([{
      name: 'auth',
      message: `Cube Cloud Auth Token${url ? ` for ${url}` : ''}`
    }]);

    return (await this.addAuthToken(auth.auth, config)).auth;
  }

  public async deployAuthForCurrentDir() {
    const dotCubeCloud = await this.loadDotCubeCloud();
    if (dotCubeCloud.url && dotCubeCloud.deploymentId) {
      const deployAuth = await this.deployAuth(dotCubeCloud.url);
      if (!deployAuth[dotCubeCloud.url]) {
        throw new Error(`Provided token isn't for ${dotCubeCloud.url}`);
      }

      return {
        ...deployAuth[dotCubeCloud.url],
        url: dotCubeCloud.url,
        deploymentId: dotCubeCloud.deploymentId
      };
    }

    const auth = await this.deployAuth();
    let url = Object.keys(auth)[0];
    if (Object.keys(auth).length > 1) {
      // eslint-disable-next-line prefer-destructuring
      url = (await inquirer.prompt([{
        type: 'list',
        name: 'url',
        message: 'Please select an organization',
        choices: Object.keys(auth)
      }])).url;
    }

    const authToken = auth[url];
    const deployments = await this.cubeCloudClient.getDeploymentsList({ auth: { ...authToken, url } });

    if (!Array.isArray(deployments)) {
      throw new Error(JSON.stringify(deployments));
    }

    if (!deployments.length) {
      // eslint-disable-next-line no-throw-literal
      throw `${url} doesn't have any managed deployments. Please create one.`;
    }

    let deploymentId = deployments[0].id;
    if (deployments.length > 1) {
      const { deployment } = await inquirer.prompt([{
        type: 'list',
        name: 'deployment',
        message: 'Please select a deployment to deploy to',
        choices: deployments
      }]);
      deploymentId = deployments.find(d => d.name === deployment).id;
    }

    await this.writeDotCubeCloud({
      url,
      deploymentId
    });

    return {
      ...authToken,
      url,
      deploymentId
    };
  }
}
