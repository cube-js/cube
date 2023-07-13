import { CommanderStatic } from 'commander';
import { isDockerImage, requireFromPackage, packageExists, getEnv } from '@cubejs-backend/shared';
import { displayError } from '../utils';
import type { ServerContainer as ServerContainerType } from '@cubejs-backend/server';

async function aggregationWarmup(options) : Promise<void> {
  const relative = isDockerImage();

  if (!packageExists('@cubejs-backend/server', relative)) {
    await displayError(
      '@cubejs-backend/server dependency not found. Please run generate command from project directory.');
  }

  const serverPackage = requireFromPackage<{ ServerContainer: any }>(
    '@cubejs-backend/server',
    {
      relative,
    }
  );

  if (!serverPackage.ServerContainer) {
    await displayError( '@cubejs-backend/server is too old. Please use @cubejs-backend/server >= v0.26.11')
  }

  const container: ServerContainerType = new serverPackage.ServerContainer({ debug: false });
  const configuration = await container.lookupConfiguration();
  const server = await container.runServerInstance(
    configuration,
    true,
    Object.keys(configuration).length === 0
  );

  let queryIteratorState = {}
  let exit = 1;

  for (; ;) {
    try {
      const { finished } = await server.runScheduledRefresh({}, {
        concurrency: configuration.scheduledRefreshConcurrency, queryIteratorState, preAggregationsWarmup: true, throwErrors: true,
      });

      if (finished) {
        exit = 0;
        break;
      }
    } catch (e: any) {
      if (e.error != "Continue wait") {
        displayError(`Something went wrong refreshing aggregations ${JSON.stringify(e)}`)
        break;
      }
    }
  }

  await server.shutdown("", true)
  console.log("Server shutdown done")
  process.exit(exit)
}

export function configureAggregationCommand(program: CommanderStatic) {
    program.command("aggregation-warmup").action((options) => aggregationWarmup(options)
        .catch(e => displayError(e.stack || e)))
}
