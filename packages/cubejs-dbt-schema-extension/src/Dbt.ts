import { AbstractExtension, UserError } from '@cubejs-backend/schema-compiler';
import { camelize } from 'inflection';
import fs from 'fs-extra';
import path from 'path';

type MetricDefinition = {
  // eslint-disable-next-line camelcase
  unique_id: string;
  name: string;
  model: string;
  // eslint-disable-next-line camelcase
  package_name: string;
  type: string;
  sql: string;
  dimensions?: string[];
  timestamp?: string;
};

type NodeDefinition = {
  // eslint-disable-next-line camelcase
  relation_name: string;
};

type DbtManifest = {
  metrics: { [metricName: string]: MetricDefinition };
  nodes: { [nodeName: string]: NodeDefinition };
};

type ModelCubeDef = {
  cubeName: string;
  metrics: MetricDefinition[],
  dimensions: string[]
  timeDimensions: string[]
};

type DbtLoadOptions = {
  cubePerMetric?: boolean;
  toExtend?: string[];
};

export class Dbt extends AbstractExtension {
  public async loadMetricCubesFromDbtProject(projectPath: string, options: DbtLoadOptions): Promise<{ [cubeName: string]: any }> {
    const dbtProjectPath = path.join(projectPath, 'dbt_project.yml');
    if (!(await fs.pathExists(dbtProjectPath))) {
      throw new UserError(`'${dbtProjectPath}' was not found. Please make sure '${projectPath}' is a path to the dbt project`);
    }
    // TODO read target path from dbt_project.yml
    const manifestPath = path.join(projectPath, 'target', 'manifest.json');
    if (!(await fs.pathExists(manifestPath))) {
      throw new UserError(`'${manifestPath}' was not found. Please run 'dbt compile' in '${projectPath}'`);
    }
    const manifest = <DbtManifest>(await fs.readJSON(manifestPath));
    const cubeDefs: { [model: string]: ModelCubeDef } = {};
    Object.keys(manifest.metrics || {}).forEach(
      metric => {
        const metricDef = manifest.metrics[metric];
        const modelName = (options || {}).cubePerMetric ? metricDef.unique_id : `${metricDef.package_name}.${metricDef.model}`;
        if (!cubeDefs[modelName]) {
          cubeDefs[modelName] = {
            cubeName: camelize(modelName.replace(/\./gi, '_')),
            dimensions: [],
            metrics: [],
            timeDimensions: []
          };
        }
        cubeDefs[modelName].metrics.push(metricDef);
        (metricDef.dimensions || []).forEach(dimension => {
          if (cubeDefs[modelName].dimensions.indexOf(dimension) === -1) {
            cubeDefs[modelName].dimensions.push(dimension);
          }
        });

        if (metricDef.timestamp) {
          if (cubeDefs[modelName].timeDimensions.indexOf(metricDef.timestamp) === -1) {
            cubeDefs[modelName].timeDimensions.push(metricDef.timestamp);
          }
        }
      }
    );

    const toExtend: { [cubeName: string]: any } = {};

    Object.keys(cubeDefs).forEach(model => {
      const cubeDef = {
        sql: () => `SELECT * FROM ${manifest.nodes[`model.${model}`].relation_name}`,
        fileName: manifestPath,

        measures: cubeDefs[model].metrics.map(metric => ({
          [camelize(metric.name, true)]: {
            sql: () => metric.sql,
            type: metric.type
          }
        })).reduce((a, b) => ({ ...a, ...b }), {}),

        dimensions: {
          ...(cubeDefs[model].dimensions.map(dimension => ({
            [camelize(dimension, true)]: {
              sql: () => dimension,
              type: 'string'
            }
          })).reduce((a, b) => ({ ...a, ...b }), {})),

          ...(cubeDefs[model].timeDimensions.map(dimension => ({
            [camelize(dimension, true)]: {
              sql: () => dimension,
              type: 'time'
            }
          })).reduce((a, b) => ({ ...a, ...b }), {}))
        }
      };
      if (options.toExtend && options.toExtend.indexOf(cubeDefs[model].cubeName) !== -1) {
        toExtend[cubeDefs[model].cubeName] = this.cubeFactory(cubeDef);
      } else {
        this.addCubeDefinition(cubeDefs[model].cubeName, cubeDef);
      }
    });

    return toExtend;
  }
}
