import { AbstractExtension, UserError } from '@cubejs-backend/schema-compiler';
import { camelize } from 'inflection';
import fs from 'fs-extra';
import path from 'path';
import fetch from 'node-fetch';

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
  relation_name?: string;
  database?: string;
  schema?: string;
  name?: string;
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

type GraphqlError = {
  message: string;
};

type GraphqlModel = {
  uniqueId: string;
  name: string;
  database: string;
  schema: string;
};

type GraphqlMetrics = {
  timestamp: string;
  dimensions: string[];
  sql: string;
  type: string;
  uniqueId: string;
  name: string;
  packageName: string;
  model: { uniqueId: string };
};

type GraphqlLoadModelsData = {
  models: GraphqlModel[];
  metrics: GraphqlMetrics[];
};

type GraphqlResponse = {
  errors?: GraphqlError[];
  data: GraphqlLoadModelsData;
};

const loadModelsQuery = `
query LoadModels($jobId: Int!) {
  models(jobId: $jobId) {
    uniqueId
    name
    database
    schema
  }
  metrics(jobId: $jobId) {
    uniqueId
    name
    packageName
    tags
    label
    runId
    description
    type
    sql
    timestamp
    timeGrains
    dimensions
    meta
    resourceType
    filters {
      field
      operator
      value
    }
    model {
      uniqueId
    }
  }
}
`;

// For reference:
// - dbt Metrics types: https://docs.getdbt.com/docs/building-a-dbt-project/metrics, https://github.com/dbt-labs/dbt-core/issues/4071#issue-102758091
// - Cube measure types: https://cube.dev/docs/schema/reference/types-and-formats#measures-types
const dbtToCubeMetricTypeMap: Record<string, string> = {
  count: 'count',
  count_distinct: 'countDistinct',
  sum: 'sum',
  average: 'avg',
  min: 'min',
  max: 'max',
};

function mapMetricType(dbtMetricType: string): string {
  if (!dbtToCubeMetricTypeMap[dbtMetricType]) {
    throw new UserError(`Unsupported dbt metric type '${dbtMetricType}'`);
  }

  return dbtToCubeMetricTypeMap[dbtMetricType];
}

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
    Object.keys(manifest.metrics).forEach(metric => {
      const regex = /^ref\('(\S+)'\)$/;
      const metricDef = manifest.metrics[metric];
      const match = metricDef.model.match(regex);
      if (!match) {
        throw new UserError(`Expected reference to the model in format ref('model_name') but found '${metricDef.model}'`);
      }
      // eslint-disable-next-line prefer-destructuring
      const modelName = match[1];
      metricDef.model = modelName.indexOf('.') !== -1 ? modelName : `model.${metricDef.package_name}.${modelName}`;
    });
    return this.loadMetricCubesFromNormalizedManifest(manifest, manifestPath, options);
  }

  public async loadMetricCubesFromDbtCloud(jobId: string | number, authToken: string, options: DbtLoadOptions): Promise<{ [cubeName: string]: any }> {
    const response = await fetch('https://metadata.cloud.getdbt.com/graphql', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        Authorization: `Bearer ${authToken}`
      },
      body: JSON.stringify({ query: loadModelsQuery, variables: { jobId } })
    });
    const res = <GraphqlResponse>(await response.json());
    if (res.errors && res.errors.length > 0) {
      throw new UserError(`Can't fetch metrics from Dbt Cloud: ${res.errors[0].message}`);
    }
    const manifest: DbtManifest = {
      metrics: res.data.metrics.map(metricDef => ({
        [metricDef.uniqueId]: {
          unique_id: metricDef.uniqueId,
          name: metricDef.name,
          model: metricDef.model.uniqueId,
          // eslint-disable-next-line camelcase
          package_name: metricDef.packageName,
          type: metricDef.type,
          sql: metricDef.sql,
          dimensions: metricDef.dimensions,
          timestamp: metricDef.timestamp,
        }
      })).reduce((a, b) => ({ ...a, ...b }), {}),
      nodes: res.data.models.map(modelDef => ({
        [modelDef.uniqueId]: {
          database: modelDef.database,
          schema: modelDef.schema,
          name: modelDef.name,
        }
      })).reduce((a, b) => ({ ...a, ...b }), {}),
    };
    return this.loadMetricCubesFromNormalizedManifest(manifest, 'dbt-cloud', options);
  }

  private loadMetricCubesFromNormalizedManifest(manifest: DbtManifest, manifestPath: string, options?: DbtLoadOptions) {
    const cubeDefs: { [model: string]: ModelCubeDef } = {};
    Object.keys(manifest.metrics || {}).forEach(
      metric => {
        const metricDef = manifest.metrics[metric];
        const modelName = (options || {}).cubePerMetric ? metricDef.unique_id : metricDef.model;
        if (!cubeDefs[modelName]) {
          cubeDefs[modelName] = {
            cubeName: camelize(modelName.replace('model.', '').replace(/\./gi, '_')),
            dimensions: [],
            metrics: [],
            timeDimensions: [],
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
      },
    );

    const toExtend: { [cubeName: string]: any } = {};

    Object.keys(cubeDefs).forEach(model => {
      const modelDef = manifest.nodes[model];
      if (!modelDef) {
        throw new UserError(`Model '${model}' is not found`);
      }
      const cubeDef = {
        sql: () => `SELECT * FROM ${modelDef.relation_name ? modelDef.relation_name : `${this.compiler.contextQuery().escapeColumnName(modelDef.database)}.${this.compiler.contextQuery().escapeColumnName(modelDef.schema)}.${this.compiler.contextQuery().escapeColumnName(modelDef.name)}`}`,
        fileName: manifestPath,

        measures: cubeDefs[model].metrics.map(metric => ({
          [camelize(metric.name, true)]: {
            sql: () => metric.sql,
            type: mapMetricType(metric.type),
          },
        })).reduce((a, b) => ({ ...a, ...b }), {}),

        dimensions: {
          ...(cubeDefs[model].dimensions.map(dimension => ({
            [camelize(dimension, true)]: {
              sql: () => dimension,
              type: 'string',
            },
          })).reduce((a, b) => ({ ...a, ...b }), {})),

          ...(cubeDefs[model].timeDimensions.map(dimension => ({
            [camelize(dimension, true)]: {
              sql: () => dimension,
              type: 'time',
            },
          })).reduce((a, b) => ({ ...a, ...b }), {})),
        },
      };
      if (options?.toExtend && options?.toExtend.indexOf(cubeDefs[model].cubeName) !== -1) {
        toExtend[cubeDefs[model].cubeName] = this.cubeFactory(cubeDef);
      } else {
        this.addCubeDefinition(cubeDefs[model].cubeName, cubeDef);
      }
    });

    return toExtend;
  }
}
