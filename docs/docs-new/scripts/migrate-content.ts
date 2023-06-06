import path, { dirname } from "path";
import {
  readFile,
  mkdir,
  writeFile,
  unlink,
  lstat,
  readdir,
  rmdir,
  rename,
  stat,
} from "fs/promises";
import glob from "glob";
import frontmatter from "front-matter";

async function deleteFileOrFolder(p: string) {
  try {
    const stats = await lstat(p);
    if (stats.isDirectory()) {
      const files = await readdir(p);
      for (const file of files) {
        const curPath = path.join(p, file);
        await deleteFileOrFolder(curPath);
      }
      await rmdir(p);
    } else {
      await unlink(p);
    }
  } catch (err) {
    console.error(`Error deleting ${p}: ${err}`);
  }
}

interface DocAttributes {
  permalink?: string;
  redirect_from?: string[];
  category?: string;
  subCategory?: string;
  title: string;
  menuOrder?: number;
}

async function cleanup() {
  const contents = await glob("pages/*");

  await Promise.all(
    contents
      .filter(
        (p) =>
          ![
            "pages/_app.tsx",
            "pages/_meta.js",
            "pages/index.mdx",
            "pages/docs.mdx",
          ].includes(p)
      )
      .map((p) => deleteFileOrFolder(path.resolve(p)))
  );
}

interface Override {
  ready: boolean;
  path: string;
  title: string;

  meta?: Record<string, string>;
}

const overrides: Record<string, Override> = {
  "Cube.js-Introduction.mdx": {
    ready: true,
    path: "product/introduction",
    title: "Introduction",
    meta: {
      introduction: "Introduction",
      "getting-started": "Getting Started",
      configuration: "Configuration",
      "data-modeling": "Data Modeling",
      caching: "Caching",
      auth: "Authentication & Authorization",
      "api-rest": "REST API",
      "api-graphql": "GraphQL API",
      "api-sql": "SQL API",

      "frontend-integrations": "Frontend Integrations",
      workspace: "Workspace",
      deployment: "Deployment",
      monitoring: "Monitoring",
      faqs: "FAQs",
    },
  },

  // Getting Started

  "Getting-Started/Overview.mdx": {
    ready: true,
    path: "product/getting-started/overview",
    title: "Getting started with Cube",
    meta: {
      overview: "Overview",
      core: "Cube Core",
      cloud: "Cube Cloud",
      "migrate-from-core": "Migrate from Cube Core",
    },
  },

  // Core

  "Getting-Started/Core/01-Overview.mdx": {
    ready: true,
    path: "product/getting-started/core/overview",
    title: "Getting started with Cube Core",
    meta: {
      overview: "Overview",
      "create-a-project": "Create a project",
      "query-data": "Query data",
      "add-a-pre-aggregation": "Add a pre-aggregation",
      "learn-more": "Learn more",
    },
  },
  "Getting-Started/Core/02-Create-a-project.mdx": {
    ready: true,
    path: "product/getting-started/core/create-a-project",
    title: "Create a project",
  },
  "Getting-Started/Core/03-Query-data.mdx": {
    ready: true,
    path: "product/getting-started/core/query-data",
    title: "Query data",
  },
  "Getting-Started/Core/04-Add-a-pre-aggregation.mdx": {
    ready: true,
    path: "product/getting-started/core/add-a-pre-aggregation",
    title: "Add a pre-aggregation",
  },
  "Getting-Started/Core/05-Learn-more.mdx": {
    ready: true,
    path: "product/getting-started/core/learn-more",
    title: "Learn more",
  },

  // Cloud

  "Getting-Started/Cloud/01-Overview.mdx": {
    ready: true,
    path: "product/getting-started/cloud/overview",
    title: "Getting started with Cube Cloud",
    meta: {
      overview: "Overview",
      "create-a-deployment": "Create a deployment",
      "generate-models": "Generate models",
      "query-data": "Query data",
      "add-a-pre-aggregation": "Add a pre-aggregation",
      "learn-more": "Learn more",
    },
  },
  "Getting-Started/Cloud/02-Create-a-deployment.mdx": {
    ready: true,
    path: "product/getting-started/cloud/create-a-deployment",
    title: "Create a deployment",
  },
  "Getting-Started/Cloud/03-Generate-models.mdx": {
    ready: true,
    path: "product/getting-started/cloud/generate-models",
    title: "Generate models",
  },
  "Getting-Started/Cloud/04-Query-data.mdx": {
    ready: true,
    path: "product/getting-started/cloud/query-data",
    title: "Query data",
  },
  "Getting-Started/Cloud/05-Add-a-pre-aggregation.mdx": {
    ready: true,
    path: "product/getting-started/cloud/add-a-pre-aggregation",
    title: "Add a pre-aggregation",
  },
  "Getting-Started/Cloud/06-Learn-more.mdx": {
    ready: true,
    path: "product/getting-started/cloud/learn-more",
    title: "Learn more",
  },

  "Getting-Started/Migrate-from-Core/Upload-with-CLI.mdx": {
    ready: true,
    path: "product/getting-started/migrate-from-core/upload-with-cli",
    title: "Import a local project to Cube Cloud with CLI",
    meta: {
      "upload-with-cli": "Upload with CLI",
      "import-gitlab-repository-via-ssh": "Import a GitLab repository",
      "import-github-repository": "Import a GitHub repository",
      "import-git-repository-via-ssh": "Import a Git repository",
      "import-bitbucket-repository-via-ssh": "Import a Bitbucket repository",
    },
  },
  "Getting-Started/Migrate-from-Core/Import-GitLab-repository-via-SSH.mdx": {
    ready: true,
    path: "product/getting-started/migrate-from-core/import-gitlab-repository-via-ssh",
    title: "Import a GitLab repository",
  },
  "Getting-Started/Migrate-from-Core/Import-GitHub-repository.mdx": {
    ready: true,
    path: "product/getting-started/migrate-from-core/import-github-repository",
    title: "Import a GitHub repository",
  },
  "Getting-Started/Migrate-from-Core/Import-Git-repository-via-SSH.mdx": {
    ready: true,
    path: "product/getting-started/migrate-from-core/import-git-repository-via-ssh",
    title: "Import a Git repository",
  },
  "Getting-Started/Migrate-from-Core/Import-Bitbucket-repository-via-SSH.mdx": {
    ready: true,
    path: "product/getting-started/migrate-from-core/import-bitbucket-repository-via-ssh",
    title: "Import a Bitbucket repository",
  },

  // configuration
  "Configuration/Overview.mdx": {
    ready: true,
    path: "product/configuration/overview",
    title: "Overview",
    meta: {
      overview: "Overview",
      "data-sources": "Connecting to data sources",
      "visualization-tools": "Connecting to visualization tools",
      vpc: "Connecting with a VPC",
      advanced: "Advanced",
    },
  },

  "Configuration/Advanced/Multitenancy.mdx": {
    ready: true,
    path: "product/configuration/advanced/multitenancy",
    title: "Multitenancy",
  },
  "Configuration/Advanced/Multiple-Data-Sources.mdx": {
    ready: true,
    path: "product/configuration/advanced/multiple-data-sources",
    title: "Multiple Data Sources",
  },

  // VPC
  "Configuration/VPC/Connecting-with-a-VPC.mdx": {
    ready: true,
    path: "product/configuration/vpc",
    title: "Connecting with a VPC",
  },
  "Configuration/VPC/Connecting-with-a-VPC-GCP.mdx": {
    ready: true,
    path: "product/configuration/vpc/gcp",
    title: "Connecting with a VPC on GCP",
  },
  "Configuration/VPC/Connecting-with-a-VPC-Azure.mdx": {
    ready: true,
    path: "product/configuration/vpc/azure",
    title: "Connecting with a VPC on Azure",
  },
  "Configuration/VPC/Connecting-with-a-VPC-AWS.mdx": {
    ready: true,
    path: "product/configuration/vpc/aws",
    title: "Connecting with a VPC on AWS",
  },

  // Visualization
  "Configuration/Connecting-to-Downstream-Tools.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools",
    title: "Connecting to visualization tools",
  },
  "Configuration/Downstream/Thoughtspot.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/thoughtspot",
    title: "Thoughtspot",
  },
  "Configuration/Downstream/Tableau.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/tableau",
    title: "Tableau",
  },
  "Configuration/Downstream/Superset.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/superset",
    title: "Superset",
  },
  "Configuration/Downstream/Streamlit.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/streamlit",
    title: "Streamlit",
  },
  "Configuration/Downstream/Retool.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/retool",
    title: "Retool",
  },
  "Configuration/Downstream/PowerBI.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/powerbi",
    title: "PowerBI",
  },
  "Configuration/Downstream/Observable.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/observable",
    title: "Observable",
  },
  "Configuration/Downstream/Metabase.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/metabase",
    title: "Metabase",
  },
  "Configuration/Downstream/Jupyter.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/jupyter",
    title: "Jupyter",
  },
  "Configuration/Downstream/Hex.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/hex",
    title: "Hex",
  },
  "Configuration/Downstream/Delphi.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/delphi",
    title: "Delphi",
  },
  "Configuration/Downstream/Deepnote.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/deepnote",
    title: "Deepnote",
  },
  "Configuration/Downstream/Budibase.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/budibase",
    title: "Budibase",
  },
  "Configuration/Downstream/Bubble.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/bubble",
    title: "Bubble",
  },
  "Configuration/Downstream/Appsmith.mdx": {
    ready: true,
    path: "product/configuration/visualization-tools/appsmith",
    title: "Appsmith",
  },

  // databases
  "Configuration/Connecting-to-the-Database.mdx": {
    ready: true,
    path: "product/configuration/data-sources",
    title: "Connecting to data sources",
  },
  "Configuration/Databases/ksqlDB.mdx": {
    ready: true,
    path: "product/configuration/data-sources/ksqldb",
    title: "ksqlDB",
    meta: {
      "aws-athena": "AWS Athena",
      "aws-redshift": "AWS Redshift",
      clickhouse: "ClickHouse",
      "databricks-jdbc": "Databricks (JDBC)",
      druid: "Druid",
      elasticsearch: "Elasticsearch",
      firebolt: "Firebolt",
      "google-bigquery": "Google BigQuery",
      hive: "Hive",
      ksqldb: "ksqlDB",
      materialize: "Materialize",
      mongodb: "MongoDB",
      "ms-sql": "MS-SQL",
      mysql: "MySQL",
      oracle: "Oracle",
      postgres: "Postgres",
      presto: "Presto",
      questdb: "QuestDB",
      snowflake: "Snowflake",
      sqlite: "SQLite",
      trino: "Trino",
    },
  },
  "Configuration/Databases/Trino.mdx": {
    ready: true,
    path: "product/configuration/data-sources/trino",
    title: "Trino",
  },
  "Configuration/Databases/Snowflake.mdx": {
    ready: true,
    path: "product/configuration/data-sources/snowflake",
    title: "Snowflake",
  },
  "Configuration/Databases/SQLite.mdx": {
    ready: true,
    path: "product/configuration/data-sources/sqlite",
    title: "SQLite",
  },
  "Configuration/Databases/QuestDB.mdx": {
    ready: true,
    path: "product/configuration/data-sources/questdb",
    title: "QuestDB",
  },
  "Configuration/Databases/Presto.mdx": {
    ready: true,
    path: "product/configuration/data-sources/presto",
    title: "Presto",
  },
  "Configuration/Databases/Postgres.mdx": {
    ready: true,
    path: "product/configuration/data-sources/postgres",
    title: "Postgres",
  },
  "Configuration/Databases/Oracle.mdx": {
    ready: true,
    path: "product/configuration/data-sources/oracle",
    title: "Oracle",
  },
  "Configuration/Databases/MySQL.mdx": {
    ready: true,
    path: "product/configuration/data-sources/mysql",
    title: "MySQL",
  },
  "Configuration/Databases/MongoDB.mdx": {
    ready: true,
    path: "product/configuration/data-sources/mongodb",
    title: "MongoDB",
  },
  "Configuration/Databases/Materialize.mdx": {
    ready: true,
    path: "product/configuration/data-sources/materialize",
    title: "Materialize",
  },
  "Configuration/Databases/MS-SQL.mdx": {
    ready: true,
    path: "product/configuration/data-sources/ms-sql",
    title: "MS-SQL",
  },
  "Configuration/Databases/Hive.mdx": {
    ready: true,
    path: "product/configuration/data-sources/hive",
    title: "Hive",
  },
  "Configuration/Databases/Google-BigQuery.mdx": {
    ready: true,
    path: "product/configuration/data-sources/google-bigquery",
    title: "Google BigQuery",
  },
  "Configuration/Databases/Firebolt.mdx": {
    ready: true,
    path: "product/configuration/data-sources/firebolt",
    title: "Firebolt",
  },
  "Configuration/Databases/Elasticsearch.mdx": {
    ready: true,
    path: "product/configuration/data-sources/elasticsearch",
    title: "Elasticsearch",
  },
  "Configuration/Databases/Druid.mdx": {
    ready: true,
    path: "product/configuration/data-sources/druid",
    title: "Druid",
  },
  "Configuration/Databases/Databricks-JDBC.mdx": {
    ready: true,
    path: "product/configuration/data-sources/databricks-jdbc",
    title: "Databricks (JDBC)",
  },
  "Configuration/Databases/ClickHouse.mdx": {
    ready: true,
    path: "product/configuration/data-sources/clickhouse",
    title: "ClickHouse",
  },
  "Configuration/Databases/AWS-Redshift.mdx": {
    ready: true,
    path: "product/configuration/data-sources/aws-redshift",
    title: "AWS Redshift",
  },
  "Configuration/Databases/AWS-Athena.mdx": {
    ready: true,
    path: "product/configuration/data-sources/aws-athena",
    title: "AWS Athena",
  },

  // Data modeling

  "Schema/Getting-Started.mdx": {
    ready: true,
    path: "product/data-modeling/overview",
    title: "Getting started with data modeling",
    meta: {
      overview: "Overview",
      fundamentals: "Fundamentals",
      reference: "Reference",
      advanced: "Advanced",
    },
  },

  // data modeling / fundamentals

  "Schema/Fundamentals/Concepts.mdx": {
    ready: true,
    path: "product/data-modeling/fundamentals/concepts",
    title: "Concepts",
  },
  "Schema/Fundamentals/Syntax.mdx": {
    ready: true,
    path: "product/data-modeling/fundamentals/syntax",
    title: "Syntax",
  },
  "Schema/Fundamentals/Working-with-Joins.mdx": {
    ready: true,
    path: "product/data-modeling/fundamentals/working-with-joins",
    title: "Working-with-Joins",
  },
  "Schema/Fundamentals/Additional-Concepts.mdx": {
    ready: true,
    path: "product/data-modeling/fundamentals/additional-concepts",
    title: "Additional-Concepts",
  },

  // data modeling / reference

  "Schema/Reference/cube.mdx": {
    ready: true,
    path: "product/data-modeling/reference/cube",
    title: "Cubes",
    meta: {
      cube: "Cubes",
      view: "Views",
      measures: "Measures",
      dimensions: "Dimensions",
      joins: "Joins",
      segments: "Segments",
      "pre-aggregations": "Pre-aggregations",
      "types-and-formats": "Types and Formats",
    },
  },
  "Schema/Reference/view.mdx": {
    ready: true,
    path: "product/data-modeling/reference/view",
    title: "Views",
  },
  "Schema/Reference/measures.mdx": {
    ready: true,
    path: "product/data-modeling/reference/measures",
    title: "Measures",
  },
  "Schema/Reference/dimensions.mdx": {
    ready: true,
    path: "product/data-modeling/reference/dimensions",
    title: "Dimensions",
  },
  "Schema/Reference/joins.mdx": {
    ready: true,
    path: "product/data-modeling/reference/joins",
    title: "Joins",
  },
  "Schema/Reference/segments.mdx": {
    ready: true,
    path: "product/data-modeling/reference/segments",
    title: "Segments",
  },
  "Schema/Reference/pre-aggregations.mdx": {
    ready: true,
    path: "product/data-modeling/reference/pre-aggregations",
    title: "Pre-aggregations",
  },
  "Schema/Reference/types-and-formats.mdx": {
    ready: true,
    path: "product/data-modeling/reference/types-and-formats",
    title: "Types and Formats",
  },

  // data modeling / advanced

  "Schema/Advanced/schema-execution-environment.mdx": {
    ready: true,
    path: "product/data-modeling/advanced/schema-execution-environment",
    title: "Execution Environment (JS models)",
    meta: {
      "schema-execution-environment": "Execution Environment (JS models)",
      "code-reusability-export-and-import": "Export and import",
      "code-reusability-extending-cubes": "Extending cubes",
      "data-blending": "Data blending",
      "dynamic-schema-creation": "Dynamic data models",
      "polymorphic-cubes": "Polymorphic cubes",
      "using-dbt": "Using dbt",
    },
  },
  "Schema/Advanced/Code-Reusability-Export-and-Import.mdx": {
    ready: true,
    path: "product/data-modeling/advanced/code-reusability-export-and-import",
    title: "Export and import",
  },
  "Schema/Advanced/Code-Reusability-Extending-Cubes.mdx": {
    ready: true,
    path: "product/data-modeling/advanced/code-reusability-extending-cubes",
    title: "Extending cubes",
  },
  "Schema/Advanced/Data-Blending.mdx": {
    ready: true,
    path: "product/data-modeling/advanced/data-blending",
    title: "Data blending",
  },
  "Schema/Advanced/Dynamic-Schema-Creation.mdx": {
    ready: true,
    path: "product/data-modeling/advanced/dynamic-schema-creation",
    title: "Dynamic data models",
  },
  "Schema/Advanced/Polymorphic-Cubes.mdx": {
    ready: true,
    path: "product/data-modeling/advanced/polymorphic-cubes",
    title: "Polymorphic cubes",
  },
  "Schema/Advanced/Using-dbt.mdx": {
    ready: true,
    path: "product/data-modeling/advanced/using-dbt",
    title: "Using dbt",
  },

  // Caching

  "Caching/Overview.mdx": {
    ready: true,
    path: "product/caching/overview",
    title: "Overview",
    meta: {
      overview: "Overview",
      "getting-started-pre-aggregations":
        "Getting started with pre-aggregations",
      "using-pre-aggregations": "Using pre-aggregations",
      "lambda-pre-aggregations": "Lambda pre-aggregations",
      "running-in-production": "Running in production",
    },
  },
  "Caching/Getting-Started-Pre-Aggregations.mdx": {
    ready: true,
    path: "product/caching/getting-started-pre-aggregations",
    title: "Getting started with pre-aggregations",
  },
  "Caching/Using-Pre-Aggregations.mdx": {
    ready: true,
    path: "product/caching/using-pre-aggregations",
    title: "Using pre-aggregations",
  },
  "Caching/Lambda-Pre-Aggregations.mdx": {
    ready: true,
    path: "product/caching/lambda-pre-aggregations",
    title: "Lambda pre-aggregations",
  },
  "Caching/Running-in-Production.mdx": {
    ready: true,
    path: "product/caching/running-in-production",
    title: "Running in production",
  },

  // auth

  "Auth/Overview.mdx": {
    ready: true,
    path: "product/auth/overview",
    title: "Overview",
    meta: {
      overview: "Overview",
      context: "Security context",
    },
  },
  "Auth/Security-Context.mdx": {
    ready: true,
    path: "product/auth/context",
    title: "Security context",
  },

  // rest api

  "REST-API/REST-API.mdx": {
    ready: true,
    path: "product/api-rest/overview",
    title: "Overview",
    meta: {
      overview: "Overview",
      "query-format": "Query format",
    },
  },
  "REST-API/Query-Format.mdx": {
    ready: true,
    path: "product/api-rest/query-format",
    title: "Query format",
  },

  // graphql api

  "GraphQL-API/GraphQL-API.mdx": {
    ready: true,
    path: "product/api-graphql",
    title: "GraphQL API",
  },

  // sql api

  "SQL-API/Overview.mdx": {
    ready: true,
    path: "product/api-sql/overview",
    title: "Overview",
    meta: {
      overview: "Overview",
      security: "Authentication and Authorization",
      joins: "Joins",
    },
  },
  "SQL-API/Authentication-and-Authorization.mdx": {
    ready: true,
    path: "product/api-sql/security",
    title: "Authentication and Authorization",
  },
  "SQL-API/Joins.mdx": {
    ready: true,
    path: "product/api-sql/joins",
    title: "Joins",
  },
  // "SQL-API/Template.mdx": {},

  // frontend

  "Frontend-Integrations/Introduction.mdx": {
    ready: true,
    path: "product/frontend-integrations/introduction",
    title: "Introduction",
    meta: {
      introduction: "Introduction",
      "real-time-data-fetch": "Real-Time data fetch",
    },
  },
  "Frontend-Integrations/Introduction-vue.mdx": {
    ready: true,
    path: "product/frontend-integrations/introduction/vue",
    title: "Vue",
    meta: {
      react: "React",
      vue: "Vue",
      angular: "Angular",
    },
  },
  "Frontend-Integrations/Introduction-react.mdx": {
    ready: true,
    path: "product/frontend-integrations/introduction/react",
    title: "React",
  },
  "Frontend-Integrations/Introduction-angular.mdx": {
    ready: true,
    path: "product/frontend-integrations/introduction/angular",
    title: "Angular",
  },
  "Frontend-Integrations/Real-Time-Data-Fetch.mdx": {
    ready: true,
    path: "product/frontend-integrations/real-time-data-fetch",
    title: "Real-Time data fetch",
  },

  // workspace

  "Workspace/Developer-Playground.mdx": {
    ready: true,
    path: "product/workspace/playground",
    title: "Playground",
    meta: {
      playground: "Playground",
      sso: "Single Sign-On",
      "dev-mode": "Development mode",
      "cube-ide": "Cube IDE",
      "query-inspection": "Query inspection",
      "pre-aggregation-inspection": "Pre-aggregation inspection",
      "access-control": "Access control",
      "sql-runner": "SQL Runner",
      preferences: "Preferences",
      cli: "CLI",
    },
  },
  "Workspace/Single-Sign-On/Overview.mdx": {
    ready: true,
    path: "product/workspace/sso",
    title: "Single Sign-On",
  },
  "Workspace/Single-Sign-On/SAML.mdx": {
    ready: true,
    path: "product/workspace/sso/saml",
    title: "SAML",
    meta: {
      saml: "SAML",
      okta: "Okta",
    },
  },
  "Workspace/Single-Sign-On/Okta.mdx": {
    ready: true,
    path: "product/workspace/sso/okta",
    title: "Okta",
  },
  "Workspace/Development-API.mdx": {
    ready: true,
    path: "product/workspace/dev-mode",
    title: "Development mode",
  },
  "Workspace/Cube-IDE.mdx": {
    ready: true,
    path: "product/workspace/cube-ide",
    title: "Cube IDE",
  },
  "Workspace/Inspecting-Queries.mdx": {
    ready: true,
    path: "product/workspace/query-inspection",
    title: "Query inspection",
  },
  "Workspace/Inspecting-Pre-aggregations.mdx": {
    ready: true,
    path: "product/workspace/pre-aggregation-inspection",
    title: "Pre-aggregation inspection",
  },
  "Workspace/Access-Control.mdx": {
    ready: true,
    path: "product/workspace/access-control",
    title: "Access control",
  },
  "Workspace/SQL-Runner.mdx": {
    ready: true,
    path: "product/workspace/sql-runner",
    title: "SQL Runner",
  },
  "Workspace/Preferences.mdx": {
    ready: true,
    path: "product/workspace/preferences",
    title: "Preferences",
  },
  "Workspace/CLI.mdx": {
    ready: true,
    path: "product/workspace/cli",
    title: "CLI",
  },

  // Deployment

  "Deployment/Overview.mdx": {
    ready: true,
    path: "product/deployment/overview",
    title: "Overview",
    meta: {
      overview: "Overview",
      "production-checklist": "Production checklist",
      cloud: "Cube Cloud",
      core: "Cube Core",
    },
  },
  "Deployment/Production-Checklist.mdx": {
    ready: true,
    path: "product/deployment/production-checklist",
    title: "Production checklist",
  },
  "Deployment/Cloud/Overview.mdx": {
    ready: true,
    path: "product/deployment/cloud/overview",
    title: "Overview",
    meta: {
      overview: "Overview",
      "auto-suspension": "Auto-suspension",
      "continuous-deployment": "Continuous deployment",
      "custom-domains": "Custom domains",
      "deployment-types": "Deployment types",
      pricing: "Pricing",
    },
  },
  "Deployment/Cloud/Auto-Suspension.mdx": {
    ready: true,
    path: "product/deployment/cloud/auto-suspension",
    title: "Auto-suspension",
  },
  "Deployment/Cloud/Continuous-Deployment.mdx": {
    ready: true,
    path: "product/deployment/cloud/continuous-deployment",
    title: "Continuous deployment",
  },
  "Deployment/Cloud/Custom-Domains.mdx": {
    ready: true,
    path: "product/deployment/cloud/custom-domains",
    title: "Custom domains",
  },
  "Deployment/Cloud/Deployment-Types.mdx": {
    ready: true,
    path: "product/deployment/cloud/deployment-types",
    title: "Deployment types",
  },
  "Deployment/Cloud/Pricing.mdx": {
    ready: true,
    path: "product/deployment/cloud/pricing",
    title: "Pricing",
  },
  "Deployment/Core/Overview.mdx": {
    ready: true,
    path: "product/deployment/core",
    title: "Cube Core",
  },

  // Monitoring

  "Monitoring/Alerts.mdx": {
    ready: true,
    path: "product/monitoring/alerts",
    title: "Alerts",
    meta: {
      alerts: "Alerts",
      "log-export": "Log Export",
    },
  },
  "Monitoring/Log-Export.mdx": {
    ready: true,
    path: "product/monitoring/log-export",
    title: "Log Export",
  },

  // reference

  "Reference/Configuration/Config.mdx": {
    ready: true,
    path: "reference/configuration/config",
    title: "Configuration options",
    meta: {
      config: "Configuration options",
      "environment-variables": "Environment variables",
    },
  },
  "Reference/Configuration/Environment-Variables-Reference.mdx": {
    ready: true,
    path: "reference/configuration/environment-variables",
    title: "Environment-variables",
  },
  "Reference/Frontend/@cubejs-client-vue.mdx": {
    ready: true,
    path: "reference/frontend/cubejs-client-vue",
    title: "@cubejs-client/vue",
  },
  "Reference/Frontend/@cubejs-client-ngx.mdx": {
    ready: true,
    path: "reference/frontend/cubejs-client-ngx",
    title: "@cubejs-client/ngx",

    meta: {
      "cubejs-client-core": "@cubejs-client/core",
      "cubejs-client-react": "@cubejs-client/react",
      "cubejs-client-ngx": "@cubejs-client/ngx",
      "cubejs-client-vue": "@cubejs-client/vue",
      "cubejs-client-ws-transport": "@cubejs-client/ws-transport",
    },
  },

  "Reference/REST-API/REST-API.mdx": {
    ready: true,
    path: "reference/rest-api",
    title: "REST API",

    meta: {
      configuration: "Configuration",
      frontend: "Frontend",
      "rest-api": "REST API",
      "graphql-api": "GraphQL API",
      "sql-api": "SQL API",
      cli: "CLI",
    },
  },
  "Reference/GraphQL-API/GraphQL-API.mdx": {
    ready: true,
    path: "reference/graphql-api",
    title: "GraphQL API",
  },
  "Reference/SQL-API/SQL-Commands.mdx": {
    ready: true,
    path: "reference/sql-api/sql-commands",
    title: "SQL commands",
    meta: {
      "sql-commands": "SQL commands",
      "sql-functions-and-operators": "SQL functions and operators",
    },
  },
  "Reference/SQL-API/SQL-Functions-and-Operators.mdx": {
    ready: true,
    path: "reference/sql-api/sql-functions-and-operators",
    title: "SQL functions and operators",
  },
  "Reference/CLI/CLI-Reference.mdx": {
    ready: true,
    path: "reference/cli",
    title: "CLI Command reference",
  },

  "Examples-Tutorials-Recipes/Examples.mdx": {
    ready: true,
    path: "guides/examples",
    title: "Examples",
    meta: {
      examples: "Examples",
      recipes: "Recipes",
    },
  },
  "Examples-Tutorials-Recipes/Recipes.mdx": {
    ready: true,
    path: "guides/recipes/overview",
    title: "Recipes",
    meta: {
      overview: "Overview",
      analytics: "Analytics",
      "access-control": "Access control",
      auth: "Authentication & Authorization",
      "data-modeling": "Data modeling",
      "data-sources": "Data sources",
      queries: "Queries",
      "query-acceleration": "Query acceleration",
      "code-reusability": "Code reusability",
      "upgrading-cube": "Upgrading Cube",
    },
  },
  "Examples-Tutorials-Recipes/Recipes/Upgrading-Cube/Migrating-from-Express-to-Docker.mdx":
    {
      ready: true,
      path: "guides/recipes/upgrading-cube/migrating-from-express-to-docker",
      title: "Migrating from Express to Docker",
    },

  "Examples-Tutorials-Recipes/Recipes/Query-acceleration/using-originalsql-and-rollups-effectively.mdx":
    {
      ready: true,
      path: "guides/recipes/query-acceleration/using-originalsql-and-rollups-effectively",
      title: "Using originalSql and rollup pre-aggregations effectively",
      meta: {
        "incrementally-building-pre-aggregations-for-a-date-range":
          "Incrementally building pre-aggregations for a date range",
        "refreshing-select-partitions": "Refreshing select partitions",
        "joining-multiple-data-sources":
          "Joining data from multiple data sources",
        "non-additivity": "Accelerating non-additive measures",
        "using-originalsql-and-rollups-effectively":
          "Using originalSql and rollup pre-aggregations effectively",
      },
    },
  "Examples-Tutorials-Recipes/Recipes/Query-acceleration/non-additivity.mdx": {
    ready: true,
    path: "guides/recipes/query-acceleration/non-additivity",
    title: "Accelerating non-additive measures",
  },
  "Examples-Tutorials-Recipes/Recipes/Query-acceleration/joining-multiple-data-sources.mdx":
    {
      ready: true,
      path: "guides/recipes/query-acceleration/joining-multiple-data-sources",
      title: "Joining data from multiple data sources",
    },
  "Examples-Tutorials-Recipes/Recipes/Query-acceleration/incrementally-building-pre-aggregations-for-a-date-range.mdx":
    {
      ready: true,
      path: "guides/recipes/query-acceleration/incrementally-building-pre-aggregations-for-a-date-range",
      title: "Incrementally building pre-aggregations for a date range",
    },
  "Examples-Tutorials-Recipes/Recipes/Query-acceleration/Refreshing-select-partitions.mdx":
    {
      ready: true,
      path: "guides/recipes/query-acceleration/refreshing-select-partitions",
      title: "Refreshing select partitions",
    },

  "Examples-Tutorials-Recipes/Recipes/Queries/pagination.mdx": {
    ready: true,
    path: "guides/recipes/queries/pagination",
    title: "Implementing pagination",
    meta: {
      pagination: "Implementing pagination",
      "getting-unique-values-for-a-field": "Getting unique values for a field",
    },
  },
  "Examples-Tutorials-Recipes/Recipes/Queries/getting-unique-values-for-a-field.mdx":
    {
      ready: true,
      path: "guides/recipes/queries/getting-unique-values-for-a-field",
      title: "Getting unique values for a field",
    },

  "Examples-Tutorials-Recipes/Recipes/Data-modeling/using-dynamic-measures.mdx":
    {
      ready: true,
      path: "guides/recipes/data-modeling/using-dynamic-measures",
      title: "Using dynamic measures",
      meta: {
        "dynamic-union-tables": "Using dynamic union tables",
        "entity-attribute-value":
          "Implementing Entity-Attribute-Value Model (EAV)",
        "passing-dynamic-parameters-in-a-query":
          "Passing dynamic parameters in a query",
        snapshots: "Implementing data snapshots",
        "using-dynamic-measures": "Using dynamic measures",
        percentiles: "Calculating averages and percentiles",
      },
    },
  "Examples-Tutorials-Recipes/Recipes/Data-modeling/snapshots.mdx": {
    ready: true,
    path: "guides/recipes/data-modeling/snapshots",
    title: "Implementing data snapshots",
  },
  "Examples-Tutorials-Recipes/Recipes/Data-modeling/percentiles.mdx": {
    ready: true,
    path: "guides/recipes/data-modeling/percentiles",
    title: "Calculating averages and percentiles",
  },
  "Examples-Tutorials-Recipes/Recipes/Data-modeling/passing-dynamic-parameters-in-a-query.mdx":
    {
      ready: true,
      path: "guides/recipes/data-modeling/passing-dynamic-parameters-in-a-query",
      title: "Passing dynamic parameters in a query",
    },
  "Examples-Tutorials-Recipes/Recipes/Data-modeling/entity-attribute-value.mdx":
    {
      ready: true,
      path: "guides/recipes/data-modeling/entity-attribute-value",
      title: "Implementing Entity-Attribute-Value Model (EAV)",
    },
  "Examples-Tutorials-Recipes/Recipes/Data-modeling/dynamic-union-tables.mdx": {
    ready: true,
    path: "guides/recipes/data-modeling/dynamic-union-tables",
    title: "Using Dynamic Union Tables",
  },

  "Examples-Tutorials-Recipes/Recipes/Code-reusability/schema-generation.mdx": {
    ready: true,
    path: "guides/recipes/code-reusability/schema-generation",
    title: "Implementing Schema Generation",
  },

  "Examples-Tutorials-Recipes/Recipes/Data-sources/multiple-sources-same-schema.mdx":
    {
      ready: true,
      path: "guides/recipes/data-sources/multiple-sources-same-schema",
      title: "Using multiple data sources",
      meta: {
        "multiple-sources-same-schema": "Using multiple data sources",
        "using-ssl-connections-to-data-source":
          "Using SSL Connections to a data source",
      },
    },
  "Examples-Tutorials-Recipes/Recipes/Data-sources/using-ssl-connections-to-data-source.mdx":
    {
      ready: true,
      path: "guides/recipes/data-sources/using-ssl-connections-to-data-source",
      title: "Using SSL Connections to a data source",
    },

  "Examples-Tutorials-Recipes/Recipes/Auth/Auth0-Guide.mdx": {
    ready: true,
    path: "guides/recipes/auth/auth0-guide",
    title: "Authenticate requests to Cube with Auth0",
    meta: {
      "auth0-guide": "Authenticate requests to Cube with Auth0",
      "aws-cognito": "Authenticate requests to Cube with AWS Cognito",
    },
  },
  "Examples-Tutorials-Recipes/Recipes/Auth/AWS-Cognito.mdx": {
    ready: true,
    path: "guides/recipes/auth/aws-cognito",
    title: "Authenticate requests to Cube with AWS Cognito",
  },

  "Examples-Tutorials-Recipes/Recipes/Queries/enforcing-mandatory-filters.mdx":
    {
      ready: true,
      path: "guides/recipes/access-control/enforcing-mandatory-filters",
      title: "Enforcing mandatory filters",
      meta: {
        "enforcing-mandatory-filters": "Enforcing mandatory filters",
        "column-based-access": "Enforcing column-based access",
        "role-based-access": "Enforcing role-based access",
        "controlling-access-to-cubes-and-views":
          "Controlling access to cubes and views",
        "using-different-schemas-for-tenants":
          "Using different data models for tenants",
      },
    },
  "Examples-Tutorials-Recipes/Recipes/Access-control/using-different-schemas-for-tenants.mdx":
    {
      ready: true,
      path: "guides/recipes/access-control/using-different-schemas-for-tenants",
      title: "Using different data models for tenants",
    },
  "Examples-Tutorials-Recipes/Recipes/Access-control/role-based-access.mdx": {
    ready: true,
    path: "guides/recipes/access-control/role-based-access",
    title: "Enforcing role-based access",
  },
  "Examples-Tutorials-Recipes/Recipes/Access-control/controlling-access-to-cubes-and-views.mdx":
    {
      ready: true,
      path: "guides/recipes/access-control/controlling-access-to-cubes-and-views",
      title: "Controlling access to cubes and views",
    },
  "Examples-Tutorials-Recipes/Recipes/Access-control/column-based-access.mdx": {
    ready: true,
    path: "guides/recipes/access-control/column-based-access",
    title: "Enforcing column-based access",
  },

  "Examples-Tutorials-Recipes/Recipes/Analytics/funnels.mdx": {
    ready: true,
    path: "guides/recipes/analytics/funnels",
    title: "Implementing funnel analysis",
    meta: {
      "active-users": "Daily, Weekly, Monthly Active Users (DAU, WAU, MAU)",
      "event-analytics": "Implementing event analytics",
      "cohort-retention": "Implementing retention analysis & cohorts",
      funnels: "Implementing Funnel Analysis",
    },
  },
  "Examples-Tutorials-Recipes/Recipes/Analytics/event-analytics.mdx": {
    ready: true,
    path: "guides/recipes/analytics/event-analytics",
    title: "Implementing event analytics",
  },
  "Examples-Tutorials-Recipes/Recipes/Analytics/cohort-retention.mdx": {
    ready: true,
    path: "guides/recipes/analytics/cohort-retention",
    title: "Implementing retention analysis & cohorts",
  },
  "Examples-Tutorials-Recipes/Recipes/Analytics/active-users.mdx": {
    ready: true,
    path: "guides/recipes/analytics/active-users",
    title: "Daily, Weekly, Monthly Active Users (DAU, WAU, MAU)",
  },

  "Guides/Style-Guide.mdx": {
    ready: true,
    path: "guides/style-guide",
    title: "Cube Style Guide",
  },
  "Guides/Data-Store-Cost-Saving-Guide.mdx": {
    ready: true,
    path: "guides/data-store-cost-saving-guide",
    title: "Data Store Cost Saving Guide",
  },
  "FAQs/Troubleshooting.mdx": {
    ready: true,
    path: "product/faqs/troubleshooting",
    title: "Troubleshooting",
  },
  "FAQs/Tips-and-Tricks.mdx": {
    ready: true,
    path: "product/faqs/tips-and-tricks",
    title: "Tips and Tricks",
  },
  "FAQs/General.mdx": {
    ready: true,
    path: "product/faqs/general",
    title: "General",
  },
};

async function main() {
  await cleanup();

  const mdxFiles = await glob("../content/**/*.mdx");

  await Promise.all(
    mdxFiles.map(async (filePath) => {
      // Strip the `../content/` prefix to get the path relative to the old docs content root
      const override = overrides[filePath.slice(11)];

      if (override && override.ready && override.path) {
        const file = await readFile(filePath, "utf8");
        const data = frontmatter<DocAttributes>(file);
        const permalink = data.attributes.permalink;

        if (data.attributes.category === "Internal") {
          return;
        }

        const targetFilePath = `pages/${override.path}.mdx`;

        const folderPath = dirname(targetFilePath);
        const folderExists = await stat(folderPath).catch(() => false);

        if (!folderExists) {
          await mkdir(folderPath, {
            recursive: true,
          });
        }

        if (override.meta) {
          await writeFile(
            path.resolve(folderPath, "_meta.js"),
            `module.exports = ${JSON.stringify(override.meta, null, 2)}`
          );
        }

        const redirects = data.attributes.redirect_from
          ? [...data.attributes.redirect_from, permalink]
          : [permalink];

        const content = `---
redirect_from:
${redirects.map((r) => `  - ${r}`).join("\n")}
---

# ${override.title}

${data.body
  .replaceAll(/<--\{"id"\s*:\s*"[^"]*"\}-->/g, "")
  .replaceAll(/<\!--(.+)-->/g, "")
  .replaceAll(/!\[image\|\d+x\d+\]\([^)]+\)/g, "")
  .replaceAll(`style="text-align: center"`, `style={{ textAlign: "center" }}`)
  .replaceAll("<pre><code>", '<pre><code>{"')
  .replaceAll("</code></pre>", '"}</code></pre>')
  .replaceAll(`style="border: none"`, `style={{ border: "none" }}`)
  .replaceAll(
    `style="width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;"`,
    `style={{
      width: "100%",
      height:500,
      border:0,
      borderRadius: 4,
      overflow:"hidden"
    }}`
  )}`;

        // uncomment when ready to move docs content
        // await rename(filePath, path.resolve(targetFilePath));

        await writeFile(path.resolve(targetFilePath), content);
      }
    })
  );
}

try {
  main();
} catch (err) {
  console.error(err);
}
