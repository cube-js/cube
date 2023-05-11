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

const OverridesPersisted = require("../overrides.json");

const Categories: Record<string, string> = {
  "Cube.js Introduction": "/",
  "Getting Started": "/getting-started/",
  Configuration: "/configuration/",
  "Data Modeling": "/data-modeling/",
  Caching: "/caching/",
  "Authentication & Authorization": "/auth/",
  "REST API": "/rest-api/",
  "GraphQL API": "/graphql-api/",
  "SQL API": "/sql-api/",
  "Frontend Integrations": "/frontend-integrations/",
  Workspace: "/workspace/",
  Deployment: "/deployment/",
  Monitoring: "/monitoring/",
  // "Examples & Tutorials": "/examples-and-tutorials/",
  // FAQs: "/faqs/",
  // "Release Notes": "/releases/",
  Reference: "/reference/",
  Guides: "/guides/",
};

type Node = {
  title: string;
  children?: Nodes;
};

type Nodes = Record<string, Node | string>;

const Structure: Nodes = {
  introduction: {
    title: "Introduction",
  },
  "getting-started": {
    title: "Getting Started",
    children: {
      overview: {
        title: "Overview",
      },
      "cube-cloud": {
        title: "Cube Cloud",
        children: {
          "getting-started-with-cube-cloud": {
            title: "Getting started with Cube Cloud",
          },
          "create-a-deployment": {
            title: "Create a deployment",
          },
          "generate-data-models": {
            title: "Generate data models",
          },
          "query-data": {
            title: "Query data",
          },
          "add-a-pre-aggregation": {
            title: "Add a pre-aggregation",
          },
          "learn-more": {
            title: "Learn More",
          },
        },
      },
      "cube-core": {
        title: "Cube Core",
        children: {
          "getting-started-with-cube-core": {
            title: "Getting started with Cube Core",
          },
          "create-a-project": {
            title: "Create a project",
          },
          "query-data": {
            title: "Query data",
          },
          "add-a-pre-aggregation": {
            title: "Add a pre-aggregation",
          },
          "learn-more": {
            title: "Learn More",
          },
        },
      },
    },
  },
  configuration: {
    title: "Configuration",
    children: {
      overview: {
        title: "Overview",
      },
      "connecting-to-data-sources": {
        title: "Connecting to data sources",
      },
      "connecting-to-visualization-tools": {
        title: "Connecting to visualization tools",
      },
      "connecting-with-a-vpc": {
        title: "Connecting with a VPC",
      },
      advanced: {
        title: "Advanced",

        children: {
          "multiple-data-sources": {
            title: "Multiple data sources",
          },
          multitenancy: {
            title: "Multitenancy",
          },
        },
      },
    },
  },
  "data-modeling": {
    title: "Data Modeling",
    children: {
      "getting-started-with-data-modeling": {
        title: "Getting Started",
      },
      fundamentals: {
        title: "Fundamentals",

        children: {
          "data-modeling-concepts": "Concepts",
          "additional-concepts": "Additional concepts",
          syntax: "Syntax",
          "working-with-joins": "Working with joins",
        },
      },
      reference: {
        title: "Reference",
      },
      advanced: {
        title: "Advanced",
      },
    },
  },
  caching: {
    title: "Caching",
    children: {},
  },
  auth: {
    title: "Authentication & Authorization",
    children: {},
  },
  "rest-api": {
    title: "REST API",
    children: {},
  },
  "graphql-api": {
    title: "GraphHQL API",
    children: {},
  },
  "sql-api": {
    title: "SQL API",
    children: {},
  },
  "frontend-integrations": {
    title: "Frontend Integrations",
    children: {},
  },
  workspace: {
    title: "Workspace",
    children: {},
  },
  deployment: {
    title: "Deployment",
    children: {},
  },
  monitoring: {
    title: "Monitoring",
    children: {},
  },
  // "examples-and-tutorials": {
  //   title: "Examples & Tutorials",
  //   children: {},
  // },
  // faqs: {
  //   title: "FAQs",
  //   children: {},
  // },
  reference: {
    title: "Reference",
    children: {},
  },
  guides: {
    title: "Guides",
    children: {},
  },
};

function processNodes(nodes: Nodes, nodePath: string): Promise<any>[] {
  return Object.keys(nodes).reduce<Promise<any>[]>((acc, p) => {
    if ((nodes[p] as any).children) {
      const children = (nodes[p] as any).children!;
      const metaPath = path.resolve("./" + nodePath, "./" + p, "./_meta.json");

      const meta = Object.keys(children).reduce((acc, k) => {
        acc[k] = children[k].title || children[k];
        return acc;
      }, {} as Record<string, string>);

      const childrenPromises = processNodes(children, nodePath + "/" + p);

      return [
        ...acc,
        writeFile(metaPath, JSON.stringify(meta, null, 2)),
        ...childrenPromises,
      ];
    }

    return acc;
  }, []);
}

const Overrides: Record<
  string,
  {
    origin: DocAttributes;
    done: boolean;
    target: {
      path: string;
      fileName: string;
      title: string;
      redirects: string[];
    };
  }
> = {};

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

const TARGET = "pages";

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
  ready?: boolean;
  path?: string;
  title?: string;
}

const files: Record<string, Override> = {
  "Cube.js-Introduction.mdx": {
    ready: true,
    path: "product/introduction",
    title: "Introduction",
  },

  // configuration
  "Configuration/Overview.mdx": {
    ready: true,
    path: "product/configuration/overview",
    title: "Overview",
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
    title: "Google-BigQuery",
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
    title: "Databricks-JDBC",
  },
  "Configuration/Databases/ClickHouse.mdx": {
    ready: true,
    path: "product/configuration/data-sources/clickhouse",
    title: "ClickHouse",
  },
  "Configuration/Databases/AWS-Redshift.mdx": {
    ready: true,
    path: "product/configuration/data-sources/aws-redshift",
    title: "AWS-Redshift",
  },
  "Configuration/Databases/AWS-Athena.mdx": {
    ready: true,
    path: "product/configuration/data-sources/aws-athena",
    title: "AWS-Athena",
  },

  //

  "Workspace/SQL-Runner.mdx": {},
  "Workspace/Preferences.mdx": {},
  "Workspace/Inspecting-Queries.mdx": {},
  "Workspace/Inspecting-Pre-aggregations.mdx": {},
  "Workspace/Development-API.mdx": {},
  "Workspace/Developer-Playground.mdx": {},
  "Workspace/Cube-IDE.mdx": {},
  "Workspace/CLI.mdx": {},
  "Workspace/Access Control.mdx": {},
  "Style-Guide/Overview.mdx": {},
  "Schema/Getting-Started.mdx": {},
  "SQL-API/Template.mdx": {},
  "SQL-API/Overview.mdx": {},
  "SQL-API/Joins.mdx": {},
  "SQL-API/Authentication-and-Authorization.mdx": {},
  "REST-API/REST-API.mdx": {},
  "REST-API/Query-Format.mdx": {},
  "Monitoring/Log-Export.mdx": {},
  "Monitoring/Alerts.mdx": {},
  "GraphQL-API/GraphQL-API.mdx": {},
  "Getting-Started/Overview.mdx": {},
  "Frontend-Integrations/Real-Time-Data-Fetch.mdx": {},
  "Frontend-Integrations/Introduction.mdx": {},
  "Frontend-Integrations/Introduction-vue.mdx": {},
  "Frontend-Integrations/Introduction-react.mdx": {},
  "Frontend-Integrations/Introduction-angular.mdx": {},
  "FAQs/Troubleshooting.mdx": {},
  "FAQs/Tips-and-Tricks.mdx": {},
  "FAQs/General.mdx": {},
  "Examples-Tutorials-Recipes/Recipes.mdx": {},
  "Examples-Tutorials-Recipes/Examples.mdx": {},
  "Deployment/Production-Checklist.mdx": {},
  "Deployment/Overview.mdx": {},
  "Caching/Using-Pre-Aggregations.mdx": {},
  "Caching/Running-in-Production.mdx": {},
  "Caching/Overview.mdx": {},
  "Caching/Lambda-Pre-Aggregations.mdx": {},
  "Caching/Getting-Started-Pre-Aggregations.mdx": {},
  "Auth/Security-Context.mdx": {},
  "Auth/Overview.mdx": {},
  "Workspace/Single-Sign-On/SAML.mdx": {},
  "Workspace/Single-Sign-On/Overview.mdx": {},
  "Workspace/Single-Sign-On/Okta.mdx": {},
  "Schema/Reference/view.mdx": {},
  "Schema/Reference/types-and-formats.mdx": {},
  "Schema/Reference/segments.mdx": {},
  "Schema/Reference/pre-aggregations.mdx": {},
  "Schema/Reference/measures.mdx": {},
  "Schema/Reference/joins.mdx": {},
  "Schema/Reference/dimensions.mdx": {},
  "Schema/Reference/cube.mdx": {},
  "Reference/REST-API/REST-API.mdx": {},
  "Schema/Fundamentals/Working-with-Joins.mdx": {},
  "Schema/Fundamentals/Syntax.mdx": {},
  "Schema/Fundamentals/Concepts.mdx": {},
  "Schema/Fundamentals/Additional-Concepts.mdx": {},
  "Schema/Advanced/schema-execution-environment.mdx": {},
  "Schema/Advanced/Using-dbt.mdx": {},
  "Schema/Advanced/Polymorphic-Cubes.mdx": {},
  "Schema/Advanced/Dynamic-Schema-Creation.mdx": {},
  "Schema/Advanced/Data-Blending.mdx": {},
  "Schema/Advanced/Code-Reusability-Extending-Cubes.mdx": {},
  "Schema/Advanced/Code-Reusability-Export-and-Import.mdx": {},
  "Reference/GraphQL-API/GraphQL-API.mdx": {},
  "Reference/Configuration/Environment-Variables-Reference.mdx": {},
  "Reference/Configuration/Config.mdx": {},
  "Getting-Started/Migrate-from-Core/Upload-with-CLI.mdx": {},
  "Getting-Started/Migrate-from-Core/Import-GitLab-repository-via-SSH.mdx": {},
  "Getting-Started/Migrate-from-Core/Import-GitHub-repository.mdx": {},
  "Getting-Started/Migrate-from-Core/Import-Git-repository-via-SSH.mdx": {},
  "Getting-Started/Migrate-from-Core/Import-Bitbucket-repository-via-SSH.mdx":
    {},
  "Reference/CLI/CLI-Reference.mdx": {},
  "Getting-Started/Core/05-Learn-more.mdx": {},
  "Getting-Started/Core/04-Add-a-pre-aggregation.mdx": {},
  "Getting-Started/Core/03-Query-data.mdx": {},
  "Getting-Started/Core/02-Create-a-project.mdx": {},
  "Getting-Started/Core/01-Overview.mdx": {},
  "Getting-Started/Cloud/06-Learn-more.mdx": {},
  "Getting-Started/Cloud/05-Add-a-pre-aggregation.mdx": {},
  "Getting-Started/Cloud/04-Query-data.mdx": {},
  "Getting-Started/Cloud/03-Generate-models.mdx": {},
  "Getting-Started/Cloud/02-Create-a-deployment.mdx": {},
  "Getting-Started/Cloud/01-Overview.mdx": {},
  "Deployment/Core/Overview.mdx": {},
  "Deployment/Cloud/Pricing.mdx": {},
  "Deployment/Cloud/Overview.mdx": {},
  "Deployment/Cloud/Deployment-Types.mdx": {},
  "Deployment/Cloud/Custom-Domains.mdx": {},
  "Deployment/Cloud/Continuous-Deployment.mdx": {},
  "Deployment/Cloud/Auto-Suspension.mdx": {},

  "Examples-Tutorials-Recipes/Recipes/Upgrading-Cube/Migrating-from-Express-to-Docker.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Query-acceleration/using-originalsql-and-rollups-effectively.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Query-acceleration/non-additivity.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Query-acceleration/joining-multiple-data-sources.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Query-acceleration/incrementally-building-pre-aggregations-for-a-date-range.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Query-acceleration/Refreshing-select-partitions.mdx":
    {},

  "Examples-Tutorials-Recipes/Recipes/Queries/pagination.mdx": {},
  "Examples-Tutorials-Recipes/Recipes/Queries/getting-unique-values-for-a-field.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Queries/enforcing-mandatory-filters.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Data-modeling/using-dynamic-measures.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Data-modeling/snapshots.mdx": {},
  "Examples-Tutorials-Recipes/Recipes/Data-modeling/percentiles.mdx": {},
  "Examples-Tutorials-Recipes/Recipes/Data-modeling/passing-dynamic-parameters-in-a-query.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Data-modeling/entity-attribute-value.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Data-modeling/dynamic-union-tables.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Code-reusability/schema-generation.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Data-sources/using-ssl-connections-to-data-source.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Data-sources/multiple-sources-same-schema.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Auth/Auth0-Guide.mdx": {},
  "Examples-Tutorials-Recipes/Recipes/Auth/AWS-Cognito.mdx": {},
  "Examples-Tutorials-Recipes/Recipes/Access-control/using-different-schemas-for-tenants.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Access-control/role-based-access.mdx": {},
  "Examples-Tutorials-Recipes/Recipes/Access-control/controlling-access-to-cubes-and-views.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Access-control/column-based-access.mdx":
    {},
  "Examples-Tutorials-Recipes/Recipes/Analytics/funnels.mdx": {},
  "Examples-Tutorials-Recipes/Recipes/Analytics/event-analytics.mdx": {},
  "Examples-Tutorials-Recipes/Recipes/Analytics/cohort-retention.mdx": {},
  "Examples-Tutorials-Recipes/Recipes/Analytics/active-users.mdx": {},
  "Reference/SQL-API/SQL-Functions-and-Operators.mdx": {},
  "Reference/SQL-API/SQL-Commands.mdx": {},
  "Reference/Frontend/@cubejs-client-vue.mdx": {},
  "Reference/Frontend/@cubejs-client-ngx.mdx": {},
};

async function main() {
  await cleanup();

  const mdxFiles = await glob("../content/**/*.mdx");

  await Promise.all(
    mdxFiles.map(async (filePath) => {
      const override = files[filePath.slice(11)];

      console.log(override);
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

        if (!folderExists)
          await mkdir(folderPath, {
            recursive: true,
          });

        await writeFile(
          path.resolve(targetFilePath),
          `# ${override.title}
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
    )}`
        );
      }
    })
  );

  // await writeFile("overrides.json", JSON.stringify(Overrides, null, 2));

  return;

  // prepare structure overrides
  await Promise.all(
    mdxFiles.map(async (filePath) => {
      const file = await readFile(filePath, "utf8");
      const data = frontmatter<DocAttributes>(file);
      const permalink = data.attributes.permalink;

      if (data.attributes.category === "Internal") {
        return;
      }

      let targetPath = "/TODO/";

      if (data.attributes.category && Categories[data.attributes.category]) {
        targetPath = Categories[data.attributes.category];
      }

      if (data.attributes.subCategory) {
        targetPath += data.attributes.subCategory
          .toLowerCase()
          .replaceAll(" ", "-");
      }

      const overrided = !!OverridesPersisted[filePath]?.done;

      const originFileName = path.basename(filePath).slice(0, -4);
      const fileName =
        targetPath === "/releases/"
          ? originFileName.replaceAll(".", "-")
          : data.attributes.title
              .toLowerCase()
              .replaceAll(" ", "-")
              .replaceAll("/", "-")
              .replaceAll("@", "");

      Overrides[filePath] = {
        origin: data.attributes,
        done: overrided,
        target: overrided
          ? OverridesPersisted[filePath].target
          : {
              path: targetPath,
              fileName,
              title: data.attributes.title,
              redirects: [
                ...(permalink && permalink !== targetPath + fileName
                  ? [permalink]
                  : []),
                ...(data.attributes.redirect_from || []),
              ],
            },
      };
    })
  );

  // persist changes
  await writeFile("overrides.json", JSON.stringify(Overrides, null, 2));

  // migrate content
  await Promise.all(
    mdxFiles.map(async (filePath) => {
      const file = await readFile(filePath, "utf8");
      const data = frontmatter<DocAttributes>(file);

      const target = Overrides[filePath]?.target;

      if (!target) {
        console.log("SKIP:", filePath);
        return;
      }

      await mkdir(path.resolve(`${TARGET}${target.path}`), {
        recursive: true,
      });

      const targetFilePath = `${TARGET}${target.path}/${target.fileName}.mdx`;

      // uncomment when ready to move docs content
      // await rename(filePath, targetFilePath);

      await writeFile(
        path.resolve(targetFilePath),
        `# ${target.title}
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
  )}`
      );
    })
  );

  const promises = processNodes(Structure, "pages");

  await Promise.all(promises);
}

try {
  main();
} catch (err) {
  console.error(err);
}
