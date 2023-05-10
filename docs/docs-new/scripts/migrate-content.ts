import path from "path";
import {
  readFile,
  mkdir,
  writeFile,
  unlink,
  lstat,
  readdir,
  rmdir,
  rename,
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
  "Examples & Tutorials": "/examples-and-tutorials/",
  FAQs: "/faqs/",
  "Release Notes": "/releases/",
  Reference: "/reference/",
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
  "examples-and-tutorials": {
    title: "Examples & Tutorials",
    children: {},
  },
  faqs: {
    title: "FAQs",
    children: {},
  },
  reference: {
    title: "Reference",
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

async function main() {
  await cleanup();

  const mdxFiles = await glob("../content/**/*.mdx");

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
