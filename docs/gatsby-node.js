const path = require('path');
const { renameCategory } = require('./src/rename-category.js');

exports.createPages = ({ actions, graphql }) => {
  const { createPage } = actions;

  const DocTemplate = path.resolve('src/templates/DocTemplate.tsx');

  return graphql(`{
    allMdx(
      limit: 1000
    ) {
      edges {
        node {
          body
          fileAbsolutePath
          frontmatter {
            permalink
            title
            scope
            category
            menuOrder
          }
        }
      }
    }
  }`).then(result => {
    if (result.errors) {
      return Promise.reject(result.errors);
    }

    result.data.allMdx.edges.forEach(({ node }) => {
      createPage({
        path: node.frontmatter.permalink,
        title: node.frontmatter.title,
        component: DocTemplate,
        context: {
          scope: node.frontmatter.scope,
          fileAbsolutePath: node.fileAbsolutePath,
          category: renameCategory(node.frontmatter.category),
          noscrollmenu: false,
          slug: node.frontmatter.permalink,
        },
      });
    });
  });
};

exports.onCreateNode = ({ node, actions, getNode }) => {
  const { createNodeField } = actions;

  if (node.internal.type === 'Mdx') {
    createNodeField({
      name: 'slug',
      node,
      value: node.frontmatter.permalink,
    });
  }
};

exports.onCreateWebpackConfig = ({ actions, stage, loaders }) => {
  // If production JavaScript and CSS build
  if (stage === 'build-javascript') {
    // Turn off source maps
    actions.setWebpackConfig({
      devtool: false,
    });
  }
  // https://www.gatsbyjs.com/docs/debugging-html-builds/#fixing-third-party-modules
  if (stage === 'build-html' || stage === 'develop-html') {
    actions.setWebpackConfig({
      module: {
        rules: [
          {
            test: /cubedev-tracking/,
            use: loaders.null(),
          },
        ],
      },
    });
  }
};
