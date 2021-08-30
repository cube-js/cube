const path = require('path');
const { renameCategory } = require('./src/rename-category.js');

exports.createPages = ({ actions, graphql }) => {
  const { createPage } = actions;

  const DocTemplate = path.resolve('src/templates/DocTemplate.tsx');

  return graphql(`{
    allMarkdownRemark(
      limit: 1000
    ) {
      edges {
        node {
          html
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

    result.data.allMarkdownRemark.edges.forEach(({ node }) => {
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

  if (node.internal.type === 'MarkdownRemark') {
    createNodeField({
      name: 'slug',
      node,
      value: node.frontmatter.permalink,
    });
  }
};

exports.onCreateWebpackConfig = ({ actions, stage }) => {
  // If production JavaScript and CSS build
  if (stage === 'build-javascript') {
    // Turn off source maps
    actions.setWebpackConfig({
      devtool: false,
    });
  }
};
