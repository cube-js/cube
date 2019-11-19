const path = require("path");
const _ = require("lodash");

exports.onCreateNode = () => ({ node, actions, getNode }) => {
  const { createNodeField } = actions;
  let slug;
  if (node.internal.type === "MarkdownRemark") {
    const fileNode = getNode(node.parent);
    const parsedFilePath = path.parse(fileNode.relativePath);
    if (
      Object.prototype.hasOwnProperty.call(node, "frontmatter") &&
      Object.prototype.hasOwnProperty.call(node.frontmatter, "title")
    ) {
      slug = `/${_.kebabCase(node.frontmatter.title)}`;
    } else {
      slug = `/${parsedFilePath.dir}/`;
    }

    createNodeField({ node, name: "slug", value: slug });
  }
};

exports.createPages = (siteConfig) => async ({ graphql, actions }) => {
  const { createPage } = actions;
  const postPage = path.join(__dirname, "src/templates/post.jsx")

  const markdownQueryResult = await graphql(
    `
      {
        allMarkdownRemark(
          sort: { fields: [frontmatter___order], order: ASC }
        ) {
          edges {
            node {
              fields {
                slug
              }
              frontmatter {
                title
                order
              }
            }
          }
        }
      }
    `
  );

  if (markdownQueryResult.errors) {
    console.error(markdownQueryResult.errors);
    throw markdownQueryResult.errors;
  }

  const postsEdges = markdownQueryResult.data.allMarkdownRemark.edges;

  postsEdges.forEach((edge, index) => {
    const nextID = index + 1;
    const prevID = index - 1;
    const nextEdge = postsEdges[nextID];
    const prevEdge = postsEdges[prevID];

    createPage({
      path: edge.node.fields.slug,
      component: postPage,
      context: {
        config: siteConfig,
        slug: edge.node.fields.slug,
        nexttitle: nextEdge && nextEdge.node.frontmatter.title,
        nextslug: nextEdge && nextEdge.node.fields.slug,
        prevtitle: prevEdge && prevEdge.node.frontmatter.title,
        prevslug: prevEdge && prevEdge.node.fields.slug,
        tableOfContents: postsEdges
      }
    });
  });
};
