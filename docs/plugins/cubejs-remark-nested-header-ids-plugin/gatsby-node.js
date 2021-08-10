const kebabCase = require('lodash.kebabcase');
const parse = require('mdast-util-from-markdown');
const visit = require('unist-util-visit');

const createPageMenu = (markdownNode) => {
  const markdownAST = parse(markdownNode.rawMarkdownBody);
  const headingNodes = [];

  const visitHeadings = (node) => {
    if (node.depth > 1 && node.depth < 4) {
      headingNodes.push(node);
    }
  };

  visit(markdownAST, 'heading', visitHeadings);

  const pageTitle = markdownNode.frontmatter.title;
  let parentHeader = null;
  const result = [{
    depth: 1,
    id: '#top',
    title: pageTitle,
  }];

  headingNodes.forEach((header) => {
    const currentHeader = header.children[0].value;

    if (header.depth === 2) {
      parentHeader = currentHeader;
    }

    const isCurrentHeaderParent = currentHeader === parentHeader;
    const id = isCurrentHeaderParent
      ? kebabCase(parentHeader)
      : kebabCase(`${parentHeader}-${currentHeader}`);
    const title = isCurrentHeaderParent
      ? parentHeader
      : currentHeader;

    result.push({
      depth: header.depth,
      id: `#${id}`,
      title,
    });
  });

  return result;
};

// This is super inefficient because it runs every time a node is created, which
// causes a large delay in build times. Gatsby v3 seems to show all nodes
// as `SitePage` only, so we either need to:
// 1) Figure out if a `SitePage` is a `MarkdownRemark` node
// 2) Use a different Gatsby Node API that runs only once
exports.onCreateNode = async ({ actions, cache, node, getNodesByType }) => {
  const { createNodeField } = actions;

  const markdownNodes = getNodesByType('MarkdownRemark');
  // eslint-disable-next-line no-restricted-syntax
  for (const markdownNode of markdownNodes) {
    const pageMenu = createPageMenu(markdownNode);
    // console.log(markdownNode.frontmatter.title, pageMenu);
    createNodeField({
      node,
      name: 'pageMenu',
      value: pageMenu,
    });
  }
};
