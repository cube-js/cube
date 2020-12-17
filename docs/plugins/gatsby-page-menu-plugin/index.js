module.exports = ({ actions, markdownAST, markdownNode }, pluginOptions) => {
  const { createNodeField } = actions;

  createNodeField({
    name: 'foo',
    node: markdownNode,
    value: 'bar',
  });

  return markdownAST;
};
