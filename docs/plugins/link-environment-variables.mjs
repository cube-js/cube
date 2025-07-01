import { visit } from "unist-util-visit";

const isString = (value) => typeof value === "string";

const isCubeEnvVar = (value) => {
  const prefixes = [
    "CUBEJS_",
    "CUBESTORE_",
    "CUBESQL_",
  ]

  return (
    !value.includes("=") &&
    !value.endsWith("_*") &&
    prefixes.some(prefix => value.startsWith(prefix))
  );
};

const getAllIndexesByType = (arr, type) => {
  var indexes = [],
    i;
  for (i = 0; i < arr.length; i++) {
    if (arr[i].type === type) {
      indexes.push(i);
    }
  }
  return indexes;
};

export default function retextSentenceSpacing() {
  return (tree) => {
    visit(tree, ["paragraph", "tableCell"], (node) => {
      const indexes = getAllIndexesByType(node.children, "inlineCode");

      for (const index of indexes) {
        const currentNode = node.children[index];
        const value = currentNode.value;

        if (value && isString(value) && isCubeEnvVar(value)) {
          const newNode = {
            children: [currentNode],
            title: null,
            type: "link",
            url: `/product/configuration/reference/environment-variables#${value.toLowerCase()}`
          };

          node.children.splice(index, 1, newNode);
        }
      }
    });

    return tree;
  };
}
