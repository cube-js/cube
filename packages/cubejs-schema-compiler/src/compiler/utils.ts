import { camelize } from 'inflection';
import { CubeDef, GraphEdge } from './CubeSymbols';

// It's a map where key - is a level and value - is a map of properties on this level to ignore camelization
const IGNORE_CAMELIZE = {
  1: {
    granularities: true,
  }
};

function camelizeObjectPart(obj: unknown, camelizeKeys: boolean, level = 0): unknown {
  if (!obj) {
    return obj;
  }

  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      obj[i] = camelizeObjectPart(obj[i], true, level + 1);
    }
  } else if (typeof obj === 'object') {
    for (const key of Object.keys(obj)) {
      if (!(level === 1 && key === 'meta')) {
        obj[key] = camelizeObjectPart(obj[key], !IGNORE_CAMELIZE[level]?.[key], level + 1);
      }

      if (camelizeKeys) {
        const camelizedKey = camelize(key, true);
        if (camelizedKey !== key) {
          obj[camelizedKey] = obj[key];
          delete obj[key];
        }
      }
    }
  }

  return obj;
}

export function camelizeCube(cube: any): unknown {
  for (const key of Object.keys(cube)) {
    const camelizedKey = camelize(key, true);
    if (camelizedKey !== key) {
      cube[camelizedKey] = cube[key];
      delete cube[key];
    }
  }

  camelizeObjectPart(cube.measures, false);
  camelizeObjectPart(cube.dimensions, false);
  camelizeObjectPart(cube.preAggregations, false);
  camelizeObjectPart(cube.cubes, false);
  camelizeObjectPart(cube.accessPolicy, false);

  return cube;
}

/**
 * This is a simple cube-views topological sorting based on Kahn's algorythm.
 */
export function topologicalSort([nodes, edges]: [Map<string, CubeDef>, GraphEdge[]]): CubeDef[] {
  const inDegree = new Map<string, Set<string>>();
  const outDegree = new Map<string, number>();

  nodes.forEach(node => {
    outDegree.set(node.name, 0);
    inDegree.set(node.name, new Set());
  });

  for (const [from, to] of edges) {
    const n = inDegree.get(to) || new Set();
    n.add(from);
    outDegree.set(from, (outDegree.get(from) ?? 0) + 1);
  }

  const queue: string[] = [...outDegree.entries()].filter(([_, deg]) => deg === 0).map(([name]) => name);

  const sorted: CubeDef[] = [];

  while (queue.length) {
    const nodeName = queue.shift();
    if (nodeName === undefined) {
      break;
    }

    const from = inDegree.get(nodeName) || new Set();

    sorted.push(nodes.get(nodeName));

    for (const neighbor of from) {
      outDegree.set(neighbor, (outDegree.get(neighbor) || 1) - 1);
      if (outDegree.get(neighbor) === 0) {
        queue.push(neighbor);
      }
    }
  }

  if (sorted.length !== nodes.size) {
    const remainingNodes = [...nodes.keys()].filter(node => !sorted.includes(node));
    throw new Error(`Cyclical dependence detected! Potential problems with ${remainingNodes.join(', ')}.`);
  }

  return sorted;
}

export function findCyclesInGraph(adjacencyList: Map<string, Set<string>>): string[][] {
  const visited = new Set<string>();
  const stack = new Set<string>();
  const cycles: string[][] = [];

  const dfs = (node: string, path: string[]) => {
    if (stack.has(node)) {
      const cycleStart = path.indexOf(node);
      cycles.push(path.slice(cycleStart));
      return;
    }
    if (visited.has(node)) return;

    visited.add(node);
    stack.add(node);
    path.push(node);

    for (const neighbor of adjacencyList.get(node) ?? []) {
      dfs(neighbor, [...path]);
    }

    stack.delete(node);
  };

  for (const node of adjacencyList.keys()) {
    if (!visited.has(node)) {
      dfs(node, []);
    }
  }

  return cycles;
}
