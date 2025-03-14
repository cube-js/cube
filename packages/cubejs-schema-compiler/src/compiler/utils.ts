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

export function topologicalSort(edges: GraphEdge[]): CubeDef[] {
  const graph = new Map();
  const outDegree = new Map();

  for (const [from, to] of edges) {
    if (!graph.has(from.name)) {
      graph.set(from.name, { cubeDef: from.cubeDef, from: [] });
      outDegree.set(from.name, 0);
    } else {
      const n = graph.get(from.name);
      if (!n.cubeDef) {
        n.cubeDef = from.cubeDef;
      }
    }

    if (to) {
      if (!graph.has(to.name)) {
        graph.set(to.name, { from: [from.name] });
        outDegree.set(to.name, 0);
      } else {
        const n = graph.get(to.name);
        n.from.push(from.name);
      }

      outDegree.set(from.name, (outDegree.get(from.name) || 0) + 1);
    }
  }

  const queue: string[] = [...outDegree.entries()].filter(([_, deg]) => deg === 0).map(([name]) => name);

  const sorted: CubeDef[] = [];

  while (queue.length) {
    const nodeName = queue.shift();
    if (nodeName === undefined) {
      break;
    }

    const node = graph.get(nodeName);

    sorted.push(node.cubeDef);

    for (const neighbor of node.from) {
      outDegree.set(neighbor, outDegree.get(neighbor) - 1);
      if (outDegree.get(neighbor) === 0) {
        queue.push(neighbor);
      }
    }
  }

  if (sorted.length !== graph.size) {
    const remainingNodes = [...graph.keys()].filter(node => !sorted.includes(node));
    throw new Error(`Cyclical dependence detected! Potential problems with ${remainingNodes.join(', ')}.`);
  }

  return sorted;
}
