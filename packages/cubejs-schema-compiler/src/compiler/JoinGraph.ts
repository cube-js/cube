import Graph from 'node-dijkstra';
import { UserError } from './UserError';

import type { CubeValidator } from './CubeValidator';
import type { CubeEvaluator, MeasureDefinition } from './CubeEvaluator';
import type { CubeDefinition, JoinDefinition } from './CubeSymbols';
import type { ErrorReporter } from './ErrorReporter';

type JoinEdge = {
  join: JoinDefinition,
  from: string,
  to: string,
  originalFrom: string,
  originalTo: string,
};

type JoinTreeJoins = JoinEdge[];

type JoinTree = {
  root: string,
  joins: JoinTreeJoins,
};

export type FinishedJoinTree = JoinTree & {
  multiplicationFactor: Record<string, boolean>,
};

export type JoinHint = string | string[];

export type JoinHints = JoinHint[];

export class JoinGraph {
  private readonly cubeValidator: CubeValidator;

  private readonly cubeEvaluator: CubeEvaluator;

  // source node -> destination node -> weight
  private nodes: Record<string, Record<string, 1>>;

  // source node -> destination node -> weight
  private undirectedNodes: Record<string, Record<string, 1>>;

  private edges: Record<string, JoinEdge>;

  private readonly builtJoins: Record<string, FinishedJoinTree>;

  private graph: Graph | null;

  private cachedConnectedComponents: Record<string, number> | null;

  public constructor(cubeValidator: CubeValidator, cubeEvaluator: CubeEvaluator) {
    this.cubeValidator = cubeValidator;
    this.cubeEvaluator = cubeEvaluator;
    this.nodes = {};
    this.undirectedNodes = {};
    this.edges = {};
    this.builtJoins = {};
    this.cachedConnectedComponents = null;
    this.graph = null;
  }

  public compile(_cubes: unknown, errorReporter: ErrorReporter): void {
    this.edges = Object.fromEntries(
      this.cubeEvaluator.cubeList
        .filter(this.cubeValidator.isCubeValid.bind(this.cubeValidator))
        .flatMap((v: CubeDefinition): [string, JoinEdge][] => this.buildJoinEdges(
          v, errorReporter.inContext(`${v.name} cube`)
        ))
    );

    const grouped: Record<string, JoinEdge[]> = {};

    for (const join of Object.values(this.edges)) {
      if (!grouped[join.from]) {
        grouped[join.from] = [];
      }
      grouped[join.from].push(join);
    }

    this.nodes = Object.fromEntries(
      Object.entries(grouped).map(([from, edges]) => [
        from,
        Object.fromEntries(edges.map((join) => [join.to, 1])),
      ])
    );

    const undirectedNodesGrouped: Record<string, JoinEdge[]> = {};

    for (const join of Object.values(this.edges)) {
      const reverseJoin: JoinEdge = {
        join: join.join,
        from: join.to,
        to: join.from,
        originalFrom: join.originalFrom,
        originalTo: join.originalTo,
      };

      for (const e of [join, reverseJoin]) {
        if (!undirectedNodesGrouped[e.to]) {
          undirectedNodesGrouped[e.to] = [];
        }
        undirectedNodesGrouped[e.to].push(e);
      }
    }

    this.undirectedNodes = Object.fromEntries(
      Object.entries(undirectedNodesGrouped).map(([to, joins]) => [
        to,
        Object.fromEntries(joins.map(join => [join.from, 1]))
      ])
    );

    this.graph = new Graph(this.nodes);
  }

  protected buildJoinEdges(cube: CubeDefinition, errorReporter: ErrorReporter): Array<[string, JoinEdge]> {
    if (!cube.joins) {
      return [];
    }

    const getMultipliedMeasures = (cubeName: string): MeasureDefinition[] => {
      const measures = this.cubeEvaluator.measuresForCube(cubeName);
      return Object.values(measures).filter((m: MeasureDefinition) => (m.sql &&
          this.cubeEvaluator.funcArguments(m.sql).length === 0 &&
          m.sql() === 'count(*)') ||
        ['sum', 'avg', 'count', 'number'].includes(m.type));
    };

    const joinRequired =
      (v) => `primary key for '${v}' is required when join is defined in order to make aggregates work properly`;

    return cube.joins
      .filter(join => {
        if (!this.cubeEvaluator.cubeExists(join.name)) {
          errorReporter.error(`Cube ${join.name} doesn't exist`);
          return false;
        }

        const fromMultipliedMeasures = getMultipliedMeasures(cube.name);
        if (!this.cubeEvaluator.primaryKeys[cube.name].length && fromMultipliedMeasures.length > 0) {
          errorReporter.error(joinRequired(cube.name));
          return false;
        }

        const toMultipliedMeasures = getMultipliedMeasures(join.name);
        if (!this.cubeEvaluator.primaryKeys[join.name].length && toMultipliedMeasures.length > 0) {
          errorReporter.error(joinRequired(join.name));
          return false;
        }

        return true;
      })
      .map(join => {
        const joinEdge: JoinEdge = {
          join,
          from: cube.name,
          to: join.name,
          originalFrom: cube.name,
          originalTo: join.name
        };

        return [`${cube.name}-${join.name}`, joinEdge] as [string, JoinEdge];
      });
  }

  public buildJoin(cubesToJoin: JoinHints): FinishedJoinTree | null {
    if (!cubesToJoin.length) {
      return null;
    }
    const key = JSON.stringify(cubesToJoin);
    if (!this.builtJoins[key]) {
      const join = cubesToJoin
        .map((cube: JoinHint): JoinTree | null => this.buildJoinTreeForRoot(cube, cubesToJoin.filter(c => c !== cube)))
        .filter((jt): jt is JoinTree => Boolean(jt))
        .sort((a, b) => a.joins.length - b.joins.length)[0];

      if (!join) {
        const errCubes = cubesToJoin.map(v => `'${v}'`).join(', ');
        throw new UserError(`Can't find join path to join ${errCubes}`);
      }

      this.builtJoins[key] = Object.assign(join, {
        multiplicationFactor: Object.fromEntries(
          cubesToJoin.map((v) => {
            const cubeName = this.cubeFromPath(v);
            const factor = this.findMultiplicationFactorFor(cubeName, join.joins);
            return [cubeName, factor];
          })
        )
      });
    }
    return this.builtJoins[key];
  }

  protected cubeFromPath(cubePath: string | string[]): string {
    if (Array.isArray(cubePath)) {
      return cubePath[cubePath.length - 1];
    }
    return cubePath;
  }

  protected buildJoinTreeForRoot(root: JoinHint, cubesToJoin: JoinHints): JoinTree | null {
    const { graph } = this;
    if (graph === null) {
      // JoinGraph was not compiled
      return null;
    }

    if (Array.isArray(root)) {
      const [newRoot, ...additionalToJoin] = root;
      if (additionalToJoin.length > 0) {
        cubesToJoin = [additionalToJoin, ...cubesToJoin];
      }
      root = newRoot;
    }

    // Flatten all target cubes from cubesToJoin hints
    const targetCubes = new Set<string>();
    for (const joinHint of cubesToJoin) {
      if (Array.isArray(joinHint)) {
        joinHint.forEach(cube => targetCubes.add(cube));
      } else {
        targetCubes.add(joinHint);
      }
    }

    // Remove root from targets if it exists
    targetCubes.delete(root);

    if (targetCubes.size === 0) {
      return { joins: [], root };
    }

    /**
     * We actually need not a list of joins between all requested nodes but
     * a minimal spanning tree that covers all the requested nodes.
     * Ideally it should be done via Steiner Tree Algorythm.
     * But Steiner Tree is an NP-hard problem. So we use a
     * Greedy algorithm with coverage tracking which is an approximate
     * method for solving Steiner tree problems that is frequently used
     * in cases when we need to connect only the necessary nodes with
     * the minimum number of edges.
     */
    const coveredNodes = new Set<string>([root]);
    const resultJoins: JoinEdge[] = [];
    const remainingTargets = new Set(targetCubes);

    while (remainingTargets.size > 0) {
      let bestPath: string[] | null = null;
      let bestTarget: string | null = null;

      // Find the shortest path from any covered node to any remaining target
      for (const coveredNode of coveredNodes) {
        for (const target of remainingTargets) {
          const path = graph.path(coveredNode, target);
          if (path && Array.isArray(path)) {
            if (bestPath === null || path.length < bestPath.length) {
              bestPath = path;
              bestTarget = target;
            }
          }
        }
      }

      if (!bestPath || !bestTarget) {
        // Cannot reach remaining targets
        return null;
      }

      // Add only the new edges from the path (skip already covered nodes)
      const pathJoins = this.joinsByPath(bestPath);
      let startIndex = 0;

      // Find the first uncovered node in the path
      for (let i = 0; i < bestPath.length; i++) {
        if (coveredNodes.has(bestPath[i])) {
          startIndex = i;
        } else {
          break;
        }
      }

      // Add edges and nodes from first uncovered node onwards
      for (let i = startIndex; i < bestPath.length - 1; i++) {
        if (!coveredNodes.has(bestPath[i + 1])) {
          resultJoins.push(pathJoins[i]);
          coveredNodes.add(bestPath[i + 1]);
        }
      }

      remainingTargets.delete(bestTarget);
    }

    return {
      joins: resultJoins,
      root
    };
  }

  protected findMultiplicationFactorFor(cube: string, joins: JoinTreeJoins): boolean {
    const visited = {};
    const self = this;
    function findIfMultipliedRecursive(currentCube: string) {
      if (visited[currentCube]) {
        return false;
      }
      visited[currentCube] = true;
      function nextNode(nextJoin: JoinEdge): string {
        return nextJoin.from === currentCube ? nextJoin.to : nextJoin.from;
      }
      const nextJoins = joins.filter(j => j.from === currentCube || j.to === currentCube);
      if (nextJoins.find(
        nextJoin => self.checkIfCubeMultiplied(currentCube, nextJoin) && !visited[nextNode(nextJoin)]
      )) {
        return true;
      }
      return !!nextJoins.find(
        nextJoin => findIfMultipliedRecursive(nextNode(nextJoin))
      );
    }
    return findIfMultipliedRecursive(cube);
  }

  protected checkIfCubeMultiplied(cube: string, join: JoinEdge): boolean {
    return join.from === cube && join.join.relationship === 'hasMany' ||
      join.to === cube && join.join.relationship === 'belongsTo';
  }

  protected joinsByPath(path: string[]): JoinEdge[] {
    const result: JoinEdge[] = [];
    for (let i = 0; i < path.length - 1; i++) {
      result.push(this.edges[`${path[i]}-${path[i + 1]}`]);
    }
    return result;
  }

  public connectedComponents(): Record<string, number> {
    if (!this.cachedConnectedComponents) {
      let componentId = 1;
      const components = {};
      Object.entries(this.nodes)
        .forEach(([node]) => {
          this.findConnectedComponent(componentId, node, components);
          componentId += 1;
        });
      this.cachedConnectedComponents = components;
    }
    return this.cachedConnectedComponents;
  }

  protected findConnectedComponent(componentId: number, node: string, components: Record<string, number>): void {
    if (!components[node]) {
      components[node] = componentId;
      Object.entries(this.undirectedNodes[node])
        .forEach(([connectedNode]) => {
          this.findConnectedComponent(componentId, connectedNode, components);
        });
    }
  }
}
