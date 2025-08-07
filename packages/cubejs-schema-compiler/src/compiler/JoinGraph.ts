import R from 'ramda';
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

  private builtJoins: Record<string, FinishedJoinTree>;

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

  public compile(cubes: unknown, errorReporter: ErrorReporter): void {
    this.edges = R.compose<
        Array<CubeDefinition>,
        Array<CubeDefinition>,
        Array<[string, JoinEdge][]>,
        Array<[string, JoinEdge]>,
        Record<string, JoinEdge>
    >(
      R.fromPairs,
      R.unnest,
      R.map((v: CubeDefinition): [string, JoinEdge][] => this.buildJoinEdges(v, errorReporter.inContext(`${v.name} cube`))),
      R.filter(this.cubeValidator.isCubeValid.bind(this.cubeValidator))
    )(this.cubeEvaluator.cubeList);

    // This requires @types/ramda@0.29 or newer
    // @ts-ignore
    this.nodes = R.compose<
        Record<string, JoinEdge>,
        Array<[string, JoinEdge]>,
        Array<JoinEdge>,
        Record<string, Array<JoinEdge> | undefined>,
        Record<string, Record<string, 1>>
    >(
      // This requires @types/ramda@0.29 or newer
      // @ts-ignore
      R.map(groupedByFrom => R.fromPairs(groupedByFrom.map(join => [join.to, 1]))),
      R.groupBy((join: JoinEdge) => join.from),
      R.map(v => v[1]),
      R.toPairs
    // @ts-ignore
    )(this.edges);

    // @ts-ignore
    this.undirectedNodes = R.compose(
      // @ts-ignore
      R.map(groupedByFrom => R.fromPairs(groupedByFrom.map(join => [join.from, 1]))),
      // @ts-ignore
      R.groupBy(join => join.to),
      R.unnest,
      // @ts-ignore
      R.map(v => [v[1], { from: v[1].to, to: v[1].from }]),
      R.toPairs
    // @ts-ignore
    )(this.edges);

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

  protected buildJoinNode(cube: CubeDefinition): Record<string, 1> {
    if (!cube.joins) {
      return {};
    }

    return cube.joins.reduce((acc, join) => {
      acc[join.name] = 1;
      return acc;
    }, {} as Record<string, 1>);
  }

  public buildJoin(cubesToJoin: JoinHints): FinishedJoinTree | null {
    if (!cubesToJoin.length) {
      return null;
    }
    const key = JSON.stringify(cubesToJoin);
    if (!this.builtJoins[key]) {
      const join = R.pipe<
          JoinHints,
          Array<JoinTree | null>,
          Array<JoinTree>,
          Array<JoinTree>
      >(
        R.map(
          (cube: JoinHint): JoinTree | null => this.buildJoinTreeForRoot(cube, R.without([cube], cubesToJoin))
        ),
        // @ts-ignore
        R.filter(R.identity),
        R.sortBy((joinTree: JoinTree) => joinTree.joins.length)
      // @ts-ignore
      )(cubesToJoin)[0];

      if (!join) {
        throw new UserError(`Can't find join path to join ${cubesToJoin.map(v => `'${v}'`).join(', ')}`);
      }

      this.builtJoins[key] = Object.assign(join, {
        multiplicationFactor: R.compose<
          JoinHints,
          Array<[string, boolean]>,
          Record<string, boolean>
        >(
          R.fromPairs,
          R.map(v => [this.cubeFromPath(v), this.findMultiplicationFactorFor(this.cubeFromPath(v), join.joins)])
        )(cubesToJoin)
      });
    }
    return this.builtJoins[key];
  }

  protected cubeFromPath(cubePath) {
    if (Array.isArray(cubePath)) {
      return cubePath[cubePath.length - 1];
    }
    return cubePath;
  }

  protected buildJoinTreeForRoot(root: JoinHint, cubesToJoin: JoinHints): JoinTree | null {
    const self = this;

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
    const nodesJoined = {};
    const result = cubesToJoin.map(joinHints => {
      if (!Array.isArray(joinHints)) {
        joinHints = [joinHints];
      }
      let prevNode = root;
      return joinHints.filter(toJoin => toJoin !== prevNode).map(toJoin => {
        if (nodesJoined[toJoin]) {
          prevNode = toJoin;
          return { joins: [] };
        }

        const path = graph.path(prevNode, toJoin);
        if (!path) {
          return null;
        }
        if (!Array.isArray(path)) {
          // Unexpected object return from graph, it should do so only when path cost was requested
          return null;
        }

        const foundJoins = self.joinsByPath(path);
        prevNode = toJoin;
        nodesJoined[toJoin] = true;
        return { cubes: path, joins: foundJoins };
      });
    }).reduce((a, b) => a.concat(b), [])
      // @ts-ignore
      .reduce((joined, res) => {
        if (!res || !joined) {
          return null;
        }
        const indexedPairs = R.compose<
          Array<JoinEdge>,
          Array<[number, JoinEdge]>
        >(
          R.addIndex(R.map)((j, i) => [i + joined.joins.length, j])
        );
        return {
          joins: [...joined.joins, ...indexedPairs(res.joins)],
        };
      }, { joins: [] });

    if (!result) {
      return null;
    }

    const pairsSortedByIndex: (joins: [number, JoinEdge][]) => JoinEdge[] =
      R.compose<
        Array<[number, JoinEdge]>,
        Array<[number, JoinEdge]>,
        Array<JoinEdge>,
        Array<JoinEdge>
      >(
        R.uniq,
        R.map(([_, join]: [number, JoinEdge]) => join),
        R.sortBy(([index]: [number, JoinEdge]) => index)
      );
    return {
      // @ts-ignore
      joins: pairsSortedByIndex(result.joins),
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
    return R.range(0, path.length - 1).map(i => this.edges[`${path[i]}-${path[i + 1]}`]);
  }

  public connectedComponents(): Record<string, number> {
    if (!this.cachedConnectedComponents) {
      let componentId = 1;
      const components = {};
      R.toPairs(this.nodes).map(nameToConnection => nameToConnection[0]).forEach(node => {
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
      R.toPairs(this.undirectedNodes[node])
        .map(connectedNodeNames => connectedNodeNames[0])
        .forEach(connectedNode => {
          this.findConnectedComponent(componentId, connectedNode, components);
        });
    }
  }
}
