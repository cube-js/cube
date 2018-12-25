const R = require('ramda');
const Graph = require('node-dijkstra');

const UserError = require('./UserError');

class JoinGraph {
  constructor(cubeValidator, cubeEvaluator) {
    this.cubeValidator = cubeValidator;
    this.cubeEvaluator = cubeEvaluator;
    this.nodes = {};
    this.edges = {};
  }

  compile(cubes, errorReporter) {
    this.edges = R.compose(
      R.fromPairs,
      R.unnest,
      R.map(v => this.buildJoinEdges(v, errorReporter.inContext(`${v.name} cube`))),
      R.filter(this.cubeValidator.isCubeValid.bind(this.cubeValidator))
    )(this.cubeEvaluator.cubeList);
    this.nodes = R.compose(
      R.map(groupedByFrom => R.fromPairs(groupedByFrom.map(join => [join.to, 1]))),
      R.groupBy(join => join.from),
      R.map(v => v[1]),
      R.toPairs
    )(this.edges);
    this.undirectedNodes = R.compose(
      R.map(groupedByFrom => R.fromPairs(groupedByFrom.map(join => [join.from, 1]))),
      R.groupBy(join => join.to),
      R.unnest,
      R.map(v => [v[1], { from: v[1].to, to: v[1].from }]),
      R.toPairs
    )(this.edges);
    this.graph = new Graph(this.nodes);
  }

  buildJoinEdges(cube, errorReporter) {
    return R.compose(
      R.filter(R.identity),
      R.map(join => {
        const multipliedMeasures = R.compose(
          R.filter(m =>
            m.sql && this.cubeEvaluator.funcArguments(m.sql).length === 0 && m.sql() === 'count(*)' ||
            ['sum', 'avg', 'count', 'number'].indexOf(m.type) !== -1
          ),
          R.values
        );
        const joinRequired = (v) =>
          `primary key for '${v}' is required when join is defined in order to make aggregates work properly`;
        if (
          !this.cubeEvaluator.primaryKeys[join[1].from] &&
          multipliedMeasures(this.cubeEvaluator.measuresForCube(join[1].from)).length > 0
        ) {
          errorReporter.error(joinRequired(join[1].from));
          return null;
        }
        if (!this.cubeEvaluator.primaryKeys[join[1].to] &&
          multipliedMeasures(this.cubeEvaluator.measuresForCube(join[1].to)).length > 0) {
          errorReporter.error(joinRequired(join[1].to));
          return null;
        }
        return join;
      }),
      R.unnest,
      R.map(join => [
        [`${cube.name}-${join[0]}`, {
          join: join[1],
          from: cube.name,
          to: join[0],
          originalFrom: cube.name,
          originalTo: join[0]
        }]
      ]),
      R.filter(R.identity),
      R.map(join => {
        if (!this.cubeEvaluator.cubeExists(join[0])) {
          errorReporter.error(`Cube ${join[0]} doesn't exist`);
          return undefined;
        }
        return join;
      }),
      R.toPairs
    )(cube.joins || {});
  }

  buildJoinNode(cube) {
    return R.compose(
      R.fromPairs,
      R.map(v => [v[0], 1]),
      R.toPairs
    )(cube.joins || {});
  }

  buildJoin(cubesToJoin) {
    if (!cubesToJoin.length) {
      return null;
    }
    const join = R.pipe(
      R.map(cube =>
        this.buildJoinTreeForRoot(cube, R.without([cube], cubesToJoin))
      ),
      R.filter(R.identity),
      R.sortBy(joinTree => joinTree.joins.length)
    )(cubesToJoin)[0];
    if (!join) {
      throw new UserError(`Can't find join path to join ${cubesToJoin.map(v => `'${v}'`).join(', ')}`);
    }
    return Object.assign(join, {
      multiplicationFactor: R.compose(
        R.fromPairs,
        R.map(v => [v, this.findMultiplicationFactorFor(v, join.joins)])
      )(cubesToJoin)
    });
  }

  buildJoinTreeForRoot(root, cubesToJoin) {
    const self = this;
    const result = cubesToJoin.map(toJoin => {
      const path = this.graph.path(root, toJoin);
      if (!path) {
        return null;
      }
      const foundJoins = self.joinsByPath(path);
      return { cubes: path, joins: foundJoins };
    }).reduce((joined, res) => {
      if (!res || !joined) {
        return null;
      }
      const indexedPairs = R.compose(
        R.addIndex(R.map)((j, i) => [i, j])
      );
      return {
        joins: joined.joins.concat(indexedPairs(res.joins))
      };
    }, { joins: [] });

    if (!result) {
      return null;
    }

    const pairsSortedByIndex =
      R.compose(R.uniq, R.map(indexToJoin => indexToJoin[1]), R.sortBy(indexToJoin => indexToJoin[0]));
    return {
      joins: pairsSortedByIndex(result.joins),
      root
    };
  }

  findMultiplicationFactorFor(cube, joins) {
    const visited = {};
    const self = this;
    function findIfMultipliedRecursive(currentCube) {
      if (visited[currentCube]) {
        return false;
      }
      visited[currentCube] = true;
      function nextNode(nextJoin) {
        return nextJoin.from === currentCube ? nextJoin.to : nextJoin.from;
      }
      const nextJoins = joins.filter(j => j.from === currentCube || j.to === currentCube);
      if (nextJoins.find(nextJoin =>
        self.checkIfCubeMultiplied(currentCube, nextJoin) && !visited[nextNode(nextJoin)]
        )) {
        return true;
      }
      return !!nextJoins.find(nextJoin =>
        findIfMultipliedRecursive(nextNode(nextJoin))
      );
    }
    return findIfMultipliedRecursive(cube);
  }

  checkIfCubeMultiplied(cube, join) {
    return join.from === cube && join.join.relationship === 'hasMany' ||
      join.to === cube && join.join.relationship === 'belongsTo';
  }

  joinsByPath(path) {
    return R.range(0, path.length - 1).map(i => this.edges[`${path[i]}-${path[i + 1]}`]);
  }

  connectedComponents() {
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

  findConnectedComponent(componentId, node, components) {
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

module.exports = JoinGraph;