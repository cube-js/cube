import camelCase from 'camelcase';
import inflection from 'inflection';
import R from 'ramda';

import { BaseMeasure, BaseQuery } from '../adapter';
import { UserError } from './UserError';

const deprectedRelationships = {
  hasOne: 'one_to_one',
  hasMany: 'one_to_many',
  belongsTo: 'many_to_one',
};

export class CubeToMetaTransformer {
  /**
   * @param {import('./CubeValidator').CubeValidator} cubeValidator
   * @param {import('./CubeEvaluator').CubeEvaluator} cubeEvaluator
   * @param {import('./ContextEvaluator').ContextEvaluator} contextEvaluator
   * @param {import('./JoinGraph').JoinGraph} joinGraph
   */
  constructor(cubeValidator, cubeEvaluator, contextEvaluator, joinGraph) {
    this.cubeValidator = cubeValidator;
    this.cubeSymbols = cubeEvaluator;
    this.cubeEvaluator = cubeEvaluator;
    this.contextEvaluator = contextEvaluator;
    this.joinGraph = joinGraph;
    this.cubes = [];
    this.splitJoins = {};
  }

  compile(cubes, errorReporter) {
    this.assignSplitJoins();

    this.cubes = this.cubeSymbols.cubeList
      .filter(this.cubeValidator.isCubeValid.bind(this.cubeValidator))
      .map((v) => this.transform(v, errorReporter.inContext(`${v.name} cube`)))
      .filter(Boolean);

    /**
     * @deprecated
     * @protected
     */
    this.queries = this.cubes;
  }

  findSplitViewJoins(baseSplitView) {
    const { undirectedNodes } = this.joinGraph;

    const viewName = baseSplitView.name;
    
    // non-split nodes
    const baseNodes = baseSplitView.cubes
      .filter((c) => !c.split)
      .map((c) => c.fullPath);
    const splitNodes = baseSplitView.cubes
      .filter((c) => c.split)
      .map((c) => c.fullPath);

    const cubeToSplitNodePath = Object.fromEntries(splitNodes
      .map((c) => {
        const paths = c.split('.');
        return [paths[paths.length - 1], c];
      }));

    const isBaseNode = (node) => baseNodes.includes(node);
    const isSplitNode = (node) => cubeToSplitNodePath[node] !== undefined;

    // baseNodeJoin - join betwen all non-split nodes
    const baseNodesJoin = this.joinGraph.buildJoin(baseNodes);
    const isBaseNodeJoinMultiplied = baseNodesJoin.joins.some(({ join }) => {
      const relationship = deprectedRelationships[join.relationship] || join.relationship;
      return relationship === 'one_to_many' || relationship === 'many_to_one';
    });

    const builtJoin = this.joinGraph.buildJoin(baseNodes.concat(splitNodes));

    const nodesToVisit = new Set(baseNodes);

    builtJoin.joins.forEach((j) => {
      nodesToVisit.add(j.from);
      nodesToVisit.add(j.to);
    });

    function findJoin(node1, node2) {
      const currentJoin = builtJoin.joins.find((j) => {
        if (j.from === node1 && j.to === node2) {
          return j;
        }
        if (j.from === node2 && j.to === node1) {
          return j;
        }

        return undefined;
      });

      if (!currentJoin) {
        throw Error(`No relationship between ${node1} and ${node2} found`);
      }

      return currentJoin;
    }

    const splitJoins = {};
    const visited = new Set();
    const queue = [{
      node: baseNodes[0],
      parent: null,
      route: []
    }];
    while (queue.length > 0) {
      const { node, parent, route } = queue.shift();
      
      if (!visited.has(node)) {
        if (isSplitNode(node)) {
          const currentJoin = findJoin(parent, node);
          
          const routeToCalculateMultiplication = [];
          let closestNode = null;
          // some other node defines this join, we need to find the node
          let i = route.length - 1;
          while (i >= 0 && !closestNode) {
            if (isBaseNode(route[i])) {
              closestNode = viewName;
            } else if (isSplitNode(route[i])) {
              closestNode = route[i];
            }
            routeToCalculateMultiplication.push(route[i]);
            i--;
          }
          
          const isRouteMultiplied = (() => {
            if (routeToCalculateMultiplication.length === 1) {
              return false;
            }

            const edges = [];
            for (let j = 0; j < routeToCalculateMultiplication.length - 1; j++) {
              const edge = [routeToCalculateMultiplication[j], routeToCalculateMultiplication[j + 1]];
              edges.push(edge);
            }

            return edges.some(([from, to]) => {
              const { join } = findJoin(from, to);

              const relationship = deprectedRelationships[join.relationship] || join.relationship;
              return relationship === 'one_to_many' || relationship === 'many_to_one';
            });
          })();

          if (!closestNode) {
            throw Error(`No closest node found for ${node}`);
          }

          const relationship = (isMultiplied) => {
            const _relationship = deprectedRelationships[currentJoin.join.relationship] || currentJoin.join.relationship;

            if (isMultiplied) {
              if (currentJoin.from === node) {
                if (_relationship === 'many_to_one') {
                  return 'many_to_many';
                }
                
                if (_relationship === 'one_to_one') {
                  return 'one_to_many';
                }
              } else {
                if (_relationship === 'one_to_many') {
                  return 'many_to_many';
                }
                
                if (_relationship === 'one_to_one') {
                  return 'many_to_one';
                }
              }
            }

            return _relationship;
          };

          const splitJoin = (() => {
            if (currentJoin.from === node) {
              const multiplied = closestNode === viewName ? (isRouteMultiplied || isBaseNodeJoinMultiplied) : isRouteMultiplied;
              return {
                node,
                relationship: relationship(multiplied),
                to: closestNode,
              };
            } else if (closestNode === viewName) {
              return {
                node: viewName,
                relationship: relationship(isBaseNodeJoinMultiplied),
                to: node,
              };
            } else {
              return {
                node: closestNode,
                relationship: relationship(isRouteMultiplied),
                to: node,
              };
            }
          })();
          
          splitJoins[splitJoin.node] = [
            ...(splitJoins[splitJoin.node] || []),
            {
              to: splitJoin.to,
              relationship: splitJoin.relationship,
            }
          ];
        }
        visited.add(node);
        queue.push(
          ...Object.keys(undirectedNodes[node] || {}).map(neighbor => ({ node: neighbor, parent: node, route: [...route, node] }))
        );
      }
    }

    return splitJoins;
  }

  /**
   * @protected
   */
  assignSplitJoins() {
    const baseSplitViews = this.cubeSymbols.cubeList.filter(
      (c) => c.isView && !c.isSplitView && c.splitId
    );
    
    for (const splitView of baseSplitViews) {
      let join = null;
      try {
        join = this.findSplitViewJoins(splitView);
      } catch (e) {
        console.log('Failed to find split view joins', e.message);
      }

      this.splitJoins[splitView.name] = join;
    }
  }
  
  /**
   * @protected
   */
  transform(cube) {
    const cubeTitle = cube.title || this.titleize(cube.name);
    
    const isCubeVisible = this.isVisible(cube, true);

    return {
      config: {
        name: cube.name,
        type: cube.isView ? 'view' : 'cube',
        splitId: cube.splitId || undefined,
        splitJoins: cube.splitJoins,
        title: cubeTitle,
        isVisible: isCubeVisible,
        public: isCubeVisible,
        description: cube.description,
        connectedComponent: this.joinGraph.connectedComponents()[cube.name],
        meta: cube.meta,
        measures: R.compose(
          R.map((nameToMetric) => ({
            ...this.measureConfig(cube.name, cubeTitle, nameToMetric),
            isVisible: isCubeVisible ? this.isVisible(nameToMetric[1], true) : false,
            public: isCubeVisible ? this.isVisible(nameToMetric[1], true) : false,
          })),
          R.toPairs
        )(cube.measures || {}),
        dimensions: R.compose(
          R.map((nameToDimension) => ({
            name: `${cube.name}.${nameToDimension[0]}`,
            title: this.title(cubeTitle, nameToDimension),
            type: nameToDimension[1].type,
            description: nameToDimension[1].description,
            shortTitle: this.title(cubeTitle, nameToDimension, true),
            suggestFilterValues:
              nameToDimension[1].suggestFilterValues == null
                ? true
                : nameToDimension[1].suggestFilterValues,
            format: nameToDimension[1].format,
            meta: nameToDimension[1].meta,
            isVisible: isCubeVisible
              ? this.isVisible(nameToDimension[1], !nameToDimension[1].primaryKey)
              : false,
            public: isCubeVisible
              ? this.isVisible(nameToDimension[1], !nameToDimension[1].primaryKey)
              : false,
            primaryKey: !!nameToDimension[1].primaryKey,
          })),
          R.toPairs
        )(cube.dimensions || {}),
        segments: R.compose(
          R.map((nameToSegment) => ({
            name: `${cube.name}.${nameToSegment[0]}`,
            title: this.title(cubeTitle, nameToSegment),
            shortTitle: this.title(cubeTitle, nameToSegment, true),
            description: nameToSegment[1].description,
            meta: nameToSegment[1].meta,
            isVisible: isCubeVisible ? this.isVisible(nameToSegment[1], true) : false,
            public: isCubeVisible ? this.isVisible(nameToSegment[1], true) : false,
          })),
          R.toPairs
        )(cube.segments || {}),
      },
    };
  }

  queriesForContext(contextId) {
    // return All queries if no context pass
    if (R.isNil(contextId) || R.isEmpty(contextId)) {
      return this.queries;
    }

    const context = this.contextEvaluator.contextDefinitions[contextId];

    // If contextId is wrong
    if (R.isNil(context)) {
      throw new UserError(`Context ${contextId} doesn't exist`);
    }

    // As for now context works on the cubes level
    return R.filter(
      (query) => R.contains(query.config.name, context.contextMembers)
    )(this.queries);
  }

  /**
   * @protected
   */
  isVisible(symbol, defaultValue) {
    if (symbol.public != null) {
      return symbol.public;
    }

    // TODO: Deprecated, should be removed in the future
    if (symbol.visible != null) {
      return symbol.visible;
    }

    // TODO: Deprecated, should be removed in the futur
    if (symbol.shown != null) {
      return symbol.shown;
    }

    return defaultValue;
  }

  measureConfig(cubeName, cubeTitle, nameToMetric) {
    const name = `${cubeName}.${nameToMetric[0]}`;
    // Support both old 'drillMemberReferences' and new 'drillMembers' keys
    const drillMembers = nameToMetric[1].drillMembers || nameToMetric[1].drillMemberReferences;

    const drillMembersArray = (drillMembers && this.cubeEvaluator.evaluateReferences(
      cubeName, drillMembers, { originalSorting: true }
    )) || [];

    // TODO support type qualifiers on min and max
    const type = BaseQuery.isCalculatedMeasureType(nameToMetric[1].type) ? nameToMetric[1].type : 'number';

    return {
      name,
      title: this.title(cubeTitle, nameToMetric),
      description: nameToMetric[1].description,
      shortTitle: this.title(cubeTitle, nameToMetric, true),
      format: nameToMetric[1].format,
      cumulativeTotal: nameToMetric[1].cumulative || BaseMeasure.isCumulative(nameToMetric[1]),
      cumulative: nameToMetric[1].cumulative || BaseMeasure.isCumulative(nameToMetric[1]),
      type,
      aggType: nameToMetric[1].aggType || nameToMetric[1].type,
      drillMembers: drillMembersArray,
      drillMembersGrouped: {
        measures: drillMembersArray.filter((member) => this.cubeEvaluator.isMeasure(member)),
        dimensions: drillMembersArray.filter((member) => this.cubeEvaluator.isDimension(member)),
      },
      meta: nameToMetric[1].meta
    };
  }

  title(cubeTitle, nameToDef, short) {
    // eslint-disable-next-line prefer-template
    return `${short ? '' : cubeTitle + ' '}${nameToDef[1].title || this.titleize(nameToDef[0])}`;
  }

  titleize(name) {
    return inflection.titleize(inflection.underscore(camelCase(name, { pascalCase: true })));
  }
}
