import camelCase from 'camelcase';
import inflection from 'inflection';
import R from 'ramda';

import { BaseMeasure, BaseQuery } from '../adapter';
import { UserError } from './UserError';

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

  /**
   * @protected
   */
  assignSplitJoins() {
    const joinPathsBySplitId = {};
    for (const entity of this.cubeSymbols.cubeList) {
      if (entity.splitId) {
        joinPathsBySplitId[entity.splitId] = [...(joinPathsBySplitId[entity.splitId] || []), entity.fullPath];
      }
    }
    
    const foundJoins = {};

    for (const [splitId, joinPaths] of Object.entries(joinPathsBySplitId)) {
      for (const [index, joinPath] of joinPaths.entries()) {
        let j = index + 1;
        while (j < joinPaths.length) {
          const pair = ([joinPath, joinPaths[j]].map((p) => (p.split('.').length > 1 ? p.split('.') : p)));
          
          try {
            const join = this.joinGraph.buildJoin(pair);
            
            const { relationship } = join.joins[0].join;
            
            if (!foundJoins[splitId]) {
              foundJoins[splitId] = [];
            }
            
            // We only need to know if 2 split views are joinable,
            // actual joins are defined by cubes' `joins` field
            let to = joinPath;
            if (to === join.root) {
              to = joinPaths[j];
            }
            
            foundJoins[splitId].push({
              pair,
              // where to put this join
              fullPath: join.root,
              // joinpath on splitId
              to,
              relationship,
              actualJoin: join
            });
          } catch (_) {
          //
          }

          j++;
        }
      }
    }
    
    const { cubeList } = this.cubeSymbols;
          
    for (const [splitId, joins] of Object.entries(foundJoins)) {
      for (const join of joins) {
        const root = cubeList.find((cl) => cl.splitId === splitId && cl.fullPath === join.fullPath);
        const to = cubeList.find((cl) => cl.splitId === splitId && cl.fullPath === join.to);

        if (!root.splitJoins) {
          root.splitJoins = [];
        }
        root.splitJoins.push({
          relationship: join.relationship,
          to: to.name
        });
      }
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
