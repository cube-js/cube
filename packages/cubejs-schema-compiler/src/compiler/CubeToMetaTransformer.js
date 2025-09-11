import inflection from 'inflection';
import R from 'ramda';
import camelCase from 'camelcase';

import { getEnv } from '@cubejs-backend/shared';
import { CubeSymbols } from './CubeSymbols';
import { UserError } from './UserError';
import { BaseMeasure } from '../adapter';

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
  transform(cube) {
    const cubeTitle = cube.title || this.titleize(cube.name);

    const isCubeVisible = this.isVisible(cube, true);

    const flatFolderSeparator = getEnv('nestedFoldersDelimiter');
    const flatFolders = [];

    const processFolder = (folder, path = [], mergedMembers = []) => {
      const flatMembers = [];
      const nestedMembers = folder.includes.map(member => {
        if (member.type === 'folder') {
          return processFolder(member, [...path, folder.name], flatMembers);
        }
        const memberName = `${cube.name}.${member.name}`;
        flatMembers.push(memberName);

        return memberName;
      });

      if (flatFolderSeparator !== '') {
        flatFolders.push({
          name: [...path, folder.name].join(flatFolderSeparator),
          members: flatMembers,
        });
      } else if (path.length > 0) {
        mergedMembers.push(...flatMembers);
      } else { // We're at the root level
        flatFolders.push({
          name: folder.name,
          members: [...new Set(flatMembers)],
        });
      }

      return {
        name: folder.name,
        members: nestedMembers,
      };
    };

    const nestedFolders = (cube.folders || []).map(f => processFolder(f));

    return {
      config: {
        name: cube.name,
        type: cube.isView ? 'view' : 'cube',
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
            aliasMember: nameToDimension[1].aliasMember,
            granularities:
              nameToDimension[1].granularities
                ? R.compose(R.map((g) => ({
                  name: g[0],
                  title: this.title(cubeTitle, g, true),
                  interval: g[1].interval,
                  offset: g[1].offset,
                  origin: g[1].origin,
                })), R.toPairs)(nameToDimension[1].granularities)
                : undefined,
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
        hierarchies: (cube.evaluatedHierarchies || []).map((it) => ({
          ...it,
          aliasMember: it.aliasMember,
          public: it.public ?? true,
          name: `${cube.name}.${it.name}`,
        })),
        folders: flatFolders,
        nestedFolders,
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
      (query) => R.includes(query.config.name, context.contextMembers)
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

    const type = CubeSymbols.toMemberDataType(nameToMetric[1].type);

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
      aliasMember: nameToMetric[1].aliasMember,
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
