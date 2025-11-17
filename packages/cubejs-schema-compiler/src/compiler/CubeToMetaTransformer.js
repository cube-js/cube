import inflection from 'inflection';
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
    const cubeName = cube.name;
    const cubeTitle = cube.title || this.titleize(cubeName);

    const isCubeVisible = this.isVisible(cube, true);

    const flatFolderSeparator = getEnv('nestedFoldersDelimiter');
    const flatFolders = [];

    const processFolder = (folder, path = [], mergedMembers = []) => {
      const flatMembers = [];
      const nestedMembers = folder.includes.map(member => {
        if (member.type === 'folder') {
          return processFolder(member, [...path, folder.name], flatMembers);
        }
        const memberName = `${cubeName}.${member.name}`;
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
        name: cubeName,
        type: cube.isView ? 'view' : 'cube',
        title: cubeTitle,
        isVisible: isCubeVisible,
        public: isCubeVisible,
        description: cube.description,
        connectedComponent: this.joinGraph.connectedComponents()[cubeName],
        meta: cube.meta,
        measures: Object.entries(cube.measures || {}).map((nameToMetric) => {
          const metricDef = nameToMetric[1];
          const measureVisibility = isCubeVisible ? this.isVisible(metricDef, true) : false;
          return {
            ...this.measureConfig(cubeName, cubeTitle, nameToMetric),
            isVisible: measureVisibility,
            public: measureVisibility,
          };
        }),
        dimensions: Object.entries(cube.dimensions || {}).map((nameToDimension) => {
          const [dimensionName, dimDef] = nameToDimension;
          const dimensionVisibility = isCubeVisible
            ? this.isVisible(dimDef, !dimDef.primaryKey)
            : false;
          const granularitiesObj = dimDef.granularities;

          return {
            name: `${cubeName}.${dimensionName}`,
            title: this.title(cubeTitle, nameToDimension),
            type: this.dimensionDataType(dimDef.type),
            description: dimDef.description,
            shortTitle: this.title(cubeTitle, nameToDimension, true),
            suggestFilterValues:
              dimDef.suggestFilterValues == null
                ? true
                : dimDef.suggestFilterValues,
            format: dimDef.format,
            meta: dimDef.meta,
            isVisible: dimensionVisibility,
            public: dimensionVisibility,
            primaryKey: !!dimDef.primaryKey,
            aliasMember: dimDef.aliasMember,
            granularities:
              granularitiesObj
                ? Object.entries(granularitiesObj).map(([gName, gDef]) => ({
                  name: gName,
                  title: this.title(cubeTitle, [gName, gDef], true),
                  interval: gDef.interval,
                  offset: gDef.offset,
                  origin: gDef.origin,
                }))
                : undefined,
          };
        }),
        segments: Object.entries(cube.segments || {}).map((nameToSegment) => {
          const [segmentName, segmentDef] = nameToSegment;
          const segmentVisibility = isCubeVisible ? this.isVisible(segmentDef, true) : false;
          return {
            name: `${cubeName}.${segmentName}`,
            title: this.title(cubeTitle, nameToSegment),
            shortTitle: this.title(cubeTitle, nameToSegment, true),
            description: segmentDef.description,
            meta: segmentDef.meta,
            isVisible: segmentVisibility,
            public: segmentVisibility,
          };
        }),
        hierarchies: (cube.evaluatedHierarchies || []).map((it) => ({
          ...it,
          public: it.public ?? true,
          name: `${cubeName}.${it.name}`,
        })),
        folders: flatFolders,
        nestedFolders,
      },
    };
  }

  queriesForContext(contextId) {
    // return All queries if no context pass
    if (contextId == null || contextId.length === 0) {
      return this.queries;
    }

    const context = this.contextEvaluator.contextDefinitions[contextId];

    // If contextId is wrong
    if (context == null) {
      throw new UserError(`Context ${contextId} doesn't exist`);
    }

    // As for now context works on the cubes level
    return this.queries.filter(
      (query) => context.contextMembers.includes(query.config.name)
    );
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

  dimensionDataType(dimensionType) {
    return dimensionType === 'switch' ? 'string' : dimensionType;
  }

  measureConfig(cubeName, cubeTitle, nameToMetric) {
    const [metricName, metricDef] = nameToMetric;
    const name = `${cubeName}.${metricName}`;

    // Support both old 'drillMemberReferences' and new 'drillMembers' keys
    const drillMembers = metricDef.drillMembers || metricDef.drillMemberReferences;

    const drillMembersArray = (drillMembers && this.cubeEvaluator.evaluateReferences(
      cubeName, drillMembers, { originalSorting: true }
    )) || [];

    const type = CubeSymbols.toMemberDataType(metricDef.type);
    const isCumulative = metricDef.cumulative || BaseMeasure.isCumulative(metricDef);

    const drillMembersGrouped = { measures: [], dimensions: [] };
    for (const member of drillMembersArray) {
      if (this.cubeEvaluator.isMeasure(member)) {
        drillMembersGrouped.measures.push(member);
      } else if (this.cubeEvaluator.isDimension(member)) {
        drillMembersGrouped.dimensions.push(member);
      }
    }

    return {
      name,
      title: this.title(cubeTitle, nameToMetric),
      description: metricDef.description,
      shortTitle: this.title(cubeTitle, nameToMetric, true),
      format: metricDef.format,
      cumulativeTotal: isCumulative,
      cumulative: isCumulative,
      type,
      aggType: metricDef.aggType || metricDef.type,
      drillMembers: drillMembersArray,
      drillMembersGrouped,
      aliasMember: metricDef.aliasMember,
      meta: metricDef.meta
    };
  }

  title(cubeTitle, nameToDef, short) {
    const prefix = short ? '' : `${cubeTitle} `;
    const suffix = nameToDef[1].title || this.titleize(nameToDef[0]);
    return `${prefix}${suffix}`;
  }

  titleize(name) {
    return inflection.titleize(inflection.underscore(camelCase(name, { pascalCase: true })));
  }
}
