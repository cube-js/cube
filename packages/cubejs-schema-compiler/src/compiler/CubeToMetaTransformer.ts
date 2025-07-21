import inflection from 'inflection';
import R from 'ramda';
import camelCase from 'camelcase';

import { getEnv } from '@cubejs-backend/shared';
import { CubeDefinitionExtended, CubeSymbols, FolderDefinition } from './CubeSymbols';
import { UserError } from './UserError';
import { BaseMeasure } from '../adapter';
import { JoinGraph } from './JoinGraph';
import { ContextEvaluator } from './ContextEvaluator';
import { CubeEvaluator, EvaluatedCube } from './CubeEvaluator';
import { CubeValidator } from './CubeValidator';
import { ErrorReporter } from './ErrorReporter';
import { CompilerInterface } from './PrepareCompiler';

export interface CubeTransformDefinition {
  config: {
    name: string;
    type: 'cube' | 'view';
    title: string;
    isVisible: boolean;
    public: boolean;
    description?: string;
    connectedComponent?: any;
    meta?: any;
    measures: {
      name: string;
      title: string;
      description?: string;
      type: string;
      aggType?: string;
      cumulative?: boolean;
      cumulativeTotal?: boolean;
      drillMembers?: string[];
      drillMemberReferences?: string[];
      drillMembersGrouped?: {
        [group: string]: string[];
      };
      meta?: any;
      isVisible: boolean;
      public: boolean;
    }[];
    dimensions: {
      name: string;
      title: string;
      shortTitle: string;
      description?: string;
      type: string;
      format?: string;
      meta?: any;
      isVisible: boolean;
      public: boolean;
      primaryKey?: boolean;
      aliasMember?: string;
      granularities?: {
        name: string;
        title: string;
        interval?: string;
        offset?: string;
        origin?: string;
      }[];
    }[];
    segments: {
      name: string;
      title: string;
      shortTitle: string;
      description?: string;
      meta?: any;
      isVisible: boolean;
      public: boolean;
    }[];
    hierarchies: {
      name: string;
      levels: string[];
      aliasMember?: string;
      public?: boolean;
    }[];
    folders: FolderDefinition[];
    nestedFolders: {
      name: string;
      members: (string | { name: string; members: any })[];
    }[];
  };
}

export class CubeToMetaTransformer implements CompilerInterface {
  private cubeValidator: CubeValidator;

  private cubeSymbols: CubeEvaluator;

  /**
   * Is public just because is used in tests. Should be private.
   */
  public cubeEvaluator: CubeEvaluator;

  private contextEvaluator: ContextEvaluator;

  private joinGraph: JoinGraph;

  public cubes: CubeTransformDefinition[];

  /**
   * @deprecated
   */
  protected queries: CubeTransformDefinition[];

  public constructor(cubeValidator: CubeValidator, cubeEvaluator: CubeEvaluator, contextEvaluator: ContextEvaluator, joinGraph: JoinGraph) {
    this.cubeValidator = cubeValidator;
    this.cubeSymbols = cubeEvaluator;
    this.cubeEvaluator = cubeEvaluator;
    this.contextEvaluator = contextEvaluator;
    this.joinGraph = joinGraph;
    this.cubes = [];
    this.queries = [];
  }

  public compile(cubes: EvaluatedCube[], errorReporter: ErrorReporter) {
    this.cubes = this.cubeSymbols.cubeList
      .filter(this.cubeValidator.isCubeValid.bind(this.cubeValidator))
      .map((v) => {
        errorReporter.inContext(`${v.name} cube`);
        return this.transform(v);
      })
      .filter(Boolean);

    this.queries = this.cubes;
  }

  protected transform(cube: CubeDefinitionExtended): CubeTransformDefinition {
    const cubeTitle = cube.title || this.titleize(cube.name);

    const isCubeVisible = this.isVisible(cube, true);

    const flatFolderSeparator = getEnv('nestedFoldersDelimiter');
    const flatFolders: FolderDefinition[] = [];

    const processFolder = (folder, path: string[] = [], mergedMembers: string[] = []) => {
      const flatMembers: string[] = [];
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
        measures: Object.entries(cube.measures || {}).map(([name, definition]) => ({
          ...this.measureConfig(cube.name, cubeTitle, [name, definition]),
          isVisible: isCubeVisible ? this.isVisible(definition, true) : false,
          public: isCubeVisible ? this.isVisible(definition, true) : false,
        })),
        dimensions: Object.entries(cube.dimensions || {}).map(([name, def]) => {
          const isVisible = isCubeVisible
            ? this.isVisible(def, !def.primaryKey)
            : false;

          return {
            name: `${cube.name}.${name}`,
            title: this.title(cubeTitle, [name, def]),
            type: def.type,
            description: def.description,
            shortTitle: this.title(cubeTitle, [name, def], true),
            suggestFilterValues:
              def.suggestFilterValues == null ? true : def.suggestFilterValues,
            format: def.format,
            meta: def.meta,
            isVisible,
            public: isVisible,
            primaryKey: !!def.primaryKey,
            aliasMember: def.aliasMember,
            granularities: def.granularities
              ? Object.entries(def.granularities).map(([gName, gDef]) => ({
                name: gName,
                title: this.title(cubeTitle, [gName, gDef], true),
                interval: gDef.interval,
                offset: gDef.offset,
                origin: gDef.origin,
              }))
              : undefined,
          };
        }),
        segments: Object.entries(cube.segments || {}).map(([name, def]) => {
          const isVisible = isCubeVisible ? this.isVisible(def, true) : false;

          return {
            name: `${cube.name}.${name}`,
            title: this.title(cubeTitle, [name, def]),
            shortTitle: this.title(cubeTitle, [name, def], true),
            description: def.description,
            meta: def.meta,
            isVisible,
            public: isVisible,
          };
        }),
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

  /**
   * @deprecated
   */
  public queriesForContext(contextId) {
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
      (query: CubeTransformDefinition) => R.includes(query.config.name, context.contextMembers)
    )(this.queries);
  }

  protected isVisible(symbol, defaultValue) {
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

  protected measureConfig(cubeName, cubeTitle, nameToMetric) {
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
      meta: nameToMetric[1].meta
    };
  }

  protected title(cubeTitle: string, nameToDef, short: boolean = false): string {
    // eslint-disable-next-line prefer-template
    return `${short ? '' : cubeTitle + ' '}${nameToDef[1].title || this.titleize(nameToDef[0])}`;
  }

  protected titleize(name: string): string {
    return inflection.titleize(inflection.underscore(camelCase(name, { pascalCase: true })));
  }
}
