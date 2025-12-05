import inflection from 'inflection';
import camelCase from 'camelcase';

import { getEnv } from '@cubejs-backend/shared';
import {
  CubeSymbolDefinition,
  CubeSymbols,
  Folder,
  FolderMember,
  GranularityDefinition,
} from './CubeSymbols';
import { UserError } from './UserError';
import { BaseMeasure } from '../adapter';
import type { CubeDefinitionExtended } from './CubeSymbols';
import type { CubeValidator } from './CubeValidator';
import type { CubeEvaluator } from './CubeEvaluator';
import type { ContextEvaluator } from './ContextEvaluator';
import type { JoinGraph } from './JoinGraph';
import type { ErrorReporter } from './ErrorReporter';
import { CompilerInterface } from './PrepareCompiler';

export type CustomNumericFormat = { type: 'custom-numeric'; value: string };
export type DimensionCustomTimeFormat = { type: 'custom-time'; value: string };
export type DimensionLinkFormat = { type: 'link'; label?: string };
export type DimensionFormat = string | DimensionLinkFormat | DimensionCustomTimeFormat | CustomNumericFormat;
export type MeasureFormat = string | CustomNumericFormat;

// Extended types for cube symbols with all runtime properties
export interface ExtendedCubeSymbolDefinition extends CubeSymbolDefinition {
  description?: string;
  meta?: any;
  title?: string;
  public?: boolean;
  visible?: boolean;
  shown?: boolean;
  suggestFilterValues?: boolean;
  aliasMember?: string;
  drillMembers?: any;
  drillMemberReferences?: any;
  cumulative?: boolean;
  aggType?: string;
}

interface ExtendedCubeDefinition extends CubeDefinitionExtended {
  title?: string;
  description?: string;
  meta?: any;
  evaluatedHierarchies?: Array<{
    name: string;
    public?: boolean;
    [key: string]: any;
  }>;
}

export type FlatFolder = {
  name: string;
  members: string[];
};

export type NestedFolder = {
  name: string;
  members: Array<string | NestedFolder>;
};

export type MeasureConfig = {
  name: string;
  title: string;
  description?: string;
  shortTitle: string;
  format?: MeasureFormat;
  cumulativeTotal: boolean;
  cumulative: boolean;
  type: string;
  aggType: string;
  drillMembers: string[];
  drillMembersGrouped: {
    measures: string[];
    dimensions: string[];
  };
  aliasMember?: string;
  meta?: any;
  isVisible: boolean;
  public: boolean;
};

export type DimensionConfig = {
  name: string;
  title: string;
  type: string;
  description?: string;
  shortTitle: string;
  suggestFilterValues: boolean;
  format?: DimensionFormat;
  meta?: any;
  isVisible: boolean;
  public: boolean;
  primaryKey: boolean;
  aliasMember?: string;
  granularities?: GranularityDefinition[];
  order?: 'asc' | 'desc';
};

export type SegmentConfig = {
  name: string;
  title: string;
  shortTitle: string;
  description?: string;
  meta?: any;
  isVisible: boolean;
  public: boolean;
};

export type HierarchyConfig = {
  name: string;
  public: boolean;
  [key: string]: any;
};

export type CubeConfig = {
  name: string;
  type: 'view' | 'cube';
  title: string;
  isVisible: boolean;
  public: boolean;
  description?: string;
  connectedComponent: number;
  meta?: any;
  measures: MeasureConfig[];
  dimensions: DimensionConfig[];
  segments: SegmentConfig[];
  hierarchies: HierarchyConfig[];
  folders: FlatFolder[];
  nestedFolders: NestedFolder[];
};

export type TransformedCube = {
  config: CubeConfig;
};

export class CubeToMetaTransformer implements CompilerInterface {
  private readonly cubeValidator: CubeValidator;

  private readonly cubeSymbols: CubeEvaluator;

  public readonly cubeEvaluator: CubeEvaluator;

  private readonly contextEvaluator: ContextEvaluator;

  private readonly joinGraph: JoinGraph;

  public cubes: TransformedCube[];

  /**
   * @deprecated
   */
  public queries: TransformedCube[];

  public constructor(
    cubeValidator: CubeValidator,
    cubeEvaluator: CubeEvaluator,
    contextEvaluator: ContextEvaluator,
    joinGraph: JoinGraph
  ) {
    this.cubeValidator = cubeValidator;
    this.cubeSymbols = cubeEvaluator;
    this.cubeEvaluator = cubeEvaluator;
    this.contextEvaluator = contextEvaluator;
    this.joinGraph = joinGraph;
    this.cubes = [];
    this.queries = [];
  }

  public compile(_cubes: any[], errorReporter: ErrorReporter): void {
    this.cubes = this.cubeSymbols.cubeList
      .filter(this.cubeValidator.isCubeValid.bind(this.cubeValidator))
      .map((v) => this.transform(v, errorReporter.inContext(`${v.name} cube`)));

    this.queries = this.cubes;
  }

  protected transform(cube: CubeDefinitionExtended, _errorReporter?: ErrorReporter): TransformedCube {
    const extendedCube = cube as ExtendedCubeDefinition;
    const cubeName = extendedCube.name;
    const cubeTitle = extendedCube.title || this.titleize(cubeName);

    const isCubeVisible = this.isVisible(extendedCube, true);

    const flatFolderSeparator = getEnv('nestedFoldersDelimiter');
    const flatFolders: FlatFolder[] = [];

    const processFolder = (folder: Folder, path: string[] = [], mergedMembers: string[] = []): NestedFolder => {
      const flatMembers: string[] = [];
      const nestedMembers: Array<string | NestedFolder> = folder.includes.map((member: FolderMember) => {
        if (member.type === 'folder' && member.includes) {
          return processFolder(member as Folder, [...path, folder.name], flatMembers);
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

    const nestedFolders: NestedFolder[] = (extendedCube.folders || []).map((f: Folder) => processFolder(f));

    return {
      config: {
        name: cubeName,
        type: extendedCube.isView ? 'view' : 'cube',
        title: cubeTitle,
        isVisible: isCubeVisible,
        public: isCubeVisible,
        description: extendedCube.description,
        connectedComponent: this.joinGraph.connectedComponents()[cubeName],
        meta: extendedCube.meta,
        measures: Object.entries(extendedCube.measures || {}).map((nameToMetric: [string, any]) => {
          const metricDef = nameToMetric[1] as ExtendedCubeSymbolDefinition;
          const measureVisibility = isCubeVisible ? this.isVisible(metricDef, true) : false;
          return {
            ...this.measureConfig(cubeName, cubeTitle, nameToMetric),
            isVisible: measureVisibility,
            public: measureVisibility,
          };
        }),
        dimensions: Object.entries(extendedCube.dimensions || {}).map((nameToDimension: [string, any]) => {
          const [dimensionName, dimDef] = nameToDimension;
          const extendedDimDef = dimDef as ExtendedCubeSymbolDefinition;
          const dimensionVisibility = isCubeVisible
            ? this.isVisible(extendedDimDef, !extendedDimDef.primaryKey)
            : false;
          const granularitiesObj = extendedDimDef.granularities;

          return {
            name: `${cubeName}.${dimensionName}`,
            title: this.title(cubeTitle, nameToDimension, false),
            type: this.dimensionDataType(extendedDimDef.type || 'string'),
            description: extendedDimDef.description,
            shortTitle: this.title(cubeTitle, nameToDimension, true),
            suggestFilterValues:
              extendedDimDef.suggestFilterValues == null
                ? true
                : extendedDimDef.suggestFilterValues,
            format: this.transformDimensionFormat(extendedDimDef),
            meta: extendedDimDef.meta,
            isVisible: dimensionVisibility,
            public: dimensionVisibility,
            primaryKey: !!extendedDimDef.primaryKey,
            aliasMember: extendedDimDef.aliasMember,
            granularities:
              granularitiesObj
                ? Object.entries(granularitiesObj).map(([gName, gDef]: [string, any]) => ({
                  name: gName,
                  title: this.title(cubeTitle, [gName, gDef], true),
                  interval: gDef.interval,
                  offset: gDef.offset,
                  origin: gDef.origin,
                }))
                : undefined,
            order: extendedDimDef.order,
          };
        }),
        segments: Object.entries(extendedCube.segments || {}).map((nameToSegment: [string, any]) => {
          const [segmentName, segmentDef] = nameToSegment;
          const extendedSegmentDef = segmentDef as ExtendedCubeSymbolDefinition;
          const segmentVisibility = isCubeVisible ? this.isVisible(extendedSegmentDef, true) : false;
          return {
            name: `${cubeName}.${segmentName}`,
            title: this.title(cubeTitle, nameToSegment, false),
            shortTitle: this.title(cubeTitle, nameToSegment, true),
            description: extendedSegmentDef.description,
            meta: extendedSegmentDef.meta,
            isVisible: segmentVisibility,
            public: segmentVisibility,
          };
        }),
        hierarchies: (extendedCube.evaluatedHierarchies || []).map((it: any) => ({
          ...it,
          public: it.public ?? true,
          name: `${cubeName}.${it.name}`,
        })),
        folders: flatFolders,
        nestedFolders,
      },
    };
  }

  public queriesForContext(contextId: string | null | undefined): TransformedCube[] {
    // return All queries if no context pass
    if (contextId == null || contextId.length === 0) {
      return this.queries;
    }

    const context = (this.contextEvaluator as any).contextDefinitions[contextId];

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
  protected isVisible(symbol: any, defaultValue: boolean): boolean {
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

  private dimensionDataType(dimensionType: string): string {
    return dimensionType === 'switch' ? 'string' : dimensionType;
  }

  private measureConfig(cubeName: string, cubeTitle: string, nameToMetric: [string, any]): Omit<MeasureConfig, 'isVisible' | 'public'> {
    const [metricName, metricDef] = nameToMetric;
    const extendedMetricDef = metricDef as ExtendedCubeSymbolDefinition;
    const name = `${cubeName}.${metricName}`;

    // Support both old 'drillMemberReferences' and new 'drillMembers' keys
    const drillMembers = extendedMetricDef.drillMembers || extendedMetricDef.drillMemberReferences;

    const drillMembersArray: string[] = (drillMembers && this.cubeEvaluator.evaluateReferences(
      cubeName, drillMembers, { originalSorting: true }
    )) || [];

    const type = CubeSymbols.toMemberDataType(extendedMetricDef.type || 'number');
    const isCumulative = extendedMetricDef.cumulative || BaseMeasure.isCumulative(extendedMetricDef);

    const drillMembersGrouped: { measures: string[]; dimensions: string[] } = { measures: [], dimensions: [] };
    for (const member of drillMembersArray) {
      if (this.cubeEvaluator.isMeasure(member)) {
        drillMembersGrouped.measures.push(member);
      } else if (this.cubeEvaluator.isDimension(member)) {
        drillMembersGrouped.dimensions.push(member);
      }
    }

    return {
      name,
      title: this.title(cubeTitle, nameToMetric, false),
      description: extendedMetricDef.description,
      shortTitle: this.title(cubeTitle, nameToMetric, true),
      format: this.transformMeasureFormat(extendedMetricDef.format),
      cumulativeTotal: isCumulative,
      cumulative: isCumulative,
      type,
      aggType: extendedMetricDef.aggType || extendedMetricDef.type || '',
      drillMembers: drillMembersArray,
      drillMembersGrouped,
      aliasMember: extendedMetricDef.aliasMember,
      meta: extendedMetricDef.meta
    };
  }

  private title(cubeTitle: string, nameToDef: [string, any], short: boolean): string {
    const prefix = short ? '' : `${cubeTitle} `;
    const def = nameToDef[1] as ExtendedCubeSymbolDefinition;
    const suffix = def.title || this.titleize(nameToDef[0]);
    return `${prefix}${suffix}`;
  }

  private titleize(name: string): string {
    return inflection.titleize(inflection.underscore(camelCase(name, { pascalCase: true })));
  }

  private transformDimensionFormat({ format, type }: ExtendedCubeSymbolDefinition): DimensionFormat | undefined {
    if (!format || typeof format === 'object') {
      return format;
    }

    const standardFormats = ['imageUrl', 'currency', 'percent', 'number', 'id'];
    if (standardFormats.includes(format)) {
      return format;
    }

    // Custom time format for time dimensions
    if (type === 'time') {
      return { type: 'custom-time', value: format };
    }

    // Custom numeric format for number dimensions
    if (type === 'number') {
      return { type: 'custom-numeric', value: format };
    }

    return format;
  }

  private transformMeasureFormat(format: string | undefined): MeasureFormat | undefined {
    if (!format) {
      return undefined;
    }

    const standardFormats = ['percent', 'currency', 'number'];
    if (standardFormats.includes(format)) {
      return format;
    }

    // Custom numeric format
    return { type: 'custom-numeric', value: format };
  }
}
