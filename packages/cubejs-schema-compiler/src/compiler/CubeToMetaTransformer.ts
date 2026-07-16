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
import type { ViewGroupEvaluator, CompiledViewGroup } from './ViewGroupEvaluator';
import type { JoinGraph } from './JoinGraph';
import type { ErrorReporter } from './ErrorReporter';
import { CompilerInterface } from './PrepareCompiler';
import { resolveNamedNumericFormat, STANDARD_FORMAT_SPECIFIERS, DEFAULT_FORMAT_SPECIFIER } from './named-numeric-formats';
import {
  EffectiveGranularity,
  NormalizedGranularitiesBlock,
  normalizeGranularitiesBlock,
  resolveDimensionGranularities,
  serializeEffectiveGranularities,
} from './GranularityResolver';
import {
  GlobalGranularitiesConfig,
  GranularitiesOption,
  buildBuiltInsCatalog,
  resolveGlobalGranularitiesSync,
} from './GlobalGranularitiesConfig';

export type CustomNumericFormat = { type: 'custom-numeric'; value: string; alias?: string };
export type DimensionCustomTimeFormat = { type: 'custom-time'; value: string };
export type DimensionLinkFormat = { type: 'link'; label?: string };
export type DimensionFormat = string | DimensionLinkFormat | DimensionCustomTimeFormat | CustomNumericFormat;
export type MeasureFormat = string | CustomNumericFormat;

export type FormatDescription = {
  name: string;
  specifier: string;
  currency?: string;
};

const EXCLUDED_MEASURE_TYPES = new Set(['string', 'boolean', 'time']);

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
  keyReference?: string;
  currency?: string;
  links?: Array<{
    name: string;
    label: string;
    url?: (...args: any[]) => string;
    dashboard?: string;
    icon?: string;
    target?: 'blank' | 'self';
    params?: Array<{ key: string; value: (...args: any[]) => string }>;
  }>;
  synthetic?: boolean;
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
  formatDescription?: FormatDescription;
  currency?: string;
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

export type LinkConfig = {
  name: string;
  label: string;
  dashboard?: string;
  icon?: string;
  target?: 'blank' | 'self';
  primary?: boolean;
  params?: string[];
};

export type DimensionConfig = {
  name: string;
  title: string;
  type: string;
  description?: string;
  shortTitle: string;
  suggestFilterValues: boolean;
  format?: DimensionFormat;
  formatDescription?: FormatDescription;
  currency?: string;
  meta?: any;
  isVisible: boolean;
  public: boolean;
  primaryKey: boolean;
  aliasMember?: string;
  /**
   * @deprecated Use `effectiveGranularities`. Lists only the model's custom granularities;
   * omits built-ins, global customs, and the `type` field. See DEPRECATION.md.
   */
  granularities?: GranularityDefinition[];
  /** Reconciled set for time dimensions: enabled built-ins + global customs + local customs. */
  effectiveGranularities?: EffectiveGranularity[];
  order?: 'asc' | 'desc';
  key?: string;
  links?: LinkConfig[];
  synthetic?: boolean;
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
  viewGroups?: string[];
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

  private readonly viewGroupEvaluator: ViewGroupEvaluator;

  private readonly joinGraph: JoinGraph;

  public cubes: TransformedCube[];

  /**
   * @deprecated
   */
  public queries: TransformedCube[];

  private readonly granularitiesOption?: GranularitiesOption;

  // Inputs for time dimensions that customize their granularity set, keyed by `cube.dimension`;
  // absent dims use the config-wide default. Read by CompilerApi variant builds; never serialized.
  public readonly granularityInputs: Map<string, NormalizedGranularitiesBlock> = new Map();

  // Set during compile() for the context-independent config forms (env / static list);
  // null when `granularities` is a function and resolution has to happen per request.
  private staticGranularityState: {
    config: GlobalGranularitiesConfig;
    catalog: Record<string, GranularityDefinition>;
    defaultSet: EffectiveGranularity[];
  } | null = null;

  public constructor(
    cubeValidator: CubeValidator,
    cubeEvaluator: CubeEvaluator,
    contextEvaluator: ContextEvaluator,
    viewGroupEvaluator: ViewGroupEvaluator,
    joinGraph: JoinGraph,
    granularitiesOption?: GranularitiesOption
  ) {
    this.cubeValidator = cubeValidator;
    this.cubeSymbols = cubeEvaluator;
    this.cubeEvaluator = cubeEvaluator;
    this.contextEvaluator = contextEvaluator;
    this.viewGroupEvaluator = viewGroupEvaluator;
    this.joinGraph = joinGraph;
    this.granularitiesOption = granularitiesOption;
    this.cubes = [];
    this.queries = [];
  }

  public get viewGroups(): CompiledViewGroup[] {
    return this.viewGroupEvaluator.compiledViewGroups;
  }

  public compile(_cubes: any[], errorReporter: ErrorReporter): void {
    this.granularityInputs.clear();
    // Env/static configs are resolved once here and baked in. The function form must never run
    // at compile time (the compiled model is shared across security contexts) — CompilerApi
    // resolves it per request from `granularityInputs`.
    if (typeof this.granularitiesOption === 'function') {
      this.staticGranularityState = null;
    } else {
      const config = resolveGlobalGranularitiesSync(this.granularitiesOption);
      const catalog = buildBuiltInsCatalog(config);
      this.staticGranularityState = {
        config,
        catalog,
        // One shared array for every time dimension without local customization — with large
        // models this avoids re-allocating an identical granularity set per dimension.
        defaultSet: serializeEffectiveGranularities(resolveDimensionGranularities(
          normalizeGranularitiesBlock(undefined),
          config.enabledBuiltIns,
          config.customGranularities,
          catalog,
        )),
      };
    }

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
      // After evaluation in CubeEvaluator, folder.includes contains resolved FolderMember items
      const nestedMembers: Array<string | NestedFolder> = (folder.includes as FolderMember[]).map((member: FolderMember) => {
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

    const viewGroupNames = extendedCube.isView
      ? this.viewGroupEvaluator.viewGroupsForView(cubeName)
      : [];

    return {
      config: {
        name: cubeName,
        type: extendedCube.isView ? 'view' : 'cube',
        title: cubeTitle,
        isVisible: isCubeVisible,
        public: isCubeVisible,
        description: extendedCube.description,
        ...(viewGroupNames.length > 0 ? { viewGroups: viewGroupNames } : {}),
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
          // `granularities` keeps its legacy custom-only shape (deprecated). The reconciled set
          // is emitted as `effectiveGranularities`: baked in here for env/static global configs,
          // or attached per request by CompilerApi variants when the config is a function.
          const { granularitiesBlock } = extendedDimDef as any;
          const dimType = this.dimensionDataType(extendedDimDef.type || 'string');
          const dimFormat = this.transformDimensionFormat(extendedDimDef);
          const dimCurrency = extendedDimDef.currency?.toUpperCase();

          let effectiveGranularities: EffectiveGranularity[] | undefined;
          if (dimType === 'time') {
            const inputs = this.granularityInputsForDimension(cubeTitle, granularitiesObj, granularitiesBlock);
            if (inputs) {
              this.granularityInputs.set(`${cubeName}.${dimensionName}`, inputs);
            }
            if (this.staticGranularityState) {
              const s = this.staticGranularityState;
              effectiveGranularities = inputs
                ? serializeEffectiveGranularities(resolveDimensionGranularities(
                  inputs, s.config.enabledBuiltIns, s.config.customGranularities, s.catalog,
                ))
                : s.defaultSet;
            }
          }

          return {
            name: `${cubeName}.${dimensionName}`,
            title: this.title(cubeTitle, nameToDimension, false),
            type: dimType,
            description: extendedDimDef.description,
            shortTitle: this.title(cubeTitle, nameToDimension, true),
            suggestFilterValues:
              extendedDimDef.suggestFilterValues == null
                ? true
                : extendedDimDef.suggestFilterValues,
            format: dimFormat,
            formatDescription: this.resolveFormatDescription(dimFormat, dimType, false, dimCurrency),
            currency: dimCurrency,
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
            ...(effectiveGranularities ? { effectiveGranularities } : {}),
            order: extendedDimDef.order,
            key: extendedDimDef.keyReference,
            ...(extendedDimDef.links ? { links: extendedDimDef.links.map((link: any) => ({
              name: link.name,
              label: link.label,
              ...(link.dashboard ? { dashboard: typeof link.dashboard === 'function' ? link.dashboard() : link.dashboard } : {}),
              icon: link.icon,
              ...(link.target ? { target: link.target } : {}),
              ...(link.primary ? { primary: true } : {}),
              ...(link.params && Array.isArray(link.params) && link.params.length > 0
                ? { params: link.params.map((p: any) => (typeof p.key === 'function' ? p.key() : p.key)) }
                : {}),
            })) } : {}),
            ...(extendedDimDef.synthetic ? { synthetic: true } : {}),
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

  // Resolution inputs for one time dimension; null = no local customization, use the default set.
  // Fields are projected from raw definitions so e.g. `sql` never leaks into meta output.
  private granularityInputsForDimension(
    cubeTitle: string,
    granularitiesObj: Record<string, GranularityDefinition> | undefined,
    granularitiesBlock: NormalizedGranularitiesBlock | undefined,
  ): NormalizedGranularitiesBlock | null {
    const hasLocalCustoms = granularitiesObj && Object.keys(granularitiesObj).length > 0;
    if (!granularitiesBlock && !hasLocalCustoms) {
      return null;
    }
    const block = granularitiesBlock || normalizeGranularitiesBlock(undefined);
    const custom: Record<string, GranularityDefinition> = {};
    for (const [gName, gDef] of Object.entries({ ...block.custom, ...(granularitiesObj || {}) })) {
      custom[gName] = {
        title: this.title(cubeTitle, [gName, gDef], true),
        interval: gDef.interval,
        offset: gDef.offset,
        origin: gDef.origin,
        ...(gDef.format !== undefined ? { format: gDef.format } : {}),
      };
    }
    return { includes: block.includes, excludes: block.excludes, custom };
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

    const format = this.transformMeasureFormat(extendedMetricDef.format);
    const currency = extendedMetricDef.currency?.toUpperCase();

    return {
      name,
      title: this.title(cubeTitle, nameToMetric, false),
      description: extendedMetricDef.description,
      shortTitle: this.title(cubeTitle, nameToMetric, true),
      format,
      formatDescription: this.resolveFormatDescription(format, type, true, currency),
      currency,
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
    const titleized = inflection.titleize(inflection.underscore(camelCase(name, { pascalCase: true })));
    // Capitalize common identifier acronyms so e.g. `userId` reads as "User ID"
    // rather than "User Id" and an `id` member becomes "ID" instead of "Id".
    return titleized.replace(/\bId(s?)\b/g, (_match, plural) => `ID${plural}`);
  }

  private transformDimensionFormat({ format: formatOrName, type }: ExtendedCubeSymbolDefinition): DimensionFormat | undefined {
    if (!formatOrName || typeof formatOrName === 'object') {
      return formatOrName;
    }

    // Resolve named numeric formats (abbr, accounting, number_X, percent_X, etc.)
    const resolved = resolveNamedNumericFormat(formatOrName);
    if (resolved) {
      return { type: 'custom-numeric', value: resolved, alias: formatOrName };
    }

    // Existing standard formats stay as-is (breaking change to convert these)
    const standardFormats = ['imageUrl', 'currency', 'percent', 'number', 'id'];
    if (standardFormats.includes(formatOrName)) {
      return formatOrName;
    }

    // Custom time format for time dimensions
    if (type === 'time') {
      return { type: 'custom-time', value: formatOrName };
    }

    // Custom numeric format for number dimensions (raw d3-format specifier)
    if (type === 'number') {
      return { type: 'custom-numeric', value: formatOrName };
    }

    return formatOrName;
  }

  private transformMeasureFormat(formatOrName: string | undefined): MeasureFormat | undefined {
    if (!formatOrName) {
      return undefined;
    }

    // Resolve named numeric formats (abbr, accounting, number_X, percent_X, etc.)
    const resolved = resolveNamedNumericFormat(formatOrName);
    if (resolved) {
      return { type: 'custom-numeric', value: resolved, alias: formatOrName };
    }

    // Existing standard formats stay as-is (breaking change to convert these)
    const standardFormats = ['percent', 'currency', 'number'];
    if (standardFormats.includes(formatOrName)) {
      return formatOrName;
    }

    // Custom numeric format (raw d3-format specifier)
    return { type: 'custom-numeric', value: formatOrName };
  }

  /**
   * Resolves a format into a FormatDescription.
   * - Measures: returned for all types except string, boolean, and time.
   * - Dimensions: returned only for number type.
   */
  private resolveFormatDescription(
    format: MeasureFormat | DimensionFormat | undefined,
    type: string,
    isMeasure: boolean,
    currency?: string,
  ): FormatDescription | undefined {
    if (isMeasure) {
      if (EXCLUDED_MEASURE_TYPES.has(type)) {
        return undefined;
      }
    } else if (type !== 'number') {
      return undefined;
    }

    let desc: FormatDescription;

    if (format && typeof format === 'object' && format.type === 'custom-numeric') {
      desc = {
        name: format.alias || 'custom',
        specifier: format.value,
      };
    } else if (typeof format === 'string' && STANDARD_FORMAT_SPECIFIERS[format]) {
      desc = { ...STANDARD_FORMAT_SPECIFIERS[format] };
    } else {
      desc = { ...DEFAULT_FORMAT_SPECIFIER };
    }

    if (currency) {
      desc.currency = currency;
    }

    return desc;
  }
}
