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
  ResolvedGranularitySet,
  GRANULARITY_STRING_FIELDS,
  normalizeGranularitiesBlock,
  resolveDimensionGranularities,
  serializeEffectiveGranularities,
} from './GranularityResolver';
import {
  GlobalGranularitiesConfig,
  DEFAULT_GRANULARITIES_CONFIG,
  buildBuiltInsCatalog,
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

  // Resolved-once global granularities config for this appId, baked into the compiled model.
  // CompilerApi resolves all config forms (env / static / function) before compile and passes the
  // result here — the transformer never sees a function.
  private readonly granularitiesConfig?: GlobalGranularitiesConfig;

  // Set during compile() from the resolved config. Everything a time dimension needs is precomputed
  // once here so the per-dimension loop does no repeated resolution for the common (plain) case:
  //  - `defaultSet`: the serialized effective set shared by every dimension without a local block;
  //  - `defaultGlobalCustoms`: the global-custom map (name -> def, `type` stripped) baked into those
  //    same plain dimensions — invariant across them, so computed once and shared by reference.
  // Both derive from a single `resolveDimensionGranularities(EMPTY_BLOCK, config)` pass.
  private granularityState!: {
    config: GlobalGranularitiesConfig;
    catalog: Record<string, GranularityDefinition>;
    defaultSet: EffectiveGranularity[];
    defaultGlobalCustoms: Record<string, GranularityDefinition>;
  };

  public constructor(
    cubeValidator: CubeValidator,
    cubeEvaluator: CubeEvaluator,
    contextEvaluator: ContextEvaluator,
    viewGroupEvaluator: ViewGroupEvaluator,
    joinGraph: JoinGraph,
    granularitiesConfig?: GlobalGranularitiesConfig
  ) {
    this.cubeValidator = cubeValidator;
    this.cubeSymbols = cubeEvaluator;
    this.cubeEvaluator = cubeEvaluator;
    this.contextEvaluator = contextEvaluator;
    this.viewGroupEvaluator = viewGroupEvaluator;
    this.joinGraph = joinGraph;
    this.granularitiesConfig = granularitiesConfig;
    this.cubes = [];
    this.queries = [];
  }

  // The resolved global config baked into this compiled model. Exposed for the /v1/granularities
  // endpoint, which serves the per-appId catalog from the compiled model rather than re-resolving.
  public get globalGranularitiesConfig(): GlobalGranularitiesConfig | undefined {
    return this.granularityState?.config;
  }

  public get viewGroups(): CompiledViewGroup[] {
    return this.viewGroupEvaluator.compiledViewGroups;
  }

  public compile(_cubes: any[], errorReporter: ErrorReporter): void {
    // The config is already resolved (env / static / function) by CompilerApi at compile time and
    // baked in here — a missing config means the default catalog.
    const config = this.granularitiesConfig ?? DEFAULT_GRANULARITIES_CONFIG;
    const catalog = buildBuiltInsCatalog(config);
    // Resolve the no-local-block ("default") set once; every plain time dimension shares both its
    // serialized wire form and its global-custom map by reference (no per-dimension resolution).
    const defaultResolved = resolveDimensionGranularities(
      normalizeGranularitiesBlock(undefined), config.enabledBuiltIns, config.customGranularities, catalog,
    );
    this.granularityState = {
      config,
      catalog,
      defaultSet: serializeEffectiveGranularities(defaultResolved),
      defaultGlobalCustoms: this.globalCustomsOf(defaultResolved, config, {}),
    };

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
          // Snapshot the dimension's LOCAL customs before any merge below: the deprecated
          // `granularities` meta field must keep listing only the model's own custom granularities.
          const localCustoms = extendedDimDef.granularities;
          const localCustomEntries = localCustoms ? Object.entries(localCustoms) : [];
          const { granularitiesBlock } = extendedDimDef as any;
          const dimType = this.dimensionDataType(extendedDimDef.type || 'string');
          const dimFormat = this.transformDimensionFormat(extendedDimDef);
          const dimCurrency = extendedDimDef.currency?.toUpperCase();

          let effectiveGranularities: EffectiveGranularity[] | undefined;
          if (dimType === 'time') {
            const s = this.granularityState;
            const inputs = this.granularityInputsForDimension(cubeTitle, localCustoms, granularitiesBlock);
            // Dimensions with a local block resolve individually; plain ones reuse the shared default
            // (both the serialized set and the global-custom map) computed once in compile().
            let globalCustoms: Record<string, GranularityDefinition>;
            if (inputs) {
              const resolved = resolveDimensionGranularities(
                inputs, s.config.enabledBuiltIns, s.config.customGranularities, s.catalog,
              );
              effectiveGranularities = serializeEffectiveGranularities(resolved);
              globalCustoms = this.globalCustomsOf(resolved, s.config, localCustoms ?? {});
            } else {
              effectiveGranularities = s.defaultSet;
              globalCustoms = s.defaultGlobalCustoms;
            }

            // Bake the effective GLOBAL customs into the dimension's `granularities` map (SQL resolves
            // customs by name from this map, and pre-agg matching reads it). Locals win over globals.
            this.mergeGlobalCustomsIntoDimension(cubeName, dimensionName, extendedDimDef, localCustoms, globalCustoms);
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
              localCustomEntries.length > 0
                ? localCustomEntries.map(([gName, gDef]: [string, any]) => ({
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

  // From an already-resolved set, extract the GLOBAL customs a dimension exposes: entries that are
  // custom, defined in the global config, and not shadowed by a local of the same name. Projected
  // through GRANULARITY_STRING_FIELDS (the shared field list, so it can't drift from serialize/hash).
  private globalCustomsOf(
    resolved: ResolvedGranularitySet,
    config: GlobalGranularitiesConfig,
    localCustoms: Record<string, GranularityDefinition>,
  ): Record<string, GranularityDefinition> {
    const out: Record<string, GranularityDefinition> = {};
    for (const [name, def] of Object.entries(resolved)) {
      if (def.type === 'custom' &&
        Object.prototype.hasOwnProperty.call(config.customGranularities, name) &&
        !Object.prototype.hasOwnProperty.call(localCustoms, name)
      ) {
        const projected: GranularityDefinition = {} as GranularityDefinition;
        for (const field of GRANULARITY_STRING_FIELDS) {
          if (def[field] !== undefined) {
            (projected as any)[field] = def[field];
          }
        }
        out[name] = projected;
      }
    }
    return out;
  }

  // Bake the precomputed effective GLOBAL customs (`globalCustoms`) into a time dimension's
  // `granularities` map — decision 3 of the per-appId compile-time bake — so the SQL symbol path
  // resolves them by name and they participate in pre-agg matching. Locals always win.
  //
  // The bake must reach BOTH object graphs the downstream paths read: the `cubeList`/`evaluatedCubes`
  // object (`dimDef`, read by timeDimensionsForCube → pre-agg matching) AND the separate
  // `symbols[cube][dim]` object that CubeSymbols.resolveGranularity reads for SQL. These are distinct
  // objects (built by different transforms) even for a plain cube, so the second write is
  // load-bearing, not a no-op. If only `dimDef` were written, SQL couldn't resolve a baked global and
  // pre-agg matching would throw when it builds a Granularity for it. We REASSIGN `granularities`
  // (never mutate in place) so a view dim sharing its source's map by reference isn't contaminated
  // before it is itself processed.
  private mergeGlobalCustomsIntoDimension(
    cubeName: string,
    dimensionName: string,
    dimDef: ExtendedCubeSymbolDefinition,
    localCustoms: Record<string, GranularityDefinition> | undefined,
    globalCustoms: Record<string, GranularityDefinition>,
  ): void {
    if (Object.keys(globalCustoms).length === 0) {
      return;
    }
    const hasLocals = !!localCustoms && Object.keys(localCustoms).length > 0;

    // With no locals the baked map IS the shared globalCustoms — assign it by reference (every plain
    // dimension then shares one object). Copy-on-write only when locals must be layered on top.
    const write = (existing: Record<string, GranularityDefinition> | undefined) => (
      existing && Object.keys(existing).length > 0
        ? { ...globalCustoms, ...existing } // globals first, locals last so locals win on collisions
        : globalCustoms
    );

    dimDef.granularities = write(hasLocals ? localCustoms : undefined);

    const symbolDim = (this.cubeEvaluator as any).symbols?.[cubeName]?.[dimensionName];
    if (symbolDim && symbolDim !== dimDef) {
      symbolDim.granularities = write(symbolDim.granularities as Record<string, GranularityDefinition> | undefined);
    }
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
