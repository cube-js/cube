import type { CubeEvaluator } from './CubeEvaluator';
import type { CubeValidator } from './CubeValidator';
import type { ErrorReporter } from './ErrorReporter';
import { CompilerInterface } from './PrepareCompiler';

/**
 * A nested view group definition, as authored inside the `includes` array of
 * another view group. Mirrors the nested folder shape.
 */
export interface ViewGroupIncludeNested {
  name: string;
  title?: string;
  description?: string;
  // eslint-disable-next-line no-use-before-define
  includes: ViewGroupInclude[] | (() => any[]);
}

/**
 * An entry of a view group's `includes`: either a view reference (a string, or
 * a transpiled bare-identifier reference) or a nested view group definition.
 */
export type ViewGroupInclude = string | (() => any) | ViewGroupIncludeNested;

export interface ViewGroupInput {
  name: string;
  title?: string;
  description?: string;
  /**
   * Legacy way of including views into a group. Kept for backward
   * compatibility. When `includes` is present, `views` is ignored.
   */
  views?: string[] | (() => string[]);
  /**
   * Preferred way of including views into a group. Supports both view
   * references and nested view group definitions (full hierarchy).
   */
  includes?: ViewGroupInclude[] | (() => any[]);
  fileName?: string;
}

export interface CompiledViewGroup {
  name: string;
  title?: string;
  description?: string;
  /**
   * The group's own direct view references at this level (not a deep flatten).
   */
  views: string[];
  /**
   * Recursive representation: view name strings interleaved with nested
   * compiled view groups, preserving authoring order.
   */
  includes: (string | CompiledViewGroup)[];
}

export class ViewGroupEvaluator implements CompilerInterface {
  private readonly cubeEvaluator: CubeEvaluator;

  private readonly cubeValidator: CubeValidator;

  private viewGroupDefinitions: Map<string, CompiledViewGroup>;

  private resolvedViewGroups: CompiledViewGroup[];

  private viewToGroups: Map<string, string[]>;

  public constructor(cubeEvaluator: CubeEvaluator, cubeValidator: CubeValidator) {
    this.cubeEvaluator = cubeEvaluator;
    this.cubeValidator = cubeValidator;
    this.viewGroupDefinitions = new Map<string, CompiledViewGroup>();
    this.resolvedViewGroups = [];
    this.viewToGroups = new Map();
  }

  public compile(viewGroups: ViewGroupInput[], errorReporter?: ErrorReporter): void {
    this.viewGroupDefinitions = new Map<string, CompiledViewGroup>();

    for (const viewGroup of viewGroups) {
      if (errorReporter) {
        this.cubeValidator.validateViewGroup(viewGroup, errorReporter);
      }

      if (errorReporter && this.viewGroupDefinitions.has(viewGroup.name)) {
        errorReporter.error(`View group "${viewGroup.name}" already exists!`);
      } else {
        this.viewGroupDefinitions.set(viewGroup.name, this.compileViewGroup(viewGroup, errorReporter));
      }
    }

    this.resolve(errorReporter);
  }

  private compileViewGroup(viewGroup: ViewGroupInput, errorReporter?: ErrorReporter): CompiledViewGroup {
    // `views` and `includes` are mutually exclusive on a view group definition;
    // this is enforced by the Joi `viewGroupSchema` (oxor). `includes` is the
    // preferred form, so it takes precedence here if both somehow slip through.
    if (viewGroup.includes !== undefined) {
      const seenNames = new Set<string>([viewGroup.name]);
      const { views, includes } = this.compileIncludes(viewGroup.includes, seenNames, errorReporter);
      return {
        name: viewGroup.name,
        title: viewGroup.title,
        description: viewGroup.description,
        views,
        includes,
      };
    }

    // Legacy `views` parameter.
    let views: string[] = [];
    if (viewGroup.views) {
      if (typeof viewGroup.views === 'function') {
        const evaluated = this.cubeEvaluator.evaluateReferences(null, viewGroup.views, { originalSorting: true });
        views = Array.isArray(evaluated) ? evaluated : [evaluated];
      } else if (Array.isArray(viewGroup.views)) {
        views = viewGroup.views;
      }
    }

    return {
      name: viewGroup.name,
      title: viewGroup.title,
      description: viewGroup.description,
      views,
      includes: views.slice(),
    };
  }

  /**
   * Recursively compiles a view group's `includes` into the group's own direct
   * view references plus a recursive `includes` representation (strings for
   * views, nested CompiledViewGroup objects for nested groups).
   */
  private compileIncludes(
    rawIncludes: ViewGroupInclude[] | (() => any[]),
    seenNames: Set<string>,
    errorReporter?: ErrorReporter,
  ): { views: string[]; includes: (string | CompiledViewGroup)[] } {
    let items: any[] = [];
    if (typeof rawIncludes === 'function') {
      const evaluated = rawIncludes();
      items = Array.isArray(evaluated) ? evaluated : [evaluated];
    } else if (Array.isArray(rawIncludes)) {
      items = rawIncludes;
    }

    const views: string[] = [];
    const includes: (string | CompiledViewGroup)[] = [];

    for (const item of items) {
      if (this.isNestedGroup(item)) {
        if (errorReporter && seenNames.has(item.name)) {
          errorReporter.error(`View group "${item.name}" already exists!`);
          // eslint-disable-next-line no-continue
          continue;
        }
        seenNames.add(item.name);

        const child = this.compileIncludes(item.includes, seenNames, errorReporter);
        includes.push({
          name: item.name,
          title: item.title,
          description: item.description,
          views: child.views,
          includes: child.includes,
        });
      } else {
        // A view reference: either a plain string or a transpiled
        // single-reference arrow function.
        for (const name of this.resolveViewReference(item)) {
          views.push(name);
          includes.push(name);
        }
      }
    }

    return { views, includes };
  }

  private isNestedGroup(item: any): item is ViewGroupIncludeNested {
    return typeof item === 'object' && item !== null && typeof item.name === 'string' && 'includes' in item;
  }

  private resolveViewReference(item: any): string[] {
    if (typeof item === 'function') {
      const evaluated = this.cubeEvaluator.evaluateReferences(null, item, { originalSorting: true });
      return Array.isArray(evaluated) ? evaluated : [evaluated];
    }
    if (typeof item === 'string') {
      return [item];
    }
    return [];
  }

  private resolve(errorReporter?: ErrorReporter): void {
    const validViewNames = new Set<string>();
    for (const cube of this.cubeEvaluator.cubeList) {
      if (cube.isView) {
        validViewNames.add(cube.name);
      }
    }

    const viewGroupMap = new Map<string, CompiledViewGroup>();
    for (const [name, def] of this.viewGroupDefinitions) {
      viewGroupMap.set(name, this.resolveGroup(def, validViewNames, errorReporter));
    }

    // Auto-attach views that reference a top-level group via their own
    // `viewGroup` / `viewGroups` properties.
    for (const cube of this.cubeEvaluator.cubeList) {
      if (!cube.isView) {
        // eslint-disable-next-line no-continue
        continue;
      }

      for (const groupName of this.viewGroupNamesForCube(cube)) {
        const group = viewGroupMap.get(groupName);
        if (!group) {
          if (errorReporter) {
            errorReporter.error(`View "${cube.name}" references view group "${groupName}" which is not defined. Define it using view_group('${groupName}', { ... }).`);
          }
        } else if (!group.views.includes(cube.name)) {
          group.views.push(cube.name);
          group.includes.push(cube.name);
        }
      }
    }

    this.resolvedViewGroups = Array.from(viewGroupMap.values());

    // Map each view to the most-specific group(s) it directly belongs to.
    this.viewToGroups = new Map();
    for (const group of this.resolvedViewGroups) {
      this.collectViewToGroups(group);
    }
  }

  /**
   * Validates a compiled group's view references against the set of real view
   * names and returns a filtered copy. References to views that do not exist
   * produce a compile error.
   */
  private resolveGroup(
    group: CompiledViewGroup,
    validViewNames: Set<string>,
    errorReporter?: ErrorReporter,
  ): CompiledViewGroup {
    const views: string[] = [];
    const includes: (string | CompiledViewGroup)[] = [];

    for (const include of group.includes) {
      if (typeof include !== 'string') {
        includes.push(this.resolveGroup(include, validViewNames, errorReporter));
      } else if (validViewNames.has(include)) {
        views.push(include);
        includes.push(include);
      } else if (errorReporter) {
        errorReporter.error(`View group "${group.name}" includes "${include}" which is not a defined view.`);
      }
    }

    return {
      name: group.name,
      title: group.title,
      description: group.description,
      views,
      includes,
    };
  }

  private viewGroupNamesForCube(cube: any): string[] {
    const groupNames: string[] = [];

    if (cube.viewGroup) {
      const resolved = typeof cube.viewGroup === 'function'
        ? this.cubeEvaluator.evaluateReferences(null, cube.viewGroup)
        : cube.viewGroup;
      const names = Array.isArray(resolved) ? resolved : [resolved];
      for (const n of names) {
        if (!groupNames.includes(n)) {
          groupNames.push(n);
        }
      }
    }

    if (cube.viewGroups) {
      let resolved: string[];
      if (typeof cube.viewGroups === 'function') {
        const evaluated = this.cubeEvaluator.evaluateReferences(null, cube.viewGroups, { originalSorting: true });
        resolved = Array.isArray(evaluated) ? evaluated : [evaluated];
      } else {
        resolved = cube.viewGroups;
      }
      for (const n of resolved) {
        if (!groupNames.includes(n)) {
          groupNames.push(n);
        }
      }
    }

    return groupNames;
  }

  /**
   * Recursively maps each view to the most-specific group it directly belongs
   * to. A view that is a direct member of a nested group maps to that nested
   * group's name, not the ancestor chain.
   */
  private collectViewToGroups(group: CompiledViewGroup): void {
    for (const viewName of group.views) {
      let groups = this.viewToGroups.get(viewName);
      if (!groups) {
        groups = [];
        this.viewToGroups.set(viewName, groups);
      }
      if (!groups.includes(group.name)) {
        groups.push(group.name);
      }
    }

    for (const include of group.includes) {
      if (typeof include !== 'string') {
        this.collectViewToGroups(include);
      }
    }
  }

  public get viewGroupList(): string[] {
    return Array.from(this.viewGroupDefinitions.keys());
  }

  public get compiledViewGroups(): CompiledViewGroup[] {
    return this.resolvedViewGroups;
  }

  public viewGroupsForView(viewName: string): string[] {
    return this.viewToGroups.get(viewName) || [];
  }
}
