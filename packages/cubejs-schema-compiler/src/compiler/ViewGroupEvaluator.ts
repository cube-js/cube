import type { CubeEvaluator } from './CubeEvaluator';
import type { ErrorReporter } from './ErrorReporter';
import { CompilerInterface } from './PrepareCompiler';

export interface ViewGroupInput {
  name: string;
  title?: string;
  description?: string;
  views?: string[] | (() => string[]);
  fileName?: string;
}

export interface CompiledViewGroup {
  name: string;
  title?: string;
  description?: string;
  views: string[];
}

export class ViewGroupEvaluator implements CompilerInterface {
  private readonly cubeEvaluator: CubeEvaluator;

  private viewGroupDefinitions: Map<string, CompiledViewGroup>;

  private resolvedViewGroups: CompiledViewGroup[];

  private viewToGroups: Map<string, string[]>;

  public constructor(cubeEvaluator: CubeEvaluator) {
    this.cubeEvaluator = cubeEvaluator;
    this.viewGroupDefinitions = new Map<string, CompiledViewGroup>();
    this.resolvedViewGroups = [];
    this.viewToGroups = new Map();
  }

  public compile(viewGroups: ViewGroupInput[], errorReporter?: ErrorReporter): void {
    this.viewGroupDefinitions = new Map<string, CompiledViewGroup>();

    for (const viewGroup of viewGroups) {
      if (errorReporter && this.viewGroupDefinitions.has(viewGroup.name)) {
        errorReporter.error(`View group "${viewGroup.name}" already exists!`);
      } else {
        this.viewGroupDefinitions.set(viewGroup.name, this.compileViewGroup(viewGroup));
      }
    }

    this.resolve(errorReporter);
  }

  private compileViewGroup(viewGroup: ViewGroupInput): CompiledViewGroup {
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
    };
  }

  private resolve(errorReporter?: ErrorReporter): void {
    const viewGroupMap = new Map<string, CompiledViewGroup>();
    const validViewNames = new Set<string>();

    for (const cube of this.cubeEvaluator.cubeList) {
      if (cube.isView) {
        validViewNames.add(cube.name);
      }
    }

    for (const [name, def] of this.viewGroupDefinitions) {
      viewGroupMap.set(name, {
        name: def.name,
        title: def.title,
        description: def.description,
        views: def.views.filter(v => validViewNames.has(v)),
      });
    }

    for (const cube of this.cubeEvaluator.cubeList) {
      if (!cube.isView) {
        // eslint-disable-next-line no-continue
        continue;
      }

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

      for (const groupName of groupNames) {
        const group = viewGroupMap.get(groupName);
        if (!group) {
          if (errorReporter) {
            errorReporter.error(`View "${cube.name}" references view group "${groupName}" which is not defined. Define it using view_group('${groupName}', { ... }).`);
          }
        } else if (!group.views.includes(cube.name)) {
          group.views.push(cube.name);
        }
      }
    }

    this.resolvedViewGroups = Array.from(viewGroupMap.values());

    this.viewToGroups = new Map();
    for (const group of this.resolvedViewGroups) {
      for (const viewName of group.views) {
        let groups = this.viewToGroups.get(viewName);
        if (!groups) {
          groups = [];
          this.viewToGroups.set(viewName, groups);
        }
        groups.push(group.name);
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
