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
  private viewGroupDefinitions: Map<string, CompiledViewGroup>;

  public constructor() {
    this.viewGroupDefinitions = new Map<string, CompiledViewGroup>();
  }

  public compile(viewGroups: ViewGroupInput[], errorReporter?: ErrorReporter): void {
    if (viewGroups.length === 0) {
      return;
    }

    this.viewGroupDefinitions = new Map<string, CompiledViewGroup>();
    for (const viewGroup of viewGroups) {
      if (errorReporter && this.viewGroupDefinitions.has(viewGroup.name)) {
        errorReporter.error(`View group "${viewGroup.name}" already exists!`);
      } else {
        this.viewGroupDefinitions.set(viewGroup.name, this.compileViewGroup(viewGroup));
      }
    }
  }

  private compileViewGroup(viewGroup: ViewGroupInput): CompiledViewGroup {
    let views: string[] = [];
    if (viewGroup.views) {
      if (typeof viewGroup.views === 'function') {
        views = viewGroup.views();
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

  public get viewGroupList(): string[] {
    return Array.from(this.viewGroupDefinitions.keys());
  }

  public get compiledViewGroups(): CompiledViewGroup[] {
    return Array.from(this.viewGroupDefinitions.values());
  }
}
