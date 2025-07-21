import { CubeEvaluator } from './CubeEvaluator';
import { ErrorReporter } from './ErrorReporter';

export type ContextDefinition = {
  name: string;
  contextMembers: string | string[];
};

export class ContextEvaluator {
  private cubeEvaluator: CubeEvaluator;

  public contextDefinitions: Record<string, ContextDefinition>;

  public constructor(cubeEvaluator: CubeEvaluator) {
    this.cubeEvaluator = cubeEvaluator;
    this.contextDefinitions = {};
  }

  public compile(contexts: any, errorReporter: ErrorReporter) {
    if (contexts.length === 0) {
      return;
    }

    const definitions: Record<string, ContextDefinition> = {};

    for (const v of contexts) {
      if (definitions[v.name]) {
        errorReporter.error(`Duplicate context name found: '${v.name}'`);
      } else {
        definitions[v.name] = this.compileContext(v);
      }
    }

    this.contextDefinitions = definitions;
  }

  private compileContext(context: any): ContextDefinition {
    return {
      name: context.name,
      contextMembers: this.cubeEvaluator.evaluateReferences(null, context.contextMembers)
    };
  }

  public get contextList(): string[] {
    return Object.keys(this.contextDefinitions);
  }
}
