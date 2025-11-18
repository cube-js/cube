import type { CubeEvaluator } from './CubeEvaluator';
import type { ErrorReporter } from './ErrorReporter';
import { CompilerInterface } from './PrepareCompiler';

export interface ContextInput {
  name: string;
  contextMembers: any;
}

export interface CompiledContext {
  name: string;
  contextMembers: any;
}

export class ContextEvaluator implements CompilerInterface {
  private cubeEvaluator: CubeEvaluator;

  private contextDefinitions: Record<string, CompiledContext>;

  public constructor(cubeEvaluator: CubeEvaluator) {
    this.cubeEvaluator = cubeEvaluator;
    this.contextDefinitions = {};
  }

  public compile(contexts: ContextInput[], _errorReporter?: ErrorReporter): void {
    if (contexts.length === 0) {
      return;
    }

    // TODO: handle duplications, context names must be uniq
    this.contextDefinitions = {};
    for (const context of contexts) {
      this.contextDefinitions[context.name] = this.compileContext(context);
    }
  }

  private compileContext(context: ContextInput): CompiledContext {
    return {
      name: context.name,
      contextMembers: this.cubeEvaluator.evaluateReferences(null, context.contextMembers)
    };
  }

  public get contextList(): string[] {
    return Object.keys(this.contextDefinitions);
  }
}
