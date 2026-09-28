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
  private readonly cubeEvaluator: CubeEvaluator;

  private contextDefinitions: Map<string, CompiledContext>;

  public constructor(cubeEvaluator: CubeEvaluator) {
    this.cubeEvaluator = cubeEvaluator;
    this.contextDefinitions = new Map<string, CompiledContext>();
  }

  public compile(contexts: ContextInput[], errorReporter?: ErrorReporter): void {
    if (contexts.length === 0) {
      return;
    }

    this.contextDefinitions = new Map<string, CompiledContext>();
    for (const context of contexts) {
      if (errorReporter && this.contextDefinitions.has(context.name)) {
        errorReporter.error(`Context "${context.name}" already exists!`);
      } else {
        this.contextDefinitions.set(context.name, this.compileContext(context));
      }
    }
  }

  private compileContext(context: ContextInput): CompiledContext {
    return {
      name: context.name,
      contextMembers: this.cubeEvaluator.evaluateReferences(null, context.contextMembers)
    };
  }

  public get contextList(): string[] {
    return Array.from(this.contextDefinitions.keys());
  }
}
