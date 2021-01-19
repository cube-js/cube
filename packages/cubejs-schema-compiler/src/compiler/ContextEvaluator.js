import R from 'ramda';

export class ContextEvaluator {
  constructor(cubeEvaluator) {
    this.cubeEvaluator = cubeEvaluator;

    this.contextDefinitions = {};
  }

  // eslint-disable-next-line no-unused-vars
  compile(contexts, errorReporter) {
    if (contexts.length === 0) {
      return;
    }

    // TODO: handle duplications, context names must be uniq
    this.contextDefinitions = R.compose(
      R.fromPairs,
      R.map(v => [v.name, this.compileContext(v)])
    )(contexts);
  }

  compileContext(context) {
    return {
      name: context.name,
      contextMembers: this.cubeEvaluator.evaluateReferences(null, context.contextMembers)
    };
  }

  get contextList() {
    return R.keys(this.contextDefinitions);
  }
}
