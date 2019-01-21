const R = require('ramda');
const inflection = require('inflection');
const BaseMeasure = require('../adapter/BaseMeasure');
const UserError = require('./UserError');

class CubeToMetaTransformer {
  constructor(cubeValidator, cubeEvaluator, contextEvaluator, joinGraph, adapter) {
    this.cubeValidator = cubeValidator;
    this.cubeSymbols = cubeEvaluator;
    this.cubeEvaluator = cubeEvaluator;
    this.contextEvaluator = contextEvaluator;
    this.joinGraph = joinGraph;
    this.adapter = adapter;
  }

  compile(cubes, errorReporter) {
    this.cubes = this.queries = this.cubeSymbols.cubeList
      .filter(this.cubeValidator.isCubeValid.bind(this.cubeValidator))
      .map((v) => this.transform(v, errorReporter.inContext(`${v.name} cube`)))
      .filter(c => !!c);
  }

  transform(cube, errorReporter) {
    const cubeTitle = cube.title || this.titleize(cube.name);
    return {
      config: {
        name: cube.name,
        title: cubeTitle,
        description: cube.description,
        connectedComponent: this.joinGraph.connectedComponents()[cube.name],
        measures: R.compose(
          R.map((nameToMetric) => this.measureConfig(cube.name, cubeTitle, nameToMetric)),
          R.filter((nameToMetric) => this.isVisible(nameToMetric[1], true)),
          R.toPairs
        )(cube.measures || {}),
        dimensions: R.compose(
          R.map((nameToDimension) => ({
            name: `${cube.name}.${nameToDimension[0]}`,
            title: this.title(cubeTitle, nameToDimension),
            type: nameToDimension[1].type,
            description: nameToDimension[1].description,
            aliasName: this.adapter.aliasName(`${cube.name}.${nameToDimension[0]}`),
            shortTitle: this.title(cubeTitle, nameToDimension, true),
            suggestFilterValues:
              nameToDimension[1].suggestFilterValues == null ? true : nameToDimension[1].suggestFilterValues,
            format: nameToDimension[1].format
          })),
          R.filter(nameToDimension =>
            this.isVisible(nameToDimension[1], !nameToDimension[1].primaryKey)
          ),
          R.toPairs
        )(cube.dimensions || {}),
        segments: R.compose(
          R.map((nameToSegment) => ({
            name: `${cube.name}.${nameToSegment[0]}`,
            title: this.title(cubeTitle, nameToSegment),
            shortTitle: this.title(cubeTitle, nameToSegment, true),
            description: nameToSegment[1].description
          })),
          R.toPairs
        )(cube.segments || {})
      }
    };
  }

  queriesForContext(contextId) {
    // return All queries if no context pass
    if (R.isNil(contextId) || R.isEmpty(contextId)) {
      return this.queries;
    }

    const context = this.contextEvaluator.contextDefinitions[contextId]

    // If contextId is wrong
    if (R.isNil(context)) {
      throw new UserError(`Context ${contextId} doesn't exist`);
    }

    // As for now context works on the cubes level
    return R.filter((query) =>
      R.contains(query.config.name, context.contextMembers)
    )(this.queries);
  }

  isVisible(symbol, defaultValue) {
    if (symbol.visible != null) {
      return symbol.visible;
    }
    if (symbol.shown != null) {
      return symbol.shown;
    }
    return defaultValue;
  }

  measureConfig(cubeName, cubeTitle, nameToMetric) {
    const name = `${cubeName}.${nameToMetric[0]}`;
    // Support both old 'drillMemberReferences' and new 'drillMembers' keys
    const drillMembers = nameToMetric[1].drillMembers || nameToMetric[1].drillMemberReferences;
    return {
      name,
      title: this.title(cubeTitle, nameToMetric),
      description: nameToMetric[1].description,
      shortTitle: this.title(cubeTitle, nameToMetric, true),
      format: nameToMetric[1].format,
      aliasName: this.adapter.aliasName(name),
      cumulativeTotal: nameToMetric[1].cumulative || BaseMeasure.isCumulative(nameToMetric[1]),
      cumulative: nameToMetric[1].cumulative || BaseMeasure.isCumulative(nameToMetric[1]),
      type: 'number', // TODO
      drillMembers: drillMembers && this.cubeEvaluator.evaluateReferences(cubeName, drillMembers, { originalSorting: true })
    };
  }

  title(cubeTitle, nameToDef, short) {
    return `${short ? '' : cubeTitle + ' '}${nameToDef[1].title || this.titleize(nameToDef[0])}`;
  }

  titleize(name) {
    return inflection.titleize(inflection.underscore(name));
  }
}

module.exports = CubeToMetaTransformer;
