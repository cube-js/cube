const Joi = require('joi');
const inlection = require('inflection');
const humps = require('humps');

const identifier = Joi.string().regex(/^[_a-zA-Z][_a-zA-Z0-9]*$/, 'identifier');

const dashboardItemSchema = Joi.object().keys({
  title: Joi.string(),
  description: Joi.string(),
  measures: Joi.func(),
  dimensions: Joi.func(),
  segments: Joi.func(),
  order: Joi.array().items(Joi.object().keys({
    member: Joi.func().required(),
    direction: Joi.any().valid('asc', 'desc').required()
  })),
  filters: Joi.array().items(Joi.object().keys({
    member: Joi.func().required(),
    operator: Joi.any().valid('contains', 'notContains', 'equals', 'set', 'notSet', 'gt', 'gte', 'lt', 'lte'),
    params: Joi.array().items(Joi.string().allow('').optional())
  })),
  timeDimension: Joi.object().keys({
    dimension: Joi.func().required(),
    dateRange: Joi.string().required(),
    granularity: Joi.any().valid('hour', 'day', 'week', 'month', 'year', null)
  }),
  visualization: Joi.object().keys({
    type: Joi.any().valid('bar', 'line', 'table', 'area', 'singleValue', 'pie').required(),
    autoScale: Joi.boolean(),
    showTotal: Joi.boolean(),
    y2Axis: Joi.boolean(),
    showLegend: Joi.boolean(),
    axisRotated: Joi.boolean(),
    showYLabel: Joi.boolean(),
    showY2Label: Joi.boolean(),
    showTrendline: Joi.boolean(),
    trendlineType: Joi.any().valid('linear', 'rolling'),
    trendlinePeriod: Joi.number(),
    showComparison: Joi.boolean(),
    showRowNumbers: Joi.boolean(),
    showBarChartSteps: Joi.boolean(),
    seriesPositioning: Joi.any().valid('stacked', 'grouped', 'proportional')
  }),
  pivot: Joi.object().keys({
    x: Joi.array(),
    y: Joi.array()
  }),
  layout: Joi.object().keys({
    w: Joi.any().valid(...Array(19).fill(0).map((_,i) => i + 6)).required(),
    h: Joi.any().valid(...Array(47).fill(0).map((_,i) => i + 4)).required(),
    x: Joi.any().valid(...Array(24).fill(0).map((_,i) => i)).required(),
    y: Joi.number().required()
  }).required()
});

const dashboardTemplateSchema = Joi.object().keys({
  name: identifier,
  description: Joi.string(),
  fileName: Joi.string(),
  title: Joi.string(),
  items: Joi.array().items(dashboardItemSchema)
});

class DashboardTemplateEvaluator {
  constructor(cubeEvaluator) {
    this.cubeEvaluator = cubeEvaluator;
    this.compiledTemplates = [];
  }

  compile(dashboardTemplates, errorReporter) {
    return dashboardTemplates.forEach((template) =>
      this.validateAndCompile(template, errorReporter.inContext(`${template.name} dashboard template`))
    );
  }

  validateAndCompile(dashboardTemplate, errorReporter) {
    Joi.validate(dashboardTemplate, dashboardTemplateSchema, (err) => {
      if (err) {
        errorReporter.error(err.message);
      } else {
        this.compiledTemplates.push(this.compileTemplate(dashboardTemplate, errorReporter));
      }
    });
  }

  compileTemplate(dashboardTemplate, errorReporter) {
    return {
      ...dashboardTemplate,
      title: dashboardTemplate.title || inlection.titleize(dashboardTemplate.name),
      items: (dashboardTemplate.items || []).map(item => this.compileItem(item, errorReporter))
    }
  }

  compileItem(itemTemplate, errorReporter) {
    if (!itemTemplate.measures && !itemTemplate.dimensions) {
      errorReporter.error(`Either measures or dimensions should be declared for valid query`);
      return;
    }
    const { type, ...restVisualizationParams } = itemTemplate.visualization;
    const config = {
      visualizationType: humps.decamelize(type),
      ...restVisualizationParams,
      name: itemTemplate.title,
      pivotMarkup: itemTemplate.pivot,
      description: itemTemplate.description
    };
    const layout = { ...itemTemplate.layout };
    if (itemTemplate.measures) {
      config.metrics = this.cubeEvaluator.evaluateReferences(null, itemTemplate.measures);
    }
    if (itemTemplate.dimensions) {
      config.dimension = this.cubeEvaluator.evaluateReferences(null, itemTemplate.dimensions);
    }
    if (itemTemplate.segments) {
      config.segments = this.cubeEvaluator.evaluateReferences(null, itemTemplate.segments);
    }
    if (itemTemplate.order) {
      config.order = itemTemplate.order.map(f => ({
        desc: f.direction === 'desc',
        id: this.cubeEvaluator.evaluateReferences(null, f.member)
      }));
    }
    if (itemTemplate.filters) {
      config.filters = itemTemplate.filters.map(f => ({
        value: f.params,
        operator: inlection.underscore(f.operator),
        dimension: this.cubeEvaluator.evaluateReferences(null, f.member)
      }));
    }
    if (itemTemplate.timeDimension) {
      config.daterange = itemTemplate.timeDimension.dateRange;
      config.granularity = itemTemplate.timeDimension.granularity;
      config.timeDimensionField = this.cubeEvaluator.evaluateReferences(null, itemTemplate.timeDimension.dimension);
    }
    return humps.decamelizeKeys({ config, layout });
  }
}

module.exports = DashboardTemplateEvaluator;
