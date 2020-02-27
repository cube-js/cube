const ajv = require("ajv")();
const inlection = require("inflection");
const humps = require("humps");
const validateSchema = ajv.compile(require("./schema/dashboardItem"));

class DashboardTemplateEvaluator {
  constructor(cubeEvaluator) {
    this.cubeEvaluator = cubeEvaluator;
    this.compiledTemplates = [];
  }

  compile(dashboardTemplates, errorReporter) {
    return dashboardTemplates.forEach(template =>
      this.validateAndCompile(
        template,
        errorReporter.inContext(`${template.name} dashboard template`)
      )
    );
  }

  validateAndCompile(dashboardTemplate, errorReporter) {
    const valid = validateSchema(dashboardTemplate);
    if (!valid) {
      errorReporter.error(ajv.errorsText(validateSchema.errors));
    } else {
      this.compiledTemplates.push(
        this.compileTemplate(dashboardTemplate, errorReporter)
      );
    }
  }

  compileTemplate(dashboardTemplate, errorReporter) {
    return {
      ...dashboardTemplate,
      title:
        dashboardTemplate.title || inlection.titleize(dashboardTemplate.name),
      items: (dashboardTemplate.items || []).map(item =>
        this.compileItem(item, errorReporter)
      )
    };
  }

  compileItem(itemTemplate, errorReporter) {
    if (!itemTemplate.measures && !itemTemplate.dimensions) {
      errorReporter.error(
        `Either measures or dimensions should be declared for valid query`
      );
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
      config.metrics = this.cubeEvaluator.evaluateReferences(
        null,
        itemTemplate.measures
      );
    }
    if (itemTemplate.dimensions) {
      config.dimension = this.cubeEvaluator.evaluateReferences(
        null,
        itemTemplate.dimensions
      );
    }
    if (itemTemplate.segments) {
      config.segments = this.cubeEvaluator.evaluateReferences(
        null,
        itemTemplate.segments
      );
    }
    if (itemTemplate.order) {
      config.order = itemTemplate.order.map(f => ({
        desc: f.direction === "desc",
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
      config.timeDimensionField = this.cubeEvaluator.evaluateReferences(
        null,
        itemTemplate.timeDimension.dimension
      );
    }
    return humps.decamelizeKeys({ config, layout });
  }
}

module.exports = DashboardTemplateEvaluator;
