module.exports = {
  type: "object",
  properties: {
    measures: {
      type: "array",
      items: { type: "string", pattern: "^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$" }
    },
    dimensions: {
      type: "array",
      items: {
        type: "string",
        pattern:
          "^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+(\\.(hour|day|week|month|year))?$"
      }
    },
    filters: {
      type: "array",
      items: {
        type: "object",
        properties: {
          dimension: {
            type: "string",
            pattern: "^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$"
          },
          member: {
            type: "string",
            pattern: "^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$"
          },
          operator: {
            enum: [
              "equals",
              "notEquals",
              "contains",
              "notContains",
              "in",
              "notIn",
              "gt",
              "gte",
              "lt",
              "lte",
              "set",
              "notSet",
              "inDateRange",
              "notInDateRange",
              "onTheDate",
              "beforeDate",
              "afterDate"
            ],
            type: ["array", "boolean", "number", "object", "string", "null"]
          },
          values: {
            type: "array",
            items: {
              anyOf: [{ type: "string", enum: ["", null] }, { type: "string" }]
            }
          }
        },
        additionalProperties: false,
        required: ["operator"]
      }
    },
    timeDimensions: {
      type: "array",
      items: {
        type: "object",
        properties: {
          dimension: {
            type: "string",
            pattern: "^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$"
          },
          granularity: {
            enum: [
              "day",
              "month",
              "year",
              "week",
              "hour",
              "minute",
              "second",
              null
            ],
            type: ["array", "boolean", "number", "object", "string", "null"]
          },
          dateRange: {
            oneOf: [
              {
                type: "array",
                minItems: 1,
                maxItems: 2,
                items: { type: "string" }
              },
              { type: "string" }
            ]
          }
        },
        additionalProperties: false,
        required: ["dimension"]
      }
    },
    order: {
      type: "object",
      patterns: [
        {
          rule: {
            enum: ["asc", "desc"],
            type: ["array", "boolean", "number", "object", "string", "null"]
          }
        }
      ]
    },
    segments: {
      type: "array",
      items: { type: "string", pattern: "^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$" }
    },
    timezone: { type: "string" },
    limit: { type: "integer", minimum: 1, maximum: 50000 },
    offset: { type: "integer", minimum: 0 },
    renewQuery: { type: "boolean" },
    ungrouped: { type: "boolean" }
  },
  additionalProperties: false
};
