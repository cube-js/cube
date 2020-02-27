module.exports = {
  type: "object",
  properties: {
    name: { type: "string", pattern: "^[_a-zA-Z][_a-zA-Z0-9]*$" },
    description: { type: "string" },
    fileName: { type: "string" },
    title: { type: "string" },
    items: {
      type: "array",
      items: {
        type: "object",
        properties: {
          title: { type: "string" },
          description: { type: "string" },
          measures: {
            type: "object"
          },
          dimensions: {
            type: "object"
          },
          segments: {
            type: "object"
          },
          order: {
            type: "array",
            items: {
              type: "object",
              properties: {
                member: {
                  type: "object"
                },
                direction: {
                  enum: ["asc", "desc"],
                  type: [
                    "array",
                    "boolean",
                    "number",
                    "object",
                    "string",
                    "null"
                  ]
                }
              },
              additionalProperties: false,
              required: ["member", "direction"]
            }
          },
          filters: {
            type: "array",
            items: {
              type: "object",
              properties: {
                member: {
                  type: "object"
                },
                operator: {
                  enum: [
                    "contains",
                    "notContains",
                    "equals",
                    "set",
                    "notSet",
                    "gt",
                    "gte",
                    "lt",
                    "lte"
                  ],
                  type: [
                    "array",
                    "boolean",
                    "number",
                    "object",
                    "string",
                    "null"
                  ]
                },
                params: {
                  type: "array",
                  items: {
                    anyOf: [{ type: "string", enum: [""] }, { type: "string" }]
                  }
                }
              },
              additionalProperties: false,
              required: ["member"]
            }
          },
          timeDimension: {
            type: "object",
            properties: {
              dimension: {
                type: "object"
              },
              dateRange: { type: "string" },
              granularity: {
                enum: ["hour", "day", "week", "month", "year", null],
                type: ["array", "boolean", "number", "object", "string", "null"]
              }
            },
            additionalProperties: false,
            required: ["dimension", "dateRange"]
          },
          visualization: {
            type: "object",
            properties: {
              type: {
                enum: ["bar", "line", "table", "area", "singleValue", "pie"],
                type: ["array", "boolean", "number", "object", "string", "null"]
              },
              autoScale: { type: "boolean" },
              showTotal: { type: "boolean" },
              y2Axis: { type: "boolean" },
              showLegend: { type: "boolean" },
              axisRotated: { type: "boolean" },
              showYLabel: { type: "boolean" },
              showY2Label: { type: "boolean" },
              showTrendline: { type: "boolean" },
              trendlineType: {
                enum: ["linear", "rolling"],
                type: ["array", "boolean", "number", "object", "string", "null"]
              },
              trendlinePeriod: { type: "number" },
              showComparison: { type: "boolean" },
              showRowNumbers: { type: "boolean" },
              showBarChartSteps: { type: "boolean" },
              seriesPositioning: {
                enum: ["stacked", "grouped", "proportional"],
                type: ["array", "boolean", "number", "object", "string", "null"]
              }
            },
            additionalProperties: false,
            required: ["type"]
          },
          pivot: {
            type: "object",
            properties: { x: { type: "array" }, y: { type: "array" } },
            additionalProperties: false
          },
          layout: {
            type: "object",
            properties: {
              w: {
                enum: [
                  6,
                  7,
                  8,
                  9,
                  10,
                  11,
                  12,
                  13,
                  14,
                  15,
                  16,
                  17,
                  18,
                  19,
                  20,
                  21,
                  22,
                  23,
                  24
                ],
                type: ["array", "boolean", "number", "object", "string", "null"]
              },
              h: {
                enum: [
                  4,
                  5,
                  6,
                  7,
                  8,
                  9,
                  10,
                  11,
                  12,
                  13,
                  14,
                  15,
                  16,
                  17,
                  18,
                  19,
                  20,
                  21,
                  22,
                  23,
                  24,
                  25,
                  26,
                  27,
                  28,
                  29,
                  30,
                  31,
                  32,
                  33,
                  34,
                  35,
                  36,
                  37,
                  38,
                  39,
                  40,
                  41,
                  42,
                  43,
                  44,
                  45,
                  46,
                  47,
                  48,
                  49,
                  50
                ],
                type: ["array", "boolean", "number", "object", "string", "null"]
              },
              x: {
                enum: [
                  0,
                  1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7,
                  8,
                  9,
                  10,
                  11,
                  12,
                  13,
                  14,
                  15,
                  16,
                  17,
                  18,
                  19,
                  20,
                  21,
                  22,
                  23
                ],
                type: ["array", "boolean", "number", "object", "string", "null"]
              },
              y: { type: "number" }
            },
            additionalProperties: false,
            required: ["w", "h", "x", "y"]
          }
        },
        additionalProperties: false,
        required: ["layout"]
      }
    }
  },
  additionalProperties: false
};
