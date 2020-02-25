module.exports = {
  type: "object",
  properties: {
    name: { type: "string", pattern: "^[_a-zA-Z][_a-zA-Z0-9]*$" },
    sql: {
      type: "object",
      properties: {},
      additionalProperties: true,
      patterns: []
    },
    refreshKey: {
      oneOf: [
        {
          type: "object",
          properties: {
            sql: {
              type: "object",
              properties: {},
              additionalProperties: true,
              patterns: []
            }
          },
          additionalProperties: false,
          patterns: [],
          required: ["sql"]
        },
        {
          type: "object",
          properties: { immutable: { type: "boolean" } },
          additionalProperties: false,
          patterns: [],
          required: ["immutable"]
        },
        {
          type: "object",
          properties: {
            every: {
              type: "string",
              pattern: "^(\\d+) (second|minute|hour|day|week)s?$"
            }
          },
          additionalProperties: false,
          patterns: []
        }
      ]
    },
    fileName: { type: "string" },
    extends: {
      type: "object",
      properties: {},
      additionalProperties: true,
      patterns: []
    },
    allDefinitions: {
      type: "object",
      properties: {},
      additionalProperties: true,
      patterns: []
    },
    title: { type: "string" },
    sqlAlias: { type: "string" },
    dataSource: { type: "string" },
    description: { type: "string" },
    joins: {
      type: "object",
      properties: {},
      additionalProperties: true,
      patterns: [
        {
          regex: {},
          rule: {
            type: "object",
            properties: {
              sql: {
                type: "object",
                properties: {},
                additionalProperties: true,
                patterns: []
              },
              relationship: {
                enum: ["hasMany", "belongsTo", "hasOne"],
                type: ["array", "boolean", "number", "object", "string", "null"]
              }
            },
            additionalProperties: false,
            patterns: [],
            required: ["sql", "relationship"]
          }
        }
      ]
    },
    measures: {
      type: "object",
      properties: {},
      additionalProperties: true,
      patterns: [
        {
          regex: {},
          rule: {
            oneOf: [
              {
                type: "object",
                properties: {
                  aliases: { type: "array", items: { type: "string" } },
                  format: {
                    enum: ["percent", "currency", "number"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  shown: { type: "boolean" },
                  visible: { type: "boolean" },
                  cumulative: { type: "boolean" },
                  filters: {
                    type: "array",
                    items: {
                      type: "object",
                      properties: {
                        sql: {
                          type: "object",
                          properties: {},
                          additionalProperties: true,
                          patterns: []
                        }
                      },
                      additionalProperties: false,
                      patterns: [],
                      required: ["sql"]
                    }
                  },
                  title: { type: "string" },
                  description: { type: "string" },
                  rollingWindow: {
                    type: "object",
                    properties: {
                      trailing: {
                        oneOf: [
                          {
                            type: "string",
                            pattern:
                              "^(-?\\d+) (minute|hour|day|week|month|year)$"
                          },
                          {
                            enum: ["unbounded"],
                            type: [
                              "array",
                              "boolean",
                              "number",
                              "object",
                              "string",
                              "null"
                            ]
                          }
                        ]
                      },
                      leading: {
                        oneOf: [
                          {
                            type: "string",
                            pattern:
                              "^(-?\\d+) (minute|hour|day|week|month|year)$"
                          },
                          {
                            enum: ["unbounded"],
                            type: [
                              "array",
                              "boolean",
                              "number",
                              "object",
                              "string",
                              "null"
                            ]
                          }
                        ]
                      },
                      offset: {
                        enum: ["start", "end"],
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
                    patterns: []
                  },
                  drillMemberReferences: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  drillMembers: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  drillFilters: {
                    type: "array",
                    items: {
                      type: "object",
                      properties: {
                        sql: {
                          type: "object",
                          properties: {},
                          additionalProperties: true,
                          patterns: []
                        }
                      },
                      additionalProperties: false,
                      patterns: [],
                      required: ["sql"]
                    }
                  },
                  sql: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  type: {
                    enum: ["count"],
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
                patterns: [],
                required: ["type"]
              },
              {
                type: "object",
                properties: {
                  aliases: { type: "array", items: { type: "string" } },
                  format: {
                    enum: ["percent", "currency", "number"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  shown: { type: "boolean" },
                  visible: { type: "boolean" },
                  cumulative: { type: "boolean" },
                  filters: {
                    type: "array",
                    items: {
                      type: "object",
                      properties: {
                        sql: {
                          type: "object",
                          properties: {},
                          additionalProperties: true,
                          patterns: []
                        }
                      },
                      additionalProperties: false,
                      patterns: [],
                      required: ["sql"]
                    }
                  },
                  title: { type: "string" },
                  description: { type: "string" },
                  rollingWindow: {
                    type: "object",
                    properties: {
                      trailing: {
                        oneOf: [
                          {
                            type: "string",
                            pattern:
                              "^(-?\\d+) (minute|hour|day|week|month|year)$"
                          },
                          {
                            enum: ["unbounded"],
                            type: [
                              "array",
                              "boolean",
                              "number",
                              "object",
                              "string",
                              "null"
                            ]
                          }
                        ]
                      },
                      leading: {
                        oneOf: [
                          {
                            type: "string",
                            pattern:
                              "^(-?\\d+) (minute|hour|day|week|month|year)$"
                          },
                          {
                            enum: ["unbounded"],
                            type: [
                              "array",
                              "boolean",
                              "number",
                              "object",
                              "string",
                              "null"
                            ]
                          }
                        ]
                      },
                      offset: {
                        enum: ["start", "end"],
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
                    patterns: []
                  },
                  drillMemberReferences: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  drillMembers: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  drillFilters: {
                    type: "array",
                    items: {
                      type: "object",
                      properties: {
                        sql: {
                          type: "object",
                          properties: {},
                          additionalProperties: true,
                          patterns: []
                        }
                      },
                      additionalProperties: false,
                      patterns: [],
                      required: ["sql"]
                    }
                  },
                  sql: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  type: {
                    enum: [
                      "number",
                      "sum",
                      "avg",
                      "min",
                      "max",
                      "countDistinct",
                      "runningTotal",
                      "countDistinctApprox"
                    ],
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
                patterns: [],
                required: ["sql", "type"]
              }
            ]
          }
        }
      ]
    },
    dimensions: {
      type: "object",
      properties: {},
      additionalProperties: true,
      patterns: [
        {
          regex: {},
          rule: {
            oneOf: [
              {
                type: "object",
                properties: {
                  aliases: { type: "array", items: { type: "string" } },
                  type: {
                    enum: ["string", "number", "boolean", "time", "geo"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  fieldType: {
                    enum: ["string"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  valuesAsSegments: { type: "boolean" },
                  primaryKey: { type: "boolean" },
                  shown: { type: "boolean" },
                  title: { type: "string" },
                  description: { type: "string" },
                  suggestFilterValues: { type: "boolean" },
                  enableSuggestions: { type: "boolean" },
                  format: {
                    oneOf: [
                      {
                        enum: [
                          "imageUrl",
                          "link",
                          "currency",
                          "percent",
                          "number",
                          "id"
                        ],
                        type: "string"
                      },
                      {
                        type: "object",
                        properties: {
                          type: { enum: ["link"], type: "string" },
                          label: { type: "string" }
                        },
                        additionalProperties: false,
                        patterns: [],
                        required: ["label"]
                      }
                    ]
                  },
                  case: {
                    type: "object",
                    properties: {
                      when: {
                        type: "array",
                        items: {
                          type: "object",
                          properties: {
                            sql: {
                              type: "object",
                              properties: {},
                              additionalProperties: true,
                              patterns: []
                            },
                            label: {
                              oneOf: [
                                { type: "string" },
                                {
                                  type: "object",
                                  properties: {
                                    sql: {
                                      type: "object",
                                      properties: {},
                                      additionalProperties: true,
                                      patterns: []
                                    }
                                  },
                                  additionalProperties: false,
                                  patterns: [],
                                  required: ["sql"]
                                }
                              ]
                            }
                          },
                          additionalProperties: false,
                          patterns: [],
                          required: ["sql"]
                        }
                      },
                      else: {
                        type: "object",
                        properties: {
                          label: {
                            oneOf: [
                              { type: "string" },
                              {
                                type: "object",
                                properties: {
                                  sql: {
                                    type: "object",
                                    properties: {},
                                    additionalProperties: true,
                                    patterns: []
                                  }
                                },
                                additionalProperties: false,
                                patterns: [],
                                required: ["sql"]
                              }
                            ]
                          }
                        },
                        additionalProperties: false,
                        patterns: []
                      }
                    },
                    additionalProperties: false,
                    patterns: []
                  }
                },
                additionalProperties: false,
                patterns: [],
                required: ["type", "case"]
              },
              {
                type: "object",
                properties: {
                  aliases: { type: "array", items: { type: "string" } },
                  type: {
                    enum: ["string", "number", "boolean", "time", "geo"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  fieldType: {
                    enum: ["string"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  valuesAsSegments: { type: "boolean" },
                  primaryKey: { type: "boolean" },
                  shown: { type: "boolean" },
                  title: { type: "string" },
                  description: { type: "string" },
                  suggestFilterValues: { type: "boolean" },
                  enableSuggestions: { type: "boolean" },
                  format: {
                    oneOf: [
                      {
                        enum: [
                          "imageUrl",
                          "link",
                          "currency",
                          "percent",
                          "number",
                          "id"
                        ],
                        type: "string"
                      },
                      {
                        type: "object",
                        properties: {
                          type: { enum: ["link"], type: "string" },
                          label: { type: "string" }
                        },
                        additionalProperties: false,
                        patterns: [],
                        required: ["label"]
                      }
                    ]
                  },
                  latitude: {
                    type: "object",
                    properties: {
                      sql: {
                        type: "object",
                        properties: {},
                        additionalProperties: true,
                        patterns: []
                      }
                    },
                    additionalProperties: false,
                    patterns: [],
                    required: ["sql"]
                  },
                  longitude: {
                    type: "object",
                    properties: {
                      sql: {
                        type: "object",
                        properties: {},
                        additionalProperties: true,
                        patterns: []
                      }
                    },
                    additionalProperties: false,
                    patterns: [],
                    required: ["sql"]
                  }
                },
                additionalProperties: false,
                patterns: [],
                required: ["type"]
              },
              {
                type: "object",
                properties: {
                  subQuery: { type: "boolean" },
                  aliases: { type: "array", items: { type: "string" } },
                  type: {
                    enum: ["string", "number", "boolean", "time", "geo"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  fieldType: {
                    enum: ["string"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  valuesAsSegments: { type: "boolean" },
                  primaryKey: { type: "boolean" },
                  shown: { type: "boolean" },
                  title: { type: "string" },
                  description: { type: "string" },
                  suggestFilterValues: { type: "boolean" },
                  enableSuggestions: { type: "boolean" },
                  format: {
                    oneOf: [
                      {
                        enum: [
                          "imageUrl",
                          "link",
                          "currency",
                          "percent",
                          "number",
                          "id"
                        ],
                        type: "string"
                      },
                      {
                        type: "object",
                        properties: {
                          type: { enum: ["link"], type: "string" },
                          label: { type: "string" }
                        },
                        additionalProperties: false,
                        patterns: [],
                        required: ["label"]
                      }
                    ]
                  },
                  sql: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  }
                },
                additionalProperties: false,
                patterns: [],
                required: ["type", "sql"]
              }
            ]
          }
        }
      ]
    },
    segments: {
      type: "object",
      properties: {},
      additionalProperties: true,
      patterns: [
        {
          regex: {},
          rule: {
            type: "object",
            properties: {
              aliases: { type: "array", items: { type: "string" } },
              sql: {
                type: "object",
                properties: {},
                additionalProperties: true,
                patterns: []
              },
              title: { type: "string" },
              description: { type: "string" }
            },
            additionalProperties: false,
            patterns: [],
            required: ["sql"]
          }
        }
      ]
    },
    preAggregations: {
      type: "object",
      properties: {},
      additionalProperties: true,
      patterns: [
        {
          regex: {},
          rule: {
            oneOf: [
              {
                type: "object",
                properties: {
                  refreshKey: {
                    oneOf: [
                      {
                        type: "object",
                        properties: {
                          sql: {
                            type: "object",
                            properties: {},
                            additionalProperties: true,
                            patterns: []
                          }
                        },
                        additionalProperties: false,
                        patterns: [],
                        required: ["sql"]
                      },
                      {
                        type: "object",
                        properties: {
                          every: {
                            type: "string",
                            pattern: "^(\\d+) (second|minute|hour|day|week)s?$"
                          },
                          incremental: { type: "boolean" },
                          updateWindow: {
                            oneOf: [
                              {
                                type: "string",
                                pattern:
                                  "^(-?\\d+) (minute|hour|day|week|month|year)$"
                              },
                              {
                                enum: ["unbounded"],
                                type: [
                                  "array",
                                  "boolean",
                                  "number",
                                  "object",
                                  "string",
                                  "null"
                                ]
                              }
                            ]
                          }
                        },
                        additionalProperties: false,
                        patterns: []
                      }
                    ]
                  },
                  useOriginalSqlPreAggregations: { type: "boolean" },
                  external: { type: "boolean" },
                  partitionGranularity: {
                    enum: ["day", "week", "month", "year"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  scheduledRefresh: { type: "boolean" },
                  indexes: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: [
                      {
                        regex: {},
                        rule: {
                          oneOf: [
                            {
                              type: "object",
                              properties: {
                                sql: {
                                  type: "object",
                                  properties: {},
                                  additionalProperties: true,
                                  patterns: []
                                }
                              },
                              additionalProperties: false,
                              patterns: [],
                              required: ["sql"]
                            },
                            {
                              type: "object",
                              properties: {
                                columns: {
                                  type: "object",
                                  properties: {},
                                  additionalProperties: true,
                                  patterns: []
                                }
                              },
                              additionalProperties: false,
                              patterns: [],
                              required: ["columns"]
                            }
                          ]
                        }
                      }
                    ]
                  },
                  type: {
                    enum: ["autoRollup"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  maxPreAggregations: { type: "number" }
                },
                additionalProperties: false,
                patterns: [],
                required: ["type"]
              },
              {
                type: "object",
                properties: {
                  refreshKey: {
                    oneOf: [
                      {
                        type: "object",
                        properties: {
                          sql: {
                            type: "object",
                            properties: {},
                            additionalProperties: true,
                            patterns: []
                          }
                        },
                        additionalProperties: false,
                        patterns: [],
                        required: ["sql"]
                      },
                      {
                        type: "object",
                        properties: {
                          every: {
                            type: "string",
                            pattern: "^(\\d+) (second|minute|hour|day|week)s?$"
                          },
                          incremental: { type: "boolean" },
                          updateWindow: {
                            oneOf: [
                              {
                                type: "string",
                                pattern:
                                  "^(-?\\d+) (minute|hour|day|week|month|year)$"
                              },
                              {
                                enum: ["unbounded"],
                                type: [
                                  "array",
                                  "boolean",
                                  "number",
                                  "object",
                                  "string",
                                  "null"
                                ]
                              }
                            ]
                          }
                        },
                        additionalProperties: false,
                        patterns: []
                      }
                    ]
                  },
                  useOriginalSqlPreAggregations: { type: "boolean" },
                  external: { type: "boolean" },
                  partitionGranularity: {
                    enum: ["day", "week", "month", "year"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  scheduledRefresh: { type: "boolean" },
                  indexes: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: [
                      {
                        regex: {},
                        rule: {
                          oneOf: [
                            {
                              type: "object",
                              properties: {
                                sql: {
                                  type: "object",
                                  properties: {},
                                  additionalProperties: true,
                                  patterns: []
                                }
                              },
                              additionalProperties: false,
                              patterns: [],
                              required: ["sql"]
                            },
                            {
                              type: "object",
                              properties: {
                                columns: {
                                  type: "object",
                                  properties: {},
                                  additionalProperties: true,
                                  patterns: []
                                }
                              },
                              additionalProperties: false,
                              patterns: [],
                              required: ["columns"]
                            }
                          ]
                        }
                      }
                    ]
                  },
                  type: {
                    enum: ["originalSql"],
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
                patterns: [],
                required: ["type"]
              },
              {
                type: "object",
                properties: {
                  refreshKey: {
                    oneOf: [
                      {
                        type: "object",
                        properties: {
                          sql: {
                            type: "object",
                            properties: {},
                            additionalProperties: true,
                            patterns: []
                          }
                        },
                        additionalProperties: false,
                        patterns: [],
                        required: ["sql"]
                      },
                      {
                        type: "object",
                        properties: {
                          every: {
                            type: "string",
                            pattern: "^(\\d+) (second|minute|hour|day|week)s?$"
                          },
                          incremental: { type: "boolean" },
                          updateWindow: {
                            oneOf: [
                              {
                                type: "string",
                                pattern:
                                  "^(-?\\d+) (minute|hour|day|week|month|year)$"
                              },
                              {
                                enum: ["unbounded"],
                                type: [
                                  "array",
                                  "boolean",
                                  "number",
                                  "object",
                                  "string",
                                  "null"
                                ]
                              }
                            ]
                          }
                        },
                        additionalProperties: false,
                        patterns: []
                      }
                    ]
                  },
                  useOriginalSqlPreAggregations: { type: "boolean" },
                  external: { type: "boolean" },
                  partitionGranularity: {
                    enum: ["day", "week", "month", "year"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  scheduledRefresh: { type: "boolean" },
                  indexes: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: [
                      {
                        regex: {},
                        rule: {
                          oneOf: [
                            {
                              type: "object",
                              properties: {
                                sql: {
                                  type: "object",
                                  properties: {},
                                  additionalProperties: true,
                                  patterns: []
                                }
                              },
                              additionalProperties: false,
                              patterns: [],
                              required: ["sql"]
                            },
                            {
                              type: "object",
                              properties: {
                                columns: {
                                  type: "object",
                                  properties: {},
                                  additionalProperties: true,
                                  patterns: []
                                }
                              },
                              additionalProperties: false,
                              patterns: [],
                              required: ["columns"]
                            }
                          ]
                        }
                      }
                    ]
                  },
                  type: {
                    enum: ["rollup"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  measureReferences: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  dimensionReferences: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  segmentReferences: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  }
                },
                additionalProperties: false,
                patterns: [],
                required: ["type"]
              },
              {
                type: "object",
                properties: {
                  refreshKey: {
                    oneOf: [
                      {
                        type: "object",
                        properties: {
                          sql: {
                            type: "object",
                            properties: {},
                            additionalProperties: true,
                            patterns: []
                          }
                        },
                        additionalProperties: false,
                        patterns: [],
                        required: ["sql"]
                      },
                      {
                        type: "object",
                        properties: {
                          every: {
                            type: "string",
                            pattern: "^(\\d+) (second|minute|hour|day|week)s?$"
                          },
                          incremental: { type: "boolean" },
                          updateWindow: {
                            oneOf: [
                              {
                                type: "string",
                                pattern:
                                  "^(-?\\d+) (minute|hour|day|week|month|year)$"
                              },
                              {
                                enum: ["unbounded"],
                                type: [
                                  "array",
                                  "boolean",
                                  "number",
                                  "object",
                                  "string",
                                  "null"
                                ]
                              }
                            ]
                          }
                        },
                        additionalProperties: false,
                        patterns: []
                      }
                    ]
                  },
                  useOriginalSqlPreAggregations: { type: "boolean" },
                  external: { type: "boolean" },
                  partitionGranularity: {
                    enum: ["day", "week", "month", "year"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  scheduledRefresh: { type: "boolean" },
                  indexes: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: [
                      {
                        regex: {},
                        rule: {
                          oneOf: [
                            {
                              type: "object",
                              properties: {
                                sql: {
                                  type: "object",
                                  properties: {},
                                  additionalProperties: true,
                                  patterns: []
                                }
                              },
                              additionalProperties: false,
                              patterns: [],
                              required: ["sql"]
                            },
                            {
                              type: "object",
                              properties: {
                                columns: {
                                  type: "object",
                                  properties: {},
                                  additionalProperties: true,
                                  patterns: []
                                }
                              },
                              additionalProperties: false,
                              patterns: [],
                              required: ["columns"]
                            }
                          ]
                        }
                      }
                    ]
                  },
                  type: {
                    enum: ["rollup"],
                    type: [
                      "array",
                      "boolean",
                      "number",
                      "object",
                      "string",
                      "null"
                    ]
                  },
                  measureReferences: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  dimensionReferences: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  segmentReferences: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  timeDimensionReference: {
                    type: "object",
                    properties: {},
                    additionalProperties: true,
                    patterns: []
                  },
                  granularity: {
                    enum: [
                      "second",
                      "minute",
                      "hour",
                      "day",
                      "week",
                      "month",
                      "year"
                    ],
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
                patterns: [],
                required: ["type", "timeDimensionReference", "granularity"]
              }
            ]
          }
        }
      ]
    }
  },
  additionalProperties: false,
  patterns: [],
  required: ["sql", "fileName"]
};
