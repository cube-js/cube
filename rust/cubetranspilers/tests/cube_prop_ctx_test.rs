mod common;

// Recommended strategy to test plugin's transform is verify
// the Visitor's behavior, instead of trying to run `process_transform` with mocks
// unless explicitly required to do so.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};

use common::{generate_code, TestEmitter};
use cubetranspilers::cube_prop_ctx_transpiler::*;
use insta::assert_snapshot;
use swc_core::ecma::ast::{EsVersion, Program};
use swc_core::{
    common::{
        errors::{Handler, HandlerFlags},
        sync::Lrc,
        FileName, SourceMap,
    },
    ecma::visit::VisitMutWith,
};
use swc_ecma_parser::{lexer::Lexer, Parser, StringInput, Syntax};

static CONTEXT_SYMBOLS: LazyLock<HashMap<String, String>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    map.insert(
        "SECURITY_CONTEXT".to_string(),
        "securityContext".to_string(),
    );
    map.insert(
        "security_context".to_string(),
        "securityContext".to_string(),
    );
    map.insert("securityContext".to_string(), "securityContext".to_string());
    map.insert("FILTER_PARAMS".to_string(), "filterParams".to_string());
    map.insert("FILTER_GROUP".to_string(), "filterGroup".to_string());
    map.insert("SQL_UTILS".to_string(), "sqlUtils".to_string());
    map
});

#[test]
fn test_incorrect_args_to_cube() {
    let cm: Lrc<SourceMap> = Default::default();
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let emitter = Box::new(TestEmitter {
        diagnostics: diagnostics.clone(),
    });
    let handler = Handler::with_emitter_and_flags(
        emitter,
        HandlerFlags {
            can_emit_warnings: true,
            ..Default::default()
        },
    );

    let js_code = r#"
            cube(`cube1`, { sql: `xxx` }, 25);
        "#;

    let fm = cm.new_source_file(
        Arc::new(FileName::Custom("input.js".into())),
        js_code.into(),
    );
    let lexer = Lexer::new(
        Syntax::Es(Default::default()),
        EsVersion::Es2020,
        StringInput::from(&*fm),
        None,
    );
    let mut parser = Parser::new_from(lexer);
    let mut program: Program = parser.parse_program().expect("Failed to parse the JS code");

    let mut visitor = CubePropTransformVisitor::new(
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        None,
        &handler,
    );
    program.visit_mut_with(&mut visitor);

    let _output_code = generate_code(&program, &cm);
    let diags = diagnostics.lock().unwrap();
    let msgs: Vec<_> = diags
        .iter()
        .filter(|msg| msg.contains("Incorrect number of arguments"))
        .collect();
    assert!(msgs.len() > 0, "Should emit errors",);
}

#[test]
fn test_simple_transform() {
    let cm: Lrc<SourceMap> = Default::default();
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let emitter = Box::new(TestEmitter {
        diagnostics: diagnostics.clone(),
    });
    let handler = Handler::with_emitter_and_flags(
        emitter,
        HandlerFlags {
            can_emit_warnings: true,
            ..Default::default()
        },
    );

    let js_code = r#"
            cube(`cube1`, {
                sql: `SELECT * FROM table`,

                dimensions: {
                    id: {
                        sql: `id`,
                        type: `number`,
                        primary_key: true,
                    },
                    created_at: {
                        sql: `created_at`,
                        type: `time`,
                    },
                    dim1Number: {
                        sql: `dim1Number`,
                        type: `number`,
                    },
                    dim2Number: {
                        sql: `dim2Number`,
                        type: `number`,
                    },
                },

                measures: {
                    count: {
                        type: `count`,
                        sql: `id`,
                    },
                    measureDim1: {
                        sql: `dim1Number`,
                        type:
                            `max`,
                    },
                    measureDim2: {
                        sql: `dim1Number`,
                        type: `min`,
                    },
                },
            });
            "#;
    // Should generate
    // cube(`cube1`, {
    //   sql: () => `SELECT *
    //           FROM table`,
    //   dimensions: {
    //     id: {
    //       sql: () => `id`,
    //       type: `number`,
    //       primary_key: true
    //     },
    //     created_at: {
    //       sql: () => `created_at`,
    //       type: `time`
    //     },
    //     dim1Number: {
    //       sql: () => `dim1Number`,
    //       type: `number`
    //     },
    //     dim2Number: {
    //       sql: () => `dim2Number`,
    //       type: `number`
    //     }
    //   },
    //   measures: {
    //     count: {
    //       type: `count`,
    //       sql: () => `id`
    //     },
    //     measureDim1: {
    //       sql: () => `dim1Number`,
    //       type: `max`
    //     },
    //     measureDim2: {
    //       sql: () => `dim1Number`,
    //       type: `min`
    //     }
    //   }
    // });

    let fm = cm.new_source_file(
        Arc::new(FileName::Custom("input.js".into())),
        js_code.into(),
    );
    let lexer = Lexer::new(
        Syntax::Es(Default::default()),
        EsVersion::Es2020,
        StringInput::from(&*fm),
        None,
    );
    let mut parser = Parser::new_from(lexer);
    let mut program: Program = parser.parse_program().expect("Failed to parse the JS code");

    let mut visitor = CubePropTransformVisitor::new(
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        None,
        &handler,
    );
    program.visit_mut_with(&mut visitor);

    let output_code = generate_code(&program, &cm);

    assert_snapshot!("simple_transform", output_code);

    let diags = diagnostics.lock().unwrap();
    assert!(
        diags.is_empty(),
        "Should not emit errors, got: {:?}",
        *diags
    );
}

#[test]
fn test_complicated_transform_1st_stage() {
    let cm: Lrc<SourceMap> = Default::default();
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let emitter = Box::new(TestEmitter {
        diagnostics: diagnostics.clone(),
    });
    let handler = Handler::with_emitter_and_flags(
        emitter,
        HandlerFlags {
            can_emit_warnings: true,
            ..Default::default()
        },
    );

    let js_code = r#"
            cube(`Orders`, {
              sql: `
               SELECT *
                    FROM public.orders
                    WHERE ${FILTER_GROUP(
                      FILTER_PARAMS.Orders.status.filter('status')
                    )}
              `,
              preAggregations: {
                main_test_range: {
                  measures: [count, rolling_count_month],
                  dimensions: [status],
                  timeDimension: createdAt,
                  granularity: `day`,
                  partitionGranularity: `month`,
                  refreshKey: {
                    every: `1 day`,
                  },
                  buildRangeStart: {
                    sql: `SELECT '2021-01-01'::DATE`
                  },
                  build_range_end: {
                    sql: `SELECT '2021-12-31'::DATE`
                  }

                }
              },
              measures: {
                division_error_test: {
                  sql: `CASE WHEN ${zero_sum} = 0 THEN 1 ELSE 1/${zero_sum} end`,
                  type: `sum`
                },
                zero_sum: {
                  sql: `id`,
                  type: `sum`
                },
                rolling_count_month: {
                  sql: `id`,
                  type: `count`,
                  rollingWindow: {
                    trailing: `1 month`,
                  },
                },
                count: {
                  type: `count`,
                  drillMembers: [id, createdAt],
                  meta: {
                    test: 1
                  }
                },
                countShipped: {
                  type: `count`,
                  filters: [{
                    sql: `${CUBE}.status = 'shipped'`
                  }],
                  drillMembers: [id, createdAt]
                },
                maxDate: {
                  type: `max`,
                  sql: `${CUBE.completedAt}`,
                }
              },
              dimensions: {
                id: {
                  sql: `id`,
                  type: `number`,
                  primaryKey: true,
                  shown: true
                },
                status: {
                  sql: `status`,
                  type: `string`
                },
                createdAt: {
                  sql: `created_at`,
                  type: `time`
                },
                completedAt: {
                  sql: `completed_at`,
                  type: `time`
                },
                test_boolean: {
                  sql: `CASE WHEN status = 'completed' THEN TRUE ELSE FALSE END`,
                  type: `boolean`
                },
                localTime: {
                  type: 'time',
                  sql: SQL_UTILS.convertTz(`completed_at`)
                },
                localYear: {
                  type: 'number',
                  sql: `EXTRACT(year from ${SQL_UTILS.convertTz('completed_at')})`
                },
              },
              segments: {
                status_completed: {
                  sql: `${CUBE}.status = 'completed'`
                }
              },
              accessPolicy: [
                    {
                        role: "*",
                        rowLevel: {
                            allowAll: true,
                        },
                    },
                    {
                        role: 'admin',
                        conditions: [
                            {
                                if: `true`,
                            },
                        ],
                        rowLevel: {
                            filters: [
                                {
                                    member: `${CUBE}.id`,
                                    operator: 'equals',
                                    values: [`1`, `2`, `3`],
                                },
                            ],
                        },
                        memberLevel: {
                            includes: `*`,
                            excludes: [`localTime`, `completedAt`],
                        },
                    },
                ]
            });
            "#;
    // Should generate
    // cube(`Orders`, {
    //     sql: (FILTER_GROUP, FILTER_PARAMS) => `
    //         SELECT *
    //         FROM public.orders
    //         WHERE ${FILTER_GROUP(FILTER_PARAMS.Orders.status.filter('status'))}
    //     `,
    //     preAggregations: {
    //         main_test_range: {
    //             measures: () => [count, rolling_count_month],
    //             dimensions: () => [status],
    //             timeDimension: () => createdAt,
    //             granularity: `day`,
    //             partitionGranularity: `month`,
    //             refreshKey: {
    //                 every: `1 day`,
    //             },
    //             buildRangeStart: {
    //                 sql: () => `SELECT '2021-01-01'::DATE`,
    //             },
    //             build_range_end: {
    //                 sql: () => `SELECT '2021-12-31'::DATE`,
    //             },
    //         },
    //     },
    //     measures: {
    //         division_error_test: {
    //             sql: () => `CASE WHEN ${zero_sum} = 0 THEN 1 ELSE 1/${zero_sum} end`,
    //             type: `sum`,
    //         },
    //         zero_sum: {
    //             sql: () => `id`,
    //             type: `sum`,
    //         },
    //         rolling_count_month: {
    //             sql: () => `id`,
    //             type: `count`,
    //             rollingWindow: {
    //                 trailing: `1 month`,
    //             },
    //         },
    //         count: {
    //             type: `count`,
    //             drillMembers: () => [id, createdAt],
    //             meta: {
    //                 test: 1,
    //             },
    //         },
    //         countShipped: {
    //             type: `count`,
    //             filters: [{
    //                 sql: CUBE => `${CUBE}.status = 'shipped'`,
    //             }],
    //             drillMembers: () => [id, createdAt],
    //         },
    //         maxDate: {
    //             type: `max`,
    //             sql: CUBE => `${CUBE.completedAt}`,
    //         },
    //     },
    //     dimensions: {
    //         id: {
    //             sql: () => `id`,
    //             type: `number`,
    //             primaryKey: true,
    //             shown: true,
    //         },
    //         status: {
    //             sql: () => `status`,
    //             type: `string`,
    //         },
    //         createdAt: {
    //             sql: () => `created_at`,
    //             type: `time`,
    //         },
    //         completedAt: {
    //             sql: () => `completed_at`,
    //             type: `time`,
    //         },
    //         test_boolean: {
    //             sql: () => `CASE WHEN status = 'completed' THEN TRUE ELSE FALSE END`,
    //             type: `boolean`,
    //         },
    //         localTime: {
    //             type: 'time',
    //             sql: SQL_UTILS => SQL_UTILS.convertTz(`completed_at`),
    //         },
    //         localYear: {
    //             type: 'number',
    //             sql: SQL_UTILS => `EXTRACT(year from ${SQL_UTILS.convertTz('completed_at')})`,
    //         },
    //     },
    //     segments: {
    //         status_completed: {
    //             sql: CUBE => `${CUBE}.status = 'completed'`,
    //         },
    //     },
    //     accessPolicy: [{
    //         role: "*",
    //         rowLevel: {
    //              allowAll: true
    //         }
    //      },
    //      {
    //         role: 'admin',
    //         conditions: [{
    //              if: () => `true`
    //         }],
    //         rowLevel: {
    //          filters: [{
    //             member: CUBE => `${CUBE}.id`,
    //             operator: 'equals',
    //             values: () => [`1`, `2`, `3`]
    //          }]
    //         },
    //         memberLevel: {
    //              includes: `*`,
    //              excludes: [`localTime`, `completedAt`]
    //         }
    //     }]
    // });

    let fm = cm.new_source_file(
        Arc::new(FileName::Custom("input.js".into())),
        js_code.into(),
    );
    let lexer = Lexer::new(
        Syntax::Es(Default::default()),
        EsVersion::Es2020,
        StringInput::from(&*fm),
        None,
    );
    let mut parser = Parser::new_from(lexer);
    let mut program: Program = parser.parse_program().expect("Failed to parse the JS code");

    let mut visitor = CubePropTransformVisitor::new(
        HashSet::new(),
        HashMap::new(),
        CONTEXT_SYMBOLS.clone(),
        None,
        &handler,
    );

    program.visit_mut_with(&mut visitor);

    let output_code = generate_code(&program, &cm);

    assert_snapshot!("complicated_transform_1st_stage", output_code);

    let diags = diagnostics.lock().unwrap();
    assert!(
        diags.is_empty(),
        "Should not emit errors, got: {:?}",
        *diags
    );
}

#[test]
fn test_complicated_transform_2nd_stage() {
    let cm: Lrc<SourceMap> = Default::default();
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let emitter = Box::new(TestEmitter {
        diagnostics: diagnostics.clone(),
    });
    let handler = Handler::with_emitter_and_flags(
        emitter,
        HandlerFlags {
            can_emit_warnings: true,
            ..Default::default()
        },
    );

    let js_code = r#"
            cube(`Orders`, {
              sql: (FILTER_GROUP, FILTER_PARAMS) => `
               SELECT *
                    FROM public.orders
                    WHERE ${FILTER_GROUP(FILTER_PARAMS.Orders.status.filter('status'))}
              `,
              preAggregations: {
                main_test_range: {
                  measures: () => [count, rolling_count_month],
                  dimensions: () => [status],
                  timeDimension: () => createdAt,
                  granularity: `day`,
                  partitionGranularity: `month`,
                  refreshKey: {
                    every: `1 day`
                  },
                  buildRangeStart: {
                    sql: () => `SELECT '2021-01-01'::DATE`
                  },
                  build_range_end: {
                    sql: () => `SELECT '2021-12-31'::DATE`
                  }
                }
              },
              measures: {
                division_error_test: {
                  sql: () => `CASE WHEN ${zero_sum} = 0 THEN 1 ELSE 1/${zero_sum} end`,
                  type: `sum`
                },
                zero_sum: {
                  sql: () => `id`,
                  type: `sum`
                },
                rolling_count_month: {
                  sql: () => `id`,
                  type: `count`,
                  rollingWindow: {
                    trailing: `1 month`
                  }
                },
                count: {
                  type: `count`,
                  drillMembers: () => [id, createdAt],
                  meta: {
                    test: 1
                  }
                },
                countShipped: {
                  type: `count`,
                  filters: [{
                    sql: CUBE => `${CUBE}.status = 'shipped'`
                  }],
                  drillMembers: () => [id, createdAt]
                },
                maxDate: {
                  type: `max`,
                  sql: CUBE => `${CUBE.completedAt}`
                }
              },
              dimensions: {
                id: {
                  sql: () => `id`,
                  type: `number`,
                  primaryKey: true,
                  shown: true
                },
                status: {
                  sql: () => `status`,
                  type: `string`
                },
                createdAt: {
                  sql: () => `created_at`,
                  type: `time`
                },
                completedAt: {
                  sql: () => `completed_at`,
                  type: `time`
                },
                test_boolean: {
                  sql: () => `CASE WHEN status = 'completed' THEN TRUE ELSE FALSE END`,
                  type: `boolean`
                },
                localTime: {
                  type: 'time',
                  sql: SQL_UTILS => SQL_UTILS.convertTz(`completed_at`)
                },
                localYear: {
                  type: 'number',
                  sql: SQL_UTILS => `EXTRACT(year from ${SQL_UTILS.convertTz('completed_at')})`
                }
              },
              segments: {
                status_completed: {
                  sql: CUBE => `${CUBE}.status = 'completed'`
                }
              },
              accessPolicy: [{
                role: "*",
                rowLevel: {
                    allowAll: true
                }
              },
              {
                role: 'admin',
                conditions: [{
                    if: () => `true`
                }],
                rowLevel: {
                filters: [{
                    member: CUBE => `${CUBE}.id`,
                    operator: 'equals',
                    values: () => [`1`, `2`, `3`]
                }]
                },
                memberLevel: {
                    includes: `*`,
                    excludes: [`localTime`, `completedAt`]
                }
              }]
            });
        "#;
    // Should generate
    // cube(`Orders`, {
    //   sql: (FILTER_GROUP, FILTER_PARAMS) => `
    //    SELECT *
    //         FROM public.orders
    //         WHERE ${FILTER_GROUP(FILTER_PARAMS.Orders.status.filter('status'))}
    //   `,
    //   preAggregations: {
    //     main_test_range: {
    //       measures: (count, rolling_count_month) => [count, rolling_count_month],
    //       dimensions: status => [status],
    //       timeDimension: createdAt => createdAt,
    //       granularity: `day`,
    //       partitionGranularity: `month`,
    //       refreshKey: {
    //         every: `1 day`
    //       },
    //       buildRangeStart: {
    //         sql: () => `SELECT '2021-01-01'::DATE`
    //       },
    //       build_range_end: {
    //         sql: () => `SELECT '2021-12-31'::DATE`
    //       }
    //     }
    //   },
    //   measures: {
    //     division_error_test: {
    //       sql: zero_sum => `CASE WHEN ${zero_sum} = 0 THEN 1 ELSE 1/${zero_sum} end`,
    //       type: `sum`
    //     },
    //     zero_sum: {
    //       sql: () => `id`,
    //       type: `sum`
    //     },
    //     rolling_count_month: {
    //       sql: () => `id`,
    //       type: `count`,
    //       rollingWindow: {
    //         trailing: `1 month`
    //       }
    //     },
    //     count: {
    //       type: `count`,
    //       drillMembers: (id, createdAt) => [id, createdAt],
    //       meta: {
    //         test: 1
    //       }
    //     },
    //     countShipped: {
    //       type: `count`,
    //       filters: [{
    //         sql: CUBE => `${CUBE}.status = 'shipped'`
    //       }],
    //       drillMembers: (id, createdAt) => [id, createdAt]
    //     },
    //     maxDate: {
    //       type: `max`,
    //       sql: CUBE => `${CUBE.completedAt}`
    //     }
    //   },
    //   dimensions: {
    //     id: {
    //       sql: () => `id`,
    //       type: `number`,
    //       primaryKey: true,
    //       shown: true
    //     },
    //     status: {
    //       sql: () => `status`,
    //       type: `string`
    //     },
    //     createdAt: {
    //       sql: () => `created_at`,
    //       type: `time`
    //     },
    //     completedAt: {
    //       sql: () => `completed_at`,
    //       type: `time`
    //     },
    //     test_boolean: {
    //       sql: () => `CASE WHEN status = 'completed' THEN TRUE ELSE FALSE END`,
    //       type: `boolean`
    //     },
    //     localTime: {
    //       type: 'time',
    //       sql: SQL_UTILS => SQL_UTILS.convertTz(`completed_at`)
    //     },
    //     localYear: {
    //       type: 'number',
    //       sql: SQL_UTILS => `EXTRACT(year from ${SQL_UTILS.convertTz('completed_at')})`
    //     }
    //   },
    //   segments: {
    //     status_completed: {
    //       sql: CUBE => `${CUBE}.status = 'completed'`
    //     }
    //   },
    //   accessPolicy: [{
    //     role: "*",
    //     rowLevel: {
    //         allowAll: true
    //     }
    //   },
    //   {
    //     role: 'admin',
    //     conditions: [{
    //       if: () => `true`
    //     }],
    //     rowLevel: {
    //       filters: [{
    //         member: CUBE => `${CUBE}.id`,
    //         operator: 'equals',
    //         values: () => [`1`, `2`, `3`]
    //       }]
    //     },
    //     memberLevel: {
    //       includes: `*`,
    //       excludes: [`localTime`, `completedAt`]
    //     }
    //   }]
    // });

    let fm = cm.new_source_file(
        Arc::new(FileName::Custom("input.js".into())),
        js_code.into(),
    );
    let lexer = Lexer::new(
        Syntax::Es(Default::default()),
        EsVersion::Es2020,
        StringInput::from(&*fm),
        None,
    );
    let mut parser = Parser::new_from(lexer);
    let mut program: Program = parser.parse_program().expect("Failed to parse the JS code");
    let mut cube_names = HashSet::new();
    cube_names.insert("Orders".to_string());
    let mut cube_symbols = HashMap::<String, HashMap<String, bool>>::new();
    let mut orders_cube_symbols = HashMap::new();
    orders_cube_symbols.insert("division_error_test".to_string(), true);
    orders_cube_symbols.insert("zero_sum".to_string(), true);
    orders_cube_symbols.insert("rolling_count_month".to_string(), true);
    orders_cube_symbols.insert("count".to_string(), true);
    orders_cube_symbols.insert("countShipped".to_string(), true);
    orders_cube_symbols.insert("id".to_string(), true);
    orders_cube_symbols.insert("status".to_string(), true);
    orders_cube_symbols.insert("createdAt".to_string(), true);
    orders_cube_symbols.insert("completedAt".to_string(), true);
    orders_cube_symbols.insert("test_boolean".to_string(), true);
    orders_cube_symbols.insert("localTime".to_string(), true);
    orders_cube_symbols.insert("localYear".to_string(), true);
    orders_cube_symbols.insert("status_completed".to_string(), true);
    orders_cube_symbols.insert("main_test_range".to_string(), true);
    cube_symbols.insert("Orders".to_string(), orders_cube_symbols);

    let mut visitor = CubePropTransformVisitor::new(
        cube_names,
        cube_symbols,
        CONTEXT_SYMBOLS.clone(),
        None,
        &handler,
    );
    program.visit_mut_with(&mut visitor);

    let output_code = generate_code(&program, &cm);

    assert_snapshot!("complicated_transform_2nd_stage", output_code);

    let diags = diagnostics.lock().unwrap();
    assert!(
        diags.is_empty(),
        "Should not emit errors, got: {:?}",
        *diags
    );
}
