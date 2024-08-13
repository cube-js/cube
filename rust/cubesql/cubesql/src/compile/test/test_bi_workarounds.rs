use cubeclient::models::V1LoadRequestQuery;

use crate::compile::{
    test::{convert_select_to_query_plan, init_testing_logger},
    DatabaseProtocol, Rewriter,
};

use super::LogicalPlanTestUtils;

#[tokio::test]
async fn test_powerbi_count_distinct_with_max_case() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan(
        r#"
        select
            "rows"."customer_gender" as "customer_gender",
            count(distinct("rows"."countDistinct")) + max(
                case
                    when "rows"."countDistinct" is null then 1
                    else 0
                end
            ) as "a0"
        from
            "public"."KibanaSampleDataEcommerce" "rows"
        group by
            "customer_gender"
        limit
            1000001
        ;"#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.countDistinct".to_string()]),
            dimensions: Some(vec!["KibanaSampleDataEcommerce.customer_gender".to_string()]),
            segments: Some(vec![]),
            time_dimensions: None,
            order: None,
            limit: Some(1000001),
            offset: None,
            filters: None,
            ungrouped: None,
        }
    )
}
