//! The console's semantic SQL test suite, run against this API: every
//! filter-bearing query of its parsing and generation suites, with the filters
//! the console's own parser read from it and what this API is expected to make
//! of the case. The SQL is the suite's; the expectations are translated to
//! Cube filters and classified here, so a divergence is a decision taken
//! rather than a surprise in the field.

use super::*;
use crate::compile::{
    test::{get_test_session, get_test_tenant_ctx_with_meta},
    DatabaseProtocol,
};
use cubeclient::models::{V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure, V1CubeMetaType};

/// What this API is expected to make of a case, which is not always what the
/// console's parser made of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// The same filters, member for member.
    Match,
    /// The rewrite engine merges two bounds on one time member into a single
    /// range filter; the members are checked, and that a range is reported.
    DateRangeMerge,
    /// The console keeps `CURRENT_DATE - INTERVAL ...` symbolic, while the
    /// engine evaluates it when it plans, so the reported value is a concrete
    /// date that moves with today; the members are checked.
    RelativeDate,
    /// A query carrying computed columns is not one the engine plans as a
    /// single Cube query, so it reports no filters.
    NotACubeQuery,
    /// The predicate has no Cube filter to be turned into, so the engine
    /// leaves it out of the Cube query and this API reports no filter for it.
    UnsupportedFilter,
    /// A raw SQL expression the console keeps as text; the engine drops it,
    /// rejects the query, or reads a filter of its own shape from it, in which
    /// case it stands on the same members.
    CustomExpression,
    /// The console's parser reads the query with a regular expression and
    /// accepts SQL the engine rejects, so this API answers with an error.
    NotPlanned,
}

/// A filter as the console's suite states it.
enum F {
    /// Member, operator and values.
    Leaf(&'static str, &'static str, &'static [&'static str]),
    /// An `or` group; a top-level `and` group is reported as sibling filters,
    /// so the suite never states one.
    Or(&'static [F]),
    /// A member alone, for an expression the console keeps as text.
    Member(&'static str),
}

impl F {
    fn to_filter(&self) -> V1LoadRequestQueryFilterItem {
        let group = |items: &[F]| {
            Some(
                items
                    .iter()
                    .map(|item| serde_json::to_value(item.to_filter()).unwrap())
                    .collect(),
            )
        };
        match self {
            F::Leaf(member, operator, values) => V1LoadRequestQueryFilterItem {
                member: Some(member.to_string()),
                operator: Some(operator.to_string()),
                values: (!values.is_empty())
                    .then(|| values.iter().map(|value| value.to_string()).collect()),
                ..Default::default()
            },
            F::Member(member) => V1LoadRequestQueryFilterItem {
                member: Some(member.to_string()),
                ..Default::default()
            },
            F::Or(items) => V1LoadRequestQueryFilterItem {
                or: group(items),
                ..Default::default()
            },
        }
    }
}

struct Case {
    sql: &'static str,
    expect: Expect,
    expected: &'static [F],
}

/// A generator case: the filters the console was given and the SQL it wrote.
/// Adding the filters to that SQL with its filters taken out is expected to
/// plan to the same filters as the SQL itself.
struct WriteCase {
    sql: &'static str,
    filters: &'static [F],
}

impl Case {
    fn expected(&self) -> Vec<V1LoadRequestQueryFilterItem> {
        self.expected.iter().map(F::to_filter).collect()
    }
}

impl WriteCase {
    fn filters(&self) -> Vec<V1LoadRequestQueryFilterItem> {
        self.filters.iter().map(F::to_filter).collect()
    }

    /// The SQL with the outermost WHERE and HAVING taken out: what the
    /// console started from before it wrote the filters.
    fn bare_sql(&self) -> String {
        let mut query = parse_single_query(self.sql).expect("the suite's SQL parses");
        let ast::SetExpr::Select(select) = query.body.as_mut() else {
            panic!("the suite's SQL is a plain SELECT");
        };
        select.selection = None;
        select.having = None;
        query.to_string()
    }
}

/// The views the console's suite is written against, so that its queries plan
/// here unchanged. Types are not part of the suite, and are taken from how
/// each member is compared.
fn suite_meta() -> Arc<MetaContext> {
    let dimension = |name: &str, kind: &str| V1CubeMetaDimension {
        name: name.to_string(),
        r#type: kind.to_string(),
        ..Default::default()
    };
    let measure = |name: &str, agg: &str| V1CubeMetaMeasure {
        name: name.to_string(),
        r#type: "number".to_string(),
        agg_type: Some(agg.to_string()),
        ..Default::default()
    };
    let view = |name: &str,
                dimensions: Vec<V1CubeMetaDimension>,
                measures: Vec<V1CubeMetaMeasure>| V1CubeMeta {
        name: name.to_string(),
        description: None,
        title: None,
        r#type: V1CubeMetaType::View,
        dimensions,
        measures,
        segments: vec![],
        joins: None,
        folders: None,
        nested_folders: None,
        hierarchies: None,
        meta: None,
    };

    get_test_tenant_ctx_with_meta(vec![
        view(
            "orders_view",
            vec![
                dimension("orders_view.id", "number"),
                dimension("orders_view.status", "string"),
                dimension("orders_view.date", "time"),
                dimension("orders_view.created_at", "time"),
                dimension("orders_view.customer_id", "number"),
                dimension("orders_view.users_state", "string"),
                dimension("orders_view.product_category", "string"),
                dimension("orders_view.customers_city", "string"),
                dimension("orders_view.products_brand", "string"),
                dimension("orders_view.description", "string"),
                dimension("orders_view.notes", "string"),
                dimension("orders_view.updated_at", "time"),
                dimension("orders_view.amount", "number"),
                dimension("orders_view.quantity", "number"),
                dimension("orders_view.discount", "number"),
                dimension("orders_view.balance", "number"),
                dimension("orders_view.adjustment", "number"),
            ],
            vec![
                measure("orders_view.count", "count"),
                measure("orders_view.total_amount", "sum"),
                measure("orders_view.revenue", "sum"),
                measure("orders_view.special_count", "count"),
                measure("orders_view.category_count", "count"),
                measure("orders_view.avg_amount", "avg"),
                measure("orders_view.discount_amount", "sum"),
                measure("orders_view.bonus_points", "sum"),
            ],
        ),
        view(
            "test_view",
            vec![
                dimension("test_view.id", "number"),
                dimension("test_view.status", "string"),
                dimension("test_view.users_city", "string"),
                dimension("test_view.users_age", "number"),
            ],
            vec![measure("test_view.count", "count")],
        ),
    ])
}

const CASES: &[Case] = &[
    Case {
        sql: "SELECT orders_view.id, MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status = 'completed' AND orders_view.customer_id > 100",
        expect: Expect::NotPlanned,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["completed"]),
            F::Leaf("orders_view.customer_id", "gt", &["100"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IN ('pending', 'completed') AND orders_view.users_state LIKE '%California%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["pending", "completed"]),
            F::Leaf("orders_view.users_state", "contains", &["California"]),
        ],
    },
    Case {
        sql: "SELECT DISTINCT orders_view.status, orders_view.product_category FROM orders_view WHERE orders_view.customer_id > 100 ORDER BY orders_view.status LIMIT 25",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "gt", &["100"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, orders_view.product_category FROM orders_view WHERE orders_view.customer_id > 100",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "gt", &["100"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.id, MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status = 'active';",
        expect: Expect::NotPlanned,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status = 'pending;review'",
        expect: Expect::NotPlanned,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["pending;review"]),
        ],
    },
    Case {
        sql: "SELECT DATE_TRUNC('month', orders_view.date) AS month, orders_view.product_category, MEASURE(orders_view.revenue) AS revenue FROM orders_view WHERE orders_view.customers_city = 'New York' GROUP BY 1, 2 ORDER BY 1, 3 DESC LIMIT 100",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customers_city", "equals", &["New York"]),
        ],
    },
    Case {
        sql: "SELECT test_view.users_city, test_view.users_age, test_view.status, CASE WHEN test_view.users_age < 25 THEN 'Young' WHEN test_view.users_age BETWEEN 25 AND 44 THEN 'Adult' WHEN test_view.users_age BETWEEN 45 AND 64 THEN 'MiddleAge' WHEN test_view.users_age >= 65 THEN 'Senior' ELSE 'Unknown' END AS age_category, CASE test_view.status WHEN 'completed' THEN 100 WHEN 'processing' THEN 75 WHEN 'pending' THEN 50 WHEN 'cancelled' THEN 0 ELSE 25 END AS status_score, test_view.users_age * 2 AS double_age, test_view.id + test_view.users_age AS id_plus_age, MEASURE(test_view.count) AS order_count FROM test_view WHERE test_view.users_age IS NOT NULL GROUP BY 1, 2, 3, 4, 5, 6, 7 LIMIT 20",
        expect: Expect::NotACubeQuery,
        expected: &[
            F::Leaf("test_view.users_age", "set", &[]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.revenue) AS revenue, MEASURE(orders_view.revenue) / NULLIF(MEASURE(orders_view.count), 0) AS avg_order_value FROM orders_view WHERE orders_view.customers_city = 'Austin'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customers_city", "equals", &["Austin"]),
        ],
    },
    Case {
        sql: "SELECT DATE_TRUNC('month', orders_view.date) AS date_month, CASE WHEN LAG(MEASURE(orders_view.revenue)) OVER (ORDER BY DATE_TRUNC('month', orders_view.date)) > 0 THEN ((MEASURE(orders_view.revenue) - LAG(MEASURE(orders_view.revenue)) OVER (ORDER BY DATE_TRUNC('month', orders_view.date))) / LAG(MEASURE(orders_view.revenue)) OVER (ORDER BY DATE_TRUNC('month', orders_view.date))) * 100 ELSE NULL END AS mom_growth_rate, MEASURE(orders_view.revenue) AS revenue, LAG(MEASURE(orders_view.revenue)) OVER (ORDER BY DATE_TRUNC('month', orders_view.date)) AS prior_month_revenue, CASE WHEN COUNT(orders_view.date) > 0 THEN ( COUNT( CASE WHEN orders_view.status = 'completed' THEN 1 END ) :: FLOAT / COUNT(orders_view.date) * 100 ) ELSE 0 END AS completed_orders_rate FROM orders_view WHERE orders_view.customers_city = 'Mountain View' GROUP BY 1",
        expect: Expect::NotACubeQuery,
        expected: &[
            F::Leaf("orders_view.customers_city", "equals", &["Mountain View"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status = 'completed'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["completed"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IS NOT DISTINCT FROM orders_view.status",
        expect: Expect::UnsupportedFilter,
        expected: &[
            F::Leaf("orders_view.status", "equals", &[]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view GROUP BY 1 HAVING MEASURE(orders_view.count) IS NOT DISTINCT FROM MEASURE(orders_view.count)",
        expect: Expect::UnsupportedFilter,
        expected: &[
            F::Leaf("orders_view.count", "equals", &[]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IS NOT DISTINCT FROM orders_view.customer_id",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.status"),
            F::Member("orders_view.customer_id"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IS NULL",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "notSet", &[]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IS NOT NULL",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "set", &[]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status = ''",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &[""]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status != ''",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "notEquals", &[""]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IS NOT NULL AND orders_view.status != ''",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "set", &[]),
            F::Leaf("orders_view.status", "notEquals", &[""]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id > 100",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "gt", &["100"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id < 500",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "lt", &["500"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id >= 250",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "gte", &["250"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id <= 750",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "lte", &["750"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status != 'cancelled'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "notEquals", &["cancelled"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status <> 'cancelled'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "notEquals", &["cancelled"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count), MEASURE(orders_view.total_amount) FROM orders_view WHERE orders_view.customer_id >= 100 AND orders_view.customer_id <= 1000 AND orders_view.id > 50 AND orders_view.status != 'cancelled'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "gte", &["100"]),
            F::Leaf("orders_view.customer_id", "lte", &["1000"]),
            F::Leaf("orders_view.id", "gt", &["50"]),
            F::Leaf("orders_view.status", "notEquals", &["cancelled"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id > 100.5 AND orders_view.customer_id <= 999.99",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "gt", &["100.5"]),
            F::Leaf("orders_view.customer_id", "lte", &["999.99"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id > -10 AND orders_view.id >= -5",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "gt", &["-10"]),
            F::Leaf("orders_view.id", "gte", &["-5"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IN ('pending', 'completed', 'shipped')",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["pending", "completed", "shipped"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IN ('pending')",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["pending"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IN ()",
        expect: Expect::NotPlanned,
        expected: &[
            F::Leaf("orders_view.status", "equals", &[]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status NOT IN ('cancelled', 'refunded', 'returned')",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "notEquals", &["cancelled", "refunded", "returned"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status NOT IN ('cancelled')",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "notEquals", &["cancelled"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status NOT IN ()",
        expect: Expect::NotPlanned,
        expected: &[
            F::Leaf("orders_view.status", "notEquals", &[]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id IN (100, 200, 300, 400)",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "equals", &["100", "200", "300", "400"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id NOT IN (0, -1, 999999)",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "notEquals", &["0", "-1", "999999"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category IN ('Electronics', 'Books', 123, 'Clothing')",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "equals", &["Electronics", "Books", "123", "Clothing"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IN ('pending', 'completed') AND orders_view.product_category NOT IN ('Discontinued', 'Backordered') AND orders_view.customer_id IN (100, 200, 300)",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["pending", "completed"]),
            F::Leaf("orders_view.product_category", "notEquals", &["Discontinued", "Backordered"]),
            F::Leaf("orders_view.customer_id", "equals", &["100", "200", "300"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view GROUP BY 1 HAVING MEASURE(orders_view.count) IN (5, 10, 15, 20)",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.count", "equals", &["5", "10", "15", "20"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view GROUP BY 1 HAVING MEASURE(orders_view.count) NOT IN (0, 1, 2)",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.count", "notEquals", &["0", "1", "2"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IN ( 'pending', 'completed', 'shipped' )",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["pending", "completed", "shipped"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category IN ('Men''s Clothing', 'Women''s Accessories', 'Kids & Baby')",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "equals", &["Men's Clothing", "Women's Accessories", "Kids & Baby"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status IN ('pending,review', 'completed', 'shipped,tracking', 'cancelled,refunded')",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["pending,review", "completed", "shipped,tracking", "cancelled,refunded"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category NOT IN ('electronics,gadgets', 'books,ebooks', 'home,garden')",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "notEquals", &["electronics,gadgets", "books,ebooks", "home,garden"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category LIKE '%Electronics%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category NOT LIKE '%Electronics%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "notContains", &["Electronics"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status LIKE 'pending%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "startsWith", &["pending"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status NOT LIKE 'pending%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "notStartsWith", &["pending"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customers_city LIKE '%York'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customers_city", "endsWith", &["York"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customers_city NOT LIKE '%York'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customers_city", "notEndsWith", &["York"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category LIKE '%elect%onic%'",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.product_category"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category NOT LIKE '%elect%onic%'",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.product_category"),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view GROUP BY 1 HAVING MEASURE(orders_view.status) LIKE 'completed%'",
        expect: Expect::NotPlanned,
        expected: &[
            F::Leaf("orders_view.status", "startsWith", &["completed"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category LIKE '%Electronics%' AND orders_view.status LIKE 'pending%' AND orders_view.customers_city NOT LIKE '%York'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
            F::Leaf("orders_view.status", "startsWith", &["pending"]),
            F::Leaf("orders_view.customers_city", "notEndsWith", &["York"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category LIKE '%%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &[""]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status LIKE ''",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.status"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status NOT LIKE ''",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.status"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category ILIKE '%Electronics%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category NOT ILIKE '%Electronics%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "notContains", &["Electronics"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status ILIKE 'pending%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "startsWith", &["pending"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status NOT ILIKE 'pending%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "notStartsWith", &["pending"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customers_city ILIKE '%York'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customers_city", "endsWith", &["York"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customers_city NOT ILIKE '%York'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customers_city", "notEndsWith", &["York"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category ILIKE '%elect%onic%'",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.product_category"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category NOT ILIKE '%elect%onic%'",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.product_category"),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view GROUP BY 1 HAVING MEASURE(orders_view.status) ILIKE 'completed%'",
        expect: Expect::NotPlanned,
        expected: &[
            F::Leaf("orders_view.status", "startsWith", &["completed"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category LIKE '%Electronics%' AND orders_view.status ILIKE 'pending%' AND orders_view.customers_city NOT ILIKE '%york'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
            F::Leaf("orders_view.status", "startsWith", &["pending"]),
            F::Leaf("orders_view.customers_city", "notEndsWith", &["york"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category ILIKE '%%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &[""]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category NOT LIKE '%%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "notContains", &[""]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category NOT ILIKE '%%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "notContains", &[""]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status ILIKE ''",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.status"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status NOT ILIKE ''",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.status"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category LIKE '%electronics%' AND orders_view.status ILIKE '%PENDING%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["electronics"]),
            F::Leaf("orders_view.status", "contains", &["PENDING"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE EXTRACT(YEAR FROM orders_view.date) = 2023",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.date", "inDateRange", &["2023-01-01", "2023-12-31"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id BETWEEN 100 AND 1000",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "gte", &["100"]),
            F::Leaf("orders_view.customer_id", "lte", &["1000"]),
        ],
    },
    Case {
        sql: "SELECT DATE_TRUNC('week', orders_view.date) AS activation_week, MEASURE(orders_view.count) AS count_alias FROM orders_view WHERE orders_view.date BETWEEN '2025-03-01' AND current_timestamp() AND orders_view.status = 'active' GROUP BY 1 LIMIT 5000",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
            F::Member("orders_view.status"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status NOT IN ('cancelled', 'refunded')",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "notEquals", &["cancelled", "refunded"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category ~ '^[A-Z].*'",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.product_category"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id = ANY(ARRAY[100, 200, 300])",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.customer_id"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE COALESCE(orders_view.status, 'unknown') ILIKE '%complete%'",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.status"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status = 'active' AND orders_view.customer_id BETWEEN 100 AND 1000",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.customer_id", "gte", &["100"]),
            F::Leaf("orders_view.customer_id", "lte", &["1000"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE GREATEST(COALESCE(orders_view.customer_id, 0), LEAST(orders_view.id, 1000)) > 50",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.customer_id"),
            F::Member("orders_view.id"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE (orders_view.customer_id * 1.5 + orders_view.id / 10) >= POW(orders_view.status::INTEGER, 2)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.customer_id"),
            F::Member("orders_view.id"),
            F::Member("orders_view.status"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL '30 days' AND orders_view.date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE CASE WHEN orders_view.customer_id > 1000 AND orders_view.status = 'premium' THEN orders_view.id % 10 = 0 WHEN orders_view.customer_id BETWEEN 500 AND 1000 THEN LENGTH(orders_view.product_category) > 5 ELSE orders_view.date > CURRENT_DATE - INTERVAL '7 days' END",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.customer_id"),
            F::Member("orders_view.status"),
            F::Member("orders_view.id"),
            F::Member("orders_view.product_category"),
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category->'metadata'->>'type' = 'electronics' AND (orders_view.customers_city::jsonb ? 'coordinates')",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.product_category"),
            F::Member("orders_view.customers_city"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id BETWEEN EXTRACT(YEAR FROM orders_view.date) * 10 AND GREATEST(1000, orders_view.id)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.customer_id"),
            F::Member("orders_view.date"),
            F::Member("orders_view.id"),
        ],
    },
    Case {
        sql: r#"SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category SIMILAR TO '%[0-9]{2,4}%' AND orders_view.customers_city ~ E'.*\\\\d+.*'"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.product_category"),
            F::Member("orders_view.customers_city"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE ARRAY_LENGTH(STRING_TO_ARRAY(orders_view.product_category, ','), 1) > 2 AND orders_view.customer_id = ANY(ARRAY[100, 200, 300])",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.product_category"),
            F::Member("orders_view.customer_id"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE MOD(orders_view.customer_id, 10) = 0 AND GREATEST(orders_view.id, 100) < 1000",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.customer_id"),
            F::Member("orders_view.id"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_ADD(CURRENT_DATE(), INTERVAL '-30 DAYS') AND orders_view.date <= NOW() AND orders_view.customer_id > EXTRACT(YEAR FROM orders_view.date)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
            F::Member("orders_view.customer_id"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.customer_id > (100 + 50) AND orders_view.total_amount >= (orders_view.count * 10)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.customer_id"),
            F::Member("orders_view.total_amount"),
            F::Member("orders_view.count"),
        ],
    },
    Case {
        sql: "SELECT DATE_TRUNC('month', orders_view.date) AS month, MEASURE(orders_view.revenue) AS revenue FROM orders_view WHERE orders_view.status = 'returned' GROUP BY 1 ORDER BY 1 LIMIT 100",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["returned"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count), MEASURE(orders_view.total_amount) FROM orders_view GROUP BY 1 HAVING MEASURE(orders_view.count) > 10 AND MEASURE(orders_view.total_amount) <= 1000",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.count", "gt", &["10"]),
            F::Leaf("orders_view.total_amount", "lte", &["1000"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.product_category, MEASURE(orders_view.revenue) FROM orders_view WHERE orders_view.status = 'active' AND orders_view.product_category IN ('Electronics', 'Books') GROUP BY 1 HAVING MEASURE(orders_view.revenue) > 100",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.product_category", "equals", &["Electronics", "Books"]),
            F::Leaf("orders_view.revenue", "gt", &["100"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status = 'pending' GROUP BY 1 HAVING MEASURE(orders_view.count) > 5",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["pending"]),
            F::Leaf("orders_view.count", "gt", &["5"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.customer_id, MEASURE(orders_view.count), MEASURE(orders_view.revenue), MEASURE(orders_view.total_amount) FROM orders_view GROUP BY 1 HAVING MEASURE(orders_view.count) >= 5 AND MEASURE(orders_view.revenue) < 1000 AND MEASURE(orders_view.total_amount) != 0",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.count", "gte", &["5"]),
            F::Leaf("orders_view.revenue", "lt", &["1000"]),
            F::Leaf("orders_view.total_amount", "notEquals", &["0"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view GROUP BY 1 HAVING MEASURE(orders_view.count) IN (1, 5, 10)",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.count", "equals", &["1", "5", "10"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.customer_id, MEASURE(orders_view.total_amount) FROM orders_view GROUP BY 1 HAVING MEASURE(orders_view.total_amount) IS NOT NULL",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.total_amount", "set", &[]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.revenue) FROM orders_view GROUP BY 1 HAVING AVG(orders_view.revenue) > 100",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.revenue"),
        ],
    },
    Case {
        sql: "SELECT orders_view.customer_id, MEASURE(orders_view.total_amount) FROM orders_view GROUP BY 1 HAVING MEASURE(orders_view.total_amount) > 50",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.total_amount", "gt", &["50"]),
        ],
    },
    Case {
        sql: "SELECT DATE_TRUNC('month', orders_view.date) AS date_month, orders_view.status, MEASURE(orders_view.count), MEASURE(orders_view.revenue) FROM orders_view WHERE orders_view.status = 'active' AND orders_view.date >= '2023-01-01' GROUP BY 1, 2 HAVING MEASURE(orders_view.count) > 10 AND MEASURE(orders_view.revenue) <= 10000 ORDER BY 1 DESC, 3 LIMIT 25",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
            F::Leaf("orders_view.count", "gt", &["10"]),
            F::Leaf("orders_view.revenue", "lte", &["10000"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE MEASURE(orders_view.count) > 0 GROUP BY 1",
        expect: Expect::UnsupportedFilter,
        expected: &[
            F::Leaf("orders_view.count", "gt", &["0"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE status = 'active' OR users_state LIKE '%California%' GROUP BY 1",
        expect: Expect::Match,
        expected: &[
            F::Or(&[
                F::Leaf("orders_view.status", "equals", &["active"]),
                F::Leaf("orders_view.users_state", "contains", &["California"]),
            ]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE (status = 'active' OR users_state LIKE '%California%') AND customer_city IN ('New York', 'Los Angeles') GROUP BY 1",
        expect: Expect::NotPlanned,
        expected: &[
            F::Or(&[
                F::Leaf("orders_view.status", "equals", &["active"]),
                F::Leaf("orders_view.users_state", "contains", &["California"]),
            ]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE (status = 'active' OR users_state LIKE '%California%') AND orders_view.product_category IN ('Acme Sports - Trial', '(closed) Acme Pharma - Trial') GROUP BY 1",
        expect: Expect::Match,
        expected: &[
            F::Or(&[
                F::Leaf("orders_view.status", "equals", &["active"]),
                F::Leaf("orders_view.users_state", "contains", &["California"]),
            ]),
            F::Leaf("orders_view.product_category", "equals", &["Acme Sports - Trial", "(closed) Acme Pharma - Trial"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE (status = 'active' OR users_state LIKE '%California%') AND orders_view.product_category = '(closed) Acme Pharma - Trial' GROUP BY 1",
        expect: Expect::Match,
        expected: &[
            F::Or(&[
                F::Leaf("orders_view.status", "equals", &["active"]),
                F::Leaf("orders_view.users_state", "contains", &["California"]),
            ]),
            F::Leaf("orders_view.product_category", "equals", &["(closed) Acme Pharma - Trial"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.total_amount) FROM orders_view WHERE (orders_view.status = 'active' OR orders_view.product_category = 'Acme Sports - Trial' OR orders_view.product_category = '(closed) Acme Pharma - Trial' OR orders_view.product_category = 'Best Case') AND orders_view.users_state IN ('CA', 'NY') LIMIT 5000",
        expect: Expect::Match,
        expected: &[
            F::Or(&[
                F::Leaf("orders_view.status", "equals", &["active"]),
                F::Leaf("orders_view.product_category", "equals", &["Acme Sports - Trial"]),
                F::Leaf("orders_view.product_category", "equals", &["(closed) Acme Pharma - Trial"]),
                F::Leaf("orders_view.product_category", "equals", &["Best Case"]),
            ]),
            F::Leaf("orders_view.users_state", "equals", &["CA", "NY"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.total_amount) FROM orders_view WHERE (orders_view.status = 'active' OR orders_view.product_category IN ('Acme Sports - Trial', '(closed) Acme Pharma - Trial', 'Best Case') OR orders_view.product_category = 'Best Case') AND orders_view.users_state IN ('CA', 'NY') LIMIT 5000",
        expect: Expect::Match,
        expected: &[
            F::Or(&[
                F::Leaf("orders_view.status", "equals", &["active"]),
                F::Leaf("orders_view.product_category", "equals", &["Acme Sports - Trial", "(closed) Acme Pharma - Trial", "Best Case"]),
                F::Leaf("orders_view.product_category", "equals", &["Best Case"]),
            ]),
            F::Leaf("orders_view.users_state", "equals", &["CA", "NY"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.total_amount) FROM orders_view WHERE (orders_view.status = 'active') AND orders_view.product_category IN ('Expansion', 'Net New') LIMIT 5000",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.product_category", "equals", &["Expansion", "Net New"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date BETWEEN '2023-01-01' AND '2023-12-31'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.date", "inDateRange", &["2023-01-01", "2023-12-31"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= '2023-01-01' AND orders_view.date < '2024-01-01' GROUP BY 1",
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeDate", &["2024-01-01"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= '2023-07-15T00:00:00Z' AND orders_view.date <= '2023-07-15T23:59:59Z'",
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-07-15T00:00:00Z"]),
            F::Leaf("orders_view.date", "beforeOrOnDate", &["2023-07-15T23:59:59Z"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date > '2023-01-01' AND orders_view.date <= '2023-12-31' AND orders_view.status = 'active'",
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeOrOnDate", &["2023-12-31"]),
            F::Leaf("orders_view.status", "equals", &["active"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= '2023-06-01' AND orders_view.date < '2023-07-01'",
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-06-01"]),
            F::Leaf("orders_view.date", "beforeDate", &["2023-07-01"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date > '2023-06-01'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.date", "afterDate", &["2023-06-01"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= '2023-01-01' AND orders_view.date <= '2023-12-31' AND orders_view.date != '2023-07-04'",
        expect: Expect::UnsupportedFilter,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeOrOnDate", &["2023-12-31"]),
            F::Leaf("orders_view.date", "notEquals", &["2023-07-04"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status = 'active' AND orders_view.date >= '2023-01-01' AND orders_view.customer_id > 100 AND orders_view.date <= '2023-12-31'",
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
            F::Leaf("orders_view.customer_id", "gt", &["100"]),
            F::Leaf("orders_view.date", "beforeOrOnDate", &["2023-12-31"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date > '2023-01-01' AND orders_view.date <= '2023-12-31'",
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeOrOnDate", &["2023-12-31"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= '2023-01-01' AND orders_view.date < '2023-12-31'",
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeDate", &["2023-12-31"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date > '2023-01-01' AND orders_view.date < '2023-12-31'",
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeDate", &["2023-12-31"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL 7 DAY",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["7 days ago"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL 2 WEEK",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2 weeks ago"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_SUB(CURRENT_DATE(), INTERVAL '3 MONTHS')",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL '1 YEAR'",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["1 year ago"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('month', CURRENT_DATE)",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["this month"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('year', CURRENT_DATE)",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["this year"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('week', CURRENT_DATE)",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["this week"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 MONTH') AND orders_view.date < DATE_TRUNC('month', CURRENT_DATE)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1 YEAR') AND orders_view.date < DATE_TRUNC('year', CURRENT_DATE)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT DATE_TRUNC('week', orders_view.date) AS date_week, MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date > '2025-01-01' AND orders_view.date < DATE_TRUNC('week', CURRENT_DATE) GROUP BY 1 ORDER BY 1 LIMIT 5000",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterDate", &["2025-01-01"]),
            F::Leaf("orders_view.date", "beforeDate", &["this week"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK') AND orders_view.date < DATE_TRUNC('week', CURRENT_DATE)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE AND orders_view.date < CURRENT_DATE + INTERVAL 1 DAY",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["today"]),
            F::Leaf("orders_view.date", "beforeDate", &["1 day from now"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL '1 DAY' AND orders_view.date < CURRENT_DATE",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["1 day ago"]),
            F::Leaf("orders_view.date", "beforeDate", &["today"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('quarter', CURRENT_DATE)",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["this quarter"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1 QUARTER') AND orders_view.date < DATE_TRUNC('quarter', CURRENT_DATE)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND orders_view.status = 'completed' GROUP BY 1",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["7 days ago"]),
            F::Leaf("orders_view.status", "equals", &["completed"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_ADD(CURRENT_DATE(), INTERVAL '-14 DAYS')",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL '2.5 DAYS'",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count), MEASURE(orders_view.revenue) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL 30 DAY AND orders_view.created_at >= CURRENT_DATE - INTERVAL 7 WEEK",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["30 days ago"]),
            F::Leaf("orders_view.created_at", "afterOrOnDate", &["7 weeks ago"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE DATE(orders_view.date) = CURRENT_DATE",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE DATE(orders_view.date) = CURRENT_DATE - INTERVAL '1 day'",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE DATE_TRUNC('week', orders_view.date) = DATE_TRUNC('week', CURRENT_DATE)",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "equals", &["this week"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE DATE_TRUNC('month', orders_view.date) = DATE_TRUNC('month', CURRENT_DATE)",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "equals", &["this month"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE DATE_TRUNC('quarter', orders_view.date) = DATE_TRUNC('quarter', CURRENT_DATE)",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "equals", &["this quarter"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE DATE_TRUNC('year', orders_view.date) = DATE_TRUNC('year', CURRENT_DATE)",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "equals", &["this year"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 week') AND orders_view.date < DATE_TRUNC('week', CURRENT_DATE)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date < DATE_TRUNC('month', CURRENT_DATE)",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "beforeDate", &["this month"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AND orders_view.date < DATE_TRUNC('month', CURRENT_DATE)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1 quarter') AND orders_view.date < DATE_TRUNC('quarter', CURRENT_DATE)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1 year') AND orders_view.date < DATE_TRUNC('year', CURRENT_DATE)",
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL '7 days'",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["7 days ago"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL '4 weeks'",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["4 weeks ago"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL '3 months'",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["3 months ago"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL '2 quarter'",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2 quarters ago"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.date >= CURRENT_DATE - INTERVAL '5 years'",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["5 years ago"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE DATE_TRUNC('month', orders_view.date) = DATE_TRUNC('month', CURRENT_DATE) AND orders_view.status IN ('active', 'pending') GROUP BY 1",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "equals", &["this month"]),
            F::Leaf("orders_view.status", "equals", &["active", "pending"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count), MEASURE(orders_view.revenue) FROM orders_view WHERE orders_view.status = 'active' GROUP BY 1 HAVING DATE_TRUNC('week', MEASURE(orders_view.revenue)) = DATE_TRUNC('week', CURRENT_DATE)",
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.revenue", "equals", &["this week"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, orders_view.missing_dimension, MEASURE(orders_view.count), MEASURE(orders_view.revenue), MEASURE(orders_view.missing_measure) FROM orders_view WHERE orders_view.status = 'active' GROUP BY 1 HAVING DATE_TRUNC('week', MEASURE(orders_view.revenue)) = DATE_TRUNC('week', CURRENT_DATE)",
        expect: Expect::NotPlanned,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.revenue", "equals", &["this week"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE orders_view.status = 'active' GROUP BY 1",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
        ],
    },
    Case {
        sql: r#"-- This is a single-line comment at the start
        /* Multi-line comment
           describing the query */
        SELECT
          orders_view.status, -- inline single-line comment
          MEASURE(orders_view.count), /* inline multi-line comment */
          -- another single-line comment
          MEASURE(orders_view.revenue)
        FROM orders_view
        -- Comment before WHERE
        WHERE orders_view.status = 'active' /* another inline comment */
        GROUP BY 1"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE (orders_view.status = 'active' AND orders_view.users_state = 'CA') AND orders_view.customers_city IN ('New York', 'Los Angeles') GROUP BY 1",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.users_state", "equals", &["CA"]),
            F::Leaf("orders_view.customers_city", "equals", &["New York", "Los Angeles"]),
        ],
    },
    Case {
        sql: "SELECT orders_view.status, MEASURE(orders_view.count) FROM orders_view WHERE (orders_view.status = 'active' AND orders_view.users_state = 'CA') GROUP BY 1",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.users_state", "equals", &["CA"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category LIKE '%Levi''s%' AND orders_view.status ILIKE 'Macy''s%'",
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["Levi's"]),
            F::Leaf("orders_view.status", "startsWith", &["Macy's"]),
        ],
    },
    Case {
        sql: "SELECT MEASURE(orders_view.count) FROM orders_view WHERE orders_view.product_category BETWEEN 'A''s' AND 'Z''s'",
        expect: Expect::UnsupportedFilter,
        expected: &[
            F::Leaf("orders_view.product_category", "gte", &["A's"]),
            F::Leaf("orders_view.product_category", "lte", &["Z's"]),
        ],
    },
    Case {
        sql: r#"SELECT
  DATE_TRUNC('month', orders_view.date) AS date_month,
  orders_view.product_category,
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  orders_view.customers_city = 'New York'
GROUP BY
  1,
  2
ORDER BY
  1,
  3 DESC
LIMIT
  100"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customers_city", "equals", &["New York"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.customer_id
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.count) != 0"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.count", "notEquals", &["0"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status IN ('pending', 'completed')
  AND orders_view.amount > 100
  AND orders_view.description ILIKE '%special%'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["pending", "completed"]),
            F::Leaf("orders_view.amount", "gt", &["100"]),
            F::Leaf("orders_view.description", "contains", &["special"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.customer_id > 100
  AND orders_view.amount < 1000
  AND orders_view.quantity >= 5
  AND orders_view.discount <= 0.2
  AND orders_view.status != 'cancelled'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "gt", &["100"]),
            F::Leaf("orders_view.amount", "lt", &["1000"]),
            F::Leaf("orders_view.quantity", "gte", &["5"]),
            F::Leaf("orders_view.discount", "lte", &["0.2"]),
            F::Leaf("orders_view.status", "notEquals", &["cancelled"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  orders_view.customer_id >= 100
  AND orders_view.customer_id <= 1000
  AND orders_view.status != 'cancelled'
  AND orders_view.product_category IN ('Electronics', 'Books')
  AND orders_view.amount > 50
GROUP BY
  1"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "gte", &["100"]),
            F::Leaf("orders_view.customer_id", "lte", &["1000"]),
            F::Leaf("orders_view.status", "notEquals", &["cancelled"]),
            F::Leaf("orders_view.product_category", "equals", &["Electronics", "Books"]),
            F::Leaf("orders_view.amount", "gt", &["50"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.balance > -100
  AND orders_view.adjustment <= -5.5"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.balance", "gt", &["-100"]),
            F::Leaf("orders_view.adjustment", "lte", &["-5.5"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status IS NULL
  AND orders_view.customer_id IS NOT NULL"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "notSet", &[]),
            F::Leaf("orders_view.customer_id", "set", &[]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status = ''
  AND orders_view.product_category != ''"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &[""]),
            F::Leaf("orders_view.product_category", "notEquals", &[""]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status IS NOT NULL
  AND orders_view.status != ''"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "set", &[]),
            F::Leaf("orders_view.status", "notEquals", &[""]),
        ],
    },
    Case {
        sql: r#"SELECT DISTINCT
  orders_view.status,
  orders_view.product_category
FROM
  orders_view
WHERE
  orders_view.customer_id > 100
  AND orders_view.date >= '2023-01-01'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customer_id", "gt", &["100"]),
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
        ],
    },
    Case {
        sql: r#"SELECT DISTINCT
  DATE_TRUNC('day', orders_view.date) AS date_day,
  orders_view.status
FROM
  orders_view
WHERE
  orders_view.status IN ('active', 'pending')
ORDER BY
  1 DESC,
  2
LIMIT
  50"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active", "pending"]),
        ],
    },
    Case {
        sql: r#"SELECT DISTINCT
  orders_view.status,
  EXTRACT(
    YEAR
    FROM
      orders_view.date
  ) AS year
FROM
  orders_view
WHERE
  orders_view.date >= '2023-01-01'
ORDER BY
  2 DESC"#,
        expect: Expect::NotACubeQuery,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
        ],
    },
    Case {
        sql: r#"SELECT
  DATE_TRUNC('month', orders_view.date) AS date_month,
  MEASURE(orders_view.count),
  LAG(MEASURE(orders_view.count), 1) OVER (
    ORDER BY
      DATE_TRUNC('month', orders_view.date)
  ) AS prior_month_orders,
  (
    (
      MEASURE(orders_view.count) - LAG(MEASURE(orders_view.count), 1) OVER (
        ORDER BY
          DATE_TRUNC('month', orders_view.date)
      )
    ) / LAG(MEASURE(orders_view.count), 1) OVER (
      ORDER BY
        DATE_TRUNC('month', orders_view.date)
    )
  ) * 100 AS growth_rate_pct
FROM
  orders_view
WHERE
  orders_view.product_category = 'Laptops'
GROUP BY
  1
ORDER BY
  1"#,
        expect: Expect::NotACubeQuery,
        expected: &[
            F::Leaf("orders_view.product_category", "equals", &["Laptops"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  EXTRACT(
    YEAR
    FROM
      orders_view.date
  ) = 2023"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.date", "inDateRange", &["2023-01-01", "2023-12-31"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status = 'active'
  AND orders_view.customer_id BETWEEN 100 AND 1000
  AND EXTRACT(
    MONTH
    FROM
      orders_view.date
  ) IN (1, 2, 3)
GROUP BY
  1"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.date"),
            F::Member("orders_view.status"),
            F::Member("orders_view.customer_id"),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  COALESCE(orders_view.status, 'unknown') ILIKE '%complete%'
  AND orders_view.customer_id = ANY(ARRAY[100, 200, 300])"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.status"),
            F::Member("orders_view.customer_id"),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category ~ '^[A-Z].*'
  AND orders_view.status !~ '.*cancelled.*'
GROUP BY
  1"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.product_category"),
            F::Member("orders_view.status"),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.customer_id,
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  (
    orders_view.customer_id * 1.5 + orders_view.id / 10
  ) >= POW (orders_view.status::INTEGER, 2)
  AND orders_view.date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY
  1"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.customer_id"),
            F::Member("orders_view.id"),
            F::Member("orders_view.status"),
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  CASE
    WHEN orders_view.customer_id > 1000
    AND orders_view.status = 'premium' THEN orders_view.id % 10 = 0
    WHEN orders_view.customer_id BETWEEN 500 AND 1000  THEN LENGTH(orders_view.product_category) > 5
    ELSE orders_view.date > CURRENT_DATE - INTERVAL '7 days'
  END"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.customer_id"),
            F::Member("orders_view.status"),
            F::Member("orders_view.id"),
            F::Member("orders_view.product_category"),
            F::Member("orders_view.date"),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  orders_view.product_category -> 'metadata' ->> 'type' = 'electronics'
  AND (orders_view.customers_city::jsonb ? 'coordinates')
GROUP BY
  1"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.product_category"),
            F::Member("orders_view.customers_city"),
        ],
    },
    Case {
        sql: r#"SELECT
  DATE_TRUNC('day', orders_view.date) AS date_day,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category SIMILAR TO '%[0-9]{2,4}%'
  AND orders_view.customers_city ~ E'.*\\\\d+.*'
GROUP BY
  1"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.product_category"),
            F::Member("orders_view.customers_city"),
        ],
    },
    Case {
        sql: r#"SELECT
  DATE_TRUNC('month', orders_view.date) AS date_month,
  orders_view.status,
  MEASURE(orders_view.count),
  MEASURE(orders_view.revenue),
  AVG(
    orders_view.revenue / NULLIF(orders_view.count, 0)
  ) AS avg_order_value
FROM
  orders_view
WHERE
  orders_view.status IN ('active', 'pending')
  AND orders_view.customer_id BETWEEN EXTRACT(
    YEAR
    FROM
      orders_view.date
  ) * 10 AND GREATEST(1000, orders_view.id)
  AND GREATEST(
    COALESCE(orders_view.customer_id, 0),
    LEAST(orders_view.id, 1000)
  ) > 50
  AND orders_view.date >= '2023-01-01'
GROUP BY
  1,
  2
ORDER BY
  1 DESC,
  5
LIMIT
  50"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.customer_id"),
            F::Member("orders_view.date"),
            F::Member("orders_view.id"),
            F::Member("orders_view.status"),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category ILIKE '%Electronics%'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category NOT ILIKE '%Electronics%'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "notContains", &["Electronics"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status ILIKE 'pending%'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "startsWith", &["pending"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status NOT ILIKE 'pending%'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "notStartsWith", &["pending"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.customers_city ILIKE '%York'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customers_city", "endsWith", &["York"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.customers_city NOT ILIKE '%York'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.customers_city", "notEndsWith", &["York"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category ILIKE '%Electronics%'
  AND orders_view.status ILIKE 'pending%'
  AND orders_view.customers_city NOT ILIKE '%York'
GROUP BY
  1"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
            F::Leaf("orders_view.status", "startsWith", &["pending"]),
            F::Leaf("orders_view.customers_city", "notEndsWith", &["York"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.description ILIKE '%elect%onic%'"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.description"),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.description NOT ILIKE '%elect%onic%'"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.description"),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category ILIKE '%Electronics%'
GROUP BY
  1
HAVING
  MEASURE(orders_view.status) ILIKE 'completed%'"#,
        expect: Expect::NotACubeQuery,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
            F::Leaf("orders_view.status", "startsWith", &["completed"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category ILIKE '%Men''s Clothing%'
  AND orders_view.description ILIKE 'Special "Premium"%'
  AND orders_view.notes ILIKE '%Order #123'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["Men's Clothing"]),
            F::Leaf("orders_view.description", "startsWith", &[r#"Special "Premium""#]),
            F::Leaf("orders_view.notes", "endsWith", &["Order #123"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category LIKE '%Electronics%'
  AND orders_view.status ILIKE 'pending%'
  AND orders_view.customers_city NOT ILIKE '%York'
GROUP BY
  1"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
            F::Leaf("orders_view.status", "startsWith", &["pending"]),
            F::Leaf("orders_view.customers_city", "notEndsWith", &["York"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category ILIKE '%Electronics%'
  AND orders_view.status ILIKE 'pending%'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
            F::Leaf("orders_view.status", "startsWith", &["pending"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date BETWEEN '2023-01-01' AND '2023-12-31'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.date", "inDateRange", &["2023-01-01", "2023-12-31"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date > '2023-06-01'
GROUP BY
  1"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.date", "afterDate", &["2023-06-01"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  orders_view.date <= '2023-12-31'"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.date", "beforeOrOnDate", &["2023-12-31"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date >= '2023-01-01'
  AND orders_view.date < '2024-01-01'
  AND orders_view.status = 'active'
GROUP BY
  1"#,
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeDate", &["2024-01-01"]),
            F::Leaf("orders_view.status", "equals", &["active"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date >= '2023-07-15T00:00:00Z'
  AND orders_view.date <= '2023-07-15T23:59:59Z'"#,
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-07-15T00:00:00Z"]),
            F::Leaf("orders_view.date", "beforeOrOnDate", &["2023-07-15T23:59:59Z"]),
        ],
    },
    Case {
        sql: r#"SELECT
  DATE_TRUNC('month', orders_view.date) AS date_month,
  orders_view.status,
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  orders_view.date BETWEEN '2023-01-01' AND '2023-12-31'
  AND orders_view.status IN ('pending', 'completed')
  AND orders_view.customer_id > 100
GROUP BY
  1,
  2
ORDER BY
  1
LIMIT
  50"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.date", "inDateRange", &["2023-01-01", "2023-12-31"]),
            F::Leaf("orders_view.status", "equals", &["pending", "completed"]),
            F::Leaf("orders_view.customer_id", "gt", &["100"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date >= CURRENT_DATE - INTERVAL '30 days'
  AND orders_view.date <= 'CURRENT_DATE'"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["30 days ago"]),
            F::Leaf("orders_view.date", "beforeOrOnDate", &["CURRENT_DATE"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date IS NOT NULL
  AND orders_view.status = 'pending'
GROUP BY
  1"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.date", "set", &[]),
            F::Leaf("orders_view.status", "equals", &["pending"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date > '2023-01-01'
  AND orders_view.date <= '2023-12-31'"#,
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeOrOnDate", &["2023-12-31"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date > '2023-01-01'
  AND orders_view.date < '2023-12-31'"#,
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeDate", &["2023-12-31"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date >= '2023-01-01'
  AND orders_view.date <= '2023-12-31'"#,
        expect: Expect::DateRangeMerge,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeOrOnDate", &["2023-12-31"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count),
  MEASURE(orders_view.total_amount)
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.count) > 10
  AND MEASURE(orders_view.total_amount) <= 1000"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.count", "gt", &["10"]),
            F::Leaf("orders_view.total_amount", "lte", &["1000"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  orders_view.status = 'active'
  AND orders_view.product_category IN ('Electronics', 'Books')
GROUP BY
  1
HAVING
  MEASURE(orders_view.revenue) > 100"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.product_category", "equals", &["Electronics", "Books"]),
            F::Leaf("orders_view.revenue", "gt", &["100"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status = 'pending'
GROUP BY
  1
HAVING
  MEASURE(orders_view.count) > 5"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["pending"]),
            F::Leaf("orders_view.count", "gt", &["5"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.customer_id,
  MEASURE(orders_view.count),
  MEASURE(orders_view.revenue),
  MEASURE(orders_view.avg_amount)
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.count) >= 5
  AND MEASURE(orders_view.revenue) < 1000
  AND MEASURE(orders_view.avg_amount) != 0"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.count", "gte", &["5"]),
            F::Leaf("orders_view.revenue", "lt", &["1000"]),
            F::Leaf("orders_view.avg_amount", "notEquals", &["0"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count),
  MEASURE(orders_view.category_count)
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.count) IN (1, 5, 10)
  AND MEASURE(orders_view.category_count) NOT IN (0, 1)"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.count", "equals", &["1", "5", "10"]),
            F::Leaf("orders_view.category_count", "notEquals", &["0", "1"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.customer_id,
  MEASURE(orders_view.discount_amount),
  MEASURE(orders_view.bonus_points)
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.discount_amount) IS NOT NULL
  AND MEASURE(orders_view.bonus_points) IS NULL"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.discount_amount", "set", &[]),
            F::Leaf("orders_view.bonus_points", "notSet", &[]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.revenue)
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.revenue) > AVG(MEASURE(orders_view.revenue)) * 1.5"#,
        expect: Expect::CustomExpression,
        expected: &[
            F::Member("orders_view.revenue"),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.customer_id,
  MEASURE(orders_view.total_amount)
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.total_amount) > 50"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.total_amount", "gt", &["50"]),
        ],
    },
    Case {
        sql: r#"SELECT
  DATE_TRUNC('month', orders_view.date) AS date_month,
  orders_view.status,
  EXTRACT(
    YEAR
    FROM
      orders_view.date
  ) AS year,
  MEASURE(orders_view.count),
  MEASURE(orders_view.revenue),
  AVG(orders_view.amount) AS avg_order_value
FROM
  orders_view
WHERE
  orders_view.status = 'active'
  AND orders_view.date >= '2023-01-01'
GROUP BY
  1,
  2,
  3
HAVING
  MEASURE(orders_view.count) > 10
  AND MEASURE(orders_view.revenue) <= 10000
ORDER BY
  1 DESC,
  4
LIMIT
  25"#,
        expect: Expect::NotACubeQuery,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
            F::Leaf("orders_view.count", "gt", &["10"]),
            F::Leaf("orders_view.revenue", "lte", &["10000"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.created_at BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.created_at", "inDateRange", &["30 days ago", "today"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.created_at BETWEEN CURRENT_DATE AND CURRENT_DATE"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.created_at", "inDateRange", &["CURRENT_DATE", "CURRENT_DATE"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.created_at BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND CURRENT_DATE"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.created_at", "inDateRange", &["this month", "today"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date > DATE_TRUNC('week', CURRENT_DATE)"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterDate", &["this week"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date >= CURRENT_DATE - INTERVAL '30 days'"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["30 days ago"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date < DATE_TRUNC('month', CURRENT_DATE)"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "beforeDate", &["this month"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date <= CURRENT_DATE"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "beforeOrOnDate", &["today"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  DATE_TRUNC('year', orders_view.date) = DATE_TRUNC('year', CURRENT_DATE)"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "equals", &["this year"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date < CURRENT_DATE + INTERVAL '7 days'"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "beforeDate", &["7 days from now"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date >= CURRENT_DATE - INTERVAL '1 day'"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["yesterday"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date > '2025-01-01'
  AND orders_view.date < DATE_TRUNC('week', CURRENT_DATE)"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "afterDate", &["2025-01-01"]),
            F::Leaf("orders_view.date", "beforeDate", &["this week"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  DATE_TRUNC('week', orders_view.date) = DATE_TRUNC('week', CURRENT_DATE)"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "equals", &["this week"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date = CURRENT_DATE - INTERVAL '30 days'"#,
        expect: Expect::RelativeDate,
        expected: &[
            F::Leaf("orders_view.date", "equals", &["30 days ago"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  (
    orders_view.status = 'active'
    OR orders_view.users_state LIKE '%California%'
  )
GROUP BY
  1"#,
        expect: Expect::Match,
        expected: &[
            F::Or(&[
                F::Leaf("orders_view.status", "equals", &["active"]),
                F::Leaf("orders_view.users_state", "contains", &["California"]),
            ]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  (
    orders_view.status = 'active'
    OR orders_view.users_state LIKE '%California%'
  )
  AND orders_view.date >= '2023-01-01'
GROUP BY
  1"#,
        expect: Expect::Match,
        expected: &[
            F::Or(&[
                F::Leaf("orders_view.status", "equals", &["active"]),
                F::Leaf("orders_view.users_state", "contains", &["California"]),
            ]),
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
        ],
    },
    Case {
        sql: r#"SELECT
  MEASURE(orders_view.total_amount)
FROM
  orders_view
WHERE
  (
    orders_view.status = 'active'
    OR orders_view.product_category = 'Acme Sports - Trial'
    OR orders_view.product_category = '(closed) Acme Pharma - Trial'
    OR orders_view.product_category = 'Best Case'
  )
  AND orders_view.users_state IN ('CA', 'NY')
LIMIT
  5000"#,
        expect: Expect::Match,
        expected: &[
            F::Or(&[
                F::Leaf("orders_view.status", "equals", &["active"]),
                F::Leaf("orders_view.product_category", "equals", &["Acme Sports - Trial"]),
                F::Leaf("orders_view.product_category", "equals", &["(closed) Acme Pharma - Trial"]),
                F::Leaf("orders_view.product_category", "equals", &["Best Case"]),
            ]),
            F::Leaf("orders_view.users_state", "equals", &["CA", "NY"]),
        ],
    },
    Case {
        sql: r#"SELECT
  DATE_TRUNC('day', orders_view.date) AS date_day,
  CONCAT(
    COALESCE(orders_view.status, ''),
    '-',
    COALESCE(orders_view.product_category, '')
  ) AS combined_label,
  MEASURE(orders_view.count) AS count
FROM
  orders_view
WHERE
  orders_view.status = 'active'
GROUP BY
  1,
  2
ORDER BY
  1,
  2
LIMIT
  5000"#,
        expect: Expect::NotACubeQuery,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.total_amount) AS total_amount
FROM
  orders_view
WHERE
  orders_view.status IN (
    'active',
    'processing',
    'shipped',
    'completed',
    'cancelled'
  )
GROUP BY
  1
ORDER BY
  CASE
    WHEN orders_view.status = 'active' THEN 1
    WHEN orders_view.status = 'processing' THEN 2
    WHEN orders_view.status = 'shipped' THEN 3
    WHEN orders_view.status = 'completed' THEN 4
    WHEN orders_view.status = 'cancelled' THEN 5
    ELSE 6
  END
LIMIT
  5000"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.status", "equals", &["active", "processing", "shipped", "completed", "cancelled"]),
        ],
    },
    Case {
        sql: r#"SELECT
  orders_view.products_brand,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.products_brand IN ('Patagonia', 'Columbia', 'Levi''s')
GROUP BY
  1
LIMIT
  5000"#,
        expect: Expect::Match,
        expected: &[
            F::Leaf("orders_view.products_brand", "equals", &["Patagonia", "Columbia", "Levi's"]),
        ],
    },
];

const WRITE_CASES: &[WriteCase] = &[
    WriteCase {
        sql: r#"SELECT
  DATE_TRUNC('month', orders_view.date) AS date_month,
  orders_view.product_category,
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  orders_view.customers_city = 'New York'
GROUP BY
  1,
  2
ORDER BY
  1,
  3 DESC
LIMIT
  100"#,
        filters: &[F::Leaf(
            "orders_view.customers_city",
            "equals",
            &["New York"],
        )],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.customer_id
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.count) != 0"#,
        filters: &[F::Leaf("orders_view.count", "notEquals", &["0"])],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.customer_id > 100
  AND orders_view.amount < 1000
  AND orders_view.quantity >= 5
  AND orders_view.discount <= 0.2
  AND orders_view.status != 'cancelled'"#,
        filters: &[
            F::Leaf("orders_view.customer_id", "gt", &["100"]),
            F::Leaf("orders_view.amount", "lt", &["1000"]),
            F::Leaf("orders_view.quantity", "gte", &["5"]),
            F::Leaf("orders_view.discount", "lte", &["0.2"]),
            F::Leaf("orders_view.status", "notEquals", &["cancelled"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  orders_view.customer_id >= 100
  AND orders_view.customer_id <= 1000
  AND orders_view.status != 'cancelled'
  AND orders_view.product_category IN ('Electronics', 'Books')
  AND orders_view.amount > 50
GROUP BY
  1"#,
        filters: &[
            F::Leaf("orders_view.customer_id", "gte", &["100"]),
            F::Leaf("orders_view.customer_id", "lte", &["1000"]),
            F::Leaf("orders_view.status", "notEquals", &["cancelled"]),
            F::Leaf(
                "orders_view.product_category",
                "equals",
                &["Electronics", "Books"],
            ),
            F::Leaf("orders_view.amount", "gt", &["50"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.balance > -100
  AND orders_view.adjustment <= -5.5"#,
        filters: &[
            F::Leaf("orders_view.balance", "gt", &["-100"]),
            F::Leaf("orders_view.adjustment", "lte", &["-5.5"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status IS NULL
  AND orders_view.customer_id IS NOT NULL"#,
        filters: &[
            F::Leaf("orders_view.status", "notSet", &[]),
            F::Leaf("orders_view.customer_id", "set", &[]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status = ''
  AND orders_view.product_category != ''"#,
        filters: &[
            F::Leaf("orders_view.status", "equals", &[""]),
            F::Leaf("orders_view.product_category", "notEquals", &[""]),
        ],
    },
    WriteCase {
        sql: r#"SELECT DISTINCT
  orders_view.status,
  orders_view.product_category
FROM
  orders_view
WHERE
  orders_view.customer_id > 100
  AND orders_view.date >= '2023-01-01'"#,
        filters: &[
            F::Leaf("orders_view.customer_id", "gt", &["100"]),
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT DISTINCT
  DATE_TRUNC('day', orders_view.date) AS date_day,
  orders_view.status
FROM
  orders_view
WHERE
  orders_view.status IN ('active', 'pending')
ORDER BY
  1 DESC,
  2
LIMIT
  50"#,
        filters: &[F::Leaf(
            "orders_view.status",
            "equals",
            &["active", "pending"],
        )],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category ILIKE '%Electronics%'"#,
        filters: &[F::Leaf(
            "orders_view.product_category",
            "contains",
            &["Electronics"],
        )],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category NOT ILIKE '%Electronics%'"#,
        filters: &[F::Leaf(
            "orders_view.product_category",
            "notContains",
            &["Electronics"],
        )],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status ILIKE 'pending%'"#,
        filters: &[F::Leaf("orders_view.status", "startsWith", &["pending"])],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status NOT ILIKE 'pending%'"#,
        filters: &[F::Leaf("orders_view.status", "notStartsWith", &["pending"])],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.customers_city ILIKE '%York'"#,
        filters: &[F::Leaf("orders_view.customers_city", "endsWith", &["York"])],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.customers_city NOT ILIKE '%York'"#,
        filters: &[F::Leaf(
            "orders_view.customers_city",
            "notEndsWith",
            &["York"],
        )],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category ILIKE '%Electronics%'
  AND orders_view.status ILIKE 'pending%'
  AND orders_view.customers_city NOT ILIKE '%York'
GROUP BY
  1"#,
        filters: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
            F::Leaf("orders_view.status", "startsWith", &["pending"]),
            F::Leaf("orders_view.customers_city", "notEndsWith", &["York"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category ILIKE '%Men''s Clothing%'
  AND orders_view.description ILIKE 'Special "Premium"%'
  AND orders_view.notes ILIKE '%Order #123'"#,
        filters: &[
            F::Leaf(
                "orders_view.product_category",
                "contains",
                &["Men's Clothing"],
            ),
            F::Leaf(
                "orders_view.description",
                "startsWith",
                &[r#"Special "Premium""#],
            ),
            F::Leaf("orders_view.notes", "endsWith", &["Order #123"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category LIKE '%Electronics%'
  AND orders_view.status ILIKE 'pending%'
  AND orders_view.customers_city NOT ILIKE '%York'
GROUP BY
  1"#,
        filters: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
            F::Leaf("orders_view.status", "startsWith", &["pending"]),
            F::Leaf("orders_view.customers_city", "notEndsWith", &["York"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.product_category ILIKE '%Electronics%'
  AND orders_view.status ILIKE 'pending%'"#,
        filters: &[
            F::Leaf("orders_view.product_category", "contains", &["Electronics"]),
            F::Leaf("orders_view.status", "startsWith", &["pending"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date BETWEEN '2023-01-01' AND '2023-12-31'"#,
        filters: &[F::Leaf(
            "orders_view.date",
            "inDateRange",
            &["2023-01-01", "2023-12-31"],
        )],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date > '2023-06-01'
GROUP BY
  1"#,
        filters: &[F::Leaf("orders_view.date", "afterDate", &["2023-06-01"])],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  orders_view.date <= '2023-12-31'"#,
        filters: &[F::Leaf(
            "orders_view.date",
            "beforeOrOnDate",
            &["2023-12-31"],
        )],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date >= '2023-01-01'
  AND orders_view.date < '2024-01-01'
  AND orders_view.status = 'active'
GROUP BY
  1"#,
        filters: &[
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeDate", &["2024-01-01"]),
            F::Leaf("orders_view.status", "equals", &["active"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date >= '2023-07-15T00:00:00Z'
  AND orders_view.date <= '2023-07-15T23:59:59Z'"#,
        filters: &[
            F::Leaf(
                "orders_view.date",
                "afterOrOnDate",
                &["2023-07-15T00:00:00Z"],
            ),
            F::Leaf(
                "orders_view.date",
                "beforeOrOnDate",
                &["2023-07-15T23:59:59Z"],
            ),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  DATE_TRUNC('month', orders_view.date) AS date_month,
  orders_view.status,
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  orders_view.date BETWEEN '2023-01-01' AND '2023-12-31'
  AND orders_view.status IN ('pending', 'completed')
  AND orders_view.customer_id > 100
GROUP BY
  1,
  2
ORDER BY
  1
LIMIT
  50"#,
        filters: &[
            F::Leaf(
                "orders_view.date",
                "inDateRange",
                &["2023-01-01", "2023-12-31"],
            ),
            F::Leaf("orders_view.status", "equals", &["pending", "completed"]),
            F::Leaf("orders_view.customer_id", "gt", &["100"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date IS NOT NULL
  AND orders_view.status = 'pending'
GROUP BY
  1"#,
        filters: &[
            F::Leaf("orders_view.date", "set", &[]),
            F::Leaf("orders_view.status", "equals", &["pending"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date > '2023-01-01'
  AND orders_view.date <= '2023-12-31'"#,
        filters: &[
            F::Leaf("orders_view.date", "afterDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeOrOnDate", &["2023-12-31"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date > '2023-01-01'
  AND orders_view.date < '2023-12-31'"#,
        filters: &[
            F::Leaf("orders_view.date", "afterDate", &["2023-01-01"]),
            F::Leaf("orders_view.date", "beforeDate", &["2023-12-31"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.date >= '2023-01-01'
  AND orders_view.date <= '2023-12-31'"#,
        filters: &[F::Leaf(
            "orders_view.date",
            "inDateRange",
            &["2023-01-01", "2023-12-31"],
        )],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count),
  MEASURE(orders_view.total_amount)
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.count) > 10
  AND MEASURE(orders_view.total_amount) <= 1000"#,
        filters: &[
            F::Leaf("orders_view.count", "gt", &["10"]),
            F::Leaf("orders_view.total_amount", "lte", &["1000"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.product_category,
  MEASURE(orders_view.revenue)
FROM
  orders_view
WHERE
  orders_view.status = 'active'
  AND orders_view.product_category IN ('Electronics', 'Books')
GROUP BY
  1
HAVING
  MEASURE(orders_view.revenue) > 100"#,
        filters: &[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf(
                "orders_view.product_category",
                "equals",
                &["Electronics", "Books"],
            ),
            F::Leaf("orders_view.revenue", "gt", &["100"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  orders_view.status = 'pending'
GROUP BY
  1
HAVING
  MEASURE(orders_view.count) > 5"#,
        filters: &[
            F::Leaf("orders_view.status", "equals", &["pending"]),
            F::Leaf("orders_view.count", "gt", &["5"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.customer_id,
  MEASURE(orders_view.count),
  MEASURE(orders_view.revenue),
  MEASURE(orders_view.avg_amount)
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.count) >= 5
  AND MEASURE(orders_view.revenue) < 1000
  AND MEASURE(orders_view.avg_amount) != 0"#,
        filters: &[
            F::Leaf("orders_view.count", "gte", &["5"]),
            F::Leaf("orders_view.revenue", "lt", &["1000"]),
            F::Leaf("orders_view.avg_amount", "notEquals", &["0"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count),
  MEASURE(orders_view.category_count)
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.count) IN (1, 5, 10)
  AND MEASURE(orders_view.category_count) NOT IN (0, 1)"#,
        filters: &[
            F::Leaf("orders_view.count", "equals", &["1", "5", "10"]),
            F::Leaf("orders_view.category_count", "notEquals", &["0", "1"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.customer_id,
  MEASURE(orders_view.discount_amount),
  MEASURE(orders_view.bonus_points)
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.discount_amount) IS NOT NULL
  AND MEASURE(orders_view.bonus_points) IS NULL"#,
        filters: &[
            F::Leaf("orders_view.discount_amount", "set", &[]),
            F::Leaf("orders_view.bonus_points", "notSet", &[]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.customer_id,
  MEASURE(orders_view.total_amount)
FROM
  orders_view
GROUP BY
  1
HAVING
  MEASURE(orders_view.total_amount) > 50"#,
        filters: &[F::Leaf("orders_view.total_amount", "gt", &["50"])],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  (
    orders_view.status = 'active'
    OR orders_view.users_state LIKE '%California%'
  )
GROUP BY
  1"#,
        filters: &[F::Or(&[
            F::Leaf("orders_view.status", "equals", &["active"]),
            F::Leaf("orders_view.users_state", "contains", &["California"]),
        ])],
    },
    WriteCase {
        sql: r#"SELECT
  orders_view.status,
  MEASURE(orders_view.count)
FROM
  orders_view
WHERE
  (
    orders_view.status = 'active'
    OR orders_view.users_state LIKE '%California%'
  )
  AND orders_view.date >= '2023-01-01'
GROUP BY
  1"#,
        filters: &[
            F::Or(&[
                F::Leaf("orders_view.status", "equals", &["active"]),
                F::Leaf("orders_view.users_state", "contains", &["California"]),
            ]),
            F::Leaf("orders_view.date", "afterOrOnDate", &["2023-01-01"]),
        ],
    },
    WriteCase {
        sql: r#"SELECT
  MEASURE(orders_view.total_amount)
FROM
  orders_view
WHERE
  (
    orders_view.status = 'active'
    OR orders_view.product_category = 'Acme Sports - Trial'
    OR orders_view.product_category = '(closed) Acme Pharma - Trial'
    OR orders_view.product_category = 'Best Case'
  )
  AND orders_view.users_state IN ('CA', 'NY')
LIMIT
  5000"#,
        filters: &[
            F::Or(&[
                F::Leaf("orders_view.status", "equals", &["active"]),
                F::Leaf(
                    "orders_view.product_category",
                    "equals",
                    &["Acme Sports - Trial"],
                ),
                F::Leaf(
                    "orders_view.product_category",
                    "equals",
                    &["(closed) Acme Pharma - Trial"],
                ),
                F::Leaf("orders_view.product_category", "equals", &["Best Case"]),
            ]),
            F::Leaf("orders_view.users_state", "equals", &["CA", "NY"]),
        ],
    },
];

fn members(filters: &[V1LoadRequestQueryFilterItem]) -> HashSet<String> {
    filters
        .iter()
        .filter_map(|filter| filter.member.clone())
        .collect()
}

fn keys(filters: &[V1LoadRequestQueryFilterItem], meta: &MetaContext) -> HashSet<FilterKey> {
    filters
        .iter()
        .map(|filter| filter_key(filter, meta))
        .collect()
}

/// Reads the filters of every case and reports each one whose filters come
/// back different from what its expectation allows.
#[tokio::test]
async fn test_console_suite_reads_filters() -> Result<(), CubeError> {
    let meta = suite_meta();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let mut mismatches = Vec::new();
    for case in CASES {
        let expected = case.expected();
        let reported = match get_sql_filters(case.sql, meta.clone(), session.clone()).await {
            Ok(reported) => {
                if case.expect == Expect::NotPlanned {
                    mismatches.push(format!(
                        "{}\n    expect: NotPlanned, but the query planned",
                        case.sql
                    ));
                }
                reported
            }
            Err(e) => {
                if !matches!(case.expect, Expect::NotPlanned | Expect::CustomExpression) {
                    mismatches.push(format!("{}\n    error: {}", case.sql, e));
                }
                continue;
            }
        };

        let holds = match case.expect {
            Expect::Match => keys(&reported, &meta) == keys(&expected, &meta),
            Expect::DateRangeMerge => {
                members(&reported) == members(&expected)
                    && reported
                        .iter()
                        .any(|filter| filter.operator.as_deref() == Some("inDateRange"))
            }
            Expect::RelativeDate => members(&reported) == members(&expected),
            Expect::NotACubeQuery | Expect::UnsupportedFilter => reported.is_empty(),
            Expect::CustomExpression => {
                reported.is_empty() || members(&reported).is_subset(&members(&expected))
            }
            // Handled above, where the query planned when it should not
            Expect::NotPlanned => true,
        };

        if !holds {
            mismatches.push(format!(
                "{}\n    expect: {:?}\n    expected: {:?}\n    reported: {:?}",
                case.sql,
                case.expect,
                keys(&expected, &meta).iter().collect::<Vec<_>>(),
                keys(&reported, &meta).iter().collect::<Vec<_>>()
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} of {} cases differ:\n\n{}",
        mismatches.len(),
        CASES.len(),
        mismatches.join("\n\n")
    );

    Ok(())
}

/// Takes the filters this API reports for each matching case and writes them
/// back with `set`, then removes them with `delete` from the original SQL, so
/// that every shape the suite carries is known to survive a round trip and not
/// only to be readable.
#[tokio::test]
async fn test_console_suite_round_trip() -> Result<(), CubeError> {
    let meta = suite_meta();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let mut mismatches = Vec::new();
    let mut checked = 0;
    for case in CASES {
        if case.expect != Expect::Match || case.expected.is_empty() {
            continue;
        }

        let reported = match get_sql_filters(case.sql, meta.clone(), session.clone()).await {
            Ok(reported) => reported,
            Err(e) => {
                mismatches.push(format!("{}\n    read error: {}", case.sql, e));
                continue;
            }
        };
        checked += 1;

        let set = match set_sql_filters(case.sql, &reported, meta.clone(), session.clone()).await {
            Ok(update) => update,
            Err(e) => {
                mismatches.push(format!("{}\n    set error: {}", case.sql, e));
                continue;
            }
        };
        if keys(&set.filters, &meta) != keys(&reported, &meta) {
            mismatches.push(format!(
                "{}\n    set changed the filters\n    before: {:?}\n    after: {:?}",
                case.sql,
                keys(&reported, &meta).iter().collect::<Vec<_>>(),
                keys(&set.filters, &meta).iter().collect::<Vec<_>>()
            ));
            continue;
        }

        // The delete goes back to the original SQL, where the predicates are
        // written the way the query wrote them rather than the way `set`
        // renders them
        match delete_sql_filters(case.sql, &reported, meta.clone(), session.clone()).await {
            Ok(update) if !update.filters.is_empty() => mismatches.push(format!(
                "{}\n    delete left {:?}",
                case.sql,
                keys(&update.filters, &meta).iter().collect::<Vec<_>>()
            )),
            Ok(_) => {}
            Err(e) => mismatches.push(format!("{}\n    delete error: {}", case.sql, e)),
        }
    }

    assert!(
        checked > 50,
        "the suite should exercise the round trip, only {} cases did",
        checked
    );
    assert!(
        mismatches.is_empty(),
        "{} of {} round trips differ:\n\n{}",
        mismatches.len(),
        checked,
        mismatches.join("\n\n")
    );

    Ok(())
}

/// Adds the filters each generator case was given to its SQL with the filters
/// taken out, and compares what the plan reports with what it reports for the
/// SQL the console wrote, so that this API writes what the console writes.
#[tokio::test]
async fn test_console_suite_writes_filters() -> Result<(), CubeError> {
    let meta = suite_meta();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

    let mut mismatches = Vec::new();
    for case in WRITE_CASES {
        let reference = match get_sql_filters(case.sql, meta.clone(), session.clone()).await {
            Ok(reference) => reference,
            Err(e) => {
                mismatches.push(format!("{}\n    reference error: {}", case.sql, e));
                continue;
            }
        };

        let bare = case.bare_sql();
        let added = add_sql_filters(&bare, &case.filters(), meta.clone(), session.clone()).await;
        let holds = added
            .as_ref()
            .is_ok_and(|update| keys(&update.filters, &meta) == keys(&reference, &meta));

        if !holds {
            mismatches.push(format!(
                "{}\n    bare: {}\n    added: {:?}\n    reference: {:?}",
                case.sql,
                bare,
                added.map(|update| (
                    update.sql,
                    keys(&update.filters, &meta).into_iter().collect::<Vec<_>>()
                )),
                keys(&reference, &meta).iter().collect::<Vec<_>>()
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} of {} write cases differ:\n\n{}",
        mismatches.len(),
        WRITE_CASES.len(),
        mismatches.join("\n\n")
    );

    Ok(())
}
