use std::sync::Arc;

use datafusion::logical_plan::{plan::Extension, Filter, LogicalPlan, PlanVisitor};

use crate::{
    compile::engine::df::{
        scan::CubeScanNode,
        wrapper::{CubeScanWrappedSqlNode, CubeScanWrapperNode},
    },
    CubeError,
};

pub trait LogicalPlanTestUtils {
    fn try_expect_root_cube_scan(&self) -> Option<&CubeScanNode>;

    fn expect_root_cube_scan(&self) -> &CubeScanNode {
        self.try_expect_root_cube_scan()
            .expect("Root node is not CubeScan")
    }

    fn find_cube_scan(&self) -> CubeScanNode;

    fn find_cube_scan_wrapped_sql(&self) -> CubeScanWrappedSqlNode;

    /// Same, but for plans that still have post processing above the pushed down part.
    fn find_cube_scan_wrapped_sql_deep(&self) -> CubeScanWrappedSqlNode;

    fn find_cube_scans(&self) -> Vec<CubeScanNode>;

    fn find_filter(&self) -> Option<Filter>;
}

impl LogicalPlanTestUtils for LogicalPlan {
    fn try_expect_root_cube_scan(&self) -> Option<&CubeScanNode> {
        let LogicalPlan::Extension(ext) = self else {
            return None;
        };
        ext.node.as_any().downcast_ref::<CubeScanNode>()
    }

    fn find_cube_scan(&self) -> CubeScanNode {
        let cube_scans = find_cube_scans_deep_search(Arc::new(self.clone()), true);
        if cube_scans.len() != 1 {
            panic!("The plan includes not 1 cube_scan!");
        }

        cube_scans[0].clone()
    }

    fn find_cube_scan_wrapped_sql(&self) -> CubeScanWrappedSqlNode {
        match self {
            LogicalPlan::Extension(Extension { node }) => {
                if let Some(wrapper_node) = node.as_any().downcast_ref::<CubeScanWrappedSqlNode>() {
                    wrapper_node.clone()
                } else {
                    panic!("Root plan node is not cube_scan_wrapped_sql!");
                }
            }
            _ => panic!("Root plan node is not extension!"),
        }
    }

    fn find_cube_scan_wrapped_sql_deep(&self) -> CubeScanWrappedSqlNode {
        pub struct FindWrappedSqlNodeVisitor(Vec<CubeScanWrappedSqlNode>);

        impl PlanVisitor for FindWrappedSqlNodeVisitor {
            type Error = CubeError;

            fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
                if let LogicalPlan::Extension(ext) = plan {
                    if let Some(node) = ext.node.as_any().downcast_ref::<CubeScanWrappedSqlNode>() {
                        self.0.push(node.clone());
                    }
                }
                Ok(true)
            }
        }

        let mut visitor = FindWrappedSqlNodeVisitor(Vec::new());
        self.accept(&mut visitor).unwrap();
        match visitor.0.len() {
            1 => visitor.0.remove(0),
            found => panic!(
                "The plan includes {} cube_scan_wrapped_sql nodes, expected 1",
                found
            ),
        }
    }

    fn find_cube_scans(&self) -> Vec<CubeScanNode> {
        find_cube_scans_deep_search(Arc::new(self.clone()), true)
    }

    fn find_filter(&self) -> Option<Filter> {
        find_filter_deep_search(Arc::new(self.clone()))
    }
}

/// SQL of every member in a pushed down request, in order.
///
/// A pushed down query carries its members as member expressions: JSON holding a generated
/// alias, the cube it came from and the SQL to evaluate. Only the SQL is worth asserting on,
/// since aliases are generated and truncated to 16 characters. Members that are plain names
/// are returned as they are, so a request can mix both.
pub fn member_expression_sql(members: &Option<Vec<String>>) -> Vec<String> {
    let Some(members) = members else {
        return vec![];
    };

    members
        .iter()
        .map(|member| {
            serde_json::from_str::<serde_json::Value>(member)
                .ok()
                .and_then(|member| member["expr"]["sql"].as_str().map(String::from))
                .unwrap_or_else(|| member.clone())
        })
        .collect()
}

pub fn find_cube_scans_deep_search(
    parent: Arc<LogicalPlan>,
    panic_if_empty: bool,
) -> Vec<CubeScanNode> {
    pub struct FindCubeScanNodeVisitor(Vec<CubeScanNode>);

    impl PlanVisitor for FindCubeScanNodeVisitor {
        type Error = CubeError;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
            if let LogicalPlan::Extension(ext) = plan {
                if let Some(scan_node) = ext.node.as_any().downcast_ref::<CubeScanNode>() {
                    self.0.push(scan_node.clone());
                } else if let Some(wrapper_node) =
                    ext.node.as_any().downcast_ref::<CubeScanWrapperNode>()
                {
                    wrapper_node.wrapped_plan.accept(self)?;
                } else if let Some(wrapper_node) =
                    ext.node.as_any().downcast_ref::<CubeScanWrappedSqlNode>()
                {
                    wrapper_node.wrapped_plan.accept(self)?;
                }
            }
            Ok(true)
        }
    }

    let mut visitor = FindCubeScanNodeVisitor(Vec::new());
    parent.accept(&mut visitor).unwrap();

    if panic_if_empty && visitor.0.len() == 0 {
        panic!("No CubeScanNode was found in plan");
    }

    visitor.0
}

pub fn find_filter_deep_search(parent: Arc<LogicalPlan>) -> Option<Filter> {
    pub struct FindFilterNodeVisitor(Option<Filter>);

    impl PlanVisitor for FindFilterNodeVisitor {
        type Error = CubeError;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
            if let LogicalPlan::Filter(filter) = plan {
                self.0 = Some(filter.clone());
            }
            Ok(true)
        }
    }

    let mut visitor = FindFilterNodeVisitor(None);
    parent.accept(&mut visitor).unwrap();
    visitor.0
}
