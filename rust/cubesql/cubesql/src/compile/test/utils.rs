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
    fn find_cube_scan(&self) -> CubeScanNode;

    fn find_cube_scan_wrapped_sql(&self) -> CubeScanWrappedSqlNode;

    fn find_cube_scans(&self) -> Vec<CubeScanNode>;

    fn find_filter(&self) -> Option<Filter>;
}

impl LogicalPlanTestUtils for LogicalPlan {
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

    fn find_cube_scans(&self) -> Vec<CubeScanNode> {
        find_cube_scans_deep_search(Arc::new(self.clone()), true)
    }

    fn find_filter(&self) -> Option<Filter> {
        find_filter_deep_search(Arc::new(self.clone()))
    }
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
