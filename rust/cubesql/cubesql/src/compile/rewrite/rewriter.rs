use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            analysis::LogicalPlanAnalysis,
            converter::LanguageToLogicalPlanConverter,
            cost::BestCubePlan,
            rules::{
                dates::DateRules, filters::FilterRules, members::MemberRules, order::OrderRules,
                split::SplitRules,
            },
            LogicalPlanLanguage,
        },
    },
    sql::AuthContext,
    CubeError,
};
use datafusion::{logical_plan::LogicalPlan, physical_plan::planner::DefaultPhysicalPlanner};
use egg::{EGraph, Extractor, Id, Rewrite, Runner};
use std::sync::Arc;

pub struct Rewriter {
    graph: EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    cube_context: Arc<CubeContext>,
}

impl Rewriter {
    pub fn new(
        graph: EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        cube_context: Arc<CubeContext>,
    ) -> Self {
        Self {
            graph,
            cube_context,
        }
    }

    pub fn rewrite_runner(&self) -> Runner<LogicalPlanLanguage, LogicalPlanAnalysis> {
        Runner::<LogicalPlanLanguage, LogicalPlanAnalysis>::new(LogicalPlanAnalysis::new(
            self.cube_context.clone(),
            Arc::new(DefaultPhysicalPlanner::default()),
        ))
        .with_iter_limit(100)
        .with_node_limit(10000)
        .with_egraph(self.graph.clone())
    }

    pub fn find_best_plan(
        &mut self,
        root: Id,
        auth_context: Arc<AuthContext>,
    ) -> Result<LogicalPlan, CubeError> {
        let runner = self.rewrite_runner();
        let rules = self.rewrite_rules();
        let runner = runner.run(rules.iter());
        log::debug!("Iterations: {:?}", runner.iterations);
        let extractor = Extractor::new(&runner.egraph, BestCubePlan);
        let (_, best) = extractor.find_best(root);
        let new_root = Id::from(best.as_ref().len() - 1);
        //log::debug!("Egraph: {:#?}", runner.egraph);
        log::debug!("Best: {:?}", best);
        self.graph = runner.egraph.clone();
        let converter =
            LanguageToLogicalPlanConverter::new(best, self.cube_context.clone(), auth_context);
        converter.to_logical_plan(new_root)
    }

    pub fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        let rules: Vec<Box<dyn RewriteRules>> = vec![
            Box::new(MemberRules::new(self.cube_context.clone())),
            Box::new(FilterRules::new(self.cube_context.clone())),
            Box::new(DateRules::new(self.cube_context.clone())),
            Box::new(OrderRules::new(self.cube_context.clone())),
            Box::new(SplitRules::new(self.cube_context.clone())),
        ];
        let mut rewrites = Vec::new();
        for r in rules {
            rewrites.extend(r.rewrite_rules());
        }
        rewrites
    }
}

pub trait RewriteRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>;
}
