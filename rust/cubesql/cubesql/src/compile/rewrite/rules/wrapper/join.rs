use crate::compile::rewrite::{rewriter::CubeRewrite, rules::wrapper::WrapperRules};

impl WrapperRules {
    pub fn join_rules(&self, _rules: &mut Vec<CubeRewrite>) {}
}
