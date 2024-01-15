use crate::compile::rewrite::{analysis::LogicalPlanAnalysis, LogicalPlanLanguage};
use datafusion::scalar::ScalarValue;
use egg::EGraph;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    convert::TryInto,
    hash::{Hash, Hasher},
};

pub struct ShaHasher {
    hasher: Sha256,
}

impl Hasher for ShaHasher {
    #[inline]
    fn finish(&self) -> u64 {
        let mut result = [0; 32];
        result.copy_from_slice(&self.hasher.clone().finalize());
        u64::from_le_bytes(result[0..8].try_into().unwrap())
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }
}

impl ShaHasher {
    pub fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    pub fn take_hasher(self) -> Sha256 {
        self.hasher
    }
}

pub fn egraph_hash(
    egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    params: Option<&HashMap<usize, ScalarValue>>,
) -> [u8; 32] {
    let mut hasher = ShaHasher::new();
    for class in egraph.classes() {
        class.id.hash(&mut hasher);
        class.nodes.len().hash(&mut hasher);
        for node in &class.nodes {
            node.hash(&mut hasher);
        }
    }
    if let Some(params) = params {
        for (k, v) in params.iter() {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
    }
    hasher.take_hasher().finalize().into()
}
