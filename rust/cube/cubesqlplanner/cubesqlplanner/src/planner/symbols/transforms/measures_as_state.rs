use super::super::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Rebuilds the symbol tree with every measure that has a mergeable
/// state form (`count_distinct_approx` → HLL state) switched to it —
/// for queries whose aggregations feed an outer merge instead of
/// producing final values: pre-aggregation builds and multi-stage
/// leaves under an aggregating stage.
pub fn measures_as_state(symbol: &Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> {
    symbol.apply_recursive(&|node| {
        if let MemberSymbol::Measure(measure) = node.as_ref() {
            if let Some(kind) = measure.kind().as_state() {
                let mut new = (**measure).clone();
                new.kind = kind;
                return Ok(Rc::new(MemberSymbol::Measure(Rc::new(new))));
            }
        }
        Ok(node.clone())
    })
}
