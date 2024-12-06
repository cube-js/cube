use super::dependecy::Dependency;
use super::{
    CubeNameSymbol, CubeTableSymbol, DimensionSymbol, MeasureSymbol, MemberSymbolType,
    SimpleSqlSymbol,
};
use std::rc::Rc;

pub struct EvaluationNode {
    symbol: MemberSymbolType,
    deps: Vec<Dependency>,
}

impl EvaluationNode {
    pub fn new(symbol: MemberSymbolType, deps: Vec<Dependency>) -> Rc<Self> {
        Rc::new(Self { symbol, deps })
    }

    pub fn new_measure(symbol: MeasureSymbol, deps: Vec<Dependency>) -> Rc<Self> {
        Rc::new(Self {
            symbol: MemberSymbolType::Measure(symbol),
            deps,
        })
    }

    pub fn new_dimension(symbol: DimensionSymbol, deps: Vec<Dependency>) -> Rc<Self> {
        Rc::new(Self {
            symbol: MemberSymbolType::Dimension(symbol),
            deps,
        })
    }

    pub fn new_cube_name(symbol: CubeNameSymbol) -> Rc<Self> {
        Rc::new(Self {
            symbol: MemberSymbolType::CubeName(symbol),
            deps: vec![],
        })
    }

    pub fn new_cube_table(symbol: CubeTableSymbol, deps: Vec<Dependency>) -> Rc<Self> {
        Rc::new(Self {
            symbol: MemberSymbolType::CubeTable(symbol),
            deps,
        })
    }

    pub fn new_simple_sql(symbol: SimpleSqlSymbol, deps: Vec<Dependency>) -> Rc<Self> {
        Rc::new(Self {
            symbol: MemberSymbolType::SimpleSql(symbol),
            deps,
        })
    }

    pub fn deps(&self) -> &Vec<Dependency> {
        &self.deps
    }

    pub fn symbol(&self) -> &MemberSymbolType {
        &self.symbol
    }

    pub fn full_name(&self) -> String {
        self.symbol.full_name()
    }

    pub fn is_measure(&self) -> bool {
        self.symbol.is_measure()
    }
    pub fn is_dimension(&self) -> bool {
        self.symbol.is_dimension()
    }
}
