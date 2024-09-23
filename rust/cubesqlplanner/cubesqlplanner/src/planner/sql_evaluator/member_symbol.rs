use super::dependecy::Dependency;
use super::{
    Compiler, CubeNameSymbol, CubeTableSymbol, DimensionSymbol, JoinConditionSymbol,
    MeasureFilterSymbol, MeasureSymbol,
};
use crate::cube_bridge::memeber_sql::MemberSql;
use cubenativeutils::CubeError;
use std::rc::Rc;
pub trait MemberSymbol {
    fn cube_name(&self) -> &String;
}

pub enum MemberSymbolType {
    Dimension(DimensionSymbol),
    Measure(MeasureSymbol),
    CubeName(CubeNameSymbol),
    CubeTable(CubeTableSymbol),
    JoinCondition(JoinConditionSymbol),
    MeasureFilter(MeasureFilterSymbol),
}

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

    pub fn new_join_condition(symbol: JoinConditionSymbol, deps: Vec<Dependency>) -> Rc<Self> {
        Rc::new(Self {
            symbol: MemberSymbolType::JoinCondition(symbol),
            deps,
        })
    }

    pub fn new_measure_filter(symbol: MeasureFilterSymbol, deps: Vec<Dependency>) -> Rc<Self> {
        Rc::new(Self {
            symbol: MemberSymbolType::MeasureFilter(symbol),
            deps,
        })
    }

    pub fn deps(&self) -> &Vec<Dependency> {
        &self.deps
    }

    pub fn symbol(&self) -> &MemberSymbolType {
        &self.symbol
    }
}

pub trait MemberSymbolFactory: Sized {
    fn symbol_name() -> String; //FIXME maybe Enum should be used
    fn is_cachable() -> bool {
        true
    }
    fn cube_name(&self) -> &String;
    fn deps_names(&self) -> Result<Vec<String>, CubeError>;
    fn member_sql(&self) -> Option<Rc<dyn MemberSql>>;
    fn build(
        self,
        deps: Vec<Dependency>,
        compiler: &mut Compiler,
    ) -> Result<Rc<EvaluationNode>, CubeError>;
}
