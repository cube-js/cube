use super::{
    CubeNameSymbol, CubeTableSymbol, DimensionSymbol, MeasureSymbol, MemberSymbol, SimpleSqlSymbol,
};
pub enum MemberSymbolType {
    Dimension(DimensionSymbol),
    Measure(MeasureSymbol),
    CubeName(CubeNameSymbol),
    CubeTable(CubeTableSymbol),
    SimpleSql(SimpleSqlSymbol),
}

impl MemberSymbolType {
    pub fn full_name(&self) -> String {
        match self {
            MemberSymbolType::Dimension(d) => d.full_name(),
            MemberSymbolType::Measure(m) => m.full_name(),
            MemberSymbolType::CubeName(c) => c.cube_name().clone(),
            MemberSymbolType::CubeTable(c) => c.cube_name().clone(),
            MemberSymbolType::SimpleSql(_) => "".to_string(),
        }
    }
    pub fn is_measure(&self) -> bool {
        matches!(self, Self::Measure(_))
    }
    pub fn is_dimension(&self) -> bool {
        matches!(self, Self::Dimension(_))
    }
}
