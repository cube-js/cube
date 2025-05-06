use cubenativeutils::CubeError;

use super::{
    CubeNameSymbol, CubeTableSymbol, DimensionSymbol, MeasureSymbol, MemberExpressionSymbol,
    TimeDimensionSymbol,
};
use crate::planner::query_tools::QueryTools;
use crate::planner::{BaseMember, MemberSymbolRef};
use std::fmt::Debug;
use std::rc::Rc;

pub enum MemberSymbol {
    Dimension(DimensionSymbol),
    TimeDimension(TimeDimensionSymbol),
    Measure(MeasureSymbol),
    CubeName(CubeNameSymbol),
    CubeTable(CubeTableSymbol),
    MemberExpression(MemberExpressionSymbol),
}

impl Debug for MemberSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dimension(_) => f.debug_tuple("Dimension").field(&self.full_name()).finish(),
            Self::TimeDimension(_) => f
                .debug_tuple("TimeDimension")
                .field(&self.full_name())
                .finish(),
            Self::Measure(_) => f.debug_tuple("Measure").field(&self.full_name()).finish(),
            Self::CubeName(_) => f.debug_tuple("CubeName").field(&self.full_name()).finish(),
            Self::CubeTable(_) => f.debug_tuple("CubeTable").field(&self.full_name()).finish(),
            Self::MemberExpression(_) => f
                .debug_tuple("MemberExpression")
                .field(&self.full_name())
                .finish(),
        }
    }
}

impl MemberSymbol {
    pub fn new_measure(symbol: MeasureSymbol) -> Rc<Self> {
        Rc::new(Self::Measure(symbol))
    }

    pub fn new_dimension(symbol: DimensionSymbol) -> Rc<Self> {
        Rc::new(Self::Dimension(symbol))
    }

    pub fn new_cube_name(symbol: CubeNameSymbol) -> Rc<Self> {
        Rc::new(Self::CubeName(symbol))
    }

    pub fn new_cube_table(symbol: CubeTableSymbol) -> Rc<Self> {
        Rc::new(Self::CubeTable(symbol))
    }

    pub fn full_name(&self) -> String {
        match self {
            Self::Dimension(d) => d.full_name(),
            Self::TimeDimension(d) => d.full_name(),
            Self::Measure(m) => m.full_name(),
            Self::CubeName(c) => c.cube_name().clone(),
            Self::CubeTable(c) => c.cube_name().clone(),
            Self::MemberExpression(e) => e.full_name().clone(),
        }
    }
    pub fn name(&self) -> String {
        match self {
            Self::Dimension(d) => d.name().clone(),
            Self::TimeDimension(d) => d.name(),
            Self::Measure(m) => m.name().clone(),
            Self::CubeName(c) => c.cube_name().clone(),
            Self::CubeTable(c) => c.cube_name().clone(),
            Self::MemberExpression(e) => e.name().clone(),
        }
    }

    pub fn cube_name(&self) -> String {
        match self {
            Self::Dimension(d) => d.cube_name().clone(),
            Self::TimeDimension(d) => d.cube_name(),
            Self::Measure(m) => m.cube_name().clone(),
            Self::CubeName(c) => c.cube_name().clone(),
            Self::CubeTable(c) => c.cube_name().clone(),
            Self::MemberExpression(e) => e.cube_name().clone(),
        }
    }

    pub fn is_multi_stage(&self) -> bool {
        match self {
            Self::Dimension(d) => d.is_multi_stage(),
            Self::TimeDimension(d) => d.is_multi_stage(),
            Self::Measure(m) => m.is_multi_stage(),
            _ => false,
        }
    }

    pub fn is_measure(&self) -> bool {
        matches!(self, Self::Measure(_))
    }

    pub fn is_dimension(&self) -> bool {
        matches!(self, Self::Dimension(_) | Self::TimeDimension(_))
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        match self {
            Self::Dimension(d) => d.get_dependencies(),
            Self::TimeDimension(d) => d.get_dependencies(),
            Self::Measure(m) => m.get_dependencies(),
            Self::CubeName(_) => vec![],
            Self::CubeTable(_) => vec![],
            Self::MemberExpression(e) => e.get_dependencies(),
        }
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        match self {
            Self::Dimension(d) => d.get_dependencies_with_path(),
            Self::TimeDimension(d) => d.get_dependencies_with_path(),
            Self::Measure(m) => m.get_dependencies_with_path(),
            Self::CubeName(_) => vec![],
            Self::CubeTable(_) => vec![],
            Self::MemberExpression(e) => e.get_dependencies_with_path(),
        }
    }

    pub fn is_reference(&self) -> bool {
        match self {
            Self::Dimension(d) => d.is_reference(),
            Self::TimeDimension(d) => d.is_reference(),
            Self::Measure(m) => m.is_reference(),
            Self::CubeName(_) => false,
            Self::CubeTable(_) => false,
            Self::MemberExpression(e) => e.is_reference(),
        }
    }

    pub fn owned_by_cube(&self) -> bool {
        match self {
            Self::Dimension(d) => d.owned_by_cube(),
            Self::TimeDimension(d) => d.owned_by_cube(),
            Self::Measure(m) => m.owned_by_cube(),
            Self::CubeName(_) => false,
            Self::CubeTable(_) => false,
            Self::MemberExpression(_) => false,
        }
    }

    pub fn as_time_dimension(&self) -> Result<&TimeDimensionSymbol, CubeError> {
        match self {
            Self::TimeDimension(d) => Ok(d),
            _ => Err(CubeError::internal(format!(
                "{} is not a time dimension",
                self.full_name()
            ))),
        }
    }

    pub fn as_dimension(&self) -> Result<&DimensionSymbol, CubeError> {
        match self {
            Self::Dimension(d) => Ok(d),
            _ => Err(CubeError::internal(format!(
                "{} is not a dimension",
                self.full_name()
            ))),
        }
    }

    pub fn as_measure(&self) -> Result<&MeasureSymbol, CubeError> {
        match self {
            Self::Measure(m) => Ok(m),
            _ => Err(CubeError::internal(format!(
                "{} is not a measure",
                self.full_name()
            ))),
        }
    }

    pub fn alias_suffix(&self) -> Option<String> {
        match self {
            Self::TimeDimension(d) => Some(d.alias_suffix()),
            _ => None,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.get_dependencies().is_empty()
    }

    // To back compatibility with code that use BaseMembers
    pub fn as_base_member(
        self: Rc<Self>,
        query_tools: Rc<QueryTools>,
    ) -> Result<Rc<dyn BaseMember>, CubeError> {
        MemberSymbolRef::try_new(self, query_tools).map(|r| r.as_base_member())
    }
}
