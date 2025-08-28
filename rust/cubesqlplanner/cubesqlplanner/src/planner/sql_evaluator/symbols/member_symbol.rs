use cubenativeutils::CubeError;

use super::{
    CubeNameSymbol, CubeTableSymbol, DimensionSymbol, MeasureSymbol, MemberExpressionSymbol,
    TimeDimensionSymbol,
};
use std::fmt::Debug;
use std::rc::Rc;

pub enum MemberSymbol {
    Dimension(Rc<DimensionSymbol>),
    TimeDimension(Rc<TimeDimensionSymbol>),
    Measure(Rc<MeasureSymbol>),
    CubeName(Rc<CubeNameSymbol>),
    CubeTable(Rc<CubeTableSymbol>),
    MemberExpression(Rc<MemberExpressionSymbol>),
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

impl PartialEq for MemberSymbol {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Dimension(l0), Self::Dimension(r0)) => l0.full_name() == r0.full_name(),
            (Self::TimeDimension(l0), Self::TimeDimension(r0)) => l0.full_name() == r0.full_name(),
            (Self::Measure(l0), Self::Measure(r0)) => l0.full_name() == r0.full_name(),
            (Self::CubeName(l0), Self::CubeName(r0)) => l0.cube_name() == r0.cube_name(),
            (Self::CubeTable(l0), Self::CubeTable(r0)) => l0.cube_name() == r0.cube_name(),
            (Self::MemberExpression(l0), Self::MemberExpression(r0)) => {
                l0.full_name() == r0.full_name()
            }
            _ => false,
        }
    }
}

impl MemberSymbol {
    pub fn new_measure(symbol: Rc<MeasureSymbol>) -> Rc<Self> {
        Rc::new(Self::Measure(symbol))
    }

    pub fn new_dimension(symbol: Rc<DimensionSymbol>) -> Rc<Self> {
        Rc::new(Self::Dimension(symbol))
    }

    pub fn new_cube_name(symbol: Rc<CubeNameSymbol>) -> Rc<Self> {
        Rc::new(Self::CubeName(symbol))
    }

    pub fn new_cube_table(symbol: Rc<CubeTableSymbol>) -> Rc<Self> {
        Rc::new(Self::CubeTable(symbol))
    }

    pub fn new_member_expression(symbol: Rc<MemberExpressionSymbol>) -> Rc<Self> {
        Rc::new(Self::MemberExpression(symbol))
    }

    pub fn new_time_dimension(symbol: Rc<TimeDimensionSymbol>) -> Rc<Self> {
        Rc::new(Self::TimeDimension(symbol))
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

    pub fn alias(&self) -> String {
        match self {
            Self::Dimension(d) => d.alias(),
            Self::TimeDimension(d) => d.alias(),
            Self::Measure(m) => m.alias(),
            Self::CubeName(c) => c.alias(),
            Self::CubeTable(c) => c.alias(),
            Self::MemberExpression(e) => e.alias(),
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

    pub fn reference_member(&self) -> Option<Rc<MemberSymbol>> {
        match self {
            Self::Dimension(d) => d.reference_member(),
            Self::TimeDimension(d) => d.reference_member(),
            Self::Measure(m) => m.reference_member(),
            Self::CubeName(_) => None,
            Self::CubeTable(_) => None,
            Self::MemberExpression(e) => e.reference_member(),
        }
    }

    pub fn resolve_reference_chain(self: Rc<Self>) -> Rc<MemberSymbol> {
        let mut current = self;
        while let Some(reference) = current.reference_member() {
            current = reference;
        }
        current
    }

    pub fn has_member_in_reference_chain(&self, member: &Rc<MemberSymbol>) -> bool {
        if self.full_name() == member.full_name() {
            return true;
        }

        let mut current = self.reference_member();
        while let Some(reference) = current {
            if reference.full_name() == member.full_name() {
                return true;
            }
            current = reference.reference_member();
        }
        false
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

    pub fn as_time_dimension(&self) -> Result<Rc<TimeDimensionSymbol>, CubeError> {
        match self {
            Self::TimeDimension(d) => Ok(d.clone()),
            _ => Err(CubeError::internal(format!(
                "{} is not a time dimension",
                self.full_name()
            ))),
        }
    }

    pub fn as_dimension(&self) -> Result<Rc<DimensionSymbol>, CubeError> {
        match self {
            Self::Dimension(d) => Ok(d.clone()),
            _ => Err(CubeError::internal(format!(
                "{} is not a dimension",
                self.full_name()
            ))),
        }
    }

    pub fn as_measure(&self) -> Result<Rc<MeasureSymbol>, CubeError> {
        match self {
            Self::Measure(m) => Ok(m.clone()),
            _ => Err(CubeError::internal(format!(
                "{} is not a measure",
                self.full_name()
            ))),
        }
    }

    pub fn as_member_expression(&self) -> Result<Rc<MemberExpressionSymbol>, CubeError> {
        match self {
            Self::MemberExpression(m) => Ok(m.clone()),
            _ => Err(CubeError::internal(format!(
                "{} is not a member expression",
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
}
