use super::{CubeNameSymbol, CubeTableSymbol, DimensionSymbol, MeasureSymbol};
use std::rc::Rc;

pub enum MemberSymbol {
    Dimension(DimensionSymbol),
    Measure(MeasureSymbol),
    CubeName(CubeNameSymbol),
    CubeTable(CubeTableSymbol),
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
            Self::Measure(m) => m.full_name(),
            Self::CubeName(c) => c.cube_name().clone(),
            Self::CubeTable(c) => c.cube_name().clone(),
        }
    }
    pub fn name(&self) -> String {
        match self {
            Self::Dimension(d) => d.name().clone(),
            Self::Measure(m) => m.name().clone(),
            Self::CubeName(c) => c.cube_name().clone(),
            Self::CubeTable(c) => c.cube_name().clone(),
        }
    }

    pub fn cube_name(&self) -> String {
        match self {
            Self::Dimension(d) => d.cube_name().clone(),
            Self::Measure(m) => m.cube_name().clone(),
            Self::CubeName(c) => c.cube_name().clone(),
            Self::CubeTable(c) => c.cube_name().clone(),
        }
    }
    pub fn is_measure(&self) -> bool {
        matches!(self, Self::Measure(_))
    }
    pub fn is_dimension(&self) -> bool {
        matches!(self, Self::Dimension(_))
    }
    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        match self {
            Self::Dimension(d) => d.get_dependencies(),
            Self::Measure(m) => m.get_dependencies(),
            Self::CubeName(_) => vec![],
            Self::CubeTable(_) => vec![],
        }
    }

    pub fn get_dependent_cubes(&self) -> Vec<String> {
        match self {
            Self::Dimension(d) => d.get_dependent_cubes(),
            Self::Measure(m) => m.get_dependent_cubes(),
            Self::CubeName(_) => vec![],
            Self::CubeTable(_) => vec![],
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.get_dependencies().is_empty()
    }
}
