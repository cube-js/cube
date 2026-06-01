use super::cube::Cube;
use super::dimension::Dimension;
use super::measure::Measure;
use super::path::CubeName;
use super::segment::Segment;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Root container for the compiled schema.
///
/// Mirrors the structural part of `CubeEvaluator.evaluatedCubes`. Built
/// once per schema version, immutable afterwards.
#[derive(Clone, Default)]
pub struct Model {
    pub cubes: HashMap<CubeName, Rc<Cube>>,
}

impl Model {
    pub fn cube(&self, name: &CubeName) -> Option<&Rc<Cube>> {
        self.cubes.get(name)
    }

    pub fn cube_by_str(&self, name: &str) -> Option<&Rc<Cube>> {
        self.cubes.get(&CubeName::new(name))
    }

    pub fn cubes_iter(&self) -> impl Iterator<Item = &Rc<Cube>> {
        self.cubes.values()
    }

    pub fn cube_names(&self) -> impl Iterator<Item = &CubeName> {
        self.cubes.keys()
    }

    pub fn all_measures(&self) -> impl Iterator<Item = &Rc<Measure>> {
        self.cubes.values().flat_map(|c| c.measures.values())
    }

    pub fn all_dimensions(&self) -> impl Iterator<Item = &Rc<Dimension>> {
        self.cubes.values().flat_map(|c| c.dimensions.values())
    }

    pub fn all_segments(&self) -> impl Iterator<Item = &Rc<Segment>> {
        self.cubes.values().flat_map(|c| c.segments.values())
    }
}

/// Skeleton builder. Real population from `CubeEvaluator` will be
/// fleshed out iteratively — for now it just produces an empty `Model`
/// or accepts pre-built cubes via `add_cube`.
pub struct ModelBuilder {
    cubes: HashMap<CubeName, Rc<Cube>>,
}

impl Default for ModelBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelBuilder {
    pub fn new() -> Self {
        Self {
            cubes: HashMap::new(),
        }
    }

    pub fn add_cube(&mut self, cube: Rc<Cube>) {
        let name = cube.name.clone();
        self.cubes.insert(name, cube);
    }

    pub fn build(self) -> Result<Model, CubeError> {
        Ok(Model { cubes: self.cubes })
    }
}
