use super::access_policy::AccessPolicy;
use super::dimension::Dimension;
use super::expression::Expression;
use super::hierarchy::Hierarchy;
use super::join::Join;
use super::measure::Measure;
use super::path::CubeName;
use super::pre_aggregation::PreAggregation;
use super::segment::Segment;
use super::view::ViewSpec;
use std::collections::HashMap;
use std::rc::Rc;

/// Source for a cube's SQL: either a `sql_table` reference or a full `sql`
/// query. Mutually exclusive in the schema.
#[derive(Clone)]
pub enum SqlSource {
    Table(Expression),
    Query(Expression),
}

#[derive(Clone)]
pub struct Cube {
    pub name: CubeName,
    pub sql_alias: Option<String>,
    pub source: Option<SqlSource>,

    pub measures: HashMap<String, Rc<Measure>>,
    pub dimensions: HashMap<String, Rc<Dimension>>,
    pub segments: HashMap<String, Rc<Segment>>,
    pub joins: Vec<Join>,
    pub hierarchies: HashMap<String, Rc<Hierarchy>>,
    pub pre_aggregations: HashMap<String, Rc<PreAggregation>>,
    pub access_policies: Vec<AccessPolicy>,

    pub primary_keys: Vec<String>,

    pub is_view: bool,
    pub calendar: bool,

    /// Present iff `is_view`.
    pub view: Option<ViewSpec>,
}

impl Cube {
    pub fn measure(&self, name: &str) -> Option<&Rc<Measure>> {
        self.measures.get(name)
    }
    pub fn dimension(&self, name: &str) -> Option<&Rc<Dimension>> {
        self.dimensions.get(name)
    }
    pub fn segment(&self, name: &str) -> Option<&Rc<Segment>> {
        self.segments.get(name)
    }
    pub fn hierarchy(&self, name: &str) -> Option<&Rc<Hierarchy>> {
        self.hierarchies.get(name)
    }
    pub fn pre_aggregation(&self, name: &str) -> Option<&Rc<PreAggregation>> {
        self.pre_aggregations.get(name)
    }

    pub fn resolved_alias(&self) -> &str {
        self.sql_alias
            .as_deref()
            .unwrap_or_else(|| self.name.as_str())
    }

    pub fn measures_iter(&self) -> impl Iterator<Item = &Rc<Measure>> {
        self.measures.values()
    }
    pub fn dimensions_iter(&self) -> impl Iterator<Item = &Rc<Dimension>> {
        self.dimensions.values()
    }
    pub fn segments_iter(&self) -> impl Iterator<Item = &Rc<Segment>> {
        self.segments.values()
    }
}
