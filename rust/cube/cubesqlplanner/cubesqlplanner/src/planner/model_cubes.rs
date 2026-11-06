use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_item_definition::JoinItemDefinition;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::CubeId;
use cubenativeutils::CubeError;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// A join a data-model cube declares.
#[derive(Clone)]
pub struct CubeJoin {
    name: String,
    alias: Option<String>,
    definition: Rc<dyn JoinItemDefinition>,
}

impl CubeJoin {
    /// The joined cube.
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn alias(&self) -> &Option<String> {
        &self.alias
    }

    /// The name the declaring cube knows the join by.
    pub fn effective_name(&self) -> &String {
        self.alias.as_ref().unwrap_or(&self.name)
    }

    pub fn definition(&self) -> &Rc<dyn JoinItemDefinition> {
        &self.definition
    }
}

/// The data model as seen by one query: the cube evaluator, plus the joins
/// each cube declares, read once per cube.
pub struct ModelCubes {
    evaluator: Rc<dyn CubeEvaluator>,
    joins: RefCell<HashMap<String, Rc<Vec<CubeJoin>>>>,
}

impl ModelCubes {
    pub fn new(evaluator: Rc<dyn CubeEvaluator>) -> Rc<Self> {
        Rc::new(Self {
            evaluator,
            joins: RefCell::new(HashMap::new()),
        })
    }

    pub fn evaluator(&self) -> &Rc<dyn CubeEvaluator> {
        &self.evaluator
    }

    pub fn joins(&self, cube_name: &str) -> Result<Rc<Vec<CubeJoin>>, CubeError> {
        if let Some(joins) = self.joins.borrow().get(cube_name) {
            return Ok(joins.clone());
        }
        let definition = self.evaluator.cube_from_path(cube_name.to_string())?;
        let joins = Rc::new(
            definition
                .joins()?
                .unwrap_or_default()
                .into_iter()
                .map(|definition| {
                    let static_data = definition.static_data();
                    CubeJoin {
                        name: static_data.name.clone(),
                        alias: static_data.alias.clone(),
                        definition,
                    }
                })
                .collect::<Vec<_>>(),
        );
        self.joins
            .borrow_mut()
            .insert(cube_name.to_string(), joins.clone());
        Ok(joins)
    }

    /// The join `cube_name` knows as `name`, its alias or the joined cube name.
    pub fn find_join(&self, cube_name: &str, name: &str) -> Result<Option<CubeJoin>, CubeError> {
        Ok(self
            .joins(cube_name)?
            .iter()
            .find(|join| join.effective_name() == name)
            .cloned())
    }

    /// The name SQL aliases of a cube instance are built from: the cube's
    /// `sql_alias` or name, extended by the join names of an instance, so no
    /// two instances of one cube share it.
    pub fn alias_base(&self, cube_id: &CubeId) -> Result<String, CubeError> {
        match cube_id.parent() {
            None => {
                let definition = self
                    .evaluator
                    .cube_from_path(cube_id.target().to_string())?;
                Ok(definition.static_data().resolved_alias().clone())
            }
            Some(parent) => Ok(PlanSqlTemplates::alias_name(&format!(
                "{}__{}",
                PlanSqlTemplates::alias_name(&self.alias_base(parent)?),
                PlanSqlTemplates::alias_name(cube_id.segment())
            ))),
        }
    }
}
