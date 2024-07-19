use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::MeasureDefinition;
use crate::planner::utils::escape_column_name;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseMeasure {
    measure: String,
    cube_evaluator: Rc<dyn CubeEvaluator>,
}

impl BaseMeasure {
    pub fn new(measure: String, cube_evaluator: Rc<dyn CubeEvaluator>) -> Rc<Self> {
        Rc::new(Self {
            measure,
            cube_evaluator,
        })
    }

    pub fn to_sql(&self) -> Result<String, CubeError> {
        self.sql()
    }

    fn sql(&self) -> Result<String, CubeError> {
        /* let primary_keys = self.cube_evaluator.static_data().primary_keys.get()
        self.measure.clone() */
        let path = self.path()?;
        let cube_name = &path[0];
        let name = &path[1];
        let primary_keys = self
            .cube_evaluator
            .static_data()
            .primary_keys
            .get(cube_name)
            .unwrap();
        let primary_key = primary_keys.first().unwrap();
        let pk_sql = self.primary_key_sql(primary_key, cube_name)?;

        let measure_definition = self.cube_evaluator.measure_by_path(self.measure.clone())?;

        let measure_type = &measure_definition.static_data().measure_type;
        let alias_name = escape_column_name(&self.alias_name()?);

        Ok(format!("{}({}) {}", measure_type, pk_sql, alias_name))
    }

    fn path(&self) -> Result<Vec<String>, CubeError> {
        self.cube_evaluator
            .parse_path("measures".to_string(), self.measure.clone())
    }

    //FIXME should be moved out from here
    fn primary_key_sql(&self, key_name: &String, cube_name: &String) -> Result<String, CubeError> {
        Ok(format!("{}.{}", escape_column_name(&cube_name), key_name))
    }

    fn alias_name(&self) -> Result<String, CubeError> {
        Ok(self.measure.replace(".", "__"))
    }
}
