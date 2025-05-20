use super::pretty_print::*;
use super::*;
use std::rc::Rc;

pub enum AggregateMultipliedSubquerySouce {
    Cube,
    MeasureSubquery(Rc<MeasureSubquery>),
}

pub struct AggregateMultipliedSubquery {
    pub schema: Rc<LogicalSchema>,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    pub keys_subquery: Rc<KeysSubQuery>,
    pub pk_cube: Rc<Cube>, //FIXME may be duplication with information in keys_subquery
    pub source: Rc<AggregateMultipliedSubquerySouce>,
}

impl PrettyPrint for AggregateMultipliedSubquery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("AggregateMultipliedSubquery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        if !self.dimension_subqueries.is_empty() {
            result.println("dimension_subqueries:", &state);
            for subquery in self.dimension_subqueries.iter() {
                subquery.pretty_print(result, &details_state);
            }
        }
        result.println("keys_subquery:", &state);
        self.keys_subquery.pretty_print(result, &details_state);
        result.println("source:", &state);
        match self.source.as_ref() {
            AggregateMultipliedSubquerySouce::Cube => {
                result.println("Cube:", &details_state);
                self.pk_cube
                    .pretty_print(result, &details_state.new_level());
            }
            AggregateMultipliedSubquerySouce::MeasureSubquery(measure_subquery) => {
                result.println(
                    &format!("MeasureSubquery: {}", measure_subquery.measures.len()),
                    &details_state,
                );
                measure_subquery.pretty_print(result, &details_state);
            }
        }
    }
}
