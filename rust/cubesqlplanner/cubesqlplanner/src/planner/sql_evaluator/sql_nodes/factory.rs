use super::{
    AutoPrefixSqlNode, CaseDimensionSqlNode, EvaluateSqlNode, FinalMeasureSqlNode,
    GeoDimensionSqlNode, MeasureFilterSqlNode, MultiStageRankNode, MultiStageWindowNode,
    RenderReferencesSqlNode, RollingWindowNode, RootSqlNode, SqlNode, TimeShiftSqlNode,
    UngroupedMeasureSqlNode, UngroupedQueryFinalMeasureSqlNode,
};
use crate::plan::schema::QualifiedColumnName;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

#[derive(Clone)]
pub struct SqlNodesFactory {
    time_shifts: HashMap<String, String>,
    ungrouped: bool,
    ungrouped_measure: bool,
    count_approx_as_state: bool,
    render_references: HashMap<String, QualifiedColumnName>,
    rendered_as_multiplied_measures: HashSet<String>,
    ungrouped_measure_references: HashMap<String, QualifiedColumnName>,
    cube_name_references: HashMap<String, String>,
    multi_stage_rank: Option<Vec<String>>,   //partition_by
    multi_stage_window: Option<Vec<String>>, //partition_by
    rolling_window: bool,
}

impl SqlNodesFactory {
    pub fn new() -> Self {
        Self {
            time_shifts: HashMap::new(),
            ungrouped: false,
            ungrouped_measure: false,
            count_approx_as_state: false,
            render_references: HashMap::new(),
            ungrouped_measure_references: HashMap::new(),
            cube_name_references: HashMap::new(),
            rendered_as_multiplied_measures: HashSet::new(),
            multi_stage_rank: None,
            multi_stage_window: None,
            rolling_window: false,
        }
    }

    pub fn set_time_shifts(&mut self, time_shifts: HashMap<String, String>) {
        self.time_shifts = time_shifts;
    }

    pub fn set_ungrouped(&mut self, value: bool) {
        self.ungrouped = value;
    }

    pub fn set_ungrouped_measure(&mut self, value: bool) {
        self.ungrouped_measure = value;
    }

    pub fn set_render_references(&mut self, value: HashMap<String, QualifiedColumnName>) {
        self.render_references = value;
    }

    pub fn render_references(&self) -> &HashMap<String, QualifiedColumnName> {
        &self.render_references
    }

    pub fn set_rendered_as_multiplied_measures(&mut self, value: HashSet<String>) {
        self.rendered_as_multiplied_measures = value;
    }

    pub fn add_render_reference(&mut self, key: String, value: QualifiedColumnName) {
        self.render_references.insert(key, value);
    }

    pub fn set_multi_stage_rank(&mut self, partition_by: Vec<String>) {
        self.multi_stage_rank = Some(partition_by);
    }

    pub fn set_multi_stage_window(&mut self, partition_by: Vec<String>) {
        self.multi_stage_window = Some(partition_by);
    }

    pub fn set_rolling_window(&mut self, value: bool) {
        self.rolling_window = value;
    }

    pub fn set_count_approx_as_state(&mut self, value: bool) {
        self.count_approx_as_state = value;
    }

    pub fn set_ungrouped_measure_references(
        &mut self,
        value: HashMap<String, QualifiedColumnName>,
    ) {
        self.ungrouped_measure_references = value;
    }

    pub fn add_ungrouped_measure_reference(&mut self, key: String, value: QualifiedColumnName) {
        self.ungrouped_measure_references.insert(key, value);
    }

    pub fn set_cube_name_references(&mut self, value: HashMap<String, String>) {
        self.cube_name_references = value;
    }

    pub fn add_cube_name_reference(&mut self, key: String, value: String) {
        self.cube_name_references.insert(key, value);
    }

    pub fn default_node_processor(&self) -> Rc<dyn SqlNode> {
        let evaluate_sql_processor = EvaluateSqlNode::new();
        let auto_prefix_processor = AutoPrefixSqlNode::new(
            evaluate_sql_processor.clone(),
            self.cube_name_references.clone(),
        );

        let measure_filter_processor = MeasureFilterSqlNode::new(auto_prefix_processor.clone());

        let measure_processor =
            self.add_ungrouped_measure_reference_if_needed(measure_filter_processor.clone());
        let measure_processor = self.final_measure_node_processor(measure_processor);
        let measure_processor = self
            .add_multi_stage_window_if_needed(measure_processor, measure_filter_processor.clone());
        let measure_processor = self.add_multi_stage_rank_if_needed(measure_processor);

        let root_node = RootSqlNode::new(
            self.dimension_processor(evaluate_sql_processor.clone()),
            measure_processor.clone(),
            auto_prefix_processor.clone(),
            evaluate_sql_processor.clone(),
        );
        RenderReferencesSqlNode::new(root_node, self.render_references.clone())
    }

    fn add_ungrouped_measure_reference_if_needed(
        &self,
        default: Rc<dyn SqlNode>,
    ) -> Rc<dyn SqlNode> {
        if !self.ungrouped_measure_references.is_empty() {
            RenderReferencesSqlNode::new(default, self.ungrouped_measure_references.clone())
        } else {
            default
        }
    }

    fn add_multi_stage_rank_if_needed(&self, default: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        if let Some(partition_by) = &self.multi_stage_rank {
            MultiStageRankNode::new(default, partition_by.clone())
        } else {
            default
        }
    }

    fn add_multi_stage_window_if_needed(
        &self,
        default: Rc<dyn SqlNode>,
        multi_stage_input: Rc<dyn SqlNode>,
    ) -> Rc<dyn SqlNode> {
        if let Some(partition_by) = &self.multi_stage_window {
            MultiStageWindowNode::new(multi_stage_input, default, partition_by.clone())
        } else {
            default
        }
    }

    fn final_measure_node_processor(&self, input: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        if self.ungrouped_measure {
            UngroupedMeasureSqlNode::new(input)
        } else if self.ungrouped {
            UngroupedQueryFinalMeasureSqlNode::new(input)
        } else {
            let final_processor = FinalMeasureSqlNode::new(
                input.clone(),
                self.rendered_as_multiplied_measures.clone(),
                self.count_approx_as_state,
            );
            if self.rolling_window {
                RollingWindowNode::new(input, final_processor)
            } else {
                final_processor
            }
        }
    }

    fn dimension_processor(&self, input: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        let input: Rc<dyn SqlNode> = GeoDimensionSqlNode::new(input);
        let input: Rc<dyn SqlNode> = CaseDimensionSqlNode::new(input);

        let input: Rc<dyn SqlNode> =
            AutoPrefixSqlNode::new(input, self.cube_name_references.clone());

        let input = if !&self.time_shifts.is_empty() {
            TimeShiftSqlNode::new(self.time_shifts.clone(), input)
        } else {
            input
        };

        input
    }
}
