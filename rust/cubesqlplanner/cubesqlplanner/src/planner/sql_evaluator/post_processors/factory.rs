use super::{FinalMeasureNodeProcessor, MeasureFilterNodeProcessor, RootNodeProcessor};
use crate::planner::sql_evaluator::default_visitor::PostProcesNodeProcessorItem;
use std::rc::Rc;

pub fn default_post_processor() -> Rc<dyn PostProcesNodeProcessorItem> {
    let final_measure_processor = FinalMeasureNodeProcessor::new();
    let measure_filter_processor = MeasureFilterNodeProcessor::new(Some(final_measure_processor));
    RootNodeProcessor::new(None, Some(measure_filter_processor))
}
