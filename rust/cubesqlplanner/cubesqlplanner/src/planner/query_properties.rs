use super::filter::compiler::FilterCompiler;
use super::query_tools::QueryTools;
use super::{BaseDimension, BaseMeasure, BaseMember, BaseTimeDimension};
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::plan::{Expr, Filter, FilterItem};
use crate::planner::sql_evaluator::collectors::{
    collect_multiplied_measures, has_multi_stage_members,
};
use crate::planner::sql_evaluator::EvaluationNode;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashSet;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct OrderByItem {
    name: String,
    desc: bool,
}

impl OrderByItem {
    pub fn new(name: String, desc: bool) -> Self {
        Self { name, desc }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn desc(&self) -> bool {
        self.desc
    }
}

enum SymbolAggregateType {
    Regular,
    Multiplied,
    MultiStage,
}

#[derive(Default, Clone)]
pub struct FullKeyAggregateMeasures {
    pub multiplied_measures: Vec<Rc<BaseMeasure>>,
    pub regular_measures: Vec<Rc<BaseMeasure>>,
    pub multi_stage_measures: Vec<Rc<BaseMeasure>>,
}

impl FullKeyAggregateMeasures {
    pub fn has_multiplied_measures(&self) -> bool {
        !self.multiplied_measures.is_empty()
    }

    pub fn has_multi_stage_measures(&self) -> bool {
        !self.multi_stage_measures.is_empty()
    }
}

#[derive(Clone)]
pub struct QueryProperties {
    measures: Vec<Rc<BaseMeasure>>,
    dimensions: Vec<Rc<BaseDimension>>,
    dimensions_filters: Vec<FilterItem>,
    time_dimensions_filters: Vec<FilterItem>,
    measures_filters: Vec<FilterItem>,
    time_dimensions: Vec<Rc<BaseTimeDimension>>,
    order_by: Vec<OrderByItem>,
    row_limit: Option<usize>,
    offset: Option<usize>,
    query_tools: Rc<QueryTools>,
}

impl QueryProperties {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<Rc<Self>, CubeError> {
        let evaluator_compiler_cell = query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

        let dimensions = if let Some(dimensions) = &options.static_data().dimensions {
            dimensions
                .iter()
                .map(|d| {
                    let evaluator = evaluator_compiler.add_dimension_evaluator(d.clone())?;
                    BaseDimension::try_new(d.clone(), query_tools.clone(), evaluator)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        let time_dimensions = if let Some(time_dimensions) = &options.static_data().time_dimensions
        {
            time_dimensions
                .iter()
                .map(|d| {
                    let evaluator =
                        evaluator_compiler.add_dimension_evaluator(d.dimension.clone())?;
                    BaseTimeDimension::try_new(
                        d.dimension.clone(),
                        query_tools.clone(),
                        evaluator,
                        d.granularity.clone(),
                        d.date_range.clone(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        let measures = if let Some(measures) = &options.static_data().measures {
            measures
                .iter()
                .map(|m| {
                    let evaluator = evaluator_compiler.add_measure_evaluator(m.clone())?;
                    BaseMeasure::try_new(m.clone(), query_tools.clone(), evaluator)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        let mut filter_compiler = FilterCompiler::new(&mut evaluator_compiler, query_tools.clone());
        if let Some(filters) = &options.static_data().filters {
            for filter in filters {
                filter_compiler.add_item(filter)?;
            }
        }
        for time_dimension in &time_dimensions {
            filter_compiler.add_time_dimension_item(time_dimension)?;
        }
        let (dimensions_filters, time_dimensions_filters, measures_filters) =
            filter_compiler.extract_result();

        let all_join_hints = evaluator_compiler.join_hints()?;
        let join = query_tools.join_graph().build_join(all_join_hints)?;
        query_tools.cached_data_mut().set_join(join);
        //FIXME may be this filter should be applied on other place
        let time_dimensions = time_dimensions
            .into_iter()
            .filter(|dim| dim.has_granularity())
            .collect_vec();

        let order_by = if let Some(order) = &options.static_data().order {
            order
                .iter()
                .map(|o| OrderByItem::new(o.id.clone(), o.is_desc()))
                .collect_vec()
        } else {
            Self::default_order(&dimensions, &time_dimensions, &measures)
        };

        let row_limit = if let Some(row_limit) = &options.static_data().row_limit {
            row_limit.parse::<usize>().ok()
        } else {
            None
        };
        let offset = if let Some(offset) = &options.static_data().offset {
            offset.parse::<usize>().ok()
        } else {
            None
        };

        Ok(Rc::new(Self {
            measures,
            dimensions,
            time_dimensions,
            time_dimensions_filters,
            dimensions_filters,
            measures_filters,
            order_by,
            row_limit,
            offset,
            query_tools,
        }))
    }

    pub fn try_new_from_precompiled(
        query_tools: Rc<QueryTools>,
        measures: Vec<Rc<BaseMeasure>>,
        dimensions: Vec<Rc<BaseDimension>>,
        time_dimensions: Vec<Rc<BaseTimeDimension>>,
        time_dimensions_filters: Vec<FilterItem>,
        dimensions_filters: Vec<FilterItem>,
        measures_filters: Vec<FilterItem>,
        order_by: Vec<OrderByItem>,
        row_limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Rc<Self>, CubeError> {
        let order_by = if order_by.is_empty() {
            Self::default_order(&dimensions, &time_dimensions, &measures)
        } else {
            order_by
        };

        Ok(Rc::new(Self {
            measures,
            dimensions,
            time_dimensions,
            time_dimensions_filters,
            dimensions_filters,
            measures_filters,
            order_by,
            row_limit,
            offset,
            query_tools,
        }))
    }

    pub fn measures(&self) -> &Vec<Rc<BaseMeasure>> {
        &self.measures
    }

    pub fn dimensions(&self) -> &Vec<Rc<BaseDimension>> {
        &self.dimensions
    }

    pub fn time_dimensions(&self) -> &Vec<Rc<BaseTimeDimension>> {
        &self.time_dimensions
    }

    pub fn time_dimensions_filters(&self) -> &Vec<FilterItem> {
        &self.time_dimensions_filters
    }

    pub fn dimensions_filters(&self) -> &Vec<FilterItem> {
        &self.dimensions_filters
    }

    pub fn measures_filters(&self) -> &Vec<FilterItem> {
        &self.measures_filters
    }

    pub fn row_limit(&self) -> Option<usize> {
        self.row_limit
    }

    pub fn offset(&self) -> Option<usize> {
        self.offset
    }

    pub fn order_by(&self) -> &Vec<OrderByItem> {
        &self.order_by
    }

    pub fn set_order_by_to_default(&mut self) {
        self.order_by =
            Self::default_order(&self.dimensions, &self.time_dimensions, &self.measures);
    }

    pub fn all_filters(&self) -> Option<Filter> {
        let items = self
            .time_dimensions_filters
            .iter()
            .chain(self.dimensions_filters.iter())
            .cloned()
            .collect_vec();
        if items.is_empty() {
            None
        } else {
            Some(Filter { items })
        }
    }

    pub fn select_all_dimensions_and_measures(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Vec<Expr>, CubeError> {
        let measures = measures.iter().map(|m| Expr::Field(m.clone()));
        let time_dimensions = self.time_dimensions.iter().map(|d| Expr::Field(d.clone()));
        let dimensions = self.dimensions.iter().map(|d| Expr::Field(d.clone()));
        Ok(dimensions.chain(time_dimensions).chain(measures).collect())
    }

    pub fn dimensions_references_and_measures(
        &self,
        cube_name: &str,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Vec<Expr>, CubeError> {
        let dimensions_refs = self
            .dimensions_for_select()
            .into_iter()
            .map(|d| Ok(Expr::Reference(Some(cube_name.to_string()), d.alias_name())));
        let measures = measures.iter().map(|m| Ok(Expr::Field(m.clone())));
        dimensions_refs
            .chain(measures)
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn dimensions_for_select(&self) -> Vec<Rc<dyn BaseMember>> {
        let time_dimensions = self
            .time_dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        let dimensions = self
            .dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        dimensions.chain(time_dimensions).collect()
    }

    pub fn dimensions_for_select_append(
        &self,
        append: &Vec<Rc<BaseDimension>>,
    ) -> Vec<Rc<dyn BaseMember>> {
        let time_dimensions = self
            .time_dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        let append_dims = append.iter().map(|d| -> Rc<dyn BaseMember> { d.clone() });
        let dimensions = self
            .dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        dimensions
            .chain(time_dimensions)
            .chain(append_dims)
            .collect()
    }

    pub fn columns_to_expr(&self, columns: &Vec<Rc<dyn BaseMember>>) -> Vec<Expr> {
        columns.iter().map(|d| Expr::Field(d.clone())).collect_vec()
    }

    pub fn all_members(&self, exclude_time_dimensions: bool) -> Vec<Rc<dyn BaseMember>> {
        let dimensions = self
            .dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        let measures = self
            .measures
            .iter()
            .map(|m| -> Rc<dyn BaseMember> { m.clone() });
        if exclude_time_dimensions {
            dimensions.chain(measures).collect_vec()
        } else {
            let time_dimensions = self
                .time_dimensions
                .iter()
                .map(|d| -> Rc<dyn BaseMember> { d.clone() });
            dimensions
                .chain(time_dimensions)
                .chain(measures)
                .collect_vec()
        }
    }

    pub fn group_by(&self) -> Vec<Expr> {
        self.dimensions
            .iter()
            .map(|f| Expr::Field(f.clone()))
            .chain(self.time_dimensions.iter().map(|f| Expr::Field(f.clone())))
            .collect()
    }

    pub fn default_order(
        dimensions: &Vec<Rc<BaseDimension>>,
        time_dimensions: &Vec<Rc<BaseTimeDimension>>,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Vec<OrderByItem> {
        let mut result = Vec::new();
        if let Some(granularity_dim) = time_dimensions.iter().find(|d| d.has_granularity()) {
            result.push(OrderByItem::new(granularity_dim.full_name(), false));
        } else if !measures.is_empty() && !dimensions.is_empty() {
            result.push(OrderByItem::new(measures[0].full_name(), true));
        } else if !dimensions.is_empty() {
            result.push(OrderByItem::new(dimensions[0].full_name(), false));
        }
        result
    }

    pub fn all_filtered_members(&self) -> HashSet<String> {
        let mut result = HashSet::new();
        for item in self.dimensions_filters().iter() {
            self.fill_members_from_filter_item(item, &mut result);
        }
        for item in self.time_dimensions_filters().iter() {
            self.fill_members_from_filter_item(item, &mut result);
        }
        for item in self.measures_filters().iter() {
            self.fill_members_from_filter_item(item, &mut result);
        }
        result
    }

    fn fill_members_from_filter_item(&self, item: &FilterItem, members: &mut HashSet<String>) {
        match item {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    self.fill_members_from_filter_item(item, members)
                }
            }
            FilterItem::Item(item) => {
                members.insert(item.member_name());
            }
        }
    }

    pub fn is_simple_query(&self) -> Result<bool, CubeError> {
        for member in self.all_members(false) {
            match self.get_symbol_aggregate_type(&member.member_evaluator())? {
                SymbolAggregateType::Regular => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    pub fn full_key_aggregate_measures(&self) -> Result<FullKeyAggregateMeasures, CubeError> {
        let mut result = FullKeyAggregateMeasures::default();
        let measures = self.measures();
        for m in measures.iter() {
            match self.get_symbol_aggregate_type(m.member_evaluator())? {
                SymbolAggregateType::Regular => result.regular_measures.push(m.clone()),
                SymbolAggregateType::Multiplied => result.multiplied_measures.push(m.clone()),
                SymbolAggregateType::MultiStage => result.multi_stage_measures.push(m.clone()),
            }
        }

        Ok(result)
    }

    fn get_symbol_aggregate_type(
        &self,
        symbol: &Rc<EvaluationNode>,
    ) -> Result<SymbolAggregateType, CubeError> {
        let symbol_type = if has_multi_stage_members(symbol)? {
            SymbolAggregateType::MultiStage
        } else if let Some(multiple) =
            collect_multiplied_measures(self.query_tools.clone(), symbol)?
        {
            if multiple.multiplied {
                SymbolAggregateType::Multiplied
            } else {
                SymbolAggregateType::Regular
            }
        } else {
            SymbolAggregateType::Regular
        };
        Ok(symbol_type)
    }
}
