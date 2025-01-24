use super::planners::QueryPlanner;
use super::query_tools::QueryTools;
use super::QueryProperties;
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::object::NativeArray;
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::NativeType;
use cubenativeutils::wrappers::{NativeContextHolder, NativeObjectHandle, NativeStruct};
use cubenativeutils::{CubeError, CubeErrorCauseType};
use std::rc::Rc;

pub struct BaseQuery<IT: InnerTypes> {
    context: NativeContextHolder<IT>,
    query_tools: Rc<QueryTools>,
    request: Rc<QueryProperties>,
}

impl<IT: InnerTypes> BaseQuery<IT> {
    pub fn try_new(
        context: NativeContextHolder<IT>,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<Self, CubeError> {
        let query_tools = QueryTools::try_new(
            options.cube_evaluator()?,
            options.base_tools()?,
            options.join_graph()?,
            options.static_data().timezone.clone(),
        )?;

        let request = QueryProperties::try_new(query_tools.clone(), options)?;

        Ok(Self {
            context,
            query_tools,
            request,
        })
    }

    pub fn build_sql_and_params(&self) -> NativeObjectHandle<IT> {
        let build_result = self.build_sql_and_params_impl();
        let result = self.context.empty_struct().unwrap();
        match build_result {
            Ok(res) => {
                result.set_field("result", res).unwrap();
            }
            Err(e) => {
                let error_descr = self.context.empty_struct().unwrap();
                let error_cause = match &e.cause {
                    CubeErrorCauseType::User(_) => "User",
                    CubeErrorCauseType::Internal(_) => "Internal",
                };
                error_descr
                    .set_field(
                        "message",
                        e.message.to_native(self.context.clone()).unwrap(),
                    )
                    .unwrap();
                error_descr
                    .set_field(
                        "cause",
                        error_cause.to_native(self.context.clone()).unwrap(),
                    )
                    .unwrap();
                result
                    .set_field("error", NativeObjectHandle::new(error_descr.into_object()))
                    .unwrap();
            }
        }

        NativeObjectHandle::new(result.into_object())
    }

    fn build_sql_and_params_impl(&self) -> Result<NativeObjectHandle<IT>, CubeError> {
        let templates = PlanSqlTemplates::new(self.query_tools.templates_render());
        let query_planner = QueryPlanner::new(self.request.clone(), self.query_tools.clone());
        let plan = query_planner.plan()?;

        let sql = plan.to_sql(&templates)?;
        let (result_sql, params) = self.query_tools.build_sql_and_params(&sql, true)?;

        let res = self.context.empty_array()?;
        res.set(0, result_sql.to_native(self.context.clone())?)?;
        res.set(1, params.to_native(self.context.clone())?)?;
        let result = NativeObjectHandle::new(res.into_object());

        Ok(result)
    }
}
