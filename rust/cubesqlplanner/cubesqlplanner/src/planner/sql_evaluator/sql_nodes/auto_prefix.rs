use super::SqlNode;
use crate::plan::Schema;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::Regex;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

pub struct AutoPrefixSqlNode {
    input: Rc<dyn SqlNode>,
    cube_references: HashMap<String, String>,
    schema: Rc<Schema>,
}

impl AutoPrefixSqlNode {
    pub fn new(input: Rc<dyn SqlNode>, cube_references: HashMap<String, String>) -> Rc<Self> {
        Rc::new(Self {
            input,
            cube_references,
            schema: Rc::new(Schema::empty()),
        })
    }

    pub fn new_with_schema(
        input: Rc<dyn SqlNode>,
        cube_references: HashMap<String, String>,
        schema: Rc<Schema>,
    ) -> Rc<Self> {
        Rc::new(Self {
            input,
            schema,
            cube_references,
        })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }

    pub fn schema(&self) -> &Rc<Schema> {
        &self.schema
    }

    pub fn cube_references(&self) -> &HashMap<String, String> {
        &self.cube_references
    }

    fn resolve_cube_alias(&self, name: &String) -> String {
        if let Some(alias) = self.cube_references.get(name) {
            alias.clone()
        } else {
            name.clone()
        }
    }

    pub fn auto_prefix_with_cube_name(
        &self,
        cube_name: &str,
        sql: &str,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        lazy_static! {
            static ref SINGLE_MEMBER_RE: Regex = Regex::new(r"^[_a-zA-Z][_a-zA-Z0-9]*$").unwrap();
        }
        let res = if SINGLE_MEMBER_RE.is_match(sql) {
            format!(
                "{}.{}",
                templates.quote_identifier(&PlanSqlTemplates::alias_name(&cube_name))?,
                sql
            )
        } else {
            sql.to_string()
        };
        Ok(res)
    }
}

impl SqlNode for AutoPrefixSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let input = self.input.to_sql(
            visitor,
            node,
            query_tools.clone(),
            node_processor.clone(),
            templates,
        )?;
        let res = match node.as_ref() {
            MemberSymbol::Dimension(ev) => {
                let cube_alias = self.resolve_cube_alias(&ev.cube_name());
                self.auto_prefix_with_cube_name(&cube_alias, &input, templates)?
            }
            MemberSymbol::Measure(ev) => {
                let cube_alias = self.resolve_cube_alias(&ev.cube_name());
                self.auto_prefix_with_cube_name(&cube_alias, &input, templates)?
            }
            MemberSymbol::CubeName(_) => {
                let cube_alias = self.resolve_cube_alias(&input);
                templates.quote_identifier(&cube_alias)?
            }
            _ => input,
        };
        Ok(res)
    }
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
