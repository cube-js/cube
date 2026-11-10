use super::SqlNode;
use crate::physical_plan::{Schema, SqlEvaluatorVisitor};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{InputId, MemberSymbol, RefTarget};
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

/// A source of a select that answers references to a plan input.
#[derive(Clone)]
pub struct InputSource {
    pub alias: String,
    pub schema: Rc<Schema>,
}

/// The sources of a select answering references to plan inputs.
#[derive(Clone, Default)]
pub struct InputSources {
    /// Per input, the sources answering its members, tried in order.
    pub inputs: HashMap<InputId, Vec<InputSource>>,
    /// Per input, the source holding the keys merged from it.
    pub keys: HashMap<InputId, InputSource>,
}

impl InputSources {
    /// The source holding the member `origin` read from `input`, and its column.
    pub fn input_column(
        &self,
        input: InputId,
        origin: &Rc<MemberSymbol>,
    ) -> Option<(&InputSource, String)> {
        self.inputs
            .get(&input)?
            .iter()
            .find_map(|source| Some((source, source.schema.resolve_member_reference(origin)?)))
    }

    /// The column `member` has in its source, when it reads a plan input
    /// directly.
    pub fn column_name(&self, member: &Rc<MemberSymbol>) -> Option<String> {
        let MemberSymbol::Ref(reference) = member.as_ref() else {
            return None;
        };
        match reference.target() {
            RefTarget::Input { input, origin } => {
                self.input_column(*input, origin).map(|(_, column)| column)
            }
            _ => None,
        }
    }
}

/// Renders a reference to plan inputs as the column of the source of the select
/// answering it. Other terminal references go to `input`.
pub struct InputReferenceSqlNode {
    input: Rc<dyn SqlNode>,
    sources: Rc<InputSources>,
}

impl InputReferenceSqlNode {
    pub fn new(input: Rc<dyn SqlNode>, sources: Rc<InputSources>) -> Rc<Self> {
        Rc::new(Self { input, sources })
    }

    fn render_input(
        &self,
        input: InputId,
        origin: &Rc<MemberSymbol>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let (source, column) = self.sources.input_column(input, origin).ok_or_else(|| {
            CubeError::internal(format!(
                "No source of the select holds {} read from input {}",
                origin.full_name(),
                input
            ))
        })?;
        templates.column_reference(&Some(source.alias.clone()), &column)
    }

    // The inputs of a key are merged by one source holding their keys.
    fn render_key(
        &self,
        inputs: &[InputId],
        origin: &Rc<MemberSymbol>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let mut sources = inputs.iter().map(|input| self.sources.keys.get(input));
        let source = match sources.next() {
            Some(Some(source)) => source,
            _ => {
                return Err(CubeError::internal(format!(
                    "No source of the select holds the key {} merged from {:?}",
                    origin.full_name(),
                    inputs
                )))
            }
        };
        if sources.any(|other| other.map(|o| &o.alias) != Some(&source.alias)) {
            return Err(CubeError::internal(format!(
                "The key {} is merged by different sources",
                origin.full_name()
            )));
        }
        let column = source
            .schema
            .resolve_member_reference(origin)
            .ok_or_else(|| {
                CubeError::internal(format!(
                    "Source {} holds no key {}",
                    source.alias,
                    origin.full_name()
                ))
            })?;
        templates.column_reference(&Some(source.alias.clone()), &column)
    }
}

impl SqlNode for InputReferenceSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let MemberSymbol::Ref(reference) = node.as_ref() else {
            return self
                .input
                .to_sql(visitor, node, query_tools, node_processor, templates);
        };
        match reference.target() {
            RefTarget::Input { input, origin } => self.render_input(*input, origin, templates),
            RefTarget::Key { inputs, origin } => self.render_key(inputs, origin, templates),
            _ => self
                .input
                .to_sql(visitor, node, query_tools, node_processor, templates),
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
