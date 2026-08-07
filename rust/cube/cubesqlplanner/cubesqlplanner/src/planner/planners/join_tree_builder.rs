use super::CommonUtils;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::planner::state::State;
use crate::planner::{JoinTree, JoinTreeItem};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Resolves a `JoinDefinition` into a `JoinTree`: looks up each cube
/// and compiles its ON SQL once, so downstream planning can reuse the
/// compiled conditions instead of recompiling them on every use.
pub struct JoinTreeBuilder {
    utils: CommonUtils,
}

impl JoinTreeBuilder {
    pub fn new(query_tools: Rc<State>) -> Self {
        Self {
            utils: CommonUtils::new(query_tools),
        }
    }

    pub fn build(&self, join: Rc<dyn JoinDefinition>) -> Result<Rc<JoinTree>, CubeError> {
        let root = self.utils.cube_from_path(join.static_data().root.clone())?;
        let mut joins = vec![];
        for join_definition in join.joins()?.iter() {
            let static_data = join_definition.static_data();
            let cube = self.utils.cube_from_path(static_data.original_to.clone())?;
            let on_sql = self.utils.compile_join_condition(join_definition.clone())?;
            let relationship = join_definition.join()?.static_data().relationship.clone();
            joins.push(JoinTreeItem::new(
                cube,
                static_data.original_from.clone(),
                on_sql,
                relationship_splits_rows(&relationship),
            ));
        }
        Ok(JoinTree::new(
            root,
            joins,
            join.static_data().multiplication_factor.clone(),
        ))
    }
}

/// Whether joining the `to` side of an edge with this relationship splits one row
/// of the `from` side into several. Only many-to-one and one-to-one keep the row
/// count, and a relationship arrives either normalized or in one of its model
/// spellings, so every spelling of those two is listed. Anything unrecognized
/// counts as splitting: requiring a primary key that is not needed only refuses a
/// usable pre-aggregation, while omitting a needed one serves collapsed rows.
fn relationship_splits_rows(relationship: &str) -> bool {
    !matches!(
        relationship,
        "belongsTo"
            | "belongs_to"
            | "many_to_one"
            | "manyToOne"
            | "hasOne"
            | "has_one"
            | "one_to_one"
            | "oneToOne"
    )
}

#[cfg(test)]
mod tests {
    use super::relationship_splits_rows;

    #[test]
    fn splits_rows_covers_every_relationship_spelling() {
        for keeps_row_count in ["belongsTo", "many_to_one", "hasOne", "one_to_one"] {
            assert!(
                !relationship_splits_rows(keeps_row_count),
                "`{keeps_row_count}` joins at most one row"
            );
        }
        for splits in ["hasMany", "one_to_many"] {
            assert!(
                relationship_splits_rows(splits),
                "`{splits}` joins many rows"
            );
        }
        assert!(
            relationship_splits_rows("something_else"),
            "an unrecognized relationship has to be treated as splitting"
        );
    }
}
