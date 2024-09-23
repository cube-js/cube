//pub mod aggregation;
pub mod builder;
pub mod expression;
pub mod filter;
pub mod from;
pub mod join;
pub mod order;
pub mod select;

use cubenativeutils::CubeError;
pub use expression::Expr;
pub use filter::{Filter, FilterItem};
pub use from::{From, FromSource};
pub use join::{Join, JoinItem};
pub use order::OrderBy;
pub use select::Select;

pub enum GenerationPlan {
    Select(Select),
}

impl GenerationPlan {
    pub fn to_sql(&self) -> Result<String, CubeError> {
        match self {
            GenerationPlan::Select(select) => select.to_sql(),
        }
    }
}
