//pub mod aggregation;
pub mod builder;
pub mod expression;
pub mod order;
//pub mod filter;
pub mod from;
pub mod select;

pub use expression::Expr;
pub use from::From;
pub use order::OrderBy;
pub use select::Select;

use std::fmt::{self, write};

pub enum GenerationPlan {
    Select(Select),
}

impl fmt::Display for GenerationPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GenerationPlan::Select(select) => {
                write!(f, "{}", select)
            }
        }
    }
}
