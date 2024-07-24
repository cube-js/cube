//pub mod aggregation;
pub mod builder;
pub mod expression;
//pub mod filter;
pub mod from;
pub mod select;

pub use expression::Expr;
pub use from::From;
pub use select::Select;

use std::fmt::{self, write};

pub enum GenerationPlan<'cx> {
    Select(Select<'cx>),
}

impl<'cx> fmt::Display for GenerationPlan<'cx> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GenerationPlan::Select(select) => {
                write!(f, "{}", select)
            }
        }
    }
}
