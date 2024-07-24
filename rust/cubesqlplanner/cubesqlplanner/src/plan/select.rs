use super::expression::Expr;
use super::from::From;
use std::fmt;

pub struct Select<'cx> {
    pub projection: Vec<Expr<'cx>>,
    pub from: From<'cx>,
}

impl<'cx> fmt::Display for Select<'cx> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "SELECT")?;
        for expr in self.projection.iter().take(1) {
            write!(f, "{}", expr)?;
        }
        for expr in self.projection.iter().skip(1) {
            write!(f, ",{}", expr)?;
        }

        writeln!(f, "")?;
        write!(f, "{}", self.from)
    }
}
