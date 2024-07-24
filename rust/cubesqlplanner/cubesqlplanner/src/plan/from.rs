use crate::planner::BaseCube;
use std::fmt;
use std::rc::Rc;

pub enum From<'cx> {
    Empty,
    Cube(Rc<BaseCube<'cx>>),
}

impl<'cx> fmt::Display for From<'cx> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            From::Empty => write!(f, ""),
            From::Cube(cube) => {
                writeln!(f, "FROM")?;
                let cubesql = cube.to_sql().map_err(|_| fmt::Error)?;
                write!(f, "{}", cubesql)
            }
        }
    }
}
