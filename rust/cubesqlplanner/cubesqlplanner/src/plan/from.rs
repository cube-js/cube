use super::Join;
use crate::planner::BaseCube;
use std::fmt;
use std::rc::Rc;

pub enum From {
    Empty,
    Cube(Rc<BaseCube>),
    Join(Rc<Join>),
}

impl fmt::Display for From {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            From::Empty => write!(f, ""),
            From::Cube(cube) => {
                let cubesql = cube.to_sql().map_err(|_| fmt::Error)?;
                write!(f, "      {} ", cubesql)
            }
            From::Join(j) => {
                write!(f, "{}", j)
            }
        }
    }
}
