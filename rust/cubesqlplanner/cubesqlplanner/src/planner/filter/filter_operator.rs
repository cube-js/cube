use cubenativeutils::CubeError;
use std::str::FromStr;
pub enum FilterOperator {
    Equal,
    NotEqual,
    InDateRange,
    In,
    NotIn,
    Set,
    NotSet,
    Gt,
    Gte,
    Lt,
    Lte,
}
/*
*
 public inPlaceholders() {
   return `(${join(', ', this.filterParams().map(p => this.allocateCastParam(p)))})`;
 }

 public inWhere(column) {
   return `${column} IN ${this.inPlaceholders()}${this.orIsNullCheck(column, false)}`;
 }
 public notInWhere(column) {
   return `${column} NOT IN ${this.inPlaceholders()}${this.orIsNullCheck(column, true)}`;
 }

 public setWhere(column) {
   return `${column} IS NOT NULL`;
 }

 public notSetWhere(column) {
   return `${column} IS NULL`;
 }

 public gtWhere(column) {
   return `${column} > ${this.firstParameter()}`;
 }

 public gteWhere(column) {
   return `${column} >= ${this.firstParameter()}`;
 }

 public ltWhere(column) {
   return `${column} < ${this.firstParameter()}`;
 }

 public lteWhere(column) {
   return `${column} <= ${this.firstParameter()}`;
 }
*/
impl FromStr for FilterOperator {
    type Err = CubeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "equals" => Ok(Self::Equal),
            "notequals" => Ok(Self::NotEqual),
            "indaterange" => Ok(Self::InDateRange),
            "in" => Ok(Self::In),
            "notin" => Ok(Self::NotIn),
            "set" => Ok(Self::Set),
            "gt" => Ok(Self::Gt),
            "gte" => Ok(Self::Gte),
            "lt" => Ok(Self::Lt),
            "lte" => Ok(Self::Lte),

            _ => Err(CubeError::user(format!("Unknown filter operator {}", s))),
        }
    }
}
