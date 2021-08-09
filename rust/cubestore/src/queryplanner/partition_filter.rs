use crate::table::{cmp_same_types, TableValue};
use arrow::datatypes::{DataType, Schema};
use datafusion::logical_plan::{Column, Expr, Operator};
use datafusion::scalar::ScalarValue;
use std::cmp::Ordering;

#[derive(Debug)]
pub struct PartitionFilter {
    /// Match on any single one of these is a match on the whole filter.
    /// Empty list is an exception and means "matches everything".
    min_max: Vec<MinMaxCondition>,
}

impl PartitionFilter {
    /// Length of `min_max` will not grow beyond this number.
    const SIZE_LIMIT: usize = 50;

    pub fn extract(s: &Schema, filters: &[Expr]) -> PartitionFilter {
        let builder = Builder { schema: s };

        let mut r = vec![];
        for f in filters {
            r = builder.extract_filter(f, r);
        }

        PartitionFilter { min_max: r }
    }

    /// Returns whether any rows between `min_row` and `max_row` could potentially match the filter.
    /// When this returns false, the corresponding rows can safely be ignored.
    pub fn can_match(
        &self,
        min_row: Option<&[TableValue]>,
        max_row: Option<&[TableValue]>,
    ) -> bool {
        if self.min_max.is_empty() {
            return true;
        }
        match (min_row, max_row) {
            (Some(mn), Some(mx)) => self.min_max.iter().any(|mm| mm.can_match(mn, mx)),
            (Some(mn), None) => self.min_max.iter().any(|mm| mm.can_match_min(mn)),
            (None, Some(mx)) => self.min_max.iter().any(|mm| mm.can_match_max(mx)),
            (None, None) => true,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct MinMaxCondition {
    min: Vec<Option<TableValue>>, // 'None' means no limit.
    max: Vec<Option<TableValue>>,
}

impl MinMaxCondition {
    /// Assuming max is unbounded.
    pub fn can_match_min(&self, min_row: &[TableValue]) -> bool {
        let n = self.max.len();
        assert_eq!(n, min_row.len());
        for i in 0..n {
            if !self.max[i].is_some() {
                return true;
            }
            let ord = cmp_same_types(self.max[i].as_ref().unwrap(), &min_row[i]);
            if ord < Ordering::Equal {
                return false;
            }
            if ord > Ordering::Equal {
                return true;
            }
            // continue if equal.
        }
        return true;
    }

    /// Assuming min is unbounded.
    pub fn can_match_max(&self, max_row: &[TableValue]) -> bool {
        let n = self.min.len();
        assert_eq!(n, max_row.len());
        for i in 0..n {
            if !self.min[i].is_some() {
                return true;
            }
            let ord = cmp_same_types(&max_row[i], self.min[i].as_ref().unwrap());
            if ord < Ordering::Equal {
                return false;
            }
            if ord > Ordering::Equal {
                return true;
            }
            // continue if equal.
        }
        return true;
    }

    pub fn can_match(&self, min_row: &[TableValue], max_row: &[TableValue]) -> bool {
        let n = self.min.len();
        assert_eq!(n, min_row.len());
        assert_eq!(n, max_row.len());
        for i in 0..n {
            if self.min[i].is_some()
                && cmp_same_types(&max_row[i], self.min[i].as_ref().unwrap()) < Ordering::Equal
            {
                return false;
            }
            if self.max[i].is_some()
                && cmp_same_types(self.max[i].as_ref().unwrap(), &min_row[i]) < Ordering::Equal
            {
                return false;
            }
            if min_row[i] != max_row[i] {
                return true;
            }
        }

        return true;
    }
}

struct Builder<'a> {
    schema: &'a Schema,
}

impl Builder<'_> {
    #[must_use]
    fn extract_filter(&self, e: &Expr, mut r: Vec<MinMaxCondition>) -> Vec<MinMaxCondition> {
        match e {
            Expr::BinaryExpr {
                left: box Expr::Column(c),
                op,
                right,
            } if Self::is_comparison(*op) => {
                if let Some(cc) = self.extract_column_compare(c, *op, right) {
                    self.apply_stat(&cc, &mut r);
                }
                return r;
            }
            Expr::BinaryExpr {
                left,
                op,
                right: box Expr::Column(c),
            } if Self::is_comparison(*op) => {
                if let Some(cc) = self.extract_column_compare(c, Self::invert_comparison(*op), left)
                {
                    self.apply_stat(&cc, &mut r);
                }
                return r;
            }
            Expr::InList {
                expr: box Expr::Column(c),
                list,
                negated: false,
            } => {
                // equivalent to <name> = <list_1> OR ... OR <name> = <list_n>.
                let elems = list.iter().map(|v| {
                    let mut r = r.clone();
                    if let Some(cc) = self.extract_column_compare(c, Operator::Eq, v) {
                        self.apply_stat(&cc, &mut r);
                        return r;
                    }
                    r
                });
                return self.handle_or(elems);
            }
            Expr::InList {
                expr: box Expr::Column(c),
                list,
                negated: true,
            } => {
                // equivalent to <name> != <list_1> AND ... AND <name> != <list_n>.
                for v in list {
                    if let Some(cc) = self.extract_column_compare(c, Operator::NotEq, v) {
                        self.apply_stat(&cc, &mut r);
                    }
                }
                return r;
            }
            Expr::BinaryExpr {
                left,
                op: Operator::And,
                right,
            } => {
                let r = self.extract_filter(left, r);
                return self.extract_filter(right, r);
            }
            Expr::BinaryExpr {
                box left,
                op: Operator::Or,
                box right,
            } => {
                return self.handle_or(
                    [left, right]
                        .iter()
                        .map(|e| self.extract_filter(e, r.clone())),
                );
            }
            Expr::Column(c) => {
                let true_expr = Expr::Literal(ScalarValue::Boolean(Some(true)));
                if let Some(cc) = self.extract_column_compare(c, Operator::Eq, &true_expr) {
                    self.apply_stat(&cc, &mut r);
                    return r;
                }
                r
            }
            // TODO: generic Not support with other expressions as children.
            Expr::Not(box Expr::Column(c)) => {
                let true_expr = Expr::Literal(ScalarValue::Boolean(Some(false)));
                if let Some(cc) = self.extract_column_compare(c, Operator::Eq, &true_expr) {
                    self.apply_stat(&cc, &mut r);
                    return r;
                }
                r
            }
            _ => r,
            // TODO: most important unsupported expressions are:
            //       - IsNull/IsNotNull
            //       - Not
            //       - Between
        }
    }

    /// <e_1> OR <e_2> OR ... OR <e_n>
    fn handle_or<Iter: Iterator<Item = Vec<MinMaxCondition>>>(
        &self,
        rs: Iter,
    ) -> Vec<MinMaxCondition> {
        let mut res = None;
        for r in rs {
            if r.is_empty() {
                return Vec::new();
            }
            let res = match &mut res {
                Some(res) => res,
                res @ None => {
                    *res = Some(r);
                    continue;
                }
            };
            res.extend(r);
            if PartitionFilter::SIZE_LIMIT < res.len() {
                Self::fold_or_inplace(res);
            }
        }

        res.unwrap_or_default()
    }

    /// Reduces the number of stored [MinMaxCondition]s, loosing some information.
    fn fold_or_inplace(cs: &mut Vec<MinMaxCondition>) {
        assert!(!cs.is_empty());
        let (r, tail) = cs.split_first_mut().unwrap();

        for c in tail {
            for i in 0..r.min.len() {
                if r.min[i].is_none() {
                    continue;
                }
                if c.min[i].is_none()
                    || cmp_same_types(c.min[i].as_ref().unwrap(), &r.min[i].as_ref().unwrap())
                        < Ordering::Equal
                {
                    r.min[i] = c.min[i].clone();
                }
            }

            for i in 0..r.max.len() {
                if r.max[i].is_none() {
                    continue;
                }
                if c.max[i].is_none()
                    || cmp_same_types(&r.max[i].as_ref().unwrap(), c.max[i].as_ref().unwrap())
                        < Ordering::Equal
                {
                    r.max[i] = c.max[i].clone();
                }
            }
        }

        cs.truncate(1);
    }

    fn extract_column_compare(
        &self,
        col: &Column,
        op: Operator,
        value: &Expr,
    ) -> Option<ColumnStat> {
        // TODO: fold constant expressions.
        let scalar;
        if let Expr::Literal(s) = value {
            scalar = s;
        } else {
            return None;
        }

        let field = self.schema.field_with_name(&col.name).ok()?;

        // TODO: all the other types. For now assume strings and numbers.
        let limit_val;
        if let Some(v) = Self::scalar_to_value(scalar, field.data_type()) {
            limit_val = v;
        } else {
            return None;
        }

        let mut cc = ColumnStat {
            col_index: self.schema.column_with_name(field.name()).unwrap().0,
            min_val: None,
            max_val: None,
        };
        match op {
            Operator::Lt => cc.max_val = Some(Self::try_minus_one(limit_val)),
            Operator::LtEq => cc.max_val = Some(limit_val),
            Operator::Gt => cc.min_val = Some(Self::try_plus_one(limit_val)),
            Operator::GtEq => cc.min_val = Some(limit_val),
            Operator::Eq => {
                cc.min_val = Some(limit_val.clone());
                cc.max_val = Some(limit_val);
            }
            Operator::NotEq => {
                if limit_val != TableValue::Null {
                    return None; // TODO: support this too.
                }
                cc.min_val = Self::min_value(field.data_type());
            }
            _ => panic!("unhandled comparison operator"),
        }

        Some(cc)
    }

    fn apply_stat(&self, c: &ColumnStat, r: &mut Vec<MinMaxCondition>) {
        if r.is_empty() {
            r.push(MinMaxCondition {
                min: vec![None; self.schema.fields().len()],
                max: vec![None; self.schema.fields().len()],
            });
        }

        for mm in r {
            if let Some(mn) = &c.min_val {
                Self::update_min(&mut mm.min[c.col_index], mn);
            }
            if let Some(mx) = &c.max_val {
                Self::update_max(&mut mm.max[c.col_index], mx);
            }
        }
    }

    fn min_value(t: &DataType) -> Option<TableValue> {
        match t {
            t if Self::is_signed_int(t) => Some(TableValue::Int(i64::min_value())),
            DataType::Utf8 => Some(TableValue::String("".to_string())),
            _ => None,
            // TODO: more data types
        }
    }

    fn try_minus_one(mut v: TableValue) -> TableValue {
        match &mut v {
            TableValue::Int(i) if *i != i64::min_value() => *i -= 1,
            _ => (),
        }
        v
    }

    fn try_plus_one(mut v: TableValue) -> TableValue {
        match &mut v {
            TableValue::Int(i) if *i != i64::max_value() => *i += 1,
            _ => (),
        }
        v
    }

    fn scalar_to_value(v: &ScalarValue, t: &DataType) -> Option<TableValue> {
        if v.is_null() {
            return Some(TableValue::Null);
        }
        match t {
            t if Self::is_signed_int(t) => Self::extract_signed_int(v),
            DataType::Boolean => Self::extract_bool(v),
            DataType::Utf8 => Self::extract_string(v),
            _ => None,
            // TODO: more data types
        }
    }

    fn extract_bool(v: &ScalarValue) -> Option<TableValue> {
        match v {
            ScalarValue::Boolean(v) => v.as_ref().map(|v| TableValue::Boolean(*v)),
            ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) => {
                if s.is_none() {
                    return None;
                }
                let s = s.as_ref().unwrap().as_str();
                let b;
                if s.eq_ignore_ascii_case("true") {
                    b = true;
                } else if s.eq_ignore_ascii_case("false") {
                    b = false;
                } else {
                    b = s.parse::<i64>().ok()? != 0;
                }
                Some(TableValue::Boolean(b))
            }
            _ => return None,
        }
    }

    fn extract_string(v: &ScalarValue) -> Option<TableValue> {
        let s = match v {
            ScalarValue::Utf8(v) => v.as_ref().map(|s| s.clone()),
            ScalarValue::Int8(v) => v.as_ref().map(|i| i.to_string()),
            ScalarValue::Int16(v) => v.as_ref().map(|i| i.to_string()),
            ScalarValue::Int32(v) => v.as_ref().map(|i| i.to_string()),
            ScalarValue::Int64(v) => v.as_ref().map(|i| i.to_string()),
            _ => return None, // TODO: casts.
        };
        Some(TableValue::String(s.unwrap()))
    }

    fn extract_signed_int(v: &ScalarValue) -> Option<TableValue> {
        let ival = match v {
            ScalarValue::Int8(v) => v.unwrap() as i64,
            ScalarValue::Int16(v) => v.unwrap() as i64,
            ScalarValue::Int32(v) => v.unwrap() as i64,
            ScalarValue::Int64(v) => v.unwrap() as i64,
            ScalarValue::Float64(v) => v.unwrap() as i64,
            ScalarValue::Float32(v) => v.unwrap() as i64,
            ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) => {
                match s.as_ref().unwrap().parse::<i64>() {
                    Ok(v) => v,
                    Err(_) => {
                        log::error!("could not convert string to int: {}", s.as_ref().unwrap());
                        return None;
                    }
                }
            }
            _ => return None, // TODO: casts.
        };
        Some(TableValue::Int(ival))
    }

    fn is_signed_int(t: &DataType) -> bool {
        match t {
            DataType::Int8 => true,
            DataType::Int16 => true,
            DataType::Int32 => true,
            DataType::Int64 => true,
            _ => false,
        }
    }

    fn update_min(l: &mut Option<TableValue>, r: &TableValue) {
        if l.is_none() || cmp_same_types(l.as_ref().unwrap(), r) < Ordering::Equal {
            *l = Some(r.clone());
        }
    }
    fn update_max(l: &mut Option<TableValue>, r: &TableValue) {
        if l.is_none() || cmp_same_types(r, l.as_ref().unwrap()) < Ordering::Equal {
            *l = Some(r.clone());
        }
    }

    fn is_comparison(op: Operator) -> bool {
        match op {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => true,
            _ => false,
        }
    }

    fn invert_comparison(op: Operator) -> Operator {
        match op {
            Operator::Eq => Operator::NotEq,
            Operator::NotEq => Operator::Eq,
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            _ => panic!("not a comparison"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::{CubeStoreParser, Statement as CubeStatement};
    use arrow::datatypes::Field;
    use datafusion::catalog::TableReference;
    use datafusion::datasource::TableProvider;
    use datafusion::logical_plan::ToDFSchema;
    use datafusion::physical_plan::udaf::AggregateUDF;
    use datafusion::physical_plan::udf::ScalarUDF;
    use datafusion::sql::planner::{ContextProvider, SqlToRel};
    use smallvec::alloc::sync::Arc;
    use sqlparser::ast::{Query, Select, SelectItem, SetExpr, Statement as SQLStatement};

    #[test]
    fn test_simple_extract() {
        let s = schema(&[("a", DataType::Int64)]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        // Comparisons.
        assert_eq!(
            extract("a < 10").min_max,
            vec![MinMaxCondition {
                min: vec![None],
                max: vec![Some(TableValue::Int(9))],
            }]
        );
        assert_eq!(
            extract("a <= 10").min_max,
            vec![MinMaxCondition {
                min: vec![None],
                max: vec![Some(TableValue::Int(10))],
            }]
        );

        assert_eq!(
            extract("a > 10").min_max,
            vec![MinMaxCondition {
                min: vec![Some(TableValue::Int(11))],
                max: vec![None],
            }],
        );
        let min_is_10 = vec![MinMaxCondition {
            min: vec![Some(TableValue::Int(10))],
            max: vec![None],
        }];
        assert_eq!(extract("a >= 10").min_max, min_is_10,);
        assert_eq!(extract("a >= 10").min_max, min_is_10,);

        // Equality.
        assert_eq!(
            extract("a = 10").min_max,
            vec![MinMaxCondition {
                min: vec![Some(TableValue::Int(10))],
                max: vec![Some(TableValue::Int(10))],
            }]
        );

        // Order does not matter.
        assert_eq!(
            extract("10 > a").min_max,
            vec![MinMaxCondition {
                min: vec![None],
                max: vec![Some(TableValue::Int(9))],
            }]
        );
        assert_eq!(
            extract("10 >= a").min_max,
            vec![MinMaxCondition {
                min: vec![None],
                max: vec![Some(TableValue::Int(10))],
            }]
        );

        // `IN` and `NOT IN` expressions.
        assert_eq!(
            extract("a IN (10)").min_max,
            vec![MinMaxCondition {
                min: vec![Some(TableValue::Int(10))],
                max: vec![Some(TableValue::Int(10))],
            }]
        );

        // TODO: more efficient encoding of these cases.
        assert_eq!(
            extract("a IN (10, 11, 12)").min_max,
            vec![
                MinMaxCondition {
                    min: vec![Some(TableValue::Int(10))],
                    max: vec![Some(TableValue::Int(10))],
                },
                MinMaxCondition {
                    min: vec![Some(TableValue::Int(11))],
                    max: vec![Some(TableValue::Int(11))],
                },
                MinMaxCondition {
                    min: vec![Some(TableValue::Int(12))],
                    max: vec![Some(TableValue::Int(12))],
                }
            ]
        );

        assert_eq!(
            extract("a NOT IN (NULL)").min_max,
            vec![] // TODO: below is expected result, but the parser never produces negated `in list`.
                   // vec![MinMaxCondition {
                   //     min: vec![Some(TableValue::Int(i64::min_value()))],
                   //     max: vec![None],
                   // }]
        );

        // Parentheses do not change anything. Note that parentheses are removed by the parser.
        assert_eq!(
            extract("((((a <= (10)))))").min_max,
            vec![MinMaxCondition {
                min: vec![None],
                max: vec![Some(TableValue::Int(10))],
            }]
        );
    }

    #[test]
    fn test_string() {
        let s = schema(&[("a", DataType::Utf8)]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        assert_eq!(
            extract("a = 'FOO'").min_max,
            vec![MinMaxCondition {
                min: vec![Some(TableValue::String("FOO".to_string()))],
                max: vec![Some(TableValue::String("FOO".to_string()))],
            }]
        );
    }

    #[test]
    fn test_bools() {
        let s = schema(&[("a", DataType::Boolean)]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        let true_cond = vec![MinMaxCondition {
            min: vec![Some(TableValue::Boolean(true))],
            max: vec![Some(TableValue::Boolean(true))],
        }];
        let false_cond = vec![MinMaxCondition {
            min: vec![Some(TableValue::Boolean(false))],
            max: vec![Some(TableValue::Boolean(false))],
        }];

        assert_eq!(extract("a = true").min_max, true_cond);
        assert_eq!(extract("a = false").min_max, false_cond);

        assert_eq!(extract("a = 'true'").min_max, true_cond);
        assert_eq!(extract("a = 'TRUE'").min_max, true_cond);
        assert_eq!(extract("a = 'false'").min_max, false_cond);
        assert_eq!(extract("a = 'FALSE'").min_max, false_cond);

        assert_eq!(extract("a = '1'").min_max, true_cond);
        assert_eq!(extract("a = '0'").min_max, false_cond);

        assert_eq!(extract("a").min_max, true_cond);
        assert_eq!(extract("NOT a").min_max, false_cond);
    }

    #[test]
    fn test_arithmetic_corner_cases() {
        let s = schema(&[("a", DataType::Int64)]);
        let extract = |sql: &str| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        assert_eq!(
            extract(&format!("a < {}", i64::min_value())).min_max,
            vec![MinMaxCondition {
                min: vec![None],
                max: vec![Some(TableValue::Int(i64::min_value()))],
            }]
        );

        assert_eq!(
            extract(&format!("a > {}", i64::max_value())).min_max,
            vec![MinMaxCondition {
                min: vec![Some(TableValue::Int(i64::max_value()))],
                max: vec![None],
            }]
        );
    }

    #[test]
    fn test_nulls() {
        let s = schema(&[("a", DataType::Int64)]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        let f = extract("a = NULL");
        assert_eq!(
            f.min_max,
            vec![MinMaxCondition {
                min: vec![Some(TableValue::Null)],
                max: vec![Some(TableValue::Null)],
            }]
        );

        assert!(!f.can_match(Some(&[TableValue::Int(1)]), Some(&[TableValue::Int(1)])));
        assert!(f.can_match(Some(&[TableValue::Null]), Some(&[TableValue::Int(1)])));

        let f = extract("a != NULL");
        assert_eq!(
            f.min_max,
            vec![MinMaxCondition {
                min: vec![Some(TableValue::Int(i64::min_value()))],
                max: vec![None],
            }]
        );

        let s = schema(&[("b", DataType::Utf8)]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);
        assert_eq!(
            extract("b != NULL").min_max,
            vec![MinMaxCondition {
                min: vec![Some(TableValue::String("".to_string()))],
                max: vec![None],
            }]
        )
    }

    #[test]
    fn test_multiple_vars() {
        let s = schema(&[
            ("a", DataType::Int64),
            ("b", DataType::Int64),
            ("c", DataType::Utf8),
        ]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        assert_eq!(
            extract("b <= 10").min_max,
            vec![MinMaxCondition {
                min: vec![None, None, None],
                max: vec![None, Some(TableValue::Int(10)), None],
            }]
        );
    }

    #[test]
    fn test_conditionals_simple() {
        let s = schema(&[("a", DataType::Int64)]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        assert_eq!(
            extract("a <= 10 AND a >= 5").min_max,
            vec![MinMaxCondition {
                min: vec![Some(TableValue::Int(5))],
                max: vec![Some(TableValue::Int(10))],
            }]
        );

        assert_eq!(
            extract("a <= 10 AND a <= 4").min_max,
            vec![MinMaxCondition {
                min: vec![None],
                max: vec![Some(TableValue::Int(4))],
            }]
        );

        assert_eq!(
            extract("a >= 10 AND a >= 4").min_max,
            vec![MinMaxCondition {
                min: vec![Some(TableValue::Int(10))],
                max: vec![None],
            }]
        );

        // TODO: improve our representation for this cases.
        assert_eq!(
            extract("a >= 10 OR a <= 5").min_max,
            vec![
                MinMaxCondition {
                    min: vec![Some(TableValue::Int(10))],
                    max: vec![None],
                },
                MinMaxCondition {
                    min: vec![None],
                    max: vec![Some(TableValue::Int(5))],
                },
            ]
        );
    }

    #[test]
    fn test_conditionals_multi_var() {
        let s = schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        assert_eq!(
            extract("a <= 10 AND b >= 5").min_max,
            vec![MinMaxCondition {
                min: vec![None, Some(TableValue::Int(5))],
                max: vec![Some(TableValue::Int(10)), None],
            },]
        );

        assert_eq!(
            extract("a <= 10 AND b >= 5 OR a = 4 AND b <= 7").min_max,
            vec![
                MinMaxCondition {
                    min: vec![None, Some(TableValue::Int(5))],
                    max: vec![Some(TableValue::Int(10)), None],
                },
                MinMaxCondition {
                    min: vec![Some(TableValue::Int(4)), None],
                    max: vec![Some(TableValue::Int(4)), Some(TableValue::Int(7))],
                }
            ]
        );

        let s = schema(&[
            ("a", DataType::Int64),
            ("b", DataType::Int64),
            ("c", DataType::Int64),
        ]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);
        assert_eq!(
            extract("a = 3 AND (b >= 5 OR c <= 3)").min_max,
            vec![
                MinMaxCondition {
                    min: vec![Some(TableValue::Int(3)), Some(TableValue::Int(5)), None],
                    max: vec![Some(TableValue::Int(3)), None, None],
                },
                MinMaxCondition {
                    min: vec![Some(TableValue::Int(3)), None, None],
                    max: vec![Some(TableValue::Int(3)), None, Some(TableValue::Int(3))],
                }
            ]
        );
    }

    #[test]
    fn test_apply() {
        let c = MinMaxCondition {
            min: vec![Some(TableValue::Int(1))],
            max: vec![Some(TableValue::Int(2))],
        };
        assert!(!c.can_match(&[TableValue::Int(-1)], &[TableValue::Int(0)]));
        assert!(!c.can_match(&[TableValue::Int(3)], &[TableValue::Int(4)]));
        assert!(c.can_match(&[TableValue::Int(0)], &[TableValue::Int(1)]));
        assert!(c.can_match(&[TableValue::Int(2)], &[TableValue::Int(3)]));

        let c = MinMaxCondition {
            min: vec![None],
            max: vec![Some(TableValue::Int(2))],
        };
        assert!(c.can_match(&[TableValue::Int(-1)], &[TableValue::Int(0)]));
        assert!(!c.can_match(&[TableValue::Int(3)], &[TableValue::Int(4)]));
        assert!(c.can_match(&[TableValue::Int(0)], &[TableValue::Int(1)]));
        assert!(c.can_match(&[TableValue::Int(2)], &[TableValue::Int(3)]));

        let c = MinMaxCondition {
            min: vec![Some(TableValue::Int(1))],
            max: vec![None],
        };
        assert!(!c.can_match(&[TableValue::Int(-1)], &[TableValue::Int(0)]));
        assert!(c.can_match(&[TableValue::Int(3)], &[TableValue::Int(4)]));
        assert!(c.can_match(&[TableValue::Int(0)], &[TableValue::Int(1)]));
        assert!(c.can_match(&[TableValue::Int(2)], &[TableValue::Int(3)]));

        let c = MinMaxCondition {
            min: vec![None],
            max: vec![None],
        };
        assert!(c.can_match(&[TableValue::Int(-1)], &[TableValue::Int(0)]));
        assert!(c.can_match(&[TableValue::Int(3)], &[TableValue::Int(4)]));
        assert!(c.can_match(&[TableValue::Int(0)], &[TableValue::Int(1)]));
        assert!(c.can_match(&[TableValue::Int(2)], &[TableValue::Int(3)]));
    }

    #[test]
    fn test_empty_filter() {
        let f = PartitionFilter::extract(
            &Schema::new(vec![]),
            &[Expr::Literal(ScalarValue::Boolean(Some(true)))],
        );
        assert_eq!(f.min_max, vec![]);
        assert!(f.can_match(Some(&[]), Some(&[])));
    }

    #[test]
    fn test_missing_min_or_max() {
        let mm = MinMaxCondition {
            min: vec![Some(TableValue::Int(10))],
            max: vec![Some(TableValue::Int(11))],
        };
        assert!(mm.can_match_min(&[TableValue::Int(9)]));
        assert!(mm.can_match_min(&[TableValue::Int(10)]));
        assert!(mm.can_match_min(&[TableValue::Int(11)]));
        assert!(!mm.can_match_min(&[TableValue::Int(12)]));

        assert!(!mm.can_match_max(&[TableValue::Int(9)]));
        assert!(mm.can_match_max(&[TableValue::Int(10)]));
        assert!(mm.can_match_max(&[TableValue::Int(11)]));
        assert!(mm.can_match_max(&[TableValue::Int(12)]));

        let mm = MinMaxCondition {
            min: vec![Some(TableValue::Int(0)), Some(TableValue::Int(10))],
            max: vec![Some(TableValue::Int(0)), Some(TableValue::Int(11))],
        };
        assert!(mm.can_match_min(&[TableValue::Int(0), TableValue::Int(9)]));
        assert!(mm.can_match_min(&[TableValue::Int(0), TableValue::Int(10)]));
        assert!(mm.can_match_min(&[TableValue::Int(0), TableValue::Int(11)]));
        assert!(!mm.can_match_min(&[TableValue::Int(0), TableValue::Int(12)]));

        assert!(!mm.can_match_max(&[TableValue::Int(0), TableValue::Int(9)]));
        assert!(mm.can_match_max(&[TableValue::Int(0), TableValue::Int(10)]));
        assert!(mm.can_match_max(&[TableValue::Int(0), TableValue::Int(11)]));
        assert!(mm.can_match_max(&[TableValue::Int(0), TableValue::Int(12)]));

        let mm = MinMaxCondition {
            min: vec![Some(TableValue::Int(0)), Some(TableValue::Int(10))],
            max: vec![Some(TableValue::Int(1)), Some(TableValue::Int(11))],
        };
        assert!(mm.can_match_min(&[TableValue::Int(-1), TableValue::Int(12)]));
        assert!(mm.can_match_max(&[TableValue::Int(3), TableValue::Int(9)]));

        let mm = MinMaxCondition {
            min: vec![None, Some(TableValue::Int(10))],
            max: vec![None, Some(TableValue::Int(11))],
        };
        assert!(mm.can_match_min(&[TableValue::Int(0), TableValue::Int(12)]));
        assert!(mm.can_match_max(&[TableValue::Int(0), TableValue::Int(9)]));
    }

    #[test]
    fn test_apply_multi_var() {
        let c = MinMaxCondition {
            min: vec![None, Some(TableValue::Int(4))],
            max: vec![None, Some(TableValue::Int(5))],
        };
        // Filter by `min` on the right column.
        assert!(!c.can_match(
            &[TableValue::Int(0), TableValue::Int(0)],
            &[TableValue::Int(0), TableValue::Int(3)]
        ));
        assert!(c.can_match(
            &[TableValue::Int(0), TableValue::Int(0)],
            &[TableValue::Int(0), TableValue::Int(4)]
        ));

        // Filter by `max` on the right column.
        assert!(!c.can_match(
            &[TableValue::Int(0), TableValue::Int(6)],
            &[TableValue::Int(0), TableValue::Int(7)]
        ));
        assert!(c.can_match(
            &[TableValue::Int(0), TableValue::Int(5)],
            &[TableValue::Int(0), TableValue::Int(7)]
        ));

        // Cannot filter on second column if the first column changes.
        assert!(c.can_match(
            &[TableValue::Int(0), TableValue::Int(0)],
            &[TableValue::Int(1), TableValue::Int(3)]
        ));
        assert!(c.can_match(
            &[TableValue::Int(0), TableValue::Int(6)],
            &[TableValue::Int(1), TableValue::Int(7)]
        ));
    }

    #[test]
    fn test_unhandled_expressions() {
        let s = schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        // Comparing two fields does not yield useful information.
        assert_eq!(extract("a = b").min_max, vec![]);
        assert_eq!(
            extract("a = 10 AND a = b").min_max,
            vec![MinMaxCondition {
                min: vec![Some(TableValue::Int(10)), None],
                max: vec![Some(TableValue::Int(10)), None],
            }]
        );
        assert_eq!(extract("a >= 10 OR a = b").min_max, vec![]);

        // Many expressions are not supported.
        assert_eq!(extract("a/10 = 124").min_max, vec![]);
        assert_eq!(extract("a = 1+1").min_max, vec![]);
    }

    #[test]
    fn test_deep_expressions() {
        let s = schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        let f = extract("(a <= 1 or b <= 2) and (a <= 2 or b <= 3) and (a <= 4 or b <= 5) and (a <= 6 or b <= 7) and (a <= 8 or b <= 9) and (a <= 10 or b <= 11)");
        // Must bail out to avoid too much compute.
        assert_eq!(
            f.min_max,
            vec![MinMaxCondition {
                min: vec![None, None],
                max: vec![None, None],
            }]
        );
    }

    #[test]
    fn test_conversions() {
        let s = schema(&[("a", DataType::Utf8)]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        assert_eq!(
            extract("a <= 1").min_max,
            vec![MinMaxCondition {
                min: vec![None],
                max: vec![Some(TableValue::String("1".to_string()))],
            }]
        );

        let s = schema(&[("b", DataType::Int64)]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);
        assert_eq!(
            extract("b <= '1'").min_max,
            vec![MinMaxCondition {
                min: vec![None],
                max: vec![Some(TableValue::Int(1))],
            }]
        );
    }

    #[test]
    fn test_limits_no_panic() {
        let s = schema(&[
            ("a", DataType::Int64),
            ("b", DataType::Int64),
            ("c", DataType::Int64),
            ("d", DataType::Int64),
            ("e", DataType::Int64),
        ]);
        let extract = |sql| PartitionFilter::extract(&s, &[parse(sql, &s)]);

        let filter = extract(
            "a IN (1,2,3,4,5,6,7,8,9) \
                 AND b = 1 \
                 AND c = 1 \
                 AND d IN (1,2,3,4,5,6,7) \
                 AND e IN (1, 2)",
        );
        assert_ne!(filter.min_max.len(), 0);
        assert!(filter.can_match(
            Some(&vec![TableValue::Int(1); 5]),
            Some(&vec![TableValue::Int(1); 5])
        ));
        let max_row = &[9, 1, 1, 7, 2];
        assert!(filter.can_match(Some(&vals(max_row)), Some(&vals(max_row))));

        // Check we keep information about min and max values for each field.
        for i in 0..s.fields().len() {
            let mut row_before = vec![1; 5];
            row_before[i] -= 1;
            assert!(
                !filter.can_match(Some(&vals(&row_before)), Some(&vals(&row_before))),
                "must not match {:?}",
                row_before
            );

            let mut row_after = max_row.to_vec();
            row_after[i] += 1;
            assert!(
                !filter.can_match(Some(&vals(&row_after)), Some(&vals(&row_after))),
                "must not match {:?}",
                row_after
            );
        }

        fn vals(is: &[i64]) -> Vec<TableValue> {
            is.iter().map(|i| TableValue::Int(*i)).collect()
        }
    }

    fn schema(s: &[(&str, DataType)]) -> Schema {
        Schema::new(
            s.iter()
                .map(|(name, dt)| Field::new(name, dt.clone(), false))
                .collect(),
        )
    }

    fn parse(s: &str, schema: &Schema) -> Expr {
        let sql_expr;
        let parsed = CubeStoreParser::new(&format!("SELECT {}", s))
            .unwrap()
            .parse_statement()
            .unwrap();
        match parsed {
            CubeStatement::Statement(SQLStatement::Query(box Query {
                body: SetExpr::Select(box Select { projection, .. }),
                ..
            })) => match projection.as_slice() {
                [SelectItem::UnnamedExpr(e)] => sql_expr = e.clone(),
                _ => panic!("unexpected projection in parse result"),
            },
            _ => panic!("unexpected parse result"),
        }

        SqlToRel::new(&NoContextProvider {})
            .sql_to_rex(&sql_expr, &schema.clone().to_dfschema().unwrap())
            .unwrap()
    }

    pub struct NoContextProvider {}
    impl ContextProvider for NoContextProvider {
        fn get_table_provider(&self, _name: TableReference) -> Option<Arc<dyn TableProvider>> {
            None
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            None
        }
    }
}

struct ColumnStat {
    col_index: usize,
    min_val: Option<TableValue>,
    max_val: Option<TableValue>,
}
