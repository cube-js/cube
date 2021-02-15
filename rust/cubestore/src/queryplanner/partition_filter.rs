use crate::table::{cmp_same_types, TableValue};
use arrow::datatypes::{DataType, Schema};
use datafusion::logical_plan::{Expr, Operator};
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
    pub fn can_match(&self, min_row: &[TableValue], max_row: &[TableValue]) -> bool {
        self.min_max.is_empty() || self.min_max.iter().any(|mm| mm.can_match(min_row, max_row))
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct MinMaxCondition {
    min: Vec<Option<TableValue>>, // 'None' means no limit.
    max: Vec<Option<TableValue>>,
}

impl MinMaxCondition {
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
                left: box Expr::Column(name, alias),
                op,
                right,
            } if Self::is_comparison(*op) => {
                if let Some(cc) = self.extract_column_compare(&name, alias.as_deref(), *op, right) {
                    self.apply_stat(&cc, &mut r);
                }
                return r;
            }
            Expr::BinaryExpr {
                left,
                op,
                right: box Expr::Column(name, alias),
            } if Self::is_comparison(*op) => {
                if let Some(cc) = self.extract_column_compare(
                    &name,
                    alias.as_deref(),
                    Self::invert_comparison(*op),
                    left,
                ) {
                    self.apply_stat(&cc, &mut r);
                }
                return r;
            }
            Expr::InList {
                expr: box Expr::Column(name, alias),
                list,
                negated: false,
            } => {
                // equivalent to <name> = <list_1> OR ... OR <name> = <list_n>.
                let elems = list.iter().map(|v| {
                    let mut r = r.clone();
                    if let Some(cc) =
                        self.extract_column_compare(&name, alias.as_deref(), Operator::Eq, v)
                    {
                        self.apply_stat(&cc, &mut r);
                        return r;
                    }
                    r
                });
                return self.handle_or(elems);
            }
            Expr::InList {
                expr: box Expr::Column(name, alias),
                list,
                negated: true,
            } => {
                // equivalent to <name> != <list_1> AND ... AND <name> != <list_n>.
                for v in list {
                    if let Some(cc) =
                        self.extract_column_compare(&name, alias.as_deref(), Operator::NotEq, v)
                    {
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
        for mut r in rs {
            if r.is_empty() {
                return Vec::new();
            }
            if res.is_none() {
                res = Some(r);
                continue;
            }
            if PartitionFilter::SIZE_LIMIT < res.as_mut().unwrap().len() + r.len() {
                assert!(r.len() <= PartitionFilter::SIZE_LIMIT);
                r.truncate(PartitionFilter::SIZE_LIMIT - r.len());
            }
            res.as_mut().unwrap().extend(r);
        }

        res.unwrap_or_default()
    }

    fn extract_column_compare(
        &self,
        col_name: &str,
        col_alias: Option<&str>,
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

        let field = datafusion::physical_plan::expressions::Column::new_with_alias(
            col_name,
            col_alias.map(|x| x.to_string()),
        )
        .lookup_field(self.schema)
        .ok()?;

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
            DataType::Utf8 => Self::extract_string(v),
            _ => None,
            // TODO: more data types
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

        assert!(!f.can_match(&[TableValue::Int(1)], &[TableValue::Int(1)]));
        assert!(f.can_match(&[TableValue::Null], &[TableValue::Int(1)]));

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
        assert!(f.can_match(&[], &[]));
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
        assert_eq!(f.min_max.len(), 50)
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
        fn get_table_provider(&self, _name: &str) -> Option<Arc<dyn TableProvider + Send + Sync>> {
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
